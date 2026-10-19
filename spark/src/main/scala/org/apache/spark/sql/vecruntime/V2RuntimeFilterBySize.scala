/*
 * Copyright 2026-2027 Angel Conde and the vecruntime contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.vecruntime

import io.vecruntime.spark.VectorConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  And,
  AttributeSet,
  BloomFilterMightContain,
  ExprId,
  PredicateHelper,
  ScalarSubquery,
  XxHash64
}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.{InjectRuntimeFilter, JoinSelectionHelper, PushDownPredicates}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan, Project, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * #735: Spark's runtime bloom filter built from a small Iceberg dimension, as from its Parquet copy.
 *
 * Spark's `InjectRuntimeFilter` builds a bloom filter from a selective filter over a scan only when that creation
 * side's estimate is at most `spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold` (10 MB). An Iceberg
 * scan estimates its rows times Spark's default column widths: at 3 TB TPC-DS `customer_demographics` filtered to
 * one marital status is estimated far above that, its Parquet copy's 7.8 MB of files below it, so q72's
 * `catalog_sales` gets that filter on Parquet and not on Iceberg and its joins carry five times the rows (20 s
 * against 11 s).
 *
 * This rule runs Spark's own rule again, after Spark's (it runs in the last optimizer batch), with that threshold
 * raised to `spark.vecruntime.optimizer.v2BroadcastBySize.maxEstimatedBytes`, and keeps only the filters it adds
 * whose creation side is one DSv2 scan under projections and filters that is small by the bytes of the files it
 * planned (`V2BroadcastBySize.byFiles`). Every other filter of that run is dropped, so a creation
 * side Spark declined on any other ground stays declined. Spark's cap on the number of runtime filters counts the
 * filters already present. The kept filters are pushed down toward their scans as Spark's later batch would.
 *
 * The other way round, Spark's own rule ran before the broadcast hints too: a join with a dimension small by its files
 * looked like a shuffle join on Iceberg, and the rule put filters on the fact that rest on that shuffle. First, each
 * of Spark's filters is checked again against Spark's own premise with the hints in place -- the join it filters
 * through is probably a shuffle join, or its application side has a shuffle below -- and dropped when neither holds,
 * as Spark would not have planted it on the Parquet copy (3 TB q7: `store_sales` probed with `promotion`'s key,
 * 1,792 of its 1,800 rows, 74 s of probe CPU to remove 0.4 % of 1.66 G rows; and with `customer_demographics`'). Off with
 * `spark.vecruntime.optimizer.v2RuntimeFilterBySize.enabled=false`, with Spark's bloom filter off, and with the
 * plugin.
 */
case class V2RuntimeFilterBySize(session: SparkSession)
    extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  override def apply(input: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (!VectorConf.v2RuntimeFilterBySizeEnabled(conf) || !conf.runtimeFilterBloomFilterEnabled) return input
    if (!input.exists(_.isInstanceOf[DataSourceV2ScanRelation])) return input
    val plan = dropUnfounded(input, conf)
    val threshold = conf.runtimeFilterCreationSideThreshold
    val raised = VectorConf.v2BroadcastBySizeMaxEstimatedBytes(conf)
    if (raised <= threshold) return plan
    val before = bloomIds(plan)
    val room = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD) - before.size
    if (room <= 0) return plan
    val local = conf.clone()
    local.setConf(SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD, raised)
    local.setConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD, room)
    val injected = SQLConf.withExistingConf(local)(InjectRuntimeFilter(plan))
    if (injected.fastEquals(plan)) return plan
    // A table's partition columns are pruned by dynamic partition pruning, which runs before Spark's rule; there
    // Spark's rule sees the pruning filter and declines a bloom filter on the same key (q14a's `ss_sold_date_sk`
    // from one month of `date_dim`). In this batch the pruning filter is no longer in that form, so a new filter on
    // a partition column is dropped here: it would only cost its build (two exchanges in q14a) and its probe.
    val partitionKeys = AttributeSet(plan.collect {
      // Identity columns only (#806): a filter on a transform's source column (`days(ts)`, `bucket(n, k)`) prunes
      // whole partitions, so a row-level bloom filter on it still filters.
      case s: DataSourceV2ScanRelation => V2PruningColumns.identityColumns(s.relation).toSeq
      case l: LogicalRelation if l.relation.isInstanceOf[HadoopFsRelation] =>
        val names = l.relation.asInstanceOf[HadoopFsRelation].partitionSchema.fieldNames
        val resolver = conf.resolver
        l.output.filter(a => names.exists(n => resolver(n, a.name)))
    }.flatten)
    def onPartition(b: BloomFilterMightContain): Boolean = b.valueExpression match {
      case XxHash64(Seq(v), _) => v.references.nonEmpty && v.references.subsetOf(partitionKeys)
      case _ => false
    }
    def keep(b: BloomFilterMightContain): Boolean = b.bloomFilterExpression match {
      case s: ScalarSubquery if before.contains(s.exprId) => true
      case s: ScalarSubquery => !onPartition(b) && smallByFiles(creationOf(s.plan), threshold)
      case _ => true
    }
    val kept = injected.transformUp {
      case f @ Filter(cond, child) =>
        val parts = splitConjunctivePredicates(cond)
        val left = parts.filter {
          case b: BloomFilterMightContain => keep(b)
          case _ => true
        }
        if (left.size == parts.size) f
        else if (left.isEmpty) child
        else Filter(left.reduce(And), child)
    }
    if (bloomIds(kept) == before) return plan
    pushDown(kept)
  }

  /**
   * `plan` without the runtime bloom filters whose join premise no longer holds (see the class comment). A filter is
   * matched to the join pairing its probed key with its creation key; a filter with no such join, or not of Spark's
   * shape (one `bloom_filter_agg` over a hashed key), is kept.
   */
  private def dropUnfounded(plan: LogicalPlan, conf: SQLConf): LogicalPlan = {
    def shuffleJoin(l: LogicalPlan, r: LogicalPlan, hint: JoinHint): Boolean =
      !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) &&
        !canBroadcastBySize(l, conf) && !canBroadcastBySize(r, conf)
    def hasShuffle(p: LogicalPlan): Boolean = p.exists {
      case Join(l, r, _, _, hint) => shuffleJoin(l, r, hint)
      case _: Aggregate | _: Window => true
      case _ => false
    }
    val joins = plan.collect { case ExtractEquiJoinKeys(_, lk, rk, _, _, l, r, hint) => (lk.zip(rk), l, r, hint) }
    def creationKey(sub: LogicalPlan) = sub.flatMap(_.expressions).flatMap(_.collect {
      case a: BloomFilterAggregate => a.child
    }).headOption.collect { case XxHash64(Seq(k), _) => k }
    def founded(b: BloomFilterMightContain): Boolean = (b.bloomFilterExpression, b.valueExpression) match {
      case (sub: ScalarSubquery, XxHash64(Seq(app), _)) =>
        creationKey(sub.plan).forall { ck =>
          val through = joins.flatMap { case (pairs, l, r, hint) =>
            pairs.collectFirst {
              case (a, c) if a.semanticEquals(app) && c.semanticEquals(ck) => (l, r, hint, l)
              case (c, a) if a.semanticEquals(app) && c.semanticEquals(ck) => (l, r, hint, r)
            }
          }
          through.isEmpty || through.exists { case (l, r, hint, side) => shuffleJoin(l, r, hint) || hasShuffle(side) }
        }
      case _ => true
    }
    plan.transformUp {
      case f @ Filter(cond, child) =>
        val parts = splitConjunctivePredicates(cond)
        val left = parts.filter {
          case b: BloomFilterMightContain => founded(b)
          case _ => true
        }
        if (left.size == parts.size) f
        else if (left.isEmpty) child
        else Filter(left.reduce(And), child)
    }
  }

  private def bloomIds(plan: LogicalPlan): Set[ExprId] =
    plan.flatMap(_.expressions.flatMap(_.collect {
      case BloomFilterMightContain(s: ScalarSubquery, _) => s.exprId
    })).toSet

  /** The creation side under the bloom filter's aggregate (`ColumnPruning` may have put a projection above it). */
  private def creationOf(p: LogicalPlan): LogicalPlan = p match {
    case a: Aggregate if a.groupingExpressions.isEmpty => a.child
    case Project(_, child) => creationOf(child)
    case other => other
  }

  /** `side` is one DSv2 scan under projections and filters, at most `threshold` by its planned files' bytes. */
  private def smallByFiles(side: LogicalPlan, threshold: Long): Boolean = {
    def scanOf(p: LogicalPlan): Option[DataSourceV2ScanRelation] = p match {
      case Project(_, child) => scanOf(child)
      case Filter(_, child) => scanOf(child)
      case s: DataSourceV2ScanRelation => Some(s)
      case _ => None
    }
    scanOf(side).flatMap(V2BroadcastBySize.byFiles(side, _)).exists(_ <= threshold)
  }

  /** The new filters pushed toward their scans, as Spark's batch after `InjectRuntimeFilter` does. */
  private def pushDown(plan: LogicalPlan): LogicalPlan = {
    var current = plan
    var i = 0
    var changed = true
    while (changed && i < 20) {
      val next = PushDownPredicates(current)
      changed = !next.fastEquals(current)
      current = next
      i += 1
    }
    current
  }
}
