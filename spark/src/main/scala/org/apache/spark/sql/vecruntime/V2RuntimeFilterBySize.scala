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
  SubqueryExpression,
  XxHash64
}
import org.apache.spark.sql.catalyst.optimizer.{InjectRuntimeFilter, PushDownPredicates}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
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
 * filters already present. The kept filters are pushed down toward their scans as Spark's later batch would. Off with
 * `spark.vecruntime.optimizer.v2RuntimeFilterBySize.enabled=false`, with Spark's bloom filter off, and with the
 * plugin.
 */
case class V2RuntimeFilterBySize(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (!VectorConf.v2RuntimeFilterBySizeEnabled(conf) || !conf.runtimeFilterBloomFilterEnabled) return plan
    if (!plan.exists(_.isInstanceOf[DataSourceV2ScanRelation])) return plan
    addSmall(dropCostly(plan, conf), conf)
  }

  /**
   * Drops the runtime bloom filters Spark's rule added whose creation side joins a DSv2 scan that is large by its
   * planned files (#735, q14a).
   *
   * Spark's rule judges the creation side by its estimate only. On Iceberg, 3 TB q14a's `item LEFT SEMI
   * cross_items` -- `item` known to hold 360,000 rows -- is within the threshold, and three filters on the sales
   * tables' item keys are built from it: each computes `cross_items` (three more scans of the sales tables) again,
   * 75 s against 60 s with Spark's bloom filter off. Its Parquet copy gets none. A creation side that is one scan
   * under projections and filters, or joins only small scans, is kept.
   */
  private def dropCostly(plan: LogicalPlan, conf: SQLConf): LogicalPlan = {
    val limit = VectorConf.v2BroadcastBySizeMaxEstimatedBytes(conf)
    def large(s: DataSourceV2ScanRelation): Boolean =
      V2BroadcastBySize.plannedBytes(s).forall(_ > limit)
    def readsLarge(p: LogicalPlan): Boolean = p.exists {
      case scan: DataSourceV2ScanRelation => large(scan)
      case _ => false
    } || p.exists(_.expressions.exists(_.exists {
      case s: SubqueryExpression => readsLarge(s.plan)
      case _ => false
    }))
    // The creation side joins another relation, or filters by an IN / EXISTS subquery that Spark's rule counted
    // as a selective filter and that a later batch turns into a semi join: either way building the filter reads
    // what that join or subquery reads.
    def costly(b: BloomFilterMightContain): Boolean = b.bloomFilterExpression match {
      case s: ScalarSubquery =>
        creationOf(s.plan).exists {
          case j: Join => readsLarge(j)
          case f: Filter => f.condition.exists {
              case q: SubqueryExpression => readsLarge(q.plan)
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
    if (
      !plan.exists(_.expressions.exists(_.exists {
        case b: BloomFilterMightContain => costly(b)
        case _ => false
      }))
    ) return plan
    plan.transformUp {
      case f @ Filter(cond, child) =>
        val parts = splitConjunctivePredicates(cond)
        val left = parts.filterNot {
          case b: BloomFilterMightContain => costly(b)
          case _ => false
        }
        if (left.size == parts.size) f
        else if (left.isEmpty) child
        else Filter(left.reduce(And), child)
    }
  }

  private def addSmall(plan: LogicalPlan, conf: SQLConf): LogicalPlan = {
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
      case s: DataSourceV2ScanRelation => V2PruningColumns.of(s.relation).toSeq
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
