/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
  Alias,
  BloomFilterMightContain,
  DynamicPruningSubquery,
  Expression,
  Literal,
  PredicateHelper,
  ScalarSubquery,
  XxHash64
}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.{ColumnPruning, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * A runtime bloom filter from one large table onto a much larger one it joins (#641).
 *
 * Spark's `InjectRuntimeFilter` builds a bloom filter only from a creation side that is a selective filter
 * directly over a scan, in practice a small filtered dimension. EMR Serverless also builds one from a whole
 * fact table when it is much smaller than the table it joins: TPC-DS q93 filters `store_sales` (2.9G rows at
 * 1 TB) by `store_returns`' ticket numbers before the shuffle, so only sales that can have a return are
 * shuffled.
 *
 * Shape: a shuffle equi-join (neither side broadcastable) that can prune the application side (inner, left
 * semi, or the null-supplying side of an outer join), where:
 * - the application key traces, through projections, filters and inner or semi joins, to a file scan of at
 *   least `spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold`;
 * - the application side is at least `spark.vecruntime.optimizer.factBloomFilter.sizeRatio` times larger in
 *   estimated bytes than the creation side;
 * - that scan has no dynamic pruning or bloom filter on the key yet.
 *
 * The scan gets `might_contain(subquery bloom of the creation key, xxhash64(key))`, the same expressions
 * Spark's rule injects, so the probe and the aggregate run vectorised. The filter's expected items come from
 * the creation side's row estimate (or its bytes over its row width); Spark caps them with
 * `runtime.bloomFilter.maxNumItems` and `maxNumBits`. It is exact: a bloom filter has no false negatives,
 * and a row whose key the creation side does not have cannot join.
 *
 * Runs in the session's last optimizer batch (after DPP and Spark's runtime filters are placed, so it can
 * see them), registered by [[RegisterLateOptimizerRules]]. A scan that already has the filter is skipped, so
 * the batch's next pass changes nothing. At most
 * `spark.sql.optimizer.runtimeFilter.number.threshold` filters per query. Off with
 * `spark.vecruntime.optimizer.factBloomFilter.enabled=false`, with the plugin, and with
 * `spark.sql.optimizer.runtime.bloomFilter.enabled=false`.
 */
case class FactBloomFilter(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper
    with JoinSelectionHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (!VectorConf.factBloomFilterEnabled(conf) || !conf.runtimeFilterBloomFilterEnabled) return plan
    var budget = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD) - countBlooms(plan)
    plan.transformUp {
      case j @ ExtractEquiJoinKeys(jt, leftKeys, rightKeys, _, _, left, right, hint)
          if budget > 0 && !canBroadcastBySize(left, conf) && !canBroadcastBySize(right, conf) &&
            !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) =>
        var newLeft = left
        var newRight = right
        leftKeys.zip(rightKeys).foreach { case (l, r) =>
          if (budget > 0 && prunesLeft(jt)) {
            inject(newLeft, l, right, r).foreach { p => newLeft = p; budget -= 1 }
          }
          if (budget > 0 && prunesRight(jt)) {
            inject(newRight, r, left, l).foreach { p => newRight = p; budget -= 1 }
          }
        }
        if ((newLeft eq left) && (newRight eq right)) j else j.withNewChildren(Seq(newLeft, newRight))
    }
  }

  private def prunesLeft(jt: JoinType): Boolean = jt == Inner || jt == LeftSemi || jt == RightOuter
  private def prunesRight(jt: JoinType): Boolean = jt == Inner || jt == LeftOuter

  private def countBlooms(plan: LogicalPlan): Int =
    plan.collect { case f: Filter => f.condition.collect { case b: BloomFilterMightContain => b }.size }.sum

  /** `app` with a bloom filter on `appKey` built from `creation`'s `creationKey`, if the shape qualifies. */
  private def inject(app: LogicalPlan, appKey: Expression, creation: LogicalPlan, creationKey: Expression)
      : Option[LogicalPlan] = {
    val conf = session.sessionState.conf
    val ratio = VectorConf.factBloomFilterSizeRatio(conf)
    if (!appKey.deterministic || !creationKey.deterministic || creationKey.references.isEmpty) return None
    if (app.stats.sizeInBytes < creation.stats.sizeInBytes * BigInt(ratio)) return None
    val threshold = conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
    onScan(app, appKey, threshold, filterFor(creation, creationKey))
  }

  private def filterFor(creation: LogicalPlan, key: Expression): Expression => Expression = { appExpr =>
    val rows = creation.stats.rowCount.getOrElse {
      val width = math.max(8L, creation.output.map(_.dataType.defaultSize.toLong).sum + 8L)
      creation.stats.sizeInBytes / width
    }
    val items = rows.max(1).min(BigInt(Long.MaxValue)).toLong
    val agg = new BloomFilterAggregate(new XxHash64(Seq(key)), Literal(items))
    val alias = Alias(agg.toAggregateExpression(), "bloomFilter")()
    // Column pruning as Spark's own rule does: the subquery reads only the creation key.
    val subquery = ScalarSubquery(ColumnPruning(Aggregate(Nil, Seq(alias), creation)), Nil)
    BloomFilterMightContain(subquery, new XxHash64(Seq(appExpr)))
  }

  /** Puts the filter right above the scan `key` traces to; `None` if it reaches none that qualifies. */
  private def onScan(p: LogicalPlan, key: Expression, threshold: Long, mk: Expression => Expression)
      : Option[LogicalPlan] = p match {
    case pr @ Project(list, child) =>
      onScan(child, replaceAlias(key, getAliasMap(pr)), threshold, mk).map(c => Project(list, c))
    case Filter(cond, child) =>
      if (hasFilterOn(cond, key)) None else onScan(child, key, threshold, mk).map(c => Filter(cond, c))
    case jn @ Join(l, r, jt, _, _) if jt == Inner || jt == LeftSemi =>
      if (key.references.subsetOf(l.outputSet)) onScan(l, key, threshold, mk).map(c => jn.copy(left = c))
      else if (jt == Inner && key.references.subsetOf(r.outputSet))
        onScan(r, key, threshold, mk).map(c => jn.copy(right = c))
      else None
    case rel: LogicalRelation if key.references.subsetOf(rel.outputSet) && rel.stats.sizeInBytes >= BigInt(threshold) =>
      Some(Filter(mk(key), rel))
    case _ => None
  }

  private def hasFilterOn(cond: Expression, key: Expression): Boolean =
    splitConjunctivePredicates(cond).exists {
      case d: DynamicPruningSubquery => d.pruningKey.semanticEquals(key)
      case BloomFilterMightContain(_, XxHash64(Seq(v), _)) => v.semanticEquals(key)
      case _ => false
    }
}
