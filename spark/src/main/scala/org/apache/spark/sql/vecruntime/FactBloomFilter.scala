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
  And,
  BloomFilterMightContain,
  DynamicPruningSubquery,
  Expression,
  Literal,
  Pmod,
  PredicateHelper,
  ScalarSubquery,
  SparkPartitionID,
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
    plan.collect { case f: Filter =>
      f.condition.collect { case b: BloomFilterMightContain => b; case p: PartitionedBloomMightContain => p }.size
    }.sum

  /** `app` with a bloom filter on `appKey` built from `creation`'s `creationKey`, if the shape qualifies. */
  private def inject(app: LogicalPlan, appKey: Expression, creation: LogicalPlan, creationKey: Expression)
      : Option[LogicalPlan] = {
    val conf = session.sessionState.conf
    val ratio = VectorConf.factBloomFilterSizeRatio(conf)
    if (!appKey.deterministic || !creationKey.deterministic || creationKey.references.isEmpty) return None
    if (app.stats.sizeInBytes < creation.stats.sizeInBytes * BigInt(ratio)) return None
    val items = expectedItems(app, appKey, creation, creationKey, VectorConf.factBloomFilterMaxSelectivity(conf))
    if (items.isEmpty) return None
    val threshold = conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
    filterFor(creation, creationKey, items.get).flatMap(mk => onScan(app, appKey, threshold, mk))
  }

  /**
   * Evidence that the filter is selective (#650), and the filter's expected item count; None declines.
   *
   * With both keys' distinct counts known (Spark `ANALYZE ... FOR COLUMNS` in the catalog, or a DSv2 source
   * that reports them -- Iceberg from Puffin theta sketches), at most `ndv(creation) / ndv(application)` of
   * the application rows can find a match (containment): fire when that is at most `maxSelectivity`, sized by
   * the creation key's distinct count, capped by the creation side's row estimate when it has one.
   *
   * Without distinct counts, fire only when the creation side is reduced below its base tables: a filter
   * other than `IS NOT NULL` somewhere under it. A whole, unfiltered table -- an entire dimension, whose keys
   * every fact row matches -- would cost the filter's build and prune nothing. Sized by the row estimate.
   */
  private def expectedItems(
      app: LogicalPlan,
      appKey: Expression,
      creation: LogicalPlan,
      creationKey: Expression,
      maxSelectivity: Double
  ): Option[Long] = {
    def clamp(n: BigInt): Long = n.max(1).min(BigInt(Long.MaxValue)).toLong
    val rowsEstimate = creation.stats.rowCount
    (KeyStats.distinctCount(creation, creationKey), KeyStats.distinctCount(app, appKey)) match {
      case (Some(cNdv), Some(aNdv)) if aNdv > 0 =>
        val effective = rowsEstimate.fold(cNdv)(_.min(cNdv))
        if (BigDecimal(effective) / BigDecimal(aNdv) <= BigDecimal(maxSelectivity)) Some(clamp(effective)) else None
      case _ =>
        if (!KeyStats.reduced(creation)) None
        else Some(clamp(rowsEstimate.getOrElse {
          val width = math.max(8L, creation.output.map(_.dataType.defaultSize.toLong).sum + 8L)
          creation.stats.sizeInBytes / width
        }))
    }
  }

  /**
   * The filter's layout for `items` expected keys, or None to decline (#653). Up to half of Spark's
   * `maxNumItems` it is a single `bloom_filter_agg`. Beyond that a single filter would be capped and saturated,
   * so it is partitioned: `B` sub-filters of at most `maxNumItems / 2` items each, 8 bits an item (fewer, down
   * to 4, to stay within `maxTotalBits`). A filter that would need more than `maxTotalBits` even at 4 bits an
   * item is declined: built saturated, it would only cost its build.
   */
  private def layout(items: Long): Option[(Int, Long, Long)] = {
    val conf = session.sessionState.conf
    val perBucket = math.max(1L, conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS) / 2)
    if (items <= perBucket) return Some((1, items, 0L))
    val total = VectorConf.factBloomFilterMaxTotalBits(conf)
    val bitsPerItem = math.min(8L, total / items)
    if (bitsPerItem < 4) return None
    val buckets = ((items + perBucket - 1) / perBucket).toInt
    val itemsPerBucket = (items + buckets - 1) / buckets
    val bitsPerBucket = math.min(itemsPerBucket * bitsPerItem, conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS))
    Some((buckets, itemsPerBucket, bitsPerBucket))
  }

  /**
   * A partitioned filter (#653): the creation keys' hashes, repartitioned by `pmod(h, B)` so each bucket's keys
   * meet in one task, one `bloom_filter_agg` per bucket after that shuffle (the aggregate's distribution is
   * already satisfied, so no per-map-task partial filters), packed into one value; the probe tests the bucket's
   * sub-filter.
   */
  private def partitionedFilter(creation: LogicalPlan, key: Expression, buckets: Int, items: Long, bits: Long)
      : Expression => Expression = { appExpr =>
    val h = Alias(new XxHash64(Seq(key)), "bloomHash")()
    val b = Alias(
      org.apache.spark.sql.catalyst.expressions.Cast(
        Pmod(h.toAttribute, Literal(buckets.toLong)),
        org.apache.spark.sql.types.IntegerType
      ),
      "bloomBucket"
    )()
    val hashed = Project(Seq(h), creation)
    val bucketed = Project(Seq(h.toAttribute, b), hashed)
    val shuffled =
      org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression(Seq(b.toAttribute), bucketed, buckets)
    val sub = Alias(
      new BloomFilterAggregate(h.toAttribute, Literal(items), Literal(bits)).toAggregateExpression(),
      "bloomPart"
    )()
    val perBucket = Aggregate(Seq(b.toAttribute), Seq(b.toAttribute, sub), shuffled)
    val packed = PartitionedBloomFilterAgg(b.toAttribute, sub.toAttribute, buckets).toAggregateExpression()
    val plan = Aggregate(Nil, Seq(Alias(packed, "bloomFilter")()), perBucket)
    PartitionedBloomMightContain(ScalarSubquery(ColumnPruning(plan), Nil), new XxHash64(Seq(appExpr)))
  }

  private def filterFor(creation: LogicalPlan, key: Expression, items: Long): Option[Expression => Expression] =
    layout(items).map {
      case (1, n, _) => singleFilter(creation, key, n)
      case (buckets, n, bits) => partitionedFilter(creation, key, buckets, n, bits)
    }

  private def singleFilter(creation: LogicalPlan, key: Expression, items: Long): Expression => Expression = { appExpr =>
    val agg = new BloomFilterAggregate(new XxHash64(Seq(key)), Literal(items))
    val buckets = VectorConf.factBloomFilterMergeBuckets(session.sessionState.conf)
    val plan =
      if (buckets <= 1) Aggregate(Nil, Seq(Alias(agg.toAggregateExpression(), "bloomFilter")()), creation)
      else {
        // Two levels (#646): one partial filter per creation-side task would all meet in a single task. Group
        // them by a bucket of the task's partition id, so `buckets` tasks each merge a share of them, then OR
        // the bucket filters in one small aggregate. Same bits and hash count at both levels: the same filter.
        val bucket = Alias(Pmod(SparkPartitionID(), Literal(buckets)), "bloomBucket")()
        val tagged = Project(creation.output :+ bucket, creation)
        val partial = Alias(agg.toAggregateExpression(), "bloomPartial")()
        val byBucket = Aggregate(Seq(bucket.toAttribute), Seq(partial), tagged)
        val merged = BloomFilterMerge(partial.toAttribute).toAggregateExpression()
        Aggregate(Nil, Seq(Alias(merged, "bloomFilter")()), byBucket)
      }
    // Column pruning as Spark's own rule does: the subquery reads only the creation key.
    val subquery = ScalarSubquery(ColumnPruning(plan), Nil)
    BloomFilterMightContain(subquery, new XxHash64(Seq(appExpr)))
  }

  /** Puts the filter right above the scan `key` traces to; `None` if it reaches none that qualifies. */
  private def onScan(p: LogicalPlan, key: Expression, threshold: Long, mk: Expression => Expression)
      : Option[LogicalPlan] = p match {
    case pr @ Project(list, child) =>
      onScan(child, replaceAlias(key, getAliasMap(pr)), threshold, mk).map(c => Project(list, c))
    case Filter(cond, child) =>
      if (hasFilterOn(cond, key)) None
      else onScan(child, key, threshold, mk).map {
        // The probe joins the filter right above the scan rather than going below it: Spark takes partition
        // filters (a dynamic pruning expression among them) only from the filter directly over the relation, so
        // a second filter in between would lose the scan's partition pruning and leave the pruning expression in
        // a filter evaluated row by row (q48, q13 and q61 at 1 TB).
        case Filter(probe, rel: LogicalRelation) if child.isInstanceOf[LogicalRelation] => Filter(And(cond, probe), rel)
        case c => Filter(cond, c)
      }
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
      case PartitionedBloomMightContain(_, XxHash64(Seq(v), _)) => v.semanticEquals(key)
      case _ => false
    }
}
