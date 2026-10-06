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
package io.vecruntime.spark

import org.apache.spark.sql.internal.SQLConf

/** Configuration keys for the spark-vector plugin. All keys are read from the session's SQLConf. */
object VectorConf {

  val Enabled = "spark.vecruntime.enabled"
  val FilterEnabled = "spark.vecruntime.exec.filter.enabled"
  val MergeRowsEnabled = "spark.vecruntime.exec.mergeRows.enabled"
  val ProjectEnabled = "spark.vecruntime.exec.project.enabled"
  val AggregateEnabled = "spark.vecruntime.exec.aggregate.enabled"
  val FinalAggregateEnabled = "spark.vecruntime.exec.aggregate.final.enabled"
  val ObjectAggregateEnabled = "spark.vecruntime.exec.objectAggregate.enabled"
  val SelectionEnabled = "spark.vecruntime.exec.selection.enabled"
  val CometShuffleEnabled = "spark.vecruntime.comet.shuffle.enabled"

  /** Our own columnar shuffle exchange (#288; needs the shuffle module and its shuffle manager). */
  val ShuffleEnabled = "spark.vecruntime.shuffle.enabled"

  /**
   * The columnar Iceberg v3 deletion-vector writer (#20, option B): on a merge-on-read table whose
   * delete file format is PUFFIN, the DELETE half of DELETE/UPDATE/MERGE is written from our Arrow
   * batches' `_file`/`_pos` lanes -- one `PositionDeleteIndex` per data file, one
   * `BaseDVFileWriter.delete(path, index, spec, partition)` call per file -- instead of Spark's
   * row-by-row `WriteDeltaExec`. Inserts and the insert half of updates stay on Iceberg's data
   * writer, and the commit stays Iceberg's own `RowDelta`. v2 tables and anything unsupported decline
   * to Spark's writer unchanged, with a printed fallback reason. Default OFF while the writer is being
   * landed slice by slice (the operator is not yet wired); it will default on once every Iceberg
   * merge-on-read suite is byte-identical with it on and off. See `docs/iceberg.md`.
   */
  val IcebergDvWriterEnabled = "spark.vecruntime.iceberg.dvWriter.enabled"

  /**
   * Logical rewrite (#632): `IsNotNull` on a grouping key that is the only difference between the
   * inputs of two aggregates (a CTE used twice) moves above the aggregates, so the input is shared.
   */
  val SharedAggregateInputsEnabled = "spark.vecruntime.optimizer.sharedAggregateInputs.enabled"

  /**
   * Logical rewrite (#633): dynamic partition pruning of a fact scan below an aggregate whose grouping
   * key is joined above it to a filtered relation (through the dimension that maps the key to the
   * fact's partition column).
   */
  val DppThroughAggregateEnabled = "spark.vecruntime.optimizer.dppThroughAggregate.enabled"

  /**
   * Logical rewrite (#634): dynamic partition pruning of a scan through a second join key, when the joined
   * dimension is unfiltered but another key of the same join comes from a filtered relation.
   */
  val TransitiveDppEnabled = "spark.vecruntime.optimizer.transitiveDpp.enabled"

  /**
   * Logical rewrite (#641): a runtime bloom filter on a shuffle join's much larger side, built from the other
   * side (a fact from a smaller fact), where Spark's own runtime filter needs a selective filtered scan.
   */
  val FactBloomFilterEnabled = "spark.vecruntime.optimizer.factBloomFilter.enabled"

  /** How many times larger (estimated bytes) the filtered side must be than the side the filter is built from. */
  val FactBloomFilterSizeRatio = "spark.vecruntime.optimizer.factBloomFilter.sizeRatio"

  /**
   * Fan-in of the filter's two-level merge (#646): the creation side's per-task partial filters are merged
   * in this many groups in parallel, then the groups' filters in one task. 1 restores a single-level merge.
   */
  val FactBloomFilterMergeBuckets = "spark.vecruntime.optimizer.factBloomFilter.mergeBuckets"

  /** The grouped aggregate emits its UTF8 keys dictionary-encoded, ids over the group table's own dictionary (#377). */
  val AggDictionaryKeys = "spark.vecruntime.agg.dictionaryKeys"
  val SortEnabled = "spark.vecruntime.exec.sort.enabled"
  val SortRunRows = "spark.vecruntime.sort.runRows"
  val SortSpillBytes = "spark.vecruntime.sort.spillBytes"
  val TakeOrderedEnabled = "spark.vecruntime.exec.takeOrdered.enabled"
  val LimitEnabled = "spark.vecruntime.exec.limit.enabled"
  val UnionEnabled = "spark.vecruntime.exec.union.enabled"
  val CoalesceEnabled = "spark.vecruntime.exec.coalesce.enabled"
  val ExpandEnabled = "spark.vecruntime.exec.expand.enabled"
  val RollupRewriteEnabled = "spark.vecruntime.exec.aggregate.rollupRewrite.enabled"

  /**
   * Logical rewrite: an existence-only self-join `t1 JOIN t2 ON t1.k = t2.k AND t1.v <> t2.v`
   * (TPC-DS q95's `ws_wh`) becomes `GROUP BY k HAVING min(v) <> max(v)` -- no many-to-many join.
   */
  val SelfJoinToAggregateEnabled = "spark.vecruntime.optimizer.selfJoinToAggregate.enabled"

  /**
   * Logical rewrite: global aggregates over the same input with different filters (TPC-DS q9's
   * scalar subqueries, q28 / q88 / q90's cross-joined aggregates) are computed in one pass.
   */
  val MergeFilteredAggregatesEnabled = "spark.vecruntime.optimizer.mergeFilteredAggregates.enabled"
  val SampleEnabled = "spark.vecruntime.exec.sample.enabled"
  val GenerateEnabled = "spark.vecruntime.exec.generate.enabled"
  val WindowEnabled = "spark.vecruntime.exec.window.enabled"
  val LocalTableScanEnabled = "spark.vecruntime.exec.localTableScan.enabled"
  val RangeEnabled = "spark.vecruntime.exec.range.enabled"
  val BroadcastExchangeEnabled = "spark.vecruntime.exec.broadcastExchange.enabled"
  val BroadcastHashJoinEnabled = "spark.vecruntime.exec.broadcastHashJoin.enabled"
  val BroadcastNestedLoopJoinEnabled = "spark.vecruntime.exec.broadcastNestedLoopJoin.enabled"
  val ShuffledHashJoinEnabled = "spark.vecruntime.exec.shuffledHashJoin.enabled"
  val SortMergeJoinEnabled = "spark.vecruntime.exec.sortMergeJoin.enabled"
  val SortMergeJoinMode = "spark.vecruntime.exec.sortMergeJoin.mode"
  val JoinMaxBuildSize = "spark.vecruntime.join.maxBuildSize"
  val JoinHashMaxBuildSize = "spark.vecruntime.join.hashMaxBuildSize"
  val JoinSpillBuckets = "spark.vecruntime.join.spillBuckets"
  val JoinSpillBytes = "spark.vecruntime.join.spillBytes"
  val JoinDenseKeys = "spark.vecruntime.join.denseKeys"

  /** #603: a hash join emits its probe-side lane columns as views over the input, gathered on first read. */
  val JoinDeferredProbe = "spark.vecruntime.join.deferredProbe"

  /** #603 step 2: a hash join emits its build-side lane columns as views over the build table. */
  val JoinDeferredBuild = "spark.vecruntime.join.deferredBuild"

  /** #603: build-side strings with at most this many distinct values are emitted as dictionary ids. */
  val JoinBuildDictionaryMax = "spark.vecruntime.join.buildDictionaryMax"

  /** #610: broadcast build keys as runtime filters on the native scan. */
  val JoinRuntimeFilters = "spark.vecruntime.join.runtimeFilters"
  val JoinRuntimeFiltersInMax = "spark.vecruntime.join.runtimeFilters.inMax"
  val JoinRuntimeFiltersMaxBuildRows = "spark.vecruntime.join.runtimeFilters.maxBuildRows"
  val JoinBuildPayload = "spark.vecruntime.join.buildPayload"
  val CometRangeShuffleEnabled = "spark.vecruntime.comet.shuffle.range.enabled"

  /** The scan-side prefetching converter (#403, lever 2): the queue depth, `0` off. */
  val ScanPrefetch = "spark.vecruntime.scan.prefetch"

  /** Our own Java Parquet page decoder behind a VectorParquetScanExec (#559), on by default (see scanNativeParquet). */
  val ScanNativeParquet = "spark.vecruntime.scan.nativeParquet.enabled"

  /** VectorParquetScanExec read-ahead (#559/#566): files of a split opened ahead, `0` off. */
  val ScanNativeParquetPrefetchFiles = "spark.vecruntime.scan.nativeParquet.prefetchFiles"

  /** VectorParquetScanExec read-ahead (#559/#566): row groups of the current file read ahead, `0` off. */
  val ScanNativeParquetPrefetchRowGroups = "spark.vecruntime.scan.nativeParquet.prefetchRowGroups"

  /** VectorParquetScanExec decode-ahead (#606): batches decoded ahead on a producer thread, `0` off. */
  val ScanNativeParquetDecodeAhead = "spark.vecruntime.scan.nativeParquet.decodeAhead"

  /** The decode-ahead producer's thread kind (#606): `virtual` or `platform`. */
  val ScanNativeParquetDecodeAheadThreads = "spark.vecruntime.scan.nativeParquet.decodeAhead.threads"

  /** VectorParquetScanExec (#612): dictionary-encoded string columns as dictionary vectors, default on. */
  val ScanNativeParquetDictionaryStrings = "spark.vecruntime.scan.nativeParquet.dictionaryStrings"

  /** #611: late materialization in the native scan. */
  val ScanNativeParquetLateMaterialization = "spark.vecruntime.scan.nativeParquet.lateMaterialization"
  val ScanNativeParquetLateMaterializationMinBytes =
    "spark.vecruntime.scan.nativeParquet.lateMaterialization.minScanBytes"

  /** Mixed chains (#280): Comet's native operators above ours through the sink leaf. Off until #281 decides an allowlist. */
  val CometMixedEnabled = "spark.vecruntime.comet.mixed.enabled"

  /** The operator allowlist of the mixed-chain pass (#281): kinds, optionally qualified, offered to Comet. */
  val CometPreferComet = "spark.vecruntime.comet.preferComet"
  val StrictFloatingPoint = "spark.vecruntime.exec.strictFloatingPoint"
  val ExplainFallbackEnabled = "spark.vecruntime.explainFallback.enabled"
  val UiEnabled = "spark.vecruntime.ui.enabled"
  val UiRetainedExecutions = "spark.vecruntime.ui.retainedExecutions"

  def isEnabled(conf: SQLConf): Boolean = bool(conf, Enabled, default = true)
  def filterEnabled(conf: SQLConf): Boolean = bool(conf, FilterEnabled, default = true)
  def mergeRowsEnabled(conf: SQLConf): Boolean = bool(conf, MergeRowsEnabled, default = true)
  def projectEnabled(conf: SQLConf): Boolean = bool(conf, ProjectEnabled, default = true)
  def aggregateEnabled(conf: SQLConf): Boolean = bool(conf, AggregateEnabled, default = true)

  /** Convert Final-mode aggregates too (their input is a shuffle, converted to columnar by Spark). */
  def finalAggregateEnabled(conf: SQLConf): Boolean = bool(conf, FinalAggregateEnabled, default = true)

  /**
   * Convert `ObjectHashAggregateExec` for the object aggregates whose buffer we can carry (#57):
   * `bloom_filter_agg` (the runtime filter's build side), `collect_list`, `collect_set`. Driven
   * through Spark's own function object per group so the partial buffer and result are byte-identical;
   * the other object aggregates (`percentile*`, `collect_top_k`, ...) keep the fallback reason. Needs
   * `aggregate.enabled` (and, for the merge modes, `aggregate.final.enabled`).
   */
  def objectAggregateEnabled(conf: SQLConf): Boolean = bool(conf, ObjectAggregateEnabled, default = true)

  /** Feed Comet's native shuffle from spark-vector operators when Comet's shuffle is configured. */
  def cometShuffleEnabled(conf: SQLConf): Boolean = bool(conf, CometShuffleEnabled, default = true)
  def shuffleEnabled(conf: SQLConf): Boolean = bool(conf, ShuffleEnabled, default = true)

  /**
   * The columnar Iceberg v3 DV writer (#20). Default `false`: the operator is being landed in slices
   * and is not wired to the planner yet, so turning it on is currently a no-op reserved for the
   * writer's own tests. Flips to `true` once the writer is byte-identical to Spark's on every
   * merge-on-read suite.
   */
  def icebergDvWriterEnabled(conf: SQLConf): Boolean = bool(conf, IcebergDvWriterEnabled, default = false)
  def aggDictionaryKeys(conf: SQLConf): Boolean = bool(conf, AggDictionaryKeys, default = true)

  /** Pass selection bitmaps between spark-vector operators instead of compacting each batch. */
  def selectionEnabled(conf: SQLConf): Boolean = bool(conf, SelectionEnabled, default = true)

  /** Convert SortExec over a columnar child (in-memory, no spill). */
  def sortEnabled(conf: SQLConf): Boolean = bool(conf, SortEnabled, default = true)

  /**
   * Rows per sorted run: the sort orders each run of this many rows as the partition arrives and
   * merges the runs on output (one run is one sort over the whole partition, #285). Bounds the
   * sort's JVM scratch to the run rather than the partition.
   */
  /**
   * The sort's memory budget per task (bytes, size strings accepted; #416): past it, sealed runs are
   * written to local disk in sorted order and merged from there, so a partition of any size sorts in
   * bounded memory. `0` or negative turns spilling off. The default is the join's build budget (1 GiB,
   * or the off-heap share per core): #451 set 32 MB from a sweep that had gigabyte runs 25-45% slower
   * on the merge-join queries, but that sweep shared the cluster's S3 path with a full run on the other
   * node group; measured alone, back to back, in the full run's own shape (the merge joins after the
   * heavy scans), 32 MB cost q14a 30% and q4 50% against the 1 GiB budget (121.8 vs 93.7 s, 106.9 vs
   * 71.2) and 256 MB recovered q14a but not q4 (97.3 / 107.1) -- the spilled runs share the node's
   * 20 GB disk with the shuffle files of the queries before them, where the 1 GiB budget never touches
   * it. The hash join's memory is bounded on its own since #459/#461, so the sorts can have the budget.
   */
  val DefaultSortSpillBytes: Long = 1L << 30
  def sortSpillBytes(conf: SQLConf, sparkConf: org.apache.spark.SparkConf): Long = {
    val explicit = conf.getConfString(SortSpillBytes, "").trim
    val v = if (explicit.nonEmpty) org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)
    else DefaultSortSpillBytes
    if (v <= 0) Long.MaxValue else v
  }
  def sortRunRows(conf: SQLConf): Int =
    scala.util.Try(conf.getConfString(SortRunRows, "").trim.toInt).toOption.filter(_ > 0).getOrElse(1 << 20)

  /** Convert TakeOrderedAndProjectExec (ORDER BY ... LIMIT) over a columnar child. */
  def takeOrderedEnabled(conf: SQLConf): Boolean = bool(conf, TakeOrderedEnabled, default = true)

  /** Convert LocalLimitExec / GlobalLimitExec / CollectLimitExec over a columnar child. */
  def limitEnabled(conf: SQLConf): Boolean = bool(conf, LimitEnabled, default = true)

  /** Convert UnionExec when at least one child is columnar (row children go through RowToColumnarExec). */
  def unionEnabled(conf: SQLConf): Boolean = bool(conf, UnionEnabled, default = true)

  /** Convert CoalesceExec over a columnar child. */
  def coalesceEnabled(conf: SQLConf): Boolean = bool(conf, CoalesceEnabled, default = true)

  /** Convert ExpandExec (grouping sets, the distinct rewrite) over a columnar child. */
  def expandEnabled(conf: SQLConf): Boolean = bool(conf, ExpandEnabled, default = true)
  def rollupRewriteEnabled(conf: SQLConf): Boolean = bool(conf, RollupRewriteEnabled, default = true)

  /** The existence-only self-join to min/max aggregate rewrite; also off when the plugin is off. */
  def selfJoinToAggregateEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, SelfJoinToAggregateEnabled, default = true)

  /** Merging global aggregates that differ only in their filters; also off when the plugin is off. */
  def mergeFilteredAggregatesEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, MergeFilteredAggregatesEnabled, default = true)

  /** Convert SampleExec without replacement over a columnar child (Spark's own Bernoulli sequence per partition). */
  def sampleEnabled(conf: SQLConf): Boolean = bool(conf, SampleEnabled, default = true)

  /** Convert GenerateExec with explode/posexplode (and the outer forms) over an array column. */
  def generateEnabled(conf: SQLConf): Boolean = bool(conf, GenerateEnabled, default = true)

  /** Convert WindowExec for the ranking functions (row_number, rank, dense_rank); the child may be a row sort. */
  def windowEnabled(conf: SQLConf): Boolean = bool(conf, WindowEnabled, default = true)

  /** Convert LocalTableScanExec (`VALUES`, small local relations) into one batch per partition; off by default. */
  def localTableScanEnabled(conf: SQLConf): Boolean = bool(conf, LocalTableScanEnabled, default = false)

  /** Convert RangeExec (`spark.range`, the `range()` table function) into native INT64 batches, so the chain above is ours from the leaf. */
  def rangeEnabled(conf: SQLConf): Boolean = bool(conf, RangeEnabled, default = true)

  /**
   * Replace a hash-join broadcast exchange over one of our plans by our columnar one (#325): the
   * build side travels as Arrow batches and our broadcast join builds its table from them. A Spark
   * consumer of the same exchange still gets Spark's relation, built from the batches on first use.
   */
  def broadcastExchangeEnabled(conf: SQLConf): Boolean = bool(conf, BroadcastExchangeEnabled, default = true)

  /** Convert BroadcastHashJoinExec over a columnar streamed side (the build side stays Spark's broadcast). */
  def broadcastHashJoinEnabled(conf: SQLConf): Boolean = bool(conf, BroadcastHashJoinEnabled, default = true)

  /**
   * A hash join on one INT32/INT64 key whose build values span a small range (at most 10x the
   * distinct keys, as Spark's own dense `LongHashedRelation`) is probed through an array indexed by
   * the key instead of the hash table (#546): a range check and one load per row.
   */
  def joinDenseKeys(conf: SQLConf): Boolean = bool(conf, JoinDenseKeys, default = true)

  /**
   * #603: a hash join emits its probe-side lane columns as views over its input batch (row ids, not copies),
   * as Velox wraps a hash probe's output in a dictionary. A chain of joins composes the ids, and a column is
   * gathered once, by the first operator that reads it, or never.
   */
  def joinDeferredProbe(conf: SQLConf): Boolean = bool(conf, JoinDeferredProbe, default = true)

  /**
   * #603 step 2: a hash join emits its build-side lane columns as views over its build table (the build row
   * ids, not copies), so a dimension string carried through a chain of joins is gathered once, where it is read.
   */
  def joinDeferredBuild(conf: SQLConf): Boolean = bool(conf, JoinDeferredBuild, default = true)

  /**
   * #603: a hash join emits a build-side string column with at most this many distinct values as ids over
   * the distinct values (a dictionary vector) rather than the bytes, so an aggregate grouping on it maps the
   * values once per batch instead of the rows, and the shuffle writer stages ids. Default 4096, the owner's
   * call on the 1 TB A/B (fourteen join queries -2.7%, q99 -9%; q18 +5%, q43 +4%); 0 keeps bytes.
   * Needs `deferredBuild`.
   */
  def joinBuildDictionaryMax(conf: SQLConf): Int = intIn(conf, JoinBuildDictionaryMax, default = 4096, max = 1 << 16)

  /**
   * #610: a broadcast hash join (inner, or left semi with the build on the right) hands the native Parquet
   * scan its streamed keys reach, through filters, projections and other inner joins, the build side's key
   * domain as a pushed filter: an IN list of at most `runtimeFilters.inMax` values, else an integer key's
   * range. Row groups without a matching key are skipped. On by default: TPC-DS 1 TB (fact keys not
   * clustered, so nothing is skipped) is neutral on its twelve longest queries, and the showcase dataset (a
   * fact table clustered on its join key) is 2.8x faster on a selective dimension.
   */
  def joinRuntimeFilters(conf: SQLConf): Boolean = bool(conf, JoinRuntimeFilters, default = true)
  def joinRuntimeFiltersInMax(conf: SQLConf): Int = intIn(conf, JoinRuntimeFiltersInMax, default = 1024, max = 1 << 16)

  /**
   * #610: the largest broadcast build side (rows) a runtime filter is derived from. Reading the key domain
   * means deserializing the whole broadcast on the driver before the streamed side starts; a large build side
   * (TPC-DS item, 300k rows at 1 TB) cost a short query 0.1-0.25 s there, and its keys rarely prune anyway.
   */
  def joinRuntimeFiltersMaxBuildRows(conf: SQLConf): Int =
    intIn(conf, JoinRuntimeFiltersMaxBuildRows, default = 65536, max = Int.MaxValue)

  /**
   * A broadcast hash join whose build side carries payload columns without a lane (arrays, maps,
   * structs) keeps them in a row store next to the lane columns (#547) instead of staying Spark's.
   * Keys and the condition must still read lane columns only.
   */
  def joinBuildPayload(conf: SQLConf): Boolean = bool(conf, JoinBuildPayload, default = true)

  /** Convert BroadcastNestedLoopJoinExec (non-equi joins) when the streamed side is columnar. */
  def broadcastNestedLoopJoinEnabled(conf: SQLConf): Boolean =
    bool(conf, BroadcastNestedLoopJoinEnabled, default = true)

  /** Convert ShuffledHashJoinExec; over Spark's row shuffle both inputs go through RowToColumnarExec. */
  def shuffledHashJoinEnabled(conf: SQLConf): Boolean = bool(conf, ShuffledHashJoinEnabled, default = true)

  /**
   * Re-express SortMergeJoinExec as our shuffled hash join when the smaller side's statistics fit
   * `spark.vecruntime.join.maxBuildSize` (#10). Opt-in: it trades Spark's streaming merge for a per-task
   * hash table, a different memory profile.
   */
  def sortMergeJoinEnabled(conf: SQLConf): Boolean = sortMergeJoinMode(conf) == "hash"

  /**
   * What becomes of SortMergeJoinExec (#286, #287): `off` leaves it to Spark; `hash` re-expresses it as
   * our shuffled hash join under the statistics rule above; `merge` plans our order-preserving merge
   * join over the sorted inputs; `auto` decides per join -- the merge join where a parent relies on the
   * ordering, where the row order can reach a limit or a sort without an exchange in between, or where
   * the hash rewrite is not allowed (no statistics, both sides large, a skew join), the hash rewrite
   * where a side's statistics fit the budget. Nothing is left to Spark's own sort-merge join since
   * #416: the sort below ours spills past its budget, so input size no longer decides. The boolean
   * flag is an alias: `true` reads as `auto`.
   */
  def sortMergeJoinMode(conf: SQLConf): String = {
    val explicit = conf.getConfString(SortMergeJoinMode, "").trim.toLowerCase
    if (explicit.nonEmpty) explicit
    else conf.getConfString(SortMergeJoinEnabled, "").trim.toLowerCase match {
      case "false" => "off" // the boolean flag still switches the rewrite off
      case _ => "auto" // the default since #311: the hash rewrite where a side fits, our merge join otherwise
    }
  }

  /**
   * Largest build side (bytes, size strings like `512m` accepted) a hash-style join converts for (#86);
   * the joins hold the build side in memory per task. Default: a per-core share of the off-heap
   * budget (`spark.memory.offHeap.size / spark.executor.cores`) when off-heap is configured, else 1 GiB.
   */
  /**
   * The buckets a shuffled hash join splits into when its build side outgrows [[joinMaxBuildSize]]
   * (#416, the grace hash join): both sides are hashed into this many local files and joined one
   * bucket at a time. `1` or less turns the split off (the build side is always held in memory).
   */
  /**
   * The build bytes a shuffled hash join holds in memory before it splits (#416): 256 MB per task.
   * Measured at 1 TB on the eight join-heaviest TPC-DS queries, back to back and alone on the cluster,
   * 256 MB and 1 GiB are the same within the band (748 against 758 s over the eight) and route every
   * join identically -- with the planner in front (`hashMaxBuildSize`), the sides the 1 GiB budget would
   * have held in memory go to the merge join under 256 MB at no cost, and neither budget spilled a byte.
   * 256 MB is then the one that leaves three quarters of the earlier headroom to the sort and the
   * operators beside the join; the broadcast budget [[joinMaxBuildSize]] is a different question (a
   * relation held once per task for the whole query) and keeps its own default. `0` or negative:
   * never split.
   */
  val DefaultJoinSpillBytes: Long = 256L << 20
  def joinSpillBytes(conf: SQLConf, sparkConf: org.apache.spark.SparkConf): Long = {
    val explicit = conf.getConfString(JoinSpillBytes, "").trim
    val v = if (explicit.nonEmpty) org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)
    else DefaultJoinSpillBytes
    if (v <= 0) Long.MaxValue else v
  }
  def joinSpillBuckets(conf: SQLConf): Int =
    scala.util.Try(conf.getConfString(JoinSpillBuckets, "").trim.toInt).toOption.getOrElse(32)

  /**
   * The most a sort-merge join's build side may weigh per task, by statistics, for `mode=auto` to
   * re-express it as the hash join: within `spark.vecruntime.join.spillBytes` it builds in memory, within
   * this cap it splits into buckets on disk once (`spillBuckets` buckets of at most `spillBytes`
   * each), and past it -- or without an estimate -- the merge join over the spilling sort takes it
   * (#416). Default `spillBuckets * spillBytes`; `0` or a negative value removes the cap.
   */
  def joinHashMaxBuildBytes(conf: SQLConf, sparkConf: org.apache.spark.SparkConf): Long = {
    val explicit = conf.getConfString(JoinHashMaxBuildSize, "").trim
    val v =
      if (explicit.nonEmpty) org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)
      else {
        val perBucket = joinSpillBytes(conf, sparkConf)
        if (perBucket == Long.MaxValue) Long.MaxValue else perBucket * joinSpillBuckets(conf)
      }
    if (v <= 0) Long.MaxValue else v
  }
  def joinMaxBuildSize(conf: SQLConf, sparkConf: org.apache.spark.SparkConf): Long = {
    val explicit = conf.getConfString(JoinMaxBuildSize, "")
    if (explicit.nonEmpty) org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)
    else {
      val offHeap = if (sparkConf.getBoolean("spark.memory.offHeap.enabled", false))
        sparkConf.getSizeAsBytes("spark.memory.offHeap.size", "0")
      else 0L
      if (offHeap > 0) math.max(1L, offHeap / math.max(1, sparkConf.getInt("spark.executor.cores", 1))) else 1L << 30
    }
  }

  /** Also hand range-partitioned exchanges (global sorts) to Comet's native shuffle. */
  def cometRangeShuffleEnabled(conf: SQLConf): Boolean = bool(conf, CometRangeShuffleEnabled, default = true)

  /**
   * Depth of the prefetching converter's queue under the first operator of ours above a Spark file
   * scan (#403, lever 2): `0` (the default) leaves the scan's batches to be converted lazily on the
   * task thread; `1` or `2` inserts [[org.apache.spark.sql.vecruntime.VectorPrefetchScanExec]], whose
   * helper thread pulls the reader's next batch and converts every column into our Arrow vectors
   * while the task thread works on the previous one. Memory grows by that many converted batches
   * per task. Larger values are accepted and capped at 8.
   */
  def scanPrefetchDepth(conf: SQLConf): Int =
    scala.util.Try(conf.getConfString(ScanPrefetch, "0").trim.toInt).toOption.filter(_ > 0).map(math.min(
      _,
      8
    )).getOrElse(0)
  def cometMixedEnabled(conf: SQLConf): Boolean = bool(conf, CometMixedEnabled, default = false)

  /**
   * Plan our own VectorParquetScanExec in place of a supported Parquet FileSourceScanExec (#559). On by default
   * since 0.0.6, except in a session where Comet's scan is active (Comet's plugin or extension registered, with
   * `spark.comet.enabled` and `spark.comet.scan.enabled` not false): there Comet's reader stays the scan unless
   * this key is set explicitly.
   */
  def scanNativeParquet(conf: SQLConf): Boolean = bool(conf, ScanNativeParquet, default = !cometScanActive(conf))

  /** Whether Comet's scan is active in this session: its plugin or extension registered and its scan not off. */
  def cometScanActive(conf: SQLConf): Boolean = {
    val registered = Seq("spark.plugins", "spark.sql.extensions").exists(k =>
      conf.getConfString(k, "").toLowerCase(java.util.Locale.ROOT).contains("comet")
    )
    registered && bool(conf, "spark.comet.enabled", default = true) &&
    bool(conf, "spark.comet.scan.enabled", default = true)
  }

  /**
   * How many files of a split `VectorParquetScanExec` opens ahead (#559/#566): status, footer and first row
   * group, on background threads while the task thread decodes. Pays off on splits made of many small files
   * (1 TB TPC-DS store_sales is ~14.6k files of ~7 MB). `0` opens each file on the task thread. Memory per
   * task grows by up to N opened files and their first row groups (compressed pages). Capped at 16.
   */
  def scanNativeParquetPrefetchFiles(conf: SQLConf): Int = intIn(conf, ScanNativeParquetPrefetchFiles, 6, 16)

  /**
   * How many row groups of the current file `VectorParquetScanExec` reads ahead (#559/#566), on platform
   * threads, chained so a file's reader is used by one thread at a time and in order. Pays off on files with
   * several row groups. `0` reads each row group on the task thread. Memory per task grows by up to N row
   * groups (compressed pages), so it scales with the row-group size. Capped at 16.
   */
  def scanNativeParquetPrefetchRowGroups(conf: SQLConf): Int =
    intIn(conf, ScanNativeParquetPrefetchRowGroups, 2, 16)

  /**
   * Decode-ahead (#606): how many batches `VectorParquetScanExec` decodes ahead of the task thread, on a
   * producer thread of each task's own, so decoding overlaps the operators above the scan. `0` (the default
   * until measured) decodes on the task thread. Memory per task grows by up to N batches. Capped at 16.
   */
  def scanNativeParquetDecodeAhead(conf: SQLConf): Int = intIn(conf, ScanNativeParquetDecodeAhead, 0, 16)

  /** The decode-ahead producer's thread kind: `virtual` (the default) or `platform`. */
  def scanNativeParquetDecodeAheadVirtual(conf: SQLConf): Boolean =
    !conf.getConfString(ScanNativeParquetDecodeAheadThreads, "virtual").trim.equalsIgnoreCase("platform")

  /**
   * #612: the native scan emits a string column's dictionary-encoded batches as dictionary vectors (ids over
   * the row group's dictionary) instead of resolving them into bytes, as Spark's scan path already hands them
   * to our operators. A row group whose pages fall back from the dictionary decodes flat from that page on.
   */
  def scanNativeParquetDictionaryStrings(conf: SQLConf): Boolean =
    bool(conf, ScanNativeParquetDictionaryStrings, default = true)

  /**
   * #611: a filter directly over the native Parquet scan hands it its condition; the scan decodes the
   * condition's columns first and the others only at the surviving rows (a run of dropped rows is skipped in
   * the page streams, a batch with no survivor is skipped whole), emitting the batch with its selection.
   * Off by default: it saves decode CPU, not I/O, and on TPC-DS at 1 TB it is even overall with q24b 10% slower.
   * Worth turning on for very selective filters over large tables, wide string columns behind a selective
   * filter, or data clustered on the filtered column (see docs/configuration.md).
   */
  def scanNativeParquetLateMaterialization(conf: SQLConf): Boolean =
    bool(conf, ScanNativeParquetLateMaterialization, default = false)

  /**
   * #611: the smallest scan (bytes on storage) a decode filter is handed to. A dimension-sized scan saves
   * nothing worth the change of output (a selection instead of compacted rows); at 1 TB the decode filters
   * on q24's store and item scans cost the query 7-13% downstream.
   */
  def scanNativeParquetLateMaterializationMinBytes(conf: SQLConf): Long =
    scala.util.Try(conf.getConfString(ScanNativeParquetLateMaterializationMinBytes, (1L << 30).toString).trim.toLong)
      .toOption
      .filter(_ >= 0)
      .getOrElse(1L << 30)

  private def intIn(conf: SQLConf, key: String, default: Int, max: Int): Int =
    scala.util.Try(conf.getConfString(key, default.toString).trim.toInt).toOption.map(n =>
      math.max(0, math.min(n, max))
    ).getOrElse(default)
  def cometPreferComet(conf: SQLConf): String = conf.getConfString(CometPreferComet, "")

  /**
   * Bit-identical floating-point results to Spark's (the default): double sums use one accumulator
   * per group and add rows in order, instead of lane-parallel and interleaved partial sums that
   * round differently in the last bits. Costs about 7% of aggregate kernel time (2.5% of TPC-H Q1
   * at SF10); `false` buys that back for queries that never compare a double sum for equality.
   * Comet's `spark.comet.exec.strictFloatingPoint` is the analogous switch with the opposite
   * default (`false`) and mechanism (on, Comet falls back to Spark for operations that may differ;
   * we compute the strict result in our kernels).
   */
  def strictFloatingPoint(conf: SQLConf): Boolean = bool(conf, StrictFloatingPoint, default = true)
  def explainFallback(conf: SQLConf): Boolean = bool(conf, ExplainFallbackEnabled, default = false)

  /** The shared-aggregate-inputs rewrite (#632); also off when the plugin is off. */
  def sharedAggregateInputsEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, SharedAggregateInputsEnabled, default = true)

  /** DPP through an aggregate (#633); also off when the plugin is off. */
  def dppThroughAggregateEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, DppThroughAggregateEnabled, default = true)

  /** Transitive DPP through a second join key (#634); also off when the plugin is off. */
  def transitiveDppEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, TransitiveDppEnabled, default = true)

  /** The fact-to-fact bloom filter (#641); also off when the plugin is off. */
  def factBloomFilterEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, FactBloomFilterEnabled, default = true)

  def factBloomFilterSizeRatio(conf: SQLConf): Int =
    math.max(2, conf.getConfString(FactBloomFilterSizeRatio, "10").trim.toInt)

  def factBloomFilterMergeBuckets(conf: SQLConf): Int =
    math.max(1, conf.getConfString(FactBloomFilterMergeBuckets, "32").trim.toInt)

  private def bool(conf: SQLConf, key: String, default: Boolean): Boolean =
    conf.getConfString(key, default.toString).trim.equalsIgnoreCase("true")

  /**
   * The UI tab is attached from the driver plugin, before any session exists, so these two are
   * read from the [[org.apache.spark.SparkConf]] rather than a session's SQLConf.
   */
  def uiEnabled(get: String => Option[String]): Boolean =
    get(UiEnabled).forall(_.trim.equalsIgnoreCase("true"))

  def uiRetainedExecutions(get: String => Option[String]): Int =
    get(UiRetainedExecutions).flatMap(v => scala.util.Try(v.trim.toInt).toOption).filter(_ > 0).getOrElse(100)
}
