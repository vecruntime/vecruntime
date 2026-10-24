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
package io.vecruntime.spark

import org.apache.spark.sql.internal.SQLConf

/** Configuration keys for the spark-vector plugin. All keys are read from the session's SQLConf. */
object VectorConf {

  /** #962: the tri-state of `spark.vecruntime.scan.nativeParquet.lateMaterialization`. */
  sealed trait LateMode
  object LateMode {
    case object Off extends LateMode
    case object On extends LateMode
    case object Auto extends LateMode
  }

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
   * to Spark's writer unchanged, with a printed fallback reason. Default ON (owner, 2026-10-23). See
   * `docs/iceberg.md`.
   */
  val IcebergDvWriterEnabled = "spark.vecruntime.iceberg.dvWriter.enabled"

  /**
   * #901 M2: under the merge-on-read writer (`dvWriter.enabled`), whether a v2 table's deletes are written as
   * columnar Parquet position delete files (default true); off, a v2 table keeps Spark's own `WriteDeltaExec`.
   */
  val IcebergPositionDeletesEnabled = "spark.vecruntime.iceberg.positionDeletes.enabled"

  /**
   * #901 R1: the native Iceberg scan resolves each data file's columns with Iceberg's own rules (iceberg-bridge's
   * `IcebergReadSpecs`: field ids, the table's name mapping, Iceberg's positional fallback ids, promotions,
   * initial defaults) instead of Spark's `clipParquetSchema` over field-id metadata. Without the bridge on the
   * classpath the scan resolves as before. On by default; off is the previous resolution, for A/B runs.
   */
  val IcebergReadSpecsEnabled = "spark.vecruntime.iceberg.readSpecs.enabled"

  /**
   * #901 R2: the native Iceberg scan applies equality deletes on each batch's lanes (iceberg-bridge's
   * `EqualityDeleteSet`: one hash probe per row over the key's canonical bytes) instead of wrapping every row for
   * Iceberg's `StructLikeSet`. The delete rows are still loaded by Iceberg's own delete loader. Without the bridge,
   * or for a key type it does not take, the row probe stays. On by default.
   */
  val IcebergNativeEqualityDeletesEnabled = "spark.vecruntime.iceberg.nativeEqualityDeletes.enabled"

  /**
   * The columnar Iceberg data-file writer (#842, W2): `INSERT INTO`, `INSERT OVERWRITE` and copy-on-write
   * `DELETE` / `UPDATE` / `MERGE INTO` into an Iceberg table write their Parquet data files from our batches
   * through `IcebergParquetAppender` (the `iceberg-bridge` module) instead of Iceberg's row writer; the commit
   * stays Iceberg's own (`write.toBatch()`). Anything it does not take declines to Spark's writer with a printed
   * reason. Default ON (owner, 2026-10-23).
   */
  val IcebergWriterEnabled = "spark.vecruntime.iceberg.writer.enabled"

  /**
   * Partial columnar merge (#901 M4a): a copy-on-write `DELETE` handled by the columnar writer copies the
   * byte-for-byte row groups of a rewritten file that have no deleted rows, instead of decoding and re-encoding
   * them. Needs the write's `_file` / `_pos` on the batches (Iceberg's copy-on-write scan projects both).
   * Unpartitioned tables only for now; a file whose schema differs from the write schema, or whose tracked
   * float/double NaN counts a copy cannot supply, falls back to the full rewrite. Default ON.
   */
  val IcebergCopyUnchangedEnabled = "spark.vecruntime.iceberg.copyUnchanged.enabled"

  /**
   * Columnar bin-pack compaction (#957 M4c): `rewrite_data_files` with the BINPACK strategy compacts a file
   * group by copying its input files' Parquet row groups byte for byte into target-sized outputs, instead of
   * decoding and re-encoding every row. A file that cannot be copied -- deletes attached, schema evolution, a
   * different partition spec, a codec mismatch, v3 unmaterialised row lineage, an unreadable encoding, or a
   * table tracking float/double NaN counts -- falls back per file to Iceberg's own bin-pack, so the result is
   * indistinguishable from the stock rewrite. Engaged only through `VectorRewriteDataFiles`, which installs the
   * vecruntime runner on Iceberg's `RewriteDataFilesSparkAction`. Default ON when the runner is used (there is
   * no correctness difference, only speed); set false to force the stock decode/encode path for A/B.
   */
  val IcebergBinPackCopyEnabled = "spark.vecruntime.iceberg.rewrite.binpackCopy.enabled"

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

  /** Logical rewrite (#635): a substring or length of one join side's string column computed below the join. */
  val NarrowBelowJoinEnabled = "spark.vecruntime.optimizer.narrowBelowJoin.enabled"

  /** Logical rewrite (#657): a star join's fact side aggregated by its join keys before the joins. */
  val AggregateBelowJoinEnabled = "spark.vecruntime.optimizer.aggregateBelowJoin.enabled"

  /** Logical rewrite (#692): a join input that only supplies grouping columns collapsed to distinct rows and a count. */
  val CountBelowJoinEnabled = "spark.vecruntime.optimizer.countBelowJoin.enabled"

  /** Logical rewrite (#840): a left semi join's right side reduced to its distinct join columns, per task. */
  val DistinctBelowSemiJoinEnabled = "spark.vecruntime.optimizer.distinctBelowSemiJoin.enabled"

  /** Logical rewrite (#904): a left anti join's right side reduced to its distinct join columns, per task. */
  val DistinctBelowAntiJoinEnabled = "spark.vecruntime.optimizer.distinctBelowAntiJoin.enabled"

  /** The reduction (rows / groups) statistics must allow for CountBelowJoin to collapse a side (#692; default 2). */
  val CountBelowJoinMinReduction = "spark.vecruntime.optimizer.countBelowJoin.minReduction"

  /** Minimum row reduction (rows / estimated groups) statistics must show for AggregateBelowJoin; 0 = none. */
  val AggregateBelowJoinMinReduction = "spark.vecruntime.optimizer.aggregateBelowJoin.minReduction"

  /** Whether AggregateBelowJoin declines when there are no statistics to show a reduction (#675; default false). */
  val AggregateBelowJoinRequireStatistics = "spark.vecruntime.optimizer.aggregateBelowJoin.requireStatistics"

  /** Whether AggregateBelowJoin's pre-aggregate is one per task with no exchange (#693; default true). */
  val AggregateBelowJoinLocal = "spark.vecruntime.optimizer.aggregateBelowJoin.local"

  /** Rows of a task after which a local pre-aggregate that does not reduce gives up (#693; 0 = only at the budget). */
  val AggregateBelowJoinProbeRows = "spark.vecruntime.optimizer.aggregateBelowJoin.probeRows"

  /**
   * Adaptive rewrite (#635): grouping keys that are columns of a broadcast join's build side are dropped
   * when that side's join key is unique at run time and is itself a grouping key.
   */
  val RemoveRedundantGroupKeysEnabled = "spark.vecruntime.optimizer.removeRedundantGroupKeys.enabled"

  /**
   * Logical rewrite (#641): a runtime bloom filter on a shuffle join's much larger side, built from the other
   * side (a fact from a smaller fact), where Spark's own runtime filter needs a selective filtered scan.
   */
  val FactBloomFilterEnabled = "spark.vecruntime.optimizer.factBloomFilter.enabled"

  /** #735: broadcast a small DSv2 (Iceberg) join input by its planned file bytes (V2BroadcastBySize). */
  val V2BroadcastBySizeEnabled = "spark.vecruntime.optimizer.v2BroadcastBySize.enabled"

  /** #735: Spark's runtime bloom filter from a small DSv2 (Iceberg) dimension by its planned file bytes. */
  val V2RuntimeFilterBySizeEnabled = "spark.vecruntime.optimizer.v2RuntimeFilterBySize.enabled"

  /** #735: V2BroadcastBySize leaves an input whose own size estimate exceeds this many bytes (default 1 GiB). */
  val V2BroadcastBySizeMaxEstimatedBytes = "spark.vecruntime.optimizer.v2BroadcastBySize.maxEstimatedBytes"

  /** Run-time evidence (#659): the build counts the creation side's actual rows and declines when not much smaller. */
  val FactBloomFilterRuntimeEvidence = "spark.vecruntime.optimizer.factBloomFilter.runtimeEvidence"

  /** The filter's build reads the join's creation-side exchange instead of a second copy (#659, AQE only). */
  val FactBloomFilterShareExchange = "spark.vecruntime.optimizer.factBloomFilter.shareExchange"

  /** Run-time evidence: the creation side's actual rows at most this share of its key's base table (#659). */
  val FactBloomFilterRuntimeMaxReduced = "spark.vecruntime.optimizer.factBloomFilter.runtimeMaxReduced"

  /** Transitive reduction (#659): a selective broadcast dimension above a shuffle join reduces its input first. */
  val FactBloomFilterTransitive = "spark.vecruntime.optimizer.factBloomFilter.transitive"

  /** How many times larger (estimated bytes) the filtered side must be than the side the filter is built from. */
  val FactBloomFilterSizeRatio = "spark.vecruntime.optimizer.factBloomFilter.sizeRatio"

  /**
   * Fan-in of the filter's two-level merge (#646): the creation side's per-task partial filters are merged
   * in this many groups in parallel, then the groups' filters in one task. 1 restores a single-level merge.
   */
  val FactBloomFilterMergeBuckets = "spark.vecruntime.optimizer.factBloomFilter.mergeBuckets"

  /**
   * With distinct counts for both join keys (#650): fire only when at most this share of the application
   * rows can find a match, `ndv(creation key) / ndv(application key)`.
   */
  val FactBloomFilterMaxSelectivity = "spark.vecruntime.optimizer.factBloomFilter.maxSelectivity"

  /**
   * Total bits a FactBloomFilter filter may take (#653). A filter needing more, even at 4 bits a key, is declined:
   * built saturated it would cost its build and prune nothing.
   */
  val FactBloomFilterMaxTotalBits = "spark.vecruntime.optimizer.factBloomFilter.maxTotalBits"

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

  /**
   * #64: write Parquet from our batches (VectorWriteFilesExec and the parquet-writer module), without the
   * columnar-to-row conversion and Spark's row-by-row ParquetWriteSupport. Default ON (owner, 2026-10-23); anything it
   * does not take keeps Spark's writer with a printed reason.
   */
  val WriteColumnar = "spark.vecruntime.write.columnar.enabled"

  /**
   * #842: the columnar writer also takes a write whose input is rows (a local relation, a row operator), through
   * Spark's RowToColumnarExec. Off by default; Spark's own write suites run with it on, so every Parquet write
   * they make goes through our writer.
   */
  val WriteColumnarFromRows = "spark.vecruntime.write.columnar.fromRows"

  /**
   * #735: Iceberg tables read with our native Parquet reader (VectorIcebergScanExec) over Iceberg's own planning:
   * each task group of data files without deletes is decoded by VectorParquetPartitionReader, any other group by
   * Iceberg's reader. Default ON (owner, 2026-10-23).
   */
  val ScanNativeIceberg = "spark.vecruntime.scan.nativeIceberg.enabled"

  /**
   * #735: VectorIcebergScanExec packs Iceberg's task groups into Spark tasks as Spark packs the files of a Parquet
   * scan: up to `spark.sql.files.maxPartitionBytes`, lowered to fill `spark.sql.files.minPartitionNum` (default the
   * leaf parallelism) cores, each file costing `spark.sql.files.openCostInBytes` more. Iceberg's adaptive split size
   * aims at one split per core with a 16 MB floor and never combines files of 19 MB: at 3 TB q40 read
   * `catalog_sales` in 1014 tasks against its Parquet copy's 338, each paying a file open and footer read.
   * Only when the scan reports no partitioning (a storage-partitioned join keeps Iceberg's groups).
   */
  val ScanNativeIcebergPackSplits = "spark.vecruntime.scan.nativeIceberg.packSplits"

  /**
   * #iceberg-file-pruning: prune Iceberg data files by a broadcast hash join's RUNTIME filters (#610) before the
   * reader opens them. The static pushed filters are already pruned by Iceberg's own planning
   * (`InclusiveMetricsEvaluator` over manifests), so those files are never opened; the runtime filters, known
   * only after the build side runs, are today ANDed into the per-file Parquet row-group statistics filter, so
   * every file is still opened (its HEAD + footer read) and only its row groups skipped. With this on (default
   * true), the runtime filters are converted to an Iceberg `Expression` and each native data file is tested with
   * `InclusiveMetricsEvaluator` on the driver; a file that cannot match is dropped before partitions are built,
   * so it is never opened. Off restores the row-group-only behaviour.
   */
  val ScanNativeIcebergRuntimeFilePruning = "spark.vecruntime.scan.nativeIceberg.runtimeFilePruning.enabled"

  /** VectorParquetScanExec read-ahead (#559/#566): files of a split opened ahead, `0` off. */
  val ScanNativeParquetPrefetchFiles = "spark.vecruntime.scan.nativeParquet.prefetchFiles"

  /** VectorParquetScanExec read-ahead (#559/#566): row groups of the current file read ahead, `0` off. */
  val ScanNativeParquetPrefetchRowGroups = "spark.vecruntime.scan.nativeParquet.prefetchRowGroups"
  val ScanNativeParquetPrefetchMaxBytes = "spark.vecruntime.scan.nativeParquet.prefetchMaxBytes"
  val ScanNativeParquetPrefetchExecutorMaxBytes = "spark.vecruntime.scan.nativeParquet.prefetchExecutorMaxBytes"

  /** VectorParquetScanExec decode-ahead (#606): batches decoded ahead on a producer thread, `0` off. */
  val ScanNativeParquetDecodeAhead = "spark.vecruntime.scan.nativeParquet.decodeAhead"

  /** The decode-ahead producer's thread kind (#606): `virtual` or `platform`. */
  val ScanNativeParquetDecodeAheadThreads = "spark.vecruntime.scan.nativeParquet.decodeAhead.threads"

  /**
   * VectorParquetScanExec parallel row-group decode (#606): how many HELPER threads decode the row groups
   * AFTER the one the task thread consumes, each on its own column-reader set and vectors, handed over in
   * file order so the output is identical. `0` (the default) decodes serially / single-producer
   * (decode-ahead). It engages only for a file with at least two row groups above `parallelDecode.minRowGroupBytes`.
   */
  val ScanNativeParquetParallelDecode = "spark.vecruntime.scan.nativeParquet.parallelDecode"

  /** The parallel-decode helper threads' kind (#606): `virtual` (the default) or `platform`. */
  val ScanNativeParquetParallelDecodeThreads = "spark.vecruntime.scan.nativeParquet.parallelDecode.threads"

  /**
   * The row-group size (its requested column chunks' compressed bytes) at or above which a file's row groups
   * are decoded in parallel (#606), when `parallelDecode` > 0 and the file has at least two such row groups.
   * Default 32 MiB. Below it the file decodes single-producer, since small row groups gain nothing from a
   * second decode thread (the TPC-DS store_sales shape, ~7 MB single-row-group files).
   */
  val ScanNativeParquetParallelDecodeMinRowGroupBytes =
    "spark.vecruntime.scan.nativeParquet.parallelDecode.minRowGroupBytes"

  /** VectorParquetScanExec (#612): dictionary-encoded string columns as dictionary vectors, default on. */
  val ScanNativeParquetDictionaryStrings = "spark.vecruntime.scan.nativeParquet.dictionaryStrings"

  /** #611: late materialization in the native scan. */
  val ScanNativeParquetLateMaterialization = "spark.vecruntime.scan.nativeParquet.lateMaterialization"
  val ScanNativeParquetLateMaterializationMinBytes =
    "spark.vecruntime.scan.nativeParquet.lateMaterialization.minScanBytes"

  /**
   * #962: plan-time gate for `auto`. The decode filter is attached only when the filter columns are a small
   * fraction of the projected bytes, weighted by estimated byte width (strings, decimals and nested columns
   * count heavily). This is the largest that fraction may be (default 0.5): at or above it the filter already
   * covers most of what the projection decodes, so late materialization cannot pay and the eager path is kept.
   */
  val ScanNativeParquetLateMaterializationMaxFilterBytesFraction =
    "spark.vecruntime.scan.nativeParquet.lateMaterialization.auto.maxFilterBytesFraction"

  /**
   * #962: read-time threshold for `auto`. A row group is late-materialized only when its estimated surviving
   * fraction (from column statistics, dictionary pages, bloom filters and the observed selectivity of earlier
   * row groups of the same file) is at or below this (default 0.2); otherwise it is decoded eagerly.
   */
  val ScanNativeParquetLateMaterializationLateFraction =
    "spark.vecruntime.scan.nativeParquet.lateMaterialization.auto.lateFraction"

  /**
   * #962: the surviving fraction below which `auto` escalates a late-materialized row group to two passes
   * (default 0.05, i.e. very low). The A/B (#901) shows two-pass overhead dominates otherwise.
   */
  val ScanNativeParquetLateMaterializationTwoPassFraction =
    "spark.vecruntime.scan.nativeParquet.lateMaterialization.auto.twoPassFraction"

  /**
   * #812: two-pass late materialization. With a decode filter (#611), each row group's filter columns are read
   * and the filter evaluated over the whole row group first; the row group is then read again, every column,
   * over only the rows the filter keeps (`ParquetFileReader.readFilteredRowGroup` with those row ranges), so the
   * pages of the other columns that hold no surviving row are not fetched or decompressed at all. Needs the
   * file's offset index; off by default. Read from the Hadoop configuration, which carries the session's
   * `spark.*` settings.
   */
  val ScanNativeParquetLateMaterializationTwoPass =
    "spark.vecruntime.scan.nativeParquet.lateMaterialization.twoPass"

  /**
   * #812: the largest fraction of a row group's rows the filter may keep for the second pass to read only
   * those (default 0.5); above it the row group is read whole, as with one pass.
   */
  val ScanNativeParquetLateMaterializationTwoPassMaxFraction =
    "spark.vecruntime.scan.nativeParquet.lateMaterialization.twoPass.maxFraction"

  /**
   * #735: a column chunk's data pages decompress through one reused `byte[]` per decompressor (zstd, snappy)
   * instead of a fresh array per page, read in place by #786. Default on.
   */
  val ScanNativeParquetReuseDecompressBuffers = "spark.vecruntime.scan.parquet.reuseDecompressBuffers.enabled"

  /** #735: parquet-java's per-row-group column-chunk buffers come from a small bounded heap pool. Default on. */
  val ScanNativeParquetPoolChunkBuffers = "spark.vecruntime.scan.parquet.poolChunkBuffers.enabled"

  /**
   * #834: a file whose length is at or under this is fetched WHOLE in one ranged GET on the prefetch thread,
   * and {@code ParquetFileReader} is served footer, chunks and page indexes from that buffer -- so a 7 MB
   * {@code store_sales} file (one row group) costs one request instead of footer-length + footer + row-group
   * reads plus any per-page GETs. The threshold itself is the gate (the Accelerator prefetches a small object
   * whole), so keep it at the size where reading every byte is cheaper than several ranged reads. `0` off (the
   * default until a 1 TB A/B settles it); e.g. `8m`.
   */
  val ScanNativeParquetSmallFileThreshold = "spark.vecruntime.scan.nativeParquet.smallFileThreshold"

  /**
   * #834: range merging. When a file is read over its planned byte ranges (the projected column chunks of the
   * split's row groups, and with the column index on only the selected pages plus the index reads), ranges
   * whose gap is at or under this are coalesced into one larger GET instead of many small seeks. `<0` (the
   * DEFAULT, until a 1 TB A/B settles it) turns range merging off, so the read path is parquet-java's own; `0`
   * merges only exactly-adjacent ranges; a positive value such as `1m` (the Accelerator's
   * {@code request.coalesce.tolerance}) swallows gaps up to that size.
   */
  val ScanNativeParquetRangeMergeGap = "spark.vecruntime.scan.nativeParquet.rangeMergeGap"

  /**
   * #834: a coalesced run longer than this is split into parts of about this size, issued in parallel on the
   * prefetch threads, so a long sequential run is not one giant stream that starves S3 bandwidth (DuckDB 2.0's
   * too-few-too-large result). `0` never splits. Default 8 MiB, the Accelerator's {@code target.request.size}.
   */
  val ScanNativeParquetRangeTargetSize = "spark.vecruntime.scan.nativeParquet.rangeTargetSize"

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
   * The columnar Iceberg merge-on-read writer (#20, #901 M1/M2): deletion vectors on v3, position delete files on
   * v2, the insert half on the columnar data writer. Default `true` (owner, 2026-10-23); anything it does not take
   * declines to Spark's `WriteDeltaExec` with a printed reason.
   */
  def icebergDvWriterEnabled(conf: SQLConf): Boolean = bool(conf, IcebergDvWriterEnabled, default = true)
  def icebergReadSpecsEnabled(conf: SQLConf): Boolean = bool(conf, IcebergReadSpecsEnabled, default = true)
  def icebergNativeEqualityDeletesEnabled(conf: SQLConf): Boolean =
    bool(conf, IcebergNativeEqualityDeletesEnabled, default = true)

  /** #901 M2: v2 position delete files under the merge-on-read writer. Default `true`. */
  def icebergPositionDeletesEnabled(conf: SQLConf): Boolean =
    bool(conf, IcebergPositionDeletesEnabled, default = true)

  /** The columnar Iceberg data-file writer (#842, W2). Default `true` (owner, 2026-10-23). */
  def icebergWriterEnabled(conf: SQLConf): Boolean = bool(conf, IcebergWriterEnabled, default = true)

  /** Copy delete-free row groups in a copy-on-write DELETE (#901 M4a). Default `true`. */
  def icebergCopyUnchangedEnabled(conf: SQLConf): Boolean = bool(conf, IcebergCopyUnchangedEnabled, default = true)
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
    // #877: with off-heap memory and no explicit setting there is no cap -- the pool's grant is the ceiling and the
    // task's fair share is only where the sort starts preferring to spill (sortFairShareBytes).
    val v = if (explicit.nonEmpty) org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)
    else if (offHeapTaskShare(sparkConf).isDefined) Long.MaxValue
    else overheadTaskShare(sparkConf).fold(DefaultSortSpillBytes)(math.min(DefaultSortSpillBytes, _))
    if (v <= 0) Long.MaxValue else v
  }

  /**
   * Without off-heap memory the sort's native runs are held against the on-heap pool, which is sized from the heap
   * and does not bound native memory against `spark.executor.memoryOverhead`, where those runs actually live. At
   * 1 TB, 30g heap / 20g overhead / 13 cores, the flat 1 GiB budget let the runs reach 17.5 GB native and an
   * executor was lost in every run; 64 MiB kept it at 6.5 GB. So the default is also bounded by half the task's share
   * of the overhead left after direct memory: `(memoryOverhead - direct) / (2 * executor.cores)` -- half, because the
   * runs' malloc arenas hold more than their counted bytes and the overhead also carries the JVM's own native memory;
   * `direct` is `-XX:MaxDirectMemorySize` from the executor's Java options, or half the overhead when it is not set
   * (the JVM's default is the heap size, which would leave nothing). At least 64 MiB. None when the overhead is not
   * set.
   */
  def overheadTaskShare(sparkConf: org.apache.spark.SparkConf): Option[Long] =
    sparkConf.getOption("spark.executor.memoryOverhead").map { o =>
      val overhead = org.apache.spark.network.util.JavaUtils.byteStringAsBytes(o.trim match {
        case s if s.nonEmpty && s.forall(_.isDigit) => s + "m" // Spark reads a bare number as MiB
        case s => s
      })
      val direct = "-XX:MaxDirectMemorySize=(\\S+)".r
        .findFirstMatchIn(sparkConf.get("spark.executor.extraJavaOptions", ""))
        .map(m => org.apache.spark.network.util.JavaUtils.byteStringAsBytes(m.group(1)))
        .getOrElse(overhead / 2)
      val cores = math.max(1, sparkConf.getInt("spark.executor.cores", 1))
      math.max(64L << 20, (overhead - direct) / (2L * cores))
    }

  /**
   * #877: the sort's soft spill trigger with off-heap memory and no explicit `spark.vecruntime.sort.spillBytes`: the
   * task's fair share of the pool ([[offHeapTaskShare]]), past which resident runs spill unless the pool has room
   * to spare -- the sort may grow by half of the pool's free bytes past it, so a task alone on an idle executor uses
   * what is free while one among busy siblings stays near its share. 0 when not applicable (an explicit cap or no
   * off-heap memory: the cap alone decides).
   */
  def sortFairShareBytes(conf: SQLConf, sparkConf: org.apache.spark.SparkConf): Long =
    if (conf.getConfString(SortSpillBytes, "").trim.nonEmpty) 0L else offHeapTaskShare(sparkConf).getOrElse(0L)

  /**
   * #868: a task's fair share of the off-heap execution pool, `spark.memory.offHeap.size /
   * spark.executor.cores`, when off-heap memory is enabled -- the most `TaskMemoryManager` grants a task
   * while every core runs one. The default cap of the operators whose memory the pool budgets, so the
   * cap follows the pool instead of being fixed; within it the pool's grant decides.
   */
  def offHeapTaskShare(sparkConf: org.apache.spark.SparkConf): Option[Long] = {
    val offHeap =
      if (sparkConf.getBoolean("spark.memory.offHeap.enabled", false))
        sparkConf.getSizeAsBytes("spark.memory.offHeap.size", "0")
      else 0L
    if (offHeap > 0) Some(math.max(1L, offHeap / math.max(1, sparkConf.getInt("spark.executor.cores", 1)))) else None
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

  /**
   * #875: what a shuffled hash join's build side may hold in memory before it buckets to disk: `joinSpillBytes`, but
   * never past the task's fair share of the off-heap pool (`offHeapTaskShare`), so a build that cannot be asked to
   * give memory back does not win the race to the pool and pin more than its share.
   */
  def joinBuildAdmissionBytes(conf: SQLConf, sparkConf: org.apache.spark.SparkConf): Long =
    offHeapTaskShare(sparkConf).fold(joinSpillBytes(conf, sparkConf))(math.min(joinSpillBytes(conf, sparkConf), _))
  def joinSpillBuckets(conf: SQLConf): Int =
    scala.util.Try(conf.getConfString(JoinSpillBuckets, "").trim.toInt).toOption.getOrElse(32)

  /**
   * #868: the executor's cap on Arrow memory (size string; `0`, the default, for none). Every Arrow buffer of the
   * plugin -- the Flight shuffle's server and client, the shuffle readers, the operators' output vectors -- comes from
   * one root allocator per executor, and past the cap an allocation fails (the task fails and is retried) instead of
   * the container being OOM-killed. Not part of `spark.memory.offHeap.size`: size it inside the memory overhead
   * (about 1 GB per executor was the Flight shuffle's share at 10 TB, #840). Read once, when the executor starts.
   */
  /**
   * #869 step 3b: count every block a task takes from the native pool in that task's Spark execution memory
   * (`TaskNativeMemory`), with the sort's and the grace hash join's reservations in the same account. `false` gives
   * each operator a consumer of its own again. Read once, when the executor starts.
   */
  val PoolAccountingEnabled = "spark.vecruntime.memory.poolAccounting.enabled"
  def poolAccountingEnabled(sparkConf: org.apache.spark.SparkConf): Boolean =
    sparkConf.getBoolean(PoolAccountingEnabled, true)

  /**
   * #941: grouped accumulators past 16,384 groups keep their state in native pool blocks, charged to the task in
   * the operators' memory mode, instead of `long[]` on the heap (at 10 TB those arrays fill the executors' old
   * generation). Below that the state stays on the heap. On by default: at 10 TB the full TPC-DS run is the same or
   * faster with GC -10% and q78's spill -19%. Read once, when the executor starts.
   */
  val AggNativeState = "spark.vecruntime.agg.nativeState"
  def aggNativeState(sparkConf: org.apache.spark.SparkConf): Boolean =
    sparkConf.getBoolean(AggNativeState, true)

  /**
   * #869 step 4: Arrow's buffers (the Flight shuffle's, the shuffle readers', the operators' output vectors) from
   * the native pool instead of Arrow's allocation manager (`-Darrow.allocation.manager.type`), so they are reused
   * blocks of the same pool. Arrow still accounts them and `spark.vecruntime.arrow.maxBytes` still caps them. Read
   * once, when the executor starts.
   */
  val ArrowNativePoolEnabled = "spark.vecruntime.arrow.nativePool.enabled"
  def arrowNativePoolEnabled(sparkConf: org.apache.spark.SparkConf): Boolean =
    sparkConf.getBoolean(ArrowNativePoolEnabled, true)

  /**
   * #869 step 4: the native pool's per-thread cache -- blocks per size class (0 to 16) and the largest block cached
   * (64 KiB to 64 MiB). A thread holds at most about `2 * depth * maxBlockBytes`. Off by default (depth 0): at 10 TB
   * the shared free lists alone were as fast as main and the cache slower. Read once, when the executor starts.
   */
  val NativePoolThreadCacheDepth = "spark.vecruntime.nativePool.threadCache.depth"
  val NativePoolThreadCacheMaxBlockBytes = "spark.vecruntime.nativePool.threadCache.maxBlockBytes"
  def nativePoolThreadCacheDepth(sparkConf: org.apache.spark.SparkConf): Int =
    sparkConf.getInt(NativePoolThreadCacheDepth, 0)
  def nativePoolThreadCacheMaxBlockBytes(sparkConf: org.apache.spark.SparkConf): Long =
    org.apache.spark.network.util.JavaUtils.byteStringAsBytes(sparkConf.get(NativePoolThreadCacheMaxBlockBytes, "1m"))

  val ArrowMaxBytes = "spark.vecruntime.arrow.maxBytes"
  def arrowMaxBytes(sparkConf: org.apache.spark.SparkConf): Long =
    org.apache.spark.network.util.JavaUtils.byteStringAsBytes(sparkConf.get(ArrowMaxBytes, "0").trim match {
      case "" => "0"
      case s => s
    })

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
    else offHeapTaskShare(sparkConf).getOrElse(1L << 30)
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

  /** #64: the columnar Parquet writer. Default on (owner, 2026-10-23). */
  def writeColumnar(conf: SQLConf): Boolean = bool(conf, WriteColumnar, default = true)
  def writeColumnarFromRows(conf: SQLConf): Boolean = bool(conf, WriteColumnarFromRows, default = false)

  /** #735: VectorIcebergScanExec, behind the native Parquet scan's own switch too. Default on (owner, 2026-10-23). */
  def scanNativeIceberg(conf: SQLConf): Boolean =
    bool(conf, ScanNativeIceberg, default = true) && scanNativeParquet(conf)

  def scanNativeIcebergPackSplits(conf: SQLConf): Boolean = bool(conf, ScanNativeIcebergPackSplits, default = true)

  /** #iceberg-file-pruning: prune Iceberg data files by runtime filters before opening them (default on). */
  def scanNativeIcebergRuntimeFilePruning(conf: SQLConf): Boolean =
    bool(conf, ScanNativeIcebergRuntimeFilePruning, default = true)

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
  def scanNativeParquetPrefetchFiles(conf: SQLConf): Int = intIn(conf, ScanNativeParquetPrefetchFiles, 12, 16)

  /**
   * How many row groups of the current file `VectorParquetScanExec` reads ahead (#559/#566), on platform
   * threads, chained so a file's reader is used by one thread at a time and in order. Pays off on files with
   * several row groups. `0` reads each row group on the task thread. Memory per task grows by up to N row
   * groups (compressed pages), so it scales with the row-group size. Capped at 16.
   */
  def scanNativeParquetPrefetchRowGroups(conf: SQLConf): Int =
    intIn(conf, ScanNativeParquetPrefetchRowGroups, 4, 16)

  /**
   * #568: the most compressed bytes one task's read-ahead (files opened ahead and row groups read ahead)
   * keeps in flight, on top of the depths above, so wide row groups cannot multiply its memory by the depth.
   * A file is counted at its split's length, a row group at its requested column chunks' size. One file or
   * row group ahead is always allowed, so the read-ahead never stops altogether. `0` or less: no cap.
   * Default 256 MiB.
   */
  def scanNativeParquetPrefetchMaxBytes(conf: SQLConf): Long =
    byteCap(conf, ScanNativeParquetPrefetchMaxBytes, 256L << 20)

  /**
   * #568: the same cap over every task of the executor: no task starts another read-ahead while the
   * executor's in flight would pass it (each task still keeps one ahead). `0` or less: no cap. Default 2 GiB.
   */
  def scanNativeParquetPrefetchExecutorMaxBytes(conf: SQLConf): Long =
    byteCap(conf, ScanNativeParquetPrefetchExecutorMaxBytes, 2L << 30)

  private def byteCap(conf: SQLConf, key: String, default: Long): Long = {
    val explicit = conf.getConfString(key, "").trim
    val v = if (explicit.nonEmpty) org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit) else default
    if (v <= 0) Long.MaxValue else v
  }

  /**
   * Decode-ahead (#606): how many batches `VectorParquetScanExec` decodes ahead of the task thread, on a
   * producer thread of each task's own, so decoding overlaps the operators above the scan. `0` (the default
   * until measured) decodes on the task thread. Memory per task grows by up to N batches. Capped at 16.
   */
  def scanNativeParquetDecodeAhead(conf: SQLConf): Int = intIn(conf, ScanNativeParquetDecodeAhead, 2, 16)

  /** The decode-ahead producer's thread kind: `virtual` (the default) or `platform`. */
  def scanNativeParquetDecodeAheadVirtual(conf: SQLConf): Boolean =
    !conf.getConfString(ScanNativeParquetDecodeAheadThreads, "virtual").trim.equalsIgnoreCase("platform")

  /**
   * Parallel row-group decode (#606): how many HELPER threads `VectorParquetScanExec` uses to decode the row
   * groups after the one the task thread consumes, each with its own column-reader set and vectors, handed
   * over in file order. `0` (the default) keeps the serial / single-producer (decode-ahead) path. It engages
   * only for a file with at least two row groups at or above `parallelDecode.minRowGroupBytes`; a file that
   * does not clear that (the TPC-DS store_sales shape) decodes as before. Off by default: a cluster running as
   * many tasks as cores already keeps every core busy, so a second decode thread per task only adds contention
   * there; the win is where cores sit idle (wide row groups, few large files). Capped at 8.
   */
  def scanNativeParquetParallelDecode(conf: SQLConf): Int = intIn(conf, ScanNativeParquetParallelDecode, 0, 8)

  /** The parallel-decode helper threads' kind: `virtual` (the default) or `platform`. */
  def scanNativeParquetParallelDecodeVirtual(conf: SQLConf): Boolean =
    !conf.getConfString(ScanNativeParquetParallelDecodeThreads, "virtual").trim.equalsIgnoreCase("platform")

  /** The per-row-group byte threshold for parallel decode (#606), default 32 MiB; `0` engages at any size. */
  def scanNativeParquetParallelDecodeMinRowGroupBytes(conf: SQLConf): Long = {
    val v = scala.util.Try(
      conf.getConfString(ScanNativeParquetParallelDecodeMinRowGroupBytes, (32L << 20).toString).trim.toLong
    ).getOrElse(32L << 20)
    math.max(0L, v)
  }

  /**
   * #612: the native scan emits a string column's dictionary-encoded batches as dictionary vectors (ids over
   * the row group's dictionary) instead of resolving them into bytes, as Spark's scan path already hands them
   * to our operators. A row group whose pages fall back from the dictionary decodes flat from that page on.
   */
  def scanNativeParquetDictionaryStrings(conf: SQLConf): Boolean =
    bool(conf, ScanNativeParquetDictionaryStrings, default = true)

  /**
   * #611/#962: whether a filter directly over the native Parquet scan is handed to it as a decode filter --
   * the scan decodes the condition's columns first and the others only at the surviving rows (a run of dropped
   * rows is skipped in the page streams, a batch with no survivor is skipped whole), emitting the batch with
   * its selection.
   *
   *   - `off` / `false`: never; the eager path always.
   *   - `on` / `true`: always, for any filter the plan-time shape check (`markDecodeFilters`) admits -- the
   *     #611 behaviour.
   *   - `auto` (the default): the plan-time gate adds the decode filter only when the filter columns are a
   *     small fraction of the projected bytes (byte-width weighted), and the reader then decides per row group
   *     from statistics / dictionaries / bloom filters / observed selectivity whether to late-materialize it.
   *
   * The A/B (#901) showed a global flip loses on TPC-DS at 1 TB (q24b +11.5%) with no query winning, so the
   * default is `auto`, which keeps the eager path except where the filter is selective on a wide projection.
   */
  def scanNativeParquetLateMaterializationMode(conf: SQLConf): LateMode = {
    val raw = conf.getConfString(ScanNativeParquetLateMaterialization, "auto").trim.toLowerCase(java.util.Locale.ROOT)
    raw match {
      case "on" | "true" => LateMode.On
      case "off" | "false" => LateMode.Off
      case _ => LateMode.Auto
    }
  }

  /** #611: late materialization is at all considered at plan time (`on` or `auto`). */
  def scanNativeParquetLateMaterialization(conf: SQLConf): Boolean =
    scanNativeParquetLateMaterializationMode(conf) != LateMode.Off

  /** #962: the plan-time filter-bytes-fraction ceiling for `auto` (default 0.5). */
  def scanNativeParquetLateMaterializationMaxFilterBytesFraction(conf: SQLConf): Double =
    doubleIn(conf, ScanNativeParquetLateMaterializationMaxFilterBytesFraction, 0.5)

  /** #962: the per-row-group surviving-fraction threshold for late materialization under `auto` (default 0.2). */
  def scanNativeParquetLateMaterializationLateFraction(conf: SQLConf): Double =
    doubleIn(conf, ScanNativeParquetLateMaterializationLateFraction, 0.2)

  /** #962: the surviving-fraction threshold below which `auto` escalates to two passes (default 0.05). */
  def scanNativeParquetLateMaterializationTwoPassFraction(conf: SQLConf): Double =
    doubleIn(conf, ScanNativeParquetLateMaterializationTwoPassFraction, 0.05)

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

  /**
   * #735: the native scan opens files with a {@code ReusingCodecFactory}, so each column chunk's data pages
   * decompress through one reused growable {@code byte[]} per decompressor (zstd and snappy) instead of a fresh
   * array per page; #786's in-place path then reads that array with no further copy. Default on.
   */
  def scanNativeParquetReuseDecompressBuffers(conf: SQLConf): Boolean =
    bool(conf, ScanNativeParquetReuseDecompressBuffers, default = true)

  /**
   * #735: the native scan opens files with a bounded heap {@code PooledHeapAllocator}, so parquet-java's
   * per-row-group column-chunk buffers come from a small pool instead of a fresh allocation each row group.
   * Default on.
   */
  def scanNativeParquetPoolChunkBuffers(conf: SQLConf): Boolean =
    bool(conf, ScanNativeParquetPoolChunkBuffers, default = true)

  /**
   * #834: a file at or under this length is read whole in one GET on the prefetch thread, before the footer,
   * so the footer, column chunks and page indexes all come from that one buffer. `0` (the default, until a
   * 1 TB A/B settles it) off. Parsed as a byte string (`8m`); `<= 0` means off.
   */
  def scanNativeParquetSmallFileThreshold(conf: SQLConf): Long = {
    val explicit = conf.getConfString(ScanNativeParquetSmallFileThreshold, "").trim
    if (explicit.isEmpty) 0L
    else scala.util.Try(org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)).toOption.getOrElse(0L)
  }

  /**
   * #834: the gap (bytes) at or under which two planned byte ranges are coalesced into one GET. `<0` (the
   * default) turns range merging off; `0` merges only adjacent; a positive byte string (`1m`) swallows gaps.
   */
  def scanNativeParquetRangeMergeGap(conf: SQLConf): Long = {
    val explicit = conf.getConfString(ScanNativeParquetRangeMergeGap, "").trim
    if (explicit.isEmpty) -1L
    else if (explicit.startsWith("-")) -1L
    else scala.util.Try(org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)).toOption.getOrElse(-1L)
  }

  /** #834: a coalesced run longer than this is split into parallel GETs. `0` never splits. Default 8 MiB. */
  def scanNativeParquetRangeTargetSize(conf: SQLConf): Long = {
    val explicit = conf.getConfString(ScanNativeParquetRangeTargetSize, "").trim
    if (explicit.isEmpty) 8L << 20
    else
      scala.util.Try(org.apache.spark.network.util.JavaUtils.byteStringAsBytes(explicit)).toOption
        .map(v => math.max(0L, v))
        .getOrElse(8L << 20)
  }

  private def intIn(conf: SQLConf, key: String, default: Int, max: Int): Int =
    scala.util.Try(conf.getConfString(key, default.toString).trim.toInt).toOption.map(n =>
      math.max(0, math.min(n, max))
    ).getOrElse(default)

  /** A fraction in [0, 1] read from `key`, falling back to `default` on a missing or malformed value. */
  private def doubleIn(conf: SQLConf, key: String, default: Double): Double =
    scala.util.Try(conf.getConfString(key, default.toString).trim.toDouble).toOption
      .filter(d => d >= 0.0 && d <= 1.0)
      .getOrElse(default)
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

  /** Narrowing projections below joins (#635); also off when the plugin is off. */
  def narrowBelowJoinEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, NarrowBelowJoinEnabled, default = true)

  /** Eager aggregation below star joins (#657); also off when the plugin is off. */
  def aggregateBelowJoinEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, AggregateBelowJoinEnabled, default = true)

  /** Join inputs that only supply grouping columns collapsed to distinct rows and a count (#692); off with the plugin. */
  def countBelowJoinEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, CountBelowJoinEnabled, default = true)

  /** A left semi join's right side as its distinct join columns, one aggregate per task (#840); off with the plugin. */
  def distinctBelowSemiJoinEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, DistinctBelowSemiJoinEnabled, default = true)

  /** A left anti join's right side as its distinct join columns, one aggregate per task (#904); off with the plugin. */
  def distinctBelowAntiJoinEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, DistinctBelowAntiJoinEnabled, default = true)

  /** CountBelowJoin leaves a side alone when statistics bound its groups above rows / this (default 2, #692). */
  def countBelowJoinMinReduction(conf: SQLConf): Double =
    math.max(1.0, conf.getConfString(CountBelowJoinMinReduction, "2").trim.toDouble)

  /**
   * AggregateBelowJoin's required reduction (rows / estimated groups); default 0, no estimate (#675: at 1 TB
   * the estimate declined q4, q11 and q74, which the pre-aggregate makes 35-50 % faster).
   */
  def aggregateBelowJoinMinReduction(conf: SQLConf): Double =
    math.max(0.0, conf.getConfString(AggregateBelowJoinMinReduction, "0").trim.toDouble)

  /** AggregateBelowJoin declines without statistics (default false, #675). */
  def aggregateBelowJoinRequireStatistics(conf: SQLConf): Boolean =
    bool(conf, AggregateBelowJoinRequireStatistics, default = false)

  /** AggregateBelowJoin's pre-aggregate local to each task (default true, #693); false plans Spark's two stages. */
  def aggregateBelowJoinLocal(conf: SQLConf): Boolean = bool(conf, AggregateBelowJoinLocal, default = true)

  /**
   * The rows a local pre-aggregate reads before it judges its reduction (default 16384, 2 batches of 8192; #708):
   * fewer groups than rows / `spark.vecruntime.agg.passThroughRatio` keeps it aggregating, more sends the rest
   * of the task through one batch at a time (#693). 0 judges only when the table reaches its memory budget.
   */
  def aggregateBelowJoinProbeRows(conf: SQLConf): Long =
    math.max(0L, conf.getConfString(AggregateBelowJoinProbeRows, "16384").trim.toLong)

  /** Dropping grouping keys a unique broadcast key determines (#635); also off when the plugin is off. */
  def removeRedundantGroupKeysEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, RemoveRedundantGroupKeysEnabled, default = true)

  /** The fact-to-fact bloom filter (#641); also off when the plugin is off. */
  /** V2BroadcastBySize (#735), default true. */
  def v2BroadcastBySizeEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, V2BroadcastBySizeEnabled, default = true)

  /** V2RuntimeFilterBySize (#735), default true. */
  def v2RuntimeFilterBySizeEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, V2RuntimeFilterBySizeEnabled, default = true)

  /** V2BroadcastBySize's cap on an input's own size estimate (#735), default 1 GiB; `1g` style sizes accepted. */
  def v2BroadcastBySizeMaxEstimatedBytes(conf: SQLConf): Long =
    scala.util.Try(
      org.apache.spark.network.util.JavaUtils.byteStringAsBytes(conf.getConfString(
        V2BroadcastBySizeMaxEstimatedBytes,
        "1g"
      ).trim)
    ).toOption.filter(_ >= 0).getOrElse(1L << 30)

  def factBloomFilterEnabled(conf: SQLConf): Boolean =
    isEnabled(conf) && bool(conf, FactBloomFilterEnabled, default = true)

  /** FactBloomFilter's run-time row-count check (#659), default true. */
  def factBloomFilterRuntimeEvidence(conf: SQLConf): Boolean =
    bool(conf, FactBloomFilterRuntimeEvidence, default = true)

  /** FactBloomFilter's shared creation-side exchange (#659), default true; needs runtimeEvidence and AQE. */
  def factBloomFilterShareExchange(conf: SQLConf): Boolean =
    bool(conf, FactBloomFilterShareExchange, default = true)

  /** FactBloomFilter's run-time reduction cap (#659), default 0.2, in (0, 1]. */
  /** FactBloomFilter's transitive reduction (#659), default true. */
  def factBloomFilterTransitive(conf: SQLConf): Boolean = bool(conf, FactBloomFilterTransitive, default = true)

  def factBloomFilterRuntimeMaxReduced(conf: SQLConf): Double =
    math.min(1.0, math.max(1e-9, conf.getConfString(FactBloomFilterRuntimeMaxReduced, "0.2").trim.toDouble))

  def factBloomFilterSizeRatio(conf: SQLConf): Int =
    math.max(2, conf.getConfString(FactBloomFilterSizeRatio, "10").trim.toInt)

  def factBloomFilterMergeBuckets(conf: SQLConf): Int =
    math.max(1, conf.getConfString(FactBloomFilterMergeBuckets, "32").trim.toInt)

  def factBloomFilterMaxTotalBits(conf: SQLConf): Long =
    math.max(64L, conf.getConfString(FactBloomFilterMaxTotalBits, (512L << 20).toString).trim.toLong)

  def factBloomFilterMaxSelectivity(conf: SQLConf): Double =
    math.min(1.0, math.max(0.0, conf.getConfString(FactBloomFilterMaxSelectivity, "0.5").trim.toDouble))

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

  val ShuffleInitialPartitionsPerCore = "spark.vecruntime.shuffle.initialPartitionsPerCore"
  val CancelEndedExecutionJobs = "spark.vecruntime.cancelEndedExecutionJobs"

  /** #788: cancel the jobs still running under a SQL execution that has ended (driver plugin; default on). */
  def cancelEndedExecutionJobs(get: String => Option[String]): Boolean =
    get(CancelEndedExecutionJobs).forall(_.trim.equalsIgnoreCase("true"))
  private val InitialPartitionNum = "spark.sql.adaptive.coalescePartitions.initialPartitionNum"

  /**
   * The shuffle partition count AQE starts from (#689), set by the driver plugin before any session exists:
   * `perCore` (`spark.vecruntime.shuffle.initialPartitionsPerCore`, default 8) times the executor cores, when
   * that is more than `spark.sql.shuffle.partitions`. AQE coalesces the partitions back to its advisory size
   * (`spark.sql.adaptive.advisoryPartitionSizeInBytes`), so a small stage ends up with as few tasks as before,
   * while a large aggregate or join stage keeps enough partitions to fit its tasks in memory: at 3 TB with 23
   * executors x 5 cores, q78 and q67 spill 201 GB and 235 GB at 200 partitions and nothing when AQE starts from
   * 900.
   *
   * `None` -- nothing is set -- when the user set `initialPartitionNum`, AQE or its coalescing is off, the core
   * count is not known up front (dynamic allocation without `maxExecutors`, or no `spark.executor.instances`),
   * the factor is 0, or the result would not raise `spark.sql.shuffle.partitions`.
   */
  def initialShufflePartitions(get: String => Option[String]): Option[Int] = {
    def flag(key: String, default: Boolean) = get(key).map(_.trim.equalsIgnoreCase("true")).getOrElse(default)
    def int(key: String) = get(key).flatMap(v => scala.util.Try(v.trim.toInt).toOption)
    val perCore = int(ShuffleInitialPartitionsPerCore).getOrElse(8)
    val dynamic = flag("spark.dynamicAllocation.enabled", default = false)
    val executors =
      if (dynamic) int("spark.dynamicAllocation.maxExecutors").filter(_ < Int.MaxValue)
      else int("spark.executor.instances")
    val cores = int("spark.executor.cores").getOrElse(1)
    val partitions = int("spark.sql.shuffle.partitions").getOrElse(200)
    if (get(InitialPartitionNum).isDefined) None
    else if (!flag("spark.sql.adaptive.enabled", default = true)) None
    else if (!flag("spark.sql.adaptive.coalescePartitions.enabled", default = true)) None
    else if (perCore <= 0) None
    else
      executors.filter(_ > 0).map(e => e.toLong * cores * perCore).filter(_ > partitions).map(n =>
        math.min(n, 100000L).toInt
      )
  }
}
