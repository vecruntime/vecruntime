# Changelog

All notable changes to vecruntime. The format follows [Keep a Changelog](https://keepachangelog.com/);
the project uses [semantic versioning](https://semver.org/) once it reaches 1.0 -- until then a minor
version may change configuration keys or defaults, always noted here.

## Unreleased

### Added

- The columnar broadcast exchange also carries the nested-loop join's broadcast (#325, slice 2):
  `VectorBroadcastNestedLoopJoinExec` builds its table from the batches, and a Spark nested-loop join
  over the same exchange gets Spark's array of rows, built from the batches on first use.

### Changed

- A hash join on a single INT32/INT64 key whose build values span a small range (at most 10x the
  distinct keys, as Spark's dense `LongHashedRelation`) probes through an array indexed by the key
  instead of the hash table (#546, `spark.vecruntime.join.denseKeys`, default on): 22.7x the hash
  probe on INT32 and 18.1x on INT64 in `JoinProbeBenchmark` (q88's shape: 27% of the keys 1..7,200).
  Joins on other keys keep the hash table unchanged.
- The nested-loop join over Spark's row broadcast reads it into columns once per executor, shared by
  the executor's tasks, instead of once per task.
- Column builders (the joins' build and gather, the sort-merge join's runs) append validity bitmaps and
  BOOL values a 64-bit word at a time instead of a bit at a time (#541): 1.7-2.2x on a nullable INT64
  append, 3.3-4.8x on BOOL.
- `CASE WHEN` blends its branches a 64-row word at a time (#541): each branch takes the undecided rows
  its mask wins as one word, validity and BOOL values are whole words, fixed-width values one bulk
  copy when a branch takes all 64 rows. 8-15x on INT64, 51-80x on BOOL.
- Adapting a Spark column turns its null bytes into the validity bitmap eight bytes per read and one
  bitmap word per 64 rows, straight into the bitmap (#541): 1.6-3.7x on a 4096-row BIGINT vector.

### Added

- A columnar broadcast exchange for the hash joins (#325, `spark.vecruntime.exec.broadcastExchange.enabled`,
  default on): `VectorBroadcastExchangeExec` broadcasts the build side's batches as Arrow IPC streams and
  `VectorBroadcastHashJoinExec` builds its table from them, so a broadcast join over one of our plans
  (or a columnar scan) no longer converts the build side to rows and back. A Spark consumer of the same
  exchange (a reused exchange, dynamic partition pruning) still gets Spark's relation, built from the
  batches on first use; dynamic partition pruning keeps its filter.

### Fixed

- A `tinyint`/`smallint` column could not be written by the aggregate's and the join's spills (the narrow
  lanes' column wrapper was not recognised).

## 0.0.3 -- 2026-09-27

Window `RANGE` frames with value offsets, a columnar `range()`, `spark_partition_id()`, an opt-in
Iceberg v3 deletion-vector writer, and the Vector Acceleration tab working with the plugin on
`--packages` / `--jars`. Getting started now uses `--packages` / `--repositories` and shows the
columnar shuffle.

### Added

- An Iceberg v3 deletion-vector writer (#20), off by default (`spark.vecruntime.iceberg.dvWriter.enabled`):
  DELETE, UPDATE and MERGE on format-version-3 merge-on-read tables write their deletes as deletion
  vectors per data file (partitioned tables and repeated deletes included; the insert half through
  Iceberg's own writer; one `RowDelta` commit). The Iceberg-typed code is the new optional
  `vecruntime-iceberg-bridge` module. `CdcMergeRunner --delete-only` measures a delete-only batch.
- `RANGE` window frames with value offsets (`sum(c) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 5
  PRECEDING AND CURRENT ROW)`, `RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING`, an unbounded side with an
  offset on the other) for `sum`/`avg`/`count`/`min`/`max` over one integral or date order key, `ASC` or
  `DESC`, either null ordering -- the last residual of #58. New `WindowFrameKernels` (two-pointer frame
  bounds replaying Spark's `SlidingWindowFunctionFrame`, frame aggregates over primitive arrays) with
  scalar twins in `ScalarReference`, a `WindowFrameBenchmark`, and the peer-bounded `RANGE` frames
  without an offset (`CURRENT ROW AND UNBOUNDED FOLLOWING`, `CURRENT ROW AND CURRENT ROW`) on the row
  path. Fallback reasons now name the key or input type (`RANGE offsets over a decimal(12,2) order key
  not supported`) instead of `RANGE frames with value offsets`.
- `VectorRangeExec`, a columnar replacement for Spark's `RangeExec` (`spark.range(...)`, the `range()`
  table-valued function): Spark's rows in Spark's partitions (the same split, the same clamping at the
  `Long` bounds, the same `outputOrdering` / `outputPartitioning`), written into native INT64 batches
  by a Vector API kernel (`SequenceKernels.range`) -- one reused vector per task, nothing allocated per
  batch -- so the filter, projection and partial aggregate over `range()` are ours from the leaf, where
  over Spark's row leaf they stayed Spark's until the first exchange. `spark.vecruntime.exec.range.enabled`
  (default `true`). Spark's SQL golden suite gains 105 accelerated executions (4219 -> 4324 of 33856;
  26 cases above the previous floor, now recorded).
- `spark_partition_id()` is compiled (`SparkPartitionIdExpr`, a constant INT32 column per task written by the
  new `SequenceKernels.fillInt`), so a projection or filter over it stays columnar.
- `VectorArrowColumnVector.reusable(...)`: an owned column that ignores the per-batch
  `closeIfFreeable()` Spark 4.1's `ColumnarToRowExec` calls (as Spark's own `WritableColumnVector`s do)
  and is freed by `close()`, for producers that refill one vector across batches.

### Changed

- Getting started (web and README) leads with `--packages` / `--repositories` against the `maven-repo`
  branch and shows the columnar shuffle. `--sun-misc-unsafe-memory-access=allow` is now in every
  command: on JDK 25 it is required, not cosmetic (without it Arrow's Netty allocator cannot address
  direct memory and the first columnar operator fails).

### Fixed

- The Vector Acceleration UI tab now serves its CSS and JS with the plugin on `--packages` or `--jars`
  (they were 404: Spark's static handler looks only in Spark's own class loader, so the tab rendered
  unstyled and without its plan DAG).

## 0.0.2 -- 2026-09-26

The first release as **vecruntime** (previously spark-vector); the repository moved to
[vecruntime/vecruntime](https://github.com/vecruntime/vecruntime). TPC-DS 1 TB on AWS Graviton4 with AQE's
defaults for both engines: 1,800.5 s against Apache Spark's 2,207.3 s (1.23x, geometric mean 1.21x).

### Changed (breaking)

- **Renamed everything to vecruntime, a clean break with no aliases or compatibility shims.**
  Anyone loading the plugin, importing the packages, depending on the artifacts, configuring the
  shuffle manager, or setting configuration keys or JVM properties must update; the old names are
  unknown, not accepted. Old → new:
  - Plugin class: `io.sparkvector.spark.VectorPlugin` → `io.vecruntime.spark.VectorPlugin`
    (`--conf spark.plugins=...`).
  - Java/Scala packages: `io.sparkvector.*` → `io.vecruntime.*` (kernels, spark, shuffle, benchmarks,
    sqltests, the Iceberg bridge `io.sparkvector.spark.iceberg` → `io.vecruntime.spark.iceberg`).
  - Maven groupId `io.sparkvector` → `io.github.vecruntime`; artifacts `spark-vector-*` →
    `vecruntime-*` (`vecruntime-parent`, `vecruntime-kernels`, `vecruntime-spark_2.13`,
    `vecruntime-shuffle_2.13`, `vecruntime-benchmarks`, `vecruntime-spark-sql-tests_2.13`); jar names
    follow.
  - Internal package: `org.apache.spark.sql.vector.*` → `org.apache.spark.sql.vecruntime.*` (the
    `Vector*Exec` operators, the shuffle exchange, the UI). It stays inside `org.apache.spark.sql`
    because Spark's `ShuffleManager` and other APIs used here are `private[spark]` / `private[sql]`;
    class names are unchanged.
  - Shuffle manager class: `spark.shuffle.manager=org.apache.spark.sql.vector.shuffle.VectorShuffleManager`
    → `org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager`. **No alias** — the old class is
    gone.
  - Configuration keys: `spark.vector.*` → `spark.vecruntime.*` (every key, e.g.
    `spark.vector.enabled` → `spark.vecruntime.enabled`, `spark.vector.shuffle.*` →
    `spark.vecruntime.shuffle.*`, `spark.vector.exec.*` → `spark.vecruntime.exec.*`). No fallback to
    the old keys.
  - JVM system properties: `sparkvector.*` → `vecruntime.*` (e.g. `sparkvector.agg.interleave` →
    `vecruntime.agg.interleave`, `sparkvector.platform` → `vecruntime.platform`).
  - **Unchanged:** `org.apache.iceberg.*` (Iceberg's package-private APIs) and every Spark / Iceberg /
    Comet name, including `org.apache.spark.sql.vectorized.*` and `spark.sql.parquet.enableVectorizedReader`.

### Changed

- **AQE sees Spark-scale map output sizes for our exchanges** (#511, #514): new keys
  `spark.vecruntime.shuffle.aqe.mapSizeScaling` (default `true`) and
  `spark.vecruntime.shuffle.aqe.sparkCompressionRatio` (default `0`, the uncompressed-bytes ratio). Our
  columnar shuffle is 1.6-4.3x smaller than Spark's for the same rows, and AQE had packed up to twice
  Spark's rows into a task; TPC-DS q67 went from 92 s with a 115 GB spill to 39 s with none.
- **The aggregate spill budget defaults to 1g**, the sort's (`spark.vecruntime.agg.spillThreshold`, #512).
- **Rebalance exchanges (the Iceberg write)**: AQE sizes the partitions by rows (#485), and the
  advisory size is scaled to our shuffle's bytes per row, with measured string bytes (#495, #506;
  `spark.vecruntime.shuffle.rebalance.advisoryScaling`, `spark.vecruntime.shuffle.rebalance.rowSizing`).
- **The shuffle no longer fsyncs its map outputs**, as Spark does not (#496); on the Iceberg CDC MERGE
  that was the last gap to Spark on the scan stage.
- **Shuffle writer**: scatter-based staged flush, warmed kernels, scatter in 64K-row chunks, off by
  default (`spark.vecruntime.shuffle.writer.scatterFlush`, #487, #488, #490).
- **Platform**: `VectorMask.fromLong` masks only on AVX-512; on Graviton's SVE the native compress path
  stays (#484). The cluster image builds for x86-64 or arm64 (#481).

### Added

- Struct columns and struct hash keys in the columnar shuffle (#480).
- `ON true` / `ON false` nested-loop joins and decimal `AVG` over a running window frame stay columnar
  (#513).
- `MERGE INTO`'s table-insert cast is compiled, so `MergeRows` stays columnar (#477).
- Iceberg adapter: int-backed and dictionary-encoded small decimals (#476, #491).
- Ported DataFusion Comet test matrices for expressions, casts, aggregates, joins and windows; the SQL
  golden-suite coverage floor rose to 4,219+ accelerated executions (#497, #500, #501, #503, #507, #510).

### Fixed

- Ordered string compare no longer uses `MemorySegment.mismatch`, which deoptimised in a loop on q67
  (#493).

## 0.0.1 -- 2026-09-24

The first preview release: the plugin as measured on the 1 TB TPC-DS campaign
(`docs/results.md`), under the Apache License 2.0.

### What it does

- Filter, Project, HashAggregate (all four modes, with spilling), Sort (spilling runs), Window,
  Expand, Generate, Union, Limit, Sample, Coalesce and the hash joins (broadcast and shuffled, the
  shuffled one a grace hash join that spills past `spark.vector.join.spillBytes`), sort-merge joins
  re-planned as hash or order-preserving merge joins -- on Arrow-layout batches with the Java Vector
  API, on the JVM, no native code. The operator and expression coverage, with what falls back and why,
  is in `docs/operators.md` and `docs/expressions.md`; the summary table is in the README.
- A columnar shuffle (`vecruntime-shuffle`): Arrow IPC record batches per reduce partition,
  zstd-compressed, served between executors over Arrow Flight or through Spark's block transfer
  (`docs/flight-shuffle.md`).
- Input from Spark's vectorized Parquet reader, from Comet's native Parquet and Iceberg scans in
  scan-only mode (zero copy), and from Iceberg's vectorized reader with merge-on-read deletes
  (v2 positional and equality deletes, v3 deletion vectors) as a selection (`docs/iceberg.md`,
  `docs/comet.md`).
- A Vector Acceleration tab in the Spark UI: per query, which operators converted and why the
  others did not.

### Measured

TPC-DS at 1 TB on EKS, eight 13-core executors with 50 GB each, 103 queries, all accelerated,
results equal to Spark's (q65 ties aside): 2557 s against Spark 4.1.3's 3309 (23% less, faster on 82
of 103) and Comet 1.0's 2514. TPC-H (22 queries) and the Iceberg merge-on-read paths verified against
Spark at SF1 and SF10. Every threshold default (`docs/configuration.md`) was set from these runs.

### Requirements

Spark 4.1.x, Scala 2.13, JDK 25 with `--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED`
on the driver and the executors; Hadoop 3.4.3 client jars in place of Spark's bundled 3.4.2 on JDK 25.
Comet 1.0 and Iceberg 1.11 optional. See "Requirements and known limitations" in the README.

### Known limitations

`ObjectHashAggregateExec` functions, cached tables, nested-type accessors and constructors, Python
UDFs and the Parquet write path fall back to Spark; the window operator does not spill; the Flight
shuffle has no TLS (use `spark.vector.shuffle.backend=block` under `spark.ssl.rpc.enabled`); measured
on x86-64 (AVX-512, AVX2) and Apple silicon (NEON), not yet on Graviton.
