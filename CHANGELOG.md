# Changelog

All notable changes to vecruntime. The format follows [Keep a Changelog](https://keepachangelog.com/);
the project uses [semantic versioning](https://semver.org/) once it reaches 1.0 -- until then a minor
version may change configuration keys or defaults, always noted here.

## Unreleased

### Added

- Regression guard that our columnar shuffle and native Iceberg scan both preserve the file count of a row-level
  write (`RebalanceSkewSplitSuite`, iceberg-bridge): the six Iceberg 1.12 `testSkew{Delete,Update,Merge}`
  scenarios (copy-on-write and merge-on-read) run with our features off and on with a tiny Iceberg advisory
  partition size (100) and `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=true` and both produce
  four files; plus a direct check that the native scan keeps one Spark partition per Iceberg task group when the
  table sets a high `read.split.open-file-cost`.
- The native Iceberg scan prunes data files by a broadcast hash join's RUNTIME filters before opening them
  (#iceberg-file-pruning, `spark.vecruntime.scan.nativeIceberg.runtimeFilePruning.enabled`, on by default). Static
  pushed filters are already pruned by Iceberg's planning, so those files are never opened; the runtime filters
  (#610 -- an IN set or an integer-key range from the broadcast build side, known only after it runs) were
  previously only ANDed into the per-file Parquet row-group statistics filter, so every data file was still opened
  (a HEAD + footer read each) and only its row groups skipped. Iceberg's own runtime filtering prunes by partition
  values only, never by column bounds. Now the runtime filters are built into an Iceberg `Expression` and the scan
  re-plans on the driver -- same snapshot, the scan's residual AND the runtime expression (Iceberg strips
  per-file bounds from its task files) -- and the files Iceberg's `InclusiveMetricsEvaluator` says
  cannot match are dropped before partitions are built, so they are never opened. Delete files (position deletes,
  deletion vectors, equality deletes) and v3 row lineage are safe to prune -- deletes only remove rows and lineage
  is per-file. A file under metrics mode `none` is kept. The re-plan reads the snapshot's manifests again (no data
  file is opened). New metric `numFilesPrunedByRuntimeFilter`; `numFiles` still counts files read.

- Grouped aggregate state in the native pool (#941, `spark.vecruntime.agg.nativeState`, on by default): past 16,384
  groups, the long sum, count and 128-bit decimal sum accumulators keep their state in one pool block each, a group's
  fields side by side, charged to the task in the operators' memory mode, instead of `long[]` on the heap. At 10 TB
  (26g heap, two full 99-query rounds each way) the run is 2,307 s against 2,328 s, GC -9.5%, q78's spill 613 GiB
  against 760 GiB; on the heaviest queries after inlining the decimal-sum loop, -2.5% (q78 -10.8 s).
- The native Iceberg scan reads the v3 merge-on-read metadata columns `_partition`, `_row_id` and
  `_last_updated_sequence_number` (#20 M3a), so a scan projecting them stays on our operators instead of falling
  back to Iceberg's reader (and, for an UPDATE's delete side over a map column, to rows). `_partition` is the
  per-file struct of the table's unified partition type (null on an unpartitioned table, as Iceberg returns it),
  from `PartitionUtil.constantsMap`; `_row_id` and `_last_updated_sequence_number` are the file's own column where
  it materialises them (copy-on-write, compaction), else `firstRowId + _pos` / `dataSequenceNumber`. Other metadata
  columns (e.g. `_deleted`) still keep Iceberg's reader.

- The native Parquet scan's read-ahead in the task's memory budget (#915): the compressed pages read ahead and the
  batches decode-ahead queues are held in Spark execution memory, granted only from free memory, so read-ahead
  narrows its window when the operators above it need the pool instead of making them spill. The byte caps
  (`prefetchMaxBytes`, `prefetchExecutorMaxBytes`) stay as the upper bound. Executor metrics
  `scanReadAheadHeldBytes`, `scanReadAheadPeakBytes`, `scanReadAheadRefusals`.
- Deeper read-ahead by default (#915): `prefetchFiles` 6 -> 12, `prefetchRowGroups` 2 -> 4, `decodeAhead` 0 -> 2.
  At 10 TB the full TPC-DS run went from 2,404 s to 2,329 s (-3.1%), with the byte caps kept as the upper bound.
- Arrow buffers on the native pool (#869, `spark.vecruntime.arrow.nativePool.enabled`, on by default): the Flight
  shuffle's buffers and the operators' output vectors are reused blocks of the same pool as the operators', not a
  `malloc` and `free` each. The pool's counters are striped, and an optional per-thread cache
  (`spark.vecruntime.nativePool.threadCache.depth` / `.maxBlockBytes`) is off by default.
- Native pool accounting (#869, `spark.vecruntime.memory.poolAccounting.enabled`, on by default): every block a
  task takes from `NativePool` is held in that task's Spark execution memory through one account, which the sort's
  and the grace hash join's reservations share, so a block is counted once and an allocation no operator reserved
  for is still in the budget. Executor metrics `nativePoolInUseBytes`, `nativePoolPeakBytes`,
  `nativePoolRetainedBytes`, `nativePoolOverdraftBytes` (charged past what the memory manager granted) and
  `nativePoolLeakedBytes` (still open when the task ended).
- `DistinctBelowSemiJoin` (#840): a left semi join's right side too large to broadcast is reduced to its
  distinct join columns, one aggregate per task, before the join's exchange
  (`spark.vecruntime.optimizer.distinctBelowSemiJoin.enabled`, default on). TPC-DS q14's `INTERSECT`s
  semi-join 8.6 G and 4.3 G sales rows at 10 TB on three item columns with a few thousand values.
- The same for left anti joins -- `NOT EXISTS`, `EXCEPT`, `NOT IN` (#904,
  `spark.vecruntime.optimizer.distinctBelowAntiJoin.enabled`, default on).

- Columnar Parquet writer (#64), behind `spark.vecruntime.write.columnar.enabled` (default off):
  `VectorWriteFilesExec` writes Parquet writes of flat types -- unpartitioned, partitioned and bucketed, with
  concurrent output writers too --
  straight from our batches. **It writes Parquet v2** (data page v2; `RLE_DICTIONARY`, falling back to
  `DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY` or `PLAIN`). The encoder lives in a new Spark-free
  `parquet-writer` module, tested against parquet-java's v2 writer; `DELTA_BINARY_PACKED` blocks use the
  Vector API. Nested types and Iceberg writes are #842. See `docs/parquet-writer.md`.
- JMH `ParquetWriteBenchmark` and `DeltaEncodeBenchmark`.
- `ColumnarParquetWriter.footer()`: the footer the writer wrote, after close (#900, #901).

- Native Parquet late materialization is now decided automatically per scan and per row group from filter
  selectivity (#962). `spark.vecruntime.scan.nativeParquet.lateMaterialization` gains a new default `auto`
  (`off`/`on`/`false`/`true` still force it off/on): at plan time the decode filter is attached only when the
  filter columns are a small fraction of the projected bytes (byte-width weighted; ceiling
  `auto.maxFilterBytesFraction`), and the reader then late-materializes a row group only when its estimated
  surviving fraction (column statistics, dictionary pages for equality/IN, bloom filters, and the observed
  selectivity of earlier row groups of the same file, with hysteresis) is at or below `auto.lateFraction`
  (two-pass escalation below `auto.twoPassFraction`), decoding eagerly otherwise. New metrics
  `numRowGroupsLate`, `numRowGroupsEager`, `lateSelectivityPpm`; `EXPLAIN` shows `DecodeFilter(auto|on)`. Locally
  (4M rows) a selective wide-projection query runs ~34% faster than `off` while `auto` matches `off` on a
  q24b-like non-selective query where the global `on` is +12% -- the regression (#901) that kept the feature off.

### Changed

- `spark.vecruntime.scan.nativeParquet.lateMaterialization` default changed from `false` to `auto` (#962); the
  key now accepts `off`/`on`/`auto` as well as the legacy `false`/`true`. `auto` keeps the eager path except
  where a filter is selective on a wide projection, so it is safe to leave on by default.
### Fixed

- The copy-on-write UPDATE / MERGE column-chunk copy writer (#901 M4b) now writes a rewritten row group's column
  chunks in **schema order**, interleaving copied and re-encoded chunks, instead of "all copied columns first,
  then all re-encoded columns". A file whose UPDATE changed a schema-later column had its chunks physically out of
  schema order; parquet-java's column-index-filtered read path -- which the engine's own sampling / sort scans use
  during the next copy-on-write rewrite -- builds a row group's page store assuming schema-ordered chunks and
  dropped the physically-first column, so read-back failed with `column ... is not in the store`. Surfaced by
  Apache Iceberg 1.12's own `TestCopyOnWriteUpdate` subquery cases run with the plugin and writers on; regression
  test `CowUpdateSubqueryReadbackSuite` (two sequential copy-on-write UPDATEs on a tiny table). The copy-path file
  is now byte-order-identical to one written column by column, which the reader handles everywhere.
- The native Iceberg scan no longer re-packs Iceberg's task groups with Spark's file-scan heuristics, which could
  merge groups Iceberg deliberately kept separate and collapse the output-file count of a row-level write. Iceberg
  plans its task groups honouring `read.split.target-size`, `read.split.open-file-cost` and
  `read.split.planning-lookback`; `VectorIcebergRDD.packed` was re-grouping them by `spark.sql.files.maxPartitionBytes`
  / `openCostInBytes` / `minPartitionNum`, so on a small cluster (or `files.minPartitionNum` low) it merged a table's
  per-file groups down to the cluster's parallelism. A copy-on-write / merge-on-read DELETE, UPDATE or MERGE on a
  table with a high `read.split.open-file-cost` (one file per group) then wrote fewer files than Spark's own writers
  -- the Iceberg 1.12 `TestCopyOnWrite/MergeOnRead#testSkew*` cases read `added-data-files` 2 where 4 was expected.
  Packing now weighs each group by Iceberg's own open-file-cost and merges only up to Iceberg's own target split size,
  so it never crosses a boundary Iceberg itself kept (a high open-file-cost keeps one partition per group); for an
  ordinary table, whose groups Iceberg has already packed to the target size, it is a no-op. The split size and
  open-file cost are read from the Iceberg scan's OWN `SparkReadConf` (reflectively, via the scan object's private
  `readConf`), so a PER-SCAN read option (`spark.read().option("split-size"|"file-open-cost", ...)`, as the bin-pack
  rewrite runner passes them) overrides the table property exactly as Iceberg resolves it -- a prior version read
  only the table property and diverged from Iceberg's grouping when a read option was in force (e.g. the bin-pack
  rewrite of mixed-size files after the copy runner declines a group that needs splitting).

### Changed

- **Iceberg defaults on** (owner, 2026-10-23): the native Iceberg scan (`spark.vecruntime.scan.nativeIceberg.enabled`),
  the columnar Iceberg writer (`spark.vecruntime.iceberg.writer.enabled`) and the columnar merge-on-read writer --
  deletion vectors on v3, position deletes on v2 (`spark.vecruntime.iceberg.dvWriter.enabled`) -- now default to
  `true`, and so does the columnar Parquet writer (`spark.vecruntime.write.columnar.enabled`, #64), which writes
  Parquet v2 data pages. Each still declines anything it does not take to Iceberg's / Spark's own path with a
  printed reason; set the key to `false` to restore the previous behaviour.

- One Spark-free `parquet` module (`vecruntime-parquet_<spark line>`, #900) replaces `parquet-writer` and takes
  the page decoders out of `kernels`: `io.vecruntime.kernels.parquet` is now `io.vecruntime.parquet.codec`, beside
  the writer's `io.vecruntime.parquet.write`. The encoders and decoders share their `DELTA_BINARY_PACKED` and
  RLE / bit-packed hybrid framing (`ParquetFraming`). The plugin jar shades `kernels` and `parquet`; code that
  imported the decoders from `kernels` imports them from the new package.
- The native scan's structural readers are Spark-free too (#900): `io.vecruntime.parquet.read` holds the leaf
  reader (`LeafColumnReader`, driven by a `LeafReadSpec` the adapter builds once per column instead of Spark type
  checks), the struct, list / map, nested-list and tree readers, and the `ColumnReader` contract; they write Arrow
  vectors through `ArrowWrite` and take the calendar rebase as a `CalendarRebase`. The Spark classes keep their
  names as thin subclasses (the schema walk from the Spark type and the column wrappers). The leaf reader no longer
  builds a Spark column wrapper it threw away on every batch.

- The native Parquet scan no longer filters pages by the column index by default (#873):
  `parquet.filter.columnindex.enabled` now defaults to `false` in `VectorParquetScanExec`. On the classic S3A
  stream every selected page was a separate request; at 1 TB TPC-DS (x86, 9 x m5.4xlarge) q75, q95, q85, q80
  and q44 took 100.2 s with it on and 73.2 s off (75.4 s before #748 turned it on). Set the key to `true` to turn it
  back on. Range merging (#834) is what would make it pay.
- The benchmark image carries `arrow-memory-unsafe` (Spark's Arrow version, 18.3.0) and the Kubernetes manifests
  run the executors with `-Darrow.allocation.manager.type=Unsafe` (#894; `ARROW_ALLOCATOR=` for Netty). Arrow's
  buffers leave `-XX:MaxDirectMemorySize`, which then holds only the Flight transport's: TPC-DS 10 TB passed with a
  6 GB direct cap at Netty's speed, where Arrow on Netty needed about 10 GB. Older images lack the jar: render
  their runs with `ARROW_ALLOCATOR=`.

### Fixed

- The native Parquet scan's parallel row-group decode (#606) no longer leaks Arrow buffers when a task ends early
  (a `LIMIT`, or a failure), fixing the intermittent `ParallelDecodeSuite` "a LIMIT that ends the task early leaks
  no vectors" failure on CI, where the whole `spark` module runs in one JVM (#981). The first fix stopped
  `close()` from `cancel(true)`-ing running helper futures; this one removes the remaining leak path.
  On close the iterator stopped its helper pool with `shutdownNow()`, which interrupts the running decode threads.
  A helper decodes pure CPU over already-fetched pages and does not poll for interruption, so the interrupt did not
  stop it cleanly -- it only made a later buffer allocation inside `readBatch` throw once the flag was set, and
  that throw escaped with buffers already taken from the job's child allocator but not yet owned by a reader's
  pool, which the decode's own cleanup could not free. The iterator now stops the pool with `shutdown()` (no
  interrupt) and drains every in-flight job to completion before closing the lead, so each job's child allocator is
  released and closed exactly once.

## 0.0.9 -- 2026-10-19

Iceberg tables are read by the native Parquet reader over Iceberg's own planning, with position deletes,
deletion vectors, equality deletes and metadata columns, and our planner rules (dynamic partition pruning,
the fact bloom filter, merged scalar subqueries, runtime filters) now apply to Iceberg scans. At TPC-DS 3 TB
on 12 x r8gd.12xlarge (2026-10-18, one pass), the Iceberg copy ran in 985 s against 995 s for Parquet, after
1,064 s against 938 s in the previous round. The native Parquet reader decodes the
remaining file kinds it used to hand to Spark's reader (unsigned ints, wide decimals from ints, intervals,
TIME, unshredded and shredded VARIANT) and passes the parquet-testing and Hardwood conformance files. The
benchmarks now read S3 through S3A's classic stream. Fixes a native memory leak of zstd contexts that
OOM-killed executors at 1 TB.

### Added

- Native Parquet reads over Iceberg's planning (`VectorIcebergScanExec`, #796), Iceberg 1.12.0 (#795):
  position deletes and v3 deletion vectors (#797), equality deletes (#798), the metadata columns `_file`,
  `_pos` and `_spec_id` (#799), and the filter above the scan pushed by field id for row-group and page
  skipping (#800).
- The native Parquet reader decodes unsigned ints, wide decimals stored as ints, `BYTE_STREAM_SPLIT` on
  `FIXED_LEN_BYTE_ARRAY` and intervals (#814); existence defaults and all-missing structs, native `TIME` and
  unshredded and shredded `VARIANT` with pushed extraction, and passes every parquet-testing and Hardwood
  conformance file (#824, landing #815, #809, #817 and #818; closes #808, #787, #750).
- Two-pass late materialization (`spark.vecruntime.scan.nativeParquet.lateMaterialization.twoPass`, off by
  default) reads the decode filter's columns first and skips the I/O of pages with no surviving row (#822).
- Column-index page filtering for lists and maps, and a pages-skipped metric (#811).

### Changed

- Our planner rules on Iceberg tables: dynamic partition pruning (#802), the fact bloom filter (#803),
  merged filtered scalar subqueries (#801); a small Iceberg join input broadcast by its file bytes (#804) and
  sized by files for runtime filters (#807); no file-byte broadcast for an input a rollup groups by (#813);
  broadcast-join runtime filters reach the native Iceberg scan (#820); Iceberg task groups packed as Spark
  packs file splits (#821); no fact bloom filter from a composite input read again (#823, q14a); Spark's
  runtime filters dropped where our Iceberg broadcast hints remove their shuffle premise (#827, q7/q26/q27).
- The native Parquet reader reads data pages in place, v2 sections apart (#805), reuses page-decompress
  buffers and pools chunk buffers (#810), and reads null bits, ints, longs and strings by address (#819).
- A count-only pass-through forwards batches instead of hashing them, removing `CountBelowJoin`'s cost on
  a side that does not reduce (#794).
- The benchmarks read S3 through S3A's classic input stream (`fs.s3a.input.stream.type=classic`) and Iceberg's
  own `S3FileIO` stream (`s3.analytics-accelerator.enabled=false`), not the S3 Analytics Accelerator, for every
  engine (#835). With the Accelerator 1.3.1 every stream open of an executor waits behind one HEAD at a time
  (`MetadataStore.asyncGet` is `synchronized` around the HEAD) and its shared caches grow over the app
  (awslabs/analytics-accelerator-s3#370), so a long run slows down as it goes: TPC-DS 1 TB on 7 x 16-core
  Graviton 4 executors, 846.4 s on the Accelerator against 664.4 s on the classic stream, the same rows.
  Results published before this change ran on the Accelerator. #834 tracks porting its small-file prefetch
  and range merging into the native scan.

### Fixed

- A copy-on-write `DELETE` / `UPDATE` whose table requests write-side coalescing (AQE `coalescePartitions`
  on plus a large `spark.sql.iceberg.advisory-partition-size`, as Iceberg's own write would use to fuse many
  small shuffle blocks into one file) now honours Iceberg's REBALANCE distribution instead of forcing the
  M4a copy path's `_file` hash, so AQE coalesces the rewrite into a single added data file the way Iceberg
  does (Apache Iceberg 1.12 `TestDelete`/`TestUpdate#testCoalesce{Delete,Update}`, snapshot
  `added-data-files` 1, not one file per source file). The ordinary copy case -- no coalescing, or the small
  default write advisory the copy-unchanged path relies on to keep files separate -- keeps the `_file`
  override, so the byte-copy fast path is unchanged.
- The columnar copy-on-write rewrite writer now stamps the table's sort order id on the data files it writes
  (`IcebergRewriteFileTaskWriter`), as Iceberg's `SparkWrite` does, instead of leaving them unsorted
  (`sortOrderId` 0). The streaming full-rewrite writer already did this (#964); the copy path matched it only
  after this fix (Apache Iceberg 1.12 `TestCopyOnWrite*#testCopyOnWrite*SetsSortOrderIdOnRewrittenDataFiles`).
- The columnar bin-pack rewrite (#967 M4c) now packs copied row groups to Iceberg's own input split size
  (`RewriteFileGroup.inputSplitSize()`), not the larger max-output roll cap, and declines the byte copy for a
  whole file group when a file holds a row group larger than that split size -- a case Iceberg handles by
  decoding and splitting the file, which a whole-row-group copy cannot reproduce -- so the compacted layout
  matches Iceberg's own `rewrite_data_files` for mixed-size inputs (Apache Iceberg 1.12
  `TestRewriteDataFilesAction#testBinPackCombineMixedFiles`).

### Documentation

- The native reader's current state and limits (#825), how it decompresses each codec (#831), and the Iceberg
  catalog settings that keep per-query planning off S3 (#826).

## 0.0.8 -- 2026-10-16

The native Parquet reader handles nested types; array, map and struct columns ride a nested lane through
sorts, top-N, joins, coalesce, limits, broadcast and our shuffle; collated strings are carried and compared.
A new `CountBelowJoin` rewrite collapses a join input that only feeds grouping columns, and AQE starts from
more shuffle partitions so large aggregates stop spilling. TPC-DS 3 TB on 12 x r8gd.12xlarge (data-on-eks
layout, 200 shuffle partitions), 2026-10-16, main `fac37478`: 1,118.6 s, 2.00x Gluten + Velox 1.6.0
(2,239.9 s) and 2.21x DataFusion Comet 0.16.0 (2,467.5 s). q24a 109.0 -> 45.6 s; starting AQE from 8
partitions per core (#790), q78 64.6 -> 46.7 s and q67 56.8 -> 33.3 s at 200 partitions.

### Added

- The native Parquet reader (`spark.vecruntime.scan.nativeParquet.enabled`, on) reads nested types: any
  nesting of lists, maps and structs, lists of structs, maps of structs, lists of lists at any depth, and
  struct columns of flat types (#736, #739, #740, #741, #742, #744). It also gains `_metadata` columns, the
  row index and `input_file_name()` (#738, #751), schema evolution (missing columns as nulls, type widening,
  #745), Parquet field-id matching with struct fields missing inside lists and maps (#746), INT96 timestamps
  and calendar rebase (#743), collated strings and bucketed scans (#747), and column-index page filtering
  (#748). A struct's string fields read as dictionary ids and its fields read zero-copy, decoding only the
  decode filter's survivors field by field (#753, #754); the strings of lists and maps read as dictionary ids
  (#756).
- Array, map and struct columns ride a nested lane (`VecType.LIST`, `MAP`, `STRUCT`) through the engine
  (#705): our shuffle's hash and range keys and payload (#719, #730), sort payload columns and
  order-preserving array and struct sort keys (#773, #774), the top-N (#777), the shuffled hash join and merge
  join as keys and payload (#768, #769, #770, #775, #778), coalesce (#776), limits (#779), the columnar
  broadcast (#780) and aggregate grouping keys (#766). Nested lanes were added in the kernels first (#718).
- Nested-element expressions over the element lane (#705): element access and `slice` (#720, #725),
  `array_contains`, `array_position`, `array_min`/`array_max` (#721), `exists`/`forall` (#722),
  `transform`, `filter`, `map_keys`/`map_values` and other array results (#723), `aggregate`, `zip_with`,
  `map_filter` and `transform_values` (#728), nested and over-nested-element lambdas and lambdas with nested
  results (#733, #737), double and decimal elements in array comparisons (#731), and wide-decimal elements as
  results and lambda variables (#763).
- Collated strings (`STRING COLLATE UTF8_LCASE`, `UNICODE_CI`, ...) are carried as their bytes on the UTF8
  lane through filters, sorts, joins, unions, limits and our shuffle, and are keyed, compared and aggregated
  on their collation keys (#781, #782): equality, `IN`/`InSet` and range comparisons compile over collation
  keys, and `min`/`max` keep the value with the least/greatest key; collation-aware hashing and operations
  that read characters stay Spark's.
- `BINARY` columns and keys cross our exchange on the UTF8 lane, hashed as Spark hashes binary (#717).
- `FLOAT` rides the `FLOAT64` lane, widened on input and narrowed on output, including as a hash and range
  key across our exchange (#726, #727).
- `TIMESTAMP_NTZ` rides the `INT64` lane, kept columnar through filters, projections, aggregates, joins,
  windows, top-N and our shuffle; zone-dependent expressions over it stay Spark's (#711).
- `CountBelowJoin`, a logical rewrite that collapses a join input that only feeds grouping columns
  (`spark.vecruntime.optimizer.countBelowJoin.enabled`, on by default): a join whose output explodes on a few
  keys (TPC-DS q24a/q24b, `customer_address` joined on `(upper(ca_country), ca_zip)` with only `ca_state`
  read above -- one task produced 367 M rows from 0.9 M at 3 TB) becomes a local pre-aggregate on the join
  keys weighted by `count(*)`, reusing the exchange the join needs. It weights long, double and decimal
  sums up to 17 digits (the product fits 38 digits); wider decimal sums are left alone (#762, #764, #767).
- Arrow Flight shuffle transport tunables (`spark.vecruntime.shuffle.flight.chunkBytes`, `channelsPerPeer`,
  `clientWindow`, `backpressureBytes`); each keeps today's behaviour when unset (#706).

### Changed

- AQE starts from more shuffle partitions (`spark.vecruntime.shuffle.initialPartitionsPerCore`, default 8):
  the driver plugin sets `spark.sql.adaptive.coalescePartitions.initialPartitionNum` to this factor times the
  executor cores when it exceeds `spark.sql.shuffle.partitions`, so large aggregates split into more, smaller
  tasks and stop spilling, while AQE coalesces small stages back down. At 3 TB with 200 partitions q78 spilled
  201 GB and q67 235 GB; starting from 920 partitions both spill nothing (q78 64.6 -> 46.7 s, q67 56.8 ->
  33.3 s). Left alone when the user set the key, when AQE or its coalescing is off, when the core count is
  unknown up front, or when the factor is 0 (#790).
- The join emits repeated probe strings as a dictionary of the probe rows: when a batch's rows repeat a probe
  row at least 4 times on average, the column leaves as a dictionary over the distinct probe rows instead of
  gathering bytes into every output row (q24a joined 0.7 M probe rows into 367 M at 3 TB) (#691).
- The merge join stops at the first matching right run for `LeftSemi`/`LeftAnti`/`ExistenceJoin` with no join
  condition, instead of walking the whole right run per left row (q14a `INTERSECT` spent 182 s in one task at
  3 TB) (#690).
- The runtime bloom-filter probe over Spark's own filter is vectorised and batched (`SparkBloomKernels`):
  it parses the serialised filter once per executor and probes a batch breadth first, no result change (it was
  13-18 % of q24a/q24b executor CPU at 3 TB) (#758).
- Hash aggregation gets a packed multi-key group lookup -- two or more UTF8/INT32/INT64/BOOL keys packed into
  a 128-bit key probed in one cache line, backing off to the general probe above its code width (#760) -- and
  one word per row in the group table's dictionary-entry map (#759). Accumulators collapse their interleaved
  copies into one past 16,384 groups, so a high-cardinality partial aggregate stops reallocating four copies
  (#761).
- `AggregateBelowJoin`'s pre-aggregate is a per-task local aggregate that adds no exchange and gives up when
  the keys do not reduce, judged after 2 batches (`...aggregateBelowJoin.probeRows` default 262144 -> 16384):
  where the keys barely repeat (q47/q57) the former partial-exchange-final plan cost 23.7 s against 6.5-7.2 s
  with the rule off (#707, #710).
- File partitions are planned without building Spark's unused Parquet reader, which broadcast the whole Hadoop
  conf (~2,000 short-lived allocations per planned scan on the driver) for a reader we never run (#699).

### Fixed

- An aggregate with a `FILTER` clause reads a struct input again: its narrowed evaluation context had dropped
  the batch's columns, so `count(DISTINCT 1), ..., count(DISTINCT (a, b))`, which Spark's distinct rewrite turns
  into `FILTER` clauses over a struct, failed with "unsupported Spark type StructType" (golden files `count.sql`
  and `udf/udf-count.sql`) (#793).
- The driver cancels the jobs of a SQL execution that has ended
  (`spark.vecruntime.cancelEndedExecutionJobs`, default on): AQE could submit a scan stage just before a query
  with an empty relation returned, and the orphaned stage held every slot into the next query (q17 idle for
  its first 11 s at 3 TB) (#791). This complements #715's refusal to submit a shuffle stage after its query
  ended (`spark.vecruntime.shuffle.skipEndedExecutionStages`, default on).
- The shuffled hash join spills the overflowing build batch before asking for another: the hold loop called
  `hasNext` once more after the batch that overflowed the budget, and the shuffle reader had already released
  that batch's buffers, so its lanes read as 0-byte segments (q14b failed at 3 TB under a coalesce/`CASE WHEN`
  build key) (#772).
- The native reader's read-ahead gives back the bytes of a failed open: an open that failed, or a task killed
  while waiting, left its reserved size queued, so the executor's in-flight count never returned to zero and
  narrowed every later task's read-ahead (#755).

## 0.0.7 -- 2026-10-09

Spark 4.2 builds, and every artifact now names its Spark line (`vecruntime-spark_4.1_2.13`,
`vecruntime-spark_4.2_2.13`). New plan rewrites, on by default:
- eager aggregation below star joins;
- a runtime bloom filter from a smaller fact table onto a larger one, with run-time evidence and vectorised
  split-block probes;
- run-time removal of redundant grouping keys;
- dynamic partition pruning through aggregates and through a second join key;
- merged filtered aggregates, shared aggregate inputs and existence-only self-joins as aggregates.

Hash aggregation keeps one or two integer keys inline, with batched probing on large tables. TPC-DS 1 TB on
x86, 2026-10-08: 1,202 s, 2.61x Spark (3,132 s) and 1.63x Comet 1.0.0 (`docs/results.md`).

### Added

- Spark 4.2 builds (`-Pspark-4.2`, #639): the profile switches Spark (4.2.0) and every version pinned to its
  line (Scala 2.13.18, Arrow 19.0.0, Parquet 1.17.0, Hadoop 3.5.0, netty 4.2.13, protobuf 4.33.5), and the spark
  module adds `src/main/spark-<line>` and `src/test/spark-<line>`. What differs is in one `SparkShims` object
  per line: the sample seed (optional in 4.2), Spark 4.2's fdlibm `asinh` / `acosh` (new kernel variants, bit
  for bit), and 4.2's own merging of differently filtered scalar subqueries (`MergeSubplans`), which
  `MergeFilteredAggregates` does on 4.1. Comet and Iceberg stay 4.1-only until they publish 4.2 artifacts.
- Every artifact names its Spark line: `vecruntime-kernels_<line>`, `vecruntime-spark_<line>_2.13`,
  `vecruntime-shuffle_<line>_2.13` (and `vecruntime-benchmarks_<line>`, `vecruntime-iceberg-bridge_<line>_2.13`,
  `vecruntime-spark-sql-tests_<line>_2.13`), with `<line>` 4.1 or 4.2 (#639).
- Redundant grouping keys removed at run time (`spark.vecruntime.optimizer.removeRedundantGroupKeys.enabled`, on):
  when a built broadcast's single integral join key turns out unique and is grouped by (or its equal stream key
  is), the aggregate stops grouping by the broadcast side's other columns -- q23a/b's `substr(i_item_desc, 1, 30)`
  key (#635, EMR's `AQERemoveRedundantGroupKeys`).

- `FactBloomFilter` run-time evidence (`spark.vecruntime.optimizer.factBloomFilter.runtimeEvidence`, on): the
  filter's build counts the creation side's actual rows and declines -- a null filter that keeps every row --
  when they are not `sizeRatio` times fewer than the application side's (#659).
- `FactBloomFilter`'s build reads the join's own creation-side exchange
  (`spark.vecruntime.optimizer.factBloomFilter.shareExchange`, on, AQE only): the creation side is no longer
  scanned a second time for the filter (#659).
- `FactBloomFilter` transitive reduction (`spark.vecruntime.optimizer.factBloomFilter.transitive`, on): a selective
  broadcast dimension above a shuffle join reduces that join's input first, which then gets its filter (q93, q50).
  `FactBloomFilter` is on by default again, with run-time evidence (#659).
- `FactBloomFilter`'s partitioned filters use split-block sub-filters (one cache line read per probe) instead of
  Spark's `BloomFilterImpl`, whose scattered reads made q93's 2.9G-row probe cost more than it saved (#659).
- The partitioned bloom filter's probe is vectorised and batched (`BloomKernels.probe`, #664): the hashes are
  remixed with 512-bit long vectors and each block is tested in one 256-bit op, with no branch per key. In JMH on
  an AVX-512 host it is 2.7 to 4 times the per-key probe. On Graviton 4 (128-bit NEON/SVE) each block is tested as
  two 128-bit halves, 1.5 to 2.2 times the per-key probe; without vectors it falls back to branch-free scalar code.
- `COALESCE(c, <boolean literal>)` over a boolean `c` evaluates `c` once; the general path evaluated each operand
  twice, which doubled the cost of `FactBloomFilter`'s probe `coalesce(might_contain(...), true)` (#659).
- `FactBloomFilter`'s partitioned filters have a power-of-two number of sub-filters, so the probe picks one with a
  mask instead of a long division per row (`floorMod` was 9.6 % of q93's executor CPU at 1 TB, #659).
- A logical rewrite that computes a substring or length of a join's smaller side below the join
  (`spark.vecruntime.optimizer.narrowBelowJoin.enabled`, on by default): the join carries the short result instead
  of the whole string. TPC-DS q23a's `substr(i_item_desc, 1, 30)` was computed on ~1.66G joined rows at 1 TB, each
  gathering the full description; it now runs once per item row (#635).
- Eager aggregation below star joins (`spark.vecruntime.optimizer.aggregateBelowJoin.enabled`, on by default,
  #657): an aggregate of `sum`/`count`/`min`/`max` over one fact table, grouped by the other join sides' columns,
  first aggregates the fact by its join keys before the inner joins, when statistics show the keys reduce the
  fact's rows at least `spark.vecruntime.optimizer.aggregateBelowJoin.minReduction` (4) times.
- A runtime bloom filter from a smaller fact table onto a larger one it joins
  (`spark.vecruntime.optimizer.factBloomFilter.enabled`, on by default): a shuffle join whose filtered side is at
  least 10x larger (estimated bytes) than the side the filter is built from gets
  `might_contain(bloom, xxhash64(key))` on the larger side. TPC-DS q93 filters `store_sales` by `store_returns`;
  Spark's own runtime filter only builds one from a selectively-filtered scan, and EMR Serverless does this.
- A logical rewrite for existence-only self-joins (`spark.vecruntime.optimizer.selfJoinToAggregate.enabled`, on by
  default): `t1 JOIN t2 ON t1.k = t2.k AND t1.v <> t2.v`, when it is only read for which keys exist (under `IN` /
  `EXISTS`, or the build side of a semi / anti join), becomes `GROUP BY k HAVING min(v) <> max(v)`. TPC-DS q95's
  `ws_wh` CTE is that join; EMR Serverless rewrites it the same way.
- A logical rewrite that computes global aggregates over the same data with different filters in one pass
  (`spark.vecruntime.optimizer.mergeFilteredAggregates.enabled`, on by default): cross-joined single-row
  aggregates (TPC-DS q28, q88, q90) and uncorrelated scalar subqueries (q9) whose inputs differ only in their
  `WHERE`s become one aggregate over the union of the filters, each aggregate keeping its own rows through
  `FILTER (WHERE ...)` (a `DISTINCT` one through `IF(p, x, NULL)`). q88 read `store_sales` eight times; EMR
  Serverless merges the same way.
- A logical rewrite that lets two uses of the same aggregate subquery share their input
  (`spark.vecruntime.optimizer.sharedAggregateInputs.enabled`, on by default): when the copies differ only by an
  inferred `IS NOT NULL` on a grouping key, that predicate moves above the aggregate, where it drops only the null
  group, and the copies plan one scan and one shuffle. TPC-DS q65 and q1 read their fact table once instead of twice.
- Dynamic partition pruning through an aggregate (`spark.vecruntime.optimizer.dppThroughAggregate.enabled`, on by
  default): a fact scan below a `GROUP BY` on a dimension attribute (such as `d_week_seq`) is pruned when that
  grouping key is joined above the aggregate to a filtered relation. The pruning set is the dimension's partition
  keys whose grouping key the filtered relation keeps. TPC-DS q59 and q2 get the pruning EMR Serverless applies;
  Spark's own DPP stops at the aggregate because the key is not a partition column.
- Transitive dynamic partition pruning (`spark.vecruntime.optimizer.transitiveDpp.enabled`, on by default): a
  scan joined to an unfiltered dimension on its partition column and on a second key is pruned, when that
  second key comes from a small filtered relation elsewhere in the join. TPC-DS q72's `inventory` (through
  `d2.d_week_seq = d1.d_week_seq`, `d1` restricted to one year) gets the pruning EMR Serverless applies.

### Fixed

- `AggregateBelowJoin` keeps the fact's partition pruning (#675): the pre-aggregate went between the fact's scan
  and its filters, so the dynamic pruning filter applied to the aggregate's output and the scan read every
  partition (q4 at 1 TB without statistics: 5.05 G rows instead of 1.97 G). It now aggregates the fact's whole
  subtree, its filters included; at 1 TB without statistics q4 -52 %, q11 -49 %, q74 -37 %, q23a -37 %, q23b -35 %
  with the rule allowed to fire.
- The hash tables' per-thread scratch (`GroupKeyTable`'s key mirrors, probe keys and dictionary-id maps) is released
  at the end of every task: it no longer keeps the last batch, dictionary and table, nor the arrays a build side or
  a large aggregate grew it to, into the thread's next task and query. After TPC-DS SF10 on `local[8]` the live
  `int[]` left over fell from 665 MB to 21 MB (#667).
- With column statistics, `FactBloomFilter` no longer declines a creation side that is reduced by a filter or a
  join because the key's whole-table distinct count is high: statistics add evidence and never remove that of a
  reduction (TPC-DS q93's returns, reduced by their join to one reason, got no filter with `ANALYZE` statistics).
- `FactBloomFilter`'s probe on a fact scan that already has a filter is added to that filter instead of a second
  one below it: Spark takes partition filters only from the filter directly over the relation, so the scan lost
  its dynamic partition pruning and the stage ran row by row (TPC-DS q48, q13, q61, q18 and q23b at 1 TB).
- TPC-DS q17 at 1 TB failed with `Cannot reserve additional contiguous bytes in the
  vectorized reader (integer overflow)`: the Final aggregate of a runtime bloom filter ran on our operator over
  Spark's `RowToColumnarExec`, which batches by row count the partial filters (one per creation-side map task, up
  to 8 MB each) into a single vector past 2 GB. That aggregate now stays Spark's `ObjectHashAggregateExec` when its
  child is row-based (#647).
- A grouped aggregate with a `FILTER (WHERE ...)` clause failed with `ArrayIndexOutOfBoundsException` when a
  batch in which no row passed the filter brought new groups (seen as `avg(decimal) FILTER (...)` grouped by
  a fine key). The filtered function now sees every batch, with the failing rows cleared.

### Changed

- `AggregateBelowJoin` no longer needs evidence of reduction (#675): `spark.vecruntime.optimizer.aggregateBelowJoin.minReduction`
  defaults to `0` (was `4`) and the new `...aggregateBelowJoin.requireStatistics` to `false`, so the rule fires on
  structure, with or without statistics. Both guards came from a 1 TB measurement whose slowdown was lost partition
  pruning (fixed above); at 1 TB they declined only queries the rule speeds up. Without statistics q4 -52 %, q11
  -49 %, q74 -37 %, q23a -37 %, q23b -35 % (nine queries -23.7 %); with statistics q4 -50 %, q11 -48 %, q74 -35 %,
  q23a -12 %, q23b -13 % (-14.0 %); identical results.

- The Iceberg suites moved to `spark/src/test/iceberg`, added only by `-Piceberg`: a build without it now
  compiles its tests (#639).

- `spark.vecruntime.optimizer.factBloomFilter.enabled` is on by default. It was off for a while during this
  cycle, after full 1 TB runs with `ANALYZE` statistics measured 1197 s off and 1208 s on; it was turned back
  on once the run-time evidence (#659) let it decline filters that do not prune.
- Hash aggregation keeps one or two INT32 keys, or one INT64 key, inline in `GroupKeyTable`'s slot array. Past
  64 K slots it probes in batches of 16 rows, and past 128 K slots it grows by four (#681). At 1 TB, without
  statistics: q23a -8 %, q23b -6 %, nine high-cardinality queries -1.5 %, identical results.
- `FactBloomFilter` builds a filter for many keys partitioned by hash bucket: the keys are shuffled by
  `pmod(xxhash64(key), B)`, one sub-filter per bucket is built after the shuffle within Spark's caps, and the probe
  tests its bucket's sub-filter. A single `bloom_filter_agg` for tens of millions of keys was capped at 8 MB and
  saturated. Total size capped by `spark.vecruntime.optimizer.factBloomFilter.maxTotalBits`; larger filters are
  declined (#653).
- `FactBloomFilter` adds a filter only on evidence that it prunes: with distinct-count statistics for both keys
  (Spark `ANALYZE ... FOR COLUMNS`, or Iceberg's Puffin statistics through DSv2), when at most
  `spark.vecruntime.optimizer.factBloomFilter.maxSelectivity` (0.5) of the filtered side can match; without them,
  only from a creation side a filter reduces. It no longer builds filters from whole dimension tables, which pruned
  nothing and delayed the scan (q19, q45, q46, q31, q38, q23b, q64 at 1 TB) (#650).
- `FactBloomFilter` merges its filter in two levels: the creation side's per-task partial filters go to 32
  groups merged in parallel (`spark.vecruntime.optimizer.factBloomFilter.mergeBuckets`), then one small merge ORs the
  group filters. A single task used to merge every partial (q17 at 1 TB: 595 and 801 partials of 8 MB, 44-59 s each).
  The filter is bit-for-bit the same (#646).
- The runtime bloom-filter probe (`BloomProbeExpr`) now skips rows an earlier `AND` conjunct already rejected,
  instead of probing every row of a batch. No result change; it was 13.9% of q24a's executor CPU at 1 TB.
- The runtime bloom-filter probe deserialises its filter once per executor instead of once per task. Each task
  receives the filter's bytes in the plan; the first one builds the `BloomFilter` and the others reuse it. At
  Spark's default 8 MB cap this was small; a filter sized for a large creation side (~128 MB) was read again by
  every task. No result change.

## 0.0.6 -- 2026-10-04

Our own Parquet scan is on by default and reads every flat type (v2 encodings, BOOLEAN, TINYINT/SMALLINT,
TIMESTAMP, wide decimals, FLOAT/BINARY/TIMESTAMP_NTZ); dictionary strings stay dictionaries from the scan
through hash joins, whose probe and build columns leave as deferred views; broadcast build keys prune the
probe scan as runtime filters (on by default); no reduce-side locality wait. Late materialization and
decode-ahead are in, off by default. Several native-scan correctness fixes. TPC-DS 1 TB on x86, four engines
in one session on 2026-10-03: 2.09x Spark (1,415 s against 2,963), Comet 1.0.0 1.51x (`docs/results.md`).

### Fixed

- The native Parquet scan's tasks now prefer the hosts Spark's `FileScanRDD` prefers (#559): up to three holding the most of the task's bytes, `localhost` dropped. They reported no preference at all, which cost HDFS block locality. On S3 nothing changes: S3A reports `localhost`, so both scans have no preference.
- The native Parquet scan returned wrong values for a decimal read with a scale other than the file's (#559): a
  `decimal(15,2)` INT64 column read as `decimal(17,4)` (Spark's decimal widening, which rescales) was decoded
  without the rescale. Each file's decimal annotation must now have the requested scale and at most the requested
  precision; otherwise the file is read by Spark's reader. Affects 0.0.4-0.0.5.
- The native Parquet scan (`spark.vecruntime.scan.nativeParquet.enabled`, off by default) failed a query whose pushed filter is selective inside a row group of a file with column indexes (#559). The error was `page overruns the row group`. parquet-java's `readNextFilteredRowGroup` also drops pages outside the filter's row ranges, and each column's remaining pages start at different rows, while the decoder expects every column's pages to cover the row group back to back. The scan now turns column-index page filtering off and reads whole row groups. Row-group statistics and dictionary pruning still apply, and the Filter above the scan applies the predicate. This affects 0.0.4–0.0.5 on sorted or clustered data.

- The native Parquet scan (`spark.vecruntime.scan.nativeParquet.enabled`, off by default) decoded a decimal
  with precision <= 18 stored as `FIXED_LEN_BYTE_ARRAY` as if it were INT64, and returned wrong values
  without an error (#559). This affects 0.0.4-0.0.5. Spark's legacy writer
  (`spark.sql.parquet.writeLegacyFormat=true`), Hive and Impala store decimals that way. The planner admits
  a column by its Spark type, but the file decides the physical type. Each file now checks every requested
  column's physical type against its lane at open time, and falls over to Spark's reader on a mismatch, as
  it does for an unsupported encoding. The same check keeps an unsigned INT32 (`UINT_32`, read by Spark as a
  bigint) out of the sign-extending INT32-to-INT64 path. The apache/parquet-testing file
  `fixed_length_decimal_legacy.parquet` reproduced it.

### Changed

- Late materialization (#611, still off by default): string columns alone are decoded at the survivors (a fixed-width column is decoded whole, which is cheaper than walking a scattered selection: 1 TB q44 had doubled its executor time); the scan stops filtering for the rest of a row group after two dense batches; scans under `lateMaterialization.minScanBytes` (1 GiB) get no decode filter. `docs/configuration.md` now says when to turn it on.
- `spark.vecruntime.join.runtimeFilters` now defaults to `true` (#610), and the key domain is read straight off the broadcast batches instead of a hash table built on the driver (which cost q61 about 0.4 s at 1 TB). TPC-DS 1 TB, off vs on, two legs each: the twelve longest queries neutral, checksums equal; a fact table clustered on its join key is 2.8x faster on a selective dimension.
- `spark.vecruntime.join.buildDictionaryMax` now defaults to `4096` (was `0`): low-cardinality build-side strings leave hash joins as dictionary ids. TPC-DS 1 TB, 4096 vs 0, two legs each way, checksums equal: fourteen join queries -2.7% (q99 5.5 -> 4.9 s, q29 -5%; q18 +5%, q43 +4%).
- Hash joins emit their build-side columns as deferred views too (#603 step 2; `spark.vecruntime.join.deferredBuild`, default `true`), over the build table and through the build row ids, gathered on first read and composed through later joins. Local TPC-DS SF1 on top of the probe views, two legs each way, checksums equal: q72 3,081 -> 2,045 ms (-34%), q50 -6%, q99 274 -> 261 ms (-5%), q85 -4%, twelve join-heavy queries 7,384 -> 6,277 ms (-15%); none slower.
- Hash joins emit their probe-side columns as deferred views (#603; `spark.vecruntime.join.deferredProbe`, default `true`): the input column plus the matched row ids, gathered on first read. The next join of a chain composes the ids instead of copying, so a column carried through a chain of joins is gathered once, or never. Velox's hash probe does the same with dictionary wrapping. Local TPC-DS SF1, two legs each way, alternating, checksums equal: q72 3,733 -> 3,051 ms (-18%), q99 314 -> 274 ms (-13%), q46 -8%, twelve join-heavy queries 8,171 -> 7,424 ms (-9%); none slower by more than 3%.
- The native Parquet scan keeps dictionary-encoded strings as dictionary vectors (#612; `spark.vecruntime.scan.nativeParquet.dictionaryStrings`, default `true`). Since #609 made the native scan the default, its string columns were resolved into flat bytes, which lost the dictionary paths Spark's scan path had: filters compacting ids, the aggregate grouping on ids (#377), string expressions once per entry. Now each row group's dictionary is one shared Arrow vector and its batches carry ids; a chunk that falls back from its dictionary mid-way decodes flat from that page on.
- Benchmarks: every configuration (Spark, Comet, ours and the mixed ones) now runs with `spark.locality.wait=0` (#559), in `submit-cluster.sh` and `TpchRunner.Configs`, so all engines are scheduled alike. Spark's reduce tasks prefer the hosts that hold their map output, which piled post-shuffle stages onto one host behind the 3 s wait, the same pile-up our shuffle now avoids (above). TPC-DS 1 TB, Spark 4.1.3, two legs each way, alternating, checksums equal:

  | query | default (s) | wait 0 (s) | change |
  |---|---|---|---|
  | q81 | 19.36 | 7.46 | -61% |
  | q18 | 8.29 | 4.48 | -46% |
  | q30 | 12.82 | 8.49 | -34% |
  | q31 | 11.93 | 8.82 | -26% |
  | q87 | 28.70 | 24.69 | -14% |
  | q95 | 101.79 | 97.89 | -4% |

  Comet was not A/B'd separately; it gets the setting for consistency. Results published before this change (`benchmarks/results/tpcds-sf1000-2026-10-02` and earlier) ran with the default wait.

- **The native Parquet scan is on by default** (#559; `spark.vecruntime.scan.nativeParquet.enabled`). It reads every flat type, passes Spark's SQL golden suite (642/0) and the project's suites, and took the 1 TB TPC-DS run of 2026-10-02 to 1,542 s against Spark's 3,099 and Comet's 1,999. In a session where Comet's scan is active (Comet's plugin or extension registered, `spark.comet.enabled` and `spark.comet.scan.enabled` not false), it stays off, so Comet's reader remains the scan. Setting the key explicitly always wins; `false` restores Spark's vectorized reader under our operators. CI's suites and golden suite now run on the native scan through the default.
- Our shuffle's reduce tasks no longer report preferred locations (#559; `spark.vecruntime.shuffle.reduceLocality.enabled`, default `false`). Spark prefers the hosts holding at least 20% of a reduce partition's map output, and a shuffled join intersects both sides' hosts. At 1 TB that often left one host, so its 13 slots ran a whole post-shuffle stage while the remaining tasks waited out the 3 s locality wait, and the next stage, reading that host's output, repeated it. A reducer still reads its own executor's blocks locally, so locality saves some network fetch, but the wait cost far more. TPC-DS 1 TB, our engine with our reader, two legs each way, alternating:

  | query | locality on (s) | off (s) | change |
  |---|---|---|---|
  | q81 | 13.28 | 4.40 | -67% |
  | q31 | 7.92 | 4.61 | -42% |
  | q87 | 16.03 | 10.11 | -37% |
  | q18 | 6.25 | 4.77 | -24% |
  | q30 | 10.57 | 8.25 | -22% |
  | q95 | 26.25 | 26.91 | +2.5% |

  Checksums are equal in every leg.

- Faster v2 page decoding in the native Parquet scan (#559). JMH, x86 / Graviton4:
  - `BYTE_STREAM_SPLIT` transposes instead of gathering bytes: 3.7x / 2.0x on DOUBLE and INT64. It uses the Vector API
    from 256-bit vectors and SWAR at 128 bits, where the Vector API's byte-to-long widening is not intrinsified
    (`-Dvecruntime.parquet.bssMode=auto`).
  - `DELTA_BINARY_PACKED` unpacks and sums whole miniblocks straight into the caller's array: 1.17-1.23x / 1.09-1.24x.
  - `DELTA_BYTE_ARRAY` rebuilds values straight into the batch's bytes, without the per-page rebuild buffer:
    1.34x / 1.49x.

  The results are unchanged: `ByteStreamSplitKernelsTest` checks every variant against its scalar reference at
  128, 256 and 512 bits. `-Dvecruntime.parquet.bssMode` and `-Dvecruntime.parquet.deltaScan` select the
  alternatives for A/B runs.

### Added

- Late materialization in the native Parquet scan (#611; `spark.vecruntime.scan.nativeParquet.lateMaterialization`, default `false`). The column decoder can skip rows per encoding (a cursor bump, a length walk, ids or values dropped, the DELTA_BYTE_ARRAY prefix chain rebuilt), in the stream or in place within a batch; a filter over the scan hands it its condition, and the scan decodes the condition's columns first and the rest only for what survives: a batch with no survivor is skipped whole, a sparse one (at most 5%) is decoded at its survivors. Off: local TPC-DS SF1 on twelve scan-heavy queries was neutral (+1.3%).
- Low-cardinality build strings as dictionary ids out of hash joins (#603; `spark.vecruntime.join.buildDictionaryMax`, default `0` = off). A build-side string column with at most that many distinct values is encoded once per build table, and its deferred views gather 4-byte ids over the shared distinct values, so the aggregate above groups on ids. Local TPC-DS SF1 at 4096, two legs each way, checksums equal: neutral overall (fourteen join queries -0.6%; q43 -8%, q18 -5%, q19 +4%), so it stays off pending a 1 TB measurement.
- Runtime filters from broadcast build keys into the native Parquet scan (#610; `spark.vecruntime.join.runtimeFilters`, default `false`). Inner and left semi broadcast joins hand the scan their streamed keys reach an IN list (at most `runtimeFilters.inMax`, 1024) or an integer range, ANDed with the pushed filters, so row groups without a matching key are skipped. Off: TPC-DS fact keys are not clustered, so it skips nothing, and local SF1 measured 1-5% slower on eleven candidate queries. Row filtering in the scan comes with #611 and reuses this path.
- Decode-ahead in the native Parquet scan (#606; `spark.vecruntime.scan.nativeParquet.decodeAhead`, default `0` = off). A producer thread per task (virtual by default, `…decodeAhead.threads=platform` for platform threads) decodes up to K batches ahead while the task thread runs the operators above the scan; batches come out in the same order. The first batch decodes on the task thread so no class initializer runs on a virtual thread; Spark's fallback reader's recycled batches hand off synchronously. Off until measured.
- FLOAT, BINARY and TIMESTAMP_NTZ columns in the native Parquet scan (#559, option A of the design note). They are decoded on the lane of the same layout and emitted as Spark's Arrow vectors: FLOAT on INT32 (`PLAIN`, dictionary, `BYTE_STREAM_SPLIT`), BINARY on UTF8 (also `FIXED_LEN_BYTE_ARRAY` read as binary), TIMESTAMP_NTZ on INT64. Operators carry them as columns without a lane, so a table with such a column no longer keeps all its other columns on Spark's reader.
- Wide decimals (`decimal(p > 18)`) in the native Parquet scan (#559), and decimals of any precision stored as
  `FIXED_LEN_BYTE_ARRAY` or `BINARY` (Spark's legacy format, Hive, Impala): `PLAIN`, dictionary and
  `DELTA_BYTE_ARRAY` pages decode into the DECIMAL128 lane (or the INT64 lane for p <= 18), as Spark converts them.
  JMH, one 20,000-value `decimal(38)` page: 13.5 pages per ms, the same as parquet-java's reader slicing the bytes.
- TIMESTAMP columns in the native Parquet scan (#559), stored as INT64 `MICROS` or `MILLIS`. `MILLIS` values are scaled to micros with Spark's overflow check. The calendar rebase is resolved per file from its footer, as Spark resolves it: a file that needs a rebase (`LEGACY`, or `EXCEPTION` on a file that is not from Spark 3+) is read by Spark's reader with its own modes, and so is an INT96 file. The plan-level refusal of a non-`CORRECTED` date rebase is gone.
- TINYINT and SMALLINT columns in the native Parquet scan (#559). They are INT32 in Parquet, so they reuse the INT32 decode in every encoding. Each value is narrowed to the declared width while decoding, as Spark's readers narrow it, so an out-of-range `INT(8)` / `INT(16)` value wraps the same way for operators.
- BOOLEAN columns in the native Parquet scan (#559): v1 `PLAIN` (bit-packed) and v2 `RLE` pages, decoded into a bit-packed lane. JMH on x86, one 20,000-value page: `PLAIN` at 837 pages per ms against 10.7 for parquet-java's reader, `RLE` at 212 against 11.2. `RLE` is decoded straight into the bitmap: runs set as bit ranges, bit-packed runs moved 64 bits a step.
- `ParquetTestingCorpusSuite`: 27 files from the Apache Parquet conformance corpus (`apache/parquet-testing`, Apache-2.0, vendored for tests) read column by column against Spark's reader, plus 3 corrupt files that must be refused.
- The native Parquet scan (`spark.vecruntime.scan.nativeParquet.enabled`) decodes `DELTA_BINARY_PACKED`
  INT32 and INT64 columns, #559 slice 2: ints, bigints, dates and decimals with precision <= 18. This is the
  encoding parquet-java writes for those columns when `parquet.writer.version=v2` and the column is not
  dictionary-encoded. Before, such a file fell over to Spark's reader whole. `DeltaBinaryPackedReader` unpacks
  each miniblock through the injected `BytePacker` and resumes inside a miniblock across batches. It is
  checked against a from-scratch encoder, against parquet-java's writers and through Spark with v2 files.
  JMH: 2.0-4.0x the pages per ms of Spark's `VectorizedDeltaBinaryPackedReader`.
- The native Parquet scan also decodes `DELTA_LENGTH_BYTE_ARRAY` strings and `BYTE_STREAM_SPLIT` INT32 /
  INT64 / DOUBLE columns (#559). For `DELTA_LENGTH_BYTE_ARRAY` it decodes the page's lengths through
  `DeltaBinaryPackedReader`, then copies the bytes into the Arrow offsets and data. For `BYTE_STREAM_SPLIT` it
  gathers each value's bytes from the K streams straight into the lane. Spark's vectorized reader rejects
  `BYTE_STREAM_SPLIT` on INT32/INT64 ("Unsupported encoding"), so those files now read with the flag on and
  fail with it off. Checked against parquet-java's writers, and through Spark with v1 and v2 files from a
  test writer that picks the encodings per column, against Spark's row-based reader. JMH, one 20,000-value
  page in 1024-row batches: `BYTE_STREAM_SPLIT` DOUBLE/INT64 8.8x and `DELTA_LENGTH_BYTE_ARRAY` 1.46x the pages
  per ms of parquet-java's value readers.
- The native Parquet scan decodes `DELTA_BYTE_ARRAY` strings (#559). This is what parquet-java writes for v2
  strings without a dictionary, so v2 files written by Spark (`parquet.writer.version=v2`) no longer fall over
  to Spark's reader for their string columns. Each page's values are rebuilt in one sequential pass from the
  prefix lengths and the `DELTA_LENGTH_BYTE_ARRAY` suffixes into a reused buffer, then batched like
  `DELTA_LENGTH_BYTE_ARRAY`. A corrupt page fails, for example on a prefix longer than the previous value. JMH:
  2.0x the pages per ms of parquet-java's `DeltaByteArrayReader` on sorted URL-like keys.

## 0.0.5 -- 2026-10-02

Our own Parquet scan (`spark.vecruntime.scan.nativeParquet.enabled`, default off) with file and
row-group read-ahead; the string-sort deoptimization storm fixed; fewer FFM checks in the group-key and
shuffle-size hot loops; and the benchmark launcher on ACCP, wider S3A read concurrency and compact
object headers. TPC-DS 1 TB on x86 with the native scan on, four engines in one session on
2026-10-02: 2.01x Spark (1,542 s against 3,099), Comet 1.0.0 1.55x (`docs/results.md`).

### Added

- Our own Parquet scan behind `spark.vecruntime.scan.nativeParquet.enabled` (default off), #559 slice 1:
  `VectorParquetScanExec` replaces a supported flat-schema Parquet `FileSourceScanExec` and decodes pages
  straight into our Arrow vectors through `NativeParquetColumnReader` (reused vectors across row groups,
  the parquet-java `BytePacker` injected into the `ColumnChunkDecoder`), one pass, no Spark `ColumnVector`
  in between -- the chain above is ours from the leaf. It reuses Spark's dynamically selected partitions
  (DPP), file splitting, bucketing, pushed data filters (row-group / page skipping) and the required +
  partition schema; batches are `spark.sql.parquet.columnarReaderBatchSize` rows, emitted as offset views
  with no copy. Slice 1 decodes `PLAIN` and dictionary encodings only: a file whose column-chunk metadata
  shows another encoding (`DELTA_*`, `BYTE_STREAM_SPLIT`) falls that file over to Spark's own vectorized
  reader at open time (correct results, no mid-decode failure). An unsupported type, nested column,
  `INT96`, non-`CORRECTED` date/timestamp rebase or a bucketed scan keeps Spark's scan with a recorded
  reason. End-to-end decode of a 4M-row file was at parity with Spark's reader and ~17% faster than the
  production Spark-reader-plus-adapter path (`docs/results.md`, `butterfly-keep`). `VectorParquetScanSuite`
  compares results row-for-row with Spark across type x encoding x page v1/v2 x nulls, several row groups
  and pages, partition columns, a pushed filter and a DPP query; `VectorParquetScanPlanSuite` pins planning.
- `spark.vecruntime.scan.nativeParquet.prefetchFiles` (default `6`) and
  `spark.vecruntime.scan.nativeParquet.prefetchRowGroups` (default `2`), `0` off, capped at 16: how far
  `VectorParquetScanExec` reads ahead (#559/#566). The next N files of a split are opened (status, footer,
  first row group), and the next M row groups of the current file are read, on background threads while the
  task thread decodes; the row-group reads are a chained `CompletableFuture` pipeline, so a file's
  `ParquetFileReader` is used by one thread at a time and in order. At 1 TB TPC-DS (store_sales is ~14.6k
  files of ~7 MB, mostly one row group each) task threads were parked on S3 at least 66% of the time; same
  session, flag on, checksums equal, against no read-ahead (experiment build: N files + one row group
  ahead): N = 2 q88 -27%, q28 -56%, q9 -60%, q44 -50%; N = 6 q28 -57%, q9 -64%, q44 -59% (q88 varied
  57-115 s per iteration at both depths). Memory per task grows by up to N opened files plus
  M row groups (compressed pages).
- Benchmarks: the cluster image ships Amazon Corretto Crypto Provider (#566), and `benchmarks/k8s/render-run.sh`
  turns it on by default (`ACCP=1`; `ACCP=0` for the JDK's own crypto) for the S3 TLS cipher and SigV4
  hashing. The rendered runs also raise the S3A read concurrency the native scan's read-ahead needs:
  `fs.s3a.connection.maximum` 200 -> 1000, `fs.s3a.threads.max` 256, `fs.s3a.max.total.tasks` 128 and the
  Analytics Accelerator's `physicalio.thread.pool.size` 192 (the values every 1 TB #559 A/B round ran with).

### Changed

- Hot loops read heap copies instead of native memory, one bulk move per column (#565). The hash
  aggregate's group-key accessors (`getInt` / `getLong` / `getDouble` / `isNull`) and `toIds`' dictionary
  path read int/long arrays and 64-row validity words mirrored from the batch (#570). The shuffle
  writer's per-batch string-size estimate for AQE map-size scaling moves to the `Utf8Sizes.paddedBytes`
  kernel over reused arrays (#569). Results are unchanged. In q67's executor profile at 1 TB the shuffle
  loop was 16.6% of the FFM liveness/bounds-check samples, and the key accessors and `toIds` about 15%.
- Benchmarks: the launcher (`benchmarks/scripts/submit-cluster.sh`, and through it `benchmarks/k8s/render-run.sh`)
  starts the driver and the executors with `-XX:+UseCompactObjectHeaders` (#578; JEP 519, product on JDK 25,
  the default from JDK 27). The AOT cache scripts (`benchmarks/k8s/aot/aot-env.sh`) build with the same
  flag, because a cache only serves a JVM with the header layout it was built with. A cache built before
  this change is ignored, not an error (`AOTMode` stays auto), until it is retrained.

### Fixed

- String sorts no longer fall into a C2 deoptimization storm (#559). The sort comparators now have their own
  byte comparison (`StringCompareKernels.compareBytesForSort`), whose branch profile is warmed at class
  initialization over every path. Before, a JVM whose first sorts never ran the 8-byte word loop out got
  that exit compiled as an `unstable_if` trap with action `none`. On 1 TB TPC-DS with q67 after the
  scan-heavy queries in one app, 1-3 of 8 executors then ran q67's sort stage at ~2.3x CPU for the rest of
  the app; the deoptimization log showed 411k-832k such traps per executor.

## 0.0.4 -- 2026-09-30

`ObjectHashAggregateExec` for `bloom_filter_agg`, `collect_list` and `collect_set`, with spill; a
columnar broadcast exchange for the hash and nested-loop joins; array, map and struct payloads on a
broadcast build side; a dense-key probe for small-range integer join keys; validity handled a 64-row
word at a time; and the scan adapter decoding dictionary ids in place. TPC-DS 1 TB against Spark:
1.37x on x86 and 1.31x on Graviton4, both measured on 2026-09-30 in one availability zone
(`docs/results.md`).

### Added

- The value-decoding core of a native Parquet scan (#559, slice 1): a dependency-free Java page
  decoder in `kernels` that turns a decompressed Parquet data page straight into Arrow-layout
  `VectorBuffers`, one pass, no Spark `ColumnVector` in between. `RleBitPackingReader` decodes the
  RLE / bit-packed hybrid encoding (definition levels and `RLE_DICTIONARY` ids, bit widths 0..32);
  `ParquetPageDecoder` decodes definition-level nulls, `RLE_DICTIONARY` ids (dictionary decoded into
  the output) and `PLAIN` values for INT32, INT64, DOUBLE, DATE, DECIMAL(p&le;18) over their int32 /
  int64 physical type, and BINARY/UTF8, for data page v1 (length-prefixed inline levels) and v2
  (header-sized level and value slices), flat columns only. Verified by a scalar-oracle kernel suite
  and cross-checked against parquet-java's own `PlainValuesWriter` / `RunLengthBitPackingHybridEncoder`
  in `spark`. The `VectorParquetScanExec` node and the `spark.vecruntime.scan.nativeParquet.enabled`
  planner switch that use it are a following slice; nothing changes in planning yet.
- `ObjectHashAggregateExec` is converted for the object aggregates whose buffer we can carry (#57,
  `spark.vecruntime.exec.objectAggregate.enabled`, default on): `bloom_filter_agg` (the runtime
  filter's build side), `collect_list` and `collect_set`. Spark carries their state between the
  Partial and Final stages as one `BinaryType` column, so we drive Spark's own
  `TypedImperativeAggregate` object per group -- the partial buffer is byte-identical to Spark's (a
  Spark Final or the bloom probe reads it unchanged), `collect_set` dedups and nulls are ignored as
  Spark does, and the `array<T>` / `binary` output is held in Spark's on-heap column vector. All four
  modes, grouped and ungrouped, mixed with ordinary aggregates in one node. Their buffers are counted
  against the task memory budget and spill past `spark.vecruntime.agg.spillThreshold`: a buffer stage
  emits its partial binary buffers and starts over, a Final spills its groups' serialized buffers
  (UTF8-carried through the grace-hash path) and merges them back a bucket at a time, so a grouped
  `collect_*` over many keys is bounded instead of pinning memory (a streaming `Complete` stage, which
  Spark 4.1's batch planner never emits, stays in memory). Other object aggregates (`percentile*`,
  `collect_top_k`, ...) keep the fallback reason.
- A broadcast hash join converts when its build side carries array, map or struct payload columns
  (#547, `spark.vecruntime.join.buildPayload`, default on): they are copied into a row store and read
  as views over the build row ids; keys and the condition still need lanes. Adaptive execution's
  choice of broadcast side no longer decides whether such a join is ours (`postgreSQL/with.sql`).
- The columnar broadcast exchange also carries the nested-loop join's broadcast (#325, slice 2):
  `VectorBroadcastNestedLoopJoinExec` builds its table from the batches, and a Spark nested-loop join
  over the same exchange gets Spark's array of rows, built from the batches on first use.

### Changed

- The scan adapter decodes a dictionary-encoded numeric Parquet column without per-batch staging
  (#551): with off-heap reader vectors (`spark.sql.columnVector.offheap.enabled`) the ids and null
  flags come out of native memory in one bulk `MemorySegment.copy` each, into per-thread scratch
  arrays that are reused, instead of a fresh `int[]`/`byte[]` per column per batch; and for a Parquet
  dictionary -- decoded whole once per column chunk -- the pass for the largest id is skipped on both
  heap kinds. The bulk copy was chosen over reading the native buffers row by row, which JMH favours
  but whose per-row segment checks were not inlined on the executors: at 1 TB it cut wall time a
  further 5-7% on q88/q28/q96. `DictionaryDecodeBenchmark` (4096-row batches, 7,200-entry
  dictionary): 2.4x INT32 / 2.1x INT64 off heap, 2.1x / 1.9x on heap versus main. Output is unchanged.
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
