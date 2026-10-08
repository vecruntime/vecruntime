# Changelog

All notable changes to vecruntime. The format follows [Keep a Changelog](https://keepachangelog.com/);
the project uses [semantic versioning](https://semver.org/) once it reaches 1.0 -- until then a minor
version may change configuration keys or defaults, always noted here.

## Unreleased

### Added

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

- `spark.vecruntime.optimizer.factBloomFilter.enabled` defaults to `false`. Full 1 TB runs with `ANALYZE` statistics:
  1197 s off, 1208 s on; the rule helps q49, q51, q28 and q50 but still slows q23b, q18, q61 and q13, whose
  creation sides are filtered dimensions that prune little. It stays available to turn on.
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
