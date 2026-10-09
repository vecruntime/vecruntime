---
layout: default
title: Running and tuning
---

# Running and tuning

## Running with spark-submit

```bash
spark-submit \
  --conf spark.plugins=io.vecruntime.spark.VectorPlugin \
  --conf spark.driver.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  --conf spark.executor.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  --jars vecruntime-spark_4.1_2.13-0.0.7.jar \
  ...
```

`spark.plugins` registers the session extension automatically; alternatively set
`spark.sql.extensions=io.vecruntime.spark.VectorSparkSessionExtensions`.

Configuration keys (all default to `true` except the last; the complete reference, every
`spark.vecruntime.*` key with its default and unit, is [docs/configuration.md](configuration.html)):

| Key | Meaning |
|---|---|
| `spark.vecruntime.enabled` | main switch |
| `spark.vecruntime.exec.filter.enabled` | convert `FilterExec` |
| `spark.vecruntime.exec.mergeRows.enabled` | convert `MergeRowsExec`, the row-level operator of a `MERGE INTO` (#21), when the join below it is ours |
| `spark.vecruntime.exec.project.enabled` | convert `ProjectExec` |
| `spark.vecruntime.exec.aggregate.enabled` | convert `HashAggregateExec` |
| `spark.vecruntime.exec.aggregate.final.enabled` | also convert Final-mode aggregates (their input is the shuffle) |
| `spark.vecruntime.exec.sort.enabled` | convert `SortExec` over a columnar child (spills past `spark.vecruntime.sort.spillBytes`, #416) |
| `spark.vecruntime.agg.spillThreshold` | hard cap on one grouped aggregate table (default `1g`, the sort's budget, #511; `0` = never spill). Below it the operator acquires its real footprint from Spark's task memory manager as the table grows and acts on a refusal: a partial aggregate emits its table and starts over, a final one spills into hash buckets and merges them one at a time (#363, #367) |
| `spark.vecruntime.agg.spillBuckets` | buckets a final aggregate spills into (default `16`) |
| `spark.vecruntime.agg.passThroughRatio` | a partial aggregate whose full table reduced its input by less than this factor stops aggregating and passes each batch on (default `1.5`, `0` = never; #376) |
| `spark.vecruntime.sort.runRows` | rows per sorted run (default 1048576): the sort orders each run as the partition arrives and k-way merges the runs on output, bounding its scratch to the run (#285) |
| `spark.vecruntime.exec.takeOrdered.enabled` | convert `TakeOrderedAndProjectExec` (`ORDER BY ... LIMIT`) over a columnar child; the per-partition top-N is columnar, the final merge of at most `limit` rows per partition goes through Spark's single-partition shuffle |
| `spark.vecruntime.exec.limit.enabled` | convert `LocalLimitExec` / `GlobalLimitExec` / `CollectLimitExec` over a columnar child (no offset); batches pass through until the boundary, the collect limit's final take goes through Spark's single-partition shuffle |
| `spark.vecruntime.exec.union.enabled` | convert `UnionExec` when at least one child is columnar (row children go through Spark's `RowToColumnarExec`); keep it on with Spark 4.1.3, whose own columnar union concatenates co-partitioned children it reports as partition-aligned |
| `spark.vecruntime.exec.coalesce.enabled` | convert `CoalesceExec` over a columnar child (no shuffle, batches forwarded) |
| `spark.vecruntime.exec.window.enabled` | convert `WindowExec` for `row_number`, `rank`, `dense_rank` and whole-partition aggregates over any child (a row sort below is converted by Spark's transitions) |
| `spark.vecruntime.exec.generate.enabled` | convert `GenerateExec` with `explode`/`posexplode` (and the `_outer` forms) over an array column: the other columns gathered through a repeat index built from the array lengths, the elements copied once per array from Spark's array vector |
| `spark.vecruntime.exec.sample.enabled` | convert `SampleExec` without replacement over a columnar child: Spark's own Bernoulli sequence per partition as a selection bitmap, so a seed returns Spark's rows |
| `spark.vecruntime.exec.localTableScan.enabled` | convert `LocalTableScanExec` (`VALUES`, local relations) into one batch per partition; **off by default** -- nothing to accelerate, it only lets small-table tests run our operators |
| `spark.vecruntime.exec.range.enabled` | convert `RangeExec` (`spark.range`, the `range()` table function) into native INT64 batches -- Spark's rows in Spark's partitions, one reused vector per task -- so the chain above `range()` is ours from the leaf (over Spark's row leaf nothing of ours ran until the first exchange) |
| `spark.vecruntime.exec.expand.enabled` | convert `ExpandExec` (`ROLLUP` / `CUBE` / `GROUPING SETS`, the `count(distinct)` rewrite) over a columnar child: one borrowed-column batch per grouping set, no data copy |
| `spark.vecruntime.exec.broadcastHashJoin.enabled` | convert `BroadcastHashJoinExec` when the streamed side is columnar or an exchange (the build side stays Spark's broadcast) |
| `spark.vecruntime.exec.broadcastNestedLoopJoin.enabled` | convert `BroadcastNestedLoopJoinExec` (non-equi joins) when the streamed side is columnar or an exchange; inner/cross, semi/anti/existence and outer joins with the streamed side preserved |
| `spark.vecruntime.join.maxBuildSize` | build-side budget (bytes or a size string): the broadcast joins convert only for a relation estimated within it (they hold it in memory per task); the shuffled hash join holds its build side in memory up to it and past it splits both sides into buckets on disk (#416, the default of `spark.vecruntime.join.spillBytes`); default 1 GiB, or `spark.memory.offHeap.size / spark.executor.cores` when off-heap is configured |
| `spark.vecruntime.join.spillBytes` | the build bytes a shuffled hash join holds in memory before it splits (#416); default `256m` (measured at 1 TB: the same speed as 1 GiB on the join-heaviest queries, the planner sending what does not fit to the merge join); `0` never splits |
| `spark.vecruntime.join.spillBuckets` | the buckets a split shuffled hash join writes each side into and joins one at a time (#416); default 32 |
| `spark.vecruntime.join.hashMaxBuildSize` | the most a sort-merge join's build side may weigh per task, by statistics, for `mode=auto` to make it the hash join (#416): within `spark.vecruntime.join.spillBytes` it builds in memory, within this cap it splits into buckets once, past it -- or without an estimate -- the merge join over the spilling sort takes it, its memory bounded whatever the inputs weigh; default `spillBuckets` x `spillBytes` (one bucketing pass), `0` removes the cap |
| `spark.vecruntime.exec.shuffledHashJoin.enabled` | convert `ShuffledHashJoinExec` (both inputs are exchanges; Spark's row shuffle is converted below us) |
| `spark.vecruntime.exec.sortMergeJoin.enabled` | compatibility alias: `false` reads as `spark.vecruntime.exec.sortMergeJoin.mode=off`, `true` as `auto` (the default since #311). Kept for compatibility -- set the mode instead. The hash rewrite (#10): `SortMergeJoinExec` re-expressed as our shuffled hash join when the smaller side's statistics fit `spark.vecruntime.join.maxBuildSize` and no parent relies on the merge's ordering; tie order under `ORDER BY` and the rows an unordered `LIMIT` picks can differ from Spark's order-preserving merge, which is why `auto` sends such joins to the merge join instead. Comet's equivalent replacement, `spark.comet.exec.forceShuffledHashJoin`, is also off by default (experimental); Comet executes the merge join natively (`spark.comet.exec.sortMergeJoin.enabled`, on by default), as `mode=merge` does here since #286. |
| `spark.vecruntime.exec.sortMergeJoin.mode` | `auto` (default since #311; was `off`), `off`, `hash` (the rewrite above), `merge` (our order-preserving merge join over Spark's sorted inputs, #286 -- every join type, no statistics needed, Spark's row order kept) or `auto` (#287): per join, the merge join where a parent relies on the ordering, where the row order can reach a `LIMIT` or a sort without an exchange in between (the hash rewrite's tie order would show), or where the hash rewrite is not allowed -- no statistics, the build side past `spark.vecruntime.join.hashMaxBuildSize` per task, a skew join -- and the hash rewrite where a side's statistics fit: in memory within `spark.vecruntime.join.spillBytes`, in buckets on disk within the cap. The boolean flag reads as `auto`. The plan prints the decision on the join (`Sort-merge join as hash join: right side fits ...`). |
| `spark.vecruntime.sort.spillBytes` | the sort's memory budget per task (#416): runs past it are written to local disk in sorted order and merged from there, which is what lets our merge join take inputs of any size; default 1 GiB (the join's build budget) -- the 32 MB of #451 came from a sweep that shared the cluster with a full run; measured alone in the full run's shape it cost the merge-join queries 30-50% (q14a 121.8 vs 93.7 s, q4 106.9 vs 71.2) because the spilled runs share the node disk with the shuffle files before them; `0` turns spilling off |
| `spark.vecruntime.comet.shuffle.range.enabled` | also hand range-partitioned exchanges (global `ORDER BY`) to Comet's native shuffle |
| `spark.vecruntime.scan.prefetch` | `0` (off); `1` or `2` inserts `VectorPrefetchScanExec` between a Spark vectorized file scan (Parquet, or Iceberg's `BatchScanExec`; not a Comet scan) and the first operator of ours above it (#403, lever 2): a helper thread per task pulls the reader's next batch and converts every column into our Arrow vectors while the task thread works on the previous one, through a queue of that many batches -- the reader's S3 and decode waits overlap our kernels, at the cost of that many converted batches of memory per task; its metrics (`prefetchWaitMs`, `readWaitMs`, `convertMs`, `batches`) say which side waited |
| `spark.vecruntime.scan.nativeParquet.enabled` | `true` (on, since 0.0.6; off when Comet's scan is active in the session); our own Parquet scan `VectorParquetScanExec` replaces a supported flat-schema Parquet `FileSourceScanExec` (#559, slice 1) and decodes pages straight into our Arrow vectors (`NativeParquetColumnReader`, reused vectors + injected parquet-java `BytePacker`), so the chain above is ours from the leaf. Reuses Spark's dynamically selected partitions (DPP), file splitting, bucketing, pushed data filters (row-group / page skipping) and the required + partition schema; batches are `spark.sql.parquet.columnarReaderBatchSize` rows. It decodes `PLAIN`, dictionary and `DELTA_BINARY_PACKED` INT32/INT64, `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY` strings and `BYTE_STREAM_SPLIT` INT32/INT64/DOUBLE, and BOOLEAN (`PLAIN` and `RLE`), TINYINT and SMALLINT, and decimals up to 38 digits stored as INT32, INT64, fixed-length or binary bytes (DECIMAL128 above 18 digits): a file with another encoding (`BYTE_STREAM_SPLIT` on a fixed-length byte array, say), or a column stored in a physical type its lane does not decode (a string in a fixed-length byte array, a decimal whose file scale differs from the requested one), falls that file over to Spark's reader at open time. TIMESTAMP columns stored as INT64 `MICROS` or `MILLIS` are decoded natively; an `INT96` timestamp file, or a file whose dates or timestamps need a calendar rebase (resolved per file from its footer, as Spark resolves it), is read by Spark's reader with that file's own modes. FLOAT, BINARY and TIMESTAMP_NTZ columns are decoded natively and carried by operators as columns without a lane. A nested column or bucketed scan keeps Spark's scan with a recorded reason. On by default, except in a session where Comet's scan is active (Comet's plugin or extension registered, `spark.comet.enabled` and `spark.comet.scan.enabled` not false), where Comet's reader stays the scan; set the key to force either way. |
| `spark.vecruntime.scan.nativeParquet.prefetchFiles` | `6`; how many files of a split `VectorParquetScanExec` opens ahead (#559/#566) -- status, footer, first row group -- on background threads while the task thread decodes; pays off on splits of many small files. `0` off. Up to N opened files per task. Capped at 16 |
| `spark.vecruntime.scan.nativeParquet.prefetchRowGroups` | `2`; how many row groups of the current file `VectorParquetScanExec` reads ahead (#559/#566), on background threads, chained so the file's reader is used by one thread at a time and in order; pays off on files with several row groups. `0` off. Up to M row groups (compressed pages) per task, so memory scales with the row-group size. Capped at 16 |
| `spark.vecruntime.scan.nativeParquet.decodeAhead` | `0` (off); how many batches the native scan decodes ahead on a producer thread per task (#606), overlapping decode with the operators above the scan; `…decodeAhead.threads` = `virtual` (default) or `platform` |
| `spark.vecruntime.scan.nativeParquet.dictionaryStrings` | `true`; the native scan emits dictionary-encoded string pages as dictionary vectors (ids over the row group's dictionary, #612), so our operators compute on the ids as they do over Spark's scan; `false` decodes flat strings |
| `spark.vecruntime.comet.mixed.enabled` | `false`; with Comet on the classpath, a Spark operator left to Spark whose children are ours goes to Comet's native operator through the sink leaf (`docs/comet.md`, #280) -- for the operator kinds `spark.vecruntime.comet.preferComet` names. |
| `spark.vecruntime.comet.preferComet` | empty; the allowlist of the mixed pass (#281): comma-separated operator kinds (`filter`, `project`, `sort`, `sortMergeJoin`, `hashJoin`, `broadcastHashJoin`, `window`, `expand`, `union`, `limit`, or `all`), each optionally qualified (`project:wideDecimal`, `filter:strings`, `sort:estimatedRows>1000000`). A listed operator above one of our chains is offered to Comet first and ours steps aside with the reason `delegated to Comet (spark.vecruntime.comet.preferComet)`; one Comet declines is ours after all, never Spark's. Empty = mixed plans allowed, none requested. An entry is added only when it meets the three-part rule of `docs/comet.md`, whose decision table (TPC-H SF10, #281) found none that does as written: the default stays empty; Comet's join (2-3.6x ours where it fires) and Comet's scan-side filter (-8% over TPC-H, the #14 dictionary decode) name the two costs to remove on our side first. |
| `spark.vecruntime.exec.strictFloatingPoint` | **on by default**: double `sum`/`avg` round exactly like Spark (one accumulator per group, rows added in order). `false` uses lane-parallel and interleaved partial sums that differ from Spark's in the last bits (about 7% of aggregate kernel time, 2.5% of TPC-H Q1) and can make an equality between two double sums fail (TPC-H Q15 returns no rows). Comet's `spark.comet.exec.strictFloatingPoint` is the analogous switch with the opposite default (`false`) and mechanism (`true` makes Comet fall back to Spark for such operations; we compute the strict result in our kernels). The benchmark configurations run with `false`, matching Comet's default |
| `spark.vecruntime.exec.selection.enabled` | pass selection bitmaps between our operators instead of compacting |
| `spark.vecruntime.comet.shuffle.enabled` | feed Comet's native shuffle from our operators when Comet's shuffle is configured |
| `spark.vecruntime.shuffle.enabled` | our own columnar shuffle exchange over Arrow IPC and Arrow Flight (#288). Default `true`, but it only takes effect with the `vecruntime-shuffle` jar on the classpath and `spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager` -- without those the exchange stays Spark's. TPC-H SF10: 0.59x the row shuffle over the 22 queries (`docs/results.md`) |
| `spark.vecruntime.shuffle.backend` | how a reducer fetches a remote map output: `flight` (default; one Flight server per executor -- for executors that stay up for the job), `block` (Spark's block transfer), or the class name of a `VectorShuffleBackend` from another jar |
| `spark.vecruntime.shuffle.compression` | body compression of the shuffle's record batches: `zstd` (default; native), `lz4` (Arrow's codec is pure Java and an order of magnitude slower) or `none` |
| `spark.vecruntime.shuffle.batchRows`, `spark.vecruntime.shuffle.batchBytes`, `spark.vecruntime.shuffle.bufferBytes` | a map task holds each reduce partition's rows until `batchRows` (default `8192`) or `batchBytes` (default `1m`) and writes them as one record batch; `bufferBytes` (default `64m`) caps what one task holds across partitions |
| `spark.vecruntime.shuffle.flushBytes` | serialised bytes a map task keeps in memory per reduce partition before spilling to a temporary file (default `1m`) |
| `spark.vecruntime.shuffle.writer.memoryLimit` | the map task's Arrow allocator limit (default `1g`), the backstop behind `bufferBytes` |
| `spark.vecruntime.shuffle.writer.dictionaryMaxRatio` | a string column is dictionary-encoded on the wire only when distinct values / rows in the record batch is at most this (default `0.5`, #356); `1` always encodes, `0` never |
| `spark.vecruntime.shuffle.flight.bindHost`, `spark.vecruntime.shuffle.flight.threads` | the Flight server's bind address (default: the executor's host name) and serving threads (default: the core count, at least 4) |
| `spark.vecruntime.ui.enabled` | attach the Vector Acceleration tab to the Spark UI (default `true`) |
| `spark.vecruntime.ui.retainedExecutions` | queries kept by that tab (default `100`) |
| `spark.vecruntime.explainFallback.enabled` | log why each operator was left to Spark (default `false`) |

The Flight shuffle server is a new network endpoint on every executor. With `spark.authenticate` on it
requires Spark's shuffle secret as a bearer token on every call and refuses unauthenticated `DoGet`s (it
refuses to start at all when auth is on but no secret is available); without `spark.authenticate` it is
as open as Spark's own block transfer in that configuration. TLS for the Flight server is not wired yet:
with `spark.ssl.rpc.enabled` the server refuses to start rather than serve in the clear -- use
`spark.vecruntime.shuffle.backend=block` there until it lands.

Both backends assume executors that stay up for the job: a lost executor loses its map outputs and
Spark recomputes them. Disposable executors need a push-based shuffle service such as Apache
Celeborn, which is future work, not part of #288 -- the `VectorShuffleBackend` seam (a commit hook
per map output, a `read` a service-backed backend overrides whole, a stream reader that decodes
several map outputs' streams concatenated) is where it plugs in.

JVM system properties for the kernels: `vecruntime.vectorBits=128|256|512` forces a vector shape
(the default is the platform's preferred one), `vecruntime.platform=neon|sve|avx2|avx512` overrides the
probed SIMD platform the kernels dispatch on (`docs/results.md`, "x86 kernel lab"), `vecruntime.agg.interleave=1|2|4` sets how many
accumulator copies the grouped aggregation rotates through when `spark.vecruntime.exec.strictFloatingPoint`
is off (default 1 on AVX-512 and 4 elsewhere; strict mode always uses one, and an explicit 1 also means Spark's summation order), `vecruntime.selection.minFraction` (default 0.5) is the
surviving fraction below which a filter compacts instead of forwarding a selection, and
`vecruntime.agg.plainDictMaxEntries` (default 512) is the number of distinct values above which a
plain (non-dictionary) string group key stops being dictionary-encoded on the fly and is hashed and
compared per row instead.

### Memory tuning

Two pools matter, and they are not the same one Spark's defaults assume.

**Off-heap: the data.** Every batch our operators produce is Arrow memory from Netty's allocator,
and so are the shuffle's record batches and the Flight server's buffers. That allocator is bounded by
the JVM's direct-memory limit, which defaults to the heap size -- so an executor with a small heap and
a large `spark.executor.memoryOverhead` still refuses Arrow allocations at the heap's size unless
`-XX:MaxDirectMemorySize` is raised. Set it to the overhead less what the JVM itself needs (the
cluster manifests use the overhead minus 2 GB), and size the overhead for the data in flight: a task's
input batch, its output batch, the shuffle writer's `bufferBytes` (64 MB per task) and, on a reducer,
the fetched blocks of the partition it is merging.

**On-heap: the tables.** The grouped aggregate's key table and accumulators, and a hash join's build
table and its heap mirrors, are Java arrays on the heap. The aggregate registers with Spark's task
memory manager and asks for its real footprint as its table grows (#367), so it lives inside the
executor's execution memory: each task is entitled to roughly `spark.executor.memory x
spark.memory.fraction / cores`, more when its neighbours are idle. The join's build side is bounded by
`spark.vecruntime.join.maxBuildSize` instead. Two rules of thumb follow. The heap
is not "just the JVM": with 13 tasks per executor and a 20 GB heap, an aggregate gets about 900 MB
before it has to spill, whatever the overhead holds. And the accumulators are interleaved for the
kernels (`vecruntime.agg.interleave`, 4 on NEON, 1 on AVX-512 and in strict mode), which multiplies
their footprint by the same factor -- strict mode is the cheapest in memory as well as the exact one.

| setting | what it bounds |
|---|---|
| `spark.executor.memory`, `spark.executor.memoryOverhead`, `-XX:MaxDirectMemorySize` | the tables (heap) and the data (direct); the 1 TB campaign ran 20 GB / 30 GB / 28 GB per 13-core executor |
| `spark.vecruntime.agg.spillThreshold` | a hard cap on one grouped aggregate table (default `1g`, #511). Below it the operator asks Spark's task memory manager for the table's real footprint as it grows and acts on a refusal: a partial aggregate emits its table and starts over, a final aggregate spills into hash buckets and merges them one at a time (#363, #367). `0` disables both |
| `spark.vecruntime.agg.spillBuckets` | buckets a final aggregate spills into (default `16`); each is merged in memory, so the buckets, not the input, must fit |
| `spark.vecruntime.agg.passThroughRatio` | a partial aggregate whose full table reduced its input by less than this factor stops aggregating and passes each batch on (default `1.5`, `0` = never; #376). The exchange receives the same rows either way |
| `spark.vecruntime.join.maxBuildSize` | the largest build side the hash joins take (per task, on the heap) |
| `spark.vecruntime.shuffle.bufferBytes`, `spark.vecruntime.shuffle.flushBytes`, `spark.vecruntime.shuffle.batchBytes` | what a map task holds before writing (direct memory): across all partitions, per partition before its temporary file, per record batch |
| `spark.sql.shuffle.partitions` | the size of a reduce task's input, hence of every table built from it: at 1 TB with 200 partitions a wide exchange hands a reducer several hundred MB of compressed input, and the final aggregate over it is the one that spills (#368 measures 1000 partitions with a 128 MB advisory size) |

The sort holds runs of `spark.vecruntime.sort.runRows` rows and spills them as Arrow IPC once their bytes
pass `spark.vecruntime.sort.spillBytes` (default 1 GiB, measured in #416; the runs are merged on the way
out), and the shuffled hash join spills its build side into buckets past `spark.vecruntime.join.spillBytes`
(default 256 MB, the grace join). The window still holds its whole partition (Arrow memory, plus an
`int` permutation per row on the heap); a partition that cannot fit should keep Spark's window.

**Disk.** Everything we write lands under `spark.local.dir`, through Spark's own block manager, so it
is sized and cleaned like Spark's shuffle files -- and on Kubernetes that is the node's disk or the
`emptyDir` the manifest mounts, not the container's image layer.

| what | when | where | how big |
|---|---|---:|---|
| a final aggregate's spill | its table passes the budget: the whole table goes out as `spillBuckets` Arrow IPC streams, hash-partitioned by key; later merged one bucket at a time | a temp local block per bucket | the table's size; freed at the operator's close |
| a partial aggregate's overflow | never to disk -- it emits its table to the exchange and starts over | -- | -- |
| a map task's shuffle output | always: one data file per map task with a partition index (Spark's layout, our IPC record batches, `zstd` by default) | the shuffle block resolver's data file | the task's compressed output; deleted when the shuffle is unregistered (#358 -- before it, never) |
| a map task's overflow | a reduce partition's serialised bytes pass `spark.vecruntime.shuffle.flushBytes` (1 MB) before the batch is closed | a temporary file beside the data file, merged into it at commit | at most the task's output |
| a reducer's fetched blocks | never to disk: one `DoGet` per remote executor streams its blocks back to back, decoded batch by batch, the previous batch freed as the next is produced | -- | a batch per open stream (one stream per remote executor), in direct memory; the `block` backend follows Spark's fetch rules |

Two numbers to keep in mind at scale. The node disk holds every live shuffle of the job -- at 1 TB with
eight executors the first run filled 20 GB nodes within minutes because map outputs were never deleted
(#358); with deletion in place the high-water mark is the largest stage's output. And an aggregate's
spill is written once and read once per bucket, so its cost is one extra pass of Arrow IPC over the
table, not a re-sort: q78's aggregate at 1 TB spilled and finished in 99 s against Spark's 114.
What went wrong at 1 TB and how each was fixed is in `docs/results.md`: map outputs never deleted
(#358, the node disks), an aggregate that never spilled (#363), a budget that undercounted the
accumulators four to eight times (#367), and a budget that then overcounted them and emptied tables
that fit (#376).

### JDK 25 and Spark 4.1

Spark 4.1 officially supports JDK 17 and 21. Running it on 25 needs two things beyond the usual
`--add-opens` set that Spark's launcher already passes:

- Hadoop 3.4.2, which Spark 4.1.3 bundles, calls `Subject.getSubject` and fails on JDK 24+
  ([HADOOP-19212](https://issues.apache.org/jira/browse/HADOOP-19212)). Replace
  `hadoop-client-api` and `hadoop-client-runtime` in `$SPARK_HOME/jars` with 3.4.3 (drop-in shaded
  jars; this project's tests do the same through Maven).
- `--sun-misc-unsafe-memory-access=allow` on the driver and executors. It is required, not cosmetic:
  without it Arrow's Netty allocator cannot address direct memory on JDK 25 and the first columnar
  operator fails (`EmptyByteBuf.memoryAddress`).

Recommended, not required: `-XX:+UseCompactObjectHeaders` on the driver and executors (#578). It is a
product feature on JDK 25 (JEP 519) and the default from JDK 27 (JEP 534). Object headers shrink from
12 to 8 bytes, which lands on Spark's row and expression objects, boxed fallback values and planning
objects rather than on our off-heap batches. The benchmark launcher (`benchmarks/scripts/submit-cluster.sh`)
passes it. A JDK AOT cache (#416) records the header layout it was built with, so build it with the
same flag: `benchmarks/k8s/aot/aot-env.sh` does.

## Requirements and known limitations

What a deployment needs, and what the plugin does not do yet -- the short list; the reasons and the
measurements behind each item are in the linked docs and issues.

**Requirements**

- **JDK 25** on the driver and the executors, with `--add-modules=jdk.incubator.vector
  --enable-native-access=ALL-UNNAMED` in both `extraJavaOptions`. The Vector API is an incubator
  module: its shape can change between JDK releases, so a JDK upgrade may need a rebuild of the
  kernels.
- **Spark 4.1.x or 4.2.x, Scala 2.13**, with the jar built for that line (`-Pspark-4.2` for 4.2). Spark
  4.1 on JDK 25 additionally needs Hadoop 3.4.3's client jars in place of the bundled 3.4.2 (the section
  above); Spark 4.2 bundles Hadoop 3.5.0, which does not.
- **Memory:** a heap-heavy split. The operators keep their tables (aggregate keys, join builds, sort
  runs) on the heap and their batches in Arrow direct memory, so give the heap more than Spark's
  defaults would and bound direct memory explicitly: at 50 GB per 13-core executor the 1 TB runs
  use a 30 GB heap and 20 GB of overhead, with `-XX:MaxDirectMemorySize` set to the overhead less
  what the JVM itself needs (about 2 GB). Plain Spark on the same nodes prefers 20 / 30. The
  [Memory tuning](#memory-tuning) section has the rules.
- **The columnar shuffle** (`spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager`)
  serves reducers over Arrow Flight from each executor on an ephemeral port
  (`spark.vecruntime.shuffle.flight.bindHost` chooses the interface): executors must reach each other
  directly. With `spark.authenticate` on, every call carries Spark's shuffle secret; TLS is not
  implemented, and under `spark.ssl.rpc.enabled` the server refuses to start -- use
  `spark.vecruntime.shuffle.backend=block` (Spark's own block transfer carrying our batches) there
  (`docs/flight-shuffle.md`). Without the manager the plugin runs over Spark's row shuffle,
  converting at the boundary.
- **Platforms measured:** x86-64 with AVX-512 (the 1 TB campaign) and AVX2, and Apple silicon
  (NEON, 128-bit lanes) for the local suites. Graviton (SVE) is untested (#253); the kernels choose
  the lane width at start-up, so it should run, but the thresholds were set on x86.
- **Comet and Iceberg are optional.** Comet 1.0 gives a native Parquet scan and a native shuffle the
  plugin can sit between; Iceberg 1.11 gives the vectorized reader and merge-on-read tables. Neither
  is needed for Parquet through Spark's own reader.

**Not converted yet (falls back to Spark, with the reason recorded)**

- `ObjectHashAggregateExec` functions we do not carry (`percentile*`, `collect_top_k`, `listagg`, ...);
  `collect_list`, `collect_set` and `bloom_filter_agg` are converted (#57). `SortAggregateExec` over
  non-string buffers (#57); cached tables, `InMemoryTableScanExec` (#55).
- Nested-type accessors and constructors: `arr[i]`, `map[key]`, struct/array/map construction, the
  lambda function families (#50); struct fields and pass-through nested columns work.
- Python UDFs (#65) and the Parquet write path, `DataWritingCommandExec` (#64).
- Decimal window aggregates over sliding frames (#28), `IGNORE NULLS` (#58), `RANGE` window offsets over decimal or timestamp (interval) order keys.
- The window operator holds its whole partition in memory (the sort and the joins spill; the window
  does not yet).
- Regular expressions, collated strings, binary and Float columns as computed values or keys (they
  pass through untouched); the full lists are the "Falls back" column of
  [Supported today](how-it-works.html#supported-today) and `docs/expressions.md`.

**Known behaviour to be aware of**

- Results equal Spark's on every TPC-DS and TPC-H query; a query whose result contains ties
  (TPC-DS q65) may order them differently, as any engine may.
- The AOT class-data cache (`benchmarks/k8s/aot/`) is off by default: it speeds start-up and
  costs the heavy queries 20% at 1 TB (`docs/results.md`).
- The scan is Spark's own vectorized Parquet reader; a query bound by the scan (q88, q9) runs at
  Spark's speed. `spark.vecruntime.scan.prefetch` converts on a helper thread and is off by default
  because the reader, not the conversion, is the cost (#403).
- Spark's SQL golden-file suite runs with the plugin (`spark-sql-tests` profile); coverage, not a
  pass rate, is tracked in `spark-sql-tests/src/test/resources/vector-sql-coverage.tsv` (#17).


## Not in scope (yet)

- The rest of the Parquet type system in our own reader (`spark.vecruntime.scan.nativeParquet.enabled`,
  #559). It reads every flat type; INT96 timestamps and nested types (structs, lists, maps) are read by
  Spark's reader instead, decided per file or per plan. FLOAT, BINARY and TIMESTAMP_NTZ are read natively
  but have no engine lane yet: operators carry them untouched, and an operator that computes on one stays
  Spark's. Comet's reader remains an option for the zero-copy case.
- A spilling window, TLS for the Flight shuffle server,
  and a push-based shuffle service for disposable executors (the `VectorShuffleBackend` seam is
  where it plugs in).
