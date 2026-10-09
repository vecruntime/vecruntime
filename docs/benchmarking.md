---
layout: default
title: Running the benchmarks
---

# Running the benchmarks

Kernel microbenchmarks (JMH):

```bash
mvn -DskipTests package
java --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
     -jar benchmarks/target/benchmarks.jar "Compare|Compact|Agg"
```

The kernel tests can run with the lane counts of other platforms, emulated (slowly) on any machine,
which is how the AVX2 and AVX-512 code paths (`compress`, 256-entry shuffle tables, 8-lane masks)
are kept honest on a laptop:

```bash
mvn -pl kernels test -Dvector.jvm.args="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Dvecruntime.vectorBits=512"
```

TPC-H, all 22 queries over the eight tables (decimals replaced by doubles, generated with DuckDB):

```bash
brew install duckdb
benchmarks/scripts/gen-tpch.sh 1                      # benchmarks/data/sf1/<table>  (lineitem: 6M rows, 207 MB)
benchmarks/scripts/gen-tpch.sh 10                     # benchmarks/data/sf10/<table> (lineitem: 60M rows, 2.1 GB)
benchmarks/scripts/gen-tpch.sh 1 benchmarks/data --decimals   # benchmarks/data/sf1-decimal: real DECIMAL(15,2) columns
mvn -DskipTests install
export JAVA_HOME=/opt/homebrew/opt/openjdk@25
benchmarks/scripts/run-tpch.sh benchmarks/data/sf1    # spark + vector, q1..q22 (--queries q1,q6 for a subset)
COMET_JAR=/path/to/comet-spark-spark4.1_2.13-1.0.0.jar \
benchmarks/scripts/run-tpch.sh benchmarks/data/sf10   # + comet-scan, comet-scan-vector, comet-scan-vector-shuffle, comet
benchmarks/scripts/gen-iceberg-mor.sh benchmarks/data/sf1   # Iceberg merge-on-read variants of lineitem (docs/iceberg.md)
benchmarks/scripts/run-tpch.sh benchmarks/data/sf1 spark,vector --iceberg benchmarks/data/iceberg --variant sf1.pos_10 --queries q1,q6,probe-count,probe-sum,probe-group
```

TPC-DS, the 99 queries (103 with the a/b variants) over the 24 tables, keeping the real
`DECIMAL(7,2)` and `DATE` columns; the query text is Spark's own (`tpcds/q*.sql` from the spark-sql
tests jar, the files its plan-stability suite runs), so each query's plan is the one Spark's optimizer
is tested against:

```bash
benchmarks/scripts/gen-tpcds.sh 1                     # benchmarks/data/tpcds-sf1/<table> (store_sales: 2.9M rows)
benchmarks/scripts/run-tpcds.sh benchmarks/data/tpcds-sf1                      # spark + vector, q1..q99 -> benchmarks/results/tpcds
benchmarks/scripts/run-tpcds.sh benchmarks/data/tpcds-sf1 spark,vector --queries q10,q35,q45
benchmarks/scripts/run-tpcds.sh --report
```

On a cluster the same runners take the session `spark-submit` built (`--cluster`), the tables from
a base URI or a catalog (`--tables s3a://bucket/tpcds/sf1000/parquet`, `--tables catalog:db`), and
write one `.jsonl` per run to any Hadoop file system (`--out s3a://...`), each row carrying Spark's
stage metrics for the query (executor time, GC, shuffle bytes, spill, peak memory) beside the plan
and the operator counts; `benchmarks/scripts/submit-cluster.sh <config> <tables> <dataset> <out>`
turns a configuration into the `spark-submit` line, and `run-tpcds.sh --cluster-report <out>` writes
the report in the layout of the data-on-EKS Comet benchmark (summary, speedup distribution,
regressions with their stage evidence, per-query table, environment) -- see `benchmarks/k8s/README.md`.

## The benchmark configurations

The named configurations are defined twice and kept in step by hand -- `TpchRunner.Configs` for the
local harness and the `ENGINE` arrays of `benchmarks/scripts/submit-cluster.sh` for a cluster (the
runner warns when the session it runs in disagrees with the configuration it is labelled with):

| configuration | what the session sets |
|---|---|
| `spark` | nothing: plain Spark, its vectorized Parquet reader, its sort-based shuffle |
| `vector` | `spark.plugins=io.vecruntime.spark.VectorPlugin`; `spark.vecruntime.exec.strictFloatingPoint=false` (Comet's rounding; see the note under Aggregation); `spark.vecruntime.exec.sortMergeJoin.mode=auto`; `spark.sql.parquet.enableVectorizedReader=true` (Spark's default, made explicit -- our operators consume its batches, the row reader would make every plan fall back); `spark.sql.columnVector.offheap.enabled=true` (#403: the reader writes Arrow's fixed-width layout into native memory and the adapter wraps those lanes in place instead of copying them) |
| `vector-shuffle` | `vector` plus `spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager` and `spark.vecruntime.shuffle.enabled=true`: our columnar exchange (#288) |
| `vector-shuffle-strict` | `vector-shuffle` with `strictFloatingPoint=true` (bit-identical double sums) |
| `comet-scan-vector-ourshuffle` | `vector-shuffle` with Comet's plugin and scan (`spark.comet.enabled`, `spark.comet.scan.enabled`, every `spark.comet.exec.*` operator off, `spark.memory.offHeap.enabled` with `OFFHEAP`, 32 g) |
| `comet-scan-vector-shuffle`, `hybrid`, `comet` | Comet's scan and native shuffle under our operators; the same with the mixed pass (`spark.vecruntime.comet.mixed.enabled`); pure Comet |

On the cluster (`benchmarks/k8s/run-matrix.sh <tables> <dataset> <out> <image> [runner args]`,
`CONFIGS="spark vector-shuffle"` selects the configurations) the executor properties are environment
variables of `render-run.sh`, and the 1 TB runs in `docs/results.md` use: `EXECUTORS=8`,
`EXEC_CORES=13`, `EXEC_MEM=30g` (heap), `EXEC_OVERHEAD=20g`, `DIRECT_MEM=30g`
(`-XX:MaxDirectMemorySize`, where our Arrow batches and the shuffle's buffers live -- see Memory
tuning), `DRIVER_CORES=2`, `DRIVER_MEM=4g`, `KEEP_EXECUTORS=1`, one iteration per query. From
2026-09-26 the 1 TB runs pass only `--conf spark.sql.shuffle.partitions=300` in `SUBMIT_ARGS` and
leave AQE at Spark's defaults: no `spark.sql.adaptive.advisoryPartitionSizeInBytes` override (64 MB)
and no `spark.sql.adaptive.coalescePartitions.minPartitionNum`. The pages published before then used
`advisoryPartitionSizeInBytes=128m` and `minPartitionNum=208`, as their configuration sections say. `EXEC_JAVA_OPTS` appends executor JVM options (a JFR recording:
`-XX:StartFlightRecording=delay=55s,duration=60s,filename=/tmp/exec.jfr,settings=profile`, copied
out of the executor pods with `kubectl cp` before the application ends), `SUBMIT_ARGS` extra
`--conf` pairs for one run (`--conf spark.vecruntime.sort.spillBytes=4g`). Read a
run's medians from the driver log before the next run of the same configuration replaces the pod,
and know that the container log rotates at 10 MB; the `.jsonl` results in `<out>` and
`run-tpcds.sh --cluster-report` are the durable record.

Each configuration runs in its own JVM and appends its measurements to
`benchmarks/results/<config>.jsonl` (`benchmarks/results/tpcds/<config>.jsonl` for TPC-DS). The runner then rewrites two reports from every `.jsonl` file,
one section per dataset (`sf1`, `sf10`, ...): `benchmarks/results/results.md` and a self-contained
`benchmarks/results/results.html` with bar charts (median, p90 whisker, speedup against plain
Spark), an accelerated-operators table per query (operators executed by our kernels or Comet over
the operators that count, the same classification the Vector Acceleration UI tab uses, so each closed
compatibility gap shows up as a query moving), the operators found in each final plan with the
reason for every one the planner declined to convert, and a checksum proving all configurations
returned the same rows (to 10 significant digits). Regenerate them
without benchmarking with
`benchmarks/scripts/run-tpch.sh --report`. See [docs/results.md](results.html) for numbers
measured on an Apple M3 Pro; at SF10, Q1 runs 1.64x faster than Spark over Spark's own scan and
1.84x over Comet's scan and shuffle (Comet end to end: 1.62x), while the highly selective Q6 stays
at 0.86x over Spark's scan. The five-configuration matrix on an x86 host after #311 (results.md,
"The five configurations"): over the 22 TPC-H SF10 queries pure Spark 86.5 s, our operators over our
columnar shuffle 59.6 s (1.45x), with Comet's scan in front 53.4 s (1.62x), hybrid 44.0 s (1.97x),
native Comet 40.6 s (2.13x); TPC-DS SF1 runs 80-87% of operators on our kernels (85-87% with Comet's
scan) with every checksum equal to Spark's, and at that scale is a coverage harness rather than a speed
benchmark.

When a result is not what you expected, profile before theorising. Java Flight Recorder attaches
to a benchmark JVM with one environment variable, and `RESULTS_DIR` keeps the profiling run out of
the report:

```bash
JVM_EXTRA="-XX:StartFlightRecording=filename=/tmp/q1.jfr,settings=profile,dumponexit=true" \
RESULTS_DIR=/tmp/profiling \
benchmarks/scripts/run-tpch.sh benchmarks/data/sf10 comet-scan-vector --queries q1 --warmup 2 --iterations 5
$JAVA_HOME/bin/jfr view hot-methods /tmp/q1.jfr
$JAVA_HOME/bin/jfr print --events jdk.ExecutionSample --stack-depth 12 /tmp/q1.jfr   # callers of a hot frame
```

Or as one command: `benchmarks/scripts/profile-query.sh benchmarks/data/sf10 comet-scan-vector q1`
runs, records and summarises (`benchmarks/scripts/jfr-summary.sh <file.jfr>` summarises any
recording: hot methods, the callers of the JDK-internal `MemorySegment` frames, the plugin's own
frames by self time, allocation, GC, waits and native methods -- text you can paste into an issue).

Every performance finding in this project came out of such a recording rather than out of the
median alone: the masked-reduction aggregate, triple string decoding, the two-lane shuffle table,
and most recently a zero-copy configuration that was slower than a copying one because Comet's scan
delivers plain strings where Spark's delivers dictionaries (`MemorySegment.mismatch` set-up was a
third of the aggregate). The `[tpch]` lines the runner prints with per-operator kernel time are the
first thing to read; the JFR hot-method list and the callers of any JDK-internal frame near the top
(`checkValidStateRaw`, `checkBounds`) are the second.
