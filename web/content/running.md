---
layout: default
title: Running and tuning
description: How to launch Spark with VecRuntime, add the columnar shuffle, size memory, and what it requires.
---

# Running and tuning

## Launch

```bash
spark-submit \
  --repositories https://raw.githubusercontent.com/vecruntime/vecruntime/maven-repo/ \
  --packages io.github.vecruntime:vecruntime-spark_4.1_2.13:0.0.9 \
  --conf spark.plugins=io.vecruntime.spark.VectorPlugin \
  --conf spark.driver.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  --conf spark.executor.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  ...
```

`spark.plugins` registers the session extension; alternatively set
`spark.sql.extensions=io.vecruntime.spark.VectorSparkSessionExtensions`. The same flags
work on `spark-shell`, `pyspark` and `spark-sql`. Offline, download the jars from the
[releases page](https://github.com/vecruntime/vecruntime/releases) and pass them with
`--jars` instead of `--packages`.

The three JDK 25 flags are required, not cosmetic: `--add-modules=jdk.incubator.vector`
enables the Vector API, `--enable-native-access` is for the zstd and Netty native code, and
`--sun-misc-unsafe-memory-access=allow` lets Arrow's allocator address direct memory —
without it the first columnar operator fails.

### The columnar shuffle

Add its jar and shuffle manager; Spark's own shuffle keeps serving every exchange the
plugin does not produce:

```bash
  --packages io.github.vecruntime:vecruntime-spark_4.1_2.13:0.0.9,io.github.vecruntime:vecruntime-shuffle_4.1_2.13:0.0.9 \
  --conf spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager \
```

The shuffle also pulls Arrow Flight, gRPC and protobuf-java (about 40 jars); `--packages`
resolves them. The Flight server has no TLS: under `spark.ssl.rpc.enabled` it refuses to
start — set `spark.vecruntime.shuffle.backend=block` (Spark's block transfer over the same
files) there. See [Flight shuffle](flight-shuffle.html).

## Memory tuning

Two pools matter, and they are not the ones Spark's defaults assume.

- **Off-heap — the data.** Every batch, shuffle record batch and Flight buffer is Arrow
  memory bounded by the JVM's direct-memory limit, which defaults to the heap size. Raise
  `-XX:MaxDirectMemorySize` explicitly (the cluster manifests use the memory overhead less
  about 2 GB for the JVM itself), and size `spark.executor.memoryOverhead` for the data in
  flight. Spark's default overhead (10 % of heap) is far too little here — about 4 GB on a
  15-core executor.
- **On-heap — the tables.** The aggregate's key table and accumulators and a hash join's
  build table are Java arrays on the heap. The aggregate registers with Spark's task memory
  manager and spills past its budget; give the heap more than Spark's defaults would.

Optional but recommended: with `arrow-memory-unsafe` on the classpath and
`-Darrow.allocation.manager.type=Unsafe`, Arrow's buffers are plain `malloc` memory that no
longer counts against `-XX:MaxDirectMemorySize` (the executor-wide cap bounds them instead),
which lowered direct memory from ~10 GB to 6 GB at 10 TB at the same speed. Also optional:
`-XX:+UseCompactObjectHeaders` on JDK 25 shrinks object headers from 12 to 8 bytes.

The spilling operators and their budgets:

| Key | Default | What it bounds |
|---|---|---|
| `spark.vecruntime.agg.spillThreshold` | `1g` | one grouped aggregate table; past it a partial aggregate restarts, a final one spills into buckets (`0` = never) |
| `spark.vecruntime.sort.spillBytes` | `1g` (no cap with off-heap enabled) | the sort's runs; past it they spill to local disk and merge back (`0` = never) |
| `spark.vecruntime.join.spillBytes` | `256m` | a shuffled hash join's build side before it buckets to disk (`0` = never) |
| `spark.vecruntime.join.maxBuildSize` | per-core off-heap share, else `1g` | the largest build side the hash joins hold in memory per task |

The window does not spill: a partition that will not fit should keep Spark's window
(`spark.vecruntime.exec.window.enabled=false`). Everything written lands under
`spark.local.dir` through Spark's block manager. The full memory model, the executor-sizing
worked example and the disk table are on the
[detailed Running page](https://github.com/vecruntime/vecruntime/blob/main/docs/running.md).

## Requirements

- **JDK 25** on driver and executors, with the two `extraJavaOptions` flags above. The
  Vector API is an incubator module, so a JDK upgrade may need a kernel rebuild.
- **Spark 4.1.x or 4.2.x, Scala 2.13**, with the jar built for that line (`-Pspark-4.2` for
  4.2). Spark 4.1 on JDK 25 also needs Hadoop 3.4.3's client jars in place of the bundled
  3.4.2 ([HADOOP-19212](https://issues.apache.org/jira/browse/HADOOP-19212)); Spark 4.2
  bundles a Hadoop that does not.
- **Memory:** a heap-heavy split with direct memory bounded explicitly (above).
- **The columnar shuffle** serves reducers over Arrow Flight on an ephemeral port, so
  executors must reach each other directly; with `spark.authenticate` on, every call carries
  Spark's shuffle secret. TLS is not implemented. Without the shuffle manager the plugin runs
  over Spark's row shuffle.
- **Platforms measured:** x86-64 (AVX-512, AVX2) and Apple silicon (NEON). Graviton (SVE) is
  untested — the kernels pick the lane width at start-up, so it should run, but the
  thresholds were set on x86.
- **Comet and Iceberg are optional**; neither is needed for Parquet through Spark's own
  reader.

## Known limitations

- Float, binary and nested values as computed values or keys fall back (they pass through
  untouched); regular expressions, collated-string computation, and zone-dependent (DST) date
  arithmetic fall back.
- Object aggregates not carried (`percentile_*`, `collect_top_k`, `listagg`), Python UDFs,
  and cached-table (`InMemoryTableScanExec`) scans fall back.
- The window operator does not spill; `IGNORE NULLS`, and `RANGE` window offsets over decimal
  or timestamp order keys, fall back.
- Results equal Spark's on every TPC-DS and TPC-H query; a result containing ties may order
  them differently, as any engine may.

The [Supported operators](operators.html), [Supported expressions](expressions.html) and
[Compatibility matrix](compatibility.html) pages are the row-by-row lists; every
`spark.vecruntime.*` key is on the [Configuration reference](configuration.html).
