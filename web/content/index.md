---
layout: default
title: Overview
description: A vectorized execution runtime for Apache Spark SQL using Java
---

# VecRuntime

VecRuntime is a Spark SQL plugin that runs the core operators — Filter, Project,
HashAggregate, Sort, Window, Range, Expand, Generate, the limits, Union and the hash,
sort-merge and nested-loop joins — on Arrow-layout columnar batches with the Java Vector
API. It runs on the JVM: no native engine, no JNI, no serialization boundary. Anything it
does not accelerate falls back to Spark, always with a recorded reason and always with
Spark's own result.

- **JVM-native.** No native runtime, JNI or external engine; SIMD comes from the Java
  Vector API (`jdk.incubator.vector`).
- **Columnar by design.** Operators consume and produce Arrow-layout batches, with a
  columnar shuffle of its own over Arrow IPC and Arrow Flight.
- **Spark-compatible.** Results equal Spark's; unsupported operators, expressions and types
  fall back transparently.
- **Incremental.** Operators accelerate one at a time while the rest of the plan runs on
  Spark, and it interoperates zero-copy with Apache DataFusion Comet.

## Getting started

VecRuntime needs **JDK 25** (for the Java Vector API) and **Spark 4.1 or 4.2, Scala 2.13**.
Register the plugin and pass the JDK 25 flags — no code changes:

```bash
spark-submit \
  --repositories https://raw.githubusercontent.com/vecruntime/vecruntime/maven-repo/ \
  --packages io.github.vecruntime:vecruntime-spark_4.1_2.13:0.0.9 \
  --conf spark.plugins=io.vecruntime.spark.VectorPlugin \
  --conf spark.driver.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  --conf spark.executor.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  ...
```

Add the columnar shuffle with its jar and
`--conf spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager`.
On Spark 4.1 with JDK 25, replace the bundled Hadoop 3.4.2 client jars with 3.4.3
([HADOOP-19212](https://issues.apache.org/jira/browse/HADOOP-19212)); Spark 4.2 bundles a
Hadoop that does not need this. The [Running and tuning](running.html) page has the full
command, the offline (`--jars`) path and memory tuning.

Version 0.0.9 is a preview release under the Apache License 2.0. It runs the whole of
TPC-DS (all 99 queries) and TPC-H (22) with every operator accelerated and returns Spark's
results.

## Results

The three engines on identical hardware and data, numbers and per-query charts on each page:

- [TPC-DS 1 TB — Spark vs VecRuntime vs DataFusion Comet](benchmarks/tpcds-1tb.html) (x86, Amazon EKS).
- [TPC-DS 1 TB on AWS Graviton4](benchmarks/tpcds-1tb-graviton.html) (arm64, Neoverse V2 / SVE2), set against the x86 run.
- [TPC-DS 3 TB — VecRuntime vs Gluten + Velox vs Comet](benchmarks/tpcds-3tb-graviton.html) (Graviton4), against the published data-on-eks results.
- [TPC-DS 10 TB on Graviton4](benchmarks/tpcds-10tb-graviton.html), 11 × m8gd.4xlarge, against the totals other engines publish for the same nodes.
- [Iceberg merge-on-read — VecRuntime vs Spark](benchmarks/iceberg-mor.html), the v2 delete-file and v3 deletion-vector merge cost at TPC-H SF1.

## What it accelerates

- **Operators:** Filter, Project, HashAggregate (partial and final, with spill), Sort,
  the limits, Union, Coalesce, Expand (`ROLLUP`/`CUBE`/`GROUPING SETS`), Generate
  (`explode`), Window, Range, and broadcast / shuffled hash, sort-merge and nested-loop
  joins. The full list with conditions is on [Supported operators](operators.html).
- **Types:** int, long, double, date, timestamp, boolean, string, and decimal to 38 digits;
  byte/short on int lanes; wide decimals on a 128-bit lane. See
  [Supported expressions](expressions.html) and the
  [Compatibility matrix](compatibility.html).
- **Parquet** read and write natively ([Apache Parquet](parquet.html)) and **Apache
  Iceberg** read and write ([Apache Iceberg](iceberg.html)), each falling back per file or
  per plan.
- **Columnar shuffle** over Arrow Flight ([Flight shuffle](flight-shuffle.html)), and
  zero-copy interop with [Comet as the scan](comet.html).

## What it does not do

- **Float, binary and nested values as computed values or keys** fall back (they pass
  through untouched); regular expressions, collated-string computation, and zone-dependent
  (DST) date arithmetic fall back.
- **Object aggregates** it does not carry (`percentile_*`, `collect_top_k`, `listagg`),
  Python UDFs, and cached-table scans fall back.
- **The window operator does not spill** yet; a partition that will not fit should keep
  Spark's window.
- **TLS for the Flight shuffle** is not wired (use the `block` backend under
  `spark.ssl.rpc.enabled`), and disposable executors need a push-based shuffle service that
  is future work.

See [How it works](how-it-works.html) for the design, and the
[Configuration reference](configuration.html) for every key.
