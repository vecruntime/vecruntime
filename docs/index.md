---
layout: default
title: Overview
description: A vectorized execution runtime for Apache Spark using Java
---

# vecruntime

A Spark SQL plugin that runs Filter, Project, HashAggregate, Sort, Window (`ROWS` and `RANGE`
frames), Range, Expand, Generate, the limits, Union and the hash, sort-merge and nested-loop joins on
Arrow-layout batches with the Java Vector API -- on the JVM, no native code -- with its own columnar
shuffle over Arrow Flight. Source, releases and the README:
[github.com/vecruntime/vecruntime](https://github.com/vecruntime/vecruntime).

vecruntime accelerates Spark SQL workloads by executing core operators directly on Arrow-layout
columnar batches using the Java Vector API (`jdk.incubator.vector`), bringing SIMD-optimized
execution to the JVM without native libraries, JNI, or serialization boundaries. Unsupported
operators, expressions, and types transparently fall back to Spark, always with a recorded reason.

## Key characteristics

- **JVM-native:** no native runtime, JNI, or external execution engine.
- **SIMD-accelerated:** uses the Java Vector API for hardware-vectorized execution.
- **Columnar by design:** operators consume and produce Arrow-layout batches.
- **Spark-compatible:** unsupported operators, expressions, and types fall back to Spark.
- **Vectorized operators:** Filter, Project, HashAggregate, Sort, Window (including `RANGE` frames with
  value offsets), Range, Expand, Generate, the limits, Union, and the hash, sort-merge and nested-loop
  joins -- the full list is on the [Operators](operators.html) page.
- **Zero-copy integration:** interoperates with columnar native components such as Apache
  DataFusion Comet without serialization between execution stages.
- **Incremental adoption:** operators can be accelerated individually while the rest of the Spark
  plan continues to execute normally.

## Getting started

vecruntime needs **JDK 25** (the Java Vector API) and Spark 4.1. On JDK 25, Spark 4.1.3's bundled
Hadoop 3.4.2 fails at start-up (`Subject.getSubject`,
[HADOOP-19212](https://issues.apache.org/jira/browse/HADOOP-19212)): replace `hadoop-client-api` and
`hadoop-client-runtime` in `$SPARK_HOME/jars` with their 3.4.3 versions (drop-in jars).

Let Spark download the plugin: `--packages` takes the Maven coordinates and `--repositories` points at
the Maven repository served from this project's `maven-repo` branch. No code changes:

```bash
spark-submit \
  --repositories https://raw.githubusercontent.com/vecruntime/vecruntime/maven-repo/ \
  --packages io.github.vecruntime:vecruntime-spark_2.13:0.0.2 \
  --conf spark.plugins=io.vecruntime.spark.VectorPlugin \
  --conf spark.driver.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  --conf spark.executor.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  ...
```

With the **columnar shuffle** (Arrow IPC map outputs, fetched over Arrow Flight) add its artifact and
its shuffle manager. Spark's own shuffle keeps serving every exchange the plugin does not produce:

```bash
spark-submit \
  --repositories https://raw.githubusercontent.com/vecruntime/vecruntime/maven-repo/ \
  --packages io.github.vecruntime:vecruntime-spark_2.13:0.0.2,io.github.vecruntime:vecruntime-shuffle_2.13:0.0.2 \
  --conf spark.plugins=io.vecruntime.spark.VectorPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager \
  --conf spark.driver.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  --conf spark.executor.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  ...
```

The same flags work on `spark-shell`, `pyspark` and `spark-sql`.

- `--sun-misc-unsafe-memory-access=allow` is required on JDK 25: without it Arrow's Netty allocator
  cannot address direct memory and the first columnar operator fails. `--add-modules` enables the
  Vector API; `--enable-native-access` is for the zstd and Netty native code.
- The plugin's Spark, Arrow and Scala dependencies are `provided`, so `--packages` downloads the
  plugin jar alone. The shuffle also pulls Arrow Flight, gRPC and protobuf-java (about 40 jars, none
  of which Spark bundles apart from versions it already ships).
- The Flight server has no TLS: where RPC encryption is on, add
  `--conf spark.vecruntime.shuffle.backend=block` (Spark's block transfer over the same files). See
  the [Configuration reference](configuration.html#columnar-shuffle-the-vecruntime-shuffle-jar).

Without network access, download the jars from the
[releases page](https://github.com/vecruntime/vecruntime/releases) (plugin jar, shuffle jar,
`SHA256SUMS`) and pass them with `--jars vecruntime-spark_2.13-0.0.2.jar` (the shuffle's Flight and
gRPC jars must then be on the classpath too; `--packages` resolves them for you).

`spark.plugins` registers the session extension automatically; alternatively set
`spark.sql.extensions=io.vecruntime.spark.VectorSparkSessionExtensions`. See the
[README](https://github.com/vecruntime/vecruntime#getting-the-jars) for the Maven coordinates, and
the [Configuration reference](configuration.html) for every `spark.vecruntime.*` key.

## Status

Version 0.0.2, a preview release under the Apache License 2.0. The plugin runs the whole of TPC-DS
(103 queries) and TPC-H (22) with every operator accelerated and returns Spark's results. On the
1 TB TPC-DS Parquet dataset on EKS, vecruntime finished the 103 queries in 2,557 s against
Spark's 3,309 s (23% less runtime, faster on 82 of 103 queries), within 2% of Apache DataFusion
Comet. The per-query tables and configurations are on the
[TPC-DS 1 TB page](benchmarks/tpcds-1tb.html).

## Benchmarks

- [Apache Spark vs vecruntime vs DataFusion Comet on TPC-DS 1 TB](benchmarks/tpcds-1tb.html) --
  103 queries on Amazon EKS, the three engines on identical hardware and data, per-query charts and tables.
- [Apache Spark vs vecruntime on TPC-DS 1 TB, AWS Graviton4](benchmarks/tpcds-1tb-graviton.html) --
  the same run on arm64 nodes (Neoverse V2, SVE2), set against the x86 run.
- [Iceberg merge-on-read: vecruntime vs Apache Spark](benchmarks/iceberg-mor.html) -- the v2
  delete-file and v3 deletion-vector merge cost at TPC-H SF1, against OSS Spark.

## Reference

- [Configuration](configuration.html) -- every `spark.vecruntime.*` key with its default.
- [Operators](operators.html), [Expressions](expressions.html) -- what converts, under which
  conditions, and why the rest falls back.
- [Compatibility matrix](compatibility.html) -- what runs on vecruntime and what falls back, row by
  row from the ported Comet test suites; [Testing & correctness](testing.html) -- how correctness is
  established (Spark's golden suite, the ported matrices, the benchmark checksums).
- [Comet as the scan](comet.html), [Apache Iceberg](iceberg.html),
  [The Flight shuffle](flight-shuffle.html).
