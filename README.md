# VecRuntime

**A vectorized execution runtime for Apache Spark SQL using Java**

VecRuntime accelerates Spark SQL workloads by executing core operators directly on **Arrow-layout columnar
batches** using the **Java Vector API**, bringing SIMD-optimized execution to the JVM without native
libraries, JNI, or serialization boundaries.

Inspired by the execution architecture of Apache DataFusion Comet, VecRuntime runs **Filter, Project,
HashAggregate, Sort, Window, Range, Expand, Generate, Union and the hash, sort-merge and nested-loop joins**,
with its own Parquet scan and its own columnar shuffle over Arrow Flight. Spark remains the fallback for
any unsupported operator, expression or data type, always with a recorded reason. It can also run on top of
**Apache DataFusion Comet**'s native scan and shuffle, over the same columnar representation.

### Key characteristics

* **JVM-native:** no native runtime, JNI, or external execution engine.
* **SIMD-accelerated:** uses the Java Vector API (`jdk.incubator.vector`) for hardware-vectorized execution,
  on x86 (AVX2, AVX-512) and arm64 (NEON, SVE).
* **Columnar by design:** operators consume and produce Arrow-layout batches.
* **Spark-compatible:** unsupported operators, expressions, and types transparently fall back to Spark.
* **Zero-copy integration:** interoperates with columnar native components such as Comet without
  serialization between execution stages.
* **Incremental adoption:** operators can be accelerated individually while the rest of the plan runs on
  Spark as usual.

## Status

Version 0.0.7, a preview release under the Apache License 2.0 (see `LICENSE` and `NOTICE`). VecRuntime runs
the whole of TPC-DS (103 queries) and TPC-H (22) and returns Spark's results. Benchmark results, with the
hardware and settings behind them, are on the [project site](https://vecruntime.github.io/vecruntime/):
[TPC-DS 1 TB on x86](https://vecruntime.github.io/vecruntime/benchmarks/tpcds-1tb.html),
[TPC-DS 1 TB on AWS Graviton4](https://vecruntime.github.io/vecruntime/benchmarks/tpcds-1tb-graviton.html),
[TPC-DS 3 TB against Gluten + Velox and DataFusion Comet](https://vecruntime.github.io/vecruntime/benchmarks/tpcds-3tb-graviton.html), and
the full lab notebook in [docs/results.md](docs/results.md). `CHANGELOG.md` has what each release carries.

## Requirements

* **Spark 4.1 or 4.2** (Scala 2.13). Each release ships one build per Spark line.
* **JDK 25** on the driver and the executors, with the Vector API module enabled (see below).

Full requirements and known limitations: [docs/running.md](docs/running.md#requirements-and-known-limitations).

## Getting the jars

Every release is on the [releases page](https://github.com/vecruntime/vecruntime/releases): the plugin jar,
the columnar shuffle jar and a `SHA256SUMS` file, for each Spark line. The same artifacts are published to a
Maven repository served from this repository's `maven-repo` branch, with no account or token needed.

| Spark | plugin | columnar shuffle |
|---|---|---|
| 4.1 | `io.github.vecruntime:vecruntime-spark_4.1_2.13:0.0.7` | `io.github.vecruntime:vecruntime-shuffle_4.1_2.13:0.0.7` |
| 4.2 | `io.github.vecruntime:vecruntime-spark_4.2_2.13:0.0.7` | `io.github.vecruntime:vecruntime-shuffle_4.2_2.13:0.0.7` |

```xml
<repositories>
  <repository>
    <id>vecruntime</id>
    <url>https://raw.githubusercontent.com/vecruntime/vecruntime/maven-repo/</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>io.github.vecruntime</groupId>
    <artifactId>vecruntime-spark_4.1_2.13</artifactId>   <!-- or vecruntime-spark_4.2_2.13 -->
    <version>0.0.7</version>
  </dependency>
</dependencies>
```

## Quick start

```bash
spark-submit \
  --repositories https://raw.githubusercontent.com/vecruntime/vecruntime/maven-repo/ \
  --packages io.github.vecruntime:vecruntime-spark_4.1_2.13:0.0.7 \
  --conf spark.plugins=io.vecruntime.spark.VectorPlugin \
  --conf spark.driver.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  --conf spark.executor.extraJavaOptions="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
  ...
```

The same coordinates work with `spark-shell`, `pyspark` and `spark-sql`. The columnar shuffle, memory
settings and every other option are covered in the documentation below.

## Documentation

* [Running and tuning](docs/running.md): `spark-submit`, the columnar shuffle, memory tuning, JDK 25 notes,
  requirements and known limitations.
* [Configuration reference](docs/configuration.md): every `spark.vecruntime.*` key with its default.
* [Supported operators](docs/operators.md), [expressions](docs/expressions.md) and the
  [compatibility matrix](docs/compatibility.md).
* [How it works](docs/how-it-works.md): the plan rewrite, the Vector Acceleration UI tab, aggregation, the
  shuffle, sort, decimals and joins.
* Integrations: [Comet as the scan](docs/comet.md), [Apache Iceberg](docs/iceberg.md),
  [the Flight shuffle](docs/flight-shuffle.md), [the native Parquet reader](docs/native-parquet-reader.md),
  [planner rules (AQE, DPP)](docs/aqe-dpp-rules.md).
* [Testing and correctness](docs/testing.md), [running the benchmarks](docs/benchmarking.md) and the
  [results](docs/results.md).

## Building

```bash
export JAVA_HOME=/path/to/jdk-25
mvn verify                    # Spark 4.1 (default): kernels, plugin and shuffle, with their tests
mvn -Pspark-4.2 verify        # the same build against Spark 4.2
mvn -Pcomet,iceberg verify    # also the Comet- and Iceberg-backed suites (Spark 4.1 only for now)
```

Run `mvn clean` when switching Spark lines in one checkout. The plugin jar is
`spark/target/vecruntime-spark_<line>_2.13-<version>.jar`; Spark and Arrow are `provided`.

| Module | Contents |
|---|---|
| `kernels/` | Java 25: Arrow-layout buffers and the SIMD kernels |
| `spark/` | Scala 2.13 + Java: the plugin, the planner rules, the expression compiler and the operators |
| `shuffle/` | the columnar shuffle (Arrow IPC over Arrow Flight) |
| `benchmarks/` | JMH microbenchmarks, the TPC-H and TPC-DS runners, local and on Kubernetes |
| `spark-sql-tests/` | Spark's own SQL golden-file suite, run with the plugin |

## Contributing

Contributions are welcome: bug reports, failing queries, benchmarks on other hardware, and code.

* **Issues.** Open one on [GitHub](https://github.com/vecruntime/vecruntime/issues) with the query, the
  Spark version and the plan or the fallback reason from the Vector Acceleration tab.
* **Pull requests.** Branch from `main`, keep one change per PR and include tests. A change is ready when the
  gate passes: `mvn -B -Pcomet,iceberg -pl kernels,spark,shuffle,benchmarks install` on JDK 25. Planner
  or expression changes also need the SQL golden suite ([docs/testing.md](docs/testing.md)), and
  performance claims need a number (JMH or TPC-H / TPC-DS).
* **Conventions.** [AGENTS.md](AGENTS.md) holds the design decisions, the coding rules and what "done" means
  for a change; read it before a larger change. Update `CHANGELOG.md` and the docs in the same PR.
* **Releases.** A release is cut by pushing a `v<version>` tag on `main`: `.github/workflows/release.yml`
  builds both Spark lines on JDK 25, attaches the jars and their checksums to the GitHub release, and
  publishes them to the `maven-repo` branch.

## Contributors

* Angel Conde ([@Neuw84](https://github.com/Neuw84)), author and maintainer.

The full list is on the [contributors page](https://github.com/vecruntime/vecruntime/graphs/contributors).

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
