---
layout: default
title: Benchmarking
description: How to run VecRuntime's benchmarks — the kernel microbenchmarks, TPC-H and TPC-DS, locally and on a cluster.
---

# Benchmarking

VecRuntime ships its own benchmark harnesses: JMH microbenchmarks for the kernels, and TPC-H and
TPC-DS runners that run each query on plain Spark and on VecRuntime over the same data and compare.
Every TPC query also checksums its result against Spark, so a benchmark that is faster but wrong fails
— the harnesses are correctness checks as well as timers.

The detailed page — the named configurations, cluster runs, event-log reports and profiling — is the
[detailed benchmarking page](https://github.com/vecruntime/vecruntime/blob/main/docs/benchmarking.md).
Measured numbers live on the [results page](results.html).

## Kernel microbenchmarks (JMH)

```bash
mvn -DskipTests package
java --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
     -jar benchmarks/target/benchmarks.jar "Compare|Compact|Agg"
```

The kernels can be run at the lane counts of other platforms, emulated on any machine, which is how
the AVX2 and AVX-512 paths stay honest on a laptop:

```bash
mvn -pl kernels test -Dvector.jvm.args="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Dvecruntime.vectorBits=512"
```

## TPC-H

All 22 queries over the eight tables; data is generated with DuckDB.

```bash
brew install duckdb
benchmarks/scripts/gen-tpch.sh 1                     # benchmarks/data/sf1/<table>
mvn -DskipTests install
export JAVA_HOME=/opt/homebrew/opt/openjdk@25
benchmarks/scripts/run-tpch.sh benchmarks/data/sf1   # spark + vector, q1..q22 (--queries q1,q6 for a subset)
```

Point `COMET_JAR` at a Comet jar to add the Comet configurations to the run. The Iceberg
merge-on-read variants are generated with `gen-iceberg-mor.sh` and run with
`--iceberg … --variant …`.

## TPC-DS

The 99 queries (103 with the a/b variants) over the 24 tables, keeping the real `DECIMAL` and `DATE`
columns. The query text is Spark's own, so each plan is the one Spark's optimizer is tested against.

```bash
benchmarks/scripts/gen-tpcds.sh 1                              # benchmarks/data/tpcds-sf1/<table>
benchmarks/scripts/run-tpcds.sh benchmarks/data/tpcds-sf1      # spark + vector -> benchmarks/results/tpcds
benchmarks/scripts/run-tpcds.sh benchmarks/data/tpcds-sf1 spark,vector --queries q10,q35,q45
benchmarks/scripts/run-tpcds.sh --report                       # rebuild reports without re-running
```

## The configurations

Runs are labelled with named configurations — `spark` (plain Spark), `vector` (our operators over
Spark's scan and shuffle), `vector-shuffle` (adds the [Flight shuffle](flight-shuffle.html)), and
several that bring Comet's scan or shuffle in front of our operators. Each runs in its own JVM and
appends to `benchmarks/results/<config>.jsonl`; the runner then rewrites a markdown and a
self-contained HTML report with median and p90 bars, speedups against Spark, the accelerated-operator
count per query, and a checksum proving every configuration returned the same rows.

## On a cluster

The same runners drive a `spark-submit` session (`--cluster`), read tables from a base URI or a
catalog, and write one `.jsonl` per run to any Hadoop filesystem, each row carrying Spark's per-stage
metrics. `benchmarks/k8s/` renders the submit line and a Kubernetes job matrix;
`run-tpcds.sh --cluster-report` writes the report in the data-on-EKS layout (summary, speedup
distribution, regressions with their stage evidence, per-query table, environment). See
`benchmarks/k8s/README.md`.

## Profiling

When a result is surprising, profile before theorising. Java Flight Recorder attaches with one
environment variable, and `profile-query.sh` records and summarises in one step:

```bash
benchmarks/scripts/profile-query.sh benchmarks/data/sf10 comet-scan-vector q1
```

Every performance finding in the project came out of such a recording rather than the median alone.

---

The named configurations in full, the cluster run variables, the event-log report layout and the JFR
workflow are on the
[detailed benchmarking page](https://github.com/vecruntime/vecruntime/blob/main/docs/benchmarking.md).
