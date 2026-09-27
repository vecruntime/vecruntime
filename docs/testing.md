---
layout: default
title: Testing & correctness
description: How vecruntime's correctness is established — Spark's SQL golden suite and its coverage floor, the ported Comet matrices, the project's own suites, and the benchmark checksums.
---

# Testing & correctness

vecruntime accelerates Spark by replacing operators, so "correct" means one thing: **the accelerated
query returns Spark's result.** Every layer below establishes that by running the same query with the
plugin on and off (or against Spark's own golden files) and comparing. There is no silent fallback —
an operator that cannot be accelerated stays on Spark with a recorded reason, and the suites assert on
those reasons too.

## 1. Spark's own SQL golden-file suite

The `spark-sql-tests` module runs Apache Spark's `SQLQueryTestSuite` — the `.sql` inputs and
`.sql.out` golden files unpacked from the `spark-sql` tests jar — with vecruntime's session extension
injected (`VectorSQLQueryTestSuite`). Every golden result must still match, whether an operator was
converted or left to Spark. This is Comet's own self-validation strategy applied here.

It is deliberately **not** part of `mvn verify` (the whole suite takes about 15 minutes) and runs on
demand through `benchmarks/scripts/run-spark-sql-tests.sh`.

- **What it compares:** each query's rows against Spark's checked-in golden output. The suite's JVM
  sets `vecruntime.agg.interleave=1` so double sums add in Spark's order and match the golden digits,
  and runs with `-Dspark.testing=true` and `-XX:-OmitStackTraceInFastThrow`.
- **How many cases run:** **642 test cases pass, 111 ignored** (the Python-UDF variants that need
  `pyspark`, absent by default). Excluded by default
  (`VectorSQLQueryTestSuite.defaultExclude = ^(explain(-[a-z]+)?|hll|kllquantiles|thetasketch|udtf/udtf)\.sql`):
  - `explain*.sql`, whose golden output is Spark's own physical plan — exactly what the extension
    replaces (Comet skips them for the same reason);
  - the DataSketches files `hll`, `kllquantiles`, `thetasketch`, whose memory library refuses to
    initialise on any JDK newer than 21 (Spark's dependency, not ours) and aborts the whole run;
  - `udtf/udtf.sql`, which needs `pyspark`.

  `SQL_TESTS_EXCLUDE='^$'` runs the excluded files anyway.
- **The coverage floor.** Passing is the low bar — a file passes just as well when *every* operator
  falls back. So the run also records, per test case, how many query executions ran at least one
  vecruntime operator, and a **full run compares those counts with the checked-in floor**,
  `spark-sql-tests/src/test/resources/vector-sql-coverage.tsv` (#17). A case whose accelerated count
  dropped **fails the suite**, naming the case, because a fallback introduced by a planner change is
  otherwise invisible. Cases above the floor are listed; `SQL_TESTS_UPDATE_BASELINE=true` rewrites it.
  The checked-in floor covers 584 test cases: 164 run at least one vecruntime operator (4,219
  accelerated executions in total; the full run on 0.0.2 measured 4,231 of 33,856), and 420 never can
  (analyzer-only cases, DDL, files with no supported operator). A filtered run
  (a name regex as the first argument) skips the floor comparison.
- The suite earns its keep: its runs have caught real correctness bugs the hand-written suites
  missed — eager `nanvl` second-argument evaluation, `count(DISTINCT)` over literal arguments,
  collated strings treated as lanes, a build side pruned to zero columns, and a bare literal
  projection that compiled but could not be materialised.
- A manual CI workflow, `.github/workflows/spark-sql-tests.yml`, runs the same script **on demand**
  (`workflow_dispatch`, with optional filter / include-excluded / JVM-args inputs).

**Run it:**

```bash
benchmarks/scripts/run-spark-sql-tests.sh                 # everything
benchmarks/scripts/run-spark-sql-tests.sh '^(group-by|join|decimal)'   # a name regex (first argument)
SQL_TESTS_EXCLUDE='^$' benchmarks/scripts/run-spark-sql-tests.sh   # include the excluded files too
SQL_TESTS_UPDATE_BASELINE=true benchmarks/scripts/run-spark-sql-tests.sh   # full run; rewrite the coverage floor
SQL_TESTS_JVM_ARGS='-Dspark.vecruntime.exec.sortMergeJoin.mode=merge' benchmarks/scripts/run-spark-sql-tests.sh 'join'
```

Requires `JAVA_HOME` on a JDK 25 and the plugin installed in `~/.m2`.

## 2. Ported DataFusion Comet test matrices

Five suites port DataFusion Comet's own expression, cast, aggregate, join and window test matrices to
vecruntime's plugin-on/off comparison model. Each case runs the query twice on one session with
`spark.vecruntime.enabled` toggled, compares the rows against Spark, and asserts either the vecruntime
operator's presence (`checkVectorized`) or the fallback and its recorded reason (`checkFallback`):

- `VectorPortedCometExprSuite` — expressions (#497)
- `VectorPortedCometCastSuite` — casts (#500)
- `VectorPortedCometAggregateSuite` — aggregates (#501)
- `VectorPortedCometJoinSuite` — joins (#507)
- `VectorPortedCometWindowSuite` — windows (#510)

The full row-by-row result — what is accelerated and what falls back with which reason — is the
[Compatibility matrix](compatibility.html).

## 3. vecruntime's own suites

Validation the project maintains directly (all on JDK 25):

- **Kernel correctness against a scalar oracle.** Every SIMD kernel has a scalar twin in
  `ScalarReference`; `kernels/src/test` compares them on random data with nulls, selections and
  awkward lengths (tails shorter than a vector, batches not a multiple of 64), run at 128, 256 and
  512 bits (`-Dvecruntime.vectorBits=…`) so the AVX2/AVX-512 paths get coverage.
- **Per-operator Spark comparison suites** (`Vector*Suite`): `VectorFilterSuite`,
  `VectorProjectSuite`, `VectorAggregateSuite`, `VectorSortSuite`, `VectorDecimalSuite` (exact
  comparison, no double tolerance), `VectorJoinSuite`, `VectorSortMergeJoinSuite`, `VectorWindowSuite`,
  and the adapter / Arrow suites plus `SparkOnJdkSmokeSuite` — each using the same
  `checkVectorized` / `checkFallback` model as the ported matrices.
- **Shuffle suites** for the columnar Arrow-Flight shuffle (`shuffle/`).
- **Comet and Iceberg integration** (only under their profiles): `CometScanSuite` /
  `CometShuffleSuite` (`-Pcomet`, tag `CometTest`) cover zero-copy scan adaptation, dictionary
  strings, the shuffle rewrite and C-Data release; `IcebergScanSuite` and `CometIcebergSuite`
  (`-Piceberg`, tag `IcebergTest`).
- **TPC-H / TPC-DS query tests** through the benchmark runners (see below).

*(Exact per-suite counts vary by revision and profile; run the suites to see the current numbers.)*

## 4. Benchmarks as correctness checks

The TPC-DS (103 queries) and TPC-H (22 queries) benchmark runners are not only timing harnesses:
every query compares **row counts and a result checksum** against Spark on the same data, so a run
that is faster but wrong fails. This holds through the 1 TB TPC-DS campaign and the TPC-H runs
documented in the [results notebook](results.html).

The one recorded exception is **q65**: its `ORDER BY s_store_name, i_item_desc LIMIT 100` has ties at
scale, so the 100 returned rows follow physical order and the checksum differs across *every* engine —
Spark against Comet included — not a vecruntime discrepancy. (q64's zero-row result is a separate,
intermittent dynamic-partition-pruning timing issue, also documented there.)

## 5. Running everything locally

- **The CI gate the crews run before a PR:**

  ```bash
  mvn -B -Pcomet,iceberg -pl kernels,spark,shuffle,benchmarks install
  ```

- **A single suite:**

  ```bash
  mvn -pl spark install -Dsuites=io.vecruntime.spark.VectorAggregateSuite
  ```

- **The kernel suite at a chosen vector width:**

  ```bash
  mvn -pl kernels test -Dvector.jvm.args="--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Dvecruntime.vectorBits=512"
  ```

- **The golden suite** — no argument runs everything; the first argument is a test-name regex:

  ```bash
  benchmarks/scripts/run-spark-sql-tests.sh                 # everything
  benchmarks/scripts/run-spark-sql-tests.sh '^(group-by|join|decimal)'   # filtered by name
  ```

- **Plain `mvn verify`** builds kernels + the Spark suites (Comet suites skipped); add `-Pcomet`,
  `-Piceberg`, or `-Pcomet,iceberg` for the profiled suites.
