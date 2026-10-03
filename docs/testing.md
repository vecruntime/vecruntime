---
layout: default
title: Testing & correctness
description: How VecRuntime's correctness is established — Spark's SQL golden suite and its coverage floor, the ported Comet matrices, the project's own suites (including the native Parquet reader's parquet-java cross-checks and conformance corpus), and the benchmark checksums.
---

# Testing & correctness

VecRuntime accelerates Spark by replacing operators, so "correct" means one thing: **the accelerated
query returns Spark's result.** Every layer below establishes that by running the same query with the
plugin on and off (or against Spark's own golden files) and comparing. There is no silent fallback —
an operator that cannot be accelerated stays on Spark with a recorded reason, and the suites assert on
those reasons too.

## 1. Spark's own SQL golden-file suite

The `spark-sql-tests` module runs Apache Spark's `SQLQueryTestSuite` — the `.sql` inputs and
`.sql.out` golden files unpacked from the `spark-sql` tests jar — with VecRuntime's session extension
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
  VecRuntime operator, and a **full run compares those counts with the checked-in floor**,
  `spark-sql-tests/src/test/resources/vector-sql-coverage.tsv` (#17). A case whose accelerated count
  dropped **fails the suite**, naming the case, because a fallback introduced by a planner change is
  otherwise invisible. Cases above the floor are listed; `SQL_TESTS_UPDATE_BASELINE=true` rewrites it.
  The checked-in floor covers 584 test cases: 164 run at least one VecRuntime operator (4,219
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
VecRuntime's plugin-on/off comparison model. Each case runs the query twice on one session with
`spark.vecruntime.enabled` toggled, compares the rows against Spark, and asserts either the VecRuntime
operator's presence (`checkVectorized`) or the fallback and its recorded reason (`checkFallback`):

- `VectorPortedCometExprSuite` — expressions (#497)
- `VectorPortedCometCastSuite` — casts (#500)
- `VectorPortedCometAggregateSuite` — aggregates (#501)
- `VectorPortedCometJoinSuite` — joins (#507)
- `VectorPortedCometWindowSuite` — windows (#510)

The full row-by-row result — what is accelerated and what falls back with which reason — is the
[Compatibility matrix](compatibility.html).

## 3. VecRuntime's own suites

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

### The native Parquet reader

Our own Parquet scan (`spark.vecruntime.scan.nativeParquet.enabled`, #559, on by default) decodes pages itself, so it is
checked against the Parquet reference implementation as well as Spark. Each encoding and type is admitted
only once all of these pass:

- **Kernel tests against a scalar reference.** `ColumnChunkDecoderTest`, `DeltaBinaryPackedReaderTest`,
  `RleBitPackingReaderTest` and `ByteStreamSplitKernelsTest` decode pages built by a from-scratch
  encoder of the spec, with nulls, across pages and at every batch size. The SIMD and SWAR variants are
  compared with their scalar twin at 128, 256 and 512 bits.
- **parquet-java cross-checks, against its writers and its readers.** `ParquetPageDecoderCrossCheckSuite`
  and `DeltaBinaryPackedCrossCheckSuite` encode values with parquet-java's own `ValuesWriter`s. Those
  are the writers Spark, Hive and most JVM engines use: `PLAIN`, RLE/dictionary, `DELTA_BINARY_PACKED`,
  `DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY`, `BYTE_STREAM_SPLIT` (FLOAT bits included, NaN payloads and
  -0.0), the boolean `PLAIN` and `RLE` writers, and the fixed-length byte-array writers used for decimals. The suites decode the same bytes with our decoder and with parquet-java's matching
  `ValuesReader`, and require identical values.
- **Round trips through Spark** (`VectorParquetScanSuite`). The suite writes v1 and v2 pages, with and
  without dictionaries, and with nulls, several row groups and filters. Each query must return Spark's
  rows, and the suite asserts that the scan read the row groups itself rather than handing them to
  Spark's reader. It also reads files written directly with parquet-java, with out-of-range and
  overflowing values, against Spark's parquet-java-based reader.
- **The Apache Parquet conformance corpus** (`ParquetTestingCorpusSuite`). The suite reads 27 files from
  `apache/parquet-testing`, vendored at a pinned commit (Apache-2.0, credited in `NOTICE`), one column at
  a time. The files were written by parquet-mr of several ages, parquet-cpp, arrow-rs and Impala, and
  cover v2 encodings, empty and all-null pages, checksums, and a dictionary page at offset 0.
  - A column the scan supports must be read natively and match Spark's row-based reader.
  - Any other column must return what Spark returns with the plugin off, rows or refusal.
  - A corrupt file from `bad_data/` that Spark refuses must be refused too.

  The corpus found a bug that predated it: a `FIXED_LEN_BYTE_ARRAY` decimal was decoded as INT64 (#592).
- **Spark's SQL golden suite with the native scan on**
  (on by default, so the suite of section 1 runs on it; before 0.0.6 it took
  `SQL_TESTS_JVM_ARGS=-Dspark.vecruntime.scan.nativeParquet.enabled=true`): 642
  succeeded, 0 failed.
- **Throughput.** Each decoder has a JMH benchmark against Spark's or parquet-java's reader for the same
  page (`DeltaBinaryPackedBenchmark`, `V2EncodingsBenchmark`). The numbers are in the
  [design note](native-parquet-reader.md).

**What we took from Hardwood.** [Hardwood](https://github.com/hardwood-hq/hardwood) (Apache-2.0) is a
Parquet reader for the JVM. We studied it, and no code was copied. Two practices came from it:
- Testing against the `apache/parquet-testing` corpus, as its conformance runner does.
- Bounding every size a file declares before allocating for it:
  - the `DELTA_BINARY_PACKED` block size is capped at 2^16;
  - a `BYTE_STREAM_SPLIT` value region that is not a whole number of values fails the page;
  - a boolean page or RLE run that claims more values than its bytes hold fails the page;
  - a bit-packed run past the stream end fails the page.

We kept our own decoders, which measured faster: the vectorized `BYTE_STREAM_SPLIT` transpose, whole
miniblocks for `DELTA_BINARY_PACKED`, and 64-bits-a-step booleans. Hardwood's are a scalar gather and
bit-at-a-time booleans.

*(Exact per-suite counts vary by revision and profile; run the suites to see the current numbers.)*

## 4. Benchmarks as correctness checks

The TPC-DS (103 queries) and TPC-H (22 queries) benchmark runners are not only timing harnesses:
every query compares **row counts and a result checksum** against Spark on the same data, so a run
that is faster but wrong fails. This holds through the 1 TB TPC-DS campaign and the TPC-H runs.

The one recorded exception is **q65**: its `ORDER BY s_store_name, i_item_desc LIMIT 100` has ties at
scale, so the 100 returned rows follow physical order and the checksum differs across *every* engine —
Spark against Comet included — not a VecRuntime discrepancy. (q64's zero-row result is a separate,
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
