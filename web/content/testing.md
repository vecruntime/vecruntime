---
layout: default
title: Testing & correctness
description: How VecRuntime's correctness is established — Spark's own SQL golden suite, the ported Comet matrices, the Apache Iceberg 1.12 upstream suites, the native-reader cross-checks and conformance corpus, and the benchmark checksums.
---

# Testing & correctness

VecRuntime accelerates Spark by replacing operators, so "correct" means one thing: the accelerated
query returns **Spark's result**. Every layer below establishes that by running the same query with
the plugin on and off — or against Spark's own golden files, or Iceberg's — and comparing. There is
no silent fallback: an operator that cannot be accelerated stays on Spark with a recorded reason, and
the suites assert on those reasons too.

The detailed page — every suite, how the coverage floor works, the parquet-java cross-checks and what
each run has caught — is the
[detailed testing page](https://github.com/vecruntime/vecruntime/blob/main/docs/testing.md).

## Spark's own SQL golden suite

`VectorSQLQueryTestSuite` runs Apache Spark's own `SQLQueryTestSuite` — Spark's `.sql` inputs and
`.sql.out` golden files — with VecRuntime's extension injected, and requires every golden result to
match whether an operator was converted or left to Spark. This is Comet's own self-validation
strategy applied here.

Passing the golden files is only the low bar: a file passes just as well when every operator falls
back. So the run also records, per case, how many executions ran at least one VecRuntime operator and
compares those counts against a **checked-in coverage floor**; a case whose accelerated count dropped
**fails the suite**, so a fallback introduced by a planner change cannot slip in unseen. The suite has
caught real correctness bugs the hand-written suites missed.

It is not part of the normal build (the whole suite takes about 15 minutes); run it on demand:

```bash
benchmarks/scripts/run-spark-sql-tests.sh                 # everything
benchmarks/scripts/run-spark-sql-tests.sh '^(group-by|join|decimal)'   # a name regex
```

## Ported DataFusion Comet matrices

Five suites port DataFusion Comet's own expression, cast, aggregate, join and window test matrices to
VecRuntime's plugin-on/off comparison model. Each case runs twice on one session with the plugin
toggled, compares the rows against Spark, and asserts either the operator's presence or the fallback
and its recorded reason. The full row-by-row result — what accelerates and what falls back with which
reason — is the [compatibility matrix](compatibility.html).

## VecRuntime's own suites

- **Kernels against a scalar oracle.** Every SIMD kernel has a scalar twin; the two are compared on
  random data with nulls, selections and awkward lengths, run at 128, 256 and 512 bits so the AVX2
  and AVX-512 paths get coverage.
- **Per-operator Spark comparison suites** (`Vector*Suite`): filter, project, aggregate, sort,
  decimal (exact, no double tolerance), joins, sort-merge join, window, and the adapter / Arrow
  suites.
- **The native Parquet reader** is also checked against the Parquet reference implementation: kernel
  tests against a from-scratch encoder of the spec, **cross-checks against parquet-java's own writers
  and readers** (the writers Spark, Hive and most JVM engines use, required to decode identically),
  round trips through Spark, and the **Apache `parquet-testing` conformance corpus** (vendored at a
  pinned commit) — each column either read natively and matching Spark, or returning exactly what
  Spark returns with the plugin off.
- **Comet and Iceberg integration** suites, under their build profiles.

## Apache Iceberg's upstream suites

The plugin runs **Apache Iceberg 1.12.0's own Spark 4.1 test suites** with every VecRuntime Iceberg
path on — native scan, columnar writer, deletion-vector / position-delete writer, copy-unchanged
(`benchmarks/iceberg-tests/`). The last full run:

| module | tests | failed | skipped |
|---|---|---|---|
| `iceberg-spark-extensions-4.1_2.13` | 1334 | 0 | 106 |
| `iceberg-spark-4.1_2.13` | 579 | 0 (after the exclusions below) | 22 |

With the plugin **off** the same suites pass, so every exclusion is about the plugin, not the
environment. There are **9 exclusions**, each triaged and none a correctness miss:

- six `*WithVectorizedReads` cases assert on Iceberg's own `BatchScanExec` plan node, which VecRuntime
  replaces with `VectorIcebergScanExec`;
- one case greps EXPLAIN text for `max(data)`, which VecRuntime's aggregate node prints differently;
- two `rewrite_data_files` bin-pack cases on v3/v4 derive a file-size threshold from Iceberg's own
  (larger) writer output, so the columnar writer's smaller pages correctly leave the input unsplit —
  rows and rewritten bytes match (the v2 runs of both pass).

Iceberg builds hard-require **JDK 17 or 21** for Gradle while the VecRuntime jars need **JDK 25** at
test runtime (the Vector API incubator module), so the harness launches Gradle on JDK 21 and runs the
test JVM on JDK 25 through a toolchain. Both JDKs must be present.

```
GRADLE_JDK=/path/to/jdk21 TEST_JDK=/path/to/jdk25 \
  benchmarks/iceberg-tests/run-iceberg-tests.sh          # plugin on
VECRUNTIME_ENABLED=false ...                              # vanilla-Iceberg baseline
```

This harness drove several real fixes (task grouping and per-scan split options, shaded/unshaded
parquet interop, copy-on-write row-group order, write coalescing, the adaptive row-group boundary).

## Benchmarks as correctness checks

The TPC-DS (99 queries) and TPC-H (22 queries) runners are not only timing harnesses: every query
compares **row counts and a result checksum** against Spark on the same data, so a run that is faster
but wrong fails. The one recorded exception is **q65**, whose `LIMIT` over tied sort keys returns rows
in physical order and so checksums differently across *every* engine, Spark vs Comet included — not a
VecRuntime discrepancy.

---

Every suite, the coverage floor, the cross-check detail and the Iceberg triage rubric are on the
[detailed testing page](https://github.com/vecruntime/vecruntime/blob/main/docs/testing.md).
