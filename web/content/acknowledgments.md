---
layout: default
title: Acknowledgments
description: The open-source projects, libraries and research VecRuntime uses, learns from, and checks itself against.
---

# Acknowledgments

VecRuntime stands on decades of open-source work — for ideas, for interfaces, and for being the
oracles we check our own correctness against. Everything below is something this project uses, learns
from, or validates itself with. The authoritative license and attribution record for distributed
dependencies is in the repository's
[`NOTICE`](https://github.com/vecruntime/vecruntime/blob/main/NOTICE) and
[`LICENSE`](https://github.com/vecruntime/vecruntime/blob/main/LICENSE); this page is the human
version.

## Projects we learned from

- **Apache DataFusion Comet** — the closest reference for a vectorized engine under Spark. VecRuntime
  can use Comet's native Parquet scan and native shuffle directly, and our compatibility matrices are
  ported from Comet's own expression, cast, aggregate, join and window test suites.
- **Apache DataFusion** — the Rust query engine underneath Comet, a continual reference for operator
  design and columnar execution.
- **DuckDB** — a reference for columnar execution done well: its asynchronous-I/O, string-view and
  memory-governor work informed our scan read-ahead and memory accounting. We also generate our TPC-H
  data with it.
- **Meta Velox** and **Umbra** — the string-view representation and other columnar-layout ideas for
  narrow-width string handling.
- **Apache Impala** and the **Parquet split-block Bloom filter** — the blocked Bloom-filter layout our
  runtime filter is modeled on.

## Libraries we depend on

VecRuntime depends on, but does not bundle, the following, each under its own license (see
[`NOTICE`](https://github.com/vecruntime/vecruntime/blob/main/NOTICE) for the canonical list).

| Project | What we use it for | License |
|---|---|---|
| [Apache Spark](https://spark.apache.org/) 4.1 | The SQL engine we plug into — catalyst, planning, scheduling, shuffle | Apache-2.0 |
| [Apache Arrow](https://arrow.apache.org/) (Java) 18.3 | The off-heap columnar buffer format our vectors are built on | Apache-2.0 |
| [Apache Parquet](https://parquet.apache.org/) (parquet-java) 1.16 | Footer/metadata handling and the file-format contract our native reader and writer implement | Apache-2.0 |
| [Apache Iceberg](https://iceberg.apache.org/) 1.12 | Table format and vectorized reader; the deletion-vector write path (optional) | Apache-2.0 |
| [Apache Hadoop](https://hadoop.apache.org/) 3.4 | The S3A filesystem and the I/O layer under the scan | Apache-2.0 |
| [Apache DataFusion Comet](https://datafusion.apache.org/comet/) 1.0 | Optional native Parquet scan and native shuffle | Apache-2.0 |
| [zstd-jni](https://github.com/luben/zstd-jni) | Shuffle and Parquet compression | BSD-2-Clause |
| [Netty](https://netty.io/) 4.2 | The transport under our Arrow Flight shuffle | Apache-2.0 |
| [Jackson](https://github.com/FasterXML/jackson) 2.21 | JSON handling in planning and config | Apache-2.0 |

Our build, test and benchmark tooling likewise rests on the open-source ecosystem: **JUnit 5**,
**ScalaTest**, **JMH**, **Spotless** / **google-java-format** / **scalafmt**, **Error Prone**, and
**Apache Maven**.

## Correctness oracles

- **Apache Spark's own SQL golden-file suite** — we run Spark's `sql-tests/` golden files with our
  extension injected and require every result to match, whether an operator ran on our path or fell
  back.
- The **[`apache/parquet-testing`](https://github.com/apache/parquet-testing) corpus** and the
  **Hardwood** test corpus — Parquet files our native reader is pinned against: every file must
  return Spark's rows or Spark's refusal.
- **TPC-H and TPC-DS**, from the [TPC](https://www.tpc.org/) — the generators behind our benchmarks.
  Our published numbers are **not** audited TPC results and are labeled as such.

## To the research community

VecRuntime is engineering applied to ideas the database research community worked out first —
vectorized and data-centric execution, late materialization, cache-conscious hashing, split-block
Bloom filters, string-view representations, morsel-driven parallelism, and overlapped I/O against
object storage. We are grateful to the researchers who published that work, often alongside the code
and benchmarks that let the rest of us build on it.

---

*If your project belongs here and isn't,
[open an issue](https://github.com/vecruntime/vecruntime/issues).*
