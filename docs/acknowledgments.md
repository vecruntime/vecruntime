---
title: Acknowledgments
description: The open-source projects, libraries and research without which VecRuntime would not exist.
---

# Acknowledgments

VecRuntime could not have been successful without the open-source community. It
stands on decades of work by other projects — for ideas, for interfaces, and,
just as importantly, for being the oracles we check our own correctness against.
Everything below is something this project actually uses, learns from, or
validates itself with; none of it is ours, and we are grateful for all of it.

The authoritative license and attribution record for distributed dependencies is
in the repository's [`NOTICE`]({{ site.repo_url }}/blob/main/NOTICE) and
[`LICENSE`]({{ site.repo_url }}/blob/main/LICENSE) files. This page is the human
version: who we learned from, and why.

## Projects we learned from

These projects shaped how VecRuntime is designed. We studied their approaches,
borrowed their good ideas, and in several cases ported their work directly.

- **Apache DataFusion Comet** — the closest reference for a vectorized engine
  under Spark. VecRuntime can use Comet's native Parquet scan and native shuffle
  directly when installed, our compatibility matrices are **ported from Comet's
  own expression, cast, aggregate, join and window test suites**, and much of our
  thinking about where a native engine wins against the JVM was sharpened by
  profiling against Comet. We are indebted to the DataFusion and Comet
  communities.
- **Apache DataFusion** — the Rust query engine underneath Comet, and a
  continual reference for operator design and columnar execution.
- **DuckDB** — a reference for columnar execution done well: its work on
  asynchronous I/O for remote Parquet, its string-view (Umbra-style) layout, and
  its memory governor directly informed our scan read-ahead and memory-accounting
  design. We also generate our TPC-H benchmark data with DuckDB.
- **Meta Velox** and **Umbra** — the string-view (`string_t`) representation and
  other columnar-layout ideas we researched for narrow-width string handling.
- **Apache Impala** and the **Parquet split-block Bloom filter** — the
  cache-friendly blocked Bloom filter layout our runtime filter is modeled on.

## Libraries we depend on

VecRuntime depends on, but does not bundle, the following software, each under
its own license (see [`NOTICE`]({{ site.repo_url }}/blob/main/NOTICE) for the
canonical list). Versions are those the project currently builds against.

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

Our build, test and benchmark tooling likewise rests on the open-source
ecosystem: **JUnit 5**, **ScalaTest**, **JMH**, **Spotless** / **google-java-format**
/ **scalafmt**, **Error Prone**, and **Apache Maven** with its plugin community.

## Correctness oracles

A performance engine is only useful if its answers are right. VecRuntime checks
itself, continuously, against work the community produced:

- **Apache Spark's own SQL golden-file test suite** — we run Spark's
  `sql-tests/` golden files with our extension injected and require every result
  to match Spark's, bit for bit, whether an operator ran on our path or fell back.
- **The [`apache/parquet-testing`](https://github.com/apache/parquet-testing)
  corpus** and the **Hardwood** test corpus — together 303 Parquet files that our
  native reader is pinned against: every file must return Spark's rows or Spark's
  refusal.
- **TPC-H and TPC-DS**, from the
  [Transaction Processing Performance Council](https://www.tpc.org/) — the query
  and data generators (dbgen / dsdgen) behind our benchmarks. Our published
  numbers are **not** audited TPC results and are labeled as such.

## To the academic and research community

VecRuntime is, at its core, engineering applied to ideas that the database
research community worked out first. The techniques this project leans on —
vectorized and data-centric query execution, late materialization, cache-conscious
hashing and aggregation, split-block Bloom filters, string-view representations,
morsel-driven parallelism and NUMA-aware scheduling, asynchronous and overlapped
I/O against object storage, and the long line of work on columnar storage and
query optimization — come from decades of papers, dissertations and the systems
built to test them.

We are grateful to the researchers who published that work, often alongside the
code and benchmarks that let the rest of us reproduce and build on it. Where this
project does something clever, it is usually because someone wrote the paper that
explained how. Any mistakes in applying those ideas are entirely our own.

---

*If your project or work belongs on this page and isn't here, that's an
oversight we'd like to fix — please
[open an issue]({{ site.repo_url }}/issues).*
