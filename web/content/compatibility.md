---
layout: default
title: Compatibility
description: The Spark versions, JDK, data types, file formats and features VecRuntime runs on, and what falls back to Spark.
---

# Compatibility

VecRuntime is a Spark SQL plugin that runs core operators on its own columnar engine and falls back to
Spark — transparently, with the same result — for anything it does not cover. This page states what it
runs on and what it does not implement. Two companion pages carry the per-case detail: the
[tested-against-Spark matrix](https://github.com/vecruntime/vecruntime/blob/main/docs/compatibility.md)
(every row a query run with the plugin on and off and compared), and
[Testing & correctness](https://github.com/vecruntime/vecruntime/blob/main/docs/testing.md).

---

## Runtime

| | |
|---|---|
| **Spark** | 4.1 or 4.2 (a jar built for that line) |
| **Scala** | 2.13 |
| **JDK** | 25 — required for the Java Vector API (`jdk.incubator.vector`) |
| **Arrow** | 18.3.0 (Spark 4.1's version) |
| **Engine** | Pure JVM — no native code, JNI or serialization boundary |

Launch needs three JVM flags on driver and executors: `--add-modules=jdk.incubator.vector`,
`--enable-native-access=ALL-UNNAMED` and `--sun-misc-unsafe-memory-access=allow` (Arrow's allocator
fails without the last on JDK 25). On JDK 25, Spark 4.1.3's bundled Hadoop must be swapped to 3.4.3
([HADOOP-19212](https://issues.apache.org/jira/browse/HADOOP-19212)). The full launch recipe is on the
[running page](https://github.com/vecruntime/vecruntime/blob/main/docs/running.md).

## Data types

| Type | Status |
|---|---|
| `boolean`, `int`, `bigint`, `double`, `date`, `timestamp`, `timestamp_ntz` | **computed** natively |
| `decimal(p ≤ 18)` | **computed** (64-bit lane) |
| `decimal(p > 18)` up to `decimal(38,s)` | **computed** (128-bit lane): carried, arithmetic, casts, group/join/sort keys, sum/avg |
| `string` (`UTF8_BINARY`) | **computed** — comparisons, matching, the string function family |
| `string` with a non-binary **collation** | **carried**, used as a join/group/sort key; character functions stay Spark's |
| `binary` | **carried**, compared and grouped as bytes; character functions stay Spark's |
| `time`, ANSI intervals (`interval day to second` / `year to month`) | **carried** on their INT64/INT32 lanes (comparison, grouping, sorting, joins, windows, count/min/max/first/last); arithmetic, casts and sum/avg stay Spark's |
| `variant` (Spark 4) | **read** natively (shredded or not), **carried** as a column without a lane |
| `array`, `map`, `struct` | **carried** by filters, projections, sorts, joins, unions, limits, windows; `struct` fields are read (`col.a.b`); any operator that *computes* on one falls back |
| `float`, `tinyint`, `smallint` | **no lane** — `int → byte/short` compiles onto the int lane, but a `float` column makes a reading operator fall back |
| `geometry`, `geography` (Spark 4.2) | **not implemented** — falls back |

## File formats

| Format | Read | Write |
|---|---|---|
| **Parquet** | native (`VectorParquetScanExec`) — v1 and v2 pages, flat and nested, every encoding Spark reads | native (`ColumnarParquetWriter`) — Parquet **v2 only** |
| **Iceberg** (format v1/v2/v3) over Parquet | native — schema evolution, deletes, time travel, metadata columns | native — insert, CTAS, copy-on-write and merge-on-read |
| **ORC** | columnar input through the adapter seam (read by Spark/Iceberg); not the native reader | not written |
| CSV, JSON, text, Avro, JDBC | row-based — out of scope, falls back | — |

Falls back on either side: **encrypted** Parquet/Iceberg, Parquet **v1 on write**, **shredded
variant on write**, geospatial types, and the few reader corners listed on the detailed Parquet and
Iceberg pages.

## Features

| Feature | Status |
|---|---|
| Operators (filter, project, aggregate, sort, window, limits, union, joins, range, expand, generate, sample) | native — see [Operators](operators.html) |
| Expressions and aggregate functions | native where a kernel exists — see [Expressions](expressions.html) |
| Columnar shuffle (Arrow IPC over Arrow Flight) | native, with the `vecruntime-shuffle` jar; Spark's shuffle still serves every exchange the plugin does not produce |
| DataFusion Comet interop (scan, shuffle, mixed operators) | zero-copy — [Comet page](https://github.com/vecruntime/vecruntime/blob/main/docs/comet.md) |
| Planner rules (optimizer rewrites, DPP) | general SQL, each with its own switch — see [Planner rules](aqe-dpp-rules.html) |
| ANSI mode | honoured — the same overflow / divide-by-zero / invalid-cast errors as Spark |
| Structured Streaming operators, pickled Python UDFs, Scala UDFs as kernels | not implemented |

Every unsupported case keeps Spark's own operator and returns Spark's result, with the reason logged
when `spark.vecruntime.explainFallback.enabled=true`.
