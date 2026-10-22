---
layout: default
title: Iceberg support at a glance
description: What VecRuntime accelerates for Apache Iceberg tables, read and write, how to turn it on, and how fast it is.
---

# Iceberg support at a glance

VecRuntime runs Spark SQL on Apache Iceberg tables with its own columnar engine. Iceberg still plans the
snapshot, prunes files and commits. VecRuntime reads the Parquet data, runs the query, and writes the files.
Whenever a case is not covered, the query falls back to Iceberg's or Spark's own code, with the same result.

Iceberg 1.12 · Spark 4.1 · format versions 1, 2 and 3

**✓** native · **◐** native with limits · **✗** falls back · *review* = in an open pull request

---

## Reading

| | |
|---|---|
| Parquet data files, flat and nested columns | **✓** |
| Position deletes, deletion vectors, equality deletes | **✓** |
| Schema evolution: add, drop, rename, reorder | **✓** |
| Type promotion: int → long, float → double, wider decimals | **✓** |
| Name mapping, files without field ids | **✓** *review: files with neither now read too* |
| Partition transforms and partition-spec evolution | **✓** |
| Time travel, branches and tags | **✓** |
| v3 default values, variants | **✓** |
| ORC / Avro files, encrypted files, `time` / `uuid` / `timestamp_ns` | **✗** |

Turn it on with `spark.vecruntime.scan.nativeIceberg.enabled=true`.

## Writing *(review)*

| | |
|---|---|
| `INSERT INTO`, `INSERT OVERWRITE`, CTAS / RTAS | **✓** |
| Copy-on-write `DELETE` / `UPDATE` / `MERGE INTO` | **✓** |
| Merge-on-read `DELETE` / `UPDATE` / `MERGE INTO`: deletion vectors (v3), position deletes (v2) | **✓** |
| Partitioned tables, every transform; clustered and fanout | **✓** |
| Distribution modes none / hash / range, without Spark's row exchange | **✓** |
| Nested columns (struct, list, map) | **✓** |
| Row lineage on v3 | **✓** |
| Merge-on-read on tables with map columns | **◐** `DELETE` and `UPDATE` are columnar end to end; a `MERGE` still reads its input as rows |
| float / double inside a list or map | **✓** |
| `fixed`, `uuid` columns | **✓** |
| `variant` columns (not shredded) | **✓** |
| ORC / Avro, encrypted tables | **✗** |

Turn it on with `spark.vecruntime.iceberg.writer.enabled=true`. For merge-on-read, also set
`spark.vecruntime.iceberg.dvWriter.enabled=true`.

Table properties are honoured as Iceberg's own writer honours them: target file size, Parquet row-group,
page and dictionary sizes, codec and level, bloom filters, metrics modes, distribution mode, fanout and
delete granularity.

## How fast

<div style="display:flex;gap:2rem;flex-wrap:wrap;margin:1rem 0">
  <div><div style="font-size:2.4rem;font-weight:700">2.9×</div><div>faster 1 TB append<br>than Iceberg's writer</div></div>
  <div><div style="font-size:2.4rem;font-weight:700">2.6×</div><div>less executor CPU</div></div>
  <div><div style="font-size:2.4rem;font-weight:700">5–7×</div><div>less garbage collection</div></div>
</div>

The run (#914) inserted TPC-DS `store_sales` at sf1000, 2.88 billion rows and about 104 GB of zstd Parquet, into
an Iceberg table on 8 executors. It took 60 s with VecRuntime against 174 s with Iceberg's writer, and the
written table was identical. More results are coming for the partitioned append, reads and CDC `MERGE`.

---

The design, every fallback reason, and the tests are on the technical page,
[Apache Iceberg](iceberg.html).
