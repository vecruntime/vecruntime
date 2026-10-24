---
layout: default
title: Iceberg support at a glance
description: What VecRuntime accelerates for Apache Iceberg tables, read and write, how to turn it on, and what it declines.
---

# Iceberg support at a glance

VecRuntime runs Spark SQL on Apache Iceberg tables with its own columnar engine. Iceberg still plans the
snapshot, prunes files and commits. VecRuntime reads the Parquet data, runs the query, and writes the files.
Whenever a case is not covered, the query falls back to Iceberg's or Spark's own code, with the same result.

Iceberg 1.12 · Spark 4.1 · format versions 1, 2 and 3. Everything below is **on by default**.

The detailed page is [detailed Iceberg page](https://github.com/vecruntime/vecruntime/blob/main/docs/iceberg.md): the design, every fallback reason, the table-property
table and the tests.

---

## What works

**Read** — the native Parquet reader reads an Iceberg table's data files, flat and nested columns alike, and
applies the deletes itself:

- Parquet data files; schema evolution (add, drop, rename, reorder); type promotion (int→long, float→double,
  wider decimals); partition transforms and partition-spec evolution; time travel, branches and tags.
- Position deletes, deletion vectors (v3) and equality deletes, applied natively.
- Name mapping for files without field ids; filter and runtime-filter (DPP, bloom) pushdown by field id.
- Metadata columns `_file`, `_pos`, `_spec_id`, `_partition`, `_row_id`, `_last_updated_sequence_number`,
  `_deleted`.
- v3 default values and `variant` columns (shredded or not).

**Write** — a columnar writer writes the Parquet data files; Iceberg still commits:

- `INSERT INTO`, `INSERT OVERWRITE`, CTAS / RTAS.
- Copy-on-write `DELETE` / `UPDATE` / `MERGE INTO` (with byte-copy of the unchanged row groups).
- Merge-on-read `DELETE` / `UPDATE` / `MERGE INTO`: deletion vectors on v3, position delete files on v2.
- Partitioned tables, every transform, clustered and fanout; none / hash / range distribution with no Spark row
  exchange; nested columns; row lineage on v3; `fixed`, `uuid`, non-shredded `variant` and (v3) `unknown` columns.

**Row-level operation modes.** The table's `write.delete.mode` / `write.update.mode` / `write.merge.mode`
(`copy-on-write` or `merge-on-read`) is honoured per command; VecRuntime never chooses the mode.

**Maintenance.** `rewrite_data_files` (BINPACK) compacts by copying input files' Parquet row groups byte for
byte instead of decoding and re-encoding, through `VectorRewriteDataFiles`, falling back per file to Iceberg's
own bin-pack where a file is not copyable.

## v2 vs v3

| | v2 | v3 |
|---|---|---|
| Merge-on-read deletes | position delete files (`write.delete.granularity` = `partition` / `file`) | deletion vectors (Puffin), one per data file |
| Equality deletes (read) | native | native |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | — | read and preserved across copy-on-write / merge-on-read writes |
| v3 default values (`initial-default`) | — | filled natively, top level and inside structs |
| `variant`, `unknown` | — | `variant` read natively; `unknown` read as null and written by omission |

Version 1 reads as v2 without delete files.

## Table properties honoured

VecRuntime consumes Iceberg's own resolved read/write config, so a property, its per-write option and the session
setting behave as they do in Iceberg's reader/writer (option over property over default):

| | |
|---|---|
| Read | `read.split.target-size`, `read.split.open-file-cost`, `read.split.planning-lookback`, `schema.name-mapping.default` |
| File sizing | `write.target-file-size-bytes`, `write.delete.target-file-size-bytes` |
| Parquet | `write.parquet.row-group-size-bytes`, `page-size-bytes`, `page-row-limit`, `dict-size-bytes`, `compression-codec` / `-level`, `bloom-filter-*`, `write.delete.parquet.*` |
| Metrics | `write.metadata.metrics.default`, `write.metadata.metrics.column.*` |
| Layout | `write.distribution-mode` (+ delete/update/merge), `write.spark.fanout.enabled`, `write.delete.granularity`, `write.data.path`, `write.object-storage.*`, sort order, partition transforms |
| Mode | `write.delete.mode`, `write.update.mode`, `write.merge.mode` (per command) |

The full table, read and write, with honour / follow-Iceberg / ignore for each, is on the
[detailed page](https://github.com/vecruntime/vecruntime/blob/main/docs/iceberg.md#table-properties).

## Configuration switches

All default **on**; set to `false` to turn a path off.

| switch | path |
|---|---|
| `spark.vecruntime.scan.nativeIceberg.enabled` | native read |
| `spark.vecruntime.iceberg.writer.enabled` | columnar write (INSERT, CTAS, copy-on-write) |
| `spark.vecruntime.iceberg.dvWriter.enabled` | merge-on-read deletion-vector / position-delete writer |
| `spark.vecruntime.iceberg.copyUnchanged.enabled` | byte-copy of unchanged row groups in copy-on-write |
| `spark.vecruntime.iceberg.rewrite.binpackCopy.enabled` | byte-copy bin-pack in `rewrite_data_files` |

## Not implemented / falls back

These keep Iceberg's or Spark's own code, with the same result:

- **ORC and Avro** data or delete files (the reader uses Iceberg's reader for that group; the writer writes
  Parquet only, so `write.format.default` of ORC/Avro declines).
- **Encrypted** files and tables (Iceberg's `FileIO` decrypts for the read; key metadata is not written).
- **`geometry` / `geography`** types; **`time`**, **`timestamp_ns` / `timestamptz_ns`** (can't be read or
  written from Spark with Iceberg 1.12).
- **Shredded `variant`** on the write side (read natively; the columnar writer declines it).
- Metadata columns other than the seven listed above; a nested default the reader cannot fill as a constant;
  a column type outside the supported set; an `unknown` list element / map value; a partition source reached
  only through a list or map.
- A scan that reports an ordering, or a metadata-table scan (`.files`, `.snapshots`), keeps Spark's scan.
- A `rewrite_data_files` input file with deletes attached, a spec mismatch, schema evolution, unmaterialised v3
  lineage, a codec mismatch, an unreadable encoding, or a NaN-tracked column is decoded/re-encoded rather than
  byte-copied (same output as Iceberg's bin-pack).

`spark.vecruntime.explainFallback.enabled=true` logs the reason for each declined scan or write.

## Verified against Iceberg's own tests

The plugin runs **Apache Iceberg 1.12.0's own Spark 4.1 test suites** with every path on
(`benchmarks/iceberg-tests/`): **1334 / 0** on the extensions suite and **579 / 0** on the core suite, with 9
triaged plugin-specific exclusions (plan-shape and byte-size assertions, not correctness).

---

The design, every fallback reason, and the tests are on the detailed page,
[the detailed Iceberg page](https://github.com/vecruntime/vecruntime/blob/main/docs/iceberg.md).
