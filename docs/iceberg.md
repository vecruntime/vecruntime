# Iceberg support

What vecruntime accelerates for Apache Iceberg tables (Iceberg 1.12, Spark 4.1), what still falls back to Iceberg's or Spark's own code, and why. Every fallback is per operation and leaves the result unchanged: the plan keeps Iceberg's reader or writer. Run with `spark.vecruntime.explainFallback.enabled=true` to have the reason logged.

Status key: ✅ native · 🟡 native with limits · ⬜ falls back · PR = the change is in review.

## Read

| Feature | Status | Notes |
|---|---|---|
| Parquet data files, flat types | ✅ | Native scan (`VectorIcebergScanExec`), zero-copy into the engine. |
| Nested types (struct, list, map) | ✅ | |
| Column resolution: field ids, name mapping, positional fallback ids | ✅ PR #920 | Iceberg's own rules (`IcebergReadSpecs`). Before #920, files without ids and without a name mapping failed in Spark's reader. |
| Schema evolution: rename, reorder, add, drop | ✅ PR #920 | Includes a new column that reuses a renamed column's name. |
| Type promotion: int→long, float→double, decimal precision | ✅ PR #920 | Includes widening into DECIMAL128. |
| v3 initial defaults | ✅ PR #920 | |
| Metadata columns (`_file`, `_pos`, `_spec_id`, `_partition`, `_row_id`, ...) | ✅ | |
| Position deletes, deletion vectors | ✅ | |
| Equality deletes | ✅ PR #920 | Probed on the batch's columns (`EqualityDeleteSet`). Before #920 they were probed row by row. Off: `spark.vecruntime.iceberg.nativeEqualityDeletes.enabled=false`. |
| time, uuid, timestamp_ns, fixed; a decimal scale change | ⬜ | Iceberg's reader. |
| ORC / Avro data files | ⬜ | Iceberg's reader. |
| Encrypted tables | ⬜ | Iceberg's reader. |

## Write

The columnar writer is enabled with `spark.vecruntime.iceberg.writer.enabled`.

| Feature | Status | Notes |
|---|---|---|
| INSERT INTO / append | ✅ PR #913, #914 | Columnar Parquet writer (`IcebergParquetAppender`). |
| INSERT OVERWRITE, dynamic and static | ✅ PR #914 | |
| CTAS / RTAS | ✅ PR #914 | Spark plans them as an inner append. |
| Copy-on-write DELETE / UPDATE / MERGE | ✅ PR #914 | v3 row lineage is kept. |
| Partitioned tables, all transforms | ✅ PR #918 | Clustered or fanout. |
| Distribution modes none / hash / range | ✅ PR #919 | Iceberg's transform functions compile, so the exchange and sort run on batches. Needs the columnar shuffle manager. |
| Nested columns | ✅ PR #919 | Includes fanout. |
| float / double inside a list or map | ⬜ | Iceberg's writer; their metrics are not computed natively yet. |
| Merge-on-read DELETE / UPDATE / MERGE, v3 deletion vectors | ✅ PR #921 | `spark.vecruntime.iceberg.dvWriter.enabled`. With the columnar writer on, the inserted and updated rows are columnar too, and row lineage is kept. |
| Merge-on-read, v2 position delete files | ✅ PR #921 | `file` and `partition` granularity. Previous file-scoped deletes are merged. `spark.vecruntime.iceberg.positionDeletes.enabled`. |
| Merge-on-read on tables with map columns | 🟡 | The write is columnar, but its input comes as rows: Iceberg's scan of maps and Spark's `MergeRows` with maps are row-based. |
| ORC / Avro data or delete files | ⬜ | Iceberg's writer. |
| Encrypted tables | ⬜ | Iceberg's writer. |
| time, uuid, timestamp_ns, fixed, variant, geometry | ⬜ | Iceberg's writer. |

## Table properties honoured by the columnar writer

| Property | Honoured |
|---|---|
| `write.target-file-size-bytes`, `write.delete.target-file-size-bytes` | ✅ |
| `write.parquet.row-group-size-bytes`, `page-size-bytes`, `page-row-limit`, `dict-size-bytes` | ✅ |
| `write.parquet.compression-codec`, `compression-level` | ✅ |
| `write.parquet.bloom-filter-*` | ✅ |
| `write.delete.parquet.*` | ✅ Applied to delete files, overriding `write.parquet.*`. |
| `write.metadata.metrics.default`, `write.metadata.metrics.column.*` | ✅ Through Iceberg's `MetricsConfig`. |
| `write.distribution-mode`, `write.spark.fanout.enabled` | ✅ |
| `write.delete.granularity` | ✅ |
| `write.format.default` other than parquet | ⬜ Falls back. |
| Spark write options and session settings (`compression-codec`, `target-file-size-bytes`, `fanout-enabled`, `distribution-mode`, `spark.sql.iceberg.*`) | ✅ Read from the write's resolved `SparkWriteConf` (its `writeProperties`, target size and fanout), so they override the table properties exactly as in Iceberg's writer. |
| `write.data.path`, `write.object-storage.enabled` | ✅ Files are named by Iceberg's `OutputFileFactory` and the table's location provider. |
| `write.parquet.row-group-check-min-record-count` / `-max-record-count` | — Not applicable: the columnar writer tracks the open row group's size after every batch rather than sampling. |
| `encryption.*` | ⬜ Encrypted tables fall back. |

## Pending

- Metrics for float/double inside lists and maps, honouring the table's metrics modes.
- `MergeRows` with map columns.
- Partial columnar merge (#901 M4): copy unchanged row groups or column chunks without decoding them.
- 1 TB benchmarks: append (row vs vector), partitioned hash, read A/B, and a CDC MERGE for merge-on-read.
