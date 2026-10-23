---
layout: default
title: Apache Iceberg
---

# Apache Iceberg

VecRuntime reads Iceberg tables through Iceberg's own planning and runs its columnar operators over the result.
Iceberg decides the snapshot, branch or tag, manifest and partition pruning, residual filters, runtime filtering
and task groups. VecRuntime decides how those task groups are read and runs the operators above them. Writes are
Spark's and Iceberg's, apart from an opt-in columnar deletion-vector writer for v3 tables.

**In short (2026-10-19):**
- **Native scan:** with `spark.vecruntime.scan.nativeIceberg.enabled=true` (off by default), our native Parquet
  reader reads the data files of an Iceberg table, flat and nested columns alike. It applies position deletes,
  deletion vectors and equality deletes itself, and reads `_file`, `_pos` and `_spec_id`.
- **When it is declined:** the whole scan or a single task group is read by Iceberg's vectorized reader, and our
  operators adapt its batches. That reader is also the path when the switch is off. Declined are some v3 and
  other column types, other metadata columns, a nested default our reader cannot fill as a constant, and ORC,
  Avro or encrypted files.
- **No Comet:** Comet is not needed for any of this.
- **Writes (#913-#921, #936):** a columnar Iceberg writer covers `INSERT`, `INSERT OVERWRITE`, CTAS/RTAS,
  copy-on-write and merge-on-read `DELETE` / `UPDATE` / `MERGE INTO`, partitioned tables and hash and range
  distribution. Merge-on-read writes deletion vectors on v3 and position delete files on v2. It is behind
  `spark.vecruntime.iceberg.writer.enabled`. See [the columnar writer](#the-write-side-the-columnar-iceberg-writer-842-901).
  A one-page summary is in [Iceberg support at a glance](iceberg-support.html).

The tables below give the state of each feature, by format version.

## What is supported

The **Native** column says whether our reader reads the data itself:
- **yes:** our reader.
- **group:** Iceberg's reader for the task groups that need it, ours for the rest.
- **scan:** Iceberg's reader for the whole scan.
- **row:** Spark's row path, when Iceberg's reader is not vectorized for the column.

Every path gives Iceberg's rows. The fallbacks are slower, not wrong.

Format version 1 reads as version 2 does, without delete files.

### Format version 2

| Feature | Native | How |
|---|---|---|
| Parquet data files | yes | `VectorIcebergScanExec` reads each task group of Parquet data files as Spark file splits, with the read-ahead and decode-ahead of our Parquet scan |
| ORC and Avro data files | group | Iceberg's reader for that group, inside the same node -- its columnar reader when Iceberg vectorizes the scan, else (a nested-column scan, #901 item 6) its row reader converted to batches. The other groups stay native |
| Snapshots, time travel, branches and tags | yes | Iceberg's planning: `VERSION AS OF`, `TIMESTAMP AS OF` and branch reads plan the snapshot's files, which we then read |
| Position deletes | yes | The file is read with its row positions. Iceberg's `BaseDeleteLoader` loads the deletes and the deleted rows are deselected, with no copy |
| Equality deletes | yes, or group | Read natively when every equality key is a native type, at the top level or (#901 item 5) nested inside a struct -- a nested key is read through its ancestor struct (null leaf under a null parent, as Iceberg's accessors), and reshaped to the delete schema when the query also projects that struct. Columns the query does not project are added for the match and dropped afterwards. Iceberg's `DeleteFilter` semantics apply, nulls matching nulls. A key of a type our reader does not decode sends the group to Iceberg's reader |
| Schema evolution: add, drop, rename, reorder | yes | Columns are matched by Iceberg field id, never by name. A column added after a file was written reads as nulls from that file |
| Type promotion (int to long, float to double, decimal precision) | yes | The native reader widens a file's narrower type as it does for Parquet tables, with and without position deletes and deletion vectors; a pushed filter compares the old files' narrow statistics with the wide literal. A decimal promoted past 18 digits reads its 64-bit files into a DECIMAL128 lane |
| Name mapping (`schema.name-mapping.default`) | yes | A file written without field ids (a table migrated or imported from plain Parquet, `migrate` or `add_files`) gets each column's Iceberg field id from the mapping by name, nested fields included, before our reader resolves its columns. Old names stay aliases after a rename, as Iceberg keeps them. A file that falls back to Spark's reader is read by name (#849) |
| Hidden partitioning and partition transforms (identity, bucket, truncate, year, month, day, hour) | yes | Iceberg's planning prunes by every transform. Partition values are read from the files, not filled in as constants |
| Partition-spec evolution | yes | Each file is planned and pruned under its own spec |
| Filter pushdown | yes | The filter above the scan skips row groups and pages by their statistics. Each column is matched to the file's by field id, so a renamed column is tested under its name in the file, and a conjunct on a column a file lacks is not pushed for that file |
| Spark's runtime filters (dynamic partition pruning, bloom filters) | yes | Iceberg's `SupportsRuntimeV2Filtering` prunes task groups. A broadcast join's build keys also skip row groups in our reader (#610) |
| Our planner rules on Iceberg | yes | Dynamic partition pruning through aggregates and transitively (`DppThroughAggregate`, `TransitiveDpp`); fact bloom filters; broadcast and runtime-filter sizing by file bytes; merged scalar subqueries (#735). Our pruning rules prune on the source column of every partition transform (identity, bucket, truncate, year, month, day, hour) in every spec the table has had, so files written under an older spec are pruned by that spec's fields (#806) |
| Task-group packing | yes | Task groups are packed into Spark tasks as Spark packs a Parquet scan's splits (`...nativeIceberg.packSplits`) |
| Storage-partitioned joins | yes | A scan that reports a partitioning keeps Iceberg's groups, unpacked. A scan that reports an ordering is read by Iceberg's reader (scan) |
| Metadata columns `_file`, `_pos`, `_spec_id` | yes | `_file` and `_spec_id` are per-split constants; `_pos` is the reader's row index |
| Metadata columns `_partition`, `_row_id`, `_last_updated_sequence_number` | yes (#20 M3a) | `_partition` is the per-file struct of the table's unified partition type (empty on an unpartitioned table, where Iceberg returns it null); `_row_id` and `_last_updated_sequence_number` are the file's own column where it materialises them (copy-on-write, compaction), else `firstRowId + _pos` / `dataSequenceNumber`. All three come from `PartitionUtil.constantsMap` and the data file, per batch |
| Metadata column `_deleted` | yes (#901 item 3) | Computed by the scan: `false` for a live row, `true` for a row a position delete, deletion vector or equality delete marks. When `_deleted` is projected the scan keeps the marked rows instead of dropping them, exactly as Iceberg's reader does, so a changelog or `SELECT _deleted` read sees them |
| Types: boolean, int, long, float, double, date, timestamp (with zone and without), string, binary, decimal | yes | `decimal(p > 18)` becomes a DECIMAL128 lane |
| Types: uuid, fixed, time | scan | Iceberg's reader reads them as Spark types our reader does not produce from these files |
| Types: struct, list, map | yes | At any depth, when every leaf is one of the flat types above. Nested struct fields are matched by their Iceberg field ids, so a renamed nested field reads as Iceberg reads it, and a field added to a struct later reads as nulls from older files. Iceberg's reader does not vectorize nested columns; a task group our reader cannot read (an ORC/Avro file, an encrypted file) is read by Iceberg's own row reader inside the node and converted to batches (#901 item 6), so the other groups stay native. Only a scan with no native group at all keeps Spark's row path |
| Encrypted files | group | Iceberg's `FileIO` decrypts them |
| `DELETE`, `UPDATE`, `MERGE INTO` (merge-on-read) | yes, with `spark.vecruntime.iceberg.writer.enabled` (#921) | Their reads run on our operators, and the `MERGE INTO` join and `MergeRows` are columnar (#21, #273). The writes are columnar too: position delete files on v2, deletion vectors on v3, and the inserted and updated rows through the columnar data writer. Off, Spark's `WriteDeltaExec` and Iceberg's writers write them |
| Copy-on-write rewrites, `INSERT`, CTAS | yes, with `spark.vecruntime.iceberg.writer.enabled` (#913-#919) | Written by the columnar writer, see [the columnar writer](#the-write-side-the-columnar-iceberg-writer-842-901). Off, the write is Spark's row path into Iceberg's Parquet writer |

### Format version 3

Everything in the version 2 table applies. The version 3 additions:

| Feature | Native | How |
|---|---|---|
| Deletion vectors (Puffin) | yes | Read once per data file, and the deleted rows are deselected as for position deletes |
| Writing deletion vectors | opt-in | `spark.vecruntime.iceberg.dvWriter.enabled` (off by default) builds each data file's `PositionDeleteIndex` from our batches and hands it to Iceberg's DV writer, for `DELETE`, `UPDATE` and `MERGE INTO`. The commit stays Iceberg's `RowDelta`. See [the write side](#the-write-side-columnar-v3-deletion-vectors-20) |
| Default values: `initial-default` | yes, or scan | A column's default is filled into the files written before the column existed, as a constant (Spark's existence default), for boolean, int, long, float, double, decimal, string, binary, date and timestamp columns -- at the top level and (#901 item 4) for a field added inside a struct, which returns the default from older files, not null. A default of another type keeps Iceberg's reader for the scan |
| Default values: `write-default` | n/a | Applied by Iceberg's writer |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | yes (#20 M3a) | The file's own column where it materialises them, else `firstRowId + _pos` / `dataSequenceNumber`, exactly as Iceberg derives them from the manifests and the files' first row ids |
| `variant`, shredded or not | yes | Iceberg writes the spec's Parquet variant group, with `typed_value` when `write.parquet.shred-variants` is on. Our Parquet reader reads it as it reads Spark's variants (#787, #812), at the top level and inside structs, under deletes. Iceberg's own vectorized reader cannot read a shredded variant ("Unsupported variant: shredded typed_value primitive"); our scan reads it either way |
| `timestamp_ns`, `timestamptz_ns` | scan | |
| `geometry`, `geography` | scan or row | |
| `unknown` | scan | |
| Table encryption | group | As encrypted files above |

## How the native scan reads a table

`VectorIcebergScanExec` wraps the `BatchScanExec` Spark planned for an Iceberg scan and keeps its input partitions
(the task groups). Per task group:

- **Parquet data files with no delete files** are read by our `VectorParquetPartitionReader` as Spark file
  splits, matched by field id.
- **A file with position deletes or a deletion vector** is read on its own with its row positions (Spark's
  temporary row-index column). Its deletes are loaded by Iceberg's `BaseDeleteLoader`, and the deleted rows are
  deselected as a selection, without a copy.
- **A file with equality deletes** is read the same way, together with its equality columns. Each group of
  equality delete files is loaded once per Spark task into a set, and a row whose values are in it is deselected.
- **Any other group** (an ORC or Avro file, an encrypted file, a delete our reader cannot apply) is read by
  Iceberg's columnar reader inside the same node when Iceberg vectorizes the scan, or (#901 item 6) by Iceberg's
  own row reader inside the node, converted to batches, when it does not (a nested-column scan); either way the
  other groups stay native and its rows match what Iceberg's own reader returns.

The node reports how many task groups each reader read (`numNativeGroups`, `numIcebergGroups`), plus files, row
groups and pages skipped.

At plan time the node declines, and Spark's `BatchScanExec` stays:
- a scan that reports an ordering;
- a nested-column scan (Iceberg not vectorized) with NO task group our reader can read, or whose input is a
  metadata table (`.files`, `.snapshots`) rather than data-file groups;
- a metadata column other than `_file`, `_pos`, `_spec_id`, `_partition`, `_row_id`,
  `_last_updated_sequence_number` and `_deleted`;
- a nested or top-level default our reader cannot fill as a constant;
- a column type outside the flat types in the table above, or a nested type with such a leaf.

The switch is off by default until the TPC-DS Iceberg A/B is in.

## Iceberg's vectorized reader under our operators

When the native scan is off or declines, Iceberg's vectorized reader (`BatchScanExec`) feeds our operators
directly. `IcebergVectorAdapter` is registered reflectively, so the plugin has no compile-time Iceberg
dependency. It adapts `IcebergArrowColumnVector` batches:

- Data and offset buffers are wrapped as `MemorySegment`s over Iceberg's shaded Arrow buffers.
- Iceberg turns off Arrow's null checking and keeps nulls in a byte-per-row `NullabilityHolder`. The adapter
  builds the validity bitmap from it (`ArrowLayout.validityFromNullBytes`) into the batch's scratch arena; the
  data buffers are not copied.
- Dictionary-encoded strings arrive as indices plus the row group's dictionary. The indices are wrapped in place,
  and the dictionary is decoded once per batch so the aggregate's dictionary-key paths apply. A dictionary
  larger than the batch is copied per row instead.
- Merge-on-read batches come with Iceberg's `int[] rowIdMapping`, a `ColumnVectorWithFilter` around every column.
  They are unwrapped into a selection over the physical rows (`SelectedColumnarBatch`) where batches enter our
  operators.
- Wide decimals arrive as big-endian `FixedSizeBinaryVector`s and are converted limb by limb into DECIMAL128 lanes
  (#257). Decimals of up to 9 digits are widened into INT64 lanes.
- Anything else is copied: constant partition columns, dictionary-encoded non-string columns, 64-bit-offset
  strings. The result is the same, only slower.

Comet's native Iceberg scan (`CometIcebergNativeScanExec`) is still adapted zero-copy when Comet is on the
classpath, but none of the above needs it.

## Catalog settings for short queries (#735)

Every query plans its Iceberg scans on the driver, and with the catalog's defaults that planning
reads S3. A Parquet table's file listing stays cached in the session, so on short queries the
Iceberg copy starts its first job later. A driver JFR profile of seven short 3 TB TPC-DS queries
(q21, q77, q1, q7, q25, q10, q60) found about 1.4 s of driver time per query blocked on S3,
against under 0.1 s before the first job on Parquet; our own planner rules took a few samples.
Two Iceberg defaults cause it:

- **The catalog's table cache expires** 30 s after a table was last used
  (`cache.expiration-interval-ms`), so most queries load the table again. With a Hadoop catalog
  on S3 each load is several round trips (the version hint, a listing, the metadata file).
- **Manifests are not cached** (`io.manifest.cache-enabled` is `false`), so each scan reads its
  table's manifest list and manifests again while Spark asks it for its task groups.

For a read-heavy workload over tables that are not being written during the session, set both on
the catalog:

```
spark.sql.catalog.<name>.cache.expiration-interval-ms=-1
spark.sql.catalog.<name>.io.manifest.cache-enabled=true
spark.sql.catalog.<name>.io.manifest.cache.expiration-interval-ms=3600000
```

`-1` keeps a loaded table until the session ends: a commit by another writer is then not seen
until `REFRESH TABLE` (or a new session), so leave it at its default where tables change under
running sessions. The manifest cache holds manifest files' bytes in the driver's heap, keyed by
path (manifests are immutable, so it cannot serve stale data), up to
`io.manifest.cache.max-total-bytes` (100 MB by default; a 3 TB TPC-DS fact table has one manifest
of about 1 MB).

On 15 short 3 TB TPC-DS queries (10 nodes, two passes each) the two settings took Iceberg's total
from 86.4 s to 79.3 s, against 62.1 s for the Parquet copy; q21, q10 and q27 came level with
Parquet. They are catalog properties, so the plugin cannot set them for you.

## The write side: the columnar Iceberg writer (#842, #901)

### Design

The columnar Iceberg writer reuses Iceberg for everything except encoding rows:

- **Iceberg's own pieces, unchanged:** the Spark `Write` Iceberg builds (its `SparkWriteConf`: target size,
  fanout, distribution, write properties, session settings), its `OutputFileFactory` (file names and the table's
  location provider), `MetricsConfig`, and its commit: `AppendFiles`, `OverwriteFiles` or `ReplacePartitions`
  for an append, overwrite or copy-on-write write, and `RowDelta` for merge-on-read.
- **Ours:** the Parquet encoding. `IcebergParquetAppender` (#913) is a `FileAppender` over the Spark-free
  columnar Parquet writer of the `parquet` module (#900). It writes Iceberg's field ids and schema into the
  footer, and fills `Metrics` from the footer and Iceberg's own field-metrics builders.
- **How the plan is taken:** `VectorIcebergWriteStrategy` plans after Spark's `V2Writes`, so the distribution
  and ordering Iceberg asks for are already in the child plan. The write consumes the child's batches;
  `RowToColumnarExec` is used only when the child produces rows.
- **Package-private Iceberg classes:** `SparkWrite`, `SparkPositionDeltaWrite`, their `Context` and the
  `DeltaTaskCommit` constructors are package-private. The bridge classes that touch them live in
  `org.apache.iceberg.spark.source` in the optional `iceberg-bridge` module, and the `spark` module loads them
  reflectively, so the plugin keeps no compile-time Iceberg dependency.

Per step:

- **#913, appender.** `IcebergParquetAppender`, with a JMH against Iceberg's `Parquet.write`.
- **#914, write node.** `VectorIcebergWriteExec` for append, dynamic and static overwrite, CTAS/RTAS (Spark
  plans them as an inner append) and copy-on-write `DELETE` / `UPDATE` / `MERGE`.
  - On v3, copy-on-write maps each write-schema field to its ordinal in the row or the metadata projection, so
    `_row_id` and `_last_updated_sequence_number` are kept as Iceberg's writer keeps them.
  - Tasks report Spark's output metrics (records and bytes written) from their data files.
- **#918, partitioned writes.** Clustered (one open file; each partition's run is a slice of the batch) and
  fanout (one open file per partition; rows gathered per partition with the engine's gather kernels).
- **#919, native distribution.** Iceberg's transform functions (`years`, `months`, `days`, `hours`, `bucket`,
  `truncate`, which reach the plan as `StaticInvoke`) compile into columnar kernels (`IcebergTransformKernels`).
  Hash and range distribution therefore run on batches, through the columnar shuffle manager, with no `SortExec`
  or `ShuffleExchangeExec` left. It also adds nested constructors (`array`, `map`, `if(p, null, nested)`) and
  fanout writes of nested columns (`NestedGatherKernels`).
- **#901 item 11, per-lane partition keys.** `PartitionKeys` computes each row's partition on lanes with the
  same `IcebergTransformKernels` — Murmur3 `bucket`, `truncate`, and the time transforms — so the write never
  boxes a value per row (no `CharSequence` per string, no `BigDecimal`/`BigInteger` per decimal). `bucket` of a
  decimal reads the unscaled value's minimal two's-complement bytes straight from the `long` (precision ≤ 18)
  or the 16-byte unscaled (wider), matching Iceberg's `BucketUtil.hash(BigDecimal)` byte for byte. Rows are
  grouped by hashing the result lanes and comparing, so one `PartitionKey` is materialised per distinct
  partition in a batch rather than one per row. A partition source may be a top-level field or a field nested
  inside (possibly nested) structs: the leaf lane is read through the struct children, null when the leaf or any
  enclosing struct is null, as Iceberg's accessor reads it.
- **#921, merge-on-read.** `VectorWriteDeltaExec` runs on batches.
  - A task walks each batch's operation lane. DELETE rows feed per-file deletion vectors on v3, or position
    delete files on v2 (`ColumnarPositionDeleteWriter`); both sit behind one `DeleteSink`.
  - INSERT and REINSERT rows are picked out by row index and written by the columnar data writer. A REINSERT
    keeps its row lineage; an INSERT leaves it to the commit (`ExtractRowLineage`).
  - An UPDATE's `Expand` carries the struct `_partition`, so the whole UPDATE plan stays columnar.
  - v2 position delete files follow Iceberg's `FanoutPositionOnlyDeleteWriter` / `SortingPositionOnlyDeleteWriter`:
    - files in path order and positions ascending;
    - `file` granularity merges and replaces a data file's previous file-scoped deletes;
    - files roll at the delete target size;
    - a file referencing several data files drops the `file_path` / `pos` bounds.

### What falls back to Iceberg's writer, and why

| Case | Why |
|---|---|
| Geometry types; a shredded `variant` (`write.parquet.shred-variants`) | Not encoded by the columnar writer yet. `time` and `timestamp_ns` columns cannot be read or written from Spark with Iceberg 1.12 at all |
| ORC or Avro data or delete files | The columnar writer writes Parquet only |
| Encrypted tables | Key metadata is not written |
| A partition source reached through a list or a map (not a single lane) | The transform needs the source as one lane; a top-level or struct-nested leaf is read on lanes, a list / map element is not |

`spark.vecruntime.explainFallback.enabled=true` logs the reason for each declined write.

### Table properties

| Property | Honoured |
|---|---|
| `write.target-file-size-bytes`, `write.delete.target-file-size-bytes` | yes |
| `write.parquet.row-group-size-bytes`, `page-size-bytes`, `page-row-limit`, `dict-size-bytes` | yes |
| `write.parquet.compression-codec`, `compression-level` | yes |
| `write.parquet.bloom-filter-*` | yes |
| `write.delete.parquet.*` | yes; for delete files they override `write.parquet.*` |
| `write.metadata.metrics.default`, `write.metadata.metrics.column.*` | yes, through `MetricsConfig` |
| `write.distribution-mode`, `write.spark.fanout.enabled`, `write.delete.granularity` | yes |
| Write options and session settings (`compression-codec`, `target-file-size-bytes`, ...) | yes; read from the write's resolved `SparkWriteConf`, so they override table properties as in Iceberg's writer |
| `write.data.path`, `write.object-storage.enabled` | yes, through `OutputFileFactory` and the location provider |
| `write.parquet.row-group-check-*` | not applicable: the open row group's size is tracked after every batch |

### Tests

Every case compares the result with a twin table written by Iceberg's own writer: the same rows, the same
partitions with the same record counts, and the snapshot summary.

- **`VectorIcebergWriteSuite`:**
  - append, overwrite, CTAS/RTAS, copy-on-write `DELETE` / `UPDATE` / `MERGE`, and v3 lineage;
  - five partitionings (identity; days; months with bucket; hours with truncate; years with decimal truncate)
    under none, hash and range distribution, with fanout off and on, asserting no Spark `SortExec` or
    `ShuffleExchangeExec` under the write;
  - partition sources inside a struct (`bucket`/`truncate`/`identity` over `st.x` / `st.y`), clustered and fanout;
  - nested columns, clustered and fanout;
  - output metrics, checked through a listener;
  - reading a table with a null identity partition through the engine.
- **`VectorDvWriteSuite` and `VectorDvWriteColumnarSuite`:**
  - v3 `DELETE`: plain, empty, repeated and partitioned;
  - an aborted task;
  - `UPDATE` and `MERGE` on unpartitioned and partitioned tables, and with struct, array and map columns;
  - an insert-only `MERGE`.

  Each is compared on DV cardinality, one live DV per data file, `_row_id` of the original rows, and snapshot
  counts. The columnar suite also asserts that no flat delta write reads rows.
- **`VectorPositionDeleteWriteSuite` and its columnar variant:** v2 tables, unpartitioned and partitioned, with
  `file` and `partition` granularity. Each runs `DELETE`, a second `DELETE`, `UPDATE` and `MERGE`, compared
  statement by statement on delete-file kind and format, and on referenced data files. A roll test is included.
- **`IcebergTransformExprsSuite`:** every transform over every source type, equal to Iceberg's own `invoke` row
  by row.
- **`PartitionKeysSuite`:** `PartitionKeys` against Iceberg's own `PartitionKey`, row for row, for every
  transform over every source type (nulls, negatives, pre-1970 dates and decimals included), over several
  batches and many distinct keys, with its grouping agreeing with Iceberg's key equality; and the same for
  partition sources nested inside (doubly-nested) structs, with null structs.
- **`StructFlatteningSuite`:** the columnar exchange's handling of a null struct constant (#922).

### Measured

**1 TB append, #914.** TPC-DS `store_sales` at sf1000: 2.88 billion rows, about 104 GB of zstd Parquet. An
`INSERT INTO` an unpartitioned v2 table, 8 executors × 13 cores, two iterations:

| writer | wall (s) | executor CPU (s) | GC (s) |
|---|---:|---:|---:|
| Iceberg's | 181.2 / 166.8 | 18,246 / 17,399 | 388 / 283 |
| columnar | 60.8 / 59.7 | 6,937 / 6,841 | 56 / 58 |

- **Speed:** 2.89x faster on the mean wall time, with 2.6x less CPU.
- **Correctness:** the row count and `sum(cast(xxhash64(*) as decimal(38,0)))` of the written table are
  identical in every run.

The partitioned (hash) append, the read A/B for #920, and a CDC `MERGE` A/B for #921 follow on their PRs.

### Bugs found on the way

- **The columnar exchange and a null struct constant (#922).** Spark's `ConstantColumnVector` of a null struct
  leaves its children unset. A partitioned `MERGE` carries one as the `_partition` of its inserted rows, and the
  exchange failed on it.
- **Copying a foreign vector with nulls.** Iceberg's `ConstantColumnVector` of a null partition value throws
  when a null row is read, and the copy path's bulk getters read every row. The 1 TB partitioned run hit it on
  `ss_store_sk`. Fixed on #919, and separately for `main`.

## The write side: columnar v3 deletion vectors (#20)

On the **write** side, the DELETE half of a merge-on-read `DELETE` / `UPDATE` / `MERGE INTO` is Spark's
row-by-row `WriteDeltaExec` driving Iceberg's delete writer. On a **format-version 3** table (deletion vectors in
Puffin files) that per-row routing can be replaced by a columnar path. Our batches already carry `_file` / `_pos`
as lanes, and the delta write's REBALANCE exchange already clusters by `_spec_id` / `_partition` / `_file`. So a
`PositionDeleteIndex` bitmap can be filled per data file and handed to Iceberg's bulk
`BaseDVFileWriter.delete(path, index, spec, partition)` once per file. The commit stays Iceberg's own `RowDelta`.
Inserts and the insert half of updates stay on Iceberg's data writer; v2 tables and unsupported shapes go to
Spark's writer unchanged.

It is behind `spark.vecruntime.iceberg.dvWriter.enabled`, off by default. It covers DELETE, UPDATE and MERGE,
partitioned tables and repeated deletes. On a CDC MERGE, though, the write is a small share of the statement, so it
stays opt-in until a released Iceberg shows a clear gain. The design, the placement decision (an optional
Iceberg-compiled bridge module or reflection) and the profile that gates the work are in
[`docs/iceberg-dv-writer.md`](iceberg-dv-writer.md).

### Write-stage parallelism: the advisory partition size

In a `MERGE` / `UPDATE` the delta write runs its two halves in separate tasks. The deletes are clustered by data
file, while the inserted rows (updated rows' new versions and inserts) have no file and land together in their own
partitions, which AQE splits to Iceberg's advisory partition size. Those insert tasks are the stage's critical
path, since they write Parquet and are CPU-bound. On the cluster CDC `MERGE` (`v3.dvmix_20_5`, a 10 % change batch
of 57.6 M rows, 8 executors / 112 slots), the default 384 MiB target split the insert half into 14 tasks and left
most slots idle:

| `spark.sql.iceberg.advisory-partition-size` | insert tasks | write stage | MERGE |
|---|---|---|---|
| default (384 MiB) | 14 | ~18 s | ~84 s |
| 128 MiB | 23-24 | 12-13 s | ~76 s |

A smaller target writes more, smaller data files (here about 1.6x as many), which costs at read time (planning and
per-file overhead) what it saves at write time. So it is a per-workload choice. Set it for a session with
`spark.sql.iceberg.advisory-partition-size` (bytes), or on the table with
`write.spark.advisory-partition-size-bytes`, when write latency matters more than file size. Neither the DV writer
nor our shuffle changes this trade-off; the numbers are the same with the writer on and off (#20).

## Other limits

- `MERGE INTO`'s columnar `MergeRows` (`VectorMergeRowsExec`, #21) converts when the merge's join is ours. Since
  #273 it is: the target is the streamed side of that join, and it carries the struct `_partition` column beside
  `_file` / `_pos` / `_spec_id` as a pass-through. A lane-less column on the join's *build* side is still refused.
- Over Iceberg's reader, the per-batch dictionary decode is not cached across the batches of a row group.
- Over Iceberg's reader, merge-on-read batches pay Iceberg's per-batch `int[]` mapping (19 % of a full-scan probe
  for us at SF1 on v3). Converting it back into a bitmap costs 1.5 %. Removing the mapping needs an Iceberg-side
  option; the native scan has no mapping.

## Tests

```bash
mvn -Piceberg -pl spark verify -Dsuites=io.vecruntime.spark.iceberg.IcebergNativeScanSuite
mvn -Piceberg -pl spark verify -Dsuites=io.vecruntime.spark.iceberg.IcebergScanSuite
```

- **Merge-on-read battery:** `IcebergNativeScanSuite` (the native scan) and `IcebergScanSuite` (Iceberg's reader
  under our operators) share `IcebergMorSuiteBase` and run the same queries. Only the expected scan node differs.
- **Native scan:** `IcebergNativeScanSuite` also covers the native readers of position deletes, deletion vectors
  and equality deletes; struct, list and map columns (with renamed and added nested fields, under deletes); the
  metadata columns; v3 initial defaults; renamed, added and promoted columns (int to bigint, float to double,
  decimal precision, on v2 and v3 with deletes); pushed and runtime filters by field id; and task-group packing.
- **Planner rules on Iceberg:** `IcebergDppSuite`, `IcebergFactBloomFilterSuite`, `IcebergBroadcastBySizeSuite`,
  `IcebergRuntimeFilterBySizeSuite` and `IcebergMergeAggregatesSuite`.
- **DV writer:** `DvWriteStrategyFallbackSuite` and `DvWriteProfileSuite`.

The tables (`IcebergTables`) are written through Spark's Iceberg catalog into a temporary Hadoop warehouse:

- `t_pos` (v2): `DELETE` producing positional delete files, then an `UPDATE`;
- `t_dv` (v3): the same, encoded as deletion vectors;
- `t_eq` (v2): positional deletes plus an equality delete file on the key column, written with the Iceberg Java
  API (`GenericAppenderFactory.newEqDeleteWriter`, committed with `newRowDelta`), the shape a streaming CDC writer
  produces;
- `lineitem` (v2): the TPC-H shaped table with about 2% of the rows deleted, for Q1 and Q6;
- `t_merge_on` / `t_merge_off`: six further DELETE/UPDATE rounds on top of `t_pos`, then a `MERGE INTO` with
  updates, deletes and inserts, run with the plugin on and off. The two tables must be identical afterwards, and
  the merged table is then queried with our operators.

Every query is compared against Spark with the plugin disabled (`checkVectorized`), and the suites assert the
expected scan node sits directly under our operators.

## Measuring merge-on-read reads: the local harness (#260)

The adapter path's claim is that a delete bitmap consumed in 64-row blocks beats Spark's `int[] rowIdMapping`,
followed per column access per row. It is measured on TPC-H `lineitem` tables carrying the delete shapes a
lakehouse table has between compactions. Everything runs on a laptop at SF1 (an evening at 5 warm-up and 10
measured iterations per cell); the 1 TB variant is #247/#249.

```
benchmarks/scripts/gen-tpch.sh 1                                 # the Parquet lineitem the variants are built from
benchmarks/scripts/gen-iceberg-mor.sh benchmarks/data/sf1        # every variant into benchmarks/data/iceberg, namespace sf1
benchmarks/scripts/gen-iceberg-mor.sh benchmarks/data/sf1 sf1 --variants plain,pos_10,dv_10
benchmarks/scripts/run-tpch.sh benchmarks/data/sf1 spark,vector \
  --iceberg benchmarks/data/iceberg --variant sf1.pos_10 \
  --queries q1,q6,probe-count,probe-sum,probe-group --warmup 5 --iterations 10
benchmarks/scripts/run-tpch.sh --report                          # per-variant sections plus the "Iceberg merge-on-read" tables
```

`gen-iceberg-mor.sh` runs `IcebergMorGenerator` with Spark alone (no plugin). It writes one table per variant into
a local Hadoop catalog (`local.<namespace>.<variant>`), the source rows as 16 data files so that every delete spans
all of them, then mutates it with merge-on-read modes. The variants:

| variant | format | mutation |
|---|---|---|
| `plain` | v2 | none: the reader's own cost |
| `pos_<pct>` | v2 | positional deletes of `<pct>` % of the rows, scattered: picked by a hash of `l_orderkey`, so every 64-row block loses a few rows |
| `pos_<pct>_clustered` | v2 | positional deletes of whole `l_shipdate` ranges from the start of the seven years: entire blocks go, which is what `EvalContext`'s active-block skipping is for |
| `pos_upd_<pct>` | v2 | `pos_10`, then an `UPDATE` of a further `<pct>` % (deletes of the old images plus small new data files) and one `MERGE INTO` that deletes, updates and inserts: the heavily mutated shape |
| `eq_<pct>` | v2 | equality delete files on `l_orderkey` (files of at most 20000 keys, about `<pct>` % of the rows), written through the Iceberg Java API the way a CDC sink does -- Spark never writes them |
| `dv_<pct>`, `dv_<pct>_clustered`, `dv_upd_<pct>` | v3 | the `pos_*` mutations encoded as deletion vectors in Puffin files |

Next to the warehouse, `README-<namespace>.md` records what each table holds: live rows (`count(*)` through
Spark's row path, the oracle every configuration is compared to), data and delete files with their formats,
delete rows and delete rows per data file, the snapshot id, and the SQL that produced it. The runner reads the
current snapshot and prints its id; pin it with `VERSION AS OF` if a table is mutated again.

`run-tpch.sh --iceberg <warehouse> --variant <namespace>.<variant>` points the `lineitem` view at that table (the
other TPC-H tables stay Parquet) and labels the dataset `iceberg:<namespace>.<variant>`. Besides Q1 and Q6, three
full-scan probes isolate the delete cost from the operator cost:
- `probe-count`: `count(*)`.
- `probe-sum`: `sum(l_extendedprice)`, one column and no predicate, the pure merge cost.
- `probe-group`: `count(*) GROUP BY l_returnflag`, a dictionary key through the selection.

Q6 is the selective predicate on top of a selection. On a table without delete files Iceberg answers
`probe-count` from its manifests (the plan is a `LocalTableScanExec`), so only the deleted variants measure a scan
there. The report lists, per query, every variant against every configuration, with the speedup over `spark` on
the same variant and over the same configuration on `plain`.

**v2** (`docs/results.md`, "Iceberg merge-on-read, v2"):
- **A fixed price per batch:** the deletes cost both engines a price per batch that does not grow with the delete
  share. Iceberg's reader builds the position index per task and the `int[] rowIdMapping` per batch before either
  engine sees a row.
- **Our lead shrinks:** `vector`'s margin over `spark` goes from 1.38x to 1.0-1.16x on the pure-merge probe, and
  from 4.4x to 3.1-4.0x on Q1.
- **Scattered or clustered:** both cost the same.
- **Equality deletes:** evaluated row at a time by Iceberg's reader and cost 3-5x. This is the one shape where
  `vector` loses a probe. The native scan applies them itself.

**v3** (deletion vectors, `docs/results.md` "v3: deletion vectors"): deletion vectors are the cheaper encoding for
both engines, one roaring bitmap per data file deserialized once.
- **The merge price over `plain`:** about +15 ms per full-scan probe for Spark and +17-22 ms for us at SF1, against
  +21-23 ms and +32-40 ms for v2 position deletes.
- **Our lead:** 1.1-1.2x over Spark on the pure-merge probes and 3-4x on Q1, flat in the delete share.
