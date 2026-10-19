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
  other column types, other metadata columns, name mappings, initial defaults, and ORC, Avro or encrypted files.
- **No Comet:** Comet is not needed for any of this.

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
| ORC and Avro data files | group | Iceberg's reader for that group, inside the same node |
| Snapshots, time travel, branches and tags | yes | Iceberg's planning: `VERSION AS OF`, `TIMESTAMP AS OF` and branch reads plan the snapshot's files, which we then read |
| Position deletes | yes | The file is read with its row positions. Iceberg's `BaseDeleteLoader` loads the deletes and the deleted rows are deselected, with no copy |
| Equality deletes | yes, or group | Read natively when every equality column is a top-level column of a native type. Columns the query does not project are added for the match and dropped afterwards. Iceberg's `DeleteFilter` semantics apply, nulls matching nulls. A nested equality column, or one of another type, sends the group to Iceberg's reader |
| Schema evolution: add, drop, rename, reorder | yes | Columns are matched by Iceberg field id, never by name. A column added after a file was written reads as nulls from that file |
| Type promotion (int to long, float to double, decimal precision) | yes | The native reader widens a file's narrower type as it does for Parquet tables, with and without position deletes and deletion vectors; a pushed filter compares the old files' narrow statistics with the wide literal. A decimal promoted past 18 digits reads its 64-bit files into a DECIMAL128 lane |
| Name mapping (`schema.name-mapping.default`) | scan | Files may carry no field ids |
| Hidden partitioning and partition transforms (identity, bucket, truncate, year, month, day, hour) | yes | Iceberg's planning prunes by every transform. Partition values are read from the files, not filled in as constants |
| Partition-spec evolution | yes | Each file is planned and pruned under its own spec |
| Filter pushdown | yes | The filter above the scan skips row groups and pages by their statistics. Each column is matched to the file's by field id, so a renamed column is tested under its name in the file, and a conjunct on a column a file lacks is not pushed for that file |
| Spark's runtime filters (dynamic partition pruning, bloom filters) | yes | Iceberg's `SupportsRuntimeV2Filtering` prunes task groups. A broadcast join's build keys also skip row groups in our reader (#610) |
| Our planner rules on Iceberg | yes | Dynamic partition pruning through aggregates and transitively (`DppThroughAggregate`, `TransitiveDpp`); fact bloom filters; broadcast and runtime-filter sizing by file bytes; merged scalar subqueries (#735). Our pruning rules prune on the source column of every partition transform (identity, bucket, truncate, year, month, day, hour) in every spec the table has had, so files written under an older spec are pruned by that spec's fields (#806) |
| Task-group packing | yes | Task groups are packed into Spark tasks as Spark packs a Parquet scan's splits (`...nativeIceberg.packSplits`) |
| Storage-partitioned joins | yes | A scan that reports a partitioning keeps Iceberg's groups, unpacked. A scan that reports an ordering is read by Iceberg's reader (scan) |
| Metadata columns `_file`, `_pos`, `_spec_id` | yes | `_file` and `_spec_id` are per-split constants; `_pos` is the reader's row index |
| Metadata columns `_partition`, `_deleted` | scan | |
| Types: boolean, int, long, float, double, date, timestamp (with zone and without), string, binary, decimal | yes | `decimal(p > 18)` becomes a DECIMAL128 lane |
| Types: uuid, fixed, time | scan | Iceberg's reader reads them as Spark types our reader does not produce from these files |
| Types: struct, list, map | yes | At any depth, when every leaf is one of the flat types above. Nested struct fields are matched by their Iceberg field ids, so a renamed nested field reads as Iceberg reads it, and a field added to a struct later reads as nulls from older files. Iceberg's reader does not vectorize nested columns, so such a scan is taken only when every task group reads natively; otherwise it stays Spark's row path |
| Encrypted files | group | Iceberg's `FileIO` decrypts them |
| `DELETE`, `UPDATE`, `MERGE INTO` (merge-on-read) | partly | Their reads run on our operators, and the `MERGE INTO` join and `MergeRows` are columnar (#21, #273). The delete and data files are written by Spark's `WriteDeltaExec` and Iceberg's writers |
| Copy-on-write rewrites, `INSERT`, CTAS | reads only | The write is Spark's row path into Iceberg's Parquet writer. A columnar writer is #64 |

### Format version 3

Everything in the version 2 table applies. The version 3 additions:

| Feature | Native | How |
|---|---|---|
| Deletion vectors (Puffin) | yes | Read once per data file, and the deleted rows are deselected as for position deletes |
| Writing deletion vectors | opt-in | `spark.vecruntime.iceberg.dvWriter.enabled` (off by default) builds each data file's `PositionDeleteIndex` from our batches and hands it to Iceberg's DV writer, for `DELETE`, `UPDATE` and `MERGE INTO`. The commit stays Iceberg's `RowDelta`. See [the write side](#the-write-side-columnar-v3-deletion-vectors-20) |
| Default values: `initial-default` | scan | Iceberg fills the default for files written before the column existed; our reader would read nulls |
| Default values: `write-default` | n/a | Applied by Iceberg's writer |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | scan | Iceberg derives them from the manifests and the files' first row ids |
| `variant`, shredded or not | scan or row | Our Parquet reader reads Spark's `variant` and shredded variants for Parquet tables (#787, #812), but the Iceberg path does not use it yet |
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
- **Any other group** is read by Iceberg's columnar reader inside the same node, and its batches are adapted as
  below.

The node reports how many task groups each reader read (`numNativeGroups`, `numIcebergGroups`), plus files, row
groups and pages skipped.

At plan time the node declines, and Spark's `BatchScanExec` stays:
- a scan that reports an ordering;
- a scan Iceberg does not vectorize (nested columns) with a task group our reader cannot read: an ORC or Avro
  file, an encrypted file, or equality deletes on a column it does not read;
- a table with a name mapping;
- a metadata column other than `_file`, `_pos` and `_spec_id`;
- a column with an initial default;
- a column type outside the flat types in the table above, or a nested type with such a leaf or a nested field
  with an initial default.

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
  metadata columns; renamed, added and promoted columns (int to bigint, float to double,
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
