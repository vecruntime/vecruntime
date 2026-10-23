---
layout: default
title: Columnar Parquet writer
---

# Columnar Parquet writer (#64)

`spark.vecruntime.write.columnar.enabled` (default `false`) writes a Spark Parquet write
(`df.write.parquet`, `INSERT` into a Parquet table, `CREATE TABLE ... USING parquet AS SELECT`) straight
from our columnar batches, with no conversion to rows and no parquet-java `ParquetWriteSupport` in
between. The work list for a complete writer (nested types, Iceberg, the remaining options) is #842.

**The writer writes Parquet v2 only**: data page v2, and the v2 encodings. There is no v1 code path.
v2 pages keep the repetition and definition levels outside the compressed body and the v2 encodings
(`DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY`) are block-oriented, which suits a vectorized encoder. Every
reader of Parquet v2 reads the files: Spark's (vectorized or not), ours, parquet-java, Trino, DuckDB.
Spark's own writer defaults to v1; a write that asks for v1 explicitly (`parquet.writer.version=v1`)
keeps Spark's writer.

## Encodings

The same choices as parquet-java's writer with `WriterVersion.PARQUET_2_0`, so our files and parquet-java's
can be compared page by page in tests:

| Parquet type | Pages | When the dictionary falls back |
|---|---|---|
| INT32, INT64 | `RLE_DICTIONARY` (PLAIN dictionary page) | `DELTA_BINARY_PACKED` |
| DOUBLE, FLOAT | `RLE_DICTIONARY` | `PLAIN` |
| BINARY (strings, binary) | `RLE_DICTIONARY` | `DELTA_BYTE_ARRAY` |
| FIXED_LEN_BYTE_ARRAY (fixed, UUID, decimal > 18) | `RLE_DICTIONARY` | `DELTA_BYTE_ARRAY` |
| INT96 (Spark timestamp) | `RLE_DICTIONARY` | `PLAIN` |
| BOOLEAN | `RLE` (no dictionary) | -- |

Since #901 every type is written by our own chunk writers, not parquet-java's v2 column writer. A FLOAT
narrowed from a FLOAT64 lane, a millisecond timestamp carried in an INT64 lane of microseconds, and a
decimal of precision up to 18 held in a DECIMAL128 lane go on `FixedWidthChunkWriter`; BOOLEAN on
`BooleanChunkWriter`; FIXED_LEN_BYTE_ARRAY and INT96 on `FixedBinaryChunkWriter`; and the leaves of
structs, arrays and maps on `NestedLeafWriter`.

A column chunk's dictionary falls back to the plain encoding when it outgrows
`parquet.dictionary.page.size`, or after the first page when the dictionary does not pay for itself.
Definition levels are RLE/bit-packed hybrid. Page statistics (min, max, null count) and the column
index are written as parquet-java writes them: binary and fixed min/max compare unsigned, a decimal's
signed two's-complement, a BOOLEAN's by value; INT96 carries only a null count (no min/max), as the
type is deprecated for statistics.

## Layout

- **`parquet` module** (`io.vecruntime.parquet.write`), with no Spark dependency: it depends on
  `kernels` and parquet-java (provided). Since #900 the module also holds the page decoders the native scan
  reads with (`io.vecruntime.parquet.codec`), and both sides take the `DELTA_BINARY_PACKED` and RLE / bit-packed
  hybrid framing from one class (`ParquetFraming`); `CodecRoundTripTest` decodes the writer's output with the
  reader's decoders, and `VectorWriteReadRoundTripSuite` reads the writer's files with our scan.
  `ColumnarParquetWriter.footer()` returns the footer it wrote (statistics, encodings, sizes per column chunk),
  which is what a table format such as Iceberg builds its file metrics from. `ColumnarParquetWriter` takes our `VectorBuffers` columns and
  writes a file through parquet-java's `ParquetFileWriter`, so the footer, the column and offset indexes
  and compression are parquet-java's own. Every column is encoded by our own chunk writers
  (`FixedWidthChunkWriter` for INT32/INT64/FLOAT/DOUBLE/millis-timestamp/DECIMAL128-lane decimals,
  `BinaryChunkWriter` for strings, `BooleanChunkWriter` for BOOLEAN, `FixedBinaryChunkWriter` for
  FIXED_LEN_BYTE_ARRAY and INT96, `NestedLeafWriter` for the leaves of nested columns); parquet-java is
  kept only for the footer, indexes and compression (#901).
- **Spark side**: `VectorWriteFilesExec` replaces Spark's `WriteFilesExec` under the write command. Spark
  keeps the command, the commit protocol, file naming, the write statistics and the table refresh; only
  the task changes. The schema is Spark's (`SparkToParquetSchemaConverter`), the footer carries Spark's
  `org.apache.spark.version` and `org.apache.spark.sql.parquet.row.metadata` keys, and the compression
  codec, page, dictionary-page and row-group sizes are read from the same keys as Spark's writer.
  `maxRecordsPerFile` rolls files in the middle of a batch.
- **Partitions and buckets** (`VectorDynamicPartitionDataWriter`, Spark's `DynamicPartitionDataSingleWriter`
  for batches): the input is sorted by the partition columns and then the bucket id, as `V1Writes` orders it
  (the projection and sort are ours: `empty2null` and Spark 4's `collation_aware_hash`, collated keys included, compile -- see the expressions page). Each batch
  is cut into runs of equal partition values and bucket id -- adjacent rows compared lane by lane, partition
  values projected once per run -- and each run is one range of the batch's columns written into the current
  file. Opening a file goes through Spark's own `renewCurrentWriter`, so the partition directories (escaping,
  `__HIVE_DEFAULT_PARTITION__`, custom locations), the bucket file names and the stats trackers are Spark's.
  Bucket ids are `pmod(murmur3hash(keys, 42), n)` from the shuffle's hash kernels over the data lanes;
  another bucket expression or key type is evaluated row by row by Spark's projection.
- **Concurrent output writers** (`spark.sql.maxConcurrentOutputFileWriters` > 0, Spark's
  `DynamicPartitionDataConcurrentWriter` for batches): the input is not sorted; up to that many files stay open
  per task, one per partition and bucket, each run of a batch written to its own. Opening the last one switches
  the task to sorted mode, as Spark does: the batch's remaining rows and every later batch go through our sort
  (by the partition columns, then the bucket id), and each file is closed when its run ends.

## Scope of the prototype, and fallbacks

Taken: Parquet writes of flat types, unpartitioned, partitioned (dynamic partitions) and bucketed -- boolean, byte, short, int, long, float,
double, decimal (every precision), date, timestamp (INT96, `TIMESTAMP_MICROS`, `TIMESTAMP_MILLIS`),
string, binary, and structs, arrays and maps of those types (#842) -- over a columnar child. Other writes keep Spark's writer, with the reason in the plan's
fallback list:

| Write | Reason |
|---|---|
| Concurrent output writers whose past-the-limit sort keys do not compile | `concurrent writers' sort key ...` |
| Hive-compatible bucketing (Hive tables) | `Hive-compatible bucketing is not supported yet (#842)` |
| Struct, array, map columns under `spark.sql.parquet.writeLegacyFormat` | `column <name> is nested and writeLegacyFormat is on` |
| Other types (intervals, variant, ...) | `column <name> has type <type>, not supported yet (#842)` |
| `parquet.writer.version=v1` | `parquet.writer.version is v1, the columnar writer writes Parquet v2` |
| Encryption; per-column `#` options other than `parquet.enable.dictionary#` and `parquet.bloom.filter.*#` | named in the reason |
| A date or timestamp with a rebase mode in write other than `CORRECTED` (Spark 4 default) | `... is not CORRECTED` |
| A decimal under `spark.sql.parquet.writeLegacyFormat` | `... writeLegacyFormat is on` |

**Spark's own write suites** run with the writer on, from the spark-sql tests jar through the `spark-sql-tests`
module, with the plugin, `spark.vecruntime.write.columnar.enabled=true`, `spark.vecruntime.write.columnar.fromRows=true`
and off-heap on.
- Suites: `ParquetIOSuite`, `ParquetEncodingSuite`, `ParquetCompressionCodecPrecedenceSuite`, `InsertSuite`,
  `PartitionedWriteSuite`, `BucketedWriteWithoutHiveSupportSuite`, `FileFormatWriterSuite` and `V1WriteCommandSuite`.
- `fromRows` takes a write of rows (a local relation, a row operator) through Spark's `RowToColumnarExec`, so that
  323 of the 503 file-source writes the suites make went through this writer. The rest are ORC, JSON, CSV or text,
  or other fallbacks.
- 211 of 214 tests pass. The three failures fail the same way with the writer off, so they come from the rest of
  VecRuntime, not the writer: the two `SPARK-41914` in-partition-sorted v1 writes, and `vectorized reader: missing
  all struct fields`.
- The runs found one difference, now fixed: like `FileFormatDataWriter`, an error from the writer is reported as
  `TASK_WRITE_FAILED`, or as the fast-fail exception for a file that already exists.

**Bloom filters** (#842): `parquet.bloom.filter.enabled`, globally or `#<column>`, with `expected.ndv#`, `fpp#`,
`max.bytes`, `adaptive.enabled` and `candidates.number#`, read as parquet-java reads them.
- Each chunk's filter is sized as parquet-java's column writers size it, and the fast writers hash every non-null
  value with the filter's own hash.
- A chunk whose pages are all dictionary-encoded gets no filter, as with parquet-java.
- `ColumnarParquetWriterTest` checks that every chunk's filter is the bitset parquet-java writes, and
  `VectorWriteSuite` checks it against Spark's writer.

**Nested columns** (#842) use Spark's standard layout: a struct is a group, an array the 3-level `LIST`, a map
the `MAP` with `key_value`.
- `NestedFieldWriter` walks each row's nested lane (Arrow's layout) and works out the Dremel repetition and
  definition levels for every leaf. A null at any level writes a null to each leaf under it; an empty array
  or map writes one a level deeper.
- Each leaf is written by its own `NestedLeafWriter` (#901): the repetition and definition levels are
  RLE/bit-packed hybrid in the data-page-v2 level sections, and the values use the same per-type encoders
  a flat column does. A reader reconstructs the records from the levels.
- On the Spark side the lane is built from Spark's getters, and a filtered batch is gathered by its
  selection.
- Tests: `NestedWriteTest` against parquet-java (nulls at every level, a list of structs holding a list,
  several batch sizes and row groups), and `VectorWriteSuite` against Spark's writer, unpartitioned and
  partitioned.
| ORC, JSON, CSV | `... is not Parquet` |
| A child that is not columnar | `child <op> is not columnar` |

## Testing

- `parquet-writer` tests write the same rows through `ColumnarParquetWriter` and parquet-java's
  `ExampleParquetWriter` (v2) and compare values, statistics and encodings, row-group cuts, dictionary
  fallback mid-chunk, and empty files; the RLE and `DELTA_BINARY_PACKED` encoders are decoded by
  parquet-java's readers. They also run at 128, 256 and 512-bit vector widths.
- `VectorWriteSuite` (Spark) writes each case through our writer and Spark's, reads both back with
  Spark's reader and ours, and compares rows, the Spark and Parquet schemas and the footer keys: every
  flat type, zstd / snappy / none, filtered and projected input (selection vectors), `maxRecordsPerFile`,
  empty input, CTAS, partitioned writes (a boolean; a date and a string with nulls and characters that
  need escaping; `maxRecordsPerFile` within a partition: the same directories and row counts as Spark's),
  bucketed tables with and without partitions (every bucket file holds the rows Spark's bucket file holds;
  a bucketed join reads them), concurrent output writers with a limit of 4 (the sorted rest past it) and of 64,
  partitioned and bucketed, with `maxRecordsPerFile`, and the fallbacks above.

## Measurements

**1 TB TPC-DS `store_sales`** (2.88 billion rows), read and written in full by `WriteBenchRunner`:
- 9 x m5.4xlarge, 8 executors x 13 cores, x86, 2026-10-19;
- two iterations per case;
- executor CPU in CPU-seconds;
- for our writer, the build with the per-lane write loops.

| Case | Our writer | Spark's writer under VecRuntime | Plain Spark |
|---|---|---|---|
| no-op sink (scan only) | 53.6 / 39.1 s | 51.9 / 38.3 s | 68.6 / 56.0 s |
| snappy | **86.4 / 84.5 s**, 3,749 / 3,494 CPU-s, 103.9 GB | 198.7 / 197.6 s, 14,991 / 14,884 CPU-s, 135.0 GB | 212.3 / 209.3 s |
| zstd | **90.0 / 92.8 s**, 4,093 / 4,051 CPU-s, 99.2 GB | 217.8 / 201.1 s, 15,949 / 15,174 CPU-s, 104.4 GB | 230.3 / 218.2 s |

- **Speed:** 2.2–2.3x faster than Spark's writer under VecRuntime, and 2.4–2.5x faster than plain Spark.
- **Write alone** (time minus the no-op scan): 33–45 s, against 147–159 s.
- **CPU:** about 4x less executor CPU.
- **Size:** the v2 files are 23% smaller on snappy and 5% smaller on zstd. Our snappy output is already as small as Spark's zstd output.
- **Where the CPU goes** (executor JFR): mostly dictionary encoding of the fixed-width columns, then RLE / bit packing. Compression (1.7%) and the S3A upload (2.6%) take little.

**Run writes with Spark's off-heap pool** (`spark.memory.offHeap.enabled=true` and a `spark.memory.offHeap.size`). Every write that sorts before it writes keeps its sorted runs in native memory, and only the off-heap pool tracks that memory. That covers a partitioned or bucketed write (the sort `V1Writes` plans) and concurrent writers past their limit. Without the off-heap pool:
- Spark counts that memory against the on-heap pool. That pool is sized from the heap, so it does not keep the runs inside the container's overhead.
- Large writes then need manual tuning (a smaller `spark.vecruntime.sort.spillBytes`, fewer tasks per executor), or executors are killed for exceeding container memory.

The numbers below use a 10g pool. The container is the same 50 GB as before: heap 22g, overhead 18g, off-heap 10g.

**Partitioned writes** (1 TB `store_sales`, snappy, `DISTRIBUTE BY ss_sold_date_sk`, then `partitionBy("ss_sold_date_sk")`; 8 executors x 13 cores, one run each):

| Source | This writer | Spark's writer under VecRuntime | Plain Spark |
|---|---|---|---|
| all rows, off-heap 10g | **961 s**, 104.1 GB, 11.6 M core-ms | 1312 s, 135.2 GB, 22.3 M core-ms | 985 s, 143.0 GB, 18.9 M core-ms |
| `WHERE ss_sold_date_sk IS NOT NULL`, off-heap disabled | **322 s / 324 s**, 100.4 GB, 11.1 M core-ms | -- | 542 s, 137.6 GB, 17.7 M core-ms |

- **All rows:** one task receives every null `ss_sold_date_sk` row (about 4.5% of the table, written as one file). It runs alone for the last 10 minutes or more, so the wall time is mostly that task. Executor CPU is the clearer comparison.
- **Without off-heap:** the all-rows write lost executors near the 53.7 GB container limit in every run, and the fetch failures recomputed the map stage: 1137–1363 s. With off-heap, no executor was lost and no stage was retried.
- **History:** before #870/#871 these writes failed with container OOMs, with both writers. The cause was the sort `V1Writes` plans before the write, whose native runs held about 4x their budget.

JMH `ParquetWriteBenchmark`, 262,144 `store_sales`-shaped rows, uncompressed, output discarded (shared
x86 host, indicative): ours 4,085 rows/ms before the per-lane write loops and about 4,700 after them, parquet-java v2 1,160,
parquet-java v1 (Spark's default) 1,042.

The `DELTA_BINARY_PACKED` block (deltas, minimum, reduction, miniblock widths) has a Vector API path,
byte-identical to the scalar loops (`-Dvecruntime.parquet.write.scalarDelta=true` selects them).
`DeltaEncodeBenchmark`, one 20,000-value page, values per µs, dedicated m5.4xlarge (Xeon 8259CL):

| data | INT32 scalar / vector | INT64 scalar / vector |
|---|---|---|
| sorted (runs) | 370 / 553 | 445 / 597 |
| keys < 1M | 333 / 529 | 401 / 520 |
| full range | 331 / 447 | 331 / 392 |
