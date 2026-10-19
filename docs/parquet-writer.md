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
| BOOLEAN, INT96, FIXED_LEN_BYTE_ARRAY | through parquet-java's v2 column writer | as parquet-java |

A column chunk's dictionary falls back to the plain encoding when it outgrows
`parquet.dictionary.page.size`, or after the first page when the dictionary does not pay for itself.
Definition levels are RLE/bit-packed hybrid. Page statistics (min, max, null count) and the column
index are written as parquet-java writes them; binary min/max compare unsigned.

## Layout

- **`parquet-writer` module** (`io.vecruntime.parquet.write`), with no Spark dependency: it depends on
  `kernels` and parquet-java (provided). `ColumnarParquetWriter` takes our `VectorBuffers` columns and
  writes a file through parquet-java's `ParquetFileWriter`, so the footer, the column and offset indexes
  and compression are parquet-java's own. INT32, INT64, DOUBLE and string columns are encoded by our chunk
  writers (`FixedWidthChunkWriter`, `BinaryChunkWriter`); the other types go to parquet-java's column
  writer from the same batch.
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
string, binary -- over a columnar child. Other writes keep Spark's writer, with the reason in the plan's
fallback list:

| Write | Reason |
|---|---|
| Concurrent output writers whose past-the-limit sort keys do not compile | `concurrent writers' sort key ...` |
| Hive-compatible bucketing (Hive tables) | `Hive-compatible bucketing is not supported yet (#842)` |
| Struct, array, map columns | `column <name> has type <type>, not supported yet (#842)` |
| `parquet.writer.version=v1` | `parquet.writer.version is v1, the columnar writer writes Parquet v2` |
| Bloom filters, encryption, per-column `#` options | named in the reason |
| A date or timestamp with a rebase mode in write other than `CORRECTED` (Spark 4 default) | `... is not CORRECTED` |
| A decimal under `spark.sql.parquet.writeLegacyFormat` | `... writeLegacyFormat is on` |
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

**Partitioned writes:** a partitioned 1 TB write (`DISTRIBUTE BY ss_sold_date_sk`, then `partitionBy`) does not complete under VecRuntime yet.
- Executors are killed for exceeding their container memory in the stage after that shuffle.
- This happens with this writer and with Spark's writer, so the cause is upstream of the writer.

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
