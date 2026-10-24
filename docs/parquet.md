---
layout: default
title: Apache Parquet
---

# Apache Parquet

VecRuntime reads and writes Parquet natively, straight between Parquet's on-disk layout and the Arrow
buffers its operators run on, with no row conversion in between. The **native reader**
(`VectorParquetScanExec`) decodes Parquet pages directly into those buffers; the **columnar writer**
(`VectorWriteFilesExec` / `ColumnarParquetWriter`) encodes them straight back out. Both are on by default,
and both fall back to Spark — per file or per plan — for anything they do not yet handle, always returning
Spark's own result.

This page is the single reference for what each side supports and how to configure it. For the design,
benchmarks and the full test matrix, see the two detailed pages:

- [Native Parquet reader](native-parquet-reader.html) — the scan's architecture, read-ahead, how reads are
  issued, the whole-corpus conformance suite, and the performance work list.
- [Columnar Parquet writer](parquet-writer.html) — the encoder layout, nested writes, bloom filters,
  partitioned and bucketed writes, and the write benchmarks.

## The reader

`spark.vecruntime.scan.nativeParquet.enabled` replaces Spark's `FileSourceScanExec` with a scan that decodes
Parquet directly into Arrow buffers, reusing the partitions, splits, bucketing, pushed filters and schema
Spark already computed. It is **on by default**, except in a session where Comet's scan is active (Comet's
plugin or extension registered with `spark.comet.enabled` and `spark.comet.scan.enabled` not `false`), where
Comet's reader stays the scan unless the key is set explicitly. A column or file it cannot decode falls over
to Spark's own vectorized reader for that one file, or keeps Spark's scan for the whole plan — the result is
always Spark's.

### What it reads

**Physical storage** (checked per file at open, `physicalMatches`): INT32, INT64, FLOAT, DOUBLE, BOOLEAN,
BINARY, `FIXED_LEN_BYTE_ARRAY` (decimals, binary, UUID), and INT96 timestamps. Unsigned integers (`UINT_32`
read as `bigint` zero-extended, `UINT_64` as `decimal(20,0)`), and INT32/INT64 read as decimals wider than 18
digits.

**Logical / Spark types.** `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `float`, `double`, `date`,
`timestamp` and `timestamp_ntz` (INT64 `MICROS`/`MILLIS`, or INT96 for `timestamp`), `decimal` to precision 38,
`string` (UTF8; a `uuid` column stored as FLBA(16) with the UUID logical type reads as its canonical-text
string, `#rtypes`), `binary`, `time` (any INT64, nanoseconds of the day), and the ANSI intervals (`interval
day to second` on INT64, `interval year to month` on INT32). `variant` is decoded too — unshredded, shredded
at the top level, and with top-level extraction pushed into the scan — as the struct of its `value` and
`metadata` binaries.

**Nested types.** Structs, lists and maps nested inside one another at any depth, in Spark's three-level
`LIST`/`MAP` form, the legacy two-level forms, and parquet-java's: lists and maps of flat types, lists of
structs, lists of lists, maps of structs, and any other combination (`NativeParquetTreeReader`).

**Encodings** (`supportsEncoding`): `PLAIN`, `PLAIN_DICTIONARY`, `RLE_DICTIONARY`, `RLE` (booleans and levels),
`BIT_PACKED` (levels); and per physical type `DELTA_BINARY_PACKED` (INT32/INT64), `DELTA_LENGTH_BYTE_ARRAY`
(BINARY), `DELTA_BYTE_ARRAY` (BINARY and `FIXED_LEN_BYTE_ARRAY`), and `BYTE_STREAM_SPLIT`
(INT32/INT64/FLOAT/DOUBLE and `FIXED_LEN_BYTE_ARRAY`). Dictionary pages are kept as dictionaries: a string
column's dictionary-encoded batches are emitted as dictionary ids over the row group's Arrow dictionary
(`dictionaryStrings`, on by default), resolved to bytes only where a page falls back from the dictionary.

**Compression codecs.** Whatever parquet-java decompresses, byte for byte as Spark's own reader: uncompressed,
snappy, zstd, gzip, lz4, lz4_raw. zstd and snappy decompress through one reused buffer per column chunk and are
read in place; the others are parquet-java's own decompressors. The codec is never a fallback reason — a file in
any codec whose decompressor is on the classpath is read natively. **BROTLI and LZO** have no codec on the
classpath (LZO is GPL, Brotli ships with neither aircompressor nor parquet-java), so a file in either fails with
parquet-java's `BadConfigurationException` exactly as Spark's reader would.

**Pruning and page skipping.** Row-group statistics, dictionary filters and bloom filters prune row groups
through parquet-java (`readNextFilteredRowGroup`). The **column index** (per-page min/max, offset index) skips
pages outside a filter's row ranges — **off by default** (`parquet.filter.columnindex.enabled`): on the classic
S3A stream every selected page is its own GET, and at 1 TB that cost 37%; it waits for range merging (#834) to
pay back. **Late materialization** (`lateMaterialization`, off by default) decodes a filter's columns first and
the others only at surviving rows; its **two-pass** mode (`lateMaterialization.twoPass`, off by default) also
skips the page I/O of row groups' non-surviving rows.

**Read-ahead.** The scan opens files and reads row groups ahead on background threads while the task thread
decodes (`prefetchFiles`, `prefetchRowGroups`), bounded in bytes per task and per executor and charged to Spark
execution memory (#915). **Decode-ahead** (`decodeAhead`, off by default) runs decoding on a producer thread; 
**parallel decode** (`parallelDecode`, off by default) decodes later row groups on helper threads. **Small-file
prefetch** (`smallFileThreshold`) and **range merging** (`rangeMergeGap`) issue fewer, larger GETs (#834) — both
off by default until a 1 TB A/B sets the thresholds.

**Other.** Every calendar rebase mode (`CORRECTED`, `LEGACY`, `EXCEPTION`), resolved per file; INT96 with its
own rebase and time-zone conversion; legacy dates. Field-id matching
(`spark.sql.parquet.fieldId.read.enabled`). Schema evolution — missing columns and fields (null, or an
existence `DEFAULT`), type widening, field-id matching — matching Spark's readers. Hive bucketing (Spark's own
bucketed file partitions, so a bucketed join needs no shuffle). The `_metadata` columns (`file_path`,
`file_name`, `file_size`, `file_block_start`, `file_block_length`, `file_modification_time`,
`_metadata.row_index`) and `input_file_name()` with its block siblings.

## The writer

`spark.vecruntime.write.columnar.enabled` writes a Spark Parquet write (`df.write.parquet`, `INSERT` into a
Parquet table, `CREATE TABLE ... USING parquet AS SELECT`) straight from our columnar batches, with no row
conversion and no parquet-java `ParquetWriteSupport`. It is **on by default** (#982). Spark keeps the write
command, the commit protocol, file naming, statistics and the table refresh; only the task changes. parquet-java
is kept for the footer, the column and offset indexes, and compression.

### What it writes

**Parquet v2 only** — data page v2, and the v2 encodings. There is no v1 code path; a write that asks for v1
(`parquet.writer.version=v1`) keeps Spark's writer. Every v2 reader (Spark, ours, parquet-java, Trino, DuckDB)
reads the files.

**Encodings**, the same choices as parquet-java's v2 writer, so files compare page by page in tests, with one
deliberate exception — **BOOLEAN is PLAIN**, not the RLE hybrid parquet-java picks for v2, because Iceberg's
vectorized Spark reader rejects an RLE boolean column. Dictionary (`RLE_DICTIONARY`) with a per-type fallback:
INT32/INT64 fall back to `DELTA_BINARY_PACKED`; DOUBLE/FLOAT/INT96 to `PLAIN`; BINARY and `FIXED_LEN_BYTE_ARRAY`
to `DELTA_BYTE_ARRAY`. The dictionary falls back when it outgrows `parquet.dictionary.page.size` or does not pay
after the first page.

**Statistics, page indexes and bloom filters** are written as parquet-java writes them (min/max compare
unsigned for binary and fixed, signed two's-complement for decimals; INT96 carries only a null count). Bloom
filters honour `parquet.bloom.filter.enabled` (global or `#<column>`) with `expected.ndv`, `fpp`, `max.bytes`,
`adaptive.enabled` and `candidates.number`; an all-dictionary chunk gets no filter, as with parquet-java.

**Sizing.** Row-group, page and dictionary-page sizes come from the same Spark/Parquet keys as Spark's writer
(`parquet.block.size`, `parquet.page.size`, `parquet.dictionary.page.size`), and `maxRecordsPerFile` rolls files
mid-batch. The row-group boundary is checked **adaptively** as records accrue (the open group's own rows), so a
small configured row-group size is honoured within a batch (#997).

**Nested types.** Structs, arrays and maps of the supported leaf types, in the standard layout and — under
`spark.sql.parquet.writeLegacyFormat` — the legacy `bag`/`array` `LIST` and `key_value` `MAP`, with the
repetition/definition levels computed from each row's Arrow lane.

**Calendar rebase and INT96.** `datetimeRebaseModeInWrite` and `int96RebaseModeInWrite` in `CORRECTED`,
`LEGACY` or `EXCEPTION`, written exactly as Spark's `ParquetWriteSupport` (the `LEGACY` footer keys included so
Spark's reader rebases back). INT96 is Spark's default timestamp output. Legacy-format decimals are written as
`FIXED_LEN_BYTE_ARRAY` of the minimal byte length.

**Partitioned and bucketed writes.** Dynamic partitions (the partition directories, escaping and
`__HIVE_DEFAULT_PARTITION__` are Spark's), Hive bucketing (Spark's `_NNNNN` suffix or Hive's `NNNNN_0_` prefix;
the bucket id is `pmod(murmur3hash(keys, 42), n)`, or Hive's `HiveHasher` for a Hive-compatible write), and
concurrent output writers (`spark.sql.maxConcurrentOutputFileWriters`).

## Configuration

Only the keys a user would set; defaults verified in
`spark/src/main/scala/io/vecruntime/spark/VectorConf.scala`.

| Key | Default | Type / values | What it does |
|---|---|---|---|
| `spark.vecruntime.scan.nativeParquet.enabled` | `true` (off when Comet's scan is active) | boolean | The native Parquet reader. `false` uses Spark's vectorized reader under our operators. |
| `spark.vecruntime.scan.nativeParquet.dictionaryStrings` | `true` | boolean | Emit a string column's dictionary-encoded batches as dictionary ids over the row group's dictionary, not resolved bytes. |
| `spark.vecruntime.scan.nativeParquet.prefetchFiles` | `12` | int, 0–16 | Files of the split opened ahead on background threads. `0` opens each on the task thread. |
| `spark.vecruntime.scan.nativeParquet.prefetchRowGroups` | `4` | int, 0–16 | Row groups of the current file read ahead. `0` reads each on the task thread. |
| `spark.vecruntime.scan.nativeParquet.prefetchMaxBytes` | `256m` | bytes | Cap on the compressed bytes one task's read-ahead holds. `0` or less: no cap. |
| `spark.vecruntime.scan.nativeParquet.prefetchExecutorMaxBytes` | `2g` | bytes | The same cap across every task of the executor. `0` or less: no cap. |
| `spark.vecruntime.scan.nativeParquet.decodeAhead` | `0` | int, 0–16 | Batches decoded ahead of the task thread on a producer thread. `0` decodes on the task thread. |
| `spark.vecruntime.scan.nativeParquet.decodeAhead.threads` | `virtual` | `virtual` / `platform` | The decode-ahead producer's thread kind. |
| `spark.vecruntime.scan.nativeParquet.parallelDecode` | `0` | int, 0–8 | Helper threads decoding later row groups in parallel. `0` keeps the serial / single-producer path. Engages only for a file with ≥2 row groups at or above `parallelDecode.minRowGroupBytes`. |
| `spark.vecruntime.scan.nativeParquet.parallelDecode.threads` | `virtual` | `virtual` / `platform` | The parallel-decode helper threads' kind. |
| `spark.vecruntime.scan.nativeParquet.parallelDecode.minRowGroupBytes` | `32m` | bytes | Per-row-group threshold for parallel decode. `0` engages at any size. |
| `spark.vecruntime.scan.nativeParquet.lateMaterialization` | `false` | boolean | Decode a filter's columns first, the others only at surviving rows. Saves decode CPU, not I/O. |
| `spark.vecruntime.scan.nativeParquet.lateMaterialization.minScanBytes` | `1g` | bytes | The smallest scan a decode filter is handed to. |
| `spark.vecruntime.scan.nativeParquet.lateMaterialization.twoPass` | `false` | boolean | Read a row group twice so the pages of other columns holding no surviving row are not fetched. Needs the file's offset index. |
| `spark.vecruntime.scan.nativeParquet.lateMaterialization.twoPass.maxFraction` | `0.5` | double | Above this surviving-row fraction, the row group is read whole. |
| `spark.vecruntime.scan.nativeParquet.smallFileThreshold` | `0` (off) | bytes | A file at or under this is fetched whole in one GET before the footer (#834). e.g. `8m`. |
| `spark.vecruntime.scan.nativeParquet.rangeMergeGap` | `-1` (off) | bytes | Coalesce planned byte ranges whose gap is at or under this into one GET (#834). `0` merges only adjacent. |
| `spark.vecruntime.scan.nativeParquet.rangeTargetSize` | `8m` | bytes | Split a coalesced run longer than this into parallel GETs. `0` never splits. |
| `spark.vecruntime.write.columnar.enabled` | `true` | boolean | The columnar Parquet writer (#982). `false` uses Spark's writer. |
| `parquet.filter.columnindex.enabled` | `false` | boolean | Parquet-java's own key: enable column-index page skipping in the native reader (#873). Set as a session key or `spark.hadoop.parquet.filter.columnindex.enabled`. |

The read path also honours the standard Spark/Parquet keys — rebase modes
(`spark.sql.parquet.datetimeRebaseModeInRead`, `...int96RebaseModeInRead`), field ids
(`spark.sql.parquet.fieldId.read.enabled`) — and the write path honours `parquet.writer.version`,
`parquet.block.size`, `parquet.page.size`, `parquet.dictionary.page.size`, `parquet.bloom.filter.*`,
`spark.sql.parquet.writeLegacyFormat`, the write rebase modes, and `spark.sql.maxConcurrentOutputFileWriters`.
See [Configuration reference](configuration.html) for the full list.

## Not implemented / falls back to Spark

Every case here returns Spark's own result — Spark's reader decodes the one file, Spark's scan stays in the
plan, Spark's writer handles the write, or the query fails with Spark's error. None is a wrong answer.

**Reader — Spark's reader for the one file:**

- A column whose stored **encoding** or **physical type** has no native path (e.g. `BYTE_STREAM_SPLIT` on a
  type without a lane, a decimal whose stored physical type or scale does not match): the whole file reads with
  Spark's reader.
- A **shredded variant below the top level** (inside a struct, list or map).
- **Encrypted Parquet** (modular encryption): decryption is parquet-java's; without the keys the read fails as
  Spark's does.
- A few reader-defined corners that are Spark's own outcome: a UTC-adjusted timestamp read as `timestamp_ntz`,
  INT32 `TIME(MILLIS)`, a missing `REQUIRED` column, some field-id corner cases, and the two legacy list/variant
  shapes (`list_of_lists_legacy_two_level_test`, `repeated_variant_group_test`).

**Reader — Spark's scan for the whole plan:**

- A **variant extraction pushed below the top level** (`spark.sql.variant.pushVariantIntoScan`).
- **Geospatial types** (`geometry`, `geography`, Spark 4.2): no decode path; the file's other columns are
  native.
- Any type **Spark 4.1 cannot read itself** (`FLOAT16`, the legacy `INTERVAL` converted type, 12-byte
  timestamps): Spark refuses the file before any scan runs.

**Reader — carried but not computed on (operators above the scan):** `variant` has no engine lane by design;
`time` and the ANSI intervals ride their INT64/INT32 lanes for carrying, comparison, grouping, sorting, joins,
windows and count/min/max/first/last, but arithmetic, casts, functions, sum and avg over them stay Spark's.

**Writer — Spark's writer for the write** (`VectorWritePlanner.reason`):

- **Encryption** (`parquet.crypto.factory.class`, `parquet.encryption.kms.client.class`).
- **Parquet v1** (`parquet.writer.version=v1`).
- A non-Parquet format (ORC, JSON, CSV, text), or a child operator that is not columnar (unless
  `write.columnar.fromRows` is on).
- A column type not yet supported: **interval, variant**, and an **empty struct** — anything outside boolean,
  byte, short, int, long, float, double, decimal, date, timestamp, `timestamp_ntz`, string, binary, and
  structs/arrays/maps of those (#842).
- A per-column `parquet.*#<column>` option other than `parquet.enable.dictionary#` and the read
  `parquet.bloom.filter.*#` options.
- Concurrent output writers whose past-the-limit sort keys do not compile on our lanes.

Not implemented on either side: **geospatial types** on write, **shredded variant** writes, Parquet **v1 data
pages** on write, and **encryption** throughout. These are tracked in the writer work list (#842) and the reader
work list; the two detailed pages carry the current state.
