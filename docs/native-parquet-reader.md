---
layout: default
title: Native Parquet reader
---

# The native Parquet reader (`VectorParquetScanExec`)

This page is the design note for VecRuntime's own Parquet scan, #559. It covers what the scan does today, how it is put together, what it does not decode yet, and the work that is left.

**In short (2026-10-03):** the scan reads every flat Parquet type and every encoding parquet-java's v1 and v2 writers produce. Structs, lists and maps of flat types, lists of structs of flat types, lists, maps and structs nested in one another at any depth are read natively too (#568). `INT96` timestamps and files in any calendar rebase mode (`LEGACY`, `EXCEPTION`, `CORRECTED`) are decoded natively too (#568 item 6). A column with a type that has no decode path (a collated string, say), anywhere in it, keeps Spark's scan for the plan. On the 1 TB TPC-DS run of 2026-10-02 it took VecRuntime to 1,542 s for the 103 queries, against 3,099 s for Spark, 1,999 s for Comet and 1,954 s for Comet's reader under our operators.

The scan is **on by default** since 0.0.6 (`spark.vecruntime.scan.nativeParquet.enabled`), except in a session where Comet's scan is active: Comet's plugin or extension registered, with `spark.comet.enabled` and `spark.comet.scan.enabled` not false. There Comet's reader stays the scan unless the key is set. Setting the key to `false` brings back Spark's vectorized reader under our operators. Its configuration keys are in the [Configuration reference](configuration.html), and the planner's fallback reasons are on the [Operators](operators.html) page.

## Why the scan exists

Spark's vectorized Parquet reader decodes into on-heap `ColumnVector`s. VecRuntime's operators then copy those batches into native Arrow memory, once per operator chain. Comet's native scan avoids that copy, but it brings its own operators and memory model.

`VectorParquetScanExec` decodes Parquet pages straight into the Arrow buffers that VecRuntime's operators read, so there is no conversion between the scan and the first operator. It reuses everything Spark already computed for the scan:
- the selected partitions, including dynamic partition pruning;
- the file splits and bucketing;
- the pushed data filters;
- the required schema and the partition schema.

## Data path

```
FileSourceScanExec (planned by Spark)
  └─ VectorParquetScanExec           (replaces it when the plan is supported; VectorParquetScanPlanner)
       └─ VectorParquetRDD            (one partition per Spark FilePartition)
            └─ VectorParquetPartitionReader   (task thread)
                 ├─ prepare(file)     open: getFileStatus, footer, clip to the required schema,
                 │                    encoding check, ParquetFileReader.open, first filtered row group
                 ├─ install(prepared) column readers, partition values, or the per-file fallback
                 └─ next()            NativeParquetColumnReader per column → ColumnChunkDecoder (kernels)
                                      → batch-owned Arrow vectors + constant partition columns
```

**Opening a file.** Each split opens its file once. The `FileStatus` comes from the split instead of a separate HEAD request (#559 slice 1). The footer is read in a single stream, and `readNextFilteredRowGroup` applies the pushed filters through parquet-java: row-group statistics, dictionary and bloom filters, and the column index for page skipping. Rows are not filtered inside the decoder. Each task prefers the same hosts as Spark's `FileScanRDD`: up to three holding the most of its bytes, with `localhost` dropped. S3A reports `localhost` for every block, so on S3 the tasks have no preference, as Spark's do; on HDFS they keep block locality. Comet's equivalent switch, `spark.comet.parquet.rowFilterPushdown.enabled`, defaults to off as well.

**Decoding.** `NativeParquetColumnReader` turns each `DataPageV1` or `DataPageV2` into a kernel page. Definition levels and values are written into one reusable buffer per reader. `ColumnChunkDecoder` then decodes, at a row offset into the output:
- the validity bitmap, built from the definition levels;
- fixed-width lanes, written straight into the Arrow data buffer;
- dictionary indices, resolved against a decoded dictionary held in a GC-managed arena, for every lane but strings;
- for a string column (#612, `…dictionaryStrings`, on by default), dictionary ids instead: the batch is a `VectorDictionaryColumnVector` over one Arrow dictionary per row group, shared by its batches and reference-counted, closed when the reader and the last batch let go. A data page that falls back from the dictionary (parquet-java does so mid-chunk once the dictionary outgrows its page limit) resolves the ids already in the batch into bytes, and the rest of the row group decodes flat.

Parquet-java's `BytePacker` is injected for the bit-unpacking. Vectors are pooled and reused from one row group to the next.

**Per-file fallback.** A file whose column-chunk metadata lists an encoding the decoder does not handle goes to `SparkFallbackFileReader`, which is Spark's own vectorized reader for that one file. The check runs when the file is opened, so results stay correct and nothing fails part-way through a decode.

## Read-ahead

At 1 TB, TPC-DS `store_sales` is about 14.6k files of about 7 MB, and most of them hold a single row group. When everything ran on the task thread, JFR showed task threads parked on S3 for at least 66% of their time, and executors used 2–3 of their 13 cores.

The read-ahead fixes that with two independent depths:

| Key | Default | What runs in the background |
|---|---|---|
| `…nativeParquet.prefetchFiles` | 6 | The next N files of the split: `prepare(file)` in full, including the first row group. |
| `…nativeParquet.prefetchRowGroups` | 2 | The next M row groups of the current file. |

A few rules hold the design together:
- **Ordering.** Row-group reads form a chained `CompletableFuture` pipeline: each read starts after the previous one completes. A `ParquetFileReader` is therefore used by one thread at a time, and row groups arrive in file order.
- **Decode on the task thread.** Pages stay compressed until the task thread decompresses and decodes them, in order.
- **Task context.** Prefetch steps run with the task's `TaskContext` set. Files opened ahead and not used are closed at task end, including when a LIMIT stops the task early.
- **Platform threads.** The pool is cached daemon platform threads; the number alive is bounded by task slots × depth. Virtual threads deadlocked on JDK 25. A file open runs class initializers (Parquet's `BloomFilterImpl`, the S3 client), and a virtual thread blocked in a class initializer still pins its carrier. On q88, every carrier of one executor was pinned behind `BloomFilterImpl.<clinit>`, which was waiting for a log4j lock held by an unmounted virtual thread. `VectorParquetPrefetchPoolSuite` checks the pool's thread kind.
- **Metrics.** Task input metrics follow `FileScanRDD`: records are counted per batch, and bytes are the task thread's FileSystem read count plus the bytes each prefetch step read on its own thread.
- **Memory.** Per task, read-ahead adds up to N opened files and M row groups of compressed pages. It is not yet capped in bytes; see the work list.

**Decode-ahead (#606, off by default).** With `…nativeParquet.decodeAhead` = K, the reader loop runs on a producer thread of the task's own, up to K batches ahead (`DecodeAheadIterator`). The task thread takes batches in order and runs the operators above the scan on them, while the producer decodes the next ones across row groups and files.
- **Ownership.** A batch is the consumer's until its next `next()`, when its vectors go back to their column reader's pool. The pool is guarded for the two threads; a vector released after its file closed is closed instead.
- **Fallback files.** A batch of Spark's fallback reader is recycled by that reader, so the producer waits until the consumer has moved past it.
- **Threads.** A fresh virtual thread per task by default, or a platform thread. The first batch is decoded on the task thread, so the decoder's classes are initialized before a virtual thread runs it; file opens stay on the platform prefetch pool. A fresh thread also keeps the task's FileSystem byte count right.
- **Close.** At task end, LIMIT or failure, the producer is stopped, the queue is drained, the producer is joined, and only then is the reader closed. A producer failure is rethrown on the task thread in order.
- **Tests.** `DecodeAheadSuite` checks results against Spark's reader for K = 1 and 4 on both thread kinds, across row groups and files; a LIMIT that ends the task early, with the allocator back at its baseline; a table mixing native and fallback files; and a producer failure.

**Arena lifetime.** A column reader exists per column per file, so it must not close a shared FFM arena. Closing a shared arena triggers a JVM-wide handshake, and on q88 those handshakes cost 8–9% of executor CPU; with GC-managed scratch they are 0.2–0.4%.

## Support matrix

| | Decoded natively | Notes |
|---|---|---|
| Types with an engine lane | `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `double`, `date`, `timestamp` and `timestamp_ntz` (INT64 `MICROS`/`MILLIS`), `decimal` up to precision 38, UTF8 `string` with the default collation, `binary` (UTF8 lane, #705) | `NativeParquetSupport.isReadable`; the planner and the reader share this check |
| Types carried without a lane | `float` | Decoded on the lane of the same layout and emitted as Arrow vectors Spark reads; operators carry them, and an operator that computes on one stays Spark's |
| Physical storage | INT32, INT64, DOUBLE, FLOAT, BOOLEAN, BINARY; `FIXED_LEN_BYTE_ARRAY` for decimals and binary | Checked per file at open (`physicalMatches`), including the decimal scale |
| Encodings | `PLAIN`, `PLAIN_DICTIONARY`, `RLE_DICTIONARY`; `RLE` for booleans; `DELTA_BINARY_PACKED` on INT32/INT64; `DELTA_LENGTH_BYTE_ARRAY` on BINARY; `DELTA_BYTE_ARRAY` on BINARY and `FIXED_LEN_BYTE_ARRAY`; `BYTE_STREAM_SPLIT` on INT32/INT64/FLOAT/DOUBLE; definition levels in `RLE`/`BIT_PACKED` | `VectorParquetScanExec.supportsEncoding` |
| Calendar | Every rebase mode: `CORRECTED` as stored, `LEGACY` rebased from the hybrid calendar, `EXCEPTION` failing on an ancient value with Spark's error; `INT96` with its own mode and time-zone conversion (#568) | Resolved per file from the footer and the session, as Spark resolves it (`ParquetRebase`) |
| Data pages | v1 and v2 | v2 levels and data are read separately |
| Schema | Flat columns; structs, structs of structs included; top-level lists and maps of flat types, lists of structs of flat types, lists of lists of any depth ending in a flat type, maps of structs of flat types, any of these as a struct's field, and any other nesting of lists, maps and structs (#568) | A nested column holding a type with no decode path keeps Spark's scan for the plan |
| Metadata columns | The file-constant `_metadata` fields (`file_path`, `file_name`, `file_size`, `file_block_start`, `file_block_length`, `file_modification_time`), `_metadata.row_index`, and `input_file_name()` with its block siblings (#568) | Constants per file, from the format's extractors as `FileScanRDD` fills them; the row index is the row group's first row index plus the position. `input_file_name()` under decode-ahead keeps Spark's scan |
| Codecs | Whatever parquet-java decompresses (snappy, zstd, gzip, lz4, …) | Decompression is done by parquet-java |

What happens when a column is not supported:
- **Spark's scan for the whole plan:** a nested column holding a type with no decode path, a type with no decode path (a string with a non-default collation, say), `input_file_name()` while decode-ahead is on (the file is opened on the producer thread, and the function reads a task-thread holder), or a bucketed scan. The planner records the reason, so VecRuntime's operators still run above Spark's scan.
- **Spark's reader for that one file**, with that file's own rebase modes and INT96 conversion: a UTC-adjusted column read as `timestamp_ntz`, a struct field the file does not hold, a decimal stored with another scale, or an encoding on a physical type the decoder has no path for (`BYTE_STREAM_SPLIT` on `FIXED_LEN_BYTE_ARRAY`, say). The rest of the scan stays native.

## Status at 1 TB TPC-DS

**Full run, 2026-10-02.** All 103 queries once, the four engines back to back in one session (8 × m5.4xlarge, one AZ, the launcher's defaults):

| Engine | Total | Faster than Spark on |
|---|---:|---:|
| Spark 4.1.3 | 3,099 s | -- |
| Comet 1.0.0 | 1,999 s | 97 |
| Comet's native scan + VecRuntime operators and shuffle | 1,954 s | 98 |
| **VecRuntime with this reader** | **1,542 s** | **99** |

The scan-bound queries gain most: q88 35 s against Spark's 96 and Comet's 84, q9 31 against 73 and 51, q28 58 against 102 and 67. The per-query tables are in `docs/results.md`.

**The scan-bound queries, flag off against flag on.** These numbers come from 8 × m5.4xlarge in one AZ, with ACCP and the S3A read settings on. The flag-off and flag-on legs ran in one session; checksums equal the references.

| Query | Flag off | Flag on | Comet's native scan (earlier session, same operators) |
|---|---|---|---|
| q28 | 113.7 s | 34.7 s | 69.2 s |
| q9 | 95.9 s | 26.9 s | 53.2 s |
| q44 | 34.6 s | 10.8 s | 22.5 s |
| q88 | 119.7 s | 92.4 s (iterations vary 38–110 s) | 81.6 s |

The flag-on column comes from the read-ahead build with the arena fix. Comet's column comes from an earlier session that had the read-ahead but not the arena fix; that session's flag-on numbers were q28 46.6 s, q9 31.0 s, q44 14.1 s and q88 83.5 s, already ahead of Comet on the first three. The flag-on/off pair (round 14) and the Comet column (round 10) are from different sessions, so compare across that boundary with care.

Where executor CPU goes on q88 with the flag on (async-profiler):

| Bucket | Share |
|---|---|
| TLS | 25% |
| Our decode | 22% |
| S3 HTTP client | 21% |
| Our operators | 17% |
| zstd | 4% |

## Work list

The items run roughly from most to least valuable.

### Encodings

These are the Parquet v2 writer encodings. A file that uses one the reader does not decode yet goes entirely to Spark's reader. Parquet-java and Spark 3.x/4.x write them when `parquet.writer.version=v2`, and other engines often do by default.

1. **`DELTA_BINARY_PACKED`** for INT32 and INT64: **done** (slice 2). `DeltaBinaryPackedReader` reads the block and miniblock headers, unpacks each miniblock through the injected `BytePacker` (widths up to 32; INT64 widths above 32 with a scalar long unpack), and adds the deltas in the column's width, wrapping as the writer does. It decodes one miniblock at a time, so a batch can stop inside one. `ColumnChunkDecoder` writes the values straight into the lane, including INT32 widened to INT64. The footer check admits the encoding only on INT32/INT64 columns. It is checked:
   - against a from-scratch encoder of the spec (`DeltaBinaryPackedReaderTest`, `ColumnChunkDecoderTest`);
   - against parquet-java's own writers (`DeltaBinaryPackedCrossCheckSuite`);
   - through Spark with `parquet.writer.version=v2` and the dictionary off (`VectorParquetScanSuite`).

   `DeltaBinaryPackedBenchmark` (JMH, 20,000 values a page) decodes 2.0–4.0x as many pages per ms as Spark's `VectorizedDeltaBinaryPackedReader`:

   | values | ours (ops/ms) | Spark (ops/ms) |
   |---|---|---|
   | sorted INT32 | 35.2 | 17.7 |
   | random INT32 | 24.4 | 10.3 |
   | INT64 timestamps | 32.7 | 14.9 |
   | 44-bit random INT64 | 29.1 | 7.2 |

   That is a kernel check, not a verdict; no TPC-DS file uses the encoding.
2. **`DELTA_LENGTH_BYTE_ARRAY`** for strings: **done**. At `feedPage`, `DeltaBinaryPackedReader` decodes the page's lengths. Its `position()` after the last value is where the bytes start; the spec pads the last miniblock, so this is exact. Batches then copy the bytes into Arrow offsets and data like `PLAIN`. The total length is checked against the page's extent. JMH: 1.46x the pages per ms of parquet-java's `DeltaLengthByteArrayValuesReader` (10.2 against 7.0, 20,000 short strings, 1024-row batches).
3. **`DELTA_BYTE_ARRAY`** for strings: **done**. This is what parquet-java writes for v2 strings without a dictionary. The page is a `DELTA_BINARY_PACKED` run of prefix lengths (bytes shared with the previous value), then a `DELTA_LENGTH_BYTE_ARRAY` of the suffixes. Each value needs the previous one, so `feedPage` rebuilds the page's values back to back in one sequential pass into a reused buffer, and the batches copy out of it like `DELTA_LENGTH_BYTE_ARRAY`. The prefix chain restarts on each page. A prefix longer than the previous value, a count mismatch between the two runs, or suffix bytes past the page end fail the page. JMH: 2.0x the pages per ms of parquet-java's `DeltaByteArrayReader` (2.54 against 1.26, 20,000 sorted URL-like keys).
4. **`BYTE_STREAM_SPLIT`** for INT32, INT64 and DOUBLE: **done**. Byte j of value i is at stream j, position i, and the stride is the value region's length over the width. The scan now passes each page's real extent to the decoder, not its reused buffer's. Each value's K bytes are gathered from the streams straight into the lane, including INT32 widened to INT64. FLOAT and fixed-length byte arrays have no lane. Spark's vectorized reader rejects this encoding on INT32/INT64, so those files read only with the flag on. JMH: 8.8x the pages per ms of parquet-java's `ByteStreamSplitValuesReaderForDouble` / `ForLong` (20.5 against 2.3). It is a transpose (Vector API or SWAR, by vector width); see the optimization pass below.
5. **`RLE` for values**: **done**, for BOOLEAN (the only physical type it applies to as a value encoding). The page holds a 4-byte little-endian length, then a width-1 RLE/bit-packed hybrid stream. `RleBitPackingReader.readBits` decodes straight into the batch's bitmap: an RLE run of 1s sets its bit range a word at a time, a run of 0s writes nothing, and a width-1 bit-packed run is already an LSB-first bitmap, so it moves 64 bits a step. See BOOLEAN below for the numbers.

**Optimization pass on items 1–4.** JMH, one 20,000-value page in 1024-row batches, ops/ms. Each before/after pair ran on one host: x86 is an AVX-512 host, Graviton4 is m8g.4xlarge (Neoverse V2) with the default SVE codegen. Graviton4 with NEON codegen (`-XX:UseSVE=0`) matched SVE within a few percent, except the strings: `DELTA_BYTE_ARRAY` 2.8 → 3.6, `DELTA_LENGTH_BYTE_ARRAY` 7.0 → 7.6.

| page | x86 before → after | Graviton4 before → after | what changed |
|---|---|---|---|
| `BYTE_STREAM_SPLIT` DOUBLE / INT64 | 21.0 / 20.5 → 77.9 / 75.2 | 15.2 / 15.2 → 31.0 / 31.0 | a transpose instead of a byte gather: the Vector API (per register, a byte vector from each stream widened to lanes, shifted and ORed) from 256 bits, SWAR (an 8x8 byte transpose in three mask-and-shift stages on plain longs) at 128 bits (`ByteStreamSplitKernels`) |
| `BYTE_STREAM_SPLIT` INT32 | 46.1 → 298.0 | 60.9 (scalar) → 62.6 | the Vector API with int lanes |
| `DELTA_BINARY_PACKED` sorted / random INT32 | 36.1 / 23.9 → 42.4 / 28.5 | 32.6 / 19.6 → 37.0 / 24.2 | whole miniblocks unpacked and summed in the caller's array; only a miniblock a read stops inside goes through the buffer |
| `DELTA_BINARY_PACKED` INT64 timestamps | 33.4 → 41.0 | 31.2 → 33.9 | the same |
| `DELTA_LENGTH_BYTE_ARRAY` | 9.4 → 11.2 | 9.7 → 11.1 | the faster length decode |
| `DELTA_BYTE_ARRAY` | 2.8 → 3.7 | 3.1 → 4.6 | values rebuilt straight into the batch's bytes; the prefix comes from the previous value in the batch, or from a saved copy at a batch boundary. No per-page rebuild buffer and copy |

The transpose and the prefix sum are switches, read once at startup, for A/B runs:
- `-Dvecruntime.parquet.bssMode=auto|vector|swar|scalar`, default `auto`. `auto` uses the Vector API for 4-byte values at every width, and for 8-byte values when a register holds at least four longs. At 128 bits (NEON, or SVE on Graviton4) widening a byte vector into two long lanes is not intrinsified, and the 8-byte vector transpose ran 0.45 against SWAR's 31. It is the same on x86 forced to `-Dvecruntime.vectorBits=128`.
- `-Dvecruntime.parquet.deltaScan=scalar|vector`, default `scalar`. The vector prefix sum (a log-step scan per register) gives the same wrapping sums. It measured the same as the serial add on x86 (43.5 against 42.4) and slower on Graviton4 NEON (28.9 against 37.0).

`ByteStreamSplitKernelsTest` compares every variant, and the dispatch, with its scalar reference at every count, offset and array-end position. It passes at 128, 256 and 512 bits (`-Dvecruntime.vectorBits`).

For each encoding:
- Add a `ParquetPageDecoder.Encoding` case.
- Add a `ColumnChunkDecoder` path that writes at a row offset.
- Admit the encoding in `SupportedEncodings` only once all of these pass:
  - cross-check tests against parquet-java's own readers, in the style of `ParquetPageDecoderCrossCheckSuite`;
  - a round trip through Spark with `parquet.writer.version=v2`;
  - a decode micro-benchmark in `ParquetDecodeBenchmark`.

### Types

**Physical types are checked per file.** The planner admits a column by its Spark type, but each file decides how the column is stored. A `decimal(p <= 18)` can be INT32, INT64, `FIXED_LEN_BYTE_ARRAY` or `BYTE_ARRAY`: Spark's legacy writer, Hive and Impala use `FIXED_LEN_BYTE_ARRAY`. At open, `VectorParquetScanExec.physicalMatches` checks every requested column's physical type against its lane, and a mismatch falls that file over to Spark's reader, like an unsupported encoding. Before this check (0.0.4–0.0.5), a `FIXED_LEN_BYTE_ARRAY` decimal was decoded as INT64 and returned wrong values. The `apache/parquet-testing` corpus found it.

**Conformance corpus.** `ParquetTestingCorpusSuite` reads 27 files from `apache/parquet-testing`. They are vendored under `spark/src/test/resources/parquet-testing/`, Apache-2.0, at the commit in `SOURCE_SHA`. The files were written by parquet-mr of several ages, parquet-cpp, arrow-rs and Impala, and cover v2 encodings, empty and null pages, checksums, a dictionary page at offset 0, and corrupt files from `bad_data/`. The suite reads each file column by column:
- a column the scan supports must be read natively, with no per-file fallback, and match Spark's row-based reader;
- any other column must return what Spark returns with the plugin off, rows or refusal;
- a corrupt file Spark refuses must be refused too.

The corpus approach follows Hardwood (`hardwood-hq/hardwood`), as does bounding every file-declared size before allocating: the `DELTA_BINARY_PACKED` block size, a `BYTE_STREAM_SPLIT` region that is not a whole number of values, and bit-packed runs past the stream end.

- **BOOLEAN.** **Done**: v1 `PLAIN` (bit-packed, LSB first) and v2 `RLE`, into a `BOOL` lane (Arrow's bit-packed vector). `PLAIN` with every row present copies up to 64 bits a step from any source bit offset to any destination bit offset. With nulls, the bits are scattered one per present row. `flushBool` masks stray bits past the batch's last row. A page that claims more values than it holds fails.
  - Checked against a scalar reference, at every batch size, with nulls and across pages (`ColumnChunkDecoderTest`).
  - Checked against `readInt` at random bit offsets (`RleBitPackingReaderTest.readBitsMatchesReadIntAtWidthOne`).
  - Cross-checked against parquet-java's `BooleanPlainValuesWriter`/`Reader` and `RunLengthBitPackingHybridValuesWriter`/`Reader` (`DeltaBinaryPackedCrossCheckSuite`).
  - Round-tripped through Spark with v1 and v2 pages (`VectorParquetScanSuite`).
  - Covered by the corpus file `rle_boolean_encoding.parquet`.

  JMH (`V2EncodingsBenchmark`, x86, one 20,000-value page in 1024-row batches):

  | page | ours (ops/ms) | parquet-java (ops/ms) |
  |---|---|---|
  | `PLAIN`, random values | 837 | 10.7 (`BooleanPlainValuesReader`) |
  | `RLE`, runs of 1–64 | 212 | 11.2 (`RunLengthBitPackingHybridValuesReader`) |

  `RLE` measured 60.7 before `readBits`, which expands each value to an `int` and repacks it.
- **TINYINT / SMALLINT.** **Done**. INT32 physical (`INTEGER(8|16, true)`), decoded by the INT32 path (every encoding it has) into the INT32 lane, with the declared type on output (`VectorNarrowIntColumnVector`). A writer may store any INT32 under the annotation. Spark's readers narrow with a `(byte)` / `(short)` cast, so `fillFixed` sign-extends the low 8 or 16 bits of each slot in place. Operators compute on the `int`, and a value outside the range must wrap there exactly as Spark wraps it. This is one pass over the batch's ints; there is no new decode kernel, so no new JMH.
  - Checked through Spark with dictionary, `PLAIN` and v2 `DELTA_BINARY_PACKED` pages, nulls, the extremes, filters, an aggregate, arithmetic and casts (`VectorParquetScanSuite`).
  - Checked on a file written by parquet-java's `ExampleParquetWriter` with out-of-range values, against Spark's parquet-java-based reader. Without the narrowing, that test's sum differs.
  - The corpus columns `tinyint_col` / `smallint_col` of the `alltypes_*` files are now read natively.
- **FLOAT, BINARY and TIMESTAMP_NTZ.** **Done**, as option A of the design note: decoded by the scan, no engine lane. Each is decoded on the lane of the same layout:
  - FLOAT's 4-byte IEEE bits on INT32: `PLAIN`, dictionary, `BYTE_STREAM_SPLIT`;
  - BINARY on the UTF8 path: `PLAIN`, dictionary, DLBA, DBA, and `FIXED_LEN_BYTE_ARRAY` read as binary;
  - TIMESTAMP_NTZ on INT64: micros, MILLIS scaled with Spark's overflow check, only from a column not adjusted to UTC; a UTC-adjusted column falls over to Spark's reader, which refuses it.

  They are emitted as Arrow `Float4Vector` / `VarBinaryVector` / `TimeStampMicroVector`, which Spark's `ArrowColumnVector` reads. FLOAT stays out of `TypeMapping` and `ArrowVectorBuffers.vecTypeOf`, so no operator takes it for an INT32 lane (#712 tracks a lane for it). BINARY has had the UTF8 lane since #705, and TIMESTAMP_NTZ the INT64 lane: operators compare, hash, sort and carry them, and the expressions that read characters or the session time zone accept only `string` / `timestamp`, so over these they stay Spark's.

  Filters and projections carry them as foreign columns; an operator that computes on one stays Spark's, reading our batches as it reads Spark's scan. A table with such a column no longer keeps all its other columns on Spark's reader.
  - **Tests:**
    - `DeltaBinaryPackedCrossCheckSuite`: FLOAT `PLAIN` and `BYTE_STREAM_SPLIT` from parquet-java's writers, checked bit for bit against its readers (NaN payloads, -0.0, infinities).
    - `VectorParquetScanSuite`: round trips for all three (v1 dictionary, v1 `PLAIN` with MILLIS, v2), and filters, an aggregate and predicates on them. Also the UTC-adjusted TIMESTAMP_NTZ fallback.
    - The corpus columns `float_col`, `date_string_col`, `string_col`, `f32`, `float_plain`, `float_byte_stream_split`, `flba5_plain` and `binary_field` are now read natively.
  - **JMH:** no new decode kernel (the INT32, UTF8 and INT64 paths are the ones measured above), so there is no new benchmark.
  - **Option B** (real lanes, so operators compute on these types) stays open; see the note.
- **TIMESTAMP.** **Done** for INT64 `MICROS` and `MILLIS`, into the INT64 lane of micros. `physicalMatches` admits the `TIMESTAMP` annotation with either unit and either `isAdjustedToUTC`, the same match as Spark's `ParquetVectorUpdaterFactory`. `MILLIS` is scaled by 1000 with `Math.multiplyExact`, as Spark's `LongAsMicrosUpdater` does, so an overflowing value fails the read instead of wrapping. Only present rows are scaled, because a null slot holds whatever the reused buffer held.
  - **Rebase, per file** (#568 item 6). At open, `fallbackModes` resolves the file's datetime and INT96 rebase specs with `DataSourceUtils.datetimeRebaseSpec` / `int96RebaseSpec`, from the writer version and legacy keys in the footer, or else the session configuration, and the INT96 time-zone conversion the way `ParquetFileFormat` does. Every column reader gets them as one `ParquetRebase`, which runs over each decoded date or timestamp batch as Spark's `ParquetVectorUpdaterFactory` runs per value: a value below the last Julian switch (`RebaseDateTime.lastSwitchJulianDay` / `lastSwitchJulianTs`) fails the read under `EXCEPTION` with `DataSourceUtils.newRebaseExceptionInRead` and is rebased from the hybrid calendar under `LEGACY` (in the file's zone); `CORRECTED` leaves it. Above the switch both rebases are the identity, so modern data costs one comparison per value and no write. Row-group pushdown builds `ParquetFilters` with the file's own rebase spec, as Spark does.
  - **INT96** (Spark's default output timestamp type, and Hive's and Impala's): **done**. The 12-byte values (nanoseconds in the day, then the Julian day, little-endian) are staged on the fixed-length binary path, `PLAIN` and dictionary, and `ColumnChunkDecoder.flushInt96` converts them to micros exactly as Spark's `binaryToSQLTimestamp` / `fromJulianDay` do; the file's INT96 rebase mode and, for a file not written by parquet-mr under `int96TimestampConversion`, the conversion from the session zone to UTC follow.
  - **The fallback reader** is opened as `ParquetFileFormat` opens Spark's vectorized reader: over the split's byte range, with the scan's Hadoop conf carrying `setupHadoopConf`'s flags (INT96 as timestamp, binary as string, the session time zone, ...), and with the file's own rebase modes and INT96 conversion. The earlier `initialize(path, columns)` read the whole file on the first split, turned `int96AsTimestamp` off, and was hard-wired to `CORRECTED`. A `LEGACY` file would have been read without its rebase.
  - **Checked through Spark** with `TIMESTAMP_MICROS` and `TIMESTAMP_MILLIS` output, v1 and v2 pages, nulls, values before 1582 and after 2038, filters and an aggregate, all natively.
  - **Checked across one table** holding an INT96 file, a `LEGACY`-calendar file and a `CORRECTED` file, each decoded natively with Spark's values, under `CORRECTED` and `EXCEPTION`. `NativeParquetRebaseSuite` adds INT96 over v1 and v2 pages with and without dictionaries, inside a struct and a list, parquet-java's and parquet-testing's `int96_from_spark`, the time-zone conversion in two zones, `LEGACY` dates and timestamps on both sides of the switch with a pushed-down filter, and `EXCEPTION` on files without Spark's metadata: native when no value needs a rebase, Spark's own error when one does.
  - **Checked on a parquet-java-written `MILLIS` file** whose micros overflow. It fails with `ArithmeticException` under both readers.
- **Wide decimals and decimals stored as bytes.** **Done**. A decimal stored as `FIXED_LEN_BYTE_ARRAY` or `BINARY` is decoded by the binary page paths: `PLAIN` (fixed-length values have no length prefix), dictionary, `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY`, which is what parquet-java's v2 writer uses for FLBA. Its big-endian two's-complement bytes are then converted at flush (`ColumnChunkDecoder.flushDecimal`):
  - **`decimal(p > 18)`** goes into the DECIMAL128 lane: two little-endian limbs, sign-extended, which is Arrow's Decimal128 layout. A 16-byte value is read as two big-endian longs. A value longer than 16 bytes must be a sign extension, or the read fails.
  - **`decimal(p <= 18)`** stored as bytes goes into the INT64 lane, exactly as Spark's `binaryToUnscaledLong` computes it. Spark's legacy writer, Hive and Impala store such decimals; since #592 those files had fallen over to Spark's reader.
  - **Scale check.** `physicalMatches` now also requires the file's decimal annotation to have the requested scale and at most the requested precision. Spark reads, for example, a `decimal(9,2)` file as `decimal(12,4)` by rescaling, and the lanes carry the stored unscaled values as they are, so such a file falls over to Spark's reader. Before this check, an INT64 (or narrow INT32) decimal read with another scale was decoded natively without the rescale: wrong values.
  - **Tests.**
    - `ColumnChunkDecoderTest`: every width from 1 to 16, FLBA and BINARY (with sign-extended values longer than 16 bytes), both lanes, against `BigInteger` and Spark's formula.
    - `DeltaBinaryPackedCrossCheckSuite`: parquet-java's `FixedLenByteArrayPlainValuesWriter` and `DeltaByteArrayWriter`, read back by its matching readers.
    - `VectorParquetScanSuite`: `decimal(20|25|38)` round trips (v1 dictionary, `PLAIN`, v2 `DELTA_BYTE_ARRAY`, legacy format) and the rescale fallback.
    - The corpus files `fixed_length_decimal*` and `byte_array_decimal` are now read natively.
  - **JMH** (`V2EncodingsBenchmark` `flba-dec38`, x86, one 20,000-value page): 13.5 pages per ms. parquet-java's `FixedLenByteArrayPlainValuesReader.readBytes` does 13.7, but it only slices the bytes and does no conversion. Before the big-endian long reads this path measured 3.4.
- **Structs** (#568): **done**, structs of structs included, over every leaf type and encoding above. A struct has no column chunk of its own; Parquet records its nulls in its leaves' definition levels. `NativeParquetStructReader` holds one reader per field (a leaf's `NativeParquetColumnReader`, or another struct reader), and each leaf decodes its rows straight into its own batch vector, as a flat column does. The struct is present on a row where its first leaf's level reaches the struct's own definition level (the optional groups from the root down to it), so `ColumnChunkDecoder` keeps that one leaf's batch levels (`setRecordLevels`) and the struct reader turns them into the struct's nulls in one pass. That is how Spark's vectorized reader computes them too. The batch is Spark's struct column over the fields' columns (`VectorNestedColumnVector.struct`), which our operators read through `getStruct` / `getChild`.
  - **Per file**, `forEachLeaf` walks the clipped group in the struct's field order and checks each leaf's physical type against its lane (`physicalMatches`). A shape it cannot follow (a field matched by id under another name, a repeated group) falls the file over. So does a requested column the file does not hold: Spark's reader fills it with nulls. Dates and timestamps under a struct count for the calendar-rebase check, and an `INT96` leaf falls the file over as at top level.
  - **Strings** under a struct are read by value, not as dictionary ids (#612).
  - **Tests** (`NativeParquetStructSuite`), each against Spark's row-based reader, with the native scan asserted to decode the row groups itself:
    - Spark-written files: every lane type as a field, nulls at the struct and at the leaf, a struct of structs, v1 and v2 pages, dictionary on and off. Nested schema pruning, filters and aggregates on fields, a shuffle.
    - parquet-java's `ExampleParquetWriter`: required groups, required leaves under optional groups, and an optional group under a required one, which Spark does not write. v1 and v2, dictionary on and off.
    - Hardwood's `NestedSchemaTest` struct-of-struct shape, and the `apache/parquet-testing` deep struct from arrow-rs (required groups of required leaves; its unsigned `INT64` fields are a decimal on Spark's side, so selecting them keeps that file on Spark's reader).
    - Fallbacks with Spark's rows: a struct field the file lacks, and an `INT96` timestamp in a struct (the file). A struct holding a list or a map is read natively (below). Field names resolve case-insensitively.
  - **JMH:** no new decode kernel. The leaves are the flat decoders measured above, and the struct adds one pass over one leaf's levels per batch.
- **Lists and maps** (#568): **done** for a top-level `array<flat>` or `map<flat, flat>`, every flat type and encoding above. Each value slot of a list's leaf is an *entry* with a repetition and a definition level, and repetition level 0 starts a row.
  - **Levels.** On the leaf's path there is exactly one repeated node, in every list form Spark reads as an array of a flat type: the three-level `LIST`, Spark's legacy `bag` / `array` form, the two-level form, and a bare `repeated` leaf. Below the level of that node's parent, the row's list is null. Below the repeated node's own level it is empty. At or above it the entry is an element, and the element is null below the leaf's maximum level.
  - **The leaf in repeated mode** (`NativeParquetColumnReader`). It reads pages ahead of the decoder, into their own buffers, because a batch's rows may run past the page in hand. Each page's repetition and definition levels are kept, and the page past its repetition levels goes to the decoder (`Page.v1At` / `v2At`). `entriesForRows(n)` counts the entries of the next `n` rows.
  - **Decoding.** `readElements` walks those entries in runs: element runs decode back to back into the batch vector, and the entries of null and empty lists are skipped (`ColumnChunkDecoder.skip`, which consumes no value for them).
  - **The batch.** `NativeParquetListReader` builds the offsets and the null rows from the first leaf's levels. A map's key and value are two leaves under one `key_value` group, so each decodes its own elements. The batch is Spark's list or map column over the leaves' vectors (`VectorNestedColumnVector.list` / `map`).
  - **Per file**, `forEachLeaf` requires one leaf for a list, or two for a map (key first, under the same repeated group). Each leaf must be repeated exactly once and stored as its lane needs; anything else falls the file over.
  - **Tests** (`NativeParquetListSuite`), each against Spark's row-based reader, with the native scan asserted to decode the row groups itself:
    - **Spark-written files**, three-level and legacy form, v1 and v2 pages, dictionary on and off. Lists of every lane type with null lists, empty lists and null elements, a list longer than many pages, and maps with null values and empty maps. Queries cover `size`, element access, `exists` / `filter` / `transform`, `map_keys` / `map_values`, `explode` of a list and of a map, aggregates, and a shuffle.
    - **parquet-java's `ExampleParquetWriter`**, v1 and v2, dictionary on and off: the two-level list, required and optional three-level elements, a bare repeated double, and a map with optional values.
    - **Hardwood's dictionary-encoded list** spanning batches.
    - **The `apache/parquet-testing` files.** `list_columns`, `null_list` and the top-level repeated primitive of `repeated_primitive_no_list` are read natively. The nested maps, `incorrect_map_schema` and `map_no_value` answer what Spark answers; the nested lists and the legacy `LIST<LIST<int32>>` are read natively (lists of lists, below).
    - **Fallbacks:** none left for nesting itself; a nested column keeps Spark's scan only for an unreadable leaf type.
  - **JMH:** no new decode kernel. The leaf decoders are the ones measured above; the list adds one pass over a leaf's levels per batch, plus the page buffers.
- **Lists of structs** (#568): **done** for an array of structs of flat fields, in Spark's three-level form, the legacy two-level form (the repeated group is the struct) and parquet-java's. Every field is a leaf under the same struct group, repeated once, so all share one entry stream: the offsets and null lists come from the first field's levels as for a list of leaves, an element's struct is null where that field's level is below the struct group's, and each field decodes its own elements (`readElements`). The batch is Spark's list column over a struct column over the fields' vectors. Tests: Spark-written files (legacy and three-level form, v1 and v2 pages, dictionary on and off; nulls at the list, the struct and the field; `transform`, `filter`, `exists`, `explode`, aggregates) and parquet-java's three-level and two-level shapes, v1 and v2, each against Spark's row-based reader.
- **Lists of lists** (#568): **done**, any depth, over one flat leaf (`NativeParquetNestedListReader`). The leaf is repeated as deep as the arrays nest; the j-th repeated node on its path gives two thresholds, where the depth-j list is present (its parent's definition level) and where an entry is one of its elements (the node's own). An entry opens a depth-j list where its repetition level is at most j - 1 and the list exists there, and a depth-j element where its repetition level is at most j and its definition level reaches the element threshold; one pass over the leaf's levels builds every depth's offsets and nulls, and the leaf decodes its elements back to back. Spark's three-level `LIST` of `LIST`, the legacy two-level form and parquet-java's are all read. Tests: Spark-written lists two and three deep (legacy and three-level form, v1 and v2 pages, dictionary on and off; nulls and empty lists at every depth; `flatten`, nested `exists`, `explode`), parquet-java's three-level and two-level shapes, and parquet-testing's `nested_lists` and `old_list_structure`, which now read natively.
- **Lists and maps inside structs** (#568): **done**. A struct's field may be any list or map the list readers follow (lists and maps of flat types, lists of structs of flat types, lists of lists); each is read at its path under the struct. The struct's nulls come from its first field, whatever it is: a flat leaf's batch levels as before, or for a list or map the level of each row's first entry (`RowLevels`), since every entry of a row carries the struct's own definition. Tests: Spark-written structs whose first field is a list, with a leaf and a map after it, and a struct of a struct of a list of lists (legacy and three-level form, v1 and v2 pages, dictionary on and off; nulls at the struct, the list, the map and the leaves), and parquet-java's struct holding a three-level `LIST`, a legacy two-level list and a `MAP`, v1 and v2, each against Spark's row-based reader.
- **Maps of structs** (#568): **done** for a map whose value is a struct of flat fields, at the top or as a struct's field. The key and every value field are leaves under the same `key_value` group, repeated once, so they share one entry stream: the offsets and null maps come from the key's levels, as for a map of leaves; a value struct is null where its first field's level is below the value group's (the key is required, so its own levels cannot tell); and each leaf decodes its own elements. The batch is Spark's map column over the key's vector and a struct column over the value fields' vectors. Tests: Spark-written maps of structs, at the top and inside a struct (v1 and v2 pages, dictionary on and off; nulls at the map, the value struct and its fields; element access, `map_keys`, `explode`, aggregates), and parquet-java's `MAP` with an optional value group, v1 and v2, each against Spark's row-based reader.
- **Any nesting** (#568): **done**. A list or map whose nesting the readers above do not follow (a list of structs holding a list or a map, a map of structs holding a list, a list of lists of structs, a list of maps, a map of lists, a map of maps) is read by `NativeParquetTreeReader`. It builds a node tree from the clipped schema and the Spark type (Spark's three-level `LIST`, the legacy two-level forms by Spark's `isElementType` rule, bare repeated fields, `MAP` groups), and every node takes its structure from the levels of the first leaf below it, which encodes it as every other leaf there does: a node inside `j` lists exists at an entry with repetition level at most `j` whose definition level reaches the innermost enclosing list's element level; a list or map at depth `j + 1` is null below its own level and has an element at an entry with repetition level at most `j + 1` reaching its element level; a struct is null below its own level; and each leaf decodes its elements back to back. Tests: Spark-written lists of structs holding a list and a map, lists of lists of structs, maps of structs holding a list, maps of lists and lists of maps (legacy and three-level form, v1 and v2 pages, dictionary on and off; nulls and empty collections at every level), parquet-java's three-level list of structs holding a list and the legacy two-level group with a repeated field, and parquet-testing's `nested_maps`, each against Spark's row-based reader.
- **Metadata columns** (#568): **done**. The file-constant `_metadata` fields are constant columns after the partition columns, filled per file by `FileFormat.updateMetadataInternalRow` with the format's extractors, on native and fallback files alike (a fallback file's batch gets them appended, as `FileScanRDD` does). `_metadata.row_index` is `NativeParquetRowIndexColumn`: the scan reads whole row groups (column-index page filtering is off), so it is the footer's first row index of the group plus the position. The reader sets `InputFileBlockHolder` per file on the task thread for `input_file_name()`, `input_file_block_start()` and `input_file_block_length()`. Before this, a query reading `_metadata` failed in the native scan and `input_file_name()` did not return Spark's value. Tests: `NativeParquetMetadataSuite`, against Spark's row-based reader, on Spark- and parquet-java-written files, partitioned tables, split files, a decode filter and an INT96 file next to native ones.

### Performance

- **Make the sort compare robust against C2 uncommon-trap storms.** When q67 runs after q88, q28, q9 and q44 in the same app, 0–3 of 8 executors run q67's sort stage at about 2.3× CPU per task for the rest of the app.
  - The deoptimization log shows hundreds of thousands of `unstable_if` traps with action `none` in `StringCompareKernels.compareBytes` at bytecode 33, where the 8-byte loop runs out. The profile never saw that exit taken, and once HotSpot stops recompiling, the trap stays.
  - Ruled out: a separate copy of the method for sorts, and `-XX:-OptimizeUnstableIf`.
  - Being validated: warming the method's branch profile at class initialization.

  This is not specific to the native scan. It also happens with the flag off, less often.
- **Decode in parallel (#606).** Decode-ahead (above) runs decoding on a producer thread per task, off by default until measured. If one producer turns out to be the bottleneck on files with many large row groups, a second decoder on alternate row groups is the next step.
- **Cap the read-ahead in bytes.** Bound how much read-ahead each task and each executor keeps in flight by compressed bytes, not just by depth, so wide row groups cannot exhaust memory.
- **Row filtering during decode (late materialization).** Decode the predicate columns first, then decode the other columns only for the rows that survive, using the page index's row ranges. q88 and q9-style predicates over a few columns would benefit. Measure it against Comet with `rowFilterPushdown` on.
- **TLS and HTTP client CPU** (about 46% of q88's executor CPU). Options: fewer and larger range reads, which means tuning the Analytics Accelerator's block sizes; HTTP/1.1 connection reuse settings; and checking that the CRT-based client really uses ACCP's AES-GCM.
- **q88 iteration variance** (38–110 s within one leg). Next step is a per-task timeline across iterations to tell stragglers apart from S3 throttling or connection churn.
- **Vectored reads.** `parquet.hadoop.vectored.io.enabled=false` was 3–8% faster in one A/B. Re-measure it with read-ahead on before changing any default.

### Correctness and operations

- Run the golden suite with the flag on in CI, not only in local gates.
- Run the read-ahead tests under an executor-loss and task-retry stress test, to show that files opened ahead are always closed.
- Add metrics for the number of files opened ahead, the time task threads waited on read-ahead, and the number of fallback files.
