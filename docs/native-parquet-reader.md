---
layout: default
title: Native Parquet reader
---

# The native Parquet reader (`VectorParquetScanExec`)

This page is the design note for VecRuntime's own Parquet scan, #559. It covers what the scan does today, how it is put together, what it does not decode yet, and the work that is left.

The scan is **off by default**. You turn it on with `spark.vecruntime.scan.nativeParquet.enabled`. Its configuration keys are in the [Configuration reference](configuration.html), and the planner's fallback reasons are on the [Operators](operators.html) page.

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

**Opening a file.** Each split opens its file once. The `FileStatus` comes from the split instead of a separate HEAD request (#559 slice 1). The footer is read in a single stream, and `readNextFilteredRowGroup` applies the pushed filters through parquet-java: row-group statistics, dictionary and bloom filters, and the column index for page skipping. Rows are not filtered inside the decoder. Comet's equivalent switch, `spark.comet.parquet.rowFilterPushdown.enabled`, defaults to off as well.

**Decoding.** `NativeParquetColumnReader` turns each `DataPageV1` or `DataPageV2` into a kernel page. Definition levels and values are written into one reusable buffer per reader. `ColumnChunkDecoder` then decodes, at a row offset into the output:
- the validity bitmap, built from the definition levels;
- fixed-width lanes, written straight into the Arrow data buffer;
- dictionary indices, resolved against a decoded dictionary held in a GC-managed arena.

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

**Arena lifetime.** A column reader exists per column per file, so it must not close a shared FFM arena. Closing a shared arena triggers a JVM-wide handshake, and on q88 those handshakes cost 8–9% of executor CPU; with GC-managed scratch they are 0.2–0.4%.

## Support matrix

| | Decoded natively | Notes |
|---|---|---|
| Physical / logical types | INT32 (`int`, `date`), INT64 (`bigint`), DOUBLE, decimal with precision ≤ 18 (INT32/INT64 physical), UTF8 `string` with the default collation | `NativeParquetSupport.isReadable`; the planner and the reader share this check |
| Encodings | `PLAIN`, `PLAIN_DICTIONARY`, `RLE_DICTIONARY`; `DELTA_BINARY_PACKED` on INT32/INT64, `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY` on strings, `BYTE_STREAM_SPLIT` on INT32/INT64/DOUBLE; definition levels in `RLE`/`BIT_PACKED` | `VectorParquetScanExec.supportsEncoding` |
| Data pages | v1 and v2 | v2 levels and data are read separately |
| Schema | Flat only | Nested types keep Spark's scan |
| Codecs | Whatever parquet-java decompresses (snappy, zstd, gzip, lz4, …) | Decompression is done by parquet-java |

What happens when a column is not supported:
- **Spark's scan for the whole plan:** an unsupported type or a nested column, `INT96`, a non-`CORRECTED` date or timestamp rebase mode, or a bucketed scan. The planner records the reason, so VecRuntime's operators still run above Spark's scan.
- **Spark's reader for that one file:** an encoding on a physical type the decoder has no path for (`BYTE_STREAM_SPLIT` on `FIXED_LEN_BYTE_ARRAY`, say). The rest of the scan stays native.

## Status at 1 TB TPC-DS

These numbers come from 8 × m5.4xlarge in one AZ, with ACCP and the S3A read settings on. The flag-off and flag-on legs ran in one session; checksums equal the references.

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
4. **`BYTE_STREAM_SPLIT`** for INT32, INT64 and DOUBLE: **done**. Byte j of value i is at stream j, position i, and the stride is the value region's length over the width. The scan now passes each page's real extent to the decoder, not its reused buffer's. Each value's K bytes are gathered from the streams straight into the lane, including INT32 widened to INT64. FLOAT and fixed-length byte arrays have no lane. Spark's vectorized reader rejects this encoding on INT32/INT64, so those files read only with the flag on. JMH: 8.8x the pages per ms of parquet-java's `ByteStreamSplitValuesReaderForDouble` / `ForLong` (20.5 against 2.3). The gather is scalar; a Vector API transpose is still open.
5. **`RLE` for values**: booleans in v2 pages, and more generally the RLE/bit-packed hybrid as a value encoding, not only for levels. It comes together with BOOLEAN support in the next section.

For each encoding:
- Add a `ParquetPageDecoder.Encoding` case.
- Add a `ColumnChunkDecoder` path that writes at a row offset.
- Admit the encoding in `SupportedEncodings` only once all of these pass:
  - cross-check tests against parquet-java's own readers, in the style of `ParquetPageDecoderCrossCheckSuite`;
  - a round trip through Spark with `parquet.writer.version=v2`;
  - a decode micro-benchmark in `ParquetDecodeBenchmark`.

### Types

- **BOOLEAN.** Bit-packed in v1 `PLAIN` and RLE in v2. It needs a decode path into Arrow's bit-packed boolean vector.
- **TINYINT / SMALLINT.** INT32 physical; they need the narrow output wrapper (`VectorNarrowIntColumnVector`) for the lane.
- **FLOAT.** Needs a FLOAT32 lane, or a widening path that keeps exact values.
- **TIMESTAMP / TIMESTAMP_NTZ.** INT64 micros or millis, with unit conversion. The rebase mode has to be honoured per file, from the file's metadata and the session configuration. INT96, the legacy Impala/Hive layout, needs a conversion kernel; until then it keeps the plan-level fallback.
- **BINARY and wide decimals** (precision > 18, `FIXED_LEN_BYTE_ARRAY`). Need a variable-width binary lane and a 128-bit decimal lane.
- **Nested types** (structs, lists, maps). Need repetition levels and Arrow list and struct builders. This is the largest item and needs its own design.

### Performance

- **Make the sort compare robust against C2 uncommon-trap storms.** When q67 runs after q88, q28, q9 and q44 in the same app, 0–3 of 8 executors run q67's sort stage at about 2.3× CPU per task for the rest of the app.
  - The deoptimization log shows hundreds of thousands of `unstable_if` traps with action `none` in `StringCompareKernels.compareBytes` at bytecode 33, where the 8-byte loop runs out. The profile never saw that exit taken, and once HotSpot stops recompiling, the trap stays.
  - Ruled out: a separate copy of the method for sorts, and `-XX:-OptimizeUnstableIf`.
  - Being validated: warming the method's branch profile at class initialization.

  This is not specific to the native scan. It also happens with the flag off, less often.
- **Decode row groups in parallel for large files.** Read-ahead overlaps I/O with decode, but decode itself is serial per task. Files with many large row groups could decode the row group after next on a second thread into separately owned vectors, while keeping the batch order.
- **Cap the read-ahead in bytes.** Bound how much read-ahead each task and each executor keeps in flight by compressed bytes, not just by depth, so wide row groups cannot exhaust memory.
- **Row filtering during decode (late materialization).** Decode the predicate columns first, then decode the other columns only for the rows that survive, using the page index's row ranges. q88 and q9-style predicates over a few columns would benefit. Measure it against Comet with `rowFilterPushdown` on.
- **TLS and HTTP client CPU** (about 46% of q88's executor CPU). Options: fewer and larger range reads, which means tuning the Analytics Accelerator's block sizes; HTTP/1.1 connection reuse settings; and checking that the CRT-based client really uses ACCP's AES-GCM.
- **q88 iteration variance** (38–110 s within one leg). Next step is a per-task timeline across iterations to tell stragglers apart from S3 throttling or connection churn.
- **Vectored reads.** `parquet.hadoop.vectored.io.enabled=false` was 3–8% faster in one A/B. Re-measure it with read-ahead on before changing any default.

### Correctness and operations

- Run the golden suite with the flag on in CI, not only in local gates.
- Run the read-ahead tests under an executor-loss and task-retry stress test, to show that files opened ahead are always closed.
- Add metrics for the number of files opened ahead, the time task threads waited on read-ahead, and the number of fallback files.
