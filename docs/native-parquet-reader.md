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
| Encodings | `PLAIN`, `PLAIN_DICTIONARY`, `RLE_DICTIONARY`; definition levels in `RLE`/`BIT_PACKED` | `VectorParquetScanExec.SupportedEncodings` |
| Data pages | v1 and v2 | v2 levels and data are read separately |
| Schema | Flat only | Nested types keep Spark's scan |
| Codecs | Whatever parquet-java decompresses (snappy, zstd, gzip, lz4, …) | Decompression is done by parquet-java |

What happens when a column is not supported:
- **Spark's scan for the whole plan:** an unsupported type or a nested column, `INT96`, a non-`CORRECTED` date or timestamp rebase mode, or a bucketed scan. The planner records the reason, so VecRuntime's operators still run above Spark's scan.
- **Spark's reader for that one file:** an unsupported encoding in a file's column chunks (`DELTA_*`, `BYTE_STREAM_SPLIT`). The rest of the scan stays native.

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

These are the Parquet v2 writer encodings. Today a file that uses any of them goes entirely to Spark's reader. Parquet-java and Spark 3.x/4.x write them when `parquet.writer.version=v2`, and other engines often do by default.

1. **`DELTA_BINARY_PACKED`** for INT32 and INT64. The pages are made of blocks, each with a minimum delta and miniblocks of bit-packed deltas, then a prefix sum. The kernel needs:
   - a block/miniblock header reader;
   - a bit unpack per miniblock width, which can reuse the injected `BytePacker`;
   - a vectorizable prefix sum written straight into the lane.

   It is the most common v2 encoding for integer and date columns.
2. **`DELTA_LENGTH_BYTE_ARRAY`** for strings: a `DELTA_BINARY_PACKED` run of lengths, followed by the concatenated bytes. It maps directly onto Arrow's offsets and data buffers, so it builds on item 1.
3. **`DELTA_BYTE_ARRAY`** for strings: a prefix length and a suffix per value, so each value reuses part of the previous one. Rebuilding the values needs the previous value, so it is sequential within a page; the output is still Arrow offsets and data.
4. **`BYTE_STREAM_SPLIT`** for FLOAT, DOUBLE, and in newer files also INT32, INT64 and fixed-length byte arrays. The bytes of each value are interleaved across K streams. Decoding is a transpose that SIMD handles well, with the Vector API, from a page into the lane.
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
