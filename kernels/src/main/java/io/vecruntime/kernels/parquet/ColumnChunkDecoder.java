/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vecruntime.kernels.parquet;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.IntFunction;

import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;

/**
 * Streaming decoder for a flat (max repetition level 0) Parquet column chunk:
 * it decodes the NEXT {@code n} rows of the chunk straight into
 * <em>batch-sized</em> reused heap primitive arrays, resuming across page and
 * run boundaries, so a consumer emits fixed-size batches without materialising
 * the whole row group and without a per-batch scan of it. This is the decode
 * core of #559 slice 1's scan node (the streaming rewrite of the
 * row-group-at-a-time first cut).
 *
 * <h2>Resumable state (a batch spans pages and runs)</h2>
 *
 * A batch size need not divide a page's row count, so a batch starts and ends
 * anywhere inside a page. The decoder keeps, for the CURRENT page: its decoded
 * definition levels ({@link #pageLevels}, cheap {@code int[valueCount]}), a ROW
 * cursor into the page ({@link #pageRow}), and the position of the next
 * unconsumed value -- a byte offset for PLAIN ({@link #plainCursor}) or a live
 * {@link RleBitPackingReader} for {@code RLE_DICTIONARY} ids ({@link
 * #idReader}, which resumes mid-run: its {@code readInts} picks up exactly
 * where the previous batch left off, including a bit-packed group split across
 * batches). The dictionary is decoded once per chunk. The consumer feeds pages
 * on demand ({@link #needsPage()} / {@link #feedPage}) and pulls rows ({@link
 * #readBatch}).
 *
 * <h2>Write path: direct off-heap stores (the #559 measured decision)</h2>
 *
 * A first cut wrote every value with a checked FFM {@code MemorySegment.set}
 * and a JFR profile showed the executor in {@code checkValidStateRaw} / {@code
 * checkBounds} / {@code VarHandleGuards} per value, 1.7x slower than Spark's
 * on-heap reader. The fix staged present values into reused heap {@code
 * int[]}/{@code long[]}/{@code double[]} and bulk-copied each finished buffer
 * into the batch's Arrow memory with one {@link MemorySegment#copy}. The
 * maintainer's requirement is "decode straight into the output, no intermediate
 * copies", so a JMH A/B (ParquetScanE2EBenchmark, 4M-row file, avgt ms/op)
 * compared the staging + one bulk copy against writing the hot fixed-width
 * lanes STRAIGHT into the Arrow data segment two ways:
 *
 * <ul>
 *   <li>staging + bulk copy: 135.2 ms/op
 *   <li>direct A -- a hoisted {@code MemorySegment} + counted loop (ValueLayout
 *       stores; one {@code MemorySegment.copy} for the plain all-present case),
 *       validity written as 64-bit words: 135.9 ms/op
 *   <li>direct B -- a {@code ByteBuffer.order(LE)} view ({@code
 *       asIntBuffer}/{@code asLongBuffer}/{@code asDoubleBuffer}) of the Arrow
 *       buffer, absolute {@code put(i, v)}: 134.8 ms/op
 * </ul>
 *
 * The three are within noise (error bars overlap; all ~20 ms/op below Spark's
 * 154). The direct forms are neither faster nor slower, so by the requirement
 * the DIRECT write is kept: the fixed-width lanes ({@link #readBatchDirectA} /
 * {@link #readBatchDirectB}) decode present values and validity straight into
 * the caller's Arrow segments with no staging array and no bulk copy. The
 * production reader uses direct A (the hoisted-{@code MemorySegment} form,
 * matching this file's existing FFM idiom); direct B is kept for the benchmark.
 * The {@link #readBatch} + {@link #flushFixed} staging path remains for UTF8
 * (its two passes already size one data buffer and bulk-copy offsets+bytes
 * once) and as the benchmark's staging baseline. The hot loops still use plain
 * array stores where a heap source is involved (the page bytes parquet-java
 * hands over, the decoded dictionary); only the DESTINATION moved from a
 * staging array to the Arrow segment.
 *
 * <h2>Null count is free</h2>
 *
 * The definition-level pass that builds validity also counts the batch's nulls
 * ({@link #batchNullCount()}); the consumer never calls Arrow's O(rows) {@code
 * getNullCount}. A batch with no nulls needs no validity buffer at all.
 */
public final class ColumnChunkDecoder {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    // Little-endian views over a plain byte[]: intrinsify to unaligned loads with no FFM
    // session/bounds/alignment checks (the page bytes are a heap byte[] parquet-java hands us).
    private static final java.lang.invoke.VarHandle BA_INT = java.lang.invoke.MethodHandles.byteArrayViewVarHandle(int[].class, java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final java.lang.invoke.VarHandle BA_LONG = java.lang.invoke.MethodHandles.byteArrayViewVarHandle(long[].class, java.nio.ByteOrder.LITTLE_ENDIAN);

    private static int leInt(byte[] a, int off) {
        return (int) BA_INT.get(a, off);
    }

    private static long leLong(byte[] a, int off) {
        return (long) BA_LONG.get(a, off);
    }

    private final VecType type;
    private final VecType physicalType;
    private final int maxDefLevel;
    private final int defBitWidth;
    private final IntFunction<GroupUnpacker> unpackerFactory;

    private int rowGroupRows; // total rows in the chunk (for the overrun guard)
    private int rowsDone; // rows emitted so far across the whole chunk

    // ---- batch-sized staging (only the lane's array is allocated) ----
    private int[] ints; // INT32 lane
    private long[] longs; // INT64 lane (also widened narrow decimal)
    private double[] doubles; // FLOAT64 lane
    private long[] validityWords; // batch validity, 64 rows per word; only when maxDefLevel > 0
    private long[] boolWords; // BOOL lane: the batch's values, 64 rows per word, LSB first (Arrow's bit order)
    private int[] utf8Offsets; // UTF8: batchRows + 1 entries
    private byte[] utf8Data; // UTF8: gathered bytes for the batch
    private int utf8Len; // bytes written into utf8Data this batch
    private int batchNulls; // nulls in the current batch (from the definition levels)

    // ---- current-page state (resumable) ----
    private byte[] pageData;
    private ParquetPageDecoder.Encoding pageEncoding;
    private int pageValueCount;
    private int pageRow; // next row within the page to emit
    private int[] pageLevels = new int[0]; // decoded def levels for the page (null lane: unused)
    private int plainCursor; // byte offset of the next unconsumed PLAIN value
    private int utf8PlainCursor; // byte offset of the next unconsumed PLAIN utf8 value (== plainCursor role)
    private RleBitPackingReader idReader; // live RLE id reader for a dictionary page (resumes mid-run)
    private DeltaBinaryPackedReader deltaReader; // live reader for a DELTA_BINARY_PACKED page (resumes mid-miniblock)
    private int[] pageLengths = new int[0]; // DELTA_LENGTH_BYTE_ARRAY: the page's value lengths, decoded at feedPage
    private int[] prefixLengths = new int[0]; // DELTA_BYTE_ARRAY: bytes each value shares with the previous one
    private int[] suffixLengths = new int[0]; // DELTA_BYTE_ARRAY: each value's suffix length
    private byte[] dbaPrev = new byte[0]; // DELTA_BYTE_ARRAY: the previous value, when it is not in this batch's bytes
    private int dbaPrevStart = -1; // DELTA_BYTE_ARRAY: the previous value's start in utf8Data, or -1 (it is in dbaPrev)
    private int dbaPrevLen; // DELTA_BYTE_ARRAY: the previous value's length
    private byte[] binarySrc; // DELTA_LENGTH_BYTE_ARRAY: the current page's bytes
    private int lengthCursor; // next unconsumed entry of pageLengths
    private int bssStart; // BYTE_STREAM_SPLIT: offset of the first stream
    private int bssStride; // BYTE_STREAM_SPLIT: values in the page (= bytes per stream)
    private int bssIndex; // BYTE_STREAM_SPLIT: next unconsumed value
    private long boolBitCursor; // BOOL PLAIN: bit offset of the next unconsumed value in pageData
    private int boolPageEnd; // BOOL: one past the last byte of the page's value region
    private RleBitPackingReader boolReader; // BOOL RLE: the page's 1-bit hybrid value stream
    private long pageEndExclusive; // PLAIN: one past the last byte of the page's value region
    private int fixedLength; // UTF8 staging of a FIXED_LEN_BYTE_ARRAY column: the value width (0: length-prefixed)

    // ---- reused per-batch scratch ----
    private int[] idScratch = new int[0]; // dictionary ids for a page-slice
    private int[] levelScratch = new int[0]; // page-level decode buffer
    private int[] deltaInts = new int[0]; // DELTA_BINARY_PACKED INT32 values for a page-slice
    private long[] deltaLongs = new long[0]; // DELTA_BINARY_PACKED INT64 (or widened INT32) values for a page-slice

    // ---- heap dictionary, decoded once per chunk ----
    private boolean hasDictionary;
    private int[] dictInts;
    private long[] dictLongs;
    private double[] dictDoubles;
    private byte[] dictBytes;
    private int[] dictOffsets;

    private final GroupUnpacker[] unpackers = new GroupUnpacker[33];
    private final boolean[] unpackerSet = new boolean[33];

    public ColumnChunkDecoder(VecType type, int maxDefLevel, int batchRows,
            IntFunction<GroupUnpacker> unpackerFactory) {
        this(type, type, maxDefLevel, batchRows, unpackerFactory);
    }

    public ColumnChunkDecoder(VecType physicalType, VecType type, int maxDefLevel,
            int batchRows, IntFunction<GroupUnpacker> unpackerFactory) {
        this.physicalType = physicalType;
        this.type = type;
        this.maxDefLevel = maxDefLevel;
        this.defBitWidth = ParquetPageDecoder.bitWidth(maxDefLevel);
        this.unpackerFactory = unpackerFactory;
        allocateBatch(batchRows);
    }

    /** Sizes the batch staging arrays for {@code batchRows} rows (grow only). */
    private void allocateBatch(int batchRows) {
        switch (type) {
            case INT32 -> ints = grow(ints, batchRows);
            case INT64 -> longs = grow(longs, batchRows);
            case FLOAT64 -> doubles = grow(doubles, batchRows);
            case BOOL -> {
                int words = (batchRows + 63) >>> 6;
                if (boolWords == null || boolWords.length < words) {
                    boolWords = new long[words];
                }
            }
            case UTF8 -> {
                if (utf8Offsets == null || utf8Offsets.length < batchRows + 1) {
                    utf8Offsets = new int[batchRows + 1];
                }
                if (utf8Data == null) {
                    utf8Data = new byte[Math.max(1 << 14, batchRows)];
                }
            }
            default -> throw new IllegalArgumentException("unsupported lane " + type);
        }
        if (maxDefLevel > 0) {
            int words = (batchRows + 63) >>> 6;
            if (validityWords == null || validityWords.length < words) {
                validityWords = new long[words];
            }
        }
    }

    /**
     * Begin a new column chunk of {@code rowGroupRows} rows (dictionary kept if
     * re-set).
     */
    public void startChunk(int rowGroupRows) {
        this.rowGroupRows = rowGroupRows;
        this.rowsDone = 0;
        this.pageData = null;
        this.pageValueCount = 0;
        this.pageRow = 0;
        this.idReader = null;
        this.deltaReader = null;
        this.hasDictionary = false;
    }

    // ------------------------------------------------------------------ dictionary (heap)

    public void setDictionary(VectorBuffers dict) {
        hasDictionary = true;
        int n = dict.length();
        switch (physicalType) {
            case INT32 -> {
                dictInts = ensureInts(dictInts, n);
                MemorySegment d = dict.data();
                for (int i = 0; i < n; i++) {
                    dictInts[i] = d.get(LE_INT, (long) i << 2);
                }
            }
            case INT64 -> {
                dictLongs = ensureLongs(dictLongs, n);
                MemorySegment d = dict.data();
                for (int i = 0; i < n; i++) {
                    dictLongs[i] = d.get(LE_LONG, (long) i << 3);
                }
            }
            case FLOAT64 -> {
                dictDoubles = dictDoubles == null || dictDoubles.length < n
                        ? new double[n]
                        : dictDoubles;
                MemorySegment d = dict.data();
                for (int i = 0; i < n; i++) {
                    dictDoubles[i] = Double.longBitsToDouble(d.get(LE_LONG, (long) i << 3));
                }
            }
            case UTF8 -> {
                dictOffsets = ensureInts(dictOffsets, n + 1);
                MemorySegment off = dict.offsets();
                for (int i = 0; i <= n; i++) {
                    dictOffsets[i] = off.get(LE_INT, (long) i << 2);
                }
                int total = dictOffsets[n];
                dictBytes = dictBytes == null || dictBytes.length < Math.max(total, 1)
                        ? new byte[Math.max(total, 1)]
                        : dictBytes;
                MemorySegment.copy(dict.data(), ValueLayout.JAVA_BYTE, 0L, dictBytes, 0,
                        total);
            }
            default -> throw new IllegalArgumentException("unsupported dictionary lane " + physicalType);
        }
    }

    public boolean hasDictionary() {
        return hasDictionary;
    }

    // ------------------------------------------------------------------ page feed

    /**
     * True when the current page is fully consumed (feed the next one before
     * {@link #readBatch}).
     */
    public boolean needsPage() {
        return pageData == null || pageRow >= pageValueCount;
    }

    /** Rows still unemitted in the current page. */
    public int pageRemaining() {
        return pageData == null ? 0 : pageValueCount - pageRow;
    }

    public int rowsDone() {
        return rowsDone;
    }

    /**
     * Hand the decoder the next fully-loaded page. Decodes its definition levels
     * (so present/null per row is known) and positions the value cursor at the
     * page's first value: a byte offset for PLAIN, or a fresh {@link
     * RleBitPackingReader} for the dictionary-id stream that {@link #readBatch}
     * then resumes across batches.
     */
    public void feedPage(Page page) {
        int n = page.valueCount;
        if (rowsDone + n > rowGroupRows) {
            throw new IllegalStateException("page overruns the row group: " + (rowsDone + n) + " > " + rowGroupRows);
        }
        this.pageData = page.data;
        this.pageEncoding = page.encoding;
        this.pageValueCount = n;
        this.pageRow = 0;
        long valuesStart;
        if (maxDefLevel == 0) {
            valuesStart = page.valuesStart();
        } else {
            if (pageLevels.length < n) {
                pageLevels = new int[Math.max(n, 2 * pageLevels.length)];
            }
            long levelStart;
            int levelLen;
            if (page.v2) {
                levelStart = page.levelsOffset;
                levelLen = page.levelsLength;
                valuesStart = page.valuesOffset;
            } else {
                int len = readLeInt(page.data, page.dataOffset);
                levelStart = page.dataOffset + 4;
                levelLen = len;
                valuesStart = levelStart + len;
            }
            reader(page.data, (int) levelStart, levelLen, defBitWidth).readInts(pageLevels, 0, n);
        }
        this.deltaReader = null;
        this.boolReader = null;
        if (type == VecType.BOOL) {
            feedBoolPage(page, valuesStart);
            return;
        }
        if (page.encoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            this.idReader = dictionaryIds(page, valuesStart);
            this.plainCursor = 0;
            this.utf8PlainCursor = 0;
        } else if (page.encoding == ParquetPageDecoder.Encoding.DELTA_BINARY_PACKED) {
            if (physicalType != VecType.INT32 && physicalType != VecType.INT64) {
                throw new IllegalArgumentException("DELTA_BINARY_PACKED on a " + physicalType + " lane");
            }
            this.idReader = null;
            this.deltaReader = new DeltaBinaryPackedReader(page.data, (int) valuesStart, (int) (page.dataEnd() - valuesStart),
                    physicalType == VecType.INT64, this::unpackerFor);
            this.plainCursor = 0;
            this.utf8PlainCursor = 0;
        } else if (page.encoding == ParquetPageDecoder.Encoding.DELTA_LENGTH_BYTE_ARRAY) {
            if (type != VecType.UTF8) {
                throw new IllegalArgumentException("DELTA_LENGTH_BYTE_ARRAY on a " + type + " lane");
            }
            // The lengths are a DELTA_BINARY_PACKED stream of every present value, then the bytes follow
            // back to back. Decode all lengths now: their stream's end is where the bytes start.
            this.idReader = null;
            int end = (int) page.dataEnd();
            DeltaBinaryPackedReader lengths = new DeltaBinaryPackedReader(page.data, (int) valuesStart, end - (int) valuesStart, false,
                    this::unpackerFor);
            int count = lengths.remaining();
            if (pageLengths.length < count) {
                pageLengths = new int[Math.max(count, 2 * pageLengths.length)];
            }
            lengths.readInts(pageLengths, 0, count);
            int bytesStart = lengths.position();
            long total = 0;
            for (int i = 0; i < count; i++) {
                if (pageLengths[i] < 0) {
                    throw new IllegalStateException("DELTA_LENGTH_BYTE_ARRAY: negative length " + pageLengths[i]);
                }
                total += pageLengths[i];
            }
            if (bytesStart + total > end) {
                throw new IllegalStateException("DELTA_LENGTH_BYTE_ARRAY: " + total + " value bytes past the end of the page");
            }
            this.lengthCursor = 0;
            this.plainCursor = 0;
            this.utf8PlainCursor = bytesStart;
            this.binarySrc = page.data;
        } else if (page.encoding == ParquetPageDecoder.Encoding.DELTA_BYTE_ARRAY) {
            if (type != VecType.UTF8) {
                throw new IllegalArgumentException("DELTA_BYTE_ARRAY on a " + type + " lane");
            }
            this.idReader = null;
            this.utf8PlainCursor = prepareDeltaByteArray(page.data, (int) valuesStart, (int) page.dataEnd());
            this.lengthCursor = 0;
            this.plainCursor = 0;
            this.binarySrc = page.data;
            this.dbaPrevStart = -1;
            this.dbaPrevLen = 0;
        } else if (page.encoding == ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT) {
            if (physicalType == VecType.UTF8) {
                throw new IllegalArgumentException("BYTE_STREAM_SPLIT on a UTF8 lane");
            }
            // K = byte width streams, each holding one byte of every present value: byte j of value i is at
            // stream j, position i.
            this.idReader = null;
            int len = (int) (page.dataEnd() - valuesStart);
            if (len < 0 || len % byteWidth() != 0) {
                throw new IllegalStateException("BYTE_STREAM_SPLIT: a " + len + "-byte value region is not a whole number of " + byteWidth() + "-byte values");
            }
            this.bssStart = (int) valuesStart;
            this.bssStride = len / byteWidth();
            this.bssIndex = 0;
            this.plainCursor = 0;
            this.utf8PlainCursor = 0;
        } else {
            this.idReader = null;
            this.plainCursor = (int) valuesStart;
            this.utf8PlainCursor = (int) valuesStart;
            this.pageEndExclusive = page.dataEnd();
        }
    }

    // ------------------------------------------------------------------ batch read

    /**
     * Prepare to fill a new batch of at most {@code batchRows} rows: sizes the
     * staging arrays and resets the batch's write position and null count.
     */
    public void startBatch(int batchRows) {
        allocateBatch(batchRows);
        utf8Len = 0;
        batchNulls = 0;
        if (type == VecType.BOOL) {
            java.util.Arrays.fill(boolWords, 0, (batchRows + 63) >>> 6, 0L); // values are ORed in
        }
        if (maxDefLevel > 0) {
            int words = (batchRows + 63) >>> 6;
            java.util.Arrays.fill(validityWords, 0, words, -1L); // start all-valid; nulls clear their bit
        }
    }

    /**
     * Decode up to {@code want} rows of the current page into the batch staging
     * arrays starting at row {@code dstBase} within the batch, and advance the
     * page cursor. Returns the number of rows written (<= {@code want}, <= the
     * page's remaining rows). Call repeatedly, feeding a new page whenever {@link
     * #needsPage()}, until a batch of the desired size is filled.
     */
    public int readBatch(int want, int dstBase) {
        if (pageData == null || pageRow >= pageValueCount) {
            return 0;
        }
        int m = Math.min(want, pageValueCount - pageRow);
        int presentCount;
        int[] present; // slot within [0,m) of each present row; null => all present
        if (maxDefLevel == 0) {
            presentCount = m;
            present = null;
        } else {
            present = idScratchPresent(m);
            presentCount = buildValidityAndPresent(dstBase, m, present);
            if (presentCount == m) {
                present = null;
            }
        }
        if (type == VecType.UTF8) {
            decodeBinary(dstBase, m, present, presentCount);
        } else if (type == VecType.BOOL) {
            decodeBool(dstBase, present, presentCount);
        } else {
            decodeFixed(dstBase, present, presentCount);
        }
        pageRow += m;
        rowsDone += m;
        return m;
    }

    public int batchNullCount() {
        return batchNulls;
    }

    // ================================================================== direct-write variants (#559 study)
    //
    // The production path (above) scatters decoded values into reused heap staging arrays and the consumer
    // bulk-copies each finished buffer into the batch's Arrow memory with one MemorySegment.copy. The
    // maintainer's requirement is "decoding straight into the output, no intermediate copies"; these
    // variants decode the hot NO-NULL fixed-width lanes (INT32/INT64/FLOAT64, plain and dictionary gather)
    // STRAIGHT into the batch vector's off-heap Arrow data segment, so a JMH A/B can decide whether the
    // staging + one bulk copy is actually worth keeping. Two ways to write without per-value FFM checks:
    //   A: hoist the MemorySegment to a local and write it in a simple counted loop (ValueLayout stores);
    //      the plain, all-present case additionally does ONE MemorySegment.copy of the page bytes.
    //   B: a ByteBuffer.order(LE) view (asIntBuffer / asLongBuffer / asDoubleBuffer) over the Arrow buffer,
    //      absolute put(i, v).
    // Not for production unless the A/B wins; the chosen path is recorded in this class's javadoc.
    //
    // These handle only the no-null fixed-width case (the review's hot paths); a batch with nulls or a UTF8
    // lane is decoded through the staging path and bulk-flushed, exactly as production, since that is not
    // the case under study.

    /**
     * Direct variant A: hoisted MemorySegment + counted loop. Writes present
     * values straight into the Arrow data segment at their row slots, and (when
     * the lane is nullable) the validity words straight into the Arrow validity
     * segment -- no staging arrays, no bulk copy. {@code outValidity} may be
     * null for a lane with no def levels.
     */
    public int readBatchDirectA(int want, int dstBase, MemorySegment outData,
            MemorySegment outValidity) {
        if (pageData == null || pageRow >= pageValueCount) {
            return 0;
        }
        int m = Math.min(want, pageValueCount - pageRow);
        int presentCount;
        int[] present;
        if (maxDefLevel == 0) {
            presentCount = m;
            present = null;
        } else {
            present = idScratchPresent(m);
            presentCount = buildValidityDirect(dstBase, m, present, outValidity);
            if (presentCount == m) {
                present = null;
            }
        }
        if (pageEncoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            idReader.readInts(ids, 0, presentCount);
            gatherFixedDirectA(ids, present, dstBase, presentCount, outData);
        } else if (decodedPage()) {
            deltaFixedDirectA(present, dstBase, presentCount, outData);
        } else {
            plainFixedDirectA(present, dstBase, presentCount, outData);
        }
        pageRow += m;
        rowsDone += m;
        return m;
    }

    /**
     * Direct variant B: ByteBuffer LE view over the Arrow data buffer, absolute
     * put.
     */
    public int readBatchDirectB(int want, int dstBase, java.nio.ByteBuffer outData,
            MemorySegment outValidity) {
        if (pageData == null || pageRow >= pageValueCount) {
            return 0;
        }
        int m = Math.min(want, pageValueCount - pageRow);
        int presentCount;
        int[] present;
        if (maxDefLevel == 0) {
            presentCount = m;
            present = null;
        } else {
            present = idScratchPresent(m);
            presentCount = buildValidityDirect(dstBase, m, present, outValidity);
            if (presentCount == m) {
                present = null;
            }
        }
        if (pageEncoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            idReader.readInts(ids, 0, presentCount);
            gatherFixedDirectB(ids, present, dstBase, presentCount, outData);
        } else if (decodedPage()) {
            deltaFixedDirectB(present, dstBase, presentCount, outData);
        } else {
            plainFixedDirectB(present, dstBase, presentCount, outData);
        }
        pageRow += m;
        rowsDone += m;
        return m;
    }

    /**
     * Build validity for {@code m} rows at {@code dstBase} and record present
     * slots, writing each completed 64-bit validity word STRAIGHT into the
     * Arrow validity segment (no staging). Returns the present count.
     */
    private int buildValidityDirect(int dstBase, int m, int[] present,
            MemorySegment outValidity) {
        int[] levels = pageLevels;
        int max = maxDefLevel;
        int lbase = pageRow;
        int pc = 0;
        int i = 0;
        while (i < m) {
            int row = dstBase + i;
            int w = row >>> 6;
            int bit = row & 63;
            int take = Math.min(64 - bit, m - i);
            long mask = 0L;
            for (int j = 0; j < take; j++) {
                int p = levels[lbase + i + j] == max ? 1 : 0;
                mask |= (long) p << (bit + j);
                present[pc] = i + j;
                pc += p;
            }
            long window = take == 64 ? -1L : (((1L << take) - 1) << bit);
            long cur = (bit == 0 && take == 64)
                    ? 0L
                    : outValidity.get(LE_LONG, (long) w << 3);
            outValidity.set(LE_LONG, (long) w << 3, (cur & ~window) | mask);
            i += take;
        }
        batchNulls += m - pc;
        return pc;
    }

    private void gatherFixedDirectA(int[] ids, int[] present, int dstBase,
            int m, MemorySegment out) {
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        switch (physicalType) {
            case INT32 -> {
                int[] d = dictInts;
                if (widen) {
                    for (int k = 0; k < m; k++) {
                        out.set(LE_LONG, (long) (dstBase + (present == null ? k : present[k])) << 3, d[ids[k]]);
                    }
                } else {
                    for (int k = 0; k < m; k++) {
                        out.set(LE_INT, (long) (dstBase + (present == null ? k : present[k])) << 2, d[ids[k]]);
                    }
                }
            }
            case INT64 -> {
                long[] d = dictLongs;
                for (int k = 0; k < m; k++) {
                    out.set(LE_LONG, (long) (dstBase + (present == null ? k : present[k])) << 3, d[ids[k]]);
                }
            }
            case FLOAT64 -> {
                double[] d = dictDoubles;
                var le = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
                for (int k = 0; k < m; k++) {
                    out.set(le, (long) (dstBase + (present == null ? k : present[k])) << 3, d[ids[k]]);
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
    }

    private void plainFixedDirectA(int[] present, int dstBase, int m,
            MemorySegment out) {
        byte[] src = pageData;
        int s = plainCursor;
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        if (present == null && !widen) {
            int width = byteWidth();
            MemorySegment.copy(MemorySegment.ofArray(src), s, out, (long) dstBase * width,
                    (long) m * width);
            plainCursor = s + m * width;
            return;
        }
        switch (physicalType) {
            case INT32 -> {
                if (widen) {
                    for (int k = 0; k < m; k++) {
                        out.set(LE_LONG, (long) (dstBase + (present == null ? k : present[k])) << 3,
                                leInt(src, s));
                        s += 4;
                    }
                } else {
                    for (int k = 0; k < m; k++) {
                        out.set(LE_INT, (long) (dstBase + present[k]) << 2, leInt(src, s));
                        s += 4;
                    }
                }
            }
            case INT64 -> {
                for (int k = 0; k < m; k++) {
                    out.set(LE_LONG, (long) (dstBase + present[k]) << 3, leLong(src, s));
                    s += 8;
                }
            }
            case FLOAT64 -> {
                var le = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
                for (int k = 0; k < m; k++) {
                    out.set(le, (long) (dstBase + present[k]) << 3, Double.longBitsToDouble(leLong(src, s)));
                    s += 8;
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
        plainCursor = s;
    }

    private void gatherFixedDirectB(int[] ids, int[] present, int dstBase,
            int m, java.nio.ByteBuffer out) {
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        out.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        switch (physicalType) {
            case INT32 -> {
                int[] d = dictInts;
                if (widen) {
                    java.nio.LongBuffer lb = out.asLongBuffer();
                    for (int k = 0; k < m; k++) {
                        lb.put(dstBase + (present == null ? k : present[k]), d[ids[k]]);
                    }
                } else {
                    java.nio.IntBuffer ib = out.asIntBuffer();
                    for (int k = 0; k < m; k++) {
                        ib.put(dstBase + (present == null ? k : present[k]), d[ids[k]]);
                    }
                }
            }
            case INT64 -> {
                java.nio.LongBuffer lb = out.asLongBuffer();
                long[] d = dictLongs;
                for (int k = 0; k < m; k++) {
                    lb.put(dstBase + (present == null ? k : present[k]), d[ids[k]]);
                }
            }
            case FLOAT64 -> {
                java.nio.DoubleBuffer db = out.asDoubleBuffer();
                double[] d = dictDoubles;
                for (int k = 0; k < m; k++) {
                    db.put(dstBase + (present == null ? k : present[k]), d[ids[k]]);
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
    }

    private void plainFixedDirectB(int[] present, int dstBase, int m,
            java.nio.ByteBuffer out) {
        byte[] src = pageData;
        int s = plainCursor;
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        var srcView = java.nio.ByteBuffer.wrap(src).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        out.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        switch (physicalType) {
            case INT32 -> {
                if (widen) {
                    java.nio.LongBuffer lb = out.asLongBuffer();
                    for (int k = 0; k < m; k++) {
                        lb.put(dstBase + (present == null ? k : present[k]), srcView.getInt(s));
                        s += 4;
                    }
                } else {
                    java.nio.IntBuffer ib = out.asIntBuffer();
                    for (int k = 0; k < m; k++) {
                        ib.put(dstBase + (present == null ? k : present[k]), srcView.getInt(s));
                        s += 4;
                    }
                }
            }
            case INT64 -> {
                java.nio.LongBuffer lb = out.asLongBuffer();
                for (int k = 0; k < m; k++) {
                    lb.put(dstBase + (present == null ? k : present[k]), srcView.getLong(s));
                    s += 8;
                }
            }
            case FLOAT64 -> {
                java.nio.DoubleBuffer db = out.asDoubleBuffer();
                for (int k = 0; k < m; k++) {
                    db.put(dstBase + (present == null ? k : present[k]), srcView.getDouble(s));
                    s += 8;
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
        plainCursor = s;
    }

    /**
     * Builds validity for the {@code m} rows at batch position {@code dstBase}
     * from the page levels at {@code pageRow}, records present slots (relative to
     * the page slice) into {@code present}, counts the batch's nulls, and returns
     * the present count. One pass, branch-free within a 64-row word.
     */
    private int buildValidityAndPresent(int dstBase, int m, int[] present) {
        long[] words = validityWords;
        int[] levels = pageLevels;
        int max = maxDefLevel;
        int lbase = pageRow;
        int pc = 0;
        int i = 0;
        while (i < m) {
            int row = dstBase + i;
            int w = row >>> 6;
            int bit = row & 63;
            int take = Math.min(64 - bit, m - i);
            long mask = 0L;
            for (int j = 0; j < take; j++) {
                int present0 = levels[lbase + i + j] == max ? 1 : 0;
                mask |= (long) present0 << (bit + j);
                present[pc] = i + j;
                pc += present0;
            }
            long window = take == 64 ? -1L : (((1L << take) - 1) << bit);
            words[w] = (words[w] & ~window) | mask;
            i += take;
        }
        batchNulls += m - pc;
        return pc;
    }

    // ------------------------------------------------------------------ fixed width

    private void decodeFixed(int dstBase, int[] present, int presentCount) {
        if (pageEncoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            idReader.readInts(ids, 0, presentCount); // resumes mid-run across batches
            gatherFixedFromDict(ids, present, dstBase, presentCount);
        } else if (decodedPage()) {
            decodeDeltaFixed(present, dstBase, presentCount);
        } else {
            decodePlainFixed(present, dstBase, presentCount);
        }
    }

    // ------------------------------------------------------------------ DELTA_BINARY_PACKED / BYTE_STREAM_SPLIT (#559)

    /**
     * Decodes a DELTA_BYTE_ARRAY page's two length runs and checks them; the
     * values themselves are rebuilt later, batch by batch, straight into the
     * batch's bytes ({@link #decodeBinary}). The page is a DELTA_BINARY_PACKED
     * run of prefix lengths (how many leading bytes each value shares with the
     * previous one, 0 for the first), then a DELTA_LENGTH_BYTE_ARRAY of the
     * suffixes: a run of their lengths and their bytes. The prefix chain restarts
     * on every page. Returns the offset of the first suffix byte.
     */
    private int prepareDeltaByteArray(byte[] data, int start, int end) {
        DeltaBinaryPackedReader prefixes = new DeltaBinaryPackedReader(data, start, end - start, false,
                this::unpackerFor);
        int count = prefixes.remaining();
        if (prefixLengths.length < count) {
            prefixLengths = new int[Math.max(count, 2 * prefixLengths.length)];
        }
        prefixes.readInts(prefixLengths, 0, count);
        int suffixStart = prefixes.position();
        DeltaBinaryPackedReader suffixes = new DeltaBinaryPackedReader(data, suffixStart, end - suffixStart, false,
                this::unpackerFor);
        if (suffixes.remaining() != count) {
            throw new IllegalStateException("DELTA_BYTE_ARRAY: " + count + " prefixes but " + suffixes.remaining() + " suffixes");
        }
        if (suffixLengths.length < count) {
            suffixLengths = new int[Math.max(count, 2 * suffixLengths.length)];
        }
        suffixes.readInts(suffixLengths, 0, count);
        int bytesStart = suffixes.position();
        long suffixTotal = 0;
        int prevLen = 0;
        for (int i = 0; i < count; i++) {
            int pre = prefixLengths[i];
            int suf = suffixLengths[i];
            if (pre < 0 || suf < 0 || pre > prevLen) {
                throw new IllegalStateException("DELTA_BYTE_ARRAY: value "
                        + i
                        + " has prefix "
                        + pre
                        + " and suffix "
                        + suf
                        + " after a "
                        + prevLen
                        + "-byte value");
            }
            prevLen = pre + suf;
            suffixTotal += suf;
        }
        if (bytesStart + suffixTotal > end) {
            throw new IllegalStateException("DELTA_BYTE_ARRAY: suffix bytes past the end of the page");
        }
        return bytesStart;
    }

    /**
     * True for a fixed-width page whose values are decoded into scratch first
     * (not PLAIN, not dictionary).
     */
    private boolean decodedPage() {
        return pageEncoding == ParquetPageDecoder.Encoding.DELTA_BINARY_PACKED || pageEncoding == ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT;
    }

    /**
     * Reads the next {@code m} present values of the current DELTA_BINARY_PACKED
     * or BYTE_STREAM_SPLIT page into the scratch array of the OUTPUT lane: {@link
     * #deltaInts} for an INT32 lane, {@link #deltaLongs} for INT64 (including
     * INT32 widened to INT64) and for FLOAT64 (as raw IEEE bits).
     */
    private void readDelta(int m) {
        if (type == VecType.INT32) {
            if (deltaInts.length < m) {
                deltaInts = new int[Math.max(m, 2 * deltaInts.length)];
            }
        } else if (deltaLongs.length < m) {
            deltaLongs = new long[Math.max(m, 2 * deltaLongs.length)];
        }
        if (pageEncoding == ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT) {
            readByteStreamSplit(m);
        } else if (type == VecType.INT32) {
            deltaReader.readInts(deltaInts, 0, m);
        } else {
            deltaReader.readLongs(deltaLongs, 0, m);
        }
    }

    /**
     * Gathers the next {@code m} values of a BYTE_STREAM_SPLIT page: value {@code
     * i}'s byte {@code j} is at {@code bssStart + j * stride + i}. Each stream is
     * read sequentially, one byte per value, so the loop is a transpose of K
     * contiguous rows.
     */
    private void readByteStreamSplit(int m) {
        if (bssIndex + m > bssStride) {
            throw new IllegalStateException("BYTE_STREAM_SPLIT: " + m + " values requested, " + (bssStride - bssIndex) + " left");
        }
        byte[] src = pageData;
        int stride = bssStride;
        if (physicalType == VecType.INT32) {
            if (type == VecType.INT32) {
                ByteStreamSplitKernels.ints(src, bssStart, stride, bssIndex, deltaInts, m);
            } else { // INT32 widened to an INT64 lane: sign-extended
                int[] tmp = id(m);
                ByteStreamSplitKernels.ints(src, bssStart, stride, bssIndex, tmp, m);
                long[] dst = deltaLongs;
                for (int k = 0; k < m; k++) {
                    dst[k] = tmp[k];
                }
            }
        } else { // INT64 / FLOAT64: eight streams
            ByteStreamSplitKernels.longs(src, bssStart, stride, bssIndex, deltaLongs, m);
        }
        bssIndex += m;
    }

    /**
     * Staging path: scatter the next {@code presentCount} delta-decoded values
     * into the batch arrays.
     */
    private void decodeDeltaFixed(int[] present, int dstBase, int presentCount) {
        readDelta(presentCount);
        if (type == VecType.FLOAT64) {
            long[] v = deltaLongs;
            for (int k = 0; k < presentCount; k++) {
                doubles[dstBase + (present == null ? k : present[k])] = Double.longBitsToDouble(v[k]);
            }
        } else if (type == VecType.INT64) {
            long[] v = deltaLongs;
            if (present == null) {
                System.arraycopy(v, 0, longs, dstBase, presentCount);
            } else {
                for (int k = 0; k < presentCount; k++) {
                    longs[dstBase + present[k]] = v[k];
                }
            }
        } else {
            int[] v = deltaInts;
            if (present == null) {
                System.arraycopy(v, 0, ints, dstBase, presentCount);
            } else {
                for (int k = 0; k < presentCount; k++) {
                    ints[dstBase + present[k]] = v[k];
                }
            }
        }
    }

    /**
     * Direct path: the next {@code m} delta-decoded values straight into the
     * Arrow data segment, one bulk copy when every row is present.
     */
    private void deltaFixedDirectA(int[] present, int dstBase, int m,
            MemorySegment out) {
        readDelta(m);
        if (type == VecType.INT64 || type == VecType.FLOAT64) { // FLOAT64: the raw IEEE bits
            long[] v = deltaLongs;
            if (present == null) {
                MemorySegment.copy(v, 0, out, LE_LONG, (long) dstBase << 3,
                        m);
            } else {
                for (int k = 0; k < m; k++) {
                    out.set(LE_LONG, (long) (dstBase + present[k]) << 3, v[k]);
                }
            }
        } else {
            int[] v = deltaInts;
            if (present == null) {
                MemorySegment.copy(v, 0, out, LE_INT, (long) dstBase << 2,
                        m);
            } else {
                for (int k = 0; k < m; k++) {
                    out.set(LE_INT, (long) (dstBase + present[k]) << 2, v[k]);
                }
            }
        }
    }

    /** Direct variant B for a DELTA_BINARY_PACKED page (benchmark path). */
    private void deltaFixedDirectB(int[] present, int dstBase, int m,
            java.nio.ByteBuffer out) {
        readDelta(m);
        out.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        if (type == VecType.INT64 || type == VecType.FLOAT64) {
            java.nio.LongBuffer lb = out.asLongBuffer();
            for (int k = 0; k < m; k++) {
                lb.put(dstBase + (present == null ? k : present[k]), deltaLongs[k]);
            }
        } else {
            java.nio.IntBuffer ib = out.asIntBuffer();
            for (int k = 0; k < m; k++) {
                ib.put(dstBase + (present == null ? k : present[k]), deltaInts[k]);
            }
        }
    }

    private void decodePlainFixed(int[] present, int dstBase, int presentCount) {
        byte[] src = pageData;
        int s = plainCursor;
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        if (present == null && !widen) {
            switch (physicalType) {
                case INT32 -> java.nio.ByteBuffer.wrap(src, s, presentCount * 4)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer()
                        .get(ints, dstBase, presentCount);
                case INT64 -> java.nio.ByteBuffer.wrap(src, s, presentCount * 8)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .asLongBuffer()
                        .get(longs, dstBase, presentCount);
                case FLOAT64 -> java.nio.ByteBuffer.wrap(src, s, presentCount * 8)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .asDoubleBuffer()
                        .get(doubles, dstBase, presentCount);
                default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
            }
            plainCursor = s + presentCount * byteWidth();
            return;
        }
        switch (physicalType) {
            case INT32 -> {
                if (widen) {
                    for (int k = 0; k < presentCount; k++) {
                        longs[dstBase + (present == null ? k : present[k])] = leInt(src, s);
                        s += 4;
                    }
                } else {
                    for (int k = 0; k < presentCount; k++) {
                        ints[dstBase + present[k]] = leInt(src, s);
                        s += 4;
                    }
                }
            }
            case INT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    longs[dstBase + present[k]] = leLong(src, s);
                    s += 8;
                }
            }
            case FLOAT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    doubles[dstBase + present[k]] = Double.longBitsToDouble(leLong(src, s));
                    s += 8;
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
        plainCursor = s;
    }

    private int byteWidth() {
        return switch (physicalType) {
            case INT32 -> 4;
            case INT64, FLOAT64 -> 8;
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        };
    }

    private void gatherFixedFromDict(int[] ids, int[] present, int dstBase,
            int presentCount) {
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        switch (physicalType) {
            case INT32 -> {
                if (widen) {
                    for (int k = 0; k < presentCount; k++) {
                        longs[dstBase + (present == null ? k : present[k])] = dictInts[ids[k]];
                    }
                } else {
                    for (int k = 0; k < presentCount; k++) {
                        ints[dstBase + (present == null ? k : present[k])] = dictInts[ids[k]];
                    }
                }
            }
            case INT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    longs[dstBase + (present == null ? k : present[k])] = dictLongs[ids[k]];
                }
            }
            case FLOAT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    doubles[dstBase + (present == null ? k : present[k])] = dictDoubles[ids[k]];
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
    }

    // ------------------------------------------------------------------ BOOLEAN (#559)

    /**
     * A BOOLEAN page: {@code PLAIN} values are bit-packed one bit per present
     * value, LSB first, from the first value byte; {@code RLE} values are a
     * 4-byte little-endian length, then an RLE/bit-packed hybrid stream at bit
     * width 1 (what parquet-java writes for v2 pages). Dictionary encoding does
     * not apply to booleans.
     */
    private void feedBoolPage(Page page, long valuesStart) {
        this.idReader = null;
        this.plainCursor = 0;
        this.utf8PlainCursor = 0;
        this.boolPageEnd = (int) page.dataEnd();
        if (page.encoding == ParquetPageDecoder.Encoding.PLAIN) {
            this.boolBitCursor = valuesStart << 3;
        } else if (page.encoding == ParquetPageDecoder.Encoding.RLE) {
            int lenAt = (int) valuesStart;
            if (lenAt + 4 > boolPageEnd) {
                throw new IllegalStateException("BOOLEAN RLE: page too short for its length prefix");
            }
            int len = readLeInt(page.data, lenAt);
            if (len < 0 || lenAt + 4 + len > boolPageEnd) {
                throw new IllegalStateException("BOOLEAN RLE: stream length " + len + " past the end of the page");
            }
            this.boolReader = reader(page.data, lenAt + 4, len, 1);
        } else {
            throw new IllegalArgumentException(page.encoding + " on a BOOL lane");
        }
    }

    /**
     * Decodes the next {@code presentCount} values into {@link #boolWords} at
     * batch rows {@code dstBase + (present == null ? k : present[k])}. PLAIN with
     * every row present moves up to 64 bits per step from any source bit offset
     * to any destination bit offset; otherwise the bits are scattered one per
     * present row.
     */
    private void decodeBool(int dstBase, int[] present, int presentCount) {
        long[] words = boolWords;
        if (boolReader != null) {
            if (present == null) {
                // Straight into the bitmap: RLE runs as bit ranges, bit-packed runs 64 bits a step.
                boolReader.readBits(words, dstBase, presentCount);
                return;
            }
            int[] v = id(presentCount);
            boolReader.readInts(v, 0, presentCount);
            for (int k = 0; k < presentCount; k++) {
                int row = dstBase + present[k];
                words[row >>> 6] |= (long) (v[k] & 1) << (row & 63);
            }
            return;
        }
        long src = boolBitCursor;
        long srcEndBit = (long) boolPageEnd << 3;
        if (src + presentCount > srcEndBit) {
            throw new IllegalStateException("BOOLEAN PLAIN: " + presentCount + " values past the end of the page");
        }
        byte[] page = pageData;
        if (present == null) {
            int k = 0;
            while (k < presentCount) {
                int take = Math.min(64, presentCount - k);
                long v = loadBits(page, src + k, take, boolPageEnd);
                int row = dstBase + k;
                int bit = row & 63;
                words[row >>> 6] |= v << bit;
                if (bit != 0 && bit + take > 64) {
                    words[(row >>> 6) + 1] |= v >>> (64 - bit);
                }
                k += take;
            }
        } else {
            for (int k = 0; k < presentCount; k++) {
                long b = src + k;
                int v = (page[(int) (b >>> 3)] >>> (int) (b & 7)) & 1;
                int row = dstBase + present[k];
                words[row >>> 6] |= (long) v << (row & 63);
            }
        }
        boolBitCursor = src + presentCount;
    }

    /**
     * {@code n} (1..64) bits of {@code a} from bit {@code bitOff}, LSB first,
     * reading no byte at or past {@code end}.
     */
    private static long loadBits(byte[] a, long bitOff, int n,
            int end) {
        int b = (int) (bitOff >>> 3);
        int shift = (int) (bitOff & 7);
        int bytes = Math.min((shift + n + 7) >>> 3, end - b); // at most 9
        long lo = 0L;
        int lim = Math.min(bytes, 8);
        if (lim == 8) {
            lo = (long) BA_LONG.get(a, b);
        } else {
            for (int i = 0; i < lim; i++) {
                lo |= ((long) (a[b + i] & 0xFF)) << (8 * i);
            }
        }
        long v = lo >>> shift;
        if (bytes == 9) {
            v |= ((long) (a[b + 8] & 0xFF)) << (64 - shift);
        }
        return n == 64 ? v : v & ((1L << n) - 1);
    }

    /**
     * Copy the finished BOOL batch of {@code rows}: the value bits, then
     * validity.
     */
    public void flushBool(int rows, MemorySegment outData, MemorySegment outValidity) {
        int words = (rows + 63) >>> 6;
        if (rows % 64 != 0) {
            boolWords[words - 1] &= (1L << (rows & 63)) - 1; // no stray bits past the last row
        }
        MemorySegment.copy(boolWords, 0, outData, LE_LONG, 0L, words);
        flushValidity(rows, outValidity);
    }

    // ------------------------------------------------------------------ binary / utf8

    private void decodeBinary(int dstBase, int m, int[] present,
            int presentCount) {
        if (decodedPage()) {
            throw new IllegalArgumentException(pageEncoding + " on a UTF8 lane");
        }
        int pos = utf8Len;
        if (pageEncoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            idReader.readInts(ids, 0, presentCount);
            int k = 0;
            for (int i = 0; i < m; i++) {
                utf8Offsets[dstBase + i] = pos;
                if (present == null || (k < presentCount && present[k] == i)) {
                    int id = ids[k++];
                    int start = dictOffsets[id];
                    int len = dictOffsets[id + 1] - start;
                    pos = appendBytes(dictBytes, start, len, pos);
                }
            }
            utf8Offsets[dstBase + m] = pos;
            utf8Len = pos;
            return;
        }
        byte[] src = pageData;
        int s = utf8PlainCursor;
        int k = 0;
        if (pageEncoding == ParquetPageDecoder.Encoding.DELTA_BYTE_ARRAY) {
            decodeDeltaByteArray(dstBase, m, present, presentCount);
            return;
        }
        if (pageEncoding == ParquetPageDecoder.Encoding.DELTA_LENGTH_BYTE_ARRAY) {
            // The lengths were decoded at feedPage; the bytes are back to back from utf8PlainCursor.
            src = binarySrc;
            int[] lens = pageLengths;
            int lc = lengthCursor;
            for (int i = 0; i < m; i++) {
                utf8Offsets[dstBase + i] = pos;
                if (present == null || (k < presentCount && present[k] == i)) {
                    int len = lens[lc++];
                    pos = appendBytes(src, s, len, pos);
                    s += len;
                    k++;
                }
            }
            utf8Offsets[dstBase + m] = pos;
            utf8Len = pos;
            utf8PlainCursor = s;
            lengthCursor = lc;
            return;
        }
        int end = (int) pageEndExclusive;
        for (int i = 0; i < m; i++) {
            utf8Offsets[dstBase + i] = pos;
            if (present == null || (k < presentCount && present[k] == i)) {
                int len;
                if (fixedLength > 0) {
                    len = fixedLength; // FIXED_LEN_BYTE_ARRAY: no length prefix
                } else {
                    if (s + 4 > end) {
                        throw new IllegalStateException("PLAIN binary: length prefix past the end of the page");
                    }
                    len = readLeInt(src, s);
                    s += 4;
                }
                if (len < 0 || len > end - s) {
                    throw new IllegalStateException("PLAIN binary: a " + len + "-byte value past the end of the page");
                }
                pos = appendBytes(src, s, len, pos);
                s += len;
                k++;
            }
        }
        utf8Offsets[dstBase + m] = pos;
        utf8Len = pos;
        utf8PlainCursor = s;
    }

    /**
     * DELTA_BYTE_ARRAY: rebuilds each present value straight into the batch's
     * bytes -- its prefix copied from the previous value (in this batch's bytes,
     * or saved from the previous call), then its suffix from the page -- so
     * every value byte is written once. At the end the last value is saved, since
     * the next call may start a new batch and reuse the bytes.
     */
    private void decodeDeltaByteArray(int dstBase, int m, int[] present,
            int presentCount) {
        byte[] src = pageData;
        int s = utf8PlainCursor;
        int lc = lengthCursor;
        int pos = utf8Len;
        int k = 0;
        int prevStart = dbaPrevStart;
        int prevLen = dbaPrevLen;
        for (int i = 0; i < m; i++) {
            utf8Offsets[dstBase + i] = pos;
            if (present == null || (k < presentCount && present[k] == i)) {
                int pre = prefixLengths[lc];
                int suf = suffixLengths[lc];
                lc++;
                int len = pre + suf;
                if (pos + len > utf8Data.length) {
                    utf8Data = java.util.Arrays.copyOf(utf8Data,
                            Math.max(pos + len, utf8Data.length * 2));
                }
                if (pre > 0) { // pre <= prevLen, checked at feedPage
                    if (prevStart >= 0) {
                        System.arraycopy(utf8Data, prevStart, utf8Data, pos, pre); // the previous value ends at or before pos
                    } else {
                        System.arraycopy(dbaPrev, 0, utf8Data, pos, pre);
                    }
                }
                System.arraycopy(src, s, utf8Data, pos + pre, suf);
                s += suf;
                prevStart = pos;
                prevLen = len;
                pos += len;
                k++;
            }
        }
        utf8Offsets[dstBase + m] = pos;
        utf8Len = pos;
        utf8PlainCursor = s;
        lengthCursor = lc;
        if (prevStart >= 0) { // keep the last value for a following call, whose batch may reuse utf8Data
            if (dbaPrev.length < prevLen) {
                dbaPrev = new byte[Math.max(prevLen, 2 * dbaPrev.length)];
            }
            System.arraycopy(utf8Data, prevStart, dbaPrev, 0, prevLen);
        }
        dbaPrevStart = -1;
        dbaPrevLen = prevLen;
    }

    private int appendBytes(byte[] src, int srcPos, int len,
                            int pos) {
        if (pos + len > utf8Data.length) {
            utf8Data = java.util.Arrays.copyOf(utf8Data,
                    Math.max(pos + len, utf8Data.length * 2));
        }
        System.arraycopy(src, srcPos, utf8Data, pos, len);
        return pos + len;
    }

    public long utf8Bytes() {
        return utf8Len;
    }

    /**
     * Stage a FIXED_LEN_BYTE_ARRAY column of {@code width}-byte values on the
     * UTF8 lane: its {@code PLAIN} values carry no length prefix. 0 restores the
     * length-prefixed BINARY layout.
     */
    public void setFixedLength(int width) {
        if (width < 0) {
            throw new IllegalArgumentException("fixed length " + width);
        }
        this.fixedLength = width;
    }

    /**
     * Converts the finished batch of {@code rows} decimals, staged as big-endian
     * two's-complement unscaled values (Parquet's FIXED_LEN_BYTE_ARRAY / BINARY
     * decimal layout), into a fixed-width lane, then copies validity.
     * <ul>
     * <li>{@code wide == false}: the INT64 lane of a {@code decimal(p <= 18)},
     * one little-endian long per row, computed exactly as Spark's {@code
     * ParquetRowConverter.binaryToUnscaledLong}: the bytes shifted into a long,
     * then sign-extended from {@code 8 * len} bits (shift counts mod 64, as the
     * JVM does).</li>
     * <li>{@code wide == true}: the DECIMAL128 lane, two little-endian limbs per
     * row (low, then high), sign-extended from the value's top bit, Arrow's
     * Decimal128 layout. A value longer than 16 bytes must be a sign extension of
     * its low 16 bytes (it then fits 128 bits), else the batch fails: no
     * DECIMAL128 holds it.</li>
     * </ul>
     * A null row (zero bytes) writes 0.
     */
    public void flushDecimal(int rows, MemorySegment outData, MemorySegment outValidity,
            boolean wide) {
        byte[] b = utf8Data;
        int[] off = utf8Offsets;
        for (int i = 0; i < rows; i++) {
            int s = off[i];
            int e = off[i + 1];
            int len = e - s;
            if (!wide) {
                long u = 0L;
                for (int j = s; j < e; j++) {
                    u = (u << 8) | (b[j] & 0xFF);
                }
                int bits = 8 * len;
                long v = len == 0 ? 0L : (u << (64 - bits)) >> (64 - bits);
                outData.set(LE_LONG, (long) i << 3, v);
                continue;
            }
            long lo = 0L;
            long hi = 0L;
            if (len > 0) {
                long sign = b[s] < 0 ? -1L : 0L;
                if (len > 16) {
                    // The bytes above the low 16 must all be the sign, and agree with the low 16's top bit.
                    byte sb = (byte) sign;
                    for (int j = s; j < e - 16; j++) {
                        if (b[j] != sb) {
                            throw new IllegalStateException("a " + len + "-byte decimal does not fit 128 bits");
                        }
                    }
                    if ((b[e - 16] < 0) != (sign < 0)) {
                        throw new IllegalStateException("a " + len + "-byte decimal does not fit 128 bits");
                    }
                    s = e - 16;
                    len = 16;
                }
                hi = sign;
                lo = sign;
                for (int j = s; j < e; j++) {
                    // Shift the 128-bit (hi, lo) left by 8 and bring in the next byte.
                    hi = (hi << 8) | (lo >>> 56);
                    lo = (lo << 8) | (b[j] & 0xFF);
                }
            }
            long at = (long) i << 4;
            outData.set(LE_LONG, at, lo);
            outData.set(LE_LONG, at + 8, hi);
        }
        flushValidity(rows, outValidity);
    }

    // ------------------------------------------------------------------ flush the batch (one bulk copy each)

    /**
     * Copy the finished fixed-width batch of {@code rows} into the Arrow data +
     * validity buffers.
     */
    public void flushFixed(int rows, MemorySegment outData, MemorySegment outValidity) {
        switch (type) {
            case INT32 -> MemorySegment.copy(ints, 0, outData, LE_INT, 0L, rows);
            case INT64 -> MemorySegment.copy(longs, 0, outData, LE_LONG, 0L, rows);
            case FLOAT64 -> MemorySegment.copy(doubles, 0, outData, ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN), 0L,
                    rows);
            default -> throw new IllegalArgumentException("not a fixed lane: " + type);
        }
        flushValidity(rows, outValidity);
    }

    /**
     * Copy the finished UTF8 batch of {@code rows}: offsets, data bytes, and
     * validity.
     */
    public void flushUtf8(int rows, MemorySegment outOffsets, MemorySegment outData,
                          MemorySegment outValidity) {
        MemorySegment.copy(utf8Offsets, 0, outOffsets, LE_INT, 0L,
                rows + 1);
        if (utf8Len > 0) {
            MemorySegment.copy(utf8Data, 0, outData, ValueLayout.JAVA_BYTE, 0L, utf8Len);
        }
        flushValidity(rows, outValidity);
    }

    private void flushValidity(int rows, MemorySegment outValidity) {
        if (outValidity == null) {
            return; // no nulls in this batch: no validity buffer
        }
        int words = (rows + 63) >>> 6;
        MemorySegment.copy(validityWords, 0, outValidity, LE_LONG, 0L, words);
    }

    // ------------------------------------------------------------------ shared

    private RleBitPackingReader dictionaryIds(Page page, long valuesStart) {
        int idBitWidth = page.data[(int) valuesStart] & 0xFF;
        int streamStart = (int) valuesStart + 1;
        int streamLen = (int) (page.dataEnd() - streamStart);
        return reader(page.data, streamStart, streamLen, idBitWidth);
    }

    private GroupUnpacker unpackerFor(int bitWidth) {
        if (unpackerFactory == null || bitWidth == 0) {
            return null;
        }
        if (!unpackerSet[bitWidth]) {
            unpackers[bitWidth] = unpackerFactory.apply(bitWidth);
            unpackerSet[bitWidth] = true;
        }
        return unpackers[bitWidth];
    }

    private RleBitPackingReader reader(byte[] page, int offset, int length,
            int bitWidth) {
        return RleBitPackingReader.overArray(page, offset, length, bitWidth, unpackerFor(bitWidth));
    }

    private int[] id(int n) {
        if (idScratch.length < n) {
            idScratch = new int[Math.max(n, 2 * idScratch.length)];
        }
        return idScratch;
    }

    private int[] idScratchPresent(int n) {
        if (levelScratch.length < n) {
            levelScratch = new int[Math.max(n, 2 * levelScratch.length)];
        }
        return levelScratch;
    }

    private static int[] grow(int[] a, int n) {
        return a == null || a.length < n
                ? new int[n]
                : a;
    }

    private static long[] grow(long[] a, int n) {
        return a == null || a.length < n
                ? new long[n]
                : a;
    }

    private static double[] grow(double[] a, int n) {
        return a == null || a.length < n
                ? new double[n]
                : a;
    }

    private static int[] ensureInts(int[] a, int n) {
        return a == null || a.length < n
                ? new int[n]
                : a;
    }

    private static long[] ensureLongs(long[] a, int n) {
        return a == null || a.length < n
                ? new long[n]
                : a;
    }

    private static int readLeInt(byte[] a, long off) {
        int o = (int) off;
        return (a[o] & 0xFF)
                | ((a[o + 1] & 0xFF) << 8)
                | ((a[o + 2] & 0xFF) << 16)
                | ((a[o + 3] & 0xFF) << 24);
    }

    // ---- staging accessors, for tests ----
    int[] intArray() {
        return ints;
    }

    long[] longArray() {
        return longs;
    }

    long[] validityWordArray() {
        return validityWords;
    }

    int[] utf8OffsetArray() {
        return utf8Offsets;
    }

    byte[] utf8DataArray() {
        return utf8Data;
    }

    /**
     * One data page as parquet-java hands it over: the decompressed bytes as a
     * {@code byte[]} plus the small page-header metadata. v1 inlines the levels
     * (int32-prefixed) before the values at {@link #dataOffset}; v2 gives level
     * and value regions as separate slices laid out end to end.
     */
    public static final class Page {
        public final byte[] data;
        public final long dataOffset;
        public final long dataLength;
        public final long levelsOffset;
        public final int levelsLength;
        public final long valuesOffset;
        public final int valueCount;
        public final boolean v2;
        public final ParquetPageDecoder.Encoding encoding;

        private Page(
                byte[] data,
                long dataOffset,
                long dataLength,
                long levelsOffset,
                int levelsLength,
                long valuesOffset,
                int valueCount,
                boolean v2,
                ParquetPageDecoder.Encoding encoding) {
            this.data = data;
            this.dataOffset = dataOffset;
            this.dataLength = dataLength;
            this.levelsOffset = levelsOffset;
            this.levelsLength = levelsLength;
            this.valuesOffset = valuesOffset;
            this.valueCount = valueCount;
            this.v2 = v2;
            this.encoding = encoding;
        }

        /**
         * A v1 page in the first {@code length} bytes of {@code data} (a reused
         * buffer may be larger than the page; BYTE_STREAM_SPLIT derives its stream
         * stride from the value region's length, so it must be exact).
         */
        public static Page v1(byte[] data, int length, int valueCount,
                              ParquetPageDecoder.Encoding encoding) {
            return new Page(data, 0, length, 0, 0, 0,
                    valueCount, false, encoding);
        }

        public static Page v1(byte[] data, int valueCount, ParquetPageDecoder.Encoding encoding) {
            return new Page(data, 0, data.length, 0, 0, 0,
                    valueCount, false, encoding);
        }

        public static Page v2(byte[] data, int levelsLength, int valueCount,
                              ParquetPageDecoder.Encoding encoding) {
            return new Page(data, 0, data.length, 0, levelsLength, levelsLength,
                    valueCount, true, encoding);
        }

        public static Page v2At(
                byte[] data,
                long levelsOffset,
                int levelsLength,
                long valuesOffset,
                long valuesLength,
                int valueCount,
                ParquetPageDecoder.Encoding encoding) {
            return new Page(data, levelsOffset, levelsLength + valuesLength, levelsOffset, levelsLength,
                    valuesOffset, valueCount, true, encoding);
        }

        long valuesStart() {
            return v2 ? valuesOffset : dataOffset;
        }

        long dataEnd() {
            return v2 ? valuesOffset + (dataLength - levelsLength) : dataOffset + dataLength;
        }
    }
}
