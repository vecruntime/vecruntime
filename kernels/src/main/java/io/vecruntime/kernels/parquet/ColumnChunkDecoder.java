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
 * Streaming decoder for a flat (max repetition level 0) Parquet column chunk: it
 * decodes the NEXT {@code n} rows of the chunk straight into <em>batch-sized</em>
 * reused heap primitive arrays, resuming across page and run boundaries, so a
 * consumer emits fixed-size batches without materialising the whole row group and
 * without a per-batch scan of it. This is the decode core of #559 slice 1's scan
 * node (the streaming rewrite of the row-group-at-a-time first cut).
 *
 * <h2>Resumable state (a batch spans pages and runs)</h2>
 * A batch size need not divide a page's row count, so a batch starts and ends
 * anywhere inside a page. The decoder keeps, for the CURRENT page: its decoded
 * definition levels ({@link #pageLevels}, cheap {@code int[valueCount]}), a ROW
 * cursor into the page ({@link #pageRow}), and the position of the next
 * unconsumed value -- a byte offset for PLAIN ({@link #plainCursor}) or a live
 * {@link RleBitPackingReader} for {@code RLE_DICTIONARY} ids ({@link #idReader},
 * which resumes mid-run: its {@code readInts} picks up exactly where the previous
 * batch left off, including a bit-packed group split across batches). The
 * dictionary is decoded once per chunk. The consumer feeds pages on demand
 * ({@link #needsPage()} / {@link #feedPage}) and pulls rows ({@link #readBatch}).
 *
 * <h2>Write path: plain array stores, one bulk copy per batch</h2>
 * A first cut wrote every value with a checked FFM {@code MemorySegment.set}; a
 * JFR profile showed the executor in {@code checkValidStateRaw} / {@code
 * checkBounds} / {@code VarHandleGuards} per value, 1.7x slower than Spark's
 * on-heap reader. So the hot loop scatters present values into a reused {@code
 * int[]}/{@code long[]}/{@code double[]} sized to the BATCH with plain array
 * stores, builds validity a 64-bit word at a time, and gathers UTF8 bytes into
 * reused arrays; the consumer copies each finished batch into the batch-owned
 * Arrow buffers in one bulk {@link MemorySegment#copy} per buffer.
 *
 * <h2>Null count is free</h2>
 * The definition-level pass that builds validity also counts the batch's nulls
 * ({@link #batchNullCount()}); the consumer never calls Arrow's O(rows)
 * {@code getNullCount}. A batch with no nulls needs no validity buffer at all.
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

    // ---- reused per-batch scratch ----
    private int[] idScratch = new int[0]; // dictionary ids for a page-slice
    private int[] levelScratch = new int[0]; // page-level decode buffer

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
        if (page.encoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            this.idReader = dictionaryIds(page, valuesStart);
            this.plainCursor = 0;
            this.utf8PlainCursor = 0;
        } else {
            this.idReader = null;
            this.plainCursor = (int) valuesStart;
            this.utf8PlainCursor = (int) valuesStart;
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
        } else {
            decodePlainFixed(present, dstBase, presentCount);
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

    // ------------------------------------------------------------------ binary / utf8

    private void decodeBinary(int dstBase, int m, int[] present,
            int presentCount) {
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
        for (int i = 0; i < m; i++) {
            utf8Offsets[dstBase + i] = pos;
            if (present == null || (k < presentCount && present[k] == i)) {
                int len = readLeInt(src, s);
                s += 4;
                pos = appendBytes(src, s, len, pos);
                s += len;
                k++;
            }
        }
        utf8Offsets[dstBase + m] = pos;
        utf8Len = pos;
        utf8PlainCursor = s;
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
