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

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;

/**
 * Decodes a whole Parquet column chunk (its dictionary page and every data
 * page) of a flat (max repetition level 0) column into <em>reused heap
 * primitive arrays</em>, then hands the finished row group to Arrow with ONE
 * bulk {@link MemorySegment#copy} per buffer. This is the decode core of
 * #559 slice 1's scan node.
 *
 * <h2>Write path: plain array stores, one bulk copy (why the rewrite)</h2>
 * The first cut wrote every value with a checked FFM {@code MemorySegment.set}
 * and every null with a checked {@code Bitmap.clear}; a JFR profile of the
 * end-to-end benchmark showed the executor spending its time in
 * {@code checkEnclosingLayout} / {@code checkValidStateRaw} / {@code checkBounds}
 * / {@code VarHandleGuards} per value, and ours ran 1.7x slower than Spark's
 * reader (which writes on-heap primitive arrays with plain stores). So the hot
 * loop now scatters present values into a reused {@code int[]}/{@code long[]}/
 * {@code double[]} sized to the row group with plain array stores, builds the
 * validity a 64-bit word at a time into a reused {@code long[]}, and gathers
 * UTF8 bytes into reused {@code int[]} offsets + {@code byte[]} data. The
 * finished arrays copy into the Arrow buffers in one shot at {@link #flushFixed}
 * / {@link #flushUtf8}: N checked FFM stores become 1 bulk copy.
 *
 * <h2>Injected unpacker (requirement 1)</h2>
 * A {@link GroupUnpacker} factory {@code (int bitWidth) -> GroupUnpacker},
 * cached per bit width; every definition-level and every {@code RLE_DICTIONARY}
 * id stream is read through {@link RleBitPackingReader#overArray} with it. A
 * {@code null} factory falls back to the built-in scalar unpack (kernels' own
 * parquet-free tests), identical result.
 *
 * <h2>Reuse (requirement 3)</h2>
 * All staging arrays, the level/id/present scratch, the dictionary (decoded once
 * into heap arrays) and the unpackers live for the decoder lifetime and are
 * reused across pages; a node reusing the decoder across row groups calls
 * {@link #reset(int)}.
 *
 * <h2>Physical vs lane</h2>
 * A narrow decimal (precision at most 9) is {@code INT32} physical but its lane
 * is the {@code INT64} unscaled value: those values are read into the {@code
 * long[]} sign-extended. {@code DATE} is {@code INT32}. Otherwise physical ==
 * lane.
 */
public final class ColumnChunkDecoder {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    // Little-endian views over a plain byte[]. These intrinsify to unaligned loads with no FFM
    // session/bounds/alignment checks -- unlike MemorySegment.get, which a JFR profile showed spending
    // ~35% in checkEnclosingLayout / VarHandleGuards / isAlignedForElement reading the page. The page
    // bytes are a heap byte[] parquet-java hands us, so we never need a MemorySegment to read them.
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

    private int rowGroupRows;
    private int rowOffset; // rows decoded so far into the staging arrays

    // ---- staging arrays (only the ones for this column's lane are allocated) ----
    private int[] ints; // INT32 lane
    private long[] longs; // INT64 lane (also widened narrow decimal)
    private double[] doubles; // FLOAT64 lane
    private long[] validityWords; // one bit per row, 64 rows per word; null when maxDefLevel == 0
    private int[] utf8Offsets; // UTF8: rowGroupRows + 1 entries
    private byte[] utf8Data; // UTF8: the gathered bytes, grows as needed
    private int utf8Len; // bytes written into utf8Data

    // ---- reused per-page scratch ----
    private int[] levelScratch = new int[0];
    private int[] idScratch = new int[0];

    // ---- heap dictionary, decoded once ----
    private boolean hasDictionary;
    private int[] dictInts;
    private long[] dictLongs;
    private double[] dictDoubles;
    private byte[] dictBytes; // UTF8 dictionary data
    private int[] dictOffsets; // UTF8 dictionary offsets (numValues + 1)

    private final GroupUnpacker[] unpackers = new GroupUnpacker[33];
    private final boolean[] unpackerSet = new boolean[33];

    public ColumnChunkDecoder(VecType type, int maxDefLevel, int rowGroupRows,
            IntFunction<GroupUnpacker> unpackerFactory) {
        this(type, type, maxDefLevel, rowGroupRows, unpackerFactory);
    }

    public ColumnChunkDecoder(VecType physicalType, VecType type, int maxDefLevel,
            int rowGroupRows, IntFunction<GroupUnpacker> unpackerFactory) {
        this.physicalType = physicalType;
        this.type = type;
        this.maxDefLevel = maxDefLevel;
        this.defBitWidth = ParquetPageDecoder.bitWidth(maxDefLevel);
        this.unpackerFactory = unpackerFactory;
        allocate(rowGroupRows);
    }

    /**
     * Sizes the staging arrays for {@code rowGroupRows} rows, growing (never
     * shrinking) the reused ones.
     */
    private void allocate(int rowGroupRows) {
        this.rowGroupRows = rowGroupRows;
        this.rowOffset = 0;
        switch (type) {
            case INT32 -> {
                if (ints == null || ints.length < rowGroupRows) {
                    ints = new int[rowGroupRows];
                }
            }
            case INT64 -> {
                if (longs == null || longs.length < rowGroupRows) {
                    longs = new long[rowGroupRows];
                }
            }
            case FLOAT64 -> {
                if (doubles == null || doubles.length < rowGroupRows) {
                    doubles = new double[rowGroupRows];
                }
            }
            case UTF8 -> {
                if (utf8Offsets == null || utf8Offsets.length < rowGroupRows + 1) {
                    utf8Offsets = new int[rowGroupRows + 1];
                }
                if (utf8Data == null) {
                    utf8Data = new byte[Math.max(1 << 16, rowGroupRows)];
                }
                utf8Len = 0;
            }
            default -> throw new IllegalArgumentException("unsupported lane " + type);
        }
        if (maxDefLevel > 0) {
            int words = (rowGroupRows + 63) >>> 6;
            if (validityWords == null || validityWords.length < words) {
                validityWords = new long[words];
            }
            // Start all-valid; a page with nulls clears the bits it finds.
            java.util.Arrays.fill(validityWords, 0, words, -1L);
        }
    }

    /** Reuse this decoder for the next row group (arrays and dictionary kept). */
    public void reset(int rowGroupRows) {
        allocate(rowGroupRows);
    }

    // ------------------------------------------------------------------ dictionary (heap)

    /**
     * Sets the chunk's dictionary from an already-decoded {@link VectorBuffers}
     * (the caller decodes the PLAIN dictionary page once). The entries are
     * copied into heap arrays so the per-value gather is an array read, not a
     * checked segment read.
     */
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

    public int rowsWritten() {
        return rowOffset;
    }

    public long utf8Bytes() {
        return utf8Len;
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

    // ------------------------------------------------------------------ page decode

    /**
     * Decodes one data page into the staging arrays at the current row offset,
     * then advances the offset by {@code page.valueCount}.
     */
    public void decodePage(Page page) {
        int n = page.valueCount;
        int base = rowOffset;
        if (base + n > rowGroupRows) {
            throw new IllegalStateException("page overruns the row group: " + (base + n) + " > " + rowGroupRows);
        }
        int presentCount;
        int[] present; // slot (relative to base) of each present value; null => every value present
        long valuesStart;
        if (maxDefLevel == 0) {
            presentCount = n;
            present = null;
            valuesStart = page.valuesStart();
        } else {
            int[] levels = level(n);
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
            reader(page.data, (int) levelStart, levelLen, defBitWidth).readInts(levels, 0, n);
            present = present(n);
            presentCount = buildValidityAndPresent(base, n, levels, present);
            if (presentCount == n) {
                present = null; // no nulls in this page: the value region is contiguous, take the bulk path
            }
        }

        if (type == VecType.UTF8) {
            decodeBinaryPage(page, base, present, presentCount, valuesStart);
        } else {
            decodeFixedPage(page, base, present, presentCount, valuesStart);
        }
        rowOffset = base + n;
    }

    /**
     * Sets the validity bits of rows {@code [base, base + n)} from {@code
     * levels} a 64-bit word at a time: a null row clears its bit (the words
     * start all-ones). Branch-free within a word -- bit {@code j} is {@code
     * (levels == max) << j} -- so a page with no nulls writes each word back as
     * {@code -1} and a page of nulls writes {@code 0}. No per-row checked
     * segment write (the old {@code Bitmap.clear} hot spot).
     *
     * <p>One pass over the page's levels does BOTH: it packs each 64-row
     * validity word (bit {@code j} set iff row present) and records the present
     * rows' slots into {@code present}, returning the present count. The word
     * packing is branch-free -- {@code (levels==max) ? 1 : 0} shifted into
     * place -- and a common all-present window writes the word as {@code -1}
     * with no per-row work beyond the compare. Fusing the two removes the
     * second full scan of the levels the old code did (validity then present),
     * the #1 decode-only hot spot in the JFR profile.
     */
    private int buildValidityAndPresent(int base, int n, int[] levels,
            int[] present) {
        long[] words = validityWords;
        int max = maxDefLevel;
        int pc = 0;
        int i = 0;
        while (i < n) {
            int row = base + i;
            int w = row >>> 6;
            int bit = row & 63;
            int take = Math.min(64 - bit, n - i);
            long mask = 0L;
            for (int j = 0; j < take; j++) {
                int present0 = levels[i + j] == max ? 1 : 0; // branch compiles to a cmp/set, no jump
                mask |= (long) present0 << (bit + j);
                present[pc] = i + j; // written unconditionally; pc only advances when present
                pc += present0;
            }
            long window = take == 64 ? -1L : (((1L << take) - 1) << bit);
            words[w] = (words[w] & ~window) | mask;
            i += take;
        }
        return pc;
    }

    // ------------------------------------------------------------------ fixed width (array stores)

    private void decodeFixedPage(Page page, int base, int[] present,
            int presentCount, long valuesStart) {
        if (page.encoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            dictionaryIds(page, valuesStart).readInts(ids, 0, presentCount);
            gatherFixedFromDict(ids, present, base, presentCount);
        } else {
            decodePlainFixed(page, valuesStart, present, base, presentCount);
        }
    }

    private void decodePlainFixed(Page page, long valuesStart, int[] present,
            int base, int presentCount) {
        byte[] src = page.data;
        int s = (int) valuesStart;
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        // No-null contiguous run: a bulk little-endian primitive-view copy from the page bytes straight into
        // the staging array (widen still needs the per-value sign-extend). This is the byte[] analogue of the
        // one MemorySegment.copy the old code did, but into the heap staging array.
        if (present == null && !widen) {
            switch (physicalType) {
                case INT32 -> java.nio.ByteBuffer.wrap(src, s, presentCount * 4)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer()
                        .get(ints, base, presentCount);
                case INT64 -> java.nio.ByteBuffer.wrap(src, s, presentCount * 8)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .asLongBuffer()
                        .get(longs, base, presentCount);
                case FLOAT64 -> java.nio.ByteBuffer.wrap(src, s, presentCount * 8)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                        .asDoubleBuffer()
                        .get(doubles, base, presentCount);
                default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
            }
            return;
        }
        switch (physicalType) {
            case INT32 -> {
                if (widen) {
                    for (int k = 0; k < presentCount; k++) {
                        longs[base + (present == null ? k : present[k])] = leInt(src, s);
                        s += 4;
                    }
                } else {
                    for (int k = 0; k < presentCount; k++) {
                        ints[base + present[k]] = leInt(src, s);
                        s += 4;
                    }
                }
            }
            case INT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    longs[base + present[k]] = leLong(src, s);
                    s += 8;
                }
            }
            case FLOAT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    doubles[base + present[k]] = Double.longBitsToDouble(leLong(src, s));
                    s += 8;
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
    }

    private void gatherFixedFromDict(int[] ids, int[] present, int base,
            int presentCount) {
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        switch (physicalType) {
            case INT32 -> {
                if (widen) {
                    for (int k = 0; k < presentCount; k++) {
                        longs[base + (present == null ? k : present[k])] = dictInts[ids[k]];
                    }
                } else {
                    for (int k = 0; k < presentCount; k++) {
                        ints[base + (present == null ? k : present[k])] = dictInts[ids[k]];
                    }
                }
            }
            case INT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    longs[base + (present == null ? k : present[k])] = dictLongs[ids[k]];
                }
            }
            case FLOAT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    doubles[base + (present == null ? k : present[k])] = dictDoubles[ids[k]];
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
    }

    // ------------------------------------------------------------------ binary / utf8 (array gather)

    private void decodeBinaryPage(Page page, int base, int[] present,
            int presentCount, long valuesStart) {
        int pos = utf8Len;
        if (page.encoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            dictionaryIds(page, valuesStart).readInts(ids, 0, presentCount);
            int k = 0;
            for (int i = 0; i < page.valueCount; i++) {
                utf8Offsets[base + i] = pos;
                if (present == null || (k < presentCount && present[k] == i)) {
                    int id = ids[k++];
                    int start = dictOffsets[id];
                    int len = dictOffsets[id + 1] - start;
                    pos = appendBytesFromArray(dictBytes, start, len, pos);
                }
            }
            utf8Offsets[base + page.valueCount] = pos;
            utf8Len = pos;
            return;
        }
        // PLAIN: each present value is a little-endian int32 length prefix then that many bytes.
        byte[] src = page.data;
        int s = (int) valuesStart;
        int k = 0;
        for (int i = 0; i < page.valueCount; i++) {
            utf8Offsets[base + i] = pos;
            if (present == null || (k < presentCount && present[k] == i)) {
                int len = readLeInt(src, s);
                s += 4;
                pos = appendBytesFromArray(src, s, len, pos);
                s += len;
                k++;
            }
        }
        utf8Offsets[base + page.valueCount] = pos;
        utf8Len = pos;
    }

    private int appendBytesFromArray(byte[] src, int srcPos, int len,
            int pos) {
        if (pos + len > utf8Data.length) {
            int cap = Math.max(pos + len, utf8Data.length * 2);
            utf8Data = java.util.Arrays.copyOf(utf8Data, cap);
        }
        System.arraycopy(src, srcPos, utf8Data, pos, len);
        return pos + len;
    }

    // ------------------------------------------------------------------ flush (one bulk copy each)

    /**
     * Copies the finished fixed-width row group into the Arrow data buffer (one
     * {@code MemorySegment.copy}) and, when the column has nulls, the validity
     * word array into the validity buffer (a second copy). Call once after the
     * last page. {@code outValidity} may be {@code null} only when the column
     * has no nulls.
     */
    public void flushFixed(MemorySegment outData, MemorySegment outValidity) {
        switch (type) {
            case INT32 -> MemorySegment.copy(ints, 0, outData, LE_INT, 0L, rowGroupRows);
            case INT64 -> MemorySegment.copy(longs, 0, outData, LE_LONG, 0L, rowGroupRows);
            case FLOAT64 -> MemorySegment.copy(doubles, 0, outData, ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN), 0L,
                    rowGroupRows);
            default -> throw new IllegalArgumentException("not a fixed lane: " + type);
        }
        flushValidity(outValidity);
    }

    /**
     * Copies the finished UTF8 row group: the offsets ({@code rowGroupRows + 1}
     * int32s), the gathered data bytes ({@link #utf8Bytes} of them), and the
     * validity when the column has nulls -- one {@code MemorySegment.copy} each.
     */
    public void flushUtf8(MemorySegment outOffsets, MemorySegment outData, MemorySegment outValidity) {
        MemorySegment.copy(utf8Offsets, 0, outOffsets, LE_INT, 0L,
                rowGroupRows + 1);
        if (utf8Len > 0) {
            MemorySegment.copy(utf8Data, 0, outData, ValueLayout.JAVA_BYTE, 0L, utf8Len);
        }
        flushValidity(outValidity);
    }

    private void flushValidity(MemorySegment outValidity) {
        if (maxDefLevel == 0) {
            if (outValidity != null) {
                Bitmap.fill(outValidity, rowGroupRows, true);
            }
            return;
        }
        int words = (rowGroupRows + 63) >>> 6;
        MemorySegment.copy(validityWords, 0, outValidity, LE_LONG, 0L, words);
    }

    /** The staging arrays, for tests. */
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

    // ------------------------------------------------------------------ shared

    private RleBitPackingReader dictionaryIds(Page page, long valuesStart) {
        int idBitWidth = page.data[(int) valuesStart] & 0xFF;
        int streamStart = (int) valuesStart + 1;
        int streamLen = (int) (page.dataEnd() - streamStart);
        return reader(page.data, streamStart, streamLen, idBitWidth);
    }

    private int[] level(int n) {
        if (levelScratch.length < n) {
            levelScratch = new int[Math.max(n, 2 * levelScratch.length)];
        }
        return levelScratch;
    }

    private int[] id(int n) {
        if (idScratch.length < n) {
            idScratch = new int[Math.max(n, 2 * idScratch.length)];
        }
        return idScratch;
    }

    private int[] present(int n) {
        // The present-slot list can be up to a page's value count; reuse idScratch's sibling.
        if (presentScratch.length < n) {
            presentScratch = new int[Math.max(n, 2 * presentScratch.length)];
        }
        return presentScratch;
    }

    private int[] presentScratch = new int[0];

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
         * A v1 page: {@code data} is the whole decompressed page (levels
         * prefixed, then values).
         */
        public static Page v1(byte[] data, int valueCount, ParquetPageDecoder.Encoding encoding) {
            return new Page(data, 0, data.length, 0, 0, 0,
                    valueCount, false, encoding);
        }

        /**
         * A v2 page whose {@code data} holds the definition levels (first {@code
         * levelsLength} bytes) followed by the values.
         */
        public static Page v2(byte[] data, int levelsLength, int valueCount,
                              ParquetPageDecoder.Encoding encoding) {
            return new Page(data, 0, data.length, 0, levelsLength, levelsLength,
                    valueCount, true, encoding);
        }

        /**
         * A v2 page whose levels and values sit at explicit offsets inside
         * {@code data} (so the node need not copy them together): {@code
         * data[levelsOffset .. levelsOffset+levelsLength)} are the levels and
         * {@code data[valuesOffset ..)} the values.
         */
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
