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
 * page) straight into caller-owned Arrow-layout output buffers, one flat (max
 * repetition level 0) column at a time. This is the decode-into-output-at-
 * offset core of #559 slice 1's scan node: {@link ParquetPageDecoder} returned
 * a fresh column per page; this class writes present values at {@code
 * [rowOffset + presentSlot]} into buffers the caller sized for the whole row
 * group, so a batch spans page boundaries and no per-page temporary column is
 * ever allocated (STATUS-1b binding contract, requirement 2).
 *
 * <h2>Injected unpacker (requirement 1)</h2>
 *
 * The chunk is constructed with a {@link GroupUnpacker} factory {@code (int
 * bitWidth) -> GroupUnpacker}, cached per bit width in a small {@code int ->
 * GroupUnpacker} map ({@link #unpackerFor}). Every definition-level stream and
 * every {@code RLE_DICTIONARY} id stream is read through {@link
 * RleBitPackingReader#overArray} with that unpacker -- no decode path on the
 * node's side constructs a scalar/{@code MemorySegment} reader. The Spark scan
 * node passes {@code Packer.LITTLE_ENDIAN.newBytePacker(bw)::unpack8Values}; a
 * {@code null} factory falls back to the built-in scalar unpack (the kernels'
 * own parquet-free tests), identical result.
 *
 * <h2>Reuse (requirement 3)</h2>
 *
 * The level/id {@code int[]} scratch and the {@code present}/{@code lengths}
 * scratch grow to the largest page seen and are reused for the whole chunk; the
 * dictionary is decoded once into {@link #dictionary} and every dictionary
 * page's ids gather straight from it into the output. The output buffers, the
 * unpackers and this decoder live for the column-chunk lifetime.
 *
 * <h2>Hot loop (requirement 4)</h2>
 *
 * The physical type is fixed for the whole chunk, so the per-value store is
 * specialised once in {@link #decodePlainFixed}/{@link #gatherFixedFromDict} by
 * a {@code switch (type)} outside the page loop -- no per-value virtual call,
 * no boxing, no per-value type switch.
 */
public final class ColumnChunkDecoder {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    private final VecType type;
    private final VecType physicalType;
    private final int maxDefLevel;
    private final int defBitWidth;

    /** Output buffers for the whole row group, written at a running offset. */
    private final MemorySegment outData;

    private final MemorySegment outValidity; // null when the column chunk has no null definition levels
    private final MemorySegment outOffsets; // UTF8 only
    private final int rowGroupRows;

    /** UTF8 data grows into this; the caller reads {@link #utf8Bytes} written. */
    private final Utf8Sink utf8;

    private final IntFunction<GroupUnpacker> unpackerFactory;
    // Small int -> GroupUnpacker cache: the def-level width and every id width seen in the chunk.
    private final GroupUnpacker[] unpackers = new GroupUnpacker[33];
    private final boolean[] unpackerSet = new boolean[33];

    private VectorBuffers dictionary; // decoded once per chunk, or null (all pages PLAIN)

    // Reused scratch, grown to the largest page.
    private int[] levelScratch = new int[0];
    private int[] idScratch = new int[0];
    private int[] presentScratch = new int[0];

    private int rowOffset; // present-rows written so far into the output (== slots for a no-null column)

    /**
     * @param type the output lane type (INT32, INT64, FLOAT64, UTF8)
     * @param maxDefLevel the column's max definition level (0 => no nulls, no
     *     level bytes)
     * @param rowGroupRows total rows in this column chunk (the caller sized the
     *     output for this)
     * @param outData the output data buffer (fixed lanes) or the UTF8
     *     indices/offset sink target
     * @param outValidity the output validity bitmap, or {@code null} when
     *     {@code maxDefLevel == 0}
     * @param outOffsets the output UTF8 offsets buffer ({@code rowGroupRows +
     *     1} int32s), or {@code null}
     * @param utf8 a growable byte sink for UTF8 data, or {@code null} for fixed
     *     types
     * @param unpackerFactory {@code bitWidth -> GroupUnpacker}, or {@code null}
     *     for the scalar fallback
     */
    public ColumnChunkDecoder(
            VecType type,
            int maxDefLevel,
            int rowGroupRows,
            MemorySegment outData,
            MemorySegment outValidity,
            MemorySegment outOffsets,
            Utf8Sink utf8,
            IntFunction<GroupUnpacker> unpackerFactory) {
        this(type, type, maxDefLevel, rowGroupRows, outData, outValidity,
                outOffsets, utf8, unpackerFactory);
    }

    /**
     * The general form, distinguishing the Parquet <em>physical</em> read type
     * from the output lane {@code type}. They differ for a narrow decimal whose
     * precision is at most 9: it is stored physically as {@code INT32} in the
     * Parquet file but the output lane is {@code INT64} (the unscaled value in
     * a long lane, Spark's own representation), so each value is read as an int
     * and sign-extended to the long slot. Otherwise {@code physicalType ==
     * type}.
     *
     * @param physicalType the type the PLAIN values and the dictionary entries
     *     are read as
     * @param type the output lane the values are written to
     */
    public ColumnChunkDecoder(
            VecType physicalType,
            VecType type,
            int maxDefLevel,
            int rowGroupRows,
            MemorySegment outData,
            MemorySegment outValidity,
            MemorySegment outOffsets,
            Utf8Sink utf8,
            IntFunction<GroupUnpacker> unpackerFactory) {
        this.physicalType = physicalType;
        this.type = type;
        this.maxDefLevel = maxDefLevel;
        this.defBitWidth = ParquetPageDecoder.bitWidth(maxDefLevel);
        this.rowGroupRows = rowGroupRows;
        this.outData = outData;
        this.outValidity = outValidity;
        this.outOffsets = outOffsets;
        this.utf8 = utf8;
        this.unpackerFactory = unpackerFactory;
        if (outValidity != null) {
            // Start all-valid; each page clears the null slots it finds. A no-null page then costs nothing.
            Bitmap.fill(outValidity, rowGroupRows, true);
        }
        if (type == VecType.UTF8 && outOffsets != null) {
            outOffsets.set(LE_INT, 0L, 0);
        }
    }

    /**
     * Decodes {@code dictionary} (a {@code PLAIN} dictionary page, {@code
     * numValues} entries) into a reusable buffer for this chunk. Call once,
     * before the first {@code RLE_DICTIONARY} data page.
     */
    public void setDictionary(VectorBuffers dictionary) {
        this.dictionary = dictionary;
    }

    public VectorBuffers dictionary() {
        return dictionary;
    }

    /**
     * Present rows written so far (equals the row offset the next page starts
     * at).
     */
    public int rowsWritten() {
        return rowOffset;
    }

    /** Total UTF8 data bytes written into {@link #utf8} so far. */
    public long utf8Bytes() {
        return utf8 == null ? 0L : utf8.length();
    }

    /**
     * The {@link GroupUnpacker} for {@code bitWidth}, built and cached on first
     * use; {@code null} width 0.
     */
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

    /**
     * Decodes one data page into the output at the current row offset, then
     * advances the offset by {@code page.valueCount}. The page bytes are a
     * {@code byte[]} (parquet-java hands the node a decompressed page as a
     * {@code BytesInput}/{@code byte[]}); the levels and ids are read through
     * the injected unpacker.
     */
    public void decodePage(Page page) {
        int n = page.valueCount;
        int base = rowOffset;
        if (base + n > rowGroupRows) {
            throw new IllegalStateException("page overruns the row group: " + (base + n) + " > " + rowGroupRows);
        }
        int presentCount;
        int[] present; // slot (relative to base) of each present value; null when the page has no nulls
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
            presentCount = 0;
            for (int i = 0; i < n; i++) {
                if (levels[i] == maxDefLevel) {
                    present[presentCount++] = i;
                } else {
                    Bitmap.clear(outValidity, base + i);
                }
            }
        }

        if (type == VecType.UTF8) {
            decodeBinaryPage(page, base, present, presentCount, valuesStart);
        } else {
            decodeFixedPage(page, base, present, presentCount, valuesStart);
        }
        rowOffset = base + n;
    }

    // ------------------------------------------------------------------ fixed width

    private void decodeFixedPage(Page page, int base, int[] present,
            int presentCount, long valuesStart) {
        if (page.encoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            dictionaryIds(page, valuesStart).readInts(ids, 0, presentCount);
            gatherFixedFromDict(ids, present, base, presentCount);
        } else if (present == null && physicalType == type) {
            // PLAIN, no nulls, no width change: the values are exactly the output for this page -- one
            // contiguous copy (the only bulk copy the contract allows).
            MemorySegment.copy(mem(page.data), valuesStart, outData, (long) base * type.byteWidth(),
                    (long) presentCount * type.byteWidth());
        } else {
            decodePlainFixed(page, valuesStart, present, base, presentCount);
        }
    }

    private void decodePlainFixed(Page page, long valuesStart, int[] present,
            int base, int presentCount) {
        MemorySegment src = mem(page.data);
        long s = valuesStart;
        // The physical read width drives the source stride; the store targets the output lane, widening a
        // narrow-decimal INT32 physical value into its INT64 lane (sign-extended).
        boolean widen = physicalType == VecType.INT32 && type == VecType.INT64;
        switch (physicalType) {
            case INT32 -> {
                if (widen) {
                    for (int k = 0; k < presentCount; k++) {
                        int slot = base + (present == null ? k : present[k]);
                        outData.set(LE_LONG, (long) slot << 3, src.get(LE_INT, s)); // sign-extend
                        s += 4;
                    }
                } else {
                    for (int k = 0; k < presentCount; k++) {
                        int slot = base + (present == null ? k : present[k]);
                        outData.set(LE_INT, (long) slot << 2, src.get(LE_INT, s));
                        s += 4;
                    }
                }
            }
            case INT64, FLOAT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    int slot = base + (present == null ? k : present[k]);
                    outData.set(LE_LONG, (long) slot << 3, src.get(LE_LONG, s));
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
                        int slot = base + (present == null ? k : present[k]);
                        outData.set(LE_LONG, (long) slot << 3, dictionary.getInt(ids[k])); // sign-extend
                    }
                } else {
                    for (int k = 0; k < presentCount; k++) {
                        int slot = base + (present == null ? k : present[k]);
                        outData.set(LE_INT, (long) slot << 2, dictionary.getInt(ids[k]));
                    }
                }
            }
            case INT64 -> {
                for (int k = 0; k < presentCount; k++) {
                    int slot = base + (present == null ? k : present[k]);
                    outData.set(LE_LONG, (long) slot << 3, dictionary.getLong(ids[k]));
                }
            }
            case FLOAT64 -> {
                MemorySegment dd = dictionary.data();
                for (int k = 0; k < presentCount; k++) {
                    int slot = base + (present == null ? k : present[k]);
                    outData.set(LE_LONG, (long) slot << 3, dd.get(LE_LONG, (long) ids[k] << 3));
                }
            }
            default -> throw new IllegalArgumentException("not a fixed physical lane: " + physicalType);
        }
    }

    // ------------------------------------------------------------------ binary / utf8

    private void decodeBinaryPage(Page page, int base, int[] present,
            int presentCount, long valuesStart) {
        // Offsets are absolute into the chunk's growing byte sink; a null slot repeats the previous offset.
        long prefix = utf8.length();
        if (page.encoding == ParquetPageDecoder.Encoding.RLE_DICTIONARY) {
            int[] ids = id(presentCount);
            dictionaryIds(page, valuesStart).readInts(ids, 0, presentCount);
            MemorySegment dictData = dictionary.data();
            MemorySegment dictOff = dictionary.offsets();
            int k = 0;
            long pos = prefix;
            for (int i = 0; i < page.valueCount; i++) {
                outOffsets.set(LE_INT, (long) (base + i) << 2, (int) pos);
                if (present == null || (k < presentCount && present[k] == i)) {
                    int id = ids[k++];
                    int start = dictOff.get(LE_INT, (long) id << 2);
                    int len = dictOff.get(LE_INT, (long) (id + 1) << 2) - start;
                    utf8.append(dictData, start, len);
                    pos += len;
                }
            }
            outOffsets.set(LE_INT, (long) (base + page.valueCount) << 2, (int) pos);
            return;
        }
        // PLAIN: each present value is a little-endian int32 length prefix then that many bytes.
        MemorySegment src = mem(page.data);
        long s = valuesStart;
        int k = 0;
        long pos = prefix;
        for (int i = 0; i < page.valueCount; i++) {
            outOffsets.set(LE_INT, (long) (base + i) << 2, (int) pos);
            if (present == null || (k < presentCount && present[k] == i)) {
                int len = src.get(LE_INT, s);
                s += 4;
                utf8.append(src, s, len);
                s += len;
                pos += len;
                k++;
            }
        }
        outOffsets.set(LE_INT, (long) (base + page.valueCount) << 2, (int) pos);
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
        if (presentScratch.length < n) {
            presentScratch = new int[Math.max(n, 2 * presentScratch.length)];
        }
        return presentScratch;
    }

    private static int readLeInt(byte[] a, long off) {
        int o = (int) off;
        return (a[o] & 0xFF)
                | ((a[o + 1] & 0xFF) << 8)
                | ((a[o + 2] & 0xFF) << 16)
                | ((a[o + 3] & 0xFF) << 24);
    }

    private static MemorySegment mem(byte[] a) {
        return MemorySegment.ofArray(a);
    }

    /**
     * One data page as parquet-java hands it over: the decompressed bytes as a
     * {@code byte[]} plus the small page-header metadata. v1 inlines the levels
     * (int32-prefixed) before the values at {@link #dataOffset}; v2 gives level
     * and value regions as separate slices.
     */
    public static final class Page {
        public final byte[] data;
        public final long dataOffset; // v1: first byte (levels then values); v2: unused for start
        public final long dataLength; // v1: bytes of this page's data; v2: levelsLength + valuesLength
        public final long levelsOffset; // v2
        public final int levelsLength; // v2
        public final long valuesOffset; // v2
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
         * A v2 page: {@code levels} is the (already sized) definition-level slice,
         * {@code values} the value slice, both as separate {@code byte[]}s
         * parquet-java gives; we lay them out end to end into one buffer for the
         * reader offsets.
         */
        public static Page v2(byte[] data, int levelsLength, int valueCount,
                              ParquetPageDecoder.Encoding encoding) {
            // `data` = definition levels (levelsLength bytes) followed by the values.
            return new Page(data, 0, data.length, 0, levelsLength, levelsLength,
                    valueCount, true, encoding);
        }

        long valuesStart() {
            return v2 ? valuesOffset : dataOffset;
        }

        long dataEnd() {
            return v2 ? valuesOffset + (dataLength - levelsLength) : dataOffset + dataLength;
        }
    }

    /**
     * A growable native byte buffer the UTF8 data of a whole column chunk is
     * appended into, reused across pages (and, by the node, across row groups by
     * {@link #reset()}). Kernels stay allocation-light: the buffer doubles only
     * when it must, and the node hands the finished bytes to Arrow with a single
     * copy per row group (a UTF8 column's data is one buffer either way).
     */
    public interface Utf8Sink {
        /** Appends {@code len} bytes of {@code src} at {@code srcOffset}. */
        void append(MemorySegment src, long srcOffset, int len);

        /** Total bytes appended since the last {@link #reset()}. */
        long length();

        /** The backing segment (valid for {@code [0, length())}). */
        MemorySegment segment();

        /** Discards everything; the buffer is reused. */
        void reset();
    }
}
