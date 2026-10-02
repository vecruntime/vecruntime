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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;

/**
 * Decodes a single Parquet data page's values straight into Arrow-layout
 * {@link MemorySegment}s, for a flat (max repetition level 0) column. This is
 * the value-decoding heart of #559 slice 1: parquet-java gives the footer, the
 * page headers and the decompressed page bytes; this class turns definition
 * levels, {@code RLE_DICTIONARY} ids and {@code PLAIN} values into a
 * {@link VectorBuffers}, one pass, no Spark {@code ColumnVector} in between.
 *
 * <p>Dependency-free (kernels has no Spark and no parquet-java): the caller
 * hands over the decompressed page as a {@code MemorySegment} plus the small
 * amount of metadata the page header carries, and an {@link Arena} for the
 * output buffers. The scalar reference the tests compare against is this
 * decoder's own straightforward reading of the same bytes; a separate spark/
 * test cross-checks it against pages parquet-java's own writers produce.
 *
 * <h2>Level layout by page version</h2>
 * <ul>
 *   <li><b>v1</b> ({@link PageV1Levels}): when {@code maxDefLevel > 0} the
 *       definition levels are an RLE/bit-packed hybrid stream at the start of
 *       the page data, prefixed by a little-endian int32 byte length; the values
 *       follow immediately after. When {@code maxDefLevel == 0} there are no
 *       level bytes and every value is present.
 *   <li><b>v2</b> ({@link PageV2Levels}): the definition-level byte length is in
 *       the page header (not inlined), the levels are always RLE (no length
 *       prefix), and the values region is a separate slice. Repetition levels
 *       (absent on a flat column) would precede the definition levels.
 * </ul>
 *
 * <h2>Physical -&gt; VecType</h2>
 * INT32 and DATE and DECIMAL(p&le;9, int32 physical) -&gt; {@link VecType#INT32};
 * INT64 and DECIMAL(10&le;p&le;18, int64 physical) -&gt; {@link VecType#INT64};
 * DOUBLE -&gt; {@link VecType#FLOAT64}; BINARY/UTF8 -&gt; {@link VecType#UTF8}.
 * (The Spark layer maps DATE to INT32 days and a narrow decimal to its unscaled
 * INT32/INT64 already; this decoder only moves the physical bytes.)
 */
public final class ParquetPageDecoder {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private ParquetPageDecoder() {}

    /** The value encoding of a data page this decoder supports. */
    public enum Encoding {
        PLAIN,
        RLE_DICTIONARY,
        /**
         * INT32/INT64 deltas (#559 slice 2). Decoded by {@link ColumnChunkDecoder}
         * through {@link DeltaBinaryPackedReader}; not by this one-shot decoder.
         */
        DELTA_BINARY_PACKED
    }

    /** One data page as parquet-java hands it over, flat column only. */
    public static final class Page {
        /**
         * The whole decompressed page data (levels + values for v1; values
         * slice for v2).
         */
        public final MemorySegment data;

        /**
         * First byte of this page's data within {@code data} (v1 levels/values
         * start here; v2 uses the offsets below).
         */
        public final long dataOffset;

        /**
         * Byte length of this page's data within {@code data} (bounds the
         * dictionary-id stream).
         */
        public final long dataLength;

        /**
         * Offset of the definition-level stream within {@code data} (v2; v1
         * ignores it, levels are prefixed).
         */
        public final long levelsOffset;

        /**
         * Byte length of the definition-level stream (v2 from the header; v1
         * reads its own int32 prefix).
         */
        public final int levelsLength;

        /**
         * Offset of the value bytes within {@code data} (v2). For v1 the values
         * start after the prefixed levels.
         */
        public final long valuesOffset;

        public final int valueCount;
        public final int maxDefLevel;
        public final boolean v2;
        public final Encoding encoding;

        public Page(
                MemorySegment data,
                long dataOffset,
                long dataLength,
                long levelsOffset,
                int levelsLength,
                long valuesOffset,
                int valueCount,
                int maxDefLevel,
                boolean v2,
                Encoding encoding) {
            this.data = data;
            this.dataOffset = dataOffset;
            this.dataLength = dataLength;
            this.levelsOffset = levelsOffset;
            this.levelsLength = levelsLength;
            this.valuesOffset = valuesOffset;
            this.valueCount = valueCount;
            this.maxDefLevel = maxDefLevel;
            this.v2 = v2;
            this.encoding = encoding;
        }

        /** A v1 page: levels (prefixed) then values start at {@link #dataOffset}. */
        public static Page v1(MemorySegment data, long dataOffset, long dataLength,
                              int valueCount, int maxDefLevel, Encoding encoding) {
            return new Page(data, dataOffset, dataLength, 0, 0, 0,
                    valueCount, maxDefLevel, false, encoding);
        }

        /**
         * A v2 page: level and value regions are separate slices given by
         * offset/length.
         */
        public static Page v2(
                MemorySegment data,
                long levelsOffset,
                int levelsLength,
                long valuesOffset,
                long valuesLength,
                int valueCount,
                int maxDefLevel,
                Encoding encoding) {
            return new Page(data, levelsOffset, levelsLength + valuesLength, levelsOffset, levelsLength,
                    valuesOffset, valueCount, maxDefLevel, true, encoding);
        }

        long levelsOffsetForV1() {
            return dataOffset;
        }

        /**
         * One past the last byte of this page's value region (bounds the
         * dictionary-id stream).
         */
        long dataEnd() {
            return v2 ? valuesOffset + (dataLength - levelsLength) : dataOffset + dataLength;
        }
    }

    /**
     * Decodes {@code page} of the given {@code type} into a fresh column in
     * {@code arena}. For {@code RLE_DICTIONARY} pages {@code dictionary} is the
     * already-decoded dictionary of the same {@link VecType} (its ids are
     * resolved into the output here -- dictionaries are decoded into the output
     * in this slice, per the issue's first slice). {@code dictionary} is
     * {@code null} for {@code PLAIN} pages.
     */
    public static VectorBuffers decode(Page page, VecType type, VectorBuffers dictionary,
            Arena arena) {
        if (page.encoding == Encoding.DELTA_BINARY_PACKED) {
            throw new IllegalArgumentException("DELTA_BINARY_PACKED is decoded by ColumnChunkDecoder, not ParquetPageDecoder");
        }
        int n = page.valueCount;
        int defBitWidth = bitWidth(page.maxDefLevel);

        // Definition levels: which of the n slots are present. maxDefLevel == 0 => all present, no bytes.
        long valuesStart;
        int[] present; // slot index of each present value, in order; length = number of present values
        int presentCount;
        MemorySegment validity;
        if (page.maxDefLevel == 0) {
            valuesStart = page.v2 ? page.valuesOffset : page.levelsOffsetForV1();
            present = null;
            presentCount = n;
            validity = null;
        } else {
            long levelStart;
            long levelLen;
            if (page.v2) {
                levelStart = page.levelsOffset;
                levelLen = page.levelsLength;
                valuesStart = page.valuesOffset;
            } else {
                // v1: int32 little-endian byte length, then that many bytes of RLE/bit-packed levels.
                int len = page.data.get(LE_INT, page.levelsOffsetForV1());
                levelStart = page.levelsOffsetForV1() + 4;
                levelLen = len;
                valuesStart = levelStart + len;
            }
            RleBitPackingReader levels = new RleBitPackingReader(page.data, levelStart, levelLen, defBitWidth);
            validity = ArrowLayout.allocateBitmap(arena, n);
            present = new int[n];
            presentCount = 0;
            int[] levelBuf = new int[n];
            levels.readInts(levelBuf, 0, n);
            for (int i = 0; i < n; i++) {
                boolean set = levelBuf[i] == page.maxDefLevel;
                Bitmap.setTo(validity, i, set);
                if (set) {
                    present[presentCount++] = i;
                }
            }
        }

        if (type == VecType.UTF8) {
            return decodeBinary(page, dictionary, arena, n, present, presentCount,
                    validity, valuesStart);
        }
        return decodeFixed(page, type, dictionary, arena, n, present,
                presentCount, validity, valuesStart);
    }

    // ------------------------------------------------------------------ fixed width

    private static VectorBuffers decodeFixed(
            Page page,
            VecType type,
            VectorBuffers dictionary,
            Arena arena,
            int n,
            int[] present,
            int presentCount,
            MemorySegment validity,
            long valuesStart) {
        int width = type.byteWidth();
        MemorySegment data = ArrowLayout.allocateData(arena, type, n);
        if (page.encoding == Encoding.RLE_DICTIONARY) {
            RleBitPackingReader ids = dictionaryIds(page, valuesStart);
            int[] idBuf = new int[presentCount];
            ids.readInts(idBuf, 0, presentCount);
            for (int k = 0; k < presentCount; k++) {
                int slot = present == null ? k : present[k];
                copyFixedFromDict(dictionary, idBuf[k], data, slot, type);
            }
        } else if (present == null) {
            // PLAIN, no nulls: the values are exactly the output, one contiguous copy.
            MemorySegment.copy(page.data, valuesStart, data, 0L, (long) n * width);
        } else { // PLAIN with nulls: values are packed, scatter into present slots.
            long src = valuesStart;
            for (int k = 0; k < presentCount; k++) {
                int slot = present[k];
                switch (type) {
                    case INT32 -> data.set(LE_INT, (long) slot << 2, page.data.get(LE_INT, src));
                    case INT64 -> data.set(LE_LONG, (long) slot << 3, page.data.get(LE_LONG, src));
                    case FLOAT64 -> data.set(LE_LONG, (long) slot << 3, page.data.get(LE_LONG, src));
                    default -> throw new IllegalArgumentException("not a fixed lane: " + type);
                }
                src += width;
            }
        }
        return SegmentVectorBuffers.fixedWidth(type, n, validity, data);
    }

    private static void copyFixedFromDict(VectorBuffers dict, int id, MemorySegment data,
            int slot, VecType type) {
        switch (type) {
            case INT32 -> data.set(LE_INT, (long) slot << 2, dict.getInt(id));
            case INT64 -> data.set(LE_LONG, (long) slot << 3, dict.getLong(id));
            case FLOAT64 -> data.set(LE_LONG, (long) slot << 3,
                    dict.data().get(LE_LONG, (long) id << 3));
            default -> throw new IllegalArgumentException("not a fixed lane: " + type);
        }
    }

    // ------------------------------------------------------------------ binary / utf8

    private static VectorBuffers decodeBinary(
            Page page,
            VectorBuffers dictionary,
            Arena arena,
            int n,
            int[] present,
            int presentCount,
            MemorySegment validity,
            long valuesStart) {
        MemorySegment offsets = ArrowLayout.allocateOffsets(arena, n);
        // Two passes: size the data buffer from the present values' lengths, then fill.
        int[] lengths = new int[presentCount];
        long total = 0;
        if (page.encoding == Encoding.RLE_DICTIONARY) {
            RleBitPackingReader ids = dictionaryIds(page, valuesStart);
            int[] resolvedIds = new int[presentCount];
            ids.readInts(resolvedIds, 0, presentCount);
            for (int k = 0; k < presentCount; k++) {
                int id = resolvedIds[k];
                int len = utf8Length(dictionary, id);
                lengths[k] = len;
                total += len;
            }
            MemorySegment data = ArrowLayout.allocateBytes(arena, Math.max(total, 1));
            fillBinaryOffsets(offsets, present, presentCount, lengths, n);
            long pos = 0;
            for (int k = 0; k < presentCount; k++) {
                int id = resolvedIds[k];
                int srcStart = dictionary.offsets().get(LE_INT, (long) id << 2);
                MemorySegment.copy(dictionary.data(), ValueLayout.JAVA_BYTE, srcStart, data, ValueLayout.JAVA_BYTE,
                        pos, lengths[k]);
                pos += lengths[k];
            }
            return SegmentVectorBuffers.utf8(n, validity, offsets, data);
        }
        // PLAIN: each value is a little-endian int32 length prefix followed by that many bytes.
        long src = valuesStart;
        long[] starts = new long[presentCount];
        for (int k = 0; k < presentCount; k++) {
            int len = page.data.get(LE_INT, src);
            src += 4;
            starts[k] = src;
            lengths[k] = len;
            total += len;
            src += len;
        }
        MemorySegment data = ArrowLayout.allocateBytes(arena, Math.max(total, 1));
        fillBinaryOffsets(offsets, present, presentCount, lengths, n);
        long pos = 0;
        for (int k = 0; k < presentCount; k++) {
            MemorySegment.copy(page.data, ValueLayout.JAVA_BYTE, starts[k], data, ValueLayout.JAVA_BYTE, pos,
                    lengths[k]);
            pos += lengths[k];
        }
        return SegmentVectorBuffers.utf8(n, validity, offsets, data);
    }

    /**
     * Writes the {@code n + 1} Arrow offsets. A null slot repeats the previous
     * offset (its value has zero length); a present slot advances by its length.
     */
    private static void fillBinaryOffsets(MemorySegment offsets, int[] present, int presentCount,
            int[] lengths, int n) {
        if (present == null) {
            int pos = 0;
            for (int i = 0; i < n; i++) {
                offsets.set(LE_INT, (long) i << 2, pos);
                pos += lengths[i];
            }
            offsets.set(LE_INT, (long) n << 2, pos);
            return;
        }
        int pos = 0;
        int k = 0;
        for (int i = 0; i < n; i++) {
            offsets.set(LE_INT, (long) i << 2, pos);
            if (k < presentCount && present[k] == i) {
                pos += lengths[k];
                k++;
            }
        }
        offsets.set(LE_INT, (long) n << 2, pos);
    }

    private static int utf8Length(VectorBuffers dict, int id) {
        MemorySegment off = dict.offsets();
        return off.get(LE_INT, (long) (id + 1) << 2) - off.get(LE_INT, (long) id << 2);
    }

    // ------------------------------------------------------------------ shared

    /**
     * The dictionary-id reader of an {@code RLE_DICTIONARY} data page: the first
     * byte at {@code valuesStart} is the bit width, then a hybrid stream of ids
     * follows, bounded by the page's value region.
     */
    private static RleBitPackingReader dictionaryIds(Page page, long valuesStart) {
        int idBitWidth = page.data.get(BYTE, valuesStart) & 0xFF;
        long streamStart = valuesStart + 1;
        long streamLen = page.dataEnd() - streamStart;
        return new RleBitPackingReader(page.data, streamStart, streamLen, idBitWidth);
    }

    /** Minimum bits to hold values {@code 0 .. maxLevel}. */
    public static int bitWidth(int maxLevel) {
        if (maxLevel == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxLevel);
    }

    /**
     * Decodes a {@code PLAIN}-encoded dictionary page (no levels, no nulls,
     * {@code numValues} entries packed contiguously) into a {@link VectorBuffers}
     * of {@code type}, for {@link #decode} to resolve {@code RLE_DICTIONARY} ids
     * against. This is the only encoding Parquet uses for a dictionary page.
     */
    public static VectorBuffers decodeDictionary(MemorySegment data, long offset, long length,
            int numValues, VecType type, Arena arena) {
        if (type == VecType.UTF8) {
            MemorySegment offsets = ArrowLayout.allocateOffsets(arena, numValues);
            int[] lengths = new int[numValues];
            long[] starts = new long[numValues];
            long src = offset;
            long total = 0;
            for (int i = 0; i < numValues; i++) {
                int len = data.get(LE_INT, src);
                src += 4;
                starts[i] = src;
                lengths[i] = len;
                total += len;
                src += len;
            }
            MemorySegment out = ArrowLayout.allocateBytes(arena, Math.max(total, 1));
            long pos = 0;
            for (int i = 0; i < numValues; i++) {
                offsets.set(LE_INT, (long) i << 2, (int) pos);
                MemorySegment.copy(data, ValueLayout.JAVA_BYTE, starts[i], out, ValueLayout.JAVA_BYTE, pos,
                        lengths[i]);
                pos += lengths[i];
            }
            offsets.set(LE_INT, (long) numValues << 2, (int) pos);
            return SegmentVectorBuffers.utf8(numValues, null, offsets, out);
        }
        int width = type.byteWidth();
        MemorySegment out = ArrowLayout.allocateData(arena, type, numValues);
        long src = offset;
        for (int i = 0; i < numValues; i++) {
            switch (type) {
                case INT32 -> out.set(LE_INT, (long) i << 2, data.get(LE_INT, src));
                case INT64, FLOAT64 -> out.set(LE_LONG, (long) i << 3, data.get(LE_LONG, src));
                default -> throw new IllegalArgumentException("unsupported dictionary lane: " + type);
            }
            src += width;
        }
        return SegmentVectorBuffers.fixedWidth(type, numValues, null, out);
    }
}
