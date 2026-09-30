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

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scalar-oracle tests for {@link ColumnChunkDecoder}: several data pages of a
 * flat column are decoded into one row-group output at a running offset and
 * compared value-by-value against the input arrays. Covers INT32/INT64/FLOAT64/
 * UTF8, PLAIN and RLE_DICTIONARY, page v1 and v2, no-null / with-null /
 * all-null pages, a row group of many pages, and the injected-GroupUnpacker
 * requirement: a counting unpacker wired through the decoder must observe
 * {@code > 0} calls on a bit-packed level or id stream (STATUS-1b requirement 1).
 */
class ColumnChunkDecoderTest {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    // ---------------------------------------------------------------- counting unpacker (requirement 1)

    @Test
    void injectedUnpackerIsUsedOnBitPackedStreams() {
        // A column with nulls (bit-packed def levels) AND a dictionary (bit-packed ids): both level and id
        // streams go through the injected unpacker, so it MUST be called. The test fails if any decode path
        // constructs a scalar reader instead of using the injected one.
        Random rnd = new Random(559);
        int rows = 3000;
        Integer[] values = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = rnd.nextInt(5) == 0 ? null : rnd.nextInt(200); // 200-entry dictionary domain, ~20% null
        }
        AtomicInteger calls = new AtomicInteger();
        try (Arena arena = Arena.ofConfined()) {
            DecodeResult r = decodeDictInt32(values, arena, 1, // one page, dictionary-encoded
                    bw -> (src, sp, dst, dp) -> {
                        calls.incrementAndGet();
                        refUnpack8(src, sp, dst, dp, bw);
                    });
            assertInt32(values, r);
        }
        assertTrue(calls.get() > 0, "injected GroupUnpacker was never called on a bit-packed page");
    }

    // ---------------------------------------------------------------- fixed types

    @Test
    void int32PlainMultiPageWithNulls() {
        Random rnd = new Random(1);
        int rows = 5000;
        Integer[] v = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = rnd.nextInt(7) == 0 ? null : rnd.nextInt();
        }
        try (Arena arena = Arena.ofConfined()) {
            assertInt32(v, decodePlainInt32(v, arena, 4, false)); // 4 v1 pages
            assertInt32(v, decodePlainInt32(v, arena, 4, true)); // 4 v2 pages
        }
    }

    @Test
    void int32PlainNoNulls() {
        int rows = 4096;
        Integer[] v = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = i * 3 - 7;
        }
        try (Arena arena = Arena.ofConfined()) {
            DecodeResult r = decodePlainInt32(v, arena, 3, false);
            assertNull(r.buffers.validity(), "no-null column must have no validity buffer");
            assertInt32(v, r);
        }
    }

    @Test
    void int32AllNullPage() {
        Integer[] v = new Integer[1000];
        // First page all-null, second all-present.
        try (Arena arena = Arena.ofConfined()) {
            for (int i = 0; i < 500; i++) {
                v[i] = null;
            }
            for (int i = 500; i < 1000; i++) {
                v[i] = i;
            }
            assertInt32(v, decodePlainInt32(v, arena, splitAt(1000, 500), false));
        }
    }

    @Test
    void int64AndDoublePlain() {
        Random rnd = new Random(2);
        int rows = 2048;
        Long[] longs = new Long[rows];
        Double[] doubles = new Double[rows];
        for (int i = 0; i < rows; i++) {
            longs[i] = rnd.nextInt(9) == 0 ? null : rnd.nextLong();
            doubles[i] = rnd.nextInt(9) == 0 ? null : rnd.nextDouble() * 1e9;
        }
        try (Arena arena = Arena.ofConfined()) {
            DecodeResult lr = decodePlainFixed(longs, VecType.INT64, arena, 2, false,
                    (i, seg, off) -> seg.set(LE_LONG, off, longs[i]));
            for (int i = 0; i < rows; i++) {
                if (longs[i] == null) {
                    assertTrue(lr.buffers.isNull(i));
                } else {
                    assertEquals(longs[i].longValue(), lr.buffers.getLong(i), "long " + i);
                }
            }
            DecodeResult dr = decodePlainFixed(doubles, VecType.FLOAT64, arena, 3, true,
                    (i, seg, off) -> seg.set(LE_LONG, off, Double.doubleToLongBits(doubles[i])));
            for (int i = 0; i < rows; i++) {
                if (doubles[i] == null) {
                    assertTrue(dr.buffers.isNull(i));
                } else {
                    assertEquals(doubles[i].doubleValue(), dr.buffers.getDouble(i), 0.0,
                            "double " + i);
                }
            }
        }
    }

    @Test
    void int32DictionaryMultiPage() {
        Random rnd = new Random(3);
        int rows = 6000;
        Integer[] v = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = rnd.nextInt(8) == 0 ? null : rnd.nextInt(300) * 11;
        }
        try (Arena arena = Arena.ofConfined()) {
            assertInt32(v, decodeDictInt32(v, arena, 5, ColumnChunkDecoderTest::scalarUnpacker));
        }
    }

    // ---------------------------------------------------------------- utf8

    @Test
    void utf8PlainAndDictionary() {
        Random rnd = new Random(4);
        int rows = 4000;
        String[] v = new String[rows];
        String[] domain = {"", "a", "hello", "vecruntime", "\u00e9\u00e8", "a longer string value here"};
        for (int i = 0; i < rows; i++) {
            v[i] = rnd.nextInt(9) == 0 ? null : domain[rnd.nextInt(domain.length)];
        }
        try (Arena arena = Arena.ofConfined()) {
            assertUtf8(v, decodeUtf8(v, arena, 4, false, false)); // PLAIN v1
            assertUtf8(v, decodeUtf8(v, arena, 4, false, true)); // PLAIN v2
            assertUtf8(v, decodeUtf8(v, arena, 4, true, false)); // dictionary v1
        }
    }

    // ================================================================ decode drivers

    private interface FixedStore {
        void store(int index, MemorySegment seg, long byteOffset);
    }

    private static final class DecodeResult {
        final VectorBuffers buffers;

        DecodeResult(VectorBuffers buffers) {
            this.buffers = buffers;
        }
    }

    private DecodeResult decodePlainInt32(Integer[] v, Arena arena, int pageSplits,
            boolean v2) {
        return decodePlainFixed(v, VecType.INT32, arena, pageSplits, v2,
                (i, seg, off) -> seg.set(LE_INT, off, v[i]));
    }

    private DecodeResult decodePlainInt32(Integer[] v, Arena arena, int[] splits,
            boolean v2) {
        return decodePlainFixedSplits(v, VecType.INT32, arena, splits, v2,
                (i, seg, off) -> seg.set(LE_INT, off, v[i]));
    }

    private <T> DecodeResult decodePlainFixed(T[] v, VecType type, Arena arena,
            int pages, boolean v2, FixedStore store) {
        return decodePlainFixedSplits(v, type, arena, evenSplits(v.length, pages), v2,
                store);
    }

    private <T> DecodeResult decodePlainFixedSplits(T[] v, VecType type, Arena arena,
            int[] splits, boolean v2, FixedStore store) {
        int rows = v.length;
        int maxDef = 1;
        MemorySegment outData = ArrowLayout.allocateData(arena, type, rows);
        MemorySegment outValidity = ArrowLayout.allocateBitmap(arena, rows);
        ColumnChunkDecoder d = new ColumnChunkDecoder(type, maxDef, rows, outData, outValidity, null,
                null, scalarFactory());
        int start = 0;
        for (int end : splits) {
            byte[] page = plainFixedPage(v, type, start, end, maxDef, v2,
                    store);
            d.decodePage(v2 ? ColumnChunkDecoder.Page.v2(page, v2LevelLen(v, start, end, maxDef), end - start, ParquetPageDecoder.Encoding.PLAIN) : ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.PLAIN));
            start = end;
        }
        assertEquals(rows, d.rowsWritten());
        VectorBuffers b = SegmentVectorBuffers.fixedWidth(type, rows, anyNull(v) ? outValidity : null,
                outData);
        return new DecodeResult(b);
    }

    private DecodeResult decodeDictInt32(Integer[] v, Arena arena, int pages,
            java.util.function.IntFunction<GroupUnpacker> factory) {
        int rows = v.length;
        int maxDef = 1;
        // Build the dictionary (distinct present values, in first-seen order).
        List<Integer> dict = new ArrayList<>();
        java.util.Map<Integer, Integer> idOf = new java.util.HashMap<>();
        int[] ids = new int[rows];
        for (int i = 0; i < rows; i++) {
            if (v[i] == null) {
                continue;
            }
            Integer id = idOf.get(v[i]);
            if (id == null) {
                id = dict.size();
                idOf.put(v[i], id);
                dict.add(v[i]);
            }
            ids[i] = id;
        }
        MemorySegment dictData = ArrowLayout.allocateData(arena, VecType.INT32, dict.size());
        for (int i = 0; i < dict.size(); i++) {
            dictData.set(LE_INT, (long) i << 2, dict.get(i));
        }
        VectorBuffers dictionary = SegmentVectorBuffers.fixedWidth(VecType.INT32, dict.size(), null, dictData);

        MemorySegment outData = ArrowLayout.allocateData(arena, VecType.INT32, rows);
        MemorySegment outValidity = ArrowLayout.allocateBitmap(arena, rows);
        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.INT32, maxDef, rows, outData, outValidity, null,
                null, factory);
        d.setDictionary(dictionary);
        int start = 0;
        for (int end : evenSplits(rows, pages)) {
            byte[] page = dictPage(v, ids, start, end, maxDef);
            d.decodePage(ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.RLE_DICTIONARY));
            start = end;
        }
        VectorBuffers b = SegmentVectorBuffers.fixedWidth(VecType.INT32, rows, anyNull(v) ? outValidity : null,
                outData);
        return new DecodeResult(b);
    }

    private DecodeResult decodeUtf8(String[] v, Arena arena, int pages,
            boolean dict, boolean v2) {
        int rows = v.length;
        int maxDef = 1;
        MemorySegment outOffsets = ArrowLayout.allocateOffsets(arena, rows);
        MemorySegment outValidity = ArrowLayout.allocateBitmap(arena, rows);
        HeapUtf8Sink sink = new HeapUtf8Sink(arena);
        VectorBuffers dictionary = null;
        int[] ids = new int[rows];
        if (dict) {
            List<String> dl = new ArrayList<>();
            java.util.Map<String, Integer> idOf = new java.util.HashMap<>();
            for (int i = 0; i < rows; i++) {
                if (v[i] == null) {
                    continue;
                }
                Integer id = idOf.get(v[i]);
                if (id == null) {
                    id = dl.size();
                    idOf.put(v[i], id);
                    dl.add(v[i]);
                }
                ids[i] = id;
            }
            dictionary = utf8Buffers(dl.toArray(new String[0]), arena);
        }
        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.UTF8, maxDef, rows, sink.segment(), outValidity,
                outOffsets, sink, scalarFactory());
        if (dict) {
            d.setDictionary(dictionary);
        }
        int start = 0;
        for (int end : evenSplits(rows, pages)) {
            byte[] page = dict ? utf8DictPage(v, ids, start, end, maxDef) : utf8PlainPage(v, start, end, maxDef, v2);
            d.decodePage(v2 && !dict
                    ? ColumnChunkDecoder.Page.v2(page, v2LevelLen(v, start, end, maxDef), end - start, ParquetPageDecoder.Encoding.PLAIN)
                    : ColumnChunkDecoder.Page.v1(page, end - start, dict ? ParquetPageDecoder.Encoding.RLE_DICTIONARY : ParquetPageDecoder.Encoding.PLAIN));
            start = end;
        }
        VectorBuffers b = SegmentVectorBuffers.utf8(rows, anyNull(v) ? outValidity : null, outOffsets,
                sink.segment());
        return new DecodeResult(b);
    }

    // ================================================================ page builders

    private <T> byte[] plainFixedPage(
            T[] v,
            VecType type,
            int start,
            int end,
            int maxDef,
            boolean v2,
            FixedStore store) {
        int width = type.byteWidth();
        byte[] levels = levelBytes(v, start, end, maxDef);
        int present = presentCount(v, start, end);
        MemorySegment vals = MemorySegment.ofArray(new byte[present * width]);
        long off = 0;
        for (int i = start; i < end; i++) {
            if (v[i] != null) {
                store.store(i, vals, off);
                off += width;
            }
        }
        return assemble(levels, vals.toArray(ValueLayout.JAVA_BYTE), maxDef, v2);
    }

    private byte[] dictPage(Integer[] v, int[] ids, int start,
                            int end, int maxDef) {
        byte[] levels = levelBytes(v, start, end, maxDef);
        int[] presentIds = presentValues(v, ids, start, end);
        int idBitWidth = bitWidthForMax(maxId(ids));
        byte[] idStream = RleBitPackingReaderTest.encode(presentIds, idBitWidth, false);
        byte[] vals = new byte[1 + idStream.length];
        vals[0] = (byte) idBitWidth;
        System.arraycopy(idStream, 0, vals, 1, idStream.length);
        return assemble(levels, vals, maxDef, false);
    }

    private byte[] utf8PlainPage(String[] v, int start, int end,
            int maxDef, boolean v2) {
        byte[] levels = levelBytes(v, start, end, maxDef);
        ByteArrayOutputStream vals = new ByteArrayOutputStream();
        for (int i = start; i < end; i++) {
            if (v[i] != null) {
                byte[] b = v[i].getBytes(StandardCharsets.UTF_8);
                writeLeInt(vals, b.length);
                vals.writeBytes(b);
            }
        }
        return assemble(levels, vals.toByteArray(), maxDef, v2);
    }

    private byte[] utf8DictPage(String[] v, int[] ids, int start,
            int end, int maxDef) {
        byte[] levels = levelBytes(v, start, end, maxDef);
        int[] presentIds = presentValues(v, ids, start, end);
        int idBitWidth = bitWidthForMax(maxId(ids));
        byte[] idStream = RleBitPackingReaderTest.encode(presentIds, idBitWidth, false);
        byte[] vals = new byte[1 + idStream.length];
        vals[0] = (byte) idBitWidth;
        System.arraycopy(idStream, 0, vals, 1, idStream.length);
        return assemble(levels, vals, maxDef, false);
    }

    /**
     * v1: int32-LE level byte length, the levels, then the values. v2: the levels
     * then the values, laid out end to end (the header carries the level length).
     */
    private byte[] assemble(byte[] levels, byte[] values, int maxDef,
                            boolean v2) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (maxDef == 0) {
            out.writeBytes(values);
            return out.toByteArray();
        }
        if (v2) {
            out.writeBytes(levels);
            out.writeBytes(values);
            return out.toByteArray();
        }
        writeLeInt(out, levels.length);
        out.writeBytes(levels);
        out.writeBytes(values);
        return out.toByteArray();
    }

    private <T> int v2LevelLen(T[] v, int start, int end,
            int maxDef) {
        return levelBytes(v, start, end, maxDef).length;
    }

    /**
     * Definition levels (1 present, 0 null) as an RLE/bit-packed hybrid at the
     * max-def bit width.
     */
    private <T> byte[] levelBytes(T[] v, int start, int end,
            int maxDef) {
        int n = end - start;
        int[] levels = new int[n];
        for (int i = 0; i < n; i++) {
            levels[i] = v[start + i] == null ? 0 : maxDef;
        }
        return RleBitPackingReaderTest.encode(levels, ParquetPageDecoder.bitWidth(maxDef), false);
    }

    // ================================================================ assertions

    private void assertInt32(Integer[] v, DecodeResult r) {
        for (int i = 0; i < v.length; i++) {
            if (v[i] == null) {
                assertTrue(r.buffers.isNull(i), "row " + i + " should be null");
            } else {
                assertEquals(v[i].intValue(), r.buffers.getInt(i), "row " + i);
            }
        }
    }

    private void assertUtf8(String[] v, DecodeResult r) {
        for (int i = 0; i < v.length; i++) {
            if (v[i] == null) {
                assertTrue(r.buffers.isNull(i), "row " + i + " should be null");
            } else {
                assertEquals(v[i], r.buffers.getString(i), "row " + i);
            }
        }
    }

    // ================================================================ helpers

    private static java.util.function.IntFunction<GroupUnpacker> scalarFactory() {
        return ColumnChunkDecoderTest::scalarUnpacker;
    }

    private static GroupUnpacker scalarUnpacker(int bitWidth) {
        return (src, sp, dst, dp) -> refUnpack8(src, sp, dst, dp, bitWidth);
    }

    private static void refUnpack8(byte[] src, int srcPos, int[] dst,
            int dstPos, int bitWidth) {
        long buf = 0;
        int bits = 0;
        int p = srcPos;
        long mask = bitWidth == 32 ? 0xFFFFFFFFL : ((1L << bitWidth) - 1);
        for (int i = 0; i < 8; i++) {
            while (bits < bitWidth) {
                buf |= ((long) (src[p++] & 0xFF)) << bits;
                bits += 8;
            }
            dst[dstPos + i] = (int) (buf & mask);
            buf >>>= bitWidth;
            bits -= bitWidth;
        }
    }

    private static <T> boolean anyNull(T[] v) {
        for (T x : v) {
            if (x == null) {
                return true;
            }
        }
        return false;
    }

    private static <T> int presentCount(T[] v, int start, int end) {
        int c = 0;
        for (int i = start; i < end; i++) {
            if (v[i] != null) {
                c++;
            }
        }
        return c;
    }

    private static <T> int[] presentValues(T[] v, int[] ids, int start,
            int end) {
        int c = presentCount(v, start, end);
        int[] out = new int[c];
        int o = 0;
        for (int i = start; i < end; i++) {
            if (v[i] != null) {
                out[o++] = ids[i];
            }
        }
        return out;
    }

    private static int maxId(int[] ids) {
        int m = 0;
        for (int id : ids) {
            m = Math.max(m, id);
        }
        return m;
    }

    private static int bitWidthForMax(int maxId) {
        return maxId == 0 ? 1 : (32 - Integer.numberOfLeadingZeros(maxId));
    }

    private static int[] evenSplits(int rows, int pages) {
        int[] out = new int[pages];
        for (int p = 0; p < pages; p++) {
            out[p] = (int) ((long) rows * (p + 1) / pages);
        }
        return out;
    }

    private static int[] splitAt(int rows, int at) {
        return new int[] {at, rows};
    }

    private static void writeLeInt(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 24) & 0xFF);
    }

    private static VectorBuffers utf8Buffers(String[] values, Arena arena) {
        int n = values.length;
        MemorySegment offsets = ArrowLayout.allocateOffsets(arena, n);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        int pos = 0;
        for (int i = 0; i < n; i++) {
            offsets.set(LE_INT, (long) i << 2, pos);
            byte[] b = values[i].getBytes(StandardCharsets.UTF_8);
            data.writeBytes(b);
            pos += b.length;
        }
        offsets.set(LE_INT, (long) n << 2, pos);
        byte[] db = data.toByteArray();
        MemorySegment dseg = ArrowLayout.allocateBytes(arena, Math.max(db.length, 1));
        MemorySegment.copy(MemorySegment.ofArray(db), 0, dseg, 0, db.length);
        return SegmentVectorBuffers.utf8(n, null, offsets, dseg);
    }

    /**
     * A growable native UTF8 sink for the tests (the node has its own reusable
     * one).
     */
    private static final class HeapUtf8Sink implements ColumnChunkDecoder.Utf8Sink {
        private final Arena arena;
        private MemorySegment buf;
        private long len;

        HeapUtf8Sink(Arena arena) {
            this.arena = arena;
            this.buf = arena.allocate(1 << 16);
        }

        @Override
        public void append(MemorySegment src, long srcOffset, int n) {
            if (len + n > buf.byteSize()) {
                long cap = Math.max(len + n, buf.byteSize() * 2);
                MemorySegment bigger = arena.allocate(cap);
                MemorySegment.copy(buf, 0, bigger, 0, len);
                buf = bigger;
            }
            MemorySegment.copy(src, srcOffset, buf, len, n);
            len += n;
        }

        @Override
        public long length() {
            return len;
        }

        @Override
        public MemorySegment segment() {
            return buf;
        }

        @Override
        public void reset() {
            len = 0;
        }
    }
}
