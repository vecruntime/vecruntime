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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.kernels.parquet.ParquetPageDecoder.Encoding;
import io.vecruntime.kernels.parquet.ParquetPageDecoder.Page;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scalar-oracle tests for {@link ParquetPageDecoder}. Pages are built here from
 * known values (the oracle is the input array); PLAIN and RLE_DICTIONARY, page
 * v1 and v2 level layouts, all-null / no-null / mixed nulls, empty strings, and
 * a dictionary page followed by a PLAIN page over the same column. A separate
 * spark/ test cross-checks against parquet-java's own writers.
 */
class ParquetPageDecoderTest {

    // --------------------------------------------------------- page byte builders

    /**
     * Writes the RLE/bit-packed def-level stream for `present` at the given max
     * level.
     */
    private static byte[] levelBytes(boolean[] present, int maxDefLevel) {
        int[] levels = new int[present.length];
        for (int i = 0; i < present.length; i++) {
            levels[i] = present[i] ? maxDefLevel : 0;
        }
        int bitWidth = ParquetPageDecoder.bitWidth(maxDefLevel);
        return RleBitPackingReaderTest.encode(levels, bitWidth, false);
    }

    private static void writeIntLe(ByteArrayOutputStream out, int v) {
        for (int b = 0; b < 4; b++) {
            out.write((v >>> (8 * b)) & 0xFF);
        }
    }

    private static void writeLongLe(ByteArrayOutputStream out, long v) {
        for (int b = 0; b < 8; b++) {
            out.write((int) ((v >>> (8 * b)) & 0xFF));
        }
    }

    /** PLAIN value bytes for the present slots of a fixed-width column. */
    private static byte[] plainFixed(long[] values, boolean[] present, VecType type) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < values.length; i++) {
            if (!present[i]) {
                continue;
            }
            switch (type) {
                case INT32 -> writeIntLe(out, (int) values[i]);
                case INT64, FLOAT64 -> writeLongLe(out, values[i]);
                default -> throw new IllegalArgumentException();
            }
        }
        return out.toByteArray();
    }

    /**
     * PLAIN value bytes for the present slots of a BINARY column (int32 length
     * prefix + bytes).
     */
    private static byte[] plainBinary(byte[][] values, boolean[] present) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < values.length; i++) {
            if (!present[i]) {
                continue;
            }
            writeIntLe(out, values[i].length);
            out.writeBytes(values[i]);
        }
        return out.toByteArray();
    }

    /**
     * RLE_DICTIONARY value bytes: 1 byte id-bit-width, then the hybrid id
     * stream for present slots.
     */
    private static byte[] dictIds(int[] ids, boolean[] present, int idBitWidth) {
        List<Integer> live = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            if (present[i]) {
                live.add(ids[i]);
            }
        }
        int[] arr = live.stream()
                .mapToInt(Integer::intValue)
                .toArray();
        byte[] stream = RleBitPackingReaderTest.encode(arr, idBitWidth, false);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(idBitWidth);
        out.writeBytes(stream);
        return out.toByteArray();
    }

    /**
     * Assembles a v1 page from level bytes (or none) and value bytes into one
     * segment.
     */
    private static Page v1Page(Arena a, byte[] valueBytes, boolean[] present,
            int maxDefLevel, Encoding enc) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (maxDefLevel > 0) {
            byte[] lvl = levelBytes(present, maxDefLevel);
            writeIntLe(out, lvl.length);
            out.writeBytes(lvl);
        }
        out.writeBytes(valueBytes);
        byte[] all = out.toByteArray();
        MemorySegment seg = a.allocate(all.length + 8L);
        MemorySegment.copy(MemorySegment.ofArray(all), 0, seg, 0, all.length);
        return Page.v1(seg, 0, all.length, present.length, maxDefLevel, enc);
    }

    /** Assembles a v2 page: separate level and value slices in one segment. */
    private static Page v2Page(Arena a, byte[] valueBytes, boolean[] present,
            int maxDefLevel, Encoding enc) {
        byte[] lvl = maxDefLevel > 0 ? levelBytes(present, maxDefLevel) : new byte[0];
        MemorySegment seg = a.allocate(lvl.length + (long) valueBytes.length + 8L);
        MemorySegment.copy(MemorySegment.ofArray(lvl), 0, seg, 0, lvl.length);
        MemorySegment.copy(MemorySegment.ofArray(valueBytes), 0, seg, lvl.length, valueBytes.length);
        return Page.v2(seg, 0, lvl.length, lvl.length, valueBytes.length, present.length,
                maxDefLevel, enc);
    }

    private static int idBitWidth(int maxId) {
        return maxId == 0 ? 1 : 32 - Integer.numberOfLeadingZeros(maxId);
    }

    // --------------------------------------------------------- assertions

    private static void assertFixed(VectorBuffers vb, long[] expected, boolean[] present,
            VecType type) {
        assertEquals(expected.length, vb.length());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(!present[i], vb.isNull(i), "null at " + i);
            if (present[i]) {
                switch (type) {
                    case INT32 -> assertEquals((int) expected[i], vb.getInt(i),
                            "value " + i);
                    case INT64 -> assertEquals(expected[i], vb.getLong(i), "value " + i);
                    case FLOAT64 -> assertEquals(Double.longBitsToDouble(expected[i]),
                            vb.getDouble(i), "value " + i);
                    default -> throw new IllegalArgumentException();
                }
            }
        }
    }

    private static void assertBinary(VectorBuffers vb, byte[][] expected, boolean[] present) {
        assertEquals(expected.length, vb.length());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(!present[i], vb.isNull(i), "null at " + i);
            if (present[i]) {
                assertArrayEquals(expected[i], vb.getUtf8Bytes(i), "bytes " + i);
            }
        }
    }

    // --------------------------------------------------------- fixed-width tests

    @Test
    void plainInt32NoNulls() {
        long[] v = {1, -2, 3, 2_000_000_000L, -1};
        boolean[] p = {true, true, true, true, true};
        for (boolean v2 : new boolean[] {false, true}) {
            try (Arena a = Arena.ofConfined()) {
                Page page = v2 ? v2Page(a, plainFixed(v, p, VecType.INT32), p, 0, Encoding.PLAIN) : v1Page(a, plainFixed(v, p, VecType.INT32), p, 0, Encoding.PLAIN);
                VectorBuffers vb = ParquetPageDecoder.decode(page, VecType.INT32, null, a);
                assertFalse(vb.hasNulls());
                assertFixed(vb, v, p, VecType.INT32);
            }
        }
    }

    @Test
    void plainInt64WithNullsV1andV2() {
        long[] v = {10L, 0L, 30L, 0L, 50L, 60L,
                0L, 80L};
        boolean[] p = {true, false, true, false, true, true,
                false, true};
        for (boolean v2 : new boolean[] {false, true}) {
            try (Arena a = Arena.ofConfined()) {
                Page page = v2 ? v2Page(a, plainFixed(v, p, VecType.INT64), p, 1, Encoding.PLAIN) : v1Page(a, plainFixed(v, p, VecType.INT64), p, 1, Encoding.PLAIN);
                VectorBuffers vb = ParquetPageDecoder.decode(page, VecType.INT64, null, a);
                assertTrue(vb.hasNulls());
                assertFixed(vb, v, p, VecType.INT64);
            }
        }
    }

    @Test
    void plainDoubleAllNull() {
        long[] v = new long[16];
        boolean[] p = new boolean[16]; // all false
        try (Arena a = Arena.ofConfined()) {
            Page page = v1Page(a, plainFixed(v, p, VecType.FLOAT64), p, 1, Encoding.PLAIN);
            VectorBuffers vb = ParquetPageDecoder.decode(page, VecType.FLOAT64, null, a);
            assertEquals(16, vb.length());
            assertEquals(16, vb.nullCount());
        }
    }

    @Test
    void plainDoubleValues() {
        double[] d = {1.5, -0.0, Math.PI, 1e300, Double.NaN};
        long[] v = new long[d.length];
        for (int i = 0; i < d.length; i++) {
            v[i] = Double.doubleToRawLongBits(d[i]);
        }
        boolean[] p = {true, true, true, true, true};
        try (Arena a = Arena.ofConfined()) {
            Page page = v2Page(a, plainFixed(v, p, VecType.FLOAT64), p, 0, Encoding.PLAIN);
            VectorBuffers vb = ParquetPageDecoder.decode(page, VecType.FLOAT64, null, a);
            for (int i = 0; i < d.length; i++) {
                assertEquals(d[i], vb.getDouble(i), 0.0);
            }
        }
    }

    @Test
    void dictionaryInt64WithNulls() {
        long[] dict = {100L, 200L, 300L, 400L};
        int[] ids = {0, 3, 1, 2, 0, 0,
                2, 3};
        boolean[] p = {true, true, false, true, true, false,
                true, true};
        long[] expected = new long[ids.length];
        for (int i = 0; i < ids.length; i++) {
            expected[i] = dict[ids[i]];
        }
        try (Arena a = Arena.ofConfined()) {
            VectorBuffers dictVb = plainFixedDict(a, dict, VecType.INT64);
            byte[] valueBytes = dictIds(ids, p, idBitWidth(dict.length - 1));
            for (boolean v2 : new boolean[] {false, true}) {
                Page page = v2 ? v2Page(a, valueBytes, p, 1, Encoding.RLE_DICTIONARY) : v1Page(a, valueBytes, p, 1, Encoding.RLE_DICTIONARY);
                VectorBuffers vb = ParquetPageDecoder.decode(page, VecType.INT64, dictVb, a);
                assertFixed(vb, expected, p, VecType.INT64);
            }
        }
    }

    // --------------------------------------------------------- binary / utf8 tests

    @Test
    void plainBinaryWithNullsAndEmpties() {
        byte[][] v = {"hello".getBytes(StandardCharsets.UTF_8), new byte[0], // empty string, present
        null,
                "worldwide".getBytes(StandardCharsets.UTF_8), "x".getBytes(StandardCharsets.UTF_8)};
        boolean[] p = {true, true, false, true, true};
        for (boolean v2 : new boolean[] {false, true}) {
            try (Arena a = Arena.ofConfined()) {
                byte[][] present = v.clone();
                Page page = v2 ? v2Page(a, plainBinary(fill(v, p), p), p, 1, Encoding.PLAIN) : v1Page(a, plainBinary(fill(v, p), p), p, 1, Encoding.PLAIN);
                VectorBuffers vb = ParquetPageDecoder.decode(page, VecType.UTF8, null, a);
                assertBinary(vb, present, p);
            }
        }
    }

    @Test
    void dictionaryStringsThenPlainSameColumn() {
        // Page 1: dictionary-encoded; page 2: PLAIN. The decoder decodes each independently, which is
        // exactly the dictionary->PLAIN fallback mid-column at the page level.
        byte[][] dict = {"apple".getBytes(StandardCharsets.UTF_8), "banana".getBytes(StandardCharsets.UTF_8),
                "cherry".getBytes(StandardCharsets.UTF_8)};
        int[] ids = {2, 0, 1, 1, 2};
        boolean[] p1 = {true, true, true, false, true};
        byte[][] expected1 = new byte[ids.length][];
        for (int i = 0; i < ids.length; i++) {
            expected1[i] = p1[i] ? dict[ids[i]] : null;
        }
        byte[][] plain = {"date".getBytes(StandardCharsets.UTF_8), null, "".getBytes(StandardCharsets.UTF_8),
                "fig".getBytes(StandardCharsets.UTF_8)};
        boolean[] p2 = {true, false, true, true};
        try (Arena a = Arena.ofConfined()) {
            // dictionary page (PLAIN encoded) -> VectorBuffers
            byte[] dictPageBytes = plainBinary(dict, allTrue(dict.length));
            MemorySegment dseg = a.allocate(dictPageBytes.length + 8L);
            MemorySegment.copy(MemorySegment.ofArray(dictPageBytes),
                    0, dseg, 0, dictPageBytes.length);
            VectorBuffers dictVb = ParquetPageDecoder.decodeDictionary(dseg, 0, dictPageBytes.length, dict.length, VecType.UTF8, a);

            Page dictPage = v1Page(a, dictIds(ids, p1, idBitWidth(dict.length - 1)), p1, 1,
                    Encoding.RLE_DICTIONARY);
            VectorBuffers vb1 = ParquetPageDecoder.decode(dictPage, VecType.UTF8, dictVb, a);
            assertBinary(vb1, expected1, p1);

            Page plainPage = v1Page(a, plainBinary(fill(plain, p2), p2), p2, 1, Encoding.PLAIN);
            VectorBuffers vb2 = ParquetPageDecoder.decode(plainPage, VecType.UTF8, null, a);
            assertBinary(vb2, plain, p2);
        }
    }

    // --------------------------------------------------------- helpers

    private static boolean[] allTrue(int n) {
        boolean[] p = new boolean[n];
        java.util.Arrays.fill(p, true);
        return p;
    }

    /**
     * Replaces null entries with empty arrays so plainBinary never dereferences
     * a null (nulls are skipped anyway).
     */
    private static byte[][] fill(byte[][] v, boolean[] present) {
        byte[][] out = v.clone();
        for (int i = 0; i < out.length; i++) {
            if (!present[i]) {
                out[i] = new byte[0];
            }
        }
        return out;
    }

    private static VectorBuffers plainFixedDict(Arena a, long[] dict, VecType type) {
        byte[] bytes = plainFixed(dict, allTrue(dict.length), type);
        MemorySegment seg = a.allocate(bytes.length + 8L);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, seg, 0, bytes.length);
        return ParquetPageDecoder.decodeDictionary(seg, 0, bytes.length, dict.length, type, a);
    }
}
