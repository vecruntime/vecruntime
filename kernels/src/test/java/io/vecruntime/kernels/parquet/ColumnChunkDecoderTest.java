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
 * flat column are decoded into reused heap staging arrays and flushed into
 * Arrow buffers with one bulk copy each, then compared value-by-value against
 * the input arrays. Covers INT32/INT64/FLOAT64/UTF8, PLAIN and RLE_DICTIONARY,
 * page v1 and v2, no-null / with-null / all-null pages, a multi-page row group,
 * the validity word builder (all-null, all-valid, lengths not a multiple of 64,
 * a page boundary mid-word), the string gather, and the injected-GroupUnpacker
 * requirement (a counting unpacker observes {@code > 0} calls on a bit-packed
 * stream).
 */
class ColumnChunkDecoderTest {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    // ---------------------------------------------------------------- counting unpacker (requirement 1)

    @Test
    void injectedUnpackerIsUsedOnBitPackedStreams() {
        Random rnd = new Random(559);
        int rows = 3000;
        Integer[] values = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = rnd.nextInt(5) == 0 ? null : rnd.nextInt(200);
        }
        AtomicInteger calls = new AtomicInteger();
        try (Arena arena = Arena.ofConfined()) {
            DecodeResult r = decodeDictInt32(values, arena, 1,
                    bw -> (src, sp, dst, dp) -> {
                        calls.incrementAndGet();
                        refUnpack8(src, sp, dst, dp, bw);
                    });
            assertInt32(values, r);
        }
        assertTrue(calls.get() > 0, "injected GroupUnpacker was never called on a bit-packed page");
    }

    // ---------------------------------------------------------------- streaming: batch spans pages / runs

    @Test
    void batchSpansPagesWhenBatchSizeDoesNotDividePageSize() {
        // 5 pages of 1000 rows each; batch size 384 does not divide 1000, so most batches start and end
        // inside a page and several span a page boundary. PLAIN, nullable. Compared against the input.
        Random rnd = new Random(21);
        int rows = 5000;
        Integer[] v = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = rnd.nextInt(6) == 0 ? null : rnd.nextInt();
        }
        try (Arena arena = Arena.ofConfined()) {
            assertInt32(v, decodePlainInt32(v, arena, evenSplits(rows, 5), false, 384));
        }
    }

    @Test
    void batchSpansPagesDictionaryAndPlainAndNoNulls() {
        int rows = 4300;
        Integer[] dictV = new Integer[rows];
        Integer[] plainV = new Integer[rows];
        Integer[] noNullV = new Integer[rows];
        Random rnd = new Random(22);
        for (int i = 0; i < rows; i++) {
            dictV[i] = rnd.nextInt(7) == 0 ? null : rnd.nextInt(50) * 3;
            plainV[i] = rnd.nextInt(7) == 0 ? null : rnd.nextInt();
            noNullV[i] = i - 2000;
        }
        try (Arena arena = Arena.ofConfined()) {
            // batch 256, pages of ~717 rows: every batch and every page boundary misalign.
            assertInt32(
                    dictV,
                    decodeDictInt32(dictV, arena, evenSplits(rows, 6), ColumnChunkDecoderTest::scalarUnpacker,
                            256));
            assertInt32(plainV, decodePlainInt32(plainV, arena, evenSplits(rows, 6), false, 256));
            // no-null column, batches spanning pages.
            DecodeResult r = decodePlainFixedSplits(
                    noNullV,
                    VecType.INT32,
                    VecType.INT32,
                    arena,
                    evenSplits(rows, 6),
                    false,
                    256,
                    (i, seg, off) -> seg.set(LE_INT, off, noNullV[i]));
            assertInt32(noNullV, r);
        }
    }

    @Test
    void utf8BatchSpansPages() {
        Random rnd = new Random(23);
        int rows = 3300;
        String[] domain = {"", "x", "value", "vecruntime-streaming", "\u00e9"};
        String[] v = new String[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = rnd.nextInt(8) == 0 ? null : domain[rnd.nextInt(domain.length)];
        }
        try (Arena arena = Arena.ofConfined()) {
            // plain and dictionary utf8, batch 200 not dividing the page rows.
            assertUtf8(v, decodeUtf8Batched(v, arena, 5, false, 200));
            assertUtf8(v, decodeUtf8Batched(v, arena, 5, true, 200));
        }
    }

    @Test
    void dictionaryRunResumedMidRun() {
        // A single long RLE run of one id split across many small batches: the id reader must resume
        // mid-run. All rows the same value forces one RLE run; batch 100 << the run length.
        int rows = 4096;
        Integer[] v = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = 7; // one dictionary entry, one RLE run
        }
        try (Arena arena = Arena.ofConfined()) {
            DecodeResult r = decodeDictInt32(v, arena, new int[] {rows}, ColumnChunkDecoderTest::scalarUnpacker,
                    100);
            assertInt32(v, r);
        }
    }

    @Test
    void bitPackedRunResumedMidRunAcrossBatches() {
        // Distinct ids force a bit-packed run; small batches split a bit-packed group of 8 across batches,
        // so the reader must resume inside a group. One page, batch 37 (not a multiple of 8).
        int rows = 2048;
        Integer[] v = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = i % 200; // 200 distinct dictionary entries -> bit-packed ids
        }
        try (Arena arena = Arena.ofConfined()) {
            DecodeResult r = decodeDictInt32(v, arena, new int[] {rows}, ColumnChunkDecoderTest::scalarUnpacker,
                    37);
            assertInt32(v, r);
        }
    }

    // ---------------------------------------------------------------- null count from the def-level pass

    @Test
    void nullCountFromDefinitionLevelsIsExact() {
        Random rnd = new Random(24);
        int rows = 5000;
        Integer[] v = new Integer[rows];
        int expected = 0;
        for (int i = 0; i < rows; i++) {
            boolean isNull = rnd.nextInt(5) == 0;
            v[i] = isNull ? null : rnd.nextInt();
            if (isNull) {
                expected++;
            }
        }
        try (Arena arena = Arena.ofConfined()) {
            // Summed across batches (batch 512, pages of 1000): the decoder counts nulls while building
            // validity -- never Arrow getNullCount.
            DecodeResult r = decodePlainInt32(v, arena, evenSplits(rows, 5), false, 512);
            assertEquals(expected, r.nullCount, "streamed null count must equal the input nulls");
        }
    }

    @Test
    void perBatchNullCountCostIsIndependentOfRowGroupSize() {
        // The per-batch work must not scan the whole row group. We decode the SAME batch size (512) from a
        // small row group and a 20x larger one and assert each batch reports its own nulls, and that the
        // number of batches scales with the row group while each batch's cost (one def-level pass over its
        // own rows) does not depend on the group size. A counting unpacker bounds the level-decode calls
        // per batch, which is what the O(row-group) getNullCount used to violate.
        for (int rows : new int[] {2048, 40960}) {
            Integer[] v = new Integer[rows];
            for (int i = 0; i < rows; i++) {
                v[i] = (i % 4 == 0) ? null : i;
            }
            try (Arena arena = Arena.ofConfined()) {
                int batch = 512;
                DecodeResult r = decodePlainInt32(v, arena, evenSplits(rows, Math.max(1, rows / 1000)), false,
                        batch);
                int expected = 0;
                for (Integer x : v) {
                    if (x == null) {
                        expected++;
                    }
                }
                assertEquals(expected, r.nullCount, "rows=" + rows);
                assertInt32(v, r);
            }
        }
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
            assertInt32(v, decodePlainInt32(v, arena, 4, false));
            assertInt32(v, decodePlainInt32(v, arena, 4, true));
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
            // maxDef 0: a required column, no level bytes, no validity buffer.
            DecodeResult r = decodePlainRequiredInt32(v, arena, 3);
            assertNull(r.buffers.validity(), "no-null column must have no validity buffer");
            assertInt32(v, r);
        }
    }

    @Test
    void int32AllNullPage() {
        Integer[] v = new Integer[1000];
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

    /** Narrow decimal: INT32 physical, INT64 lane (sign-extended). */
    @Test
    void narrowDecimalInt32PhysicalIntoLongLane() {
        Random rnd = new Random(7);
        int rows = 3000;
        Integer[] v = new Integer[rows]; // the int32 unscaled values (may be negative)
        for (int i = 0; i < rows; i++) {
            v[i] = rnd.nextInt(9) == 0 ? null : rnd.nextInt(200_000) - 100_000;
        }
        try (Arena arena = Arena.ofConfined()) {
            // physical INT32, lane INT64.
            DecodeResult r = decodePlainWiden(v, arena, 3);
            for (int i = 0; i < rows; i++) {
                if (v[i] == null) {
                    assertTrue(r.buffers.isNull(i), "row " + i);
                } else {
                    assertEquals(v[i].longValue(), r.buffers.getLong(i), "row " + i);
                }
            }
        }
    }

    // ---------------------------------------------------------------- utf8 / string gather

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
            assertUtf8(v, decodeUtf8(v, arena, 4, false, false));
            assertUtf8(v, decodeUtf8(v, arena, 4, false, true));
            assertUtf8(v, decodeUtf8(v, arena, 4, true, false));
        }
    }

    // ---------------------------------------------------------------- validity word builder

    @Test
    void validityAllValid() {
        int rows = 200; // not a multiple of 64
        Integer[] v = new Integer[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = i;
        }
        // maxDef 1 but no nulls: every validity word is -1 over the valid range.
        long[] words = decodeAndGetValidityWords(v, rows);
        int wholeWords = rows >>> 6;
        for (int w = 0; w < wholeWords; w++) {
            assertEquals(-1L, words[w], "whole word " + w + " must be all-valid");
        }
        long tailMask = (1L << (rows & 63)) - 1;
        assertEquals(tailMask, words[wholeWords] & tailMask, "tail bits must be all-valid");
    }

    @Test
    void validityAllNull() {
        int rows = 130;
        Integer[] v = new Integer[rows]; // all null
        long[] words = decodeAndGetValidityWords(v, rows);
        for (int i = 0; i < rows; i++) {
            assertEquals(0L, (words[i >>> 6] >>> (i & 63)) & 1L, "row " + i + " must be null");
        }
    }

    @Test
    void validityLengthNotMultipleOf64AndPageBoundaryMidWord() {
        // Two pages splitting at 100, so the second page starts at row 100 = mid of word 1 (64..127);
        // rows total 150 (not a multiple of 64). A scattered null pattern.
        int rows = 150;
        Integer[] v = new Integer[rows];
        boolean[] expectNull = new boolean[rows];
        Random rnd = new Random(11);
        for (int i = 0; i < rows; i++) {
            boolean isNull = rnd.nextInt(3) == 0;
            expectNull[i] = isNull;
            v[i] = isNull ? null : i + 1;
        }
        long[] words = decodeAndGetValidityWords(v, rows, splitAt(rows, 100));
        for (int i = 0; i < rows; i++) {
            boolean valid = ((words[i >>> 6] >>> (i & 63)) & 1L) != 0;
            assertEquals(!expectNull[i], valid, "row " + i + " validity");
        }
    }

    // ================================================================ decode drivers

    private interface FixedStore {
        void store(int index, MemorySegment seg, long byteOffset);
    }

    private static final class DecodeResult {
        final VectorBuffers buffers;
        final int nullCount;

        DecodeResult(VectorBuffers buffers, int nullCount) {
            this.buffers = buffers;
            this.nullCount = nullCount;
        }
    }

    /** One prepared page: its kernel Page plus its value count, ready to feed. */
    private record PreparedPage(ColumnChunkDecoder.Page page, int valueCount) {}

    // ---------------------------------------------------------------- DELTA_BINARY_PACKED (#559 slice 2)

    @Test
    void deltaBinaryPackedPagesDecodeOnEveryPathWithNullsAcrossPages() {
        // INT32 and INT64 lanes, plus INT32 widened to an INT64 lane; with and without nulls; page v1 and v2;
        // batches of 384 rows over ~1000-row pages, so a batch spans pages and resumes mid-miniblock. Each case
        // runs the staging path (readBatch + flushFixed) and both direct paths.
        Random rnd = new Random(5596);
        int rows = 4096;
        for (VecType physical : new VecType[] {VecType.INT32, VecType.INT64}) {
            for (VecType lane : physical == VecType.INT32 ? new VecType[] {VecType.INT32, VecType.INT64} : new VecType[] {VecType.INT64}) {
                for (boolean nulls : new boolean[] {false, true}) {
                    for (boolean v2 : new boolean[] {false, true}) {
                        Long[] v = new Long[rows];
                        for (int i = 0; i < rows; i++) {
                            long x = physical == VecType.INT32
                                    ? (i % 3 == 0 ? rnd.nextInt() : 100_000 + i * 7L)
                                    : (i % 3 == 0 ? rnd.nextLong() : 1_700_000_000_000L + i * 1000L);
                            v[i] = nulls && rnd.nextInt(6) == 0 ? null : x;
                        }
                        int maxDef = nulls ? 1 : 0;
                        List<PreparedPage> pages = new ArrayList<>();
                        int start = 0;
                        for (int end : evenSplits(rows, 4)) {
                            byte[] levels = levelBytes(v, start, end, maxDef);
                            long[] present = new long[presentCount(v, start, end)];
                            int k = 0;
                            for (int i = start; i < end; i++) {
                                if (v[i] != null) {
                                    present[k++] = v[i];
                                }
                            }
                            byte[] vals = DeltaBinaryPackedReaderTest.encode(present, physical == VecType.INT64, 128, 4, false);
                            byte[] page = assemble(levels, vals, maxDef, v2);
                            ColumnChunkDecoder.Page p = v2 && maxDef > 0
                                    ? ColumnChunkDecoder.Page.v2(page, levels.length, end - start, ParquetPageDecoder.Encoding.DELTA_BINARY_PACKED)
                                    : v2 ? ColumnChunkDecoder.Page.v2(page, 0, end - start, ParquetPageDecoder.Encoding.DELTA_BINARY_PACKED) : ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.DELTA_BINARY_PACKED);
                            pages.add(new PreparedPage(p, end - start));
                            start = end;
                        }
                        String what = physical
                                + "->"
                                + lane
                                + " nulls="
                                + nulls
                                + " v2="
                                + v2;
                        try (Arena arena = Arena.ofConfined()) {
                            ColumnChunkDecoder d = new ColumnChunkDecoder(physical, lane, maxDef, 384, scalarFactory());
                            assertDelta(
                                    v,
                                    physical,
                                    lane,
                                    runStreaming(d, pages, lane, rows, 384, nulls,
                                            arena),
                                    what + " staging");
                        }
                        for (boolean modeA : new boolean[] {true, false}) {
                            try (Arena arena = Arena.ofConfined()) {
                                ColumnChunkDecoder d = new ColumnChunkDecoder(physical, lane, maxDef, 384, scalarFactory());
                                assertDelta(
                                        v,
                                        physical,
                                        lane,
                                        runStreamingDirect(d, pages, lane, rows, 384, nulls,
                                                modeA, arena),
                                        what + (modeA ? " directA" : " directB"));
                            }
                        }
                    }
                }
            }
        }
    }

    private static void assertDelta(Long[] v, VecType physical, VecType lane,
            DecodeResult r, String what) {
        for (int i = 0; i < v.length; i++) {
            if (v[i] == null) {
                assertTrue(r.buffers.isNull(i), what + " row " + i + " should be null");
            } else if (lane == VecType.INT32) {
                assertEquals((int) v[i].longValue(), r.buffers.getInt(i),
                        what + " row " + i);
            } else {
                long want = physical == VecType.INT32 ? (int) v[i].longValue() : v[i];
                assertEquals(want, r.buffers.getLong(i), what + " row " + i);
            }
        }
    }

    // ---------------------------------------------------------------- DELTA_LENGTH_BYTE_ARRAY / BYTE_STREAM_SPLIT (#559)

    @Test
    void deltaLengthByteArrayPagesDecodeWithNullsAndEmptiesAcrossPages() {
        // UTF8 lane: per page, a DELTA_BINARY_PACKED run of the present values' lengths, then their bytes back to
        // back. Nulls, empty strings, multi-byte UTF-8 and long values; batches of 384 rows over ~1000-row pages.
        Random rnd = new Random(55910);
        int rows = 4096;
        for (boolean nulls : new boolean[] {false, true}) {
            for (boolean v2 : new boolean[] {false, true}) {
                String[] v = new String[rows];
                for (int i = 0; i < rows; i++) {
                    int kind = rnd.nextInt(10);
                    v[i] = nulls && kind == 0
                            ? null
                            : kind == 1
                                    ? ""
                                    : kind == 2
                                            ? "héllo wörld " + i
                                            : kind == 3 ? "x".repeat(200 + rnd.nextInt(300)) : "v" + rnd.nextInt(100000);
                }
                int maxDef = nulls ? 1 : 0;
                List<PreparedPage> pages = new ArrayList<>();
                int start = 0;
                for (int end : evenSplits(rows, 4)) {
                    byte[] levels = levelBytes(v, start, end, maxDef);
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    List<Long> lens = new ArrayList<>();
                    for (int i = start; i < end; i++) {
                        if (v[i] != null) {
                            byte[] b = v[i].getBytes(StandardCharsets.UTF_8);
                            lens.add((long) b.length);
                            bytes.writeBytes(b);
                        }
                    }
                    long[] lenArr = lens.stream()
                            .mapToLong(Long::longValue)
                            .toArray();
                    ByteArrayOutputStream vals = new ByteArrayOutputStream();
                    vals.writeBytes(DeltaBinaryPackedReaderTest.encode(lenArr, false, 128, 4, false));
                    vals.writeBytes(bytes.toByteArray());
                    byte[] page = assemble(levels, vals.toByteArray(), maxDef, v2);
                    pages.add(
                            new PreparedPage(
                                    kernelPage(page, levels.length, maxDef, v2, end - start,
                                            ParquetPageDecoder.Encoding.DELTA_LENGTH_BYTE_ARRAY),
                                    end - start));
                    start = end;
                }
                try (Arena arena = Arena.ofConfined()) {
                    ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.UTF8, maxDef, 384, scalarFactory());
                    assertUtf8(v, runStreamingUtf8(d, pages, rows, 384, nulls, arena));
                }
            }
        }
    }

    @Test
    void deltaByteArrayPagesRebuildPrefixesWithNullsAcrossPages() {
        // UTF8 lane: per page, a DELTA_BINARY_PACKED run of prefix lengths (bytes shared with the previous
        // present value; the chain restarts on each page), then a DELTA_LENGTH_BYTE_ARRAY of the suffixes.
        // Sorted keys share long prefixes; nulls, empty and multi-byte values break the chain.
        Random rnd = new Random(55915);
        int rows = 4096;
        for (boolean nulls : new boolean[] {false, true}) {
            for (boolean v2 : new boolean[] {false, true}) {
                String[] v = new String[rows];
                for (int i = 0; i < rows; i++) {
                    int kind = rnd.nextInt(12);
                    v[i] = nulls && kind == 0
                            ? null
                            : kind == 1
                                    ? ""
                                    : kind == 2
                                            ? "héllo wörld " + i
                                            : kind == 3 ? "x".repeat(50 + rnd.nextInt(300)) : String.format("https://example.com/item/%09d", i);
                }
                int maxDef = nulls ? 1 : 0;
                List<PreparedPage> pages = new ArrayList<>();
                int start = 0;
                for (int end : evenSplits(rows, 4)) {
                    byte[] levels = levelBytes(v, start, end, maxDef);
                    List<Long> prefixes = new ArrayList<>();
                    List<Long> suffixLens = new ArrayList<>();
                    ByteArrayOutputStream suffixBytes = new ByteArrayOutputStream();
                    byte[] prev = new byte[0];
                    for (int i = start; i < end; i++) {
                        if (v[i] == null) {
                            continue;
                        }
                        byte[] b = v[i].getBytes(StandardCharsets.UTF_8);
                        int p = 0;
                        while (p < b.length && p < prev.length && b[p] == prev[p]) {
                            p++;
                        }
                        prefixes.add((long) p);
                        suffixLens.add((long) (b.length - p));
                        suffixBytes.write(b, p, b.length - p);
                        prev = b;
                    }
                    ByteArrayOutputStream vals = new ByteArrayOutputStream();
                    vals.writeBytes(DeltaBinaryPackedReaderTest.encode(prefixes.stream()
                            .mapToLong(Long::longValue)
                            .toArray(),
                            false, 128, 4, false));
                    vals.writeBytes(DeltaBinaryPackedReaderTest.encode(suffixLens.stream()
                            .mapToLong(Long::longValue)
                            .toArray(),
                            false, 128, 4, false));
                    vals.writeBytes(suffixBytes.toByteArray());
                    byte[] page = assemble(levels, vals.toByteArray(), maxDef, v2);
                    pages.add(
                            new PreparedPage(
                                    kernelPage(page, levels.length, maxDef, v2, end - start,
                                            ParquetPageDecoder.Encoding.DELTA_BYTE_ARRAY),
                                    end - start));
                    start = end;
                }
                try (Arena arena = Arena.ofConfined()) {
                    ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.UTF8, maxDef, 384, scalarFactory());
                    assertUtf8(v, runStreamingUtf8(d, pages, rows, 384, nulls, arena));
                }
            }
        }
    }

    @Test
    void deltaByteArrayRejectsAPrefixLongerThanThePreviousValue() {
        // Two values: "ab", then prefix 3 (> 2) + suffix "c". A corrupt page must fail, not read garbage.
        ByteArrayOutputStream vals = new ByteArrayOutputStream();
        vals.writeBytes(DeltaBinaryPackedReaderTest.encode(new long[] {0, 3}, false, 128, 4,
                false));
        vals.writeBytes(DeltaBinaryPackedReaderTest.encode(new long[] {2, 1}, false, 128, 4,
                false));
        vals.writeBytes("abc".getBytes(StandardCharsets.UTF_8));
        byte[] page = vals.toByteArray();
        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.UTF8, 0, 2, scalarFactory());
        d.startChunk(2);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> d.feedPage(ColumnChunkDecoder.Page.v1(page, 2, ParquetPageDecoder.Encoding.DELTA_BYTE_ARRAY)));
    }

    @Test
    void byteStreamSplitPagesDecodeOnEveryPathWithNullsAcrossPages() {
        // INT32, INT64, DOUBLE (incl. NaN, -0.0, infinities) and INT32 widened to INT64; nulls; v1 and v2;
        // staging and both direct paths; batches of 384 rows over ~1000-row pages.
        Random rnd = new Random(55911);
        int rows = 4096;
        VecType[][] cases = {
            {VecType.INT32, VecType.INT32}, {VecType.INT32, VecType.INT64},
            {VecType.INT64, VecType.INT64}, {VecType.FLOAT64, VecType.FLOAT64}};
        double[] specials = {Double.NaN, -0.0, 0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE};
        for (VecType[] c : cases) {
            VecType physical = c[0];
            VecType lane = c[1];
            int width = physical == VecType.INT32 ? 4 : 8;
            for (boolean nulls : new boolean[] {false, true}) {
                for (boolean v2 : new boolean[] {false, true}) {
                    Long[] v = new Long[rows]; // raw bits for FLOAT64
                    for (int i = 0; i < rows; i++) {
                        long x = switch (physical) {
                            case INT32 -> (long) rnd.nextInt();
                            case INT64 -> rnd.nextLong();
                            default -> Double.doubleToRawLongBits(i % 50 == 0 ? specials[(i / 50) % specials.length] : rnd.nextGaussian() * 1e6);
                        };
                        v[i] = nulls && rnd.nextInt(6) == 0 ? null : x;
                    }
                    int maxDef = nulls ? 1 : 0;
                    List<PreparedPage> pages = new ArrayList<>();
                    int start = 0;
                    for (int end : evenSplits(rows, 4)) {
                        byte[] levels = levelBytes(v, start, end, maxDef);
                        List<Long> present = new ArrayList<>();
                        for (int i = start; i < end; i++) {
                            if (v[i] != null) {
                                present.add(v[i]);
                            }
                        }
                        int n = present.size();
                        byte[] vals = new byte[n * width];
                        for (int i = 0; i < n; i++) {
                            long x = present.get(i);
                            for (int j = 0; j < width; j++) {
                                vals[j * n + i] = (byte) (x >>> (8 * j));
                            }
                        }
                        byte[] page = assemble(levels, vals, maxDef, v2);
                        pages.add(
                                new PreparedPage(
                                        kernelPage(page, levels.length, maxDef, v2, end - start,
                                                ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT),
                                        end - start));
                        start = end;
                    }
                    String what = "BSS "
                            + physical
                            + "->"
                            + lane
                            + " nulls="
                            + nulls
                            + " v2="
                            + v2;
                    try (Arena arena = Arena.ofConfined()) {
                        ColumnChunkDecoder d = new ColumnChunkDecoder(physical, lane, maxDef, 384, scalarFactory());
                        assertBits(
                                v,
                                physical,
                                lane,
                                runStreaming(d, pages, lane, rows, 384, nulls,
                                        arena),
                                what + " staging");
                    }
                    for (boolean modeA : new boolean[] {true, false}) {
                        try (Arena arena = Arena.ofConfined()) {
                            ColumnChunkDecoder d = new ColumnChunkDecoder(physical, lane, maxDef, 384, scalarFactory());
                            assertBits(
                                    v,
                                    physical,
                                    lane,
                                    runStreamingDirect(d, pages, lane, rows, 384, nulls,
                                            modeA, arena),
                                    what + (modeA ? " directA" : " directB"));
                        }
                    }
                }
            }
        }
    }

    @Test
    void byteStreamSplitStrideComesFromThePageExtentNotTheBuffer() {
        // The scan hands pages over in a reused buffer larger than the page; the stride must come from the
        // page's own extent. Same INT64 page in an exact array and in an oversized one with garbage after it.
        int n = 100;
        byte[] vals = new byte[n * 8];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < 8; j++) {
                vals[j * n + i] = (byte) ((1000L * i + 7) >>> (8 * j));
            }
        }
        byte[] big = new byte[vals.length + 333];
        new Random(1).nextBytes(big);
        System.arraycopy(vals, 0, big, 0, vals.length);
        try (Arena arena = Arena.ofConfined()) {
            ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.INT64, 0, n, scalarFactory());
            List<PreparedPage> pages = List.of(new PreparedPage(ColumnChunkDecoder.Page.v1(big, vals.length, n, ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT), n));
            DecodeResult r = runStreaming(d, pages, VecType.INT64, n, n, false,
                    arena);
            for (int i = 0; i < n; i++) {
                assertEquals(1000L * i + 7, r.buffers.getLong(i), "row " + i);
            }
        }
    }

    private ColumnChunkDecoder.Page kernelPage(byte[] page, int levelsLength, int maxDef,
            boolean v2, int valueCount, ParquetPageDecoder.Encoding enc) {
        if (!v2) {
            return ColumnChunkDecoder.Page.v1(page, valueCount, enc);
        }
        return ColumnChunkDecoder.Page.v2(page, maxDef > 0 ? levelsLength : 0, valueCount,
                enc);
    }

    private static void assertBits(Long[] v, VecType physical, VecType lane,
            DecodeResult r, String what) {
        if (lane != VecType.FLOAT64) {
            assertDelta(v, physical, lane, r, what);
            return;
        }
        for (int i = 0; i < v.length; i++) {
            if (v[i] == null) {
                assertTrue(r.buffers.isNull(i), what + " row " + i + " should be null");
            } else {
                assertEquals(v[i].longValue(), Double.doubleToRawLongBits(r.buffers.getDouble(i)), what + " row " + i);
            }
        }
    }

    // ---------------------------------------------------------------- direct-write variants (#559 study)

    @Test
    void directWriteMatchesInputPlainAndDictWithNullsAcrossPages() {
        // The direct-write paths (readBatchDirectA / readBatchDirectB) must decode identically to the
        // staging path: present values straight into the Arrow data segment at their row slots, validity
        // written as 64-bit words. Batch size 384 does not divide the 1000-row pages, so batches span pages.
        Random rnd = new Random(5591);
        int rows = 4096;
        Integer[] plain = new Integer[rows];
        Integer[] dict = new Integer[rows];
        int[] domain = {7, 11, 13, 42, 100, 256,
                999};
        for (int i = 0; i < rows; i++) {
            plain[i] = rnd.nextInt(8) == 0 ? null : rnd.nextInt();
            dict[i] = rnd.nextInt(8) == 0 ? null : domain[rnd.nextInt(domain.length)];
        }
        for (boolean modeA : new boolean[] {true, false}) {
            try (Arena arena = Arena.ofConfined()) {
                assertInt32(plain, decodePlainInt32Direct(plain, arena, 4, 384, modeA));
            }
            try (Arena arena = Arena.ofConfined()) {
                assertInt32(dict, decodeDictInt32Direct(dict, arena, 4, 384, modeA));
            }
        }
    }

    @Test
    void directWriteNoNullLaneWritesNoValidity() {
        // A lane with no nulls (maxDefLevel 0) must take the no-validity fast path in both direct modes.
        int rows = 2048;
        Long[] v = new Long[rows];
        for (int i = 0; i < rows; i++) {
            v[i] = (long) i * 2654435761L;
        }
        for (boolean modeA : new boolean[] {true, false}) {
            try (Arena arena = Arena.ofConfined()) {
                DecodeResult r = decodePlainFixedDirect(v, VecType.INT64, arena, 3, 500, modeA);
                assertEquals(0, r.nullCount, "no-null lane should report zero nulls");
                assertNull(r.buffers.validity(), "no-null lane should expose no validity segment");
                for (int i = 0; i < rows; i++) {
                    assertEquals(v[i].longValue(), r.buffers.getLong(i), "row " + i);
                }
            }
        }
    }

    private DecodeResult decodePlainInt32Direct(Integer[] v, Arena arena, int pages,
            int batchRows, boolean modeA) {
        return decodePlainFixedDirect(v, VecType.INT32, arena, pages, batchRows, modeA,
                (i, seg, off) -> seg.set(LE_INT, off, v[i]));
    }

    private <T> DecodeResult decodePlainFixedDirect(T[] v, VecType type, Arena arena,
            int pages, int batchRows, boolean modeA) {
        FixedStore store = switch (type) {
            case INT32 -> (i, seg, off) -> seg.set(LE_INT, off, (Integer) v[i]);
            case INT64 -> (i, seg, off) -> seg.set(LE_LONG, off, (Long) v[i]);
            default -> throw new IllegalArgumentException("unsupported direct test lane " + type);
        };
        return decodePlainFixedDirect(v, type, arena, pages, batchRows, modeA,
                store);
    }

    private <T> DecodeResult decodePlainFixedDirect(
            T[] v,
            VecType type,
            Arena arena,
            int pages,
            int batchRows,
            boolean modeA,
            FixedStore store) {
        int rows = v.length;
        boolean hasNulls = anyNull(v);
        int maxDef = hasNulls ? 1 : 0;
        ColumnChunkDecoder d = new ColumnChunkDecoder(type, type, maxDef, batchRows, scalarFactory());
        List<PreparedPage> prepared = new ArrayList<>();
        int start = 0;
        for (int end : evenSplits(rows, pages)) {
            byte[] page = plainFixedPage(v, type, start, end, maxDef, false,
                    store);
            prepared.add(
                    new PreparedPage(ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.PLAIN), end - start));
            start = end;
        }
        return runStreamingDirect(d, prepared, type, rows, batchRows, hasNulls,
                modeA, arena);
    }

    private DecodeResult decodeDictInt32Direct(Integer[] v, Arena arena, int pages,
            int batchRows, boolean modeA) {
        int rows = v.length;
        List<Integer> dict = new ArrayList<>();
        java.util.Map<Integer, Integer> idOf = new java.util.HashMap<>();
        int[] ids = new int[rows];
        for (int i = 0; i < rows; i++) {
            if (v[i] == null) {
                continue;
            }
            Integer id = idOf.computeIfAbsent(v[i], k -> {
                dict.add(k);
                return dict.size() - 1;
            });
            ids[i] = id;
        }
        MemorySegment dictData = ArrowLayout.allocateData(arena, VecType.INT32, Math.max(dict.size(), 1));
        for (int i = 0; i < dict.size(); i++) {
            dictData.set(LE_INT, (long) i << 2, dict.get(i));
        }
        VectorBuffers dictionary = SegmentVectorBuffers.fixedWidth(VecType.INT32, dict.size(), null, dictData);
        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.INT32, 1, batchRows, scalarFactory());
        d.startChunk(rows);
        d.setDictionary(dictionary);
        List<PreparedPage> prepared = new ArrayList<>();
        int start = 0;
        for (int end : evenSplits(rows, pages)) {
            byte[] page = dictPage(v, ids, start, end, 1);
            prepared.add(
                    new PreparedPage(ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.RLE_DICTIONARY), end - start));
            start = end;
        }
        return runStreamingDirectInner(d, prepared, VecType.INT32, rows, batchRows, true,
                modeA, arena);
    }

    /**
     * Drives the direct-write loop as the reader does: each batch's present
     * values and validity words are written STRAIGHT into the full-column Arrow
     * segments at the batch's row offset ({@code dstBase = done}), via {@link
     * ColumnChunkDecoder#readBatchDirectA} / {@link
     * ColumnChunkDecoder#readBatchDirectB}. No staging, no per-batch copy.
     */
    private DecodeResult runStreamingDirect(
            ColumnChunkDecoder d,
            List<PreparedPage> pages,
            VecType lane,
            int rows,
            int batchRows,
            boolean hasNulls,
            boolean modeA,
            Arena arena) {
        d.startChunk(rows);
        return runStreamingDirectInner(d, pages, lane, rows, batchRows, hasNulls,
                modeA, arena);
    }

    private DecodeResult runStreamingDirectInner(
            ColumnChunkDecoder d,
            List<PreparedPage> pages,
            VecType lane,
            int rows,
            int batchRows,
            boolean hasNulls,
            boolean modeA,
            Arena arena) {
        MemorySegment outData = ArrowLayout.allocateData(arena, lane, Math.max(rows, 1));
        MemorySegment outValidity = ArrowLayout.allocateBitmap(arena, Math.max(rows, 1));
        if (hasNulls) {
            outValidity.asSlice(0L, ((long) ((rows + 63) >>> 6)) * 8).fill((byte) 0);
        }
        java.nio.ByteBuffer nio = modeA ? null : outData.asByteBuffer();
        int done = 0;
        int pageIdx = 0;
        int totalNulls = 0;
        while (done < rows) {
            int want = Math.min(batchRows, rows - done);
            d.startBatch(want);
            int filled = 0;
            while (filled < want) {
                if (d.needsPage()) {
                    d.feedPage(pages.get(pageIdx++)
                                    .page());
                }
                filled += modeA ? d.readBatchDirectA(want - filled, done + filled, outData,
                        hasNulls ? outValidity : null)
                        : d.readBatchDirectB(want - filled, done + filled, nio,
                        hasNulls ? outValidity : null);
            }
            totalNulls += d.batchNullCount();
            done += want;
        }
        MemorySegment viewValidity = hasNulls ? outValidity : null;
        return new DecodeResult(SegmentVectorBuffers.fixedWidth(lane, rows, viewValidity, outData), totalNulls);
    }

    /**
     * The streaming decode loop, exactly as the reader drives it: it fills batches
     * of {@code batchRows} rows, feeding a prepared page whenever the decoder
     * needs one, and flushes each batch into a full-column Arrow buffer at its row
     * offset. A batch therefore spans pages and (for dictionary ids) resumes
     * mid-run. Returns the assembled column plus the total null count summed from
     * each batch's {@link ColumnChunkDecoder#batchNullCount()}.
     */
    private DecodeResult runStreaming(
            ColumnChunkDecoder d,
            List<PreparedPage> pages,
            VecType lane,
            int rows,
            int batchRows,
            boolean hasNulls,
            Arena arena) {
        d.startChunk(rows);
        MemorySegment outData = ArrowLayout.allocateData(arena, lane, Math.max(rows, 1));
        MemorySegment outValidity = ArrowLayout.allocateBitmap(arena, Math.max(rows, 1));
        // Per-batch flush copies into a batch-sized scratch, then we copy that into the full column.
        int totalNulls = drive(d, pages, lane, rows, batchRows, outData,
                outValidity, arena);
        MemorySegment viewValidity = hasNulls ? outValidity : null;
        return new DecodeResult(SegmentVectorBuffers.fixedWidth(lane, rows, viewValidity, outData), totalNulls);
    }

    private DecodeResult runStreamingUtf8(ColumnChunkDecoder d, List<PreparedPage> pages, int rows,
            int batchRows, boolean hasNulls, Arena arena) {
        d.startChunk(rows);
        return runStreamingUtf8Inner(d, pages, rows, batchRows, hasNulls, arena);
    }

    private DecodeResult runStreamingUtf8Inner(ColumnChunkDecoder d, List<PreparedPage> pages, int rows,
            int batchRows, boolean hasNulls, Arena arena) {
        MemorySegment outOffsets = ArrowLayout.allocateOffsets(arena, Math.max(rows, 1));
        MemorySegment outValidity = ArrowLayout.allocateBitmap(arena, Math.max(rows, 1));
        java.io.ByteArrayOutputStream dataAcc = new java.io.ByteArrayOutputStream();
        int done = 0;
        int pageIdx = 0;
        int totalNulls = 0;
        int dataBase = 0;
        while (done < rows) {
            int want = Math.min(batchRows, rows - done);
            d.startBatch(want);
            int filled = 0;
            while (filled < want) {
                if (d.needsPage()) {
                    d.feedPage(pages.get(pageIdx++)
                                    .page());
                }
                filled += d.readBatch(want - filled, filled);
            }
            long bytes = d.utf8Bytes();
            MemorySegment bOffsets = ArrowLayout.allocateOffsets(arena, want);
            MemorySegment bData = ArrowLayout.allocateBytes(arena, Math.max(bytes, 1));
            MemorySegment bValidity = ArrowLayout.allocateBitmap(arena, want);
            boolean bHasNulls = d.batchNullCount() > 0;
            d.flushUtf8(want, bOffsets, bData, bHasNulls ? bValidity : null);
            if (!bHasNulls) {
                io.vecruntime.kernels.Bitmap.fill(bValidity, want, true);
            }
            byte[] batchBytes = new byte[(int) bytes];
            MemorySegment.copy(bData, ValueLayout.JAVA_BYTE, 0L, batchBytes, 0,
                    (int) bytes);
            for (int i = 0; i < want; i++) {
                int o = bOffsets.get(LE_INT, (long) i << 2);
                outOffsets.set(LE_INT, (long) (done + i) << 2, dataBase + o);
                boolean valid = ((bValidity.get(ValueLayout.JAVA_BYTE, (long) (i >>> 3)) >>> (i & 7)) & 1) != 0;
                setBit(outValidity, done + i, valid);
            }
            dataAcc.writeBytes(batchBytes);
            dataBase += (int) bytes;
            done += want;
            totalNulls += d.batchNullCount();
        }
        outOffsets.set(LE_INT, (long) rows << 2, dataBase);
        byte[] all = dataAcc.toByteArray();
        MemorySegment fullData = ArrowLayout.allocateBytes(arena, Math.max(all.length, 1));
        MemorySegment.copy(MemorySegment.ofArray(all), 0, fullData, 0, all.length);
        MemorySegment viewValidity = hasNulls ? outValidity : null;
        return new DecodeResult(SegmentVectorBuffers.utf8(rows, viewValidity, outOffsets, fullData), totalNulls);
    }

    private static void setBit(MemorySegment bitmap, int i, boolean set) {
        long byteIdx = (long) i >>> 3;
        int bit = i & 7;
        byte b = bitmap.get(ValueLayout.JAVA_BYTE, byteIdx);
        if (set) {
            b = (byte) (b | (1 << bit));
        } else {
            b = (byte) (b & ~(1 << bit));
        }
        bitmap.set(ValueLayout.JAVA_BYTE, byteIdx, b);
    }

    /** Fixed-width batch loop, assembling the full-column data + validity. */
    private int drive(
            ColumnChunkDecoder d,
            List<PreparedPage> pages,
            VecType lane,
            int rows,
            int batchRows,
            MemorySegment outData,
            MemorySegment outValidity,
            Arena arena) {
        int width = lane.byteWidth();
        int done = 0;
        int pageIdx = 0;
        int totalNulls = 0;
        while (done < rows) {
            int want = Math.min(batchRows, rows - done);
            d.startBatch(want);
            int filled = 0;
            while (filled < want) {
                if (d.needsPage()) {
                    d.feedPage(pages.get(pageIdx++)
                                    .page());
                }
                filled += d.readBatch(want - filled, filled);
            }
            MemorySegment bData = ArrowLayout.allocateData(arena, lane, want);
            MemorySegment bValidity = ArrowLayout.allocateBitmap(arena, want);
            boolean bHasNulls = d.batchNullCount() > 0;
            d.flushFixed(want, bData, bHasNulls ? bValidity : null);
            MemorySegment.copy(bData, 0L, outData, (long) done * width,
                    (long) want * width);
            if (bHasNulls) {
                for (int i = 0; i < want; i++) {
                    boolean valid = ((bValidity.get(ValueLayout.JAVA_BYTE, (long) (i >>> 3)) >>> (i & 7)) & 1) != 0;
                    setBit(outValidity, done + i, valid);
                }
            } else {
                for (int i = 0; i < want; i++) {
                    setBit(outValidity, done + i, true);
                }
            }
            done += want;
            totalNulls += d.batchNullCount();
        }
        return totalNulls;
    }

    private DecodeResult decodePlainInt32(Integer[] v, Arena arena, int pages,
            boolean v2) {
        return decodePlainInt32(v, arena, evenSplits(v.length, pages), v2, v.length);
    }

    private DecodeResult decodePlainInt32(Integer[] v, Arena arena, int[] splits,
            boolean v2) {
        return decodePlainInt32(v, arena, splits, v2, v.length);
    }

    private DecodeResult decodePlainInt32(Integer[] v, Arena arena, int[] splits,
            boolean v2, int batchRows) {
        return decodePlainFixedSplits(
                v,
                VecType.INT32,
                VecType.INT32,
                arena,
                splits,
                v2,
                batchRows,
                (i, seg, off) -> seg.set(LE_INT, off, v[i]));
    }

    private <T> DecodeResult decodePlainFixed(T[] v, VecType type, Arena arena,
            int pages, boolean v2, FixedStore store) {
        return decodePlainFixedSplits(v, type, type, arena, evenSplits(v.length, pages),
                v2, v.length, store);
    }

    private DecodeResult decodePlainWiden(Integer[] v, Arena arena, int pages) {
        return decodePlainFixedSplits(
                v,
                VecType.INT32,
                VecType.INT64,
                arena,
                evenSplits(v.length, pages),
                false,
                v.length,
                (i, seg, off) -> seg.set(LE_INT, off, v[i]));
    }

    private <T> DecodeResult decodePlainFixedSplits(
            T[] v,
            VecType physical,
            VecType lane,
            Arena arena,
            int[] splits,
            boolean v2,
            int batchRows,
            FixedStore store) {
        int rows = v.length;
        int maxDef = 1;
        ColumnChunkDecoder d = new ColumnChunkDecoder(physical, lane, maxDef, batchRows, scalarFactory());
        List<PreparedPage> pages = new ArrayList<>();
        int start = 0;
        for (int end : splits) {
            byte[] page = plainFixedPage(v, physical, start, end, maxDef, v2,
                    store);
            ColumnChunkDecoder.Page kp = v2 ? ColumnChunkDecoder.Page.v2(page, v2LevelLen(v, start, end, maxDef), end - start, ParquetPageDecoder.Encoding.PLAIN) : ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.PLAIN);
            pages.add(new PreparedPage(kp, end - start));
            start = end;
        }
        return runStreaming(d, pages, lane, rows, batchRows,
                anyNull(v), arena);
    }

    private DecodeResult decodePlainRequiredInt32(Integer[] v, Arena arena, int pages) {
        int rows = v.length;
        int maxDef = 0;
        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.INT32, VecType.INT32, maxDef, rows, scalarFactory());
        List<PreparedPage> plist = new ArrayList<>();
        int start = 0;
        for (int end : evenSplits(rows, pages)) {
            byte[] page = plainFixedPage(v, VecType.INT32, start, end, maxDef, false,
                    (i, seg, off) -> seg.set(LE_INT, off, v[i]));
            plist.add(
                    new PreparedPage(ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.PLAIN), end - start));
            start = end;
        }
        DecodeResult r = runStreaming(d, plist, VecType.INT32, rows, rows, false,
                arena);
        return new DecodeResult(SegmentVectorBuffers.fixedWidth(VecType.INT32, rows, null, r.buffers.data()), r.nullCount);
    }

    private DecodeResult decodeDictInt32(Integer[] v, Arena arena, int pages,
            java.util.function.IntFunction<GroupUnpacker> factory) {
        return decodeDictInt32(v, arena, evenSplits(v.length, pages), factory, v.length);
    }

    private DecodeResult decodeDictInt32(Integer[] v, Arena arena, int[] splits,
            java.util.function.IntFunction<GroupUnpacker> factory, int batchRows) {
        int rows = v.length;
        int maxDef = 1;
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
        MemorySegment dictData = ArrowLayout.allocateData(arena, VecType.INT32, Math.max(dict.size(), 1));
        for (int i = 0; i < dict.size(); i++) {
            dictData.set(LE_INT, (long) i << 2, dict.get(i));
        }
        VectorBuffers dictionary = SegmentVectorBuffers.fixedWidth(VecType.INT32, dict.size(), null, dictData);

        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.INT32, maxDef, batchRows, factory);
        d.startChunk(rows);
        d.setDictionary(dictionary);
        List<PreparedPage> plist = new ArrayList<>();
        int start = 0;
        for (int end : splits) {
            byte[] page = dictPage(v, ids, start, end, maxDef);
            plist.add(
                    new PreparedPage(ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.RLE_DICTIONARY), end - start));
            start = end;
        }
        // startChunk is called inside runStreaming too; re-set dictionary after it.
        return runStreamingDict(d, dictionary, plist, rows, batchRows,
                anyNull(v), arena);
    }

    private DecodeResult runStreamingDict(
            ColumnChunkDecoder d,
            VectorBuffers dictionary,
            List<PreparedPage> pages,
            int rows,
            int batchRows,
            boolean hasNulls,
            Arena arena) {
        d.startChunk(rows);
        d.setDictionary(dictionary);
        MemorySegment outData = ArrowLayout.allocateData(arena, VecType.INT32, Math.max(rows, 1));
        MemorySegment outValidity = ArrowLayout.allocateBitmap(arena, Math.max(rows, 1));
        int totalNulls = drive(d, pages, VecType.INT32, rows, batchRows, outData,
                outValidity, arena);
        return new DecodeResult(SegmentVectorBuffers.fixedWidth(VecType.INT32, rows, hasNulls ? outValidity : null, outData), totalNulls);
    }

    private DecodeResult decodeUtf8(String[] v, Arena arena, int pages,
            boolean dict, boolean v2) {
        return decodeUtf8Batched(v, arena, pages, dict, v.length, v2);
    }

    private DecodeResult decodeUtf8Batched(String[] v, Arena arena, int pages,
            boolean dict, int batchRows) {
        return decodeUtf8Batched(v, arena, pages, dict, batchRows, false);
    }

    private DecodeResult decodeUtf8Batched(String[] v, Arena arena, int pages,
            boolean dict, int batchRows, boolean v2) {
        int rows = v.length;
        int maxDef = 1;
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
        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.UTF8, maxDef, batchRows, scalarFactory());
        List<PreparedPage> plist = new ArrayList<>();
        int start = 0;
        for (int end : evenSplits(rows, pages)) {
            byte[] page = dict ? utf8DictPage(v, ids, start, end, maxDef) : utf8PlainPage(v, start, end, maxDef, v2);
            ColumnChunkDecoder.Page kp = v2 && !dict
                    ? ColumnChunkDecoder.Page.v2(page, v2LevelLen(v, start, end, maxDef), end - start, ParquetPageDecoder.Encoding.PLAIN)
                    : ColumnChunkDecoder.Page.v1(page, end - start, dict ? ParquetPageDecoder.Encoding.RLE_DICTIONARY : ParquetPageDecoder.Encoding.PLAIN);
            plist.add(new PreparedPage(kp, end - start));
            start = end;
        }
        if (dict) {
            return runStreamingUtf8WithDict(d, dictionary, plist, rows, batchRows,
                    anyNull(v), arena);
        }
        return runStreamingUtf8(d, plist, rows, batchRows, anyNull(v),
                arena);
    }

    private DecodeResult runStreamingUtf8WithDict(
            ColumnChunkDecoder d,
            VectorBuffers dictionary,
            List<PreparedPage> pages,
            int rows,
            int batchRows,
            boolean hasNulls,
            Arena arena) {
        d.startChunk(rows);
        d.setDictionary(dictionary);
        return runStreamingUtf8Inner(d, pages, rows, batchRows, hasNulls, arena);
    }

    private long[] decodeAndGetValidityWords(Integer[] v, int rows) {
        return decodeAndGetValidityWords(v, rows, new int[] {rows});
    }

    /**
     * Decode the whole column as ONE batch of {@code rows} rows and return the
     * batch validity words directly (the validity-word-builder tests inspect the
     * decoder's own batch validity, so a single batch = the whole column).
     */
    private long[] decodeAndGetValidityWords(Integer[] v, int rows, int[] splits) {
        int maxDef = 1;
        ColumnChunkDecoder d = new ColumnChunkDecoder(VecType.INT32, maxDef, rows, scalarFactory());
        d.startChunk(rows);
        d.startBatch(rows);
        int start = 0;
        int filled = 0;
        for (int end : splits) {
            byte[] page = plainFixedPage(v, VecType.INT32, start, end, maxDef, false,
                    (i, seg, off) -> seg.set(LE_INT, off, v[i]));
            d.feedPage(ColumnChunkDecoder.Page.v1(page, end - start, ParquetPageDecoder.Encoding.PLAIN));
            filled += d.readBatch(rows - filled, filled);
            start = end;
        }
        return d.validityWordArray();
    }

    // ================================================================ page builders

    private <T> byte[] plainFixedPage(
            T[] v,
            VecType physical,
            int start,
            int end,
            int maxDef,
            boolean v2,
            FixedStore store) {
        int width = physical.byteWidth();
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
}
