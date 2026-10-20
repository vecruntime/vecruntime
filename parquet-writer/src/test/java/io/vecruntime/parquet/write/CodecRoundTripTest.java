/*
 * Copyright 2026-2027 Angel Conde and the vecruntime contributors
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
package io.vecruntime.parquet.write;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.parquet.DeltaBinaryPackedReader;
import io.vecruntime.kernels.parquet.GroupUnpacker;
import io.vecruntime.kernels.parquet.RleBitPackingReader;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * #900 phase 0: this module's encoders decoded by the reader's own decoders in
 * {@code kernels} ({@link DeltaBinaryPackedReader}, {@link
 * RleBitPackingReader}), so the two implementations of the same framing cannot
 * drift apart while they are merged into one codec. The other encoder tests
 * decode with parquet-java; this one pins the pair we ship together. Every case
 * runs with the reader's scalar group unpack and with parquet-java's {@code
 * BytePacker} injected (the path the Spark reader takes), and the delta encoder
 * with and without its vector block path.
 */
class CodecRoundTripTest {

    private static final int[] SIZES = {
        0,
        1,
        2,
        7,
        8,
        9,
        31,
        32,
        33,
        127,
        128,
        129,
        130,
        255,
        256,
        257,
        1000,
        4097,
        20000
    };

    @SuppressWarnings("deprecation") // BytePacker.unpack8Values(byte[]...): the reader's injection seam
    private static GroupUnpacker packer(int bitWidth) {
        return Packer.LITTLE_ENDIAN.newBytePacker(bitWidth)::unpack8Values;
    }

    private static final IntFunction<GroupUnpacker> PACKERS = CodecRoundTripTest::packer;

    /** The reader's scalar group unpack (null) and parquet-java's injected one. */
    private static final List<IntFunction<GroupUnpacker>> UNPACKERS = unpackers();

    private static List<IntFunction<GroupUnpacker>> unpackers() {
        List<IntFunction<GroupUnpacker>> u = new ArrayList<>();
        u.add(null);
        u.add(PACKERS);
        return u;
    }

    // ---- DELTA_BINARY_PACKED

    /**
     * Value shapes that stress the deltas: runs, ramps, random, extremes and
     * wrap-around.
     */
    private static long[] longs(Random r, int n, int shape) {
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = switch (shape) {
                        case 0 -> 42; // all deltas 0: width-0 miniblocks
                        case 1 -> i * 3L - 1000; // constant delta
                        case 2 -> r.nextInt(1 << 10); // small random
                        case 3 -> r.nextLong(); // full-width random deltas (overflowing)
                        case 4 -> (i & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE; // extreme alternation
                        case 5 -> i % 300 < 150 ? i : -i * 1_000_003L; // width changes between miniblocks
                        default -> Long.MAX_VALUE - i; // descending near the top
                    };
        }
        return v;
    }

    private static long[] ints(Random r, int n, int shape) {
        long[] v = longs(r, n, shape);
        for (int i = 0; i < n; i++) {
            v[i] = (int) v[i]; // the encoder keeps the low 32 bits
        }
        return v;
    }

    private static byte[] encodeDelta(long[] v, int n, boolean int64,
            boolean vector) {
        ByteSink s = new ByteSink(16);
        DeltaBinaryPackedEncoder e = new DeltaBinaryPackedEncoder(vector);
        if (int64) {
            e.encodeLongs(v, n, s);
        } else {
            e.encodeInts(v, n, s);
        }
        return s.toByteArray();
    }

    @Test
    void deltaBinaryPackedLongsRoundTrip() {
        Random r = new Random(900);
        for (int shape = 0; shape <= 6; shape++) {
            for (int n : SIZES) {
                long[] v = longs(r, n, shape);
                for (boolean vector : new boolean[] {false, true}) {
                    byte[] page = encodeDelta(v, n, true, vector);
                    for (IntFunction<GroupUnpacker> u : UNPACKERS) {
                        DeltaBinaryPackedReader d = new DeltaBinaryPackedReader(page, 0, page.length, true, u);
                        assertEquals(n, d.remaining(), "count, shape " + shape + ", n " + n);
                        long[] out = new long[n];
                        d.readLongs(out, 0, n);
                        assertArrayEquals(v, out, "shape "
                                + shape
                                + ", n "
                                + n
                                + ", vector "
                                + vector
                                + ", packer "
                                + (u != null));
                    }
                }
            }
        }
    }

    @Test
    void deltaBinaryPackedIntsRoundTrip() {
        Random r = new Random(901);
        for (int shape = 0; shape <= 6; shape++) {
            for (int n : SIZES) {
                long[] v = ints(r, n, shape);
                int[] expected = new int[n];
                for (int i = 0; i < n; i++) {
                    expected[i] = (int) v[i];
                }
                for (boolean vector : new boolean[] {false, true}) {
                    byte[] page = encodeDelta(v, n, false, vector);
                    for (IntFunction<GroupUnpacker> u : UNPACKERS) {
                        DeltaBinaryPackedReader d = new DeltaBinaryPackedReader(page, 0, page.length, false, u);
                        assertEquals(n, d.remaining(), "count, shape " + shape + ", n " + n);
                        int[] out = new int[n];
                        d.readInts(out, 0, n);
                        assertArrayEquals(expected, out, "shape "
                                + shape
                                + ", n "
                                + n
                                + ", vector "
                                + vector
                                + ", packer "
                                + (u != null));
                    }
                }
            }
        }
    }

    /**
     * Reads split at odd boundaries cross miniblocks and blocks mid-way, as
     * batches do.
     */
    @Test
    void deltaBinaryPackedReadsInPieces() {
        Random r = new Random(902);
        int n = 10_000;
        long[] v = longs(r, n, 5);
        byte[] page = encodeDelta(v, n, true, true);
        DeltaBinaryPackedReader d = new DeltaBinaryPackedReader(page, 0, page.length, true, PACKERS);
        long[] out = new long[n];
        int done = 0;
        while (done < n) {
            int k = Math.min(n - done, 1 + r.nextInt(300));
            d.readLongs(out, done, k);
            done += k;
        }
        assertArrayEquals(v, out);
        assertEquals(0, d.remaining());
    }

    /**
     * A page that carries more after the delta stream: the reader stops at the
     * stream's end.
     */
    @Test
    void deltaBinaryPackedStreamAtAnOffset() {
        Random r = new Random(903);
        int n = 777;
        long[] v = longs(r, n, 2);
        byte[] body = encodeDelta(v, n, true, false);
        byte[] page = new byte[body.length + 13];
        System.arraycopy(body, 0, page, 5, body.length);
        DeltaBinaryPackedReader d = new DeltaBinaryPackedReader(page, 5, body.length, true, null);
        long[] out = new long[n];
        d.readLongs(out, 0, n);
        assertArrayEquals(v, out);
        assertEquals(5 + body.length, d.position(), "consumed exactly the stream");
    }

    // ---- RLE / bit-packed hybrid

    /** Runs and alternations at {@code bitWidth}, like levels and dictionary ids. */
    private static int[] hybridValues(Random r, int n, int bitWidth) {
        int[] v = new int[n];
        int i = 0;
        while (i < n) {
            int len = r.nextInt(4) == 0 ? 1 + r.nextInt(40) : 1 + r.nextInt(3);
            int x;
            if (bitWidth == 0) {
                x = 0;
            } else if (bitWidth == 32) {
                x = r.nextInt();
            } else {
                x = (int) r.nextLong(1L << bitWidth);
            }
            for (int k = 0;
                 k < len && i < n;
                 k++) {
                v[i++] = x;
            }
        }
        return v;
    }

    @Test
    void rleHybridRoundTripsAtEveryBitWidth() {
        Random r = new Random(904);
        try (Arena arena = Arena.ofConfined()) {
            for (int bw = 0; bw <= 32; bw++) {
                for (int n : SIZES) {
                    int[] v = hybridValues(r, n, bw);
                    ByteSink s = new ByteSink(16);
                    RleHybridEncoder.encode(v, n, bw, s);
                    byte[] page = s.toByteArray();
                    String at = "bitWidth " + bw + ", n " + n;

                    MemorySegment seg = arena.allocate(Math.max(1, page.length));
                    MemorySegment.copy(MemorySegment.ofArray(page), 0, seg, 0, page.length);
                    int[] out = new int[n];
                    new RleBitPackingReader(seg, 0, page.length, bw).readInts(out, 0, n);
                    assertArrayEquals(v, out, at + ", native segment");

                    int[] out2 = new int[n];
                    RleBitPackingReader.overArray(page, 0, page.length, bw,
                            bw == 0 ? null : packer(bw))
                            .readInts(out2, 0, n);
                    assertArrayEquals(v, out2, at + ", array + packer");

                    RleBitPackingReader one = RleBitPackingReader.overArray(page, 0, page.length, bw, null);
                    for (int i = 0; i < n; i++) {
                        assertEquals(v[i], one.readInt(), at + ", readInt at " + i);
                    }
                }
            }
        }
    }

    /**
     * Validity bitmaps encoded as definition levels and read back as bits, as
     * the reader builds validity.
     */
    @Test
    void validityLevelsRoundTripAsBits() {
        Random r = new Random(905);
        try (Arena arena = Arena.ofConfined()) {
            for (int n : SIZES) {
                for (int pattern = 0; pattern <= 4; pattern++) {
                    MemorySegment validity = arena.allocate(8L * ((n + 64) / 64));
                    boolean[] set = new boolean[n];
                    for (int i = 0; i < n; i++) {
                        set[i] = switch (pattern) {
                                    case 0 -> true;
                                    case 1 -> false;
                                    case 2 -> r.nextBoolean();
                                    case 3 -> i % 100 < 70; // long runs
                                    default -> r.nextInt(50) != 0; // sparse nulls
                                };
                        if (set[i]) {
                            Bitmap.set(validity, i);
                        }
                    }
                    // A row range that starts mid-word, as a page boundary inside a batch does.
                    int from = n > 3 ? 3 : 0;
                    ByteSink s = new ByteSink(16);
                    RleHybridEncoder.encodeValidity(validity, from, n, s);
                    byte[] page = s.toByteArray();
                    int m = n - from;
                    long[] words = new long[(m + 63) / 64 + 1];
                    RleBitPackingReader.overArray(page, 0, page.length, 1, null).readBits(words, 0, m);
                    for (int i = 0; i < m; i++) {
                        boolean bit = (words[i >>> 6] & (1L << (i & 63))) != 0;
                        assertEquals(set[from + i], bit, "n "
                                + n
                                + ", pattern "
                                + pattern
                                + ", row "
                                + (from + i));
                    }

                    int[] levels = new int[m];
                    new RleBitPackingReader(MemorySegment.ofArray(page), 0, page.length, 1).readInts(levels, 0, m);
                    for (int i = 0; i < m; i++) {
                        assertEquals(set[from + i] ? 1 : 0, levels[i], "levels, n " + n + ", row " + (from + i));
                    }
                }
            }
        }
    }

    /** {@code encodeValidity} with no bitmap: every row is a value. */
    @Test
    void allValidLevelsRoundTrip() {
        for (int n : SIZES) {
            ByteSink s = new ByteSink(16);
            RleHybridEncoder.encodeValidity(null, 0, n, s);
            byte[] page = s.toByteArray();
            int[] levels = new int[n];
            RleBitPackingReader.overArray(page, 0, page.length, 1, null).readInts(levels, 0, n);
            for (int i = 0; i < n; i++) {
                assertEquals(1, levels[i], "n " + n + ", row " + i);
            }
        }
    }
}
