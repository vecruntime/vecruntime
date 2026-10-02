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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link DeltaBinaryPackedReader} against a from-scratch reference encoder
 * of the Parquet spec (the cross-check against parquet-java's own writers lives
 * in spark/, {@code ParquetPageDecoderCrossCheckSuite}). Covers lengths around
 * the miniblock and block sizes, a single value, wrapping INT32 and INT64 deltas
 * (MIN/MAX neighbours, so 32- and 64-bit miniblocks), several block layouts,
 * reads split at random points, a truncated last miniblock, the injected
 * unpacker and the INT32-widened-to-long read.
 */
class DeltaBinaryPackedReaderTest {

    private static final int[] LENGTHS = {
        0,
        1,
        2,
        7,
        31,
        32,
        33,
        127,
        128,
        129,
        255,
        256,
        257,
        1000,
        4099
    };

    // ---------------------------------------------------------------- reference encoder

    /**
     * Encodes {@code values} (INT32 when {@code int64} is false: deltas wrap in
     * 32 bits) the way the spec describes. {@code truncateTail} omits the zero
     * padding of the last miniblock, which a reader must accept.
     */
    static byte[] encode(long[] values, boolean int64, int blockSize,
                         int miniblocks, boolean truncateTail) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int perMini = blockSize / miniblocks;
        uleb(out, blockSize);
        uleb(out, miniblocks);
        uleb(out, values.length);
        uleb(out, zigzag(values.length == 0 ? 0 : values[0]));
        int i = 1;
        while (i < values.length) {
            int n = Math.min(blockSize, values.length - i);
            long[] deltas = new long[n];
            long min = Long.MAX_VALUE;
            for (int k = 0; k < n; k++) {
                long d = int64 ? values[i + k] - values[i + k - 1] : (long) ((int) values[i + k] - (int) values[i + k - 1]);
                deltas[k] = d;
                min = Math.min(min, d);
            }
            uleb(out, zigzag(min));
            int used = (n + perMini - 1) / perMini;
            int[] widths = new int[miniblocks];
            long[] rel = new long[n];
            for (int k = 0; k < n; k++) {
                rel[k] = int64 ? deltas[k] - min : ((int) deltas[k] - (int) min) & 0xFFFFFFFFL;
            }
            for (int m = 0; m < used; m++) {
                long or = 0;
                for (int k = m * perMini; k < Math.min(n, (m + 1) * perMini); k++) {
                    or |= rel[k];
                }
                widths[m] = 64 - Long.numberOfLeadingZeros(or);
            }
            for (int m = 0; m < miniblocks; m++) {
                out.write(m < used ? widths[m] : 0);
            }
            for (int m = 0; m < used; m++) {
                int w = widths[m];
                byte[] packed = new byte[w * perMini / 8 + 8];
                for (int k = 0; k < perMini; k++) {
                    int idx = m * perMini + k;
                    long v = idx < n ? rel[idx] : 0;
                    for (int b = 0; b < w; b++) {
                        if (((v >>> b) & 1) != 0) {
                            long bit = (long) k * w + b;
                            packed[(int) (bit >>> 3)] |= (byte) (1 << (bit & 7));
                        }
                    }
                }
                int len = w * perMini / 8;
                boolean last = m == used - 1 && i + n >= values.length;
                if (truncateTail && last) {
                    int valuesInMini = n - m * perMini;
                    len = (valuesInMini * w + 7) / 8;
                }
                out.write(packed, 0, len);
            }
            i += n;
        }
        return out.toByteArray();
    }

    private static void uleb(ByteArrayOutputStream out, long v) {
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    private static long zigzag(long v) {
        return (v << 1) ^ (v >> 63);
    }

    /**
     * The reference scalar unpack of eight values, as parquet's BytePacker does
     * it.
     */
    static void refUnpack8(byte[] src, int sp, int[] dst,
                           int dp, int bw) {
        for (int k = 0; k < 8; k++) {
            int v = 0;
            for (int b = 0; b < bw; b++) {
                int bit = k * bw + b;
                if (((src[sp + (bit >>> 3)] >>> (bit & 7)) & 1) != 0) {
                    v |= 1 << b;
                }
            }
            dst[dp + k] = v;
        }
    }

    // ---------------------------------------------------------------- data

    private static long[] ints(Random rnd, int n, int kind) {
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = switch (kind) {
                        case 0 -> i * 3L + 1000; // sorted, constant delta (width 0)
                        case 1 -> rnd.nextInt(1000); // small random
                        case 2 -> rnd.nextInt(); // full-range random: 32-bit deltas, wrapping
                        default -> i % 2 == 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE; // extreme wrap
                    };
        }
        return v;
    }

    private static long[] longs(Random rnd, int n, int kind) {
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = switch (kind) {
                        case 0 -> 1_700_000_000_000_000L + i * 1000L; // timestamps
                        case 1 -> rnd.nextLong() >> 20; // 44-bit random: widths > 32
                        case 2 -> rnd.nextLong(); // full range: 64-bit deltas, wrapping
                        default -> i % 2 == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
                    };
        }
        return v;
    }

    // ---------------------------------------------------------------- tests

    @Test
    void int32RoundTripsAcrossLengthsLayoutsAndValueShapes() {
        Random rnd = new Random(5592);
        int[][] layouts = {{128, 4}, {256, 8}, {32, 1},
                {64, 2}};
        for (int[] layout : layouts) {
            for (int n : LENGTHS) {
                for (int kind = 0; kind < 4; kind++) {
                    for (boolean trunc : new boolean[] {false, true}) {
                        long[] v = ints(rnd, n, kind);
                        byte[] enc = encode(v, false, layout[0], layout[1], trunc);
                        DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(enc, 0, enc.length, false, null);
                        assertEquals(n, r.remaining());
                        int[] got = new int[n];
                        r.readInts(got, 0, n);
                        int[] want = new int[n];
                        for (int i = 0; i < n; i++) {
                            want[i] = (int) v[i];
                        }
                        assertArrayEquals(want, got, "layout "
                                + layout[0]
                                + "/"
                                + layout[1]
                                + " n="
                                + n
                                + " kind="
                                + kind
                                + " trunc="
                                + trunc);
                        assertEquals(0, r.remaining());
                    }
                }
            }
        }
    }

    @Test
    void int64RoundTripsIncludingWidthsAbove32() {
        Random rnd = new Random(5593);
        for (int n : LENGTHS) {
            for (int kind = 0; kind < 4; kind++) {
                for (boolean trunc : new boolean[] {false, true}) {
                    long[] v = longs(rnd, n, kind);
                    byte[] enc = encode(v, true, 128, 4, trunc);
                    DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(enc, 0, enc.length, true, null);
                    long[] got = new long[n];
                    r.readLongs(got, 0, n);
                    assertArrayEquals(v, got, "n=" + n + " kind=" + kind + " trunc=" + trunc);
                }
            }
        }
    }

    @Test
    void readsSplitAtRandomPointsResumeMidMiniblock() {
        Random rnd = new Random(5594);
        for (int rep = 0; rep < 50; rep++) {
            int n = 1 + rnd.nextInt(3000);
            boolean int64 = rnd.nextBoolean();
            long[] v = int64 ? longs(rnd, n, rnd.nextInt(4)) : ints(rnd, n, rnd.nextInt(4));
            byte[] enc = encode(v, int64, 128, 4, rnd.nextBoolean());
            // The stream inside a larger array, with garbage around it, as a page region.
            byte[] page = new byte[enc.length + 37];
            rnd.nextBytes(page);
            System.arraycopy(enc, 0, page, 11, enc.length);
            DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(page, 11, enc.length, int64, null);
            long[] got = new long[n];
            int done = 0;
            while (done < n) {
                int take = Math.min(n - done, 1 + rnd.nextInt(200));
                if (int64) {
                    r.readLongs(got, done, take);
                } else {
                    int[] tmp = new int[take];
                    r.readInts(tmp, 0, take);
                    for (int k = 0; k < take; k++) {
                        got[done + k] = tmp[k];
                    }
                }
                done += take;
            }
            long[] want = v.clone();
            if (!int64) {
                for (int i = 0; i < n; i++) {
                    want[i] = (int) v[i];
                }
            }
            assertArrayEquals(want, got, "rep " + rep);
        }
    }

    @Test
    void int32ReadAsLongsIsSignExtended() {
        long[] v = {-5, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 42};
        byte[] enc = encode(v, false, 128, 4, false);
        DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(enc, 0, enc.length, false, null);
        long[] got = new long[v.length];
        r.readLongs(got, 0, v.length);
        assertArrayEquals(v, got);
    }

    @Test
    void injectedUnpackerIsUsedAndMatches() {
        Random rnd = new Random(5595);
        long[] v = ints(rnd, 2000, 1);
        byte[] enc = encode(v, false, 128, 4, false);
        AtomicInteger calls = new AtomicInteger();
        IntFunction<GroupUnpacker> f = bw -> (src, sp, dst, dp) -> {
            calls.incrementAndGet();
            refUnpack8(src, sp, dst, dp, bw);
        };
        DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(enc, 0, enc.length, false, f);
        int[] got = new int[v.length];
        r.readInts(got, 0, v.length);
        for (int i = 0; i < v.length; i++) {
            assertEquals((int) v[i], got[i]);
        }
        assertTrue(calls.get() > 0, "injected GroupUnpacker was never called");
    }

    @Test
    void readingPastTheEndAndBadHeadersFail() {
        byte[] enc = encode(new long[] {1, 2, 3}, false, 128,
                4, false);
        DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(enc, 0, enc.length, false, null);
        assertThrows(IllegalStateException.class, () -> r.readInts(new int[4], 0, 4));
        // block size not a multiple of 8 values per miniblock
        ByteArrayOutputStream bad = new ByteArrayOutputStream();
        uleb(bad, 100);
        uleb(bad, 3);
        uleb(bad, 1);
        uleb(bad, 0);
        byte[] b = bad.toByteArray();
        assertThrows(IllegalStateException.class, () -> new DeltaBinaryPackedReader(b, 0, b.length, false, null));
        // a header cut short
        assertThrows(IllegalStateException.class, () -> new DeltaBinaryPackedReader(enc, 0, 2, false, null));
    }
}
