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
package io.vecruntime.kernels;

import java.util.Arrays;
import java.util.Random;

import io.vecruntime.kernels.reference.ScalarReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code RANGE} frame bounds and the frame aggregates against the scalar
 * reference, which replays Spark's sliding buffer and re-aggregates every frame
 * from scratch.
 */
class WindowFrameKernelsTest {

    private static final int[] SIZES = {0, 1, 2, 7, 63, 64,
            65, 129, 1000, 4097};

    /**
     * Bound shapes: {loUnbounded, lo, hiUnbounded, hi}, offsets in key units
     * (PRECEDING negative).
     */
    private static final long[][] FRAMES = {
        {1, 0, 1, 0}, // UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        {1, 0, 0, 0}, // UNBOUNDED PRECEDING AND CURRENT ROW
        {0, 0, 1, 0}, // CURRENT ROW AND UNBOUNDED FOLLOWING
        {0, -5, 0, 0}, // 5 PRECEDING AND CURRENT ROW
        {0, -3, 0, 2}, // 3 PRECEDING AND 2 FOLLOWING
        {0, 1, 0, 3}, // 1 FOLLOWING AND 3 FOLLOWING (empty frames near the end)
        {0, -7, 0, -2}, // 7 PRECEDING AND 2 PRECEDING
        {1, 0, 0, 4}, // UNBOUNDED PRECEDING AND 4 FOLLOWING
        {0, -4, 1, 0}, // 4 PRECEDING AND UNBOUNDED FOLLOWING
        {0, 0, 0, 0}, // CURRENT ROW AND CURRENT ROW (peers)
        {0, -1000, 0, 1000}, // wider than the partition
    };

    /**
     * A partition sorted by the key: duplicates, gaps, a null block first or
     * last.
     */
    private static long[] sortedKeys(Random rnd, int n, int keyBits,
            boolean descending, boolean nullsFirst, long[][] validityOut) {
        long[] keys = new long[n];
        int nulls = n < 4 ? 0 : rnd.nextInt(n / 4);
        long span = keyBits == 8
                ? 40
                : keyBits == 16 ? 400 : 4L * Math.max(1, n);
        for (int i = 0; i < n - nulls; i++) {
            keys[i] = rnd.nextLong(span) - span / 3;
            if (keyBits < 64) {
                int shift = 64 - keyBits;
                keys[i] = (keys[i] << shift) >> shift;
            }
        }
        Arrays.sort(keys, 0, n - nulls);
        if (descending) {
            for (int i = 0, j = n - nulls - 1;
                 i < j;
                 i++, j--) {
                long t = keys[i];
                keys[i] = keys[j];
                keys[j] = t;
            }
        }
        long[] validity = null;
        if (nulls > 0) {
            validity = new long[(n + 63) >>> 6];
            // Nulls sit where the null ordering puts them: first or last.
            int start = nullsFirst ? 0 : n - nulls;
            if (nullsFirst) {
                long[] shifted = new long[n];
                System.arraycopy(keys, 0, shifted, nulls, n - nulls);
                keys = shifted;
            }
            for (int i = 0; i < n; i++) {
                boolean isNull = i >= start && i < start + nulls;
                if (!isNull) {
                    validity[i >>> 6] |= 1L << (i & 63);
                }
                if (isNull) {
                    keys[i] = rnd.nextLong(); // whatever the lane holds under a null
                }
            }
        }
        validityOut[0] = validity;
        return keys;
    }

    private static long[] randomValidity(Random rnd, int n, double fraction) {
        if (fraction == 0.0) {
            return null;
        }
        long[] v = new long[(n + 63) >>> 6];
        for (int i = 0; i < n; i++) {
            if (rnd.nextDouble() >= fraction) {
                v[i >>> 6] |= 1L << (i & 63);
            }
        }
        return v;
    }

    @Test
    void boundsMatchSparksBufferWalkForEveryFrameShapeOrderAndKeyWidth() {
        Random rnd = new Random(58);
        for (int n : SIZES) {
            for (int keyBits : new int[] {8, 16, 32, 64}) {
                for (boolean descending : new boolean[] {false, true}) {
                    for (boolean nullsFirst : new boolean[] {true, false}) {
                        long[][] validity = new long[1][];
                        long[] keys = sortedKeys(rnd, n, keyBits, descending, nullsFirst, validity);
                        for (long[] f : FRAMES) {
                            // DESC flips the sign of the offsets, as Spark's UnaryMinus does.
                            long lo = descending ? -f[1] : f[1];
                            long hi = descending ? -f[3] : f[3];
                            int[] lo1 = new int[n];
                            int[] hi1 = new int[n];
                            int[] lo2 = new int[n];
                            int[] hi2 = new int[n];
                            WindowFrameKernels.rangeBounds(
                                    keys,
                                    validity[0],
                                    n,
                                    descending,
                                    nullsFirst,
                                    f[0] == 1,
                                    lo,
                                    f[2] == 1,
                                    hi,
                                    keyBits,
                                    false,
                                    lo1,
                                    hi1);
                            ScalarReference.rangeBounds(
                                    keys,
                                    validity[0],
                                    n,
                                    descending,
                                    nullsFirst,
                                    f[0] == 1,
                                    lo,
                                    f[2] == 1,
                                    hi,
                                    keyBits,
                                    false,
                                    lo2,
                                    hi2);
                            String label = "n="
                                    + n
                                    + " bits="
                                    + keyBits
                                    + " desc="
                                    + descending
                                    + " nullsFirst="
                                    + nullsFirst
                                    + " frame="
                                    + Arrays.toString(f);
                            assertArrayEquals(lo2, lo1, label + " lo");
                            assertArrayEquals(hi2, hi1, label + " hi");
                            for (int i = 0; i < n; i++) {
                                assertTrue(lo1[i] <= hi1[i], label + " row " + i);
                                assertTrue(i == 0 || (lo1[i] >= lo1[i - 1] && hi1[i] >= hi1[i - 1]),
                                        label + " monotone " + i);
                            }
                            checkAggregates(rnd, n, lo1, hi1, label);
                        }
                    }
                }
            }
        }
    }

    private static void checkAggregates(Random rnd, int n, int[] lo,
            int[] hi, String label) {
        long[] longs = new long[n];
        double[] doubles = new double[n];
        for (int i = 0; i < n; i++) {
            longs[i] = rnd.nextLong(2_000_000) - 1_000_000;
            doubles[i] = rnd.nextInt(10) == 0 ? Double.NaN : (rnd.nextDouble() - 0.5) * 1e6;
            if (rnd.nextInt(17) == 0) {
                doubles[i] = rnd.nextBoolean() ? -0.0 : 0.0;
            }
        }
        for (double fraction : new double[] {0.0, 0.2}) {
            long[] validity = randomValidity(rnd, n, fraction);
            int words = (n + 63) >>> 6;
            long[] outL1 = new long[n];
            long[] outL2 = new long[n];
            long[] v1 = new long[words];
            long[] v2 = new long[words];
            double[] outD1 = new double[n];
            double[] outD2 = new double[n];
            String l = label + " nulls=" + fraction;

            WindowFrameKernels.frameCount(validity, lo, hi, n, false, outL1);
            ScalarReference.frameCount(validity, lo, hi, n, false, outL2);
            assertArrayEquals(outL2, outL1, l + " count");
            WindowFrameKernels.frameCount(validity, lo, hi, n, true, outL1);
            ScalarReference.frameCount(validity, lo, hi, n, true, outL2);
            assertArrayEquals(outL2, outL1, l + " count(*)");

            for (boolean checked : new boolean[] {false, true}) {
                WindowFrameKernels.frameSumLong(longs, validity, lo, hi, n, checked,
                        outL1, v1);
                ScalarReference.frameSumLong(longs, validity, lo, hi, n, checked,
                        outL2, v2);
                assertArrayEquals(v2, v1, l + " sum validity checked=" + checked);
                assertMaskedEquals(outL2, outL1, v1, n, l + " sum checked=" + checked);
            }
            for (boolean average : new boolean[] {false, true}) {
                WindowFrameKernels.frameSumDouble(doubles, validity, lo, hi, n, average,
                        outD1, v1);
                ScalarReference.frameSumDouble(doubles, validity, lo, hi, n, average,
                        outD2, v2);
                assertArrayEquals(v2, v1, l + " double validity avg=" + average);
                assertMaskedEquals(outD2, outD1, v1, n, l + " double avg=" + average);
            }
            for (boolean isMin : new boolean[] {true, false}) {
                WindowFrameKernels.frameMinMaxLong(longs, validity, lo, hi, n, isMin,
                        outL1, v1);
                ScalarReference.frameMinMaxLong(longs, validity, lo, hi, n, isMin,
                        outL2, v2);
                assertArrayEquals(v2, v1, l + " minmax long validity min=" + isMin);
                assertMaskedEquals(outL2, outL1, v1, n, l + " minmax long min=" + isMin);
                WindowFrameKernels.frameMinMaxDouble(doubles, validity, lo, hi, n, isMin,
                        outD1, v1);
                ScalarReference.frameMinMaxDouble(doubles, validity, lo, hi, n, isMin,
                        outD2, v2);
                assertArrayEquals(v2, v1, l + " minmax double validity min=" + isMin);
                assertMaskedEquals(outD2, outD1, v1, n, l + " minmax double min=" + isMin);
            }
        }
    }

    private static void assertMaskedEquals(long[] expected, long[] actual, long[] validity,
            int n, String label) {
        for (int i = 0; i < n; i++) {
            if (WindowFrameKernels.valid(validity, i)) {
                assertEquals(expected[i], actual[i], label + " row " + i);
            }
        }
    }

    /**
     * Bit-identical doubles (NaN payload aside): the sums must add in Spark's
     * order.
     */
    private static void assertMaskedEquals(double[] expected, double[] actual, long[] validity,
            int n, String label) {
        for (int i = 0; i < n; i++) {
            if (WindowFrameKernels.valid(validity, i)) {
                assertEquals(Double.doubleToLongBits(expected[i]),
                        Double.doubleToLongBits(actual[i]), label
                                + " row "
                                + i
                                + " expected "
                                + expected[i]
                                + " got "
                                + actual[i]);
            }
        }
    }

    @Test
    void handWrittenFrames() {
        // ASC, 5 PRECEDING AND CURRENT ROW over keys with gaps and duplicates.
        long[] keys = {1, 2, 2, 8, 9, 20};
        int[] lo = new int[6];
        int[] hi = new int[6];
        WindowFrameKernels.rangeBounds(
                keys,
                null,
                6,
                false,
                true,
                false,
                -5,
                false,
                0,
                64,
                false,
                lo,
                hi);
        assertArrayEquals(new int[] {0, 0, 0, 3, 3, 5},
                lo);
        assertArrayEquals(new int[] {1, 3, 3, 4, 5, 6},
                hi);
        // 1 FOLLOWING AND 3 FOLLOWING: empty frames are lo == hi.
        WindowFrameKernels.rangeBounds(
                keys,
                null,
                6,
                false,
                true,
                false,
                1,
                false,
                3,
                64,
                false,
                lo,
                hi);
        assertArrayEquals(new int[] {1, 3, 3, 4, 5, 6},
                lo);
        assertArrayEquals(new int[] {3, 3, 3, 5, 5, 6},
                hi);
        // DESC with the offsets negated (Spark's UnaryMinus): 5 PRECEDING AND CURRENT ROW is [k + 5, k].
        long[] desc = {20, 9, 8, 2, 2, 1};
        WindowFrameKernels.rangeBounds(
                desc,
                null,
                6,
                true,
                false,
                false,
                5,
                false,
                0,
                64,
                false,
                lo,
                hi);
        assertArrayEquals(new int[] {0, 1, 1, 3, 3, 3},
                lo);
        assertArrayEquals(new int[] {1, 2, 3, 5, 5, 6},
                hi);
        // Nulls first: the null rows' frame is the null block, whatever the offsets; a valued row never sees them.
        long[] withNulls = {0, 0, 3, 4, 10};
        long[] validity = {0b11100};
        WindowFrameKernels.rangeBounds(
                withNulls,
                validity,
                5,
                false,
                true,
                false,
                -2,
                false,
                0,
                64,
                false,
                lo,
                hi);
        assertArrayEquals(new int[] {0, 0, 2, 2, 4},
                Arrays.copyOf(lo, 5));
        assertArrayEquals(new int[] {2, 2, 3, 4, 5},
                Arrays.copyOf(hi, 5));
        // Nulls last (DESC default).
        long[] descNulls = {10, 4, 3, 0, 0};
        long[] validityLast = {0b00111};
        WindowFrameKernels.rangeBounds(
                descNulls,
                validityLast,
                5,
                true,
                false,
                false,
                2,
                false,
                0,
                64,
                false,
                lo,
                hi);
        assertArrayEquals(new int[] {0, 1, 1, 3, 3},
                Arrays.copyOf(lo, 5));
        assertArrayEquals(new int[] {1, 2, 3, 5, 5},
                Arrays.copyOf(hi, 5));
        // An int key whose bound wraps: non-ANSI wraps like Spark's Add, ANSI raises.
        long[] nearMax = {Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
        int[] lo2 = new int[2];
        int[] hi2 = new int[2];
        WindowFrameKernels.rangeBounds(
                nearMax,
                null,
                2,
                false,
                true,
                false,
                0,
                false,
                5,
                32,
                false,
                lo2,
                hi2);
        int[] lo3 = new int[2];
        int[] hi3 = new int[2];
        ScalarReference.rangeBounds(
                nearMax,
                null,
                2,
                false,
                true,
                false,
                0,
                false,
                5,
                32,
                false,
                lo3,
                hi3);
        assertArrayEquals(lo3, lo2);
        assertArrayEquals(hi3, hi2);
        assertThrows(
                ArithmeticException.class,
                () -> WindowFrameKernels.rangeBounds(
                        nearMax,
                        null,
                        2,
                        false,
                        true,
                        false,
                        0,
                        false,
                        5,
                        32,
                        true,
                        lo2,
                        hi2));
        assertEquals((long) (byte) 130, WindowFrameKernels.bound(127, 3, 8, false));
        assertEquals(Long.MIN_VALUE, WindowFrameKernels.bound(Long.MAX_VALUE, 1, 64, false));
        assertThrows(ArithmeticException.class, () -> WindowFrameKernels.bound(Long.MAX_VALUE, 1, 64, true));
    }

    @Test
    void checkedSumRaisesWhereSparksPartialSumOverflows() {
        // MAX + 1 - 1: the frame's total fits, its partial sum does not -- Spark's ANSI sum raises.
        long[] values = {Long.MAX_VALUE, 1, -1};
        int[] lo = {0, 0, 0};
        int[] hi = {3, 3, 3};
        long[] out = new long[3];
        long[] validity = new long[1];
        assertThrows(
                ArithmeticException.class,
                () -> WindowFrameKernels.frameSumLong(values, null, lo, hi, 3, true,
                        out, validity));
        WindowFrameKernels.frameSumLong(values, null, lo, hi, 3, false,
                out, validity);
        assertEquals(Long.MAX_VALUE, out[0]);
        // Doubles: the frame [1, 3) restarted from its first row, not the previous frame's sum minus a row.
        double[] d = {1e16, 1.0, 1.0};
        int[] lo2 = {0, 1};
        int[] hi2 = {3, 3};
        double[] outD = new double[2];
        WindowFrameKernels.frameSumDouble(d, null, lo2, hi2, 2, false,
                outD, validity);
        assertEquals(1e16 + 1.0 + 1.0, outD[0]);
        assertEquals(2.0, outD[1]);
    }
}
