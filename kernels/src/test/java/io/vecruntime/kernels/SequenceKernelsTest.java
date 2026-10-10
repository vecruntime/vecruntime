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
package io.vecruntime.kernels;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import io.vecruntime.kernels.reference.ScalarReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link SequenceKernels}: consecutive values over all or selected lanes,
 * carried across batches.
 */
class SequenceKernelsTest {

    @Test
    void constantIntFillMatchesTheScalarReference() {
        try (Arena arena = Arena.ofConfined()) {
            for (int n : TestData.LENGTHS) {
                for (int value : new int[] {0, 1, 7, -3, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
                    MemorySegment got = ArrowLayout.allocateData(arena, VecType.INT32, Math.max(n, 1));
                    MemorySegment want = ArrowLayout.allocateData(arena, VecType.INT32, Math.max(n, 1));
                    SequenceKernels.fillInt(got, n, value);
                    ScalarReference.fillInt(want, n, value);
                    for (int i = 0; i < n; i++) {
                        assertEquals(want.getAtIndex(VectorBuffers.LE_INT, i),
                                got.getAtIndex(VectorBuffers.LE_INT, i), "lane " + i + " of " + n + " value " + value);
                    }
                }
            }
        }
    }

    @Test
    void fillsFullLaneBlocksAndTails() {
        try (Arena arena = Arena.ofConfined()) {
            for (int n : new int[] {0, 1, 7, 8, 63, 64,
                    65, 1000, 4096}) {
                MemorySegment out = ArrowLayout.allocateData(arena, VecType.INT64, Math.max(n, 1));
                long start = (7L << 33) + 12345;
                long next = SequenceKernels.iota(out, n, start);
                assertEquals(start + n, next, "next after " + n);
                for (int i = 0; i < n; i++) {
                    assertEquals(start + i, out.getAtIndex(VectorBuffers.LE_LONG, i), "lane " + i + " of " + n);
                }
            }
        }
    }

    @Test
    void steppedSequenceMatchesTheScalarReference() {
        try (Arena arena = Arena.ofConfined()) {
            Random rnd = new Random(11);
            long[] steps = {
                1,
                2,
                3,
                7,
                -1,
                -2,
                -5,
                1000,
                -1000,
                1L << 40,
                -(1L << 40)
            };
            for (int n : TestData.LENGTHS) {
                for (long step : steps) {
                    long start = rnd.nextLong() >> 8; // leaves room for n * step on either side
                    MemorySegment out = ArrowLayout.allocateData(arena, VecType.INT64, Math.max(n, 1));
                    MemorySegment expected = ArrowLayout.allocateData(arena, VecType.INT64, Math.max(n, 1));
                    long next = SequenceKernels.range(out, n, start, step);
                    long expectedNext = ScalarReference.range(expected, n, start, step);
                    assertEquals(expectedNext, next, "next after " + n + " by " + step);
                    for (int i = 0; i < n; i++) {
                        assertEquals(expected.getAtIndex(VectorBuffers.LE_LONG, i),
                                out.getAtIndex(VectorBuffers.LE_LONG, i), "lane " + i + " of " + n + " by " + step);
                    }
                }
            }
        }
    }

    @Test
    void steppedSequenceReachesTheLongBoundsWithoutWrapping() {
        // The last batches of range(Long.MAX_VALUE - k, Long.MAX_VALUE, step) and of the mirror image with
        // a negative step: the final lane lands exactly on the bound, the kernel must not run past it.
        try (Arena arena = Arena.ofConfined()) {
            for (int n : new int[] {1, 5, 64, 65, 1000}) {
                for (long step : new long[] {1, 3, 64}) {
                    MemorySegment out = ArrowLayout.allocateData(arena, VecType.INT64, n);
                    long top = Long.MAX_VALUE - (long) (n - 1) * step;
                    assertEquals(Long.MAX_VALUE + step, SequenceKernels.range(out, n, top, step)); // wraps, unused
                    assertEquals(Long.MAX_VALUE, out.getAtIndex(VectorBuffers.LE_LONG, n - 1));
                    assertEquals(top, out.getAtIndex(VectorBuffers.LE_LONG, 0));
                    long bottom = Long.MIN_VALUE + (long) (n - 1) * step;
                    SequenceKernels.range(out, n, bottom, -step);
                    assertEquals(Long.MIN_VALUE, out.getAtIndex(VectorBuffers.LE_LONG, n - 1));
                    assertEquals(bottom, out.getAtIndex(VectorBuffers.LE_LONG, 0));
                    for (int i = 1; i < n; i++) {
                        assertEquals(bottom - (long) i * step, out.getAtIndex(VectorBuffers.LE_LONG, i));
                    }
                }
            }
        }
    }

    @Test
    void numbersOnlySelectedLanesAndCarriesTheCounter() {
        try (Arena arena = Arena.ofConfined()) {
            Random rnd = new Random(3);
            int n = 777;
            MemorySegment selection = ArrowLayout.allocateBitmap(arena, n);
            Bitmap.fill(selection, n, false);
            int selected = 0;
            for (int i = 0; i < n; i++) {
                if (rnd.nextInt(3) == 0) {
                    Bitmap.set(selection, i);
                    selected++;
                }
            }
            MemorySegment out = ArrowLayout.allocateData(arena, VecType.INT64, n);
            long start = 2L << 33;
            long next = SequenceKernels.iotaSelected(out, n, selection, start);
            assertEquals(start + selected, next);
            long expected = start;
            for (int i = 0; i < n; i++) {
                if (Bitmap.isSet(selection, i)) {
                    assertEquals(expected++, out.getAtIndex(VectorBuffers.LE_LONG, i), "selected lane " + i);
                }
            }
            // A second batch continues where the first stopped, with and without a selection.
            long after = SequenceKernels.iota(out, 10, next);
            assertEquals(next + 10, after);
            assertEquals(next, out.getAtIndex(VectorBuffers.LE_LONG, 0));
            assertEquals(next + 9, out.getAtIndex(VectorBuffers.LE_LONG, 9));
            // No selection given: every lane is numbered.
            assertEquals(start + n, SequenceKernels.iotaSelected(out, n, null, start));
        }
    }
}
