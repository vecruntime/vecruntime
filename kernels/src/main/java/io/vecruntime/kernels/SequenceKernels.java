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

import java.lang.foreign.MemorySegment;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Consecutive INT64 values written into a column: {@code start, start + 1,
 * ...}. Behind {@code monotonically_increasing_id()}, whose value is a
 * per-partition prefix plus the row's number in the partition, and -- with a
 * step -- behind the columnar {@code range()} leaf, whose batches are one
 * arithmetic sequence each. With a selection
 * only the selected rows are numbered (Spark numbers the rows that reach the
 * expression, and a forwarded selection carries rows a filter already dropped);
 * unselected lanes get an arbitrary value and are masked by the caller. Returns
 * the next value, so the caller can carry the counter across batches.
 */
public final class SequenceKernels {
    static final VectorSpecies<Long> L = Species.L;

    /**
     * {@code 0, 1, ..., lanes - 1}: the lane offsets of a block, built once (no
     * per-call array).
     */
    private static final LongVector LANE_INDEX = LongVector.fromArray(L, laneIndex(L.length()), 0);

    private SequenceKernels() {}

    private static long[] laneIndex(int lanes) {
        long[] idx = new long[lanes];
        for (int k = 0; k < lanes; k++) {
            idx[k] = k;
        }
        return idx;
    }

    /**
     * Fills lanes {@code [0, n)} with {@code start ..}; returns {@code start +
     * n}.
     */
    public static long iota(MemorySegment out, int n, long start) {
        int lanes = L.length();
        int i = 0;
        for (; i + lanes <= n; i += lanes) {
            LANE_INDEX.add(start + i).intoMemorySegment(out, (long) i << 3, java.nio.ByteOrder.LITTLE_ENDIAN);
        }
        for (; i < n; i++) {
            out.setAtIndex(VectorBuffers.LE_LONG, i, start + i);
        }
        return start + n;
    }

    /**
     * Fills lanes {@code [0, n)} with the arithmetic sequence {@code start,
     * start + step, start + 2 * step, ...} -- the rows of one batch of Spark's
     * {@code range()} -- and returns {@code start + n * step}, the start of the
     * next batch. The lane offsets {@code 0, step, ..., (lanes - 1) * step} are
     * one broadcast multiply of the lane index and are added to a broadcast of
     * the block's base value, so the loop is one vector add and one store per
     * block; the tail is scalar.
     * Arithmetic wraps: the caller guarantees every value of the batch is
     * within {@code long} (Spark's range never emits a value outside
     * {@code [start, end)}), so no lane can wrap.
     */
    public static long range(MemorySegment out, int n, long start,
            long step) {
        if (step == 1L) {
            return iota(out, n, start);
        }
        int lanes = L.length();
        LongVector laneOffsets = LANE_INDEX.mul(step); // 0, step, ..., (lanes - 1) * step
        long blockStep = lanes * step;
        long base = start;
        int i = 0;
        for (; i + lanes <= n; i += lanes) {
            laneOffsets.add(base).intoMemorySegment(out, (long) i << 3, java.nio.ByteOrder.LITTLE_ENDIAN);
            base += blockStep;
        }
        for (; i < n; i++) {
            out.setAtIndex(VectorBuffers.LE_LONG, i, base);
            base += step;
        }
        return base;
    }

    /**
     * Numbers only the selected lanes of {@code [0, n)} in order; returns the
     * next value.
     */
    public static long iotaSelected(MemorySegment out, int n, MemorySegment selection,
            long start) {
        if (selection == null) {
            return iota(out, n, start);
        }
        long next = start;
        for (int w = 0, words = Bitmap.wordsFor(n);
             w < words;
             w++) {
            long word = Bitmap.wordAt(selection, w, n);
            int base = w << 6;
            while (word != 0L) {
                int k = Long.numberOfTrailingZeros(word);
                out.setAtIndex(VectorBuffers.LE_LONG, base + k, next++);
                word &= word - 1;
            }
        }
        return next;
    }
}
