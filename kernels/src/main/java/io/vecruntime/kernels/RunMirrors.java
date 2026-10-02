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

/**
 * Heap mirrors of one output column's sorted runs, for {@link RunMerge}'s
 * gathers (#565): each run's UTF8 offsets as an {@code int[]} and its validity as
 * 64-row words, built once per run column and reused for every output batch the
 * merge emits from it (a spilled run's refilled piece is a new column and is
 * mirrored again). Reading those per row from the segments paid a liveness and a
 * bounds check per access -- on the 1 TB executors that held even for a segment
 * loaded once per stretch of one run, which is why this copies instead.
 *
 * <p>Only offsets and validity are mirrored (4 bytes and one bit per row); the
 * values themselves stay in the runs' segments. One instance per output column
 * of a merge, owned by the operator.
 */
public final class RunMirrors {
    private VectorBuffers[] bound = new VectorBuffers[0];
    private int[][] offsets = new int[0][];
    private long[][] validity = new long[0][];

    /**
     * Makes the mirrors match {@code sources}: a run whose column object
     * changed since the last call is mirrored again, the others are kept.
     */
    @SuppressWarnings("ReferenceEquality") // the mirrors are keyed on each run's column object itself
    public void bind(VectorBuffers[] sources) {
        int k = sources.length;
        if (bound.length < k) {
            bound = java.util.Arrays.copyOf(bound, k);
            offsets = java.util.Arrays.copyOf(offsets, k);
            validity = java.util.Arrays.copyOf(validity, k);
        }
        for (int r = 0; r < k; r++) {
            VectorBuffers s = sources[r];
            if (s == bound[r]) {
                continue;
            }
            bound[r] = s;
            int n = s.length();
            if (s.type() == VecType.UTF8 && !s.isDictionaryEncoded()) {
                int[] off = offsets[r];
                if (off == null || off.length < n + 1) {
                    off = new int[n + 1];
                    offsets[r] = off;
                }
                MemorySegment.copy(s.offsets(), VectorBuffers.LE_INT, 0L, off, 0,
                        n + 1);
            } else {
                offsets[r] = null;
            }
            MemorySegment v = s.validity();
            if (v == null) {
                validity[r] = null;
            } else {
                int words = Bitmap.wordsFor(n);
                long[] w = new long[words];
                for (int i = 0; i < words; i++) {
                    w[i] = Bitmap.wordAt(v, i, n);
                }
                validity[r] = w;
            }
        }
    }

    /**
     * Run {@code r}'s offsets ({@code length + 1} entries), or null when it is
     * not plain UTF8.
     */
    int[] offsets(int r) {
        return offsets[r];
    }

    /** Run {@code r}'s validity words, or null when every row is valid. */
    long[] validity(int r) {
        return validity[r];
    }

    static boolean valid(long[] words, int i) {
        return words == null || ((words[i >>> 6] >>> i) & 1L) != 0L;
    }
}
