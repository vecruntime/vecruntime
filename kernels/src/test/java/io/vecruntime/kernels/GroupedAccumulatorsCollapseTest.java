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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The accumulators that keep {@link GroupedAccumulators#INTERLEAVE} copies
 * collapse to one past {@link GroupedAccumulators#COLLAPSE_GROUPS} groups
 * (#689): sums and counts must be exact across the collapse, with the rows of
 * a batch spread over every copy before it, nulls and selections. Run with
 * {@code -Dvecruntime.agg.interleave=4} (the Graviton default) to exercise the
 * collapse; with one copy it checks the same answers without it.
 */
class GroupedAccumulatorsCollapseTest {

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void sumsAndCountsSurviveTheCollapse(boolean nulls, boolean selections) {
        Random rnd = new Random(689
                + (nulls ? 1 : 0)
                + (selections ? 2 : 0));
        int max = GroupedAccumulators.COLLAPSE_GROUPS * 5;
        GroupedAccumulators.LongSum lsum = new GroupedAccumulators.LongSum();
        GroupedAccumulators.LongSum checked = new GroupedAccumulators.LongSum(true);
        GroupedAccumulators.DoubleSum dsum = new GroupedAccumulators.DoubleSum();
        GroupedAccumulators.Count all = new GroupedAccumulators.Count();
        GroupedAccumulators.Count nonNull = new GroupedAccumulators.Count();
        long[] refSum = new long[max];
        long[] refCount = new long[max];
        long[] refRows = new long[max];
        int groups = 0;
        for (int b = 0; groups < max; b++) {
            // Groups grow batch by batch through the threshold; most rows go to the early (busy) groups, so each
            // early group has values in every copy before the collapse.
            groups = Math.min(max, 64 << b);
            int n = 4096;
            try (Arena arena = Arena.ofConfined()) {
                int[] ids = new int[n];
                long[] v = new long[n];
                double[] d = new double[n];
                boolean[] nul = new boolean[n];
                for (int i = 0; i < n; i++) {
                    ids[i] = rnd.nextInt(4) == 0 ? rnd.nextInt(groups) : rnd.nextInt(Math.min(groups, 16));
                    v[i] = rnd.nextLong(2_000_001L) - 1_000_000L;
                    d[i] = v[i]; // integral doubles: their sums are exact whatever the order
                    nul[i] = nulls && rnd.nextInt(5) == 0;
                }
                MemorySegment sel = selections && b % 2 == 1
                        ? TestData.randomBitmap(arena, rnd, n)
                        : null;
                if (sel != null) {
                    for (int i = 0; i < n; i++) {
                        if (!Bitmap.isSet(sel, i)) {
                            ids[i] = -1;
                        }
                    }
                }
                VectorBuffers longs = ArrowLayout.ofLongs(arena, v, nulls ? nul : null);
                VectorBuffers doubles = ArrowLayout.ofDoubles(arena, d, nulls ? nul : null);
                GroupAssignment a = GroupAssignment.of(ids, n, groups, arena, sel);
                lsum.update(longs, a);
                checked.update(longs, a);
                dsum.update(doubles, a);
                all.updateAll(a);
                nonNull.updateNonNull(longs, a);
                for (int i = 0; i < n; i++) {
                    if (ids[i] < 0) {
                        continue;
                    }
                    refRows[ids[i]]++;
                    if (!nul[i]) {
                        refSum[ids[i]] += v[i];
                        refCount[ids[i]]++;
                    }
                }
            }
            for (int g = 0; g < groups; g++) {
                assertEquals(refSum[g], lsum.sum(g), "sum, group " + g + " after batch " + b);
                assertEquals(refSum[g], checked.sum(g), "checked sum, group " + g);
                assertEquals(refCount[g], lsum.count(g), "sum's count, group " + g);
                assertEquals((double) refSum[g], dsum.sum(g),
                        "double sum, group " + g);
                assertEquals(refCount[g], dsum.count(g), "double sum's count, group " + g);
                assertEquals(refRows[g], all.count(g), "count(*), group " + g);
                assertEquals(refCount[g], nonNull.count(g), "count(x), group " + g);
            }
        }
    }
}
