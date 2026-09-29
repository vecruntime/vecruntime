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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DenseKeyIndex} against {@link GroupKeyTable#lookup}, the probe it
 * stands in for (#546).
 */
class DenseKeyIndexTest {

    private static VectorBuffers column(Arena arena, VecType t, long[] values,
            boolean[] nulls) {
        if (t == VecType.INT64) {
            return ArrowLayout.ofLongs(arena, values.clone(), nulls);
        }
        int[] ints = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            ints[i] = (int) values[i];
        }
        return ArrowLayout.ofInts(arena, ints, nulls);
    }

    /**
     * The rows whose key is non-null, as the join passes them (null when every
     * row is).
     */
    private static MemorySegment nonNull(Arena arena, boolean[] nulls, int n) {
        if (nulls == null) {
            return null;
        }
        MemorySegment bm = Bitmap.allocate(arena, n);
        for (int i = 0; i < n; i++) {
            Bitmap.setTo(bm, i, !nulls[i]);
        }
        return bm;
    }

    @ParameterizedTest
    @ValueSource(strings = {"INT32", "INT64"})
    void matchesTheHashTableOnRandomKeys(String typeName) {
        VecType t = VecType.valueOf(typeName);
        Random rnd = new Random(546);
        try (Arena arena = Arena.ofConfined()) {
            int indexed = 0;
            for (int trial = 0; trial < 40; trial++) {
                long base = t == VecType.INT64
                        ? (rnd.nextBoolean() ? 1L << 40 : -(1L << 40))
                        : rnd.nextInt(2001) - 1000;
                int span = 1 + rnd.nextInt(3000);
                int nb = 1 + rnd.nextInt(2000);
                long[] build = new long[nb];
                boolean[] buildNulls = rnd.nextBoolean() ? new boolean[nb] : null;
                for (int i = 0; i < nb; i++) {
                    build[i] = base + rnd.nextInt(span); // duplicates included
                    if (buildNulls != null) {
                        buildNulls[i] = rnd.nextInt(10) == 0;
                    }
                }
                VectorBuffers bk = column(arena, t, build, buildNulls);
                GroupKeyTable table = new GroupKeyTable(new VecType[] {t}, true);
                int[] ids = new int[nb];
                int groups = table.assign(new VectorBuffers[] {bk}, nb, ids, nonNull(arena, buildNulls, nb));
                DenseKeyIndex dense = DenseKeyIndex.tryBuild(bk, nb, ids, groups);
                if (dense == null) {
                    continue; // the range rule declined (few distinct keys over a wide span)
                }
                // Probe keys around and outside the range, with nulls and odd lengths.
                int np = 1 + rnd.nextInt(5000);
                long[] probe = new long[np];
                boolean[] probeNulls = rnd.nextBoolean() ? new boolean[np] : null;
                for (int i = 0; i < np; i++) {
                    probe[i] = base - 50 + rnd.nextInt(span + 100);
                    if (probeNulls != null) {
                        probeNulls[i] = rnd.nextInt(7) == 0;
                    }
                }
                VectorBuffers pk = column(arena, t, probe, probeNulls);
                MemorySegment sel = nonNull(arena, probeNulls, np);
                int[] expected = new int[np];
                int expectedMatched = table.lookup(new VectorBuffers[] {pk}, np, expected, sel);
                int[] actual = new int[np];
                int matched = dense.lookup(pk, np, actual, sel, new DenseKeyIndex.Scratch());
                assertArrayEquals(expected, actual, "trial " + trial);
                assertEquals(expectedMatched, matched, "trial " + trial);
                indexed++;
            }
            assertTrue(indexed >= 20, "only " + indexed + " of 40 trials built a dense index");
        }
    }

    @Test
    void honoursASelectionThatIsNotJustTheNulls() {
        try (Arena arena = Arena.ofConfined()) {
            long[] build = {1, 2, 3, 4, 5};
            VectorBuffers bk = column(arena, VecType.INT32, build, null);
            int[] ids = {0, 1, 2, 3, 4};
            DenseKeyIndex dense = DenseKeyIndex.tryBuild(bk, 5, ids, 5);
            assertNotNull(dense);
            int n = 130; // past two words, a partial third
            long[] probe = new long[n];
            for (int i = 0; i < n; i++) {
                probe[i] = 1L + (i % 5);
            }
            MemorySegment sel = Bitmap.allocate(arena, n);
            for (int i = 0; i < n; i += 3) {
                Bitmap.set(sel, i);
            }
            int[] out = new int[n];
            int matched = dense.lookup(column(arena, VecType.INT32, probe, null), n, out, sel,
                    new DenseKeyIndex.Scratch());
            int expected = 0;
            for (int i = 0; i < n; i++) {
                if (i % 3 == 0) {
                    assertEquals(i % 5, out[i], "row " + i);
                    expected++;
                } else {
                    assertEquals(-1, out[i], "row " + i);
                }
            }
            assertEquals(expected, matched);
        }
    }

    @Test
    void declinesAWideRangeAndNonIntegerKeys() {
        try (Arena arena = Arena.ofConfined()) {
            // 3 distinct keys over a range of 1,000: past DENSE_FACTOR x keys.
            VectorBuffers wide = column(arena, VecType.INT32, new long[] {0, 500, 999},
                    null);
            assertNull(DenseKeyIndex.tryBuild(wide, 3, new int[] {0, 1, 2},
                    3));
            // Past MAX_RANGE even with many keys.
            VectorBuffers far = column(arena, VecType.INT64, new long[] {0, 1L << 30},
                    null);
            assertNull(DenseKeyIndex.tryBuild(far, 2, new int[] {0, 1}, 2));
            // Extreme values: the range computation must not overflow into "small".
            VectorBuffers extremes = column(arena, VecType.INT64, new long[] {Long.MIN_VALUE, Long.MAX_VALUE}, null);
            assertNull(DenseKeyIndex.tryBuild(extremes, 2, new int[] {0, 1}, 2));
            VectorBuffers doubles = ArrowLayout.ofDoubles(arena, new double[] {1, 2}, null);
            assertNull(DenseKeyIndex.tryBuild(doubles, 2, new int[] {0, 1}, 2));
            // Every build row excluded (all keys null): nothing to index.
            assertNull(DenseKeyIndex.tryBuild(column(arena, VecType.INT32, new long[] {1, 2}, null), 2,
                    new int[] {-1, -1}, 0));
        }
    }

    @Test
    void acceptsOnlyItsOwnPlainType() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers bk = column(arena, VecType.INT32, new long[] {7, 8}, null);
            DenseKeyIndex dense = DenseKeyIndex.tryBuild(bk, 2, new int[] {0, 1}, 2);
            assertNotNull(dense);
            assertEquals(2, dense.range());
            assertTrue(dense.accepts(bk));
            assertFalse(dense.accepts(column(arena, VecType.INT64, new long[] {7}, null)));
        }
    }

    @Test
    void probesAtTheEdgesOfTheIntRange() {
        try (Arena arena = Arena.ofConfined()) {
            long[] build = {Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
            VectorBuffers bk = column(arena, VecType.INT32, build, null);
            DenseKeyIndex dense = DenseKeyIndex.tryBuild(bk, 2, new int[] {0, 1}, 2);
            assertNotNull(dense);
            long[] probe = {Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE - 1, 0};
            int[] out = new int[4];
            dense.lookup(column(arena, VecType.INT32, probe, null), 4, out, null,
                    new DenseKeyIndex.Scratch());
            assertArrayEquals(new int[] {-1, 1, 0, -1}, out);
        }
    }
}
