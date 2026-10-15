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
import java.lang.foreign.ValueLayout;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The packed multi-key path of {@link GroupKeyTable#assign} (#692) against
 * the same table with the path disabled, fed the same batches: every row's
 * group id, the group count, the stored keys and a later {@link
 * GroupKeyTable#lookup} must be identical. Keys mix dictionary-encoded and
 * plain UTF8, INT32, INT64 and BOOL columns with nulls, selections, tiny
 * batches and growing cardinality (each code width widens several times);
 * the path must also turn itself off -- with the same answers -- when the
 * codes outgrow 64 bits, most rows are new groups, or a UTF8 column turns to
 * records.
 */
class GroupKeyTablePackedKeysTest {

    private static final VecType[] Q24 = {VecType.UTF8, VecType.UTF8, VecType.UTF8, VecType.INT32, VecType.INT64, VecType.UTF8,
            VecType.BOOL};

    /**
     * One batch: per column the values (String / Integer / Long / Boolean, null
     * = null).
     */
    private static Object[][] batch(Random rnd, VecType[] types, int n,
            int[] distinct, double nullRate) {
        Object[][] v = new Object[types.length][n];
        for (int c = 0; c < types.length; c++) {
            for (int i = 0; i < n; i++) {
                if (rnd.nextDouble() < nullRate) {
                    continue;
                }
                int g = rnd.nextInt(distinct[c]);
                v[c][i] = switch (types[c]) {
                            case UTF8 -> "v" + c + "-" + g;
                            case INT32 -> g % 2 == 0 ? g : -g; // negative values must not collide with positive codes
                            case INT64 -> g % 3 == 0 ? (long) g + (1L << 40) : (long) g;
                            case BOOL -> g % 2 == 0;
                            default -> throw new IllegalArgumentException();
                        };
            }
        }
        return v;
    }

    /**
     * The batch as columns; UTF8 column {@code c} dictionary encoded when
     * {@code dict} says so.
     */
    private static VectorBuffers[] columns(Arena arena, VecType[] types, Object[][] v,
            int n, boolean dict) {
        VectorBuffers[] cols = new VectorBuffers[types.length];
        for (int c = 0; c < types.length; c++) {
            boolean[] nulls = new boolean[n];
            for (int i = 0; i < n; i++) {
                nulls[i] = v[c][i] == null;
            }
            cols[c] = switch (types[c]) {
                        case UTF8 -> {
                            String[] s = new String[n];
                            for (int i = 0; i < n; i++) {
                                s[i] = (String) v[c][i];
                            }
                            if (!dict || c % 2 == 1) {
                                yield ArrowLayout.ofStrings(arena, s);
                            }
                            // Dictionary encoded as a join's output is: the batch indexes a larger dictionary of rows.
                            String[] entries = new String[n * 2];
                            MemorySegment idx = arena.allocate(ValueLayout.JAVA_INT, Math.max(1, n));
                            for (int i = 0; i < n; i++) {
                                entries[2 * i + 1] = s[i] == null ? "unused" : s[i];
                                entries[2 * i] = "decoy-" + i;
                                idx.setAtIndex(ValueLayout.JAVA_INT, i, 2 * i + 1);
                            }
                            yield SegmentVectorBuffers.dictionaryUtf8(n, ArrowLayout.validityFrom(arena, nulls, n), idx, ArrowLayout.ofStrings(arena, entries));
                        }
                        case INT32 -> {
                            int[] a = new int[n];
                            for (int i = 0; i < n; i++) {
                                a[i] = v[c][i] == null ? 0 : (Integer) v[c][i];
                            }
                            yield ArrowLayout.ofInts(arena, a, nulls);
                        }
                        case INT64 -> {
                            long[] a = new long[n];
                            for (int i = 0; i < n; i++) {
                                a[i] = v[c][i] == null ? 0L : (Long) v[c][i];
                            }
                            yield ArrowLayout.ofLongs(arena, a, nulls);
                        }
                        case BOOL -> {
                            boolean[] a = new boolean[n];
                            for (int i = 0; i < n; i++) {
                                a[i] = v[c][i] != null && (Boolean) v[c][i];
                            }
                            yield ArrowLayout.ofBooleans(arena, a, nulls);
                        }
                        default -> throw new IllegalArgumentException();
                    };
        }
        return cols;
    }

    /**
     * Feeds both tables the same batches; returns whether the packed table
     * still took the packed path at the end.
     */
    private static boolean compare(
            VecType[] types,
            GroupKeyTable packed,
            GroupKeyTable general,
            int batches,
            java.util.function.IntFunction<int[]> distinctOfBatch,
            double nullRate,
            boolean selections,
            long seed) {
        general.disablePacked();
        Random rnd = new Random(seed);
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < batches; b++) {
                int n = b % 9 == 4
                        ? 7
                        : (b % 13 == 6 ? 0 : 4096);
                Object[][] v = batch(rnd, types, n, distinctOfBatch.apply(b), nullRate);
                VectorBuffers[] cols = columns(arena, types, v, n, b % 2 == 0);
                MemorySegment sel = null;
                if (selections && b % 3 == 1) {
                    boolean[] keep = new boolean[n];
                    for (int i = 0; i < n; i++) {
                        keep[i] = rnd.nextInt(3) != 0;
                    }
                    sel = ArrowLayout.ofBooleans(arena, keep, null).data();
                }
                int[] a = new int[n];
                int[] g = new int[n];
                int sa = packed.assign(cols, n, a, sel);
                int sg = general.assign(cols, n, g, sel);
                assertArrayEquals(g, a, "batch " + b);
                assertEquals(sg, sa, "groups after batch " + b);
                if (b % 10 == 9) {
                    int[] la = new int[n];
                    int[] lg = new int[n];
                    packed.lookup(cols, n, la, null);
                    general.lookup(cols, n, lg, null);
                    assertArrayEquals(lg, la, "lookup after batch " + b);
                }
            }
        }
        for (int c = 0; c < types.length; c++) {
            for (int gid = 0; gid < general.size(); gid++) {
                assertEquals(general.isNull(c, gid), packed.isNull(c, gid));
                if (general.isNull(c, gid)) {
                    continue;
                }
                switch (types[c]) {
                    case UTF8 -> assertEquals(general.getString(c, gid), packed.getString(c, gid));
                    case INT32 -> assertEquals(general.getInt(c, gid), packed.getInt(c, gid));
                    case INT64 -> assertEquals(general.getLong(c, gid), packed.getLong(c, gid));
                    case BOOL -> assertEquals(general.getBoolean(c, gid), packed.getBoolean(c, gid));
                    default -> throw new IllegalArgumentException();
                }
            }
        }
        return packed.packedLookup();
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.0, 0.05, 0.4})
    void q24ShapedKeysTakeThePackedPathAndAgree(double nullRate) {
        // Few groups, many rows: the packed path stays on. Cardinality grows over the batches, so every column's
        // code width widens several times and the packed table refills each time.
        boolean on = compare(
                Q24,
                new GroupKeyTable(Q24),
                new GroupKeyTable(Q24),
                60,
                b -> new int[] {2 + b / 20, 3 + b / 20, 2,
                        3 + b / 15, 2 + b / 15, 3, 2},
                nullRate,
                true,
                692);
        assertTrue(on, "the packed path should still be on for few groups");
    }

    @Test
    void mostlyNewGroupsTurnThePathOffWithTheSameAnswers() {
        VecType[] types = {VecType.UTF8, VecType.INT64};
        boolean on = compare(
                types,
                new GroupKeyTable(types),
                new GroupKeyTable(types),
                40,
                b -> new int[] {20_000, 50_000},
                0.01,
                false,
                7);
        assertFalse(on, "a stream of new groups should leave the packed path");
    }

    @Test
    void codesBeyondOneHundredTwentyEightBitsTurnThePathOff() {
        // Ten integer keys of up to 8191 values each need 10 x 14 bits once the widths have their headroom: eight
        // fill the two words, the ninth does not fit.
        VecType[] types = new VecType[10];
        java.util.Arrays.fill(types, VecType.INT64);
        types[3] = VecType.INT32;
        int[] distinct = new int[10];
        java.util.Arrays.fill(distinct, 8191);
        boolean on = compare(types, new GroupKeyTable(types), new GroupKeyTable(types), 12,
                b -> distinct, 0.0, true, 11);
        assertFalse(on);
    }

    @Test
    void keysWiderThanSixtyFourBitsUseBothWords() {
        // Five keys of ~4000 values each: 5 x 13 = 65 bits, so one code lives in the high word. Few groups, so the
        // path stays on.
        VecType[] types = {VecType.UTF8, VecType.INT64, VecType.UTF8, VecType.INT32, VecType.INT64};
        GroupKeyTable packed = new GroupKeyTable(types);
        // Correlated columns (all from one group number) keep the groups few although each column has many values.
        boolean on = compareCorrelated(types, packed, new GroupKeyTable(types), 4000, 40,
                692);
        assertTrue(on);
    }

    /**
     * Every column a function of one group number below {@code groups}, so
     * groups stay few.
     */
    private static boolean compareCorrelated(VecType[] types, GroupKeyTable packed, GroupKeyTable general,
            int groups, int batches, long seed) {
        general.disablePacked();
        Random rnd = new Random(seed);
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < batches; b++) {
                int n = 4096;
                Object[][] v = new Object[types.length][n];
                for (int i = 0; i < n; i++) {
                    int g = rnd.nextInt(groups);
                    for (int c = 0; c < types.length; c++) {
                        int x = (g * (c + 7)) % groups;
                        v[c][i] = switch (types[c]) {
                                    case UTF8 -> "s" + c + "-" + x;
                                    case INT32 -> -x;
                                    case INT64 -> (long) x << 33;
                                    case BOOL -> x % 2 == 0;
                                    default -> throw new IllegalArgumentException();
                                };
                    }
                }
                VectorBuffers[] cols = columns(arena, types, v, n, b % 2 == 0);
                int[] a = new int[n];
                int[] gg = new int[n];
                assertEquals(general.assign(cols, n, gg, null), packed.assign(cols, n, a, null));
                assertArrayEquals(gg, a, "batch " + b);
            }
        }
        return packed.packedLookup();
    }

    @Test
    void aUtf8ColumnTurningToRecordsTurnsThePathOff() {
        VecType[] types = {VecType.UTF8, VecType.INT32};
        // A dictionary limit of 50 values: the string column turns to records mid-stream.
        boolean on = compare(
                types,
                new GroupKeyTable(types, 50),
                new GroupKeyTable(types, 50),
                30,
                b -> new int[] {b < 10 ? 20 : 400, 6},
                0.05,
                true,
                13);
        assertFalse(on);
    }

    @Test
    void oneKeyOrAnUnsupportedTypeNeverTakesThePath() {
        assertFalse(new GroupKeyTable(new VecType[] {VecType.UTF8}).packedLookup());
        assertFalse(new GroupKeyTable(new VecType[] {VecType.INT32, VecType.INT32}).packedLookup()); // inline keys
        assertFalse(new GroupKeyTable(new VecType[] {VecType.UTF8, VecType.DECIMAL128}).packedLookup());
        assertFalse(new GroupKeyTable(new VecType[] {VecType.UTF8, VecType.FLOAT64}).packedLookup());
        assertFalse(new GroupKeyTable(new VecType[] {VecType.UTF8, VecType.INT32}, false).packedLookup());
        assertTrue(new GroupKeyTable(new VecType[] {VecType.UTF8, VecType.INT32}).packedLookup());
    }
}
