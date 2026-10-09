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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The inline integer-key path of {@link GroupKeyTable#assign} (#677) against a
 * {@code HashMap}: one INT32, two INT32 or one INT64 key, many groups so the
 * table grows several times, keys that collide in their packed or hashed form
 * (negative values, the high and low INT32 swapped, values equal in their low
 * 32 bits). Batches with no null go the inline way; a batch with nulls, or with
 * a selection, goes the general way into the same table, and after a null group
 * the table stays general -- every case must agree with the reference, and the
 * stored keys and {@link GroupKeyTable#lookup} must agree with the groups.
 */
class GroupKeyTableInlineKeysTest {

    private static VectorBuffers column(Arena arena, VecType t, long[] v,
            boolean[] nulls) {
        if (t == VecType.INT32) {
            int[] a = new int[v.length];
            for (int i = 0; i < v.length; i++) {
                a[i] = (int) v[i];
            }
            return ArrowLayout.ofInts(arena, a, nulls);
        }
        return ArrowLayout.ofLongs(arena, v, nulls);
    }

    private static long value(Random rnd, VecType t, int distinct) {
        long g = rnd.nextInt(distinct);
        return switch (rnd.nextInt(4)) {
            case 0 -> -g; // negative: sign extension must not merge it with a positive packed key
            case 1 -> t == VecType.INT64 ? g + (1L << 32) : g; // equal low 32 bits to `g`
            default -> g;
        };
    }

    @ParameterizedTest
    @CsvSource({
        "INT32, 1, none", "INT32, 2, none", "INT64, 1, none",
        "INT32, 1, nullsLater", "INT32, 2, nullsLater", "INT64, 1, nullsLater",
        "INT32, 2, selection"})
    void groupsMatchTheReference(VecType t, int keys, String twist) {
        VecType[] types = keys == 2 ? new VecType[] {t, t} : new VecType[] {t};
        GroupKeyTable table = new GroupKeyTable(types);
        Map<List<Long>, Integer> reference = new HashMap<>();
        Map<Integer, List<Long>> keyOfGroup = new HashMap<>();
        Random rnd = new Random(677 + keys * 31 + twist.hashCode());
        int distinct = 30_000;
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < 40; b++) {
                int n = b % 7 == 3 ? 17 : 4096;
                boolean withNulls = twist.equals("nullsLater") && b == 25;
                boolean withSelection = twist.equals("selection") && b % 5 == 4;
                long[][] v = new long[keys][n];
                boolean[][] nul = new boolean[keys][];
                for (int c = 0; c < keys; c++) {
                    nul[c] = withNulls ? new boolean[n] : null;
                    for (int i = 0; i < n; i++) {
                        v[c][i] = value(rnd, t, distinct);
                        if (withNulls && rnd.nextInt(5) == 0) {
                            nul[c][i] = true;
                            v[c][i] = 0;
                        }
                    }
                }
                if (keys == 2) {
                    // The pair swapped: (a, b) and (b, a) are different groups.
                    for (int i = 0; i < n; i += 11) {
                        v[1][i] = v[0][i];
                        v[0][i] = v[1][(i + 1) % n];
                    }
                }
                VectorBuffers[] cols = new VectorBuffers[keys];
                for (int c = 0; c < keys; c++) {
                    cols[c] = column(arena, t, v[c], nul[c]);
                }
                MemorySegment selection = null;
                boolean[] selected = new boolean[n];
                Arrays.fill(selected, true);
                if (withSelection) {
                    int[] rows = java.util.stream.IntStream.range(0, n)
                            .filter(i -> i % 3 != 0)
                            .toArray();
                    selection = ArrowLayout.selectionFromIndices(arena, rows, rows.length, n);
                    Arrays.fill(selected, false);
                    for (int r : rows) {
                        selected[r] = true;
                    }
                }
                int[] ids = new int[n];
                int groups = table.assign(cols, n, ids, selection);
                for (int i = 0; i < n; i++) {
                    if (!selected[i]) {
                        assertEquals(-1, ids[i], "unselected row " + i + " of batch " + b);
                        continue;
                    }
                    List<Long> tuple = new ArrayList<>();
                    for (int c = 0; c < keys; c++) {
                        tuple.add(nul[c] != null && nul[c][i]
                                ? null
                                : (t == VecType.INT32 ? (long) (int) v[c][i] : v[c][i]));
                    }
                    Integer known = reference.get(tuple);
                    if (known == null) {
                        assertEquals(null, keyOfGroup.get(ids[i]), "row "
                                + i
                                + " of batch "
                                + b
                                + " "
                                + tuple
                                + " joined the group of "
                                + keyOfGroup.get(ids[i]));
                        reference.put(tuple, ids[i]);
                        keyOfGroup.put(ids[i], tuple);
                    } else {
                        assertEquals(known.intValue(), ids[i], "row " + i + " of batch " + b + " " + tuple);
                    }
                }
                assertEquals(reference.size(), groups, "groups after batch " + b);
                if (selection == null) {
                    int[] probe = new int[n];
                    assertEquals(n, table.lookup(cols, n, probe, null), "lookup batch " + b);
                    assertEquals(Arrays.toString(ids), Arrays.toString(probe), "lookup ids batch " + b);
                }
            }
        }
        // Every group's stored key is the tuple it was created for.
        for (Map.Entry<Integer, List<Long>> e : keyOfGroup.entrySet()) {
            int gid = e.getKey();
            for (int c = 0; c < keys; c++) {
                Long want = e.getValue().get(c);
                assertEquals(want == null, table.isNull(c, gid), "null of group " + gid + " column " + c);
                if (want != null) {
                    long got = t == VecType.INT32 ? table.getInt(c, gid) : table.getLong(c, gid);
                    assertEquals(want.longValue(), got, "key of group " + gid + " column " + c);
                }
            }
        }
    }
}
