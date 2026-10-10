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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The per-thread map from an input dictionary's entries to a table's ids
 * ({@code GroupKeyTable.toIds}) is reused while the same dictionary object keeps
 * arriving. It must not be reused by another table on the same thread, nor by
 * an insert after a lookup cached that table's misses as -1.
 */
class GroupKeyTableEntryCacheTest {

    private static VectorBuffers col(Arena arena, VectorBuffers dict, int... idx) {
        VectorBuffers ix = ArrowLayout.ofInts(arena, idx, new boolean[idx.length]);
        return SegmentVectorBuffers.dictionaryUtf8(idx.length, ix.validity(), ix.data(), dict);
    }

    @Test
    void anInsertAfterALookupOnTheSameDictionaryAddsTheKeys() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers dict = ArrowLayout.ofStrings(arena, new String[] {"a", "b", "c"});
            VectorBuffers c = col(arena, dict, 0, 1, 2, 0);
            GroupKeyTable t = new GroupKeyTable(new VecType[] {VecType.UTF8}, true);
            int[] probe = new int[4];
            assertEquals(0, t.lookup(new VectorBuffers[] {c}, 4, probe, null), "an empty table finds nothing");
            int[] ids = new int[4];
            assertEquals(3, t.assign(new VectorBuffers[] {c}, 4, ids), "three distinct keys");
            assertEquals(ids[0], ids[3], "a and a");
            assertNotEquals(ids[0], ids[1], "a and b");
            assertNotEquals(ids[1], ids[2], "b and c");
            assertNotEquals(ids[0], ids[2], "a and c");
            // A lookup after the insert, on the same dictionary, finds every key.
            assertEquals(4, t.lookup(new VectorBuffers[] {c}, 4, probe, null), "all found");
            for (int i = 0; i < 4; i++) {
                assertEquals(ids[i], probe[i], "row " + i);
            }
        }
    }

    @Test
    void anInsertAfterALookupThatReusedAnInsertsMapAddsTheMissedKeys() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers dict = ArrowLayout.ofStrings(arena, new String[] {"a", "b", "c", "d"});
            GroupKeyTable t = new GroupKeyTable(new VecType[] {VecType.UTF8}, true);
            // The insert maps entries a and b only.
            VectorBuffers first = col(arena, dict, 0, 1);
            int[] ab = new int[2];
            assertEquals(2, t.assign(new VectorBuffers[] {first}, 2, ab), "a and b");
            // A lookup over the same dictionary object reuses that map and misses c and d (#593).
            VectorBuffers second = nullableCol(
                    arena,
                    dict,
                    new int[] {0, 2, 3, 0},
                    new boolean[] {false, false, false, true});
            int[] probe = new int[4];
            assertEquals(1, t.lookup(new VectorBuffers[] {second}, 4, probe, null), "only a is found");
            assertEquals(ab[0], probe[0], "a");
            assertEquals(-1, probe[1], "c is not in the table");
            assertEquals(-1, probe[2], "d is not in the table");
            assertEquals(-1, probe[3], "nor is null");
            // The insert after it adds c, d and null as three new groups, not one.
            int[] ids = new int[4];
            assertEquals(5, t.assign(new VectorBuffers[] {second}, 4, ids), "a, b, c, d and null");
            assertEquals(ab[0], ids[0], "a keeps its group");
            assertNotEquals(ids[1], ids[2], "c and d");
            assertNotEquals(ids[1], ids[3], "c and null");
            assertNotEquals(ids[2], ids[3], "d and null");
        }
    }

    private static VectorBuffers nullableCol(Arena arena, VectorBuffers dict, int[] idx,
            boolean[] nulls) {
        VectorBuffers ix = ArrowLayout.ofInts(arena, idx, nulls);
        return SegmentVectorBuffers.dictionaryUtf8(idx.length, ix.validity(), ix.data(), dict);
    }

    @Test
    void twoTablesOnOneThreadKeepTheirOwnIds() {
        try (Arena arena = Arena.ofConfined()) {
            GroupKeyTable t1 = new GroupKeyTable(new VecType[] {VecType.UTF8}, true);
            GroupKeyTable t2 = new GroupKeyTable(new VecType[] {VecType.UTF8}, true);
            // t1 learns c, b, a first, so its dictionary ids differ from t2's.
            t1.assign(new VectorBuffers[] {col(arena, ArrowLayout.ofStrings(arena, new String[] {"c", "b", "a"}), 0,
                    1, 2)
                },
                    3, new int[3]);
            VectorBuffers dict = ArrowLayout.ofStrings(arena, new String[] {"a", "b", "c"});
            VectorBuffers c = col(arena, dict, 0, 1, 2);
            int[] a1 = new int[3];
            assertEquals(3, t1.assign(new VectorBuffers[] {c}, 3, a1), "t1 keeps three keys");
            // The same dictionary object reaches t2 on the same thread.
            int[] a2 = new int[3];
            assertEquals(3, t2.assign(new VectorBuffers[] {c}, 3, a2), "t2 has three keys");
            VectorBuffers fresh = col(arena, ArrowLayout.ofStrings(arena, new String[] {"a", "b", "c"}), 0,
                    1, 2);
            int[] again = new int[3];
            assertEquals(3, t2.lookup(new VectorBuffers[] {fresh}, 3, again, null), "t2 finds a, b, c");
            for (int i = 0; i < 3; i++) {
                assertEquals(a2[i], again[i], "t2 row " + i);
            }
            int[] back = new int[3];
            assertEquals(3, t1.lookup(new VectorBuffers[] {c}, 3, back, null), "t1 finds a, b, c");
            for (int i = 0; i < 3; i++) {
                assertEquals(a1[i], back[i], "t1 row " + i);
            }
        }
    }
}
