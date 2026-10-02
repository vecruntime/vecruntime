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
