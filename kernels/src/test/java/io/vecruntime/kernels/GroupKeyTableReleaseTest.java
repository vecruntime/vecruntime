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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #667: {@link GroupKeyTable#releaseThreadScratch} leaves the thread holding no
 * batch, dictionary or table and none of the arrays a large build side grew the
 * per-thread scratch to, and the tables still assign and probe correctly
 * afterwards (the per-thread dictionary entry maps are rebuilt).
 */
class GroupKeyTableReleaseTest {

    private static final int ROWS = 3 * GroupKeyTable.KEEP_ELEMENTS;

    private static VectorBuffers[] keys(Arena arena, VectorBuffers dict, int rows) {
        int[] i32 = new int[rows];
        long[] i64 = new long[rows];
        int[] idx = new int[rows];
        boolean[] nulls = new boolean[rows];
        for (int r = 0; r < rows; r++) {
            i32[r] = r % 1000;
            i64[r] = r / 1000;
            idx[r] = r % 3;
            nulls[r] = r % 97 == 0;
        }
        VectorBuffers ix = ArrowLayout.ofInts(arena, idx, new boolean[rows]);
        return new VectorBuffers[] {
            ArrowLayout.ofInts(arena, i32, nulls), ArrowLayout.ofLongs(arena, i64, new boolean[rows]),
                    SegmentVectorBuffers.dictionaryUtf8(rows, ix.validity(), ix.data(), dict),};
    }

    @Test
    void releaseDropsLargeScratchAndReferencesAndTablesStillWork() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers dict = ArrowLayout.ofStrings(arena, new String[] {"a", "b", "c"});
            VectorBuffers[] k = keys(arena, dict, ROWS);
            VecType[] types = {VecType.INT32, VecType.INT64, VecType.UTF8};
            GroupKeyTable build = new GroupKeyTable(types, true);
            int[] ids = new int[ROWS];
            int groups = build.assign(k, ROWS, ids);
            int[] probe = new int[ROWS];
            assertEquals(ROWS, build.lookup(k, ROWS, probe, null), "every row found before the release");

            long[] before = GroupKeyTable.threadScratchFootprint();
            assertTrue(before[0] >= ROWS, "the build side grew the scratch to its row count: " + before[0]);
            assertTrue(before[1] > 0, "the scratch references the last batch");

            GroupKeyTable.releaseThreadScratch();
            long[] after = GroupKeyTable.threadScratchFootprint();
            assertTrue(after[0] <= GroupKeyTable.KEEP_ELEMENTS, "large arrays dropped: " + after[0]);
            assertEquals(0, after[1], "no batch, dictionary or table referenced");

            // The same table and the same dictionary object afterwards: same ids, no new groups.
            int[] again = new int[ROWS];
            assertEquals(ROWS, build.lookup(k, ROWS, again, null), "every row found after the release");
            int[] reassigned = new int[ROWS];
            assertEquals(groups, build.assign(k, ROWS, reassigned), "no new groups after the release");
            for (int r = 0; r < ROWS; r++) {
                assertEquals(ids[r], again[r], "lookup row " + r);
                assertEquals(ids[r], reassigned[r], "assign row " + r);
            }

            // A second table on this thread after a release keeps its own ids.
            GroupKeyTable other = new GroupKeyTable(types, true);
            int[] small = new int[300];
            VectorBuffers[] head = keys(arena, dict, 300);
            other.assign(head, 300, small);
            GroupKeyTable.releaseThreadScratch();
            int[] found = new int[300];
            assertEquals(300, other.lookup(head, 300, found, null), "the second table finds its rows");
            for (int r = 0; r < 300; r++) {
                assertEquals(small[r], found[r], "second table row " + r);
            }
        }
    }

    @Test
    void releaseKeepsBatchSizedScratch() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers dict = ArrowLayout.ofStrings(arena, new String[] {"a", "b", "c"});
            VectorBuffers[] k = keys(arena, dict, 4096);
            GroupKeyTable t = new GroupKeyTable(new VecType[] {VecType.INT32, VecType.INT64, VecType.UTF8}, true);
            GroupKeyTable.releaseThreadScratch();
            t.assign(k, 4096, new int[4096]);
            long largest = GroupKeyTable.threadScratchFootprint()[0];
            GroupKeyTable.releaseThreadScratch();
            assertEquals(largest, GroupKeyTable.threadScratchFootprint()[0], "batch-sized arrays are kept for reuse");
        }
    }
}
