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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #941 phase 2: an inline-key table that grows past {@link
 * GroupKeyTable#NATIVE_FAST_SLOTS} moves its key/id pairs into a pool block
 * and keeps assigning the same groups; a table that was not allowed to, or a
 * null key group, keeps the pairs off the pool; {@link GroupKeyTable#close}
 * gives the block back.
 */
class GroupKeyTableNativeInlineTest {

    private boolean before;

    @BeforeEach
    void on() {
        before = GroupedAccumulators.nativeState();
        GroupedAccumulators.setNativeState(true);
    }

    @AfterEach
    void restore() {
        GroupedAccumulators.setNativeState(before);
    }

    /**
     * Assigns 40 batches of INT64 keys over {@code distinct} values, checking
     * every id against a map.
     */
    private static void fill(GroupKeyTable table, int distinct, long seed,
            int nullBatch) {
        Map<Long, Integer> reference = new HashMap<>();
        Random rnd = new Random(seed);
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < 40; b++) {
                int n = 4096 * 4;
                long[] v = new long[n];
                boolean[] nul = b == nullBatch ? new boolean[n] : null;
                for (int i = 0; i < n; i++) {
                    long g = rnd.nextInt(distinct);
                    v[i] = rnd.nextBoolean() ? -g : g + (1L << 32);
                    if (nul != null && i % 97 == 0) {
                        nul[i] = true;
                        v[i] = 0;
                    }
                }
                int[] ids = new int[n];
                int groups = table.assign(new VectorBuffers[] {ArrowLayout.ofLongs(arena, v, nul)},
                        n, ids, null);
                for (int i = 0; i < n; i++) {
                    Long key = nul != null && nul[i] ? Long.MIN_VALUE : v[i];
                    Integer known = reference.putIfAbsent(key, ids[i]);
                    if (known != null) {
                        assertEquals(known.intValue(), ids[i], "row " + i + " of batch " + b);
                    }
                }
                assertEquals(reference.size(), groups, "groups after batch " + b);
            }
        }
        for (Map.Entry<Long, Integer> e : reference.entrySet()) {
            if (e.getKey() != Long.MIN_VALUE) {
                assertEquals(e.getKey().longValue(),
                        table.getLong(0, e.getValue()), "key of group " + e.getValue());
            }
        }
    }

    @Test
    void pairsMoveIntoThePoolAndBack() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.INT64});
        table.allowNative();
        fill(table, 300_000, 941, -1);
        assertTrue(table.inlineKeys(), "inline");
        assertTrue(table.nativeInline(), "past " + GroupKeyTable.NATIVE_FAST_SLOTS + " slots the pairs are in the pool");
        assertTrue(table.nativeBytes() >= 16L * GroupKeyTable.NATIVE_FAST_SLOTS, "pool bytes " + table.nativeBytes());
        table.close();
        assertEquals(0L, table.nativeBytes(), "closed");
    }

    @Test
    void notAllowedStaysOnTheHeap() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.INT64});
        fill(table, 300_000, 942, -1);
        assertTrue(table.inlineKeys(), "inline");
        assertFalse(table.nativeInline(), "a table that was not allowed keeps its pairs on the heap");
        assertEquals(0L, table.nativeBytes());
    }

    @Test
    void aNullKeyDropsThePoolPairs() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.INT64});
        table.allowNative();
        fill(table, 300_000, 943, 35);
        assertFalse(table.inlineKeys(), "a null key group ends the inline path");
        assertFalse(table.nativeInline(), "and gives the pairs' pool block back");
        // #941 phase 2b: the generic slots, past the threshold too, stay in the pool until close
        assertTrue(table.nativeSlots(), "the generic slots are in the pool");
        assertEquals(8L * Integer.highestOneBit((int) (table.nativeBytes() / 8)), table.nativeBytes(),
                "only the slot entries are left in the pool");
        table.close();
        assertEquals(0L, table.nativeBytes(), "closed");
    }
}
