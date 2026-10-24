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
 * #941 phase 2b: the generic path's slots, one packed hash/id entry each, move
 * into a pool block once a table that allows it passes {@link
 * GroupKeyTable#NATIVE_FAST_SLOTS}, and keep assigning the same groups --
 * across further growth and across a UTF8 column's switch to record mode,
 * which rebuilds the slots with new hashes. A table that was not allowed keeps
 * them on the heap; {@link GroupKeyTable#close} gives the block back.
 */
class GroupKeyTableNativeSlotsTest {

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
     * Two INT64 keys (not an inline shape): 100 batches over 2M combinations
     * (~1.1M groups), every id checked.
     */
    private static void fillLongPairs(GroupKeyTable table) {
        Map<Long, Integer> reference = new HashMap<>();
        Random rnd = new Random(941);
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < 100; b++) {
                int n = 16384;
                long[] a = new long[n];
                long[] c = new long[n];
                for (int i = 0; i < n; i++) {
                    long g = rnd.nextInt(1 << 21);
                    a[i] = g >>> 4;
                    c[i] = -(g & 15) - (1L << 40);
                }
                int[] ids = new int[n];
                int groups = table.assign(new VectorBuffers[] {ArrowLayout.ofLongs(arena, a, null), ArrowLayout.ofLongs(arena, c, null)}, n,
                        ids, null);
                for (int i = 0; i < n; i++) {
                    long key = (a[i] << 4) | (-(c[i] + (1L << 40)));
                    Integer known = reference.putIfAbsent(key, ids[i]);
                    if (known != null) {
                        assertEquals(known.intValue(), ids[i], "row " + i + " of batch " + b);
                    }
                }
                assertEquals(reference.size(), groups, "groups after batch " + b);
            }
        }
        for (Map.Entry<Long, Integer> e : reference.entrySet()) {
            long key = e.getKey();
            assertEquals(key >>> 4, table.getLong(0, e.getValue()), "first key of group " + e.getValue());
            assertEquals(-(key & 15) - (1L << 40), table.getLong(1, e.getValue()), "second key of group " + e.getValue());
        }
    }

    @Test
    void slotsMoveIntoThePoolAndAreFreed() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.INT64, VecType.INT64});
        table.allowNative();
        fillLongPairs(table);
        assertFalse(table.inlineKeys(), "two INT64 keys probe the generic way");
        assertTrue(table.nativeSlots(), "past " + GroupKeyTable.NATIVE_FAST_SLOTS + " slots the entries are in the pool");
        assertTrue(table.nativeBytes() >= 8L * GroupKeyTable.NATIVE_FAST_SLOTS, "pool bytes " + table.nativeBytes());
        table.close();
        assertEquals(0L, table.nativeBytes(), "closed");
    }

    @Test
    void notAllowedKeepsTheSlotsOnTheHeap() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.INT64, VecType.INT64});
        fillLongPairs(table);
        assertFalse(table.nativeSlots(), "a table that was not allowed keeps its slots on the heap");
        assertTrue(table.packedSlots(), "past " + GroupKeyTable.PACKED_SLOTS + " slots, packed");
        assertEquals(0L, table.nativeBytes());
    }

    /** Below PACKED_SLOTS the table keeps today's split slots and hashes. */
    @Test
    void aSmallTableKeepsTheSplitSlots() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.INT64, VecType.INT64});
        table.allowNative();
        Map<Long, Integer> reference = new HashMap<>();
        Random rnd = new Random(944);
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < 8; b++) {
                int n = 4096;
                long[] a = new long[n];
                long[] c = new long[n];
                for (int i = 0; i < n; i++) {
                    long g = rnd.nextInt(50_000);
                    a[i] = g;
                    c[i] = -g;
                }
                int[] ids = new int[n];
                int groups = table.assign(new VectorBuffers[] {ArrowLayout.ofLongs(arena, a, null), ArrowLayout.ofLongs(arena, c, null)}, n,
                        ids, null);
                for (int i = 0; i < n; i++) {
                    Integer known = reference.putIfAbsent(a[i], ids[i]);
                    if (known != null) {
                        assertEquals(known.intValue(), ids[i], "row " + i + " of batch " + b);
                    }
                }
                assertEquals(reference.size(), groups, "groups after batch " + b);
            }
        }
        assertFalse(table.packedSlots(), "below the threshold the slots stay split");
        assertFalse(table.nativeSlots());
        table.close();
    }

    /**
     * A UTF8 key whose dictionary passes the limit after the slots are in the
     * pool: the switch to records rehashes every group into the pool block.
     */
    @Test
    void recordSwitchRebuildsThePoolSlots() {
        int limit = 800_000;
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.UTF8}, limit);
        table.allowNative();
        Map<String, Integer> reference = new HashMap<>();
        Random rnd = new Random(942);
        boolean sawNativeDictionary = false;
        int next = 0;
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < 70; b++) {
                int n = 16384;
                String[] v = new String[n];
                for (int i = 0; i < n; i++) {
                    // mostly new values, a quarter repeats of earlier ones
                    v[i] = next > 0 && rnd.nextInt(4) == 0
                            ? "k" + rnd.nextInt(next)
                            : "k" + next++;
                }
                int[] ids = new int[n];
                int groups = table.assign(new VectorBuffers[] {ArrowLayout.ofStrings(arena, v)},
                        n, ids, null);
                if (table.nativeSlots() && table.isDictionaryColumn(0)) {
                    sawNativeDictionary = true;
                }
                for (int i = 0; i < n; i++) {
                    Integer known = reference.putIfAbsent(v[i], ids[i]);
                    if (known != null) {
                        assertEquals(known.intValue(), ids[i], "row " + i + " of batch " + b);
                    }
                }
                assertEquals(reference.size(), groups, "groups after batch " + b);
            }
        }
        assertTrue(sawNativeDictionary, "the slots reached the pool while the column was still a dictionary");
        assertFalse(table.isDictionaryColumn(0), "past " + limit + " values the column is in record mode");
        assertTrue(table.nativeSlots());
        for (Map.Entry<String, Integer> e : reference.entrySet()) {
            assertEquals(e.getKey(), table.getString(0, e.getValue()), "key of group " + e.getValue());
        }
        table.close();
        assertEquals(0L, table.nativeBytes(), "closed");
    }
}
