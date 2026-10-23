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
import java.nio.charset.StandardCharsets;
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
 * #941 phase 2d, the aarch64 defaults, forced here on any architecture: a
 * table that packs its slots from the start, and a UTF8 key whose dictionary
 * moves into pool blocks past its size and keeps assigning, emitting and
 * switching to records the same groups.
 */
class GroupKeyTableNativeDictionaryTest {

    private boolean before;

    @BeforeEach
    void on() {
        before = GroupedAccumulators.nativeState();
        GroupedAccumulators.setNativeState(true);
        StringDictionary.setNativeBytesForTests(4096); // a few hundred values
        GroupKeyTable.setPackedSlotsForTests(0); // packed from the first slot
    }

    @AfterEach
    void restore() {
        GroupedAccumulators.setNativeState(before);
        StringDictionary.setNativeBytesForTests(-1);
        GroupKeyTable.setPackedSlotsForTests(-1);
    }

    /**
     * Assigns batches of "product-<n>" over {@code distinct} values (plus an
     * INT32 key), checking every id.
     */
    private static Map<String, Integer> fill(GroupKeyTable table, int distinct, int batches,
            long seed) {
        Map<String, Integer> reference = new HashMap<>();
        Random rnd = new Random(seed);
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < batches; b++) {
                int n = 2048;
                String[] v = new String[n];
                int[] y = new int[n];
                for (int i = 0; i < n; i++) {
                    int g = rnd.nextInt(distinct);
                    v[i] = "product-" + g + "-" + "x".repeat(g % 23);
                    y[i] = 1998 + (g & 3);
                }
                int[] ids = new int[n];
                int groups = table.assign(new VectorBuffers[] {ArrowLayout.ofStrings(arena, v), ArrowLayout.ofInts(arena, y, null)}, n,
                        ids, null);
                for (int i = 0; i < n; i++) {
                    Integer known = reference.putIfAbsent(v[i], ids[i]);
                    if (known != null) {
                        assertEquals(known.intValue(), ids[i], "row " + i + " of batch " + b);
                    }
                }
                assertEquals(reference.size(), groups, "groups after batch " + b);
            }
        }
        return reference;
    }

    @Test
    void theDictionaryMovesIntoThePoolAndKeepsItsValues() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.UTF8, VecType.INT32}, true);
        table.allowNative();
        Map<String, Integer> reference = fill(table, 20_000, 20, 941);
        assertTrue(table.packedSlots() || table.nativeSlots(), "packed from the start");
        assertTrue(table.nativeDictionary(0), "past its size the dictionary is in the pool");
        assertTrue(table.isDictionaryColumn(0));
        for (Map.Entry<String, Integer> e : reference.entrySet()) {
            assertEquals(e.getKey(), table.getString(0, e.getValue()), "key of group " + e.getValue());
        }
        // the key output gathers from the pool
        int groups = reference.size();
        try (Arena arena = Arena.ofConfined()) {
            long bytes = table.utf8Bytes(0, 0, groups);
            MemorySegment validity = arena.allocate((groups + 7) / 8 + 8);
            MemorySegment data = arena.allocate(Math.max(8, bytes));
            MemorySegment offsets = arena.allocate(4L * (groups + 1));
            table.writeKeys(0, 0, groups, validity, data, offsets);
            for (Map.Entry<String, Integer> e : reference.entrySet()) {
                int gid = e.getValue();
                int from = offsets.get(VectorBuffers.LE_INT, 4L * gid);
                int to = offsets.get(VectorBuffers.LE_INT, 4L * (gid + 1));
                byte[] b = new byte[to - from];
                MemorySegment.copy(data, ValueLayout.JAVA_BYTE, from, b, 0, b.length);
                assertEquals(e.getKey(), new String(b, StandardCharsets.UTF_8), "emitted key of group " + gid);
            }
        }
        assertTrue(table.nativeBytes() > 0);
        table.close();
        assertEquals(0L, table.nativeBytes(), "closed");
    }

    @Test
    void notAllowedKeepsTheDictionaryOnTheHeap() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.UTF8, VecType.INT32}, true);
        fill(table, 20_000, 10, 942);
        assertFalse(table.nativeDictionary(0));
        assertEquals(0L, table.nativeBytes());
    }

    /**
     * A dictionary in the pool that passes the record limit is copied out into
     * records and freed.
     */
    @Test
    void aPoolDictionarySwitchesToRecords() {
        GroupKeyTable table = new GroupKeyTable(new VecType[] {VecType.UTF8, VecType.INT32}, 15_000);
        table.allowNative();
        Map<String, Integer> reference = fill(table, 40_000, 30, 943);
        assertFalse(table.isDictionaryColumn(0), "past the limit the column is in record mode");
        for (Map.Entry<String, Integer> e : reference.entrySet()) {
            assertEquals(e.getKey(), table.getString(0, e.getValue()), "key of group " + e.getValue());
        }
        table.close();
        assertEquals(0L, table.nativeBytes(), "closed");
    }
}
