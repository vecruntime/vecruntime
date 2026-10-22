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
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #941: the same answers as {@link GroupedAccumulatorsCollapseTest} with the
 * state moved into pool blocks past {@link GroupedAccumulators#COLLAPSE_GROUPS}
 * groups (every inherited case grows the tables to five times the threshold),
 * plus the native state's own lifecycle: its bytes are reported, overflow still
 * throws when checked, and close gives the blocks back.
 */
class GroupedAccumulatorsNativeStateTest extends GroupedAccumulatorsCollapseTest {

    private boolean before;

    @BeforeEach
    void nativeOn() {
        before = GroupedAccumulators.nativeState();
        GroupedAccumulators.setNativeState(true);
    }

    @AfterEach
    void restore() {
        GroupedAccumulators.setNativeState(before);
    }

    private static GroupAssignment assign(int[] ids, int groups, Arena arena) {
        return GroupAssignment.of(ids, ids.length, groups, arena, false);
    }

    @Test
    void longSumGoesNativePastTheThresholdAndGivesItBack() {
        int groups = GroupedAccumulators.COLLAPSE_GROUPS * 4;
        Random rnd = new Random(941);
        long[] ref = new long[groups];
        long[] refCount = new long[groups];
        long inUse = NativePool.shared().inUseBytes();
        GroupedAccumulators.LongSum acc = new GroupedAccumulators.LongSum();
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < 8; b++) {
                int n = 4096;
                int[] ids = new int[n];
                long[] v = new long[n];
                for (int i = 0; i < n; i++) {
                    ids[i] = rnd.nextInt(groups);
                    v[i] = rnd.nextLong(1_000_000L);
                    ref[ids[i]] += v[i];
                    refCount[ids[i]]++;
                }
                acc.update(ArrowLayout.ofLongs(arena, v, null), assign(ids, groups, arena));
            }
        }
        assertTrue(acc.nativeBytes() >= 16L * groups, "native state past the threshold");
        assertTrue(NativePool.shared().inUseBytes() - inUse >= acc.nativeBytes());
        for (int g = 0; g < groups; g++) {
            assertEquals(ref[g], acc.sum(g), "sum of group " + g);
            assertEquals(refCount[g], acc.count(g), "count of group " + g);
        }
        acc.close();
        assertEquals(inUse, NativePool.shared().inUseBytes(),
                "blocks back in the pool");
    }

    @Test
    void checkedOverflowStillThrowsOnTheNativeState() {
        int groups = GroupedAccumulators.COLLAPSE_GROUPS * 2;
        GroupedAccumulators.LongSum acc = new GroupedAccumulators.LongSum(true);
        try (Arena arena = Arena.ofConfined()) {
            int[] ids = new int[groups];
            long[] v = new long[groups];
            for (int i = 0; i < groups; i++) {
                ids[i] = i;
                v[i] = Long.MAX_VALUE - 1;
            }
            acc.update(ArrowLayout.ofLongs(arena, v, null), assign(ids, groups, arena));
            assertTrue(acc.nativeBytes() > 0);
            long[] one = {2L};
            int[] first = {0};
            assertThrows(ArithmeticException.class,
                    () -> acc.update(ArrowLayout.ofLongs(arena, one, null), assign(first, groups, arena)));
        } finally {
            acc.close();
        }
    }

    @Test
    void stateStaysOnTheHeapBelowTheThresholdAndWhenOff() {
        try (Arena arena = Arena.ofConfined()) {
            GroupedAccumulators.LongSum small = new GroupedAccumulators.LongSum();
            int[] ids = {0, 1, 2};
            small.update(ArrowLayout.ofLongs(arena, new long[] {1, 2, 3}, null), assign(ids, 3, arena));
            assertEquals(0L, small.nativeBytes());

            GroupedAccumulators.setNativeState(false);
            int groups = GroupedAccumulators.COLLAPSE_GROUPS * 2;
            int[] many = new int[groups];
            long[] v = new long[groups];
            for (int i = 0; i < groups; i++) {
                many[i] = i;
                v[i] = i;
            }
            GroupedAccumulators.LongSum off = new GroupedAccumulators.LongSum();
            off.update(ArrowLayout.ofLongs(arena, v, null), assign(many, groups, arena));
            assertEquals(0L, off.nativeBytes());
            assertEquals(groups - 1L, off.sum(groups - 1));
        }
    }
}
