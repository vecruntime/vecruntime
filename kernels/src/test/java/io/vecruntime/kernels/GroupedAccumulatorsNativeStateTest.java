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
    void wideSumAndCountGoNativeWithTheSameAnswers() {
        int groups = GroupedAccumulators.COLLAPSE_GROUPS * 3;
        Random rnd = new Random(9410);
        java.math.BigInteger[] ref = new java.math.BigInteger[groups];
        java.util.Arrays.fill(ref, java.math.BigInteger.ZERO);
        long[] refCount = new long[groups];
        long[] refNonNull = new long[groups];
        long inUse = NativePool.shared().inUseBytes();
        GroupedAccumulators.WideLongSum wide = new GroupedAccumulators.WideLongSum();
        GroupedAccumulators.WideLongSum wideLongs = new GroupedAccumulators.WideLongSum();
        GroupedAccumulators.Count all = new GroupedAccumulators.Count();
        GroupedAccumulators.Count nonNull = new GroupedAccumulators.Count();
        java.math.BigInteger[] refLongs = new java.math.BigInteger[groups];
        java.util.Arrays.fill(refLongs, java.math.BigInteger.ZERO);
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < 6; b++) {
                int n = 4096;
                int[] ids = new int[n];
                java.math.BigInteger[] d = new java.math.BigInteger[n];
                long[] v = new long[n];
                boolean[] nul = new boolean[n];
                for (int i = 0; i < n; i++) {
                    // Groups reach the threshold in the second batch, so the state moves mid-stream.
                    ids[i] = rnd.nextInt(b == 0 ? 1024 : groups);
                    d[i] = new java.math.BigInteger(100, rnd).subtract(java.math.BigInteger.ONE.shiftLeft(99));
                    v[i] = rnd.nextLong();
                    nul[i] = rnd.nextInt(7) == 0;
                    refCount[ids[i]]++;
                    if (!nul[i]) {
                        ref[ids[i]] = ref[ids[i]].add(d[i]);
                        refLongs[ids[i]] = refLongs[ids[i]].add(java.math.BigInteger.valueOf(v[i]));
                        refNonNull[ids[i]]++;
                    }
                }
                int seen = b == 0 ? 1024 : groups;
                GroupAssignment a = assign(ids, seen, arena);
                wide.update(ArrowLayout.ofDecimal128(arena, d, nul), a);
                wideLongs.update(ArrowLayout.ofLongs(arena, v, nul), a);
                all.updateAll(a);
                nonNull.updateNonNull(ArrowLayout.ofLongs(arena, v, nul), a);
            }
        }
        assertTrue(wide.nativeBytes() >= 24L * groups);
        assertTrue(all.nativeBytes() >= 8L * groups);
        for (int g = 0; g < groups; g++) {
            assertEquals(ref[g], wide.sum(g), "wide decimal sum of group " + g);
            assertEquals(refLongs[g], wideLongs.sum(g), "wide long sum of group " + g);
            assertEquals(refNonNull[g], wide.count(g), "wide count of group " + g);
            assertEquals(refCount[g], all.count(g), "count(*) of group " + g);
            assertEquals(refNonNull[g], nonNull.count(g), "count(x) of group " + g);
        }
        wide.close();
        wideLongs.close();
        all.close();
        nonNull.close();
        assertEquals(inUse, NativePool.shared().inUseBytes(),
                "blocks back in the pool");
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
