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

/**
 * #941: a grouped accumulator's state in one {@link NativePool} block, a
 * group's {@code stride} fields side by side (sum and count of a long sum, hi,
 * lo and count of a wide sum), so a row's update touches one cache line, not
 * one per array. Field {@code f} of group {@code g} is long {@code g * stride +
 * f} of {@link #segment()}.
 *
 * <p>{@code PagedAccumulatorBenchmark}, sum + count scatter over uniform ids
 * against today's two heap {@code long[]}: on Graviton 4 1.4-3.8x at every
 * size; on Skylake-SP and Sapphire Rapids 0.75-0.86x up to 1,024 groups (the
 * table is in L1 and the segment access costs), 1.1-1.3x from 16k groups and
 * up to 2.3x at 4M. Fixed-size pages instead of one block lost on Graviton (the
 * page lookup), so the block doubles and copies like the heap arrays do --
 * only past {@code COLLAPSE_GROUPS}, where it starts.
 *
 * <p>Each block has a pooled arena of its own, so a growth gives the old block
 * straight back to the pool, and {@link #close} the last one; the pool charges
 * the blocks to the task's account in the operators' memory mode. One task
 * thread updates a table.
 */
final class InterleavedSlots implements AutoCloseable {
    static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;

    private final int stride;
    private Arena arena;
    private MemorySegment seg;
    private int capacity;

    /** Room for {@code capacity} groups of {@code stride} longs, all zero. */
    InterleavedSlots(int stride, int capacity) {
        this.stride = stride;
        this.arena = NativePool.shared().newArena();
        this.seg = arena.allocate(8L * stride * capacity, 64).fill((byte) 0);
        this.capacity = capacity;
    }

    /** The block the update loops index; replaced by {@link #grow}. */
    MemorySegment segment() {
        return seg;
    }

    int stride() {
        return stride;
    }

    int capacity() {
        return capacity;
    }

    long bytes() {
        return 8L * stride * capacity;
    }

    long get(int g, int f) {
        return seg.getAtIndex(LONG, (long) g * stride + f);
    }

    void set(int g, int f, long v) {
        seg.setAtIndex(LONG, (long) g * stride + f, v);
    }

    /** Field {@code f} of groups {@code [0, n)} from a one-copy heap array. */
    void load(int f, long[] from, int n) {
        for (int g = 0; g < n; g++) {
            seg.setAtIndex(LONG, (long) g * stride + f, from[g]);
        }
    }

    /**
     * Grows to {@code newCapacity} groups, the new tail zero; the old block
     * goes back to the pool.
     */
    void grow(int newCapacity) {
        if (newCapacity <= capacity) {
            return;
        }
        Arena next = NativePool.shared().newArena();
        long used = bytes();
        MemorySegment grown = next.allocate(8L * stride * newCapacity, 64);
        MemorySegment.copy(seg, 0, grown, 0, used);
        grown.asSlice(used).fill((byte) 0);
        arena.close();
        arena = next;
        seg = grown;
        capacity = newCapacity;
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
            seg = null;
            capacity = 0;
        }
    }
}
