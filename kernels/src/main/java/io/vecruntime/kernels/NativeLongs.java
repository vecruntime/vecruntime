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
 * A growable array of longs in a {@link NativePool} block (#941): the state of
 * a grouped accumulator once its table is past {@link
 * GroupedAccumulators#COLLAPSE_GROUPS} groups, where a {@code long[]} of
 * millions of entries is old-generation churn on every doubling. Each
 * generation owns its own pooled arena, so a growth hands the old block
 * straight back to the pool instead of holding it until the operator closes. A
 * pooled arena opened on a task thread charges that task's account (#869 step
 * 3b), so these bytes are in the task's execution memory in the operators'
 * mode. Not thread-safe; {@link #close()} once.
 */
final class NativeLongs implements AutoCloseable {
    static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;

    private Arena arena;
    private MemorySegment seg;
    private int length;

    /**
     * {@code length} longs: the first {@code from.length} copied from {@code
     * from}, the rest zero.
     */
    NativeLongs(long[] from, int fromLength, int length) {
        this.arena = NativePool.shared().newArena();
        this.seg = arena.allocate(8L * length, 64);
        MemorySegment.copy(from, 0, seg, LONG, 0, fromLength);
        seg.asSlice(8L * fromLength).fill((byte) 0);
        this.length = length;
    }

    /** The segment the update loops index; replaced by {@link #grow}. */
    MemorySegment segment() {
        return seg;
    }

    int length() {
        return length;
    }

    long bytes() {
        return 8L * length;
    }

    long get(int i) {
        return seg.getAtIndex(LONG, i);
    }

    /**
     * Grows to {@code newLength} longs, the new tail zero, and gives the old
     * block back.
     */
    void grow(int newLength) {
        if (newLength <= length) {
            return;
        }
        Arena next = NativePool.shared().newArena();
        MemorySegment grown = next.allocate(8L * newLength, 64);
        MemorySegment.copy(seg, 0, grown, 0, 8L * length);
        grown.asSlice(8L * length).fill((byte) 0);
        arena.close();
        arena = next;
        seg = grown;
        length = newLength;
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
            seg = null;
            length = 0;
        }
    }
}
