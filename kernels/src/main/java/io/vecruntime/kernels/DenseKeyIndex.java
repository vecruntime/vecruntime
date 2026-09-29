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

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * A hash join's key table for a single integer key whose build values span a
 * small range (#546): the group id of key {@code k} is {@code slots[k - min]},
 * so a probe is a range check and one array load per row -- no hash, no slot
 * search, no key compare. Spark's {@code LongHashedRelation} makes the same
 * switch ({@code LongToUnsafeRowMap} in dense mode) under the same rule: the
 * range may be at most {@link #DENSE_FACTOR} times the number of distinct keys.
 *
 * <p>Built once from the build side's key column and the group ids
 * {@link GroupKeyTable#assign} gave it, and read-only afterwards, so a shared
 * broadcast table is probed by several tasks at once; each probe brings its own
 * {@link Scratch}.
 */
public final class DenseKeyIndex {

    /**
     * The largest range, relative to the distinct keys, that is indexed densely
     * (Spark's).
     */
    public static final int DENSE_FACTOR = 10;

    /** The largest range indexed at all: 4 M slots, 16 MB. */
    public static final int MAX_RANGE = 1 << 22;

    private final VecType type;
    private final long min;
    private final int[] slots;

    private DenseKeyIndex(VecType type, long min, int[] slots) {
        this.type = type;
        this.min = min;
        this.slots = slots;
    }

    /**
     * Per-probe scratch: the batch's key values as a Java array (one bulk copy
     * per batch).
     */
    public static final class Scratch {
        int[] ints = new int[0];
        long[] longs = new long[0];
    }

    /**
     * The index over build rows {@code 0..n)} of {@code key}, whose group ids
     * are {@code ids} ({@code -1}: a row with a null key or not selected, never
     * matched), or {@code null} when the key is not a plain INT32/INT64 column
     * or its values span too wide a range.
     */
    public static DenseKeyIndex tryBuild(VectorBuffers key, int n, int[] ids,
            int groups) {
        VecType t = key.type();
        if ((t != VecType.INT32 && t != VecType.INT64)
                || key.isDictionaryEncoded()
                || groups <= 0) {
            return null;
        }
        boolean wide = t == VecType.INT64;
        MemorySegment data = key.data();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = 0; i < n; i++) {
            if (ids[i] >= 0) {
                long v = wide ? data.get(VectorBuffers.LE_LONG, (long) i << 3) : data.get(VectorBuffers.LE_INT, (long) i << 2);
                if (v < min) {
                    min = v;
                }
                if (v > max) {
                    max = v;
                }
            }
        }
        if (min > max) {
            return null;
        }
        // max - min overflows only when the range is far past MAX_RANGE anyway.
        long range = max - min + 1;
        if (range <= 0 || range > MAX_RANGE || range > (long) DENSE_FACTOR * groups) {
            return null;
        }
        int[] slots = new int[(int) range];
        Arrays.fill(slots, -1);
        for (int i = 0; i < n; i++) {
            if (ids[i] >= 0) {
                long v = wide ? data.get(VectorBuffers.LE_LONG, (long) i << 3) : data.get(VectorBuffers.LE_INT, (long) i << 2);
                slots[(int) (v - min)] = ids[i];
            }
        }
        return new DenseKeyIndex(t, min, slots);
    }

    /**
     * Whether a probe column can be looked up here: the build key's type, plain
     * (not dictionary-encoded).
     */
    public boolean accepts(VectorBuffers key) {
        return key.type() == type && !key.isDictionaryEncoded();
    }

    /** The number of slots (the key range). */
    public int range() {
        return slots.length;
    }

    /**
     * As {@link GroupKeyTable#lookup}: {@code outIds[i]} is the group id of row
     * {@code i}'s key, or {@code -1} when no build row has it or the row is not
     * set in {@code selection} ({@code null}: every row; a hash join passes the
     * rows whose keys are non-null). Returns the number of rows that matched.
     */
    public int lookup(VectorBuffers key, int n, int[] outIds,
                      MemorySegment selection, Scratch scratch) {
        int[] s = slots;
        long range = s.length;
        if (type == VecType.INT32) {
            int[] keys = scratch.ints;
            if (keys.length < n) {
                keys = new int[Math.max(n, keys.length * 2)];
                scratch.ints = keys;
            }
            MemorySegment.copy(key.data(), VectorBuffers.LE_INT, 0L, keys, 0,
                    n);
            for (int i = 0; i < n; i++) {
                long off = keys[i] - min;
                outIds[i] = Long.compareUnsigned(off, range) < 0 ? s[(int) off] : -1;
            }
        } else {
            long[] keys = scratch.longs;
            if (keys.length < n) {
                keys = new long[Math.max(n, keys.length * 2)];
                scratch.longs = keys;
            }
            MemorySegment.copy(key.data(), VectorBuffers.LE_LONG, 0L, keys, 0,
                    n);
            for (int i = 0; i < n; i++) {
                long off = keys[i] - min;
                outIds[i] = Long.compareUnsigned(off, range) < 0 ? s[(int) off] : -1;
            }
        }
        if (selection != null) {
            for (int w = 0, words = Bitmap.wordsFor(n);
                 w < words;
                 w++) {
                long off = ~Bitmap.wordAt(selection, w, n) & Bitmap.lowBits(Math.min(64, n - (w << 6)));
                while (off != 0L) {
                    outIds[(w << 6) + Long.numberOfTrailingZeros(off)] = -1;
                    off &= off - 1;
                }
            }
        }
        int matched = 0;
        for (int i = 0; i < n; i++) {
            matched += (outIds[i] >>> 31) ^ 1;
        }
        return matched;
    }
}
