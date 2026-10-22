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

import java.util.Arrays;
import java.util.HashMap;

/**
 * Spark's map-key dedup ({@code spark.sql.mapKeyDedupPolicy}) over a map
 * constructor's interleaved entries, exactly as {@code ArrayBasedMapBuilder}
 * does it, per row: the entries of row {@code r} are the {@code k} positions
 * {@code r*k .. r*k+k-1} of the key and value lanes. Two keys are equal when
 * they encode alike under {@link NestedKeyCodec#encodeKeyForDedup} ({@code NaN
 * == NaN}, {@code -0.0 == 0.0}, strings by bytes, nested keys structurally).
 *
 * <ul>
 *   <li>EXCEPTION (the default): a repeated key is an error. The kernel does
 *       not depend on Spark, so it throws {@link DuplicateKey} carrying the
 *       offending entry index; the caller reads the key there and raises
 *       Spark's {@code DUPLICATED_MAP_KEY}.
 *   <li>LAST_WIN: the last value of a repeated key is kept at the first
 *       occurrence's position, as {@code ArrayBasedMapBuilder} does.
 * </ul>
 *
 * Returns the surviving entry indices (into the {@code k*n} interleaved
 * positions) in order, with the {@code length + 1} offsets of the deduped map.
 * Only entries of ACTIVE rows are inspected; an inactive row keeps its {@code
 * k} entries untouched (it is dropped downstream) so the kernel never raises
 * for a row the operator discards.
 */
public final class MapDedupKernels {

    public static final int EXCEPTION = 0;
    public static final int LAST_WIN = 1;

    private MapDedupKernels() {}

    /**
     * A duplicate key under EXCEPTION: {@code entry} is its position in the
     * interleaved key lane.
     */
    public static final class DuplicateKey extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public final int entry;

        public DuplicateKey(int entry) {
            super("duplicate map key");
            this.entry = entry;
        }
    }

    /** The deduped shape: the surviving entry indices and the map's offsets. */
    public static final class Result {
        public final int[] entries;
        public final int[] offsets; // length n + 1

        Result(int[] entries, int[] offsets) {
            this.entries = entries;
            this.offsets = offsets;
        }
    }

    /**
     * Dedups each row's map entries under the given policy, returning the
     * surviving entries and offsets.
     *
     * @param keys the interleaved key lane ({@code k} per row)
     * @param k entries per row
     * @param n rows
     * @param active a validity bitmap of the rows to inspect, or {@code null}
     *     for all rows
     * @param policy {@link #EXCEPTION} or {@link #LAST_WIN}
     */
    public static Result dedup(VectorBuffers keys, int k, int n,
            java.lang.foreign.MemorySegment active, int policy) {
        int[] entries = new int[Math.max(n * k, 1)];
        int[] offsets = new int[n + 1];
        int out = 0;
        // Reused per row: signature -> position in `entries` of this row's first occurrence of that key.
        HashMap<Key, Integer> seen = new HashMap<>();
        for (int r = 0; r < n; r++) {
            boolean inspect = active == null || Bitmap.isSet(active, r);
            int base = r * k;
            if (!inspect) {
                // Keep the row's entries verbatim; it is dropped downstream, so never dedup or raise.
                for (int j = 0; j < k; j++) {
                    entries[out++] = base + j;
                }
                offsets[r + 1] = out;
                continue;
            }
            seen.clear();
            int rowStart = out;
            for (int j = 0; j < k; j++) {
                int e = base + j;
                Key sig = new Key(NestedKeyCodec.encodeKeyForDedup(keys, e));
                Integer at = seen.get(sig);
                if (at == null) {
                    seen.put(sig, out);
                    entries[out++] = e;
                } else if (policy == LAST_WIN) {
                    // Keep the first occurrence's position; its value becomes this entry's value.
                    entries[at] = e;
                } else {
                    throw new DuplicateKey(e);
                }
            }
            offsets[r + 1] = out;
            if (rowStart == out) {
                offsets[r + 1] = out; // a zero-entry row (k == 0) keeps its empty span
            }
        }
        return new Result(Arrays.copyOf(entries, out), offsets);
    }

    /** A byte-array key with value equality, for the per-row dedup map. */
    private static final class Key {
        private final byte[] bytes;
        private final int hash;

        Key(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key other && Arrays.equals(bytes, other.bytes);
        }
    }
}
