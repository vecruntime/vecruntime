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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * {@link PartitionKernels#mixColumn} against a per-row scalar hash (#565: the
 * mixing loops read offsets, indices and validity from heap copies). Spark
 * equivalence itself is {@code PartitionKernelsSuite}'s; this pins the new paths:
 * plain and dictionary UTF8 with and without nulls, a dictionary over twice the
 * rows (read per row), INT32 / INT64 / FLOAT64 with nulls, native and heap
 * segments, prefixes of the rows and scratch reuse across sizes.
 */
class PartitionMixTest {

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    private static VectorBuffers onHeap(VectorBuffers c) {
        if (c.isDictionaryEncoded()) {
            VectorBuffers d = c.dictionary();
            return SegmentVectorBuffers.dictionaryUtf8(
                    c.length(),
                    heap(c.validity()),
                    heap(c.data()),
                    SegmentVectorBuffers.utf8(d.length(), heap(d.validity()), heap(d.offsets()),
                            heap(d.data())));
        }
        if (c.type() == VecType.UTF8) {
            return SegmentVectorBuffers.utf8(c.length(), heap(c.validity()), heap(c.offsets()),
                    heap(c.data()));
        }
        return SegmentVectorBuffers.fixedWidth(c.type(), c.length(), heap(c.validity()),
                heap(c.data()));
    }

    /** The per-row reference: Spark's rule, a null row keeps its hash. */
    private static int[] expected(VectorBuffers col, PartitionKernels.KeyKind kind, int[] seeds,
            int n) {
        int[] h = seeds.clone();
        for (int i = 0; i < n; i++) {
            if (col.isNull(i)) {
                continue;
            }
            h[i] = switch (kind) {
                        case INT -> PartitionKernels.hashInt(col.getInt(i), h[i]);
                        case LONG -> PartitionKernels.hashLong(col.getLong(i), h[i]);
                        case DOUBLE -> PartitionKernels.hashLong(PartitionKernels.doubleBits(col.getDouble(i)), h[i]);
                        case UTF8 -> PartitionKernels.hashUnsafeBytes(col.getUtf8Bytes(i), h[i]);
                        default -> throw new IllegalArgumentException(kind.toString());
                    };
        }
        return h;
    }

    private static String word(Random rnd) {
        int len = rnd.nextInt(0, 23);
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < len; k++) {
            sb.append(rnd.nextInt(9) == 0 ? 'é' : (char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    private static VectorBuffers dictionary(Arena arena, Random rnd, int n,
            int entries, double nullFraction) {
        String[] dict = new String[entries];
        for (int e = 0; e < entries; e++) {
            dict[e] = word(rnd);
        }
        int[] ids = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            nulls[i] = rnd.nextDouble() < nullFraction;
            // A null row's id is masked to entry 0 by the kernel; give it one far outside the dictionary.
            ids[i] = nulls[i] ? 1_000_000_007 : rnd.nextInt(entries);
        }
        VectorBuffers idx = ArrowLayout.ofInts(arena, ids, nulls);
        return SegmentVectorBuffers.dictionaryUtf8(n, idx.validity(), idx.data(),
                ArrowLayout.ofStrings(arena, dict));
    }

    private static void check(VectorBuffers col, PartitionKernels.KeyKind kind, int n,
            Random rnd, String what) {
        int[] seeds = new int[col.length()];
        for (int i = 0; i < seeds.length; i++) {
            seeds[i] = rnd.nextInt();
        }
        VectorBuffers[] kinds = {col, onHeap(col)};
        for (int k = 0; k < kinds.length; k++) {
            int[] actual = seeds.clone();
            PartitionKernels.mixColumn(kinds[k], kind, actual, n);
            assertArrayEquals(expected(col, kind, seeds, n), actual, what + (k == 0 ? " native" : " heap"));
        }
    }

    @Test
    void everyMixingPathMatchesThePerRowHash() {
        Random rnd = new Random(565);
        // Sizes in an order that grows and shrinks the per-thread scratch.
        int[] sizes = {4097, 1, 64, 65, 1000, 3,
                4097, 129};
        try (Arena arena = Arena.ofConfined()) {
            for (int n : sizes) {
                for (double nf : new double[] {0.0, 0.2}) {
                    boolean[] nulls = TestData.nulls(rnd, n, nf);
                    String[] strings = new String[n];
                    for (int i = 0; i < n; i++) {
                        strings[i] = nulls[i] ? null : word(rnd);
                    }
                    String tag = " n=" + n + " nulls=" + nf;
                    check(ArrowLayout.ofStrings(arena, strings), PartitionKernels.KeyKind.UTF8, n, rnd,
                            "plain" + tag);
                    check(dictionary(arena, rnd, n, Math.max(1, n / 5), nf), PartitionKernels.KeyKind.UTF8, n, rnd,
                            "dictionary" + tag);
                    check(dictionary(arena, rnd, n, PartitionKernels.DICTIONARY_COPY_FACTOR * n + 1, nf), PartitionKernels.KeyKind.UTF8,
                            n, rnd, "large dictionary" + tag);
                    check(TestData.ints(arena, rnd, n, nulls), PartitionKernels.KeyKind.INT, n, rnd,
                            "int" + tag);
                    check(TestData.longs(arena, rnd, n, nulls), PartitionKernels.KeyKind.LONG, n, rnd,
                            "long" + tag);
                    check(TestData.doubles(arena, rnd, n, nulls), PartitionKernels.KeyKind.DOUBLE, n, rnd,
                            "double" + tag);
                    // A prefix of the rows: the writer hashes fewer rows than a column can hold.
                    check(ArrowLayout.ofStrings(arena, strings), PartitionKernels.KeyKind.UTF8,
                            n / 2, rnd, "plain prefix" + tag);
                }
            }
        }
    }
}
