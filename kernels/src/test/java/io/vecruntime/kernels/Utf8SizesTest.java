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
import java.nio.charset.StandardCharsets;
import java.util.Random;

import io.vecruntime.kernels.reference.ScalarReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link Utf8Sizes#paddedBytes} against the per-row scalar oracle. */
class Utf8SizesTest {

    private static String randomString(Random rnd) {
        int len = switch (rnd.nextInt(6)) {
            case 0 -> 0;
            case 1 -> rnd.nextInt(1, 8);
            case 2 -> 8;
            case 3 -> rnd.nextInt(9, 17);
            default -> rnd.nextInt(17, 70);
        };
        StringBuilder sb = new StringBuilder();
        while (sb.toString().getBytes(StandardCharsets.UTF_8).length < len) {
            sb.append(rnd.nextInt(5) == 0 ? "é" : String.valueOf((char) ('a' + rnd.nextInt(26))));
        }
        return sb.toString();
    }

    private static String[] strings(Random rnd, int n, double nullFraction) {
        String[] v = new String[n];
        for (int i = 0; i < n; i++) {
            v[i] = rnd.nextDouble() < nullFraction ? null : randomString(rnd);
        }
        return v;
    }

    /**
     * A dictionary column of {@code n} rows over {@code entries} entries; a
     * null row carries an index far outside the dictionary, which must never
     * be read.
     */
    private static VectorBuffers dictionary(Arena arena, Random rnd, int n,
            int entries, double nullFraction) {
        String[] dict = strings(rnd, entries, 0.0);
        int[] ids = new int[n];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            nulls[i] = rnd.nextDouble() < nullFraction;
            ids[i] = nulls[i] ? 1_000_000_007 : rnd.nextInt(entries);
        }
        VectorBuffers indices = ArrowLayout.ofInts(arena, ids, nulls);
        return SegmentVectorBuffers.dictionaryUtf8(n, indices.validity(), indices.data(),
                ArrowLayout.ofStrings(arena, dict));
    }

    @Test
    void plainColumnsMatchTheOracle() {
        Random rnd = new Random(565);
        Utf8Sizes.Scratch scratch = new Utf8Sizes.Scratch();
        try (Arena arena = Arena.ofConfined()) {
            for (int n : TestData.LENGTHS) {
                for (double nf : new double[] {0.0, 0.1, 0.9, 1.0}) {
                    VectorBuffers col = ArrowLayout.ofStrings(arena, strings(rnd, n, nf));
                    assertEquals(ScalarReference.paddedUtf8Bytes(col, n),
                            Utf8Sizes.paddedBytes(col, n, scratch), "plain n=" + n + " nulls=" + nf);
                    // A prefix of the rows, as the writer passes when trailing key columns are dropped.
                    int m = n / 2;
                    assertEquals(ScalarReference.paddedUtf8Bytes(col, m),
                            Utf8Sizes.paddedBytes(col, m, scratch), "plain prefix n=" + m + " nulls=" + nf);
                }
            }
        }
    }

    @Test
    void slicedColumnsKeepAbsoluteOffsets() {
        Random rnd = new Random(7);
        Utf8Sizes.Scratch scratch = new Utf8Sizes.Scratch();
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers col = ArrowLayout.ofStrings(arena, strings(rnd, 1000, 0.2));
            for (int from : new int[] {64, 128, 512}) {
                VectorBuffers s = col.slice(from, 1000);
                assertEquals(ScalarReference.paddedUtf8Bytes(s, s.length()),
                        Utf8Sizes.paddedBytes(s, s.length(), scratch), "slice from " + from);
            }
        }
    }

    @Test
    void dictionaryColumnsMatchTheOracleOnBothPaths() {
        Random rnd = new Random(1);
        Utf8Sizes.Scratch scratch = new Utf8Sizes.Scratch();
        try (Arena arena = Arena.ofConfined()) {
            for (int n : TestData.LENGTHS) {
                if (n == 0) {
                    continue;
                }
                // Small dictionaries are copied whole; one past DICTIONARY_COPY_FACTOR * n is read per row.
                for (int entries : new int[] {1, 5, Math.max(1, n / 3), Utf8Sizes.DICTIONARY_COPY_FACTOR * n,
                        Utf8Sizes.DICTIONARY_COPY_FACTOR * n + 1}) {
                    for (double nf : new double[] {0.0, 0.3, 1.0}) {
                        VectorBuffers col = dictionary(arena, rnd, n, entries, nf);
                        assertEquals(ScalarReference.paddedUtf8Bytes(col, n),
                                Utf8Sizes.paddedBytes(col, n, scratch), "dictionary n=" + n + " entries=" + entries + " nulls=" + nf);
                    }
                }
            }
        }
    }

    @Test
    void scratchIsReusedAcrossShrinkingAndGrowingBatches() {
        Random rnd = new Random(3);
        Utf8Sizes.Scratch scratch = new Utf8Sizes.Scratch();
        try (Arena arena = Arena.ofConfined()) {
            for (int n : new int[] {4097, 3, 1000, 65, 4097, 1}) {
                VectorBuffers plain = ArrowLayout.ofStrings(arena, strings(rnd, n, 0.25));
                VectorBuffers dict = dictionary(arena, rnd, n, Math.max(1, n / 4), 0.25);
                assertEquals(ScalarReference.paddedUtf8Bytes(plain, n), Utf8Sizes.paddedBytes(plain, n, scratch));
                assertEquals(ScalarReference.paddedUtf8Bytes(dict, n), Utf8Sizes.paddedBytes(dict, n, scratch));
            }
        }
    }

    @Test
    void nonStringColumnsCountNothing() {
        Utf8Sizes.Scratch scratch = new Utf8Sizes.Scratch();
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers ints = ArrowLayout.ofInts(arena, new int[] {1, 2, 3}, null);
            assertEquals(0L, Utf8Sizes.paddedBytes(ints, 3, scratch));
            assertEquals(0L, ScalarReference.paddedUtf8Bytes(ints, 3));
            VectorBuffers empty = ArrowLayout.ofStrings(arena, new String[] {"abc"});
            assertEquals(0L, Utf8Sizes.paddedBytes(empty, 0, scratch));
            assertEquals(8L, Utf8Sizes.paddedBytes(empty, 1, scratch));
        }
    }
}
