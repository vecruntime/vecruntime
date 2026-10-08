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

import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The batch probes of the split-block filter (#664) agree with the per-key
 * reference, bit for bit.
 */
class BloomKernelsTest {

    private static int[][] filters(SplittableRandom rnd, int parts, int keysPerPart,
            long[] keys, boolean emptyPart) {
        int[][] fs = new int[parts][];
        for (int p = 0; p < parts; p++) {
            fs[p] = BloomKernels.create(keysPerPart * 8L);
        }
        for (int k = 0; k < keys.length; k++) {
            long h = rnd.nextLong();
            int p = (int) Math.floorMod(h, (long) parts);
            if (emptyPart && p == parts - 1) {
                k--;
                continue;
            }
            keys[k] = h;
            BloomKernels.put(fs[p], h);
        }
        if (emptyPart) {
            fs[parts - 1] = null; // a bucket with no key
        }
        return fs;
    }

    private static void checkAll(int[][] fs, long[] hashes, int n) {
        long[] ref = new long[(n + 63) >>> 6];
        long[] got = new long[(n + 63) >>> 6];
        long[] xs = new long[n];
        BloomKernels.probeReference(fs, hashes, n, ref);
        BloomKernels.probeScalar(fs, hashes, n, got);
        assertArrayEquals(ref, got, "scalar");
        BloomKernels.probeVector(fs, hashes, n, got, xs);
        assertArrayEquals(ref, got, "vector");
        BloomKernels.probe(fs, hashes, n, got, xs);
        assertArrayEquals(ref, got, "probe");
    }

    @Test
    void batchProbesAgreeWithTheReference() {
        SplittableRandom rnd = new SplittableRandom(11);
        // power-of-two count (masked), other count (floorMod), one filter; an empty bucket in each multi-part case
        for (int parts : new int[] {1, 4, 7}) {
            long[] keys = new long[5000];
            int[][] fs = filters(rnd, parts, 2000, keys, parts > 1);
            // odd lengths exercise the vector remix's scalar tail and partial result words
            for (int n : new int[] {0, 1, 7, 63, 64, 65,
                    1000, 4096}) {
                long[] hashes = new long[n];
                for (int i = 0; i < n; i++) {
                    hashes[i] = (i & 1) == 0 ? keys[rnd.nextInt(keys.length)] : rnd.nextLong();
                }
                hashes = n > 3 ? withEdges(hashes) : hashes;
                checkAll(fs, hashes, n);
            }
        }
    }

    private static long[] withEdges(long[] hashes) {
        hashes[0] = Long.MIN_VALUE;
        hashes[1] = -1L;
        hashes[2] = 0L;
        hashes[3] = Long.MAX_VALUE;
        return hashes;
    }

    @Test
    void everyKeyIsFound() {
        SplittableRandom rnd = new SplittableRandom(5);
        long[] keys = new long[20000];
        int[][] fs = filters(rnd, 8, 4000, keys, false);
        long[] out = new long[(keys.length + 63) >>> 6];
        BloomKernels.probe(fs, keys, keys.length, out, new long[keys.length]);
        for (int i = 0; i < keys.length; i++) {
            assertTrue((out[i >>> 6] & (1L << (i & 63))) != 0, "key " + i + " not found");
        }
    }
}
