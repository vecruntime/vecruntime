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

import java.nio.ByteBuffer;

/**
 * Spark's own runtime bloom filter ({@code BloomFilterImplV2}, what
 * {@code bloom_filter_agg} builds) probed a batch at a time (#692).
 *
 * <p>Spark's {@code mightContainLong} tests one key at a time: {@code k} bit
 * reads at {@code (h1 * Integer.MAX_VALUE + i * h2) % bitSize}, leaving at the
 * first clear bit. Each read is a likely cache miss in a multi-megabyte filter,
 * and the branch on it serialises them: the next key's reads cannot start
 * before this key's miss resolves. At 3 TB that probe was 13 to 18 % of q24a's
 * and q24b's executor samples.
 *
 * <p>{@link #probe} answers the same question, bit for bit, breadth first:
 * round {@code i} tests hash function {@code i} for every key still alive and
 * keeps the survivors. A round's reads are independent of each other, so the
 * misses of many keys are in flight together, and a key still leaves after its
 * first clear bit. The filter is read from Spark's serialised form (version 2,
 * big-endian) into a {@code long[]} once ({@link #parse}).
 */
public final class SparkBloomKernels {

    private SparkBloomKernels() {}

    /** A version-2 Spark bloom filter: its hash count, seed and bit words. */
    public static final class Filter {
        public final int numHashFunctions;
        public final int seed;
        public final long[] words;
        public final long bitSize;

        Filter(int numHashFunctions, int seed, long[] words) {
            this.numHashFunctions = numHashFunctions;
            this.seed = seed;
            this.words = words;
            this.bitSize = (long) words.length * Long.SIZE;
        }
    }

    /**
     * The filter {@code bytes} serialise ({@code BloomFilter.writeTo}), or
     * {@code null} when they are not version 2 (version 1 hashes differently:
     * leave it to Spark's own class).
     */
    public static Filter parse(byte[] bytes) {
        if (bytes.length < 16) {
            return null;
        }
        ByteBuffer b = ByteBuffer.wrap(bytes); // big-endian, as DataOutputStream wrote it
        if (b.getInt() != 2) {
            return null;
        }
        int k = b.getInt();
        int seed = b.getInt();
        int numWords = b.getInt();
        if (numWords <= 0 || k <= 0 || b.remaining() < (long) numWords * 8L) {
            return null;
        }
        long[] words = new long[numWords];
        b.asLongBuffer().get(words);
        return new Filter(k, seed, words);
    }

    /**
     * Writes into {@code out} bit {@code i} (Arrow order: bit {@code i & 63} of
     * word {@code i >>> 6}) whether {@code f} might contain {@code items[i]}, as
     * {@code BloomFilterImplV2.mightContainLong} answers, for the first
     * {@code n} items. {@code combined} ({@code >= n} longs) and {@code alive}
     * ({@code >= n} ints) are scratch.
     */
    public static void probe(
            Filter f,
            long[] items,
            int n,
            long[] out,
            long[] combined,
            int[] alive,
            int[] h2s) {
        int words = (n + 63) >>> 6;
        for (int w = 0; w < words; w++) {
            out[w] = 0L;
        }
        long[] bits = f.words;
        long bitSize = f.bitSize;
        // Round 1 over every key: its two murmur hashes and its first bit.
        int m = 0;
        for (int i = 0; i < n; i++) {
            int h1 = PartitionKernels.hashLong(items[i], f.seed);
            int h2 = PartitionKernels.hashLong(items[i], h1);
            long c = (long) h1 * Integer.MAX_VALUE + h2;
            long idx = (c < 0 ? ~c : c) % bitSize;
            // Branch-free keep: every key is written, the count moves only for a set bit.
            alive[m] = i;
            combined[m] = c;
            h2s[m] = h2;
            m += (int) ((bits[(int) (idx >>> 6)] >>> idx) & 1L);
        }
        // Rounds 2..k over the survivors only.
        for (int r = 1;
             r < f.numHashFunctions && m > 0;
             r++) {
            int kept = 0;
            for (int j = 0; j < m; j++) {
                long c = combined[j] + h2s[j];
                long idx = (c < 0 ? ~c : c) % bitSize;
                alive[kept] = alive[j];
                combined[kept] = c;
                h2s[kept] = h2s[j];
                kept += (int) ((bits[(int) (idx >>> 6)] >>> idx) & 1L);
            }
            m = kept;
        }
        for (int j = 0; j < m; j++) {
            int i = alive[j];
            out[i >>> 6] |= 1L << i;
        }
    }

    /** One key, as Spark's {@code mightContainLong}: the reference for tests. */
    public static boolean mightContainLong(Filter f, long item) {
        int h1 = PartitionKernels.hashLong(item, f.seed);
        int h2 = PartitionKernels.hashLong(item, h1);
        long c = (long) h1 * Integer.MAX_VALUE;
        for (int i = 0; i < f.numHashFunctions; i++) {
            c += h2;
            long idx = (c < 0 ? ~c : c) % f.bitSize;
            if ((f.words[(int) (idx >>> 6)] & (1L << idx)) == 0) {
                return false;
            }
        }
        return true;
    }
}
