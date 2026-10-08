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

import java.util.Arrays;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * The split-block bloom filter of FactBloomFilter (#659): 256-bit blocks of
 * eight 32-bit words; a key's remixed hash picks one block and sets (or,
 * probing, must find) one bit in each of its eight words. This class is the one
 * definition of the layout and hashing; {@code
 * org.apache.spark.sql.vecruntime.BlockedBloomFilter} delegates here.
 *
 * <p>A partitioned filter is an array of sub-filters, {@code null} for a bucket
 * with no key; a key's sub-filter is {@code floorMod(h, parts.length)}, a mask
 * when the count is a power of two. {@link #probe} writes one result bit per
 * key into {@code out} (Arrow bit order: key {@code i} is bit {@code i & 63} of
 * word {@code i >>> 6}).
 *
 * <p>{@link #probe} (#664) remixes the batch's hashes in the widest {@code
 * LongVector} (eight per op on AVX-512), then tests each key's block in one
 * 256-bit op: the eight bit masks from one lanewise multiply-and-shift by the
 * salts, compared with the block in one compare. There is no data-dependent
 * branch per key, so the loads of consecutive keys' blocks -- each a likely
 * cache miss in a multi-megabyte filter -- are in flight together. On an
 * AVX-512 host it is 2.7 to 4 times {@link #mightContain} per key (JMH, {@code
 * BloomProbeBenchmark}). Packing two keys' masks into one 512-bit vector
 * measured slower than two 256-bit tests. Without 256-bit vectors the probe is
 * the same branch-free loop in scalar code.
 */
public final class BloomKernels {

    private BloomKernels() {}

    static final int[] SALTS = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b,
            0x9efc4947, 0x5c6bfb31};

    private static final VectorSpecies<Integer> I256 = IntVector.SPECIES_256;
    private static final VectorSpecies<Long> L = LongVector.SPECIES_PREFERRED;
    private static final IntVector SALT256 = IntVector.fromArray(I256, SALTS, 0);

    /**
     * Whether 256-bit integer vectors are native here; otherwise {@link #probe}
     * is scalar.
     */
    static final boolean VECTOR = IntVector.SPECIES_PREFERRED.vectorBitSize() >= 256;

    /**
     * MurmurHash3's 64-bit finalizer: every output bit depends on every input
     * bit.
     */
    public static long mix(long h) {
        long x = h;
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        return x ^ (x >>> 33);
    }

    /** The first word of the key's block in a filter of {@code numWords} words. */
    public static int blockBase(int numWords, long x) {
        return ((int) (((x >>> 32) * (long) (numWords >>> 3)) >>> 32)) << 3;
    }

    /**
     * The words of an empty filter of at least {@code bits} bits (whole 256-bit
     * blocks, at least one).
     */
    public static int[] create(long bits) {
        long blocks = Math.max(1L,
                Math.min((bits + 255) / 256, Integer.MAX_VALUE / 8L));
        return new int[(int) blocks * 8];
    }

    /** Adds the key with hash {@code h}. */
    public static void put(int[] words, long h) {
        long x = mix(h);
        int base = blockBase(words.length, x);
        int key = (int) x;
        for (int i = 0; i < 8; i++) {
            words[base + i] |= 1 << ((key * SALTS[i]) >>> 27);
        }
    }

    /**
     * Whether the key with hash {@code h} may be in the filter: one key, early
     * exit on the first missing bit.
     */
    public static boolean mightContain(int[] words, long h) {
        long x = mix(h);
        int base = blockBase(words.length, x);
        int key = (int) x;
        for (int i = 0; i < 8; i++) {
            if ((words[base + i] & (1 << ((key * SALTS[i]) >>> 27))) == 0) {
                return false;
            }
        }
        return true;
    }

    private static int part(long h, int count, long mask) {
        return (int) (mask >= 0 ? (h & mask) : Math.floorMod(h, (long) count));
    }

    private static long maskOf(int count) {
        return Integer.bitCount(count) == 1 ? count - 1L : -1L;
    }

    /**
     * Probes the first {@code n} hashes against the partitioned filter {@code
     * parts}; {@code xs} is scratch of at least {@code n} longs. Writes {@code
     * ceil(n / 64)} words of {@code out}.
     */
    public static void probe(int[][] parts, long[] hashes, int n,
            long[] out, long[] xs) {
        if (VECTOR) {
            probeVector(parts, hashes, n, out, xs);
        } else {
            probeScalar(parts, hashes, n, out);
        }
    }

    /**
     * {@link #mightContain} per key: the reference the batch probes are tested
     * against.
     */
    public static void probeReference(int[][] parts, long[] hashes, int n,
            long[] out) {
        long mask = maskOf(parts.length);
        Arrays.fill(out, 0, (n + 63) >>> 6, 0L);
        for (int i = 0; i < n; i++) {
            long h = hashes[i];
            int[] w = parts[part(h, parts.length, mask)];
            if (w != null && mightContain(w, h)) {
                out[i >>> 6] |= 1L << (i & 63);
            }
        }
    }

    /**
     * Branch-free scalar batch: all eight words tested, the missing bits ORed
     * together.
     */
    public static void probeScalar(int[][] parts, long[] hashes, int n,
            long[] out) {
        long mask = maskOf(parts.length);
        Arrays.fill(out, 0, (n + 63) >>> 6, 0L);
        for (int i = 0; i < n; i++) {
            long h = hashes[i];
            int[] w = parts[part(h, parts.length, mask)];
            if (w == null) {
                continue;
            }
            long x = mix(h);
            int base = blockBase(w.length, x);
            int key = (int) x;
            int miss = 0;
            for (int j = 0; j < 8; j++) {
                miss |= (1 << ((key * SALTS[j]) >>> 27)) & ~w[base + j];
            }
            out[i >>> 6] |= (miss == 0 ? 1L : 0L) << (i & 63);
        }
    }

    /**
     * Vector batch: remix in the widest long vectors, then one 256-bit test per
     * key.
     */
    public static void probeVector(int[][] parts, long[] hashes, int n,
            long[] out, long[] xs) {
        long mask = maskOf(parts.length);
        remix(hashes, n, xs);
        Arrays.fill(out, 0, (n + 63) >>> 6, 0L);
        IntVector one = IntVector.broadcast(I256, 1);
        for (int i = 0; i < n; i++) {
            int[] w = parts[part(hashes[i], parts.length, mask)];
            if (w == null) {
                continue;
            }
            long x = xs[i];
            IntVector bits = one.lanewise(VectorOperators.LSHL,
                    IntVector.broadcast(I256, (int) x)
                            .mul(SALT256)
                            .lanewise(VectorOperators.LSHR, 27));
            IntVector block = IntVector.fromArray(I256, w, blockBase(w.length, x));
            boolean hit = block.and(bits)
                               .eq(bits)
                               .allTrue();
            out[i >>> 6] |= (hit ? 1L : 0L) << (i & 63);
        }
    }

    /** {@link #mix} of {@code n} hashes into {@code xs}, a vector at a time. */
    static void remix(long[] hashes, int n, long[] xs) {
        int i = 0;
        int bound = L.loopBound(n);
        for (; i < bound; i += L.length()) {
            LongVector x = LongVector.fromArray(L, hashes, i);
            x = x.lanewise(VectorOperators.XOR, x.lanewise(VectorOperators.LSHR, 33)).mul(0xff51afd7ed558ccdL);
            x = x.lanewise(VectorOperators.XOR, x.lanewise(VectorOperators.LSHR, 33)).mul(0xc4ceb9fe1a85ec53L);
            x = x.lanewise(VectorOperators.XOR, x.lanewise(VectorOperators.LSHR, 33));
            x.intoArray(xs, i);
        }
        for (; i < n; i++) {
            xs[i] = mix(hashes[i]);
        }
    }
}
