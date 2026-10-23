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
package io.vecruntime.benchmarks;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.NativePool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * #941 phase 2a: where the grouped aggregate's hash table lives. {@code
 * GroupKeyTable}'s generic path keeps an open-addressing table as two heap
 * {@code int[]} -- {@code slots} (group id or -1) and {@code groupHashes} --
 * and the keys in per-column arrays indexed by group id. This measures the
 * lookup of a batch of rows whose groups already exist (the steady state of a
 * partial aggregate) in the shapes the table could take:
 *
 * <ul>
 *   <li>{@code probeHeapSplit}: today -- two {@code int[]} plus a {@code
 *       long[]} of keys;
 *   <li>{@code probeHeapPacked}: one heap {@code long[]}, hash and group id in
 *       one entry (one cache line a probe instead of two);
 *   <li>{@code probeNativePacked}: the same packed entry in a pool block, keys
 *       in a pool block;
 *   <li>{@code probeHeapInline}: the same inline entry in a heap {@code
 *       long[]} -- what the table's #677 fast path already is for integer
 *       keys, so the fair baseline for moving that path into the pool;
 *   <li>{@code probeNativeInline}: a pool block whose entry carries the key
 *       itself next to the group id (hash re-derived only on a mismatch), the
 *       shape of the table's inline-key fast path (#677), so a hit touches one
 *       line and never the key column.
 * </ul>
 *
 * Load factor 0.5, as the table keeps; uniform keys over {@code groups}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class GroupTableStorageBenchmark {

    static final int N = 4096;
    static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;

    @Param({"1024", "16384", "262144", "1048576", "4194304"})
    int groups;

    Arena pool;
    long[] rowKeys = new long[N];
    int[] rowHashes = new int[N];
    int[] out = new int[N];

    int mask;
    int[] slots;
    int[] hashes;
    long[] keys;
    long[] packed;
    long[] heapInline; // pairs on the heap: key, (hash << 32 | id) -- the table's #677 fast layout
    MemorySegment nPacked;
    MemorySegment nKeys;
    MemorySegment nInline; // pairs: key, (hash << 32 | id); id -1 = empty

    static int hash(long k) {
        long h = k * 0x9E3779B97F4A7C15L;
        return (int) (h ^ (h >>> 32));
    }

    @Setup(Level.Trial)
    public void setup() {
        pool = NativePool.shared().newArena();
        int cap = Integer.highestOneBit(groups) << 2; // load 0.25-0.5
        mask = cap - 1;
        slots = new int[cap];
        hashes = new int[cap];
        packed = new long[cap];
        heapInline = new long[2 * cap];
        for (int p = 0; p < cap; p++) {
            heapInline[2 * p + 1] = -1L;
        }
        java.util.Arrays.fill(slots, -1);
        java.util.Arrays.fill(packed, -1L);
        keys = new long[groups];
        nPacked = pool.allocate(8L * cap, 64);
        nInline = pool.allocate(16L * cap, 64);
        nKeys = pool.allocate(8L * groups, 64);
        for (long p = 0; p < cap; p++) {
            nPacked.setAtIndex(LONG, p, -1L);
            nInline.setAtIndex(LONG, 2 * p + 1, -1L);
        }
        Random rnd = new Random(11);
        for (int g = 0; g < groups; g++) {
            long k = rnd.nextLong();
            keys[g] = k;
            nKeys.setAtIndex(LONG, g, k);
            int h = hash(k);
            int pos = h & mask;
            while (slots[pos] >= 0) {
                pos = (pos + 1) & mask;
            }
            slots[pos] = g;
            hashes[pos] = h;
            long e = ((long) h << 32) | g;
            packed[pos] = e;
            nPacked.setAtIndex(LONG, pos, e);
            nInline.setAtIndex(LONG, 2L * pos, k);
            nInline.setAtIndex(LONG, 2L * pos + 1, e);
            heapInline[2 * pos] = k;
            heapInline[2 * pos + 1] = e;
        }
        for (int i = 0; i < N; i++) {
            long k = keys[rnd.nextInt(groups)];
            rowKeys[i] = k;
            rowHashes[i] = hash(k);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.close();
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int probeHeapSplit() {
        int[] s = slots;
        int[] hs = hashes;
        long[] ks = keys;
        int m = mask;
        int acc = 0;
        for (int i = 0; i < N; i++) {
            int h = rowHashes[i];
            long k = rowKeys[i];
            int pos = h & m;
            int id;
            while ((id = s[pos]) >= 0
                    && (hs[pos] != h || ks[id] != k)) {
                pos = (pos + 1) & m;
            }
            out[i] = id;
            acc += id;
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int probeHeapPacked() {
        long[] t = packed;
        long[] ks = keys;
        int m = mask;
        int acc = 0;
        for (int i = 0; i < N; i++) {
            int h = rowHashes[i];
            long k = rowKeys[i];
            int pos = h & m;
            int id;
            long e;
            while ((e = t[pos]) != -1L
                    && ((int) (e >>> 32) != h || ks[(int) e] != k)) {
                pos = (pos + 1) & m;
            }
            id = (int) e;
            out[i] = id;
            acc += id;
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int probeNativePacked() {
        MemorySegment t = nPacked;
        MemorySegment ks = nKeys;
        int m = mask;
        int acc = 0;
        for (int i = 0; i < N; i++) {
            int h = rowHashes[i];
            long k = rowKeys[i];
            long pos = h & m;
            long e;
            while ((e = t.getAtIndex(LONG, pos)) != -1L
                    && ((int) (e >>> 32) != h || ks.getAtIndex(LONG, (int) e) != k)) {
                pos = (pos + 1) & m;
            }
            int id = (int) e;
            out[i] = id;
            acc += id;
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int probeHeapInline() {
        long[] t = heapInline;
        int m = mask;
        int acc = 0;
        for (int i = 0; i < N; i++) {
            long k = rowKeys[i];
            int pos = rowHashes[i] & m;
            long e;
            while ((e = t[2 * pos + 1]) != -1L && t[2 * pos] != k) {
                pos = (pos + 1) & m;
            }
            int id = (int) e;
            out[i] = id;
            acc += id;
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int probeNativeInline() {
        MemorySegment t = nInline;
        int m = mask;
        int acc = 0;
        for (int i = 0; i < N; i++) {
            long k = rowKeys[i];
            long pos = rowHashes[i] & m;
            long e;
            while ((e = t.getAtIndex(LONG, 2 * pos + 1)) != -1L && t.getAtIndex(LONG, 2 * pos) != k) {
                pos = (pos + 1) & m;
            }
            int id = (int) e;
            out[i] = id;
            acc += id;
        }
        return acc;
    }
}
