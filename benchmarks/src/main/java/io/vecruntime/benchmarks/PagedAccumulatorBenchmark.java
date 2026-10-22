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
import java.nio.ByteOrder;
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
 * #941, paged design: grouped accumulator state in fixed-size {@link NativePool}
 * pages instead of one block per array that doubles and is copied. Group
 * {@code g} lives in page {@code g >> shift} at slot {@code g & mask}. The
 * question is whether that extra shift, mask and page lookup per update is
 * hidden behind the memory latency of the scatter, at every table size, so
 * the state can go native without a group-count threshold.
 *
 * <p>Scatter of a long sum plus count over uniform ids (worst locality), all
 * with the batch copied into heap scratch first, as the kernel does:
 *
 * <ul>
 *   <li>{@code scatterArray}: today, two {@code long[]} on the heap;
 *   <li>{@code scatterFlatSegment}: one pool block per array (the per-array
 *       design measured on the cluster);
 *   <li>{@code scatterPagedSegments}: a {@code MemorySegment[]} of pages per
 *       array (page object load + its bounds check per access);
 *   <li>{@code scatterPagedAddr}: page base addresses in a {@code long[]},
 *       accessed through one segment spanning the address space (no page
 *       object, no per-page bounds check);
 *   <li>{@code scatterPagedAddrPair}: as above, with a group's sum and count
 *       side by side in one page, so an update touches one cache line, not two.
 * </ul>
 *
 * Building a table of {@code groups} from empty: {@code buildArray} doubles
 * and copies from 1,024 as the heap state does; {@code buildPaged} appends
 * pages taken from the pool.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class PagedAccumulatorBenchmark {

    static final int N = 4096;
    static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    /** Every address, for access by absolute address into pool pages. */
    static final MemorySegment ALL = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);

    @Param({"4", "1024", "16384", "262144", "1048576", "4194304"})
    int groups;

    /** Page size in KiB. */
    @Param({"64", "256", "1024"})
    int pageKiB;

    Arena inputArena;
    Arena pool;
    MemorySegment input;
    int[] ids;
    long[] scratch = new long[N];

    long[] sumArr;
    long[] countArr;
    MemorySegment sumSeg;
    MemorySegment countSeg;

    int shift; // log2(longs per page)
    long mask;
    int pairShift; // log2(groups per page) when a group holds two longs
    long pairMask;
    MemorySegment[] sumPages;
    MemorySegment[] countPages;
    long[] sumAddr;
    long[] countAddr;
    long[] pairAddr;

    @Setup(Level.Trial)
    public void setup() {
        inputArena = Arena.ofShared();
        pool = NativePool.shared().newArena();
        Random rnd = new Random(7);
        input = inputArena.allocate(8L * N, 64);
        ids = new int[N];
        for (int i = 0; i < N; i++) {
            input.setAtIndex(LE_LONG, i, rnd.nextInt(1_000_000));
            ids[i] = rnd.nextInt(groups);
        }
        sumArr = new long[groups];
        countArr = new long[groups];
        sumSeg = pool.allocate(8L * groups, 64).fill((byte) 0);
        countSeg = pool.allocate(8L * groups, 64).fill((byte) 0);

        long pageBytes = 1024L * pageKiB;
        int perPage = (int) (pageBytes / 8);
        shift = Integer.numberOfTrailingZeros(perPage);
        mask = perPage - 1;
        pairShift = shift - 1;
        pairMask = (perPage >> 1) - 1;
        int pages = (groups + perPage - 1) / perPage;
        int pairPages = (groups + (perPage >> 1) - 1) / (perPage >> 1);
        sumPages = new MemorySegment[pages];
        countPages = new MemorySegment[pages];
        sumAddr = new long[pages];
        countAddr = new long[pages];
        for (int p = 0; p < pages; p++) {
            sumPages[p] = pool.allocate(pageBytes, 64).fill((byte) 0);
            countPages[p] = pool.allocate(pageBytes, 64).fill((byte) 0);
            sumAddr[p] = sumPages[p].address();
            countAddr[p] = countPages[p].address();
        }
        pairAddr = new long[pairPages];
        for (int p = 0; p < pairPages; p++) {
            pairAddr[p] = pool.allocate(pageBytes, 64)
                              .fill((byte) 0)
                              .address();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.close();
        inputArena.close();
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterArray() {
        long[] x = scratch;
        MemorySegment.copy(input, LE_LONG, 0, x, 0, N);
        long[] sum = sumArr;
        long[] count = countArr;
        int[] g = ids;
        for (int i = 0; i < N; i++) {
            int k = g[i];
            sum[k] += x[i];
            count[k]++;
        }
        return sum[g[0]];
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterFlatSegment() {
        long[] x = scratch;
        MemorySegment.copy(input, LE_LONG, 0, x, 0, N);
        MemorySegment sum = sumSeg;
        MemorySegment count = countSeg;
        int[] g = ids;
        for (int i = 0; i < N; i++) {
            long k = g[i];
            sum.setAtIndex(LONG, k, sum.getAtIndex(LONG, k) + x[i]);
            count.setAtIndex(LONG, k, count.getAtIndex(LONG, k) + 1);
        }
        return sum.getAtIndex(LONG, g[0]);
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterPagedSegments() {
        long[] x = scratch;
        MemorySegment.copy(input, LE_LONG, 0, x, 0, N);
        MemorySegment[] sum = sumPages;
        MemorySegment[] count = countPages;
        int s = shift;
        long m = mask;
        int[] g = ids;
        for (int i = 0; i < N; i++) {
            int k = g[i];
            int p = k >>> s;
            long o = k & m;
            MemorySegment sp = sum[p];
            MemorySegment cp = count[p];
            sp.setAtIndex(LONG, o, sp.getAtIndex(LONG, o) + x[i]);
            cp.setAtIndex(LONG, o, cp.getAtIndex(LONG, o) + 1);
        }
        int k = g[0];
        return sum[k >>> s].getAtIndex(LONG, k & m);
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterPagedAddr() {
        long[] x = scratch;
        MemorySegment.copy(input, LE_LONG, 0, x, 0, N);
        MemorySegment all = ALL;
        long[] sum = sumAddr;
        long[] count = countAddr;
        int s = shift;
        long m = mask;
        int[] g = ids;
        for (int i = 0; i < N; i++) {
            int k = g[i];
            int p = k >>> s;
            long o = (k & m) << 3;
            long sa = sum[p] + o;
            long ca = count[p] + o;
            all.set(LONG, sa, all.get(LONG, sa) + x[i]);
            all.set(LONG, ca, all.get(LONG, ca) + 1);
        }
        int k = g[0];
        return all.get(LONG, sum[k >>> s] + ((k & m) << 3));
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterPagedAddrPair() {
        long[] x = scratch;
        MemorySegment.copy(input, LE_LONG, 0, x, 0, N);
        MemorySegment all = ALL;
        long[] pages = pairAddr;
        int s = pairShift;
        long m = pairMask;
        int[] g = ids;
        for (int i = 0; i < N; i++) {
            int k = g[i];
            long a = pages[k >>> s] + ((k & m) << 4);
            all.set(LONG, a, all.get(LONG, a) + x[i]);
            all.set(LONG, a + 8, all.get(LONG, a + 8) + 1);
        }
        int k = g[0];
        return all.get(LONG, pages[k >>> s] + ((k & m) << 4));
    }

    /**
     * A table of {@code groups} built from empty the heap way: doubling from
     * 1,024.
     */
    @Benchmark
    public long buildArray() {
        int cap = Math.min(1024, groups);
        long[] a = new long[cap];
        while (cap < groups) {
            long[] grown = new long[cap << 1];
            System.arraycopy(a, 0, grown, 0, cap);
            a = grown;
            cap <<= 1;
        }
        return a.length;
    }

    /** The same table built by appending pool pages. */
    @Benchmark
    public long buildPaged() {
        long pageBytes = 1024L * pageKiB;
        int perPage = (int) (pageBytes / 8);
        int pages = (groups + perPage - 1) / perPage;
        long last = 0;
        try (Arena a = NativePool.shared().newArena()) {
            for (int p = 0; p < pages; p++) {
                last = a.allocate(pageBytes, 64)
                        .fill((byte) 0)
                        .address();
            }
        }
        return last;
    }
}
