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
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
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
 * #941 phase 1: where the grouped accumulators live. Today {@code
 * GroupedAccumulators} keeps its state in {@code long[]} on the heap, which at
 * 10 TB fills the executors' old generation. The candidate is a block from
 * {@link NativePool} (counted in the operators' memory mode). This measures the
 * storage choice on the loops the kernel runs, in the shape the kernel runs
 * them, so the move is decided on numbers:
 *
 * <ul>
 *   <li>the many-groups scatter of a long sum plus count, one copy ({@code ids}
 *       uniform over {@code groups}: the worst locality, the partial aggregates
 *       of q78/q67);
 *   <li>the same with the batch read straight from its Arrow segment instead of
 *       copied into a heap scratch array first;
 *   <li>a Vector API index-map scatter on arrays, as a control only (the API
 *       has no segment form, and with duplicate ids in one vector it drops
 *       updates, so it is not a correct kernel);
 *   <li>the collapse of four interleaved copies into one (#689), scalar and
 *       Vector API, on arrays and on segments;
 *   <li>growth: doubling the state, {@code System.arraycopy} into a new array
 *       against {@code MemorySegment.copy} into a new pool block.
 * </ul>
 *
 * Pool segments are global-scope views of malloc'd blocks, so an access carries
 * a bounds check but no arena liveness check.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class AccumulatorStorageBenchmark {

    static final int N = 4096;
    static final int COPIES = 4;
    static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final VectorSpecies<Long> L = LongVector.SPECIES_PREFERRED;

    @Param({"4", "1024", "16384", "1048576", "4194304"})
    int groups;

    Arena inputArena;
    Arena pool;
    MemorySegment input; // the batch's values, as an Arrow data buffer holds them
    int[] ids;
    long[] scratch = new long[N];

    long[] sumArr;
    long[] countArr;
    MemorySegment sumSeg;
    MemorySegment countSeg;

    long[] copiesArr; // COPIES x collapseCap, the layout below COLLAPSE_GROUPS
    MemorySegment copiesSeg;
    long[] collapsedArr;
    MemorySegment collapsedSeg;
    int collapseCap;

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

        // The interleaved copies exist only up to COLLAPSE_GROUPS (16k); above, collapse a
        // 16k table so the case still measures the loop rather than nothing.
        collapseCap = Math.min(groups, 1 << 14);
        collapseCap = Math.max(collapseCap, L.length());
        copiesArr = new long[COPIES * collapseCap];
        for (int i = 0; i < copiesArr.length; i++) {
            copiesArr[i] = rnd.nextInt(1000);
        }
        copiesSeg = pool.allocate(8L * copiesArr.length, 64);
        MemorySegment.copy(copiesArr, 0, copiesSeg, LONG, 0, copiesArr.length);
        collapsedArr = new long[collapseCap];
        collapsedSeg = pool.allocate(8L * collapseCap, 64);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.close();
        inputArena.close();
    }

    // ---- many-groups scatter: sum + count, one copy ------------------------------------

    /** Today: the batch copied into a heap scratch array, state in heap arrays. */
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

    /** State in a pool block, the batch still copied into heap scratch first. */
    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterSegment() {
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

    /**
     * State in a pool block, the batch read straight from its segment: no copy
     * at all.
     */
    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterSegmentDirect() {
        MemorySegment in = input;
        MemorySegment sum = sumSeg;
        MemorySegment count = countSeg;
        int[] g = ids;
        for (int i = 0; i < N; i++) {
            long k = g[i];
            sum.setAtIndex(LONG, k, sum.getAtIndex(LONG, k) + in.getAtIndex(LE_LONG, i));
            count.setAtIndex(LONG, k, count.getAtIndex(LONG, k) + 1);
        }
        return sum.getAtIndex(LONG, g[0]);
    }

    /**
     * Control, not a kernel: a Vector API index-map gather-add-scatter on the
     * arrays. Lanes with equal ids lose updates, so the result is wrong
     * whenever a vector holds a repeated group; it only shows what the
     * instruction sequence costs on this CPU.
     */
    @Benchmark
    @OperationsPerInvocation(N)
    public long scatterVectorControl() {
        long[] x = scratch;
        MemorySegment.copy(input, LE_LONG, 0, x, 0, N);
        long[] sum = sumArr;
        long[] count = countArr;
        int[] g = ids;
        int step = L.length();
        LongVector one = LongVector.broadcast(L, 1);
        int i = 0;
        for (; i + step <= N; i += step) {
            LongVector v = LongVector.fromArray(L, x, i);
            LongVector.fromArray(L, sum, 0, g, i)
                    .add(v)
                    .intoArray(sum, 0, g, i);
            LongVector.fromArray(L, count, 0, g, i)
                    .add(one)
                    .intoArray(count, 0, g, i);
        }
        for (; i < N; i++) {
            int k = g[i];
            sum[k] += x[i];
            count[k]++;
        }
        return sum[g[0]];
    }

    // ---- collapse of the interleaved copies (#689) -------------------------------------

    @Benchmark
    public long collapseArrayScalar() {
        long[] c = copiesArr;
        long[] out = collapsedArr;
        int cap = collapseCap;
        System.arraycopy(c, 0, out, 0, cap);
        for (int k = 1; k < COPIES; k++) {
            int base = k * cap;
            for (int g = 0; g < cap; g++) {
                out[g] += c[base + g];
            }
        }
        return out[cap - 1];
    }

    @Benchmark
    public long collapseArrayVector() {
        long[] c = copiesArr;
        long[] out = collapsedArr;
        int cap = collapseCap;
        int step = L.length();
        int bound = L.loopBound(cap);
        int g = 0;
        for (; g < bound; g += step) {
            LongVector acc = LongVector.fromArray(L, c, g);
            for (int k = 1; k < COPIES; k++) {
                acc = acc.add(LongVector.fromArray(L, c, k * cap + g));
            }
            acc.intoArray(out, g);
        }
        for (; g < cap; g++) {
            long s = c[g];
            for (int k = 1; k < COPIES; k++) {
                s += c[k * cap + g];
            }
            out[g] = s;
        }
        return out[cap - 1];
    }

    @Benchmark
    public long collapseSegmentScalar() {
        MemorySegment c = copiesSeg;
        MemorySegment out = collapsedSeg;
        long cap = collapseCap;
        for (long g = 0; g < cap; g++) {
            long s = c.getAtIndex(LONG, g);
            for (int k = 1; k < COPIES; k++) {
                s += c.getAtIndex(LONG, k * cap + g);
            }
            out.setAtIndex(LONG, g, s);
        }
        return out.getAtIndex(LONG, cap - 1);
    }

    @Benchmark
    public long collapseSegmentVector() {
        MemorySegment c = copiesSeg;
        MemorySegment out = collapsedSeg;
        int cap = collapseCap;
        int step = L.length();
        int bound = L.loopBound(cap);
        ByteOrder bo = ByteOrder.nativeOrder();
        int g = 0;
        for (; g < bound; g += step) {
            LongVector acc = LongVector.fromMemorySegment(L, c, 8L * g, bo);
            for (int k = 1; k < COPIES; k++) {
                acc = acc.add(LongVector.fromMemorySegment(L, c, 8L * ((long) k * cap + g), bo));
            }
            acc.intoMemorySegment(out, 8L * g, bo);
        }
        for (; g < cap; g++) {
            long s = c.getAtIndex(LONG, g);
            for (int k = 1; k < COPIES; k++) {
                s += c.getAtIndex(LONG, (long) k * cap + g);
            }
            out.setAtIndex(LONG, g, s);
        }
        return out.getAtIndex(LONG, cap - 1);
    }

    // ---- growth: double the state -------------------------------------------------------

    @Benchmark
    public long growArray() {
        long[] grown = new long[2 * groups];
        System.arraycopy(sumArr, 0, grown, 0, groups);
        return grown[groups - 1];
    }

    @Benchmark
    public long growSegment() {
        try (Arena a = NativePool.shared().newArena()) {
            MemorySegment grown = a.allocate(16L * groups, 64);
            MemorySegment.copy(sumSeg, 0, grown, 0, 8L * groups);
            grown.asSlice(8L * groups).fill((byte) 0);
            return grown.getAtIndex(LONG, groups - 1);
        }
    }
}
