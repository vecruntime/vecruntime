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
package io.vecruntime.benchmarks;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.WindowFrameKernels;
import io.vecruntime.kernels.reference.ScalarReference;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * {@code RANGE} window frames with value offsets over one sorted partition of
 * {@link #N} rows: the two-pointer bound search, then each frame aggregate,
 * against the scalar reference (Spark's own shape: the buffer walked and every
 * frame re-aggregated from scratch) and against a boxed per-row re-aggregation
 * standing for the row path this replaced (a {@code java.lang.Long} per value,
 * as the held-partition sliding path boxed it).
 *
 * <p>{@code frameRows} is the average number of rows a frame spans (the key is
 * a random walk with steps in {@code [0, 4)}, so an offset of {@code 2 *
 * frameRows} covers about that many rows); {@code nullFraction} the share of
 * null input values.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(WindowFrameBenchmark.N)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class WindowFrameBenchmark {

    static final int N = 65536;

    /** Rows per frame: a narrow moving window and a wide one. */
    @Param({"8", "256"})
    int frameRows;

    /** Fraction of null input values. */
    @Param({"0.0", "0.1"})
    double nullFraction;

    long[] keys;
    long[] longs;
    double[] doubles;
    long[] validity;
    int[] lo;
    int[] hi;
    long[] outLongs;
    double[] outDoubles;
    long[] outValidity;
    Long[] boxed;

    @Setup(Level.Trial)
    public void setup() {
        Random rnd = new Random(58);
        keys = new long[N];
        longs = new long[N];
        doubles = new double[N];
        boxed = new Long[N];
        validity = nullFraction == 0.0 ? null : new long[(N + 63) >>> 6];
        long k = 0;
        for (int i = 0; i < N; i++) {
            k += rnd.nextInt(4);
            keys[i] = k;
            longs[i] = rnd.nextLong(1_000_000);
            doubles[i] = rnd.nextDouble() * 1000;
            boolean valid = validity == null || rnd.nextDouble() >= nullFraction;
            if (valid && validity != null) {
                validity[i >>> 6] |= 1L << (i & 63);
            }
            boxed[i] = valid ? Long.valueOf(longs[i]) : null;
        }
        lo = new int[N];
        hi = new int[N];
        outLongs = new long[N];
        outDoubles = new double[N];
        outValidity = new long[(N + 63) >>> 6];
        // Frames of about frameRows rows: [k - 2 * frameRows, k].
        WindowFrameKernels.rangeBounds(
                keys,
                null,
                N,
                false,
                true,
                false,
                -2L * frameRows,
                false,
                0,
                64,
                false,
                lo,
                hi);
    }

    @Benchmark
    public int rangeBounds_kernel() {
        WindowFrameKernels.rangeBounds(
                keys,
                null,
                N,
                false,
                true,
                false,
                -2L * frameRows,
                false,
                0,
                64,
                false,
                lo,
                hi);
        return lo[N - 1];
    }

    @Benchmark
    public int rangeBounds_reference() {
        ScalarReference.rangeBounds(
                keys,
                null,
                N,
                false,
                true,
                false,
                -2L * frameRows,
                false,
                0,
                64,
                false,
                lo,
                hi);
        return lo[N - 1];
    }

    @Benchmark
    public long sumLong_kernel() {
        WindowFrameKernels.frameSumLong(longs, validity, lo, hi, N, false,
                outLongs, outValidity);
        return outLongs[N - 1];
    }

    @Benchmark
    public long sumLongChecked_kernel() {
        WindowFrameKernels.frameSumLong(longs, validity, lo, hi, N, true,
                outLongs, outValidity);
        return outLongs[N - 1];
    }

    @Benchmark
    public long sumLong_reference() {
        ScalarReference.frameSumLong(longs, validity, lo, hi, N, false,
                outLongs, outValidity);
        return outLongs[N - 1];
    }

    /**
     * The boxed per-row re-aggregation of the row path: a fresh accumulator per
     * row, a boxed read per value.
     */
    @Benchmark
    public long sumLong_boxedRowPath() {
        Arrays.fill(outValidity, 0L);
        for (int i = 0; i < N; i++) {
            long sum = 0;
            long count = 0;
            for (int j = lo[i];
                 j < hi[i];
                 j++) {
                Object v = boxed[j];
                if (v != null) {
                    sum += ((Long) v).longValue();
                    count++;
                }
            }
            Object result = count == 0 ? null : Long.valueOf(sum);
            outLongs[i] = result == null ? 0L : ((Long) result).longValue();
        }
        return outLongs[N - 1];
    }

    @Benchmark
    public double sumDouble_kernel() {
        WindowFrameKernels.frameSumDouble(doubles, validity, lo, hi, N, false,
                outDoubles, outValidity);
        return outDoubles[N - 1];
    }

    @Benchmark
    public double sumDouble_reference() {
        ScalarReference.frameSumDouble(doubles, validity, lo, hi, N, false,
                outDoubles, outValidity);
        return outDoubles[N - 1];
    }

    @Benchmark
    public long count_kernel() {
        WindowFrameKernels.frameCount(validity, lo, hi, N, false, outLongs);
        return outLongs[N - 1];
    }

    @Benchmark
    public long count_reference() {
        ScalarReference.frameCount(validity, lo, hi, N, false, outLongs);
        return outLongs[N - 1];
    }

    @Benchmark
    public long maxLong_kernel() {
        WindowFrameKernels.frameMinMaxLong(longs, validity, lo, hi, N, false,
                outLongs, outValidity);
        return outLongs[N - 1];
    }

    @Benchmark
    public long maxLong_reference() {
        ScalarReference.frameMinMaxLong(longs, validity, lo, hi, N, false,
                outLongs, outValidity);
        return outLongs[N - 1];
    }

    @Benchmark
    public double minDouble_kernel() {
        WindowFrameKernels.frameMinMaxDouble(doubles, validity, lo, hi, N, true,
                outDoubles, outValidity);
        return outDoubles[N - 1];
    }
}
