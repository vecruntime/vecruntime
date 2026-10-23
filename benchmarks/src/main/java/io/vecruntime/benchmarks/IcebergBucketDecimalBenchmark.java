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
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.Decimal128;
import io.vecruntime.kernels.IcebergTransformKernels;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Iceberg's {@code bucket} of a decimal over a whole batch (#901 item 11): the
 * old per-row path, which formed the unscaled value's two's-complement bytes
 * with a {@link BigInteger} and a {@code byte[]} per row, against the new
 * {@link IcebergTransformKernels#bucketDecimal} / {@code bucketDecimal128},
 * which read those bytes straight from the limbs with no allocation. Both
 * produce Iceberg's hash; this measures the allocation the fix removes, on a
 * narrow decimal (INT64 unscaled, precision &le; 18) and a wide one
 * (DECIMAL128).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class IcebergBucketDecimalBenchmark {

    static final int BATCH = 4096;
    static final int BUCKETS = 128;

    Arena arena;
    VectorBuffers narrow; // INT64 unscaled values (precision <= 18)
    VectorBuffers wide; // DECIMAL128 unscaled values
    long[] narrowUnscaled;
    BigInteger[] wideUnscaled;
    MemorySegment out;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(7);
        narrowUnscaled = new long[BATCH];
        long[] data = new long[BATCH];
        for (int i = 0; i < BATCH; i++) {
            narrowUnscaled[i] = rnd.nextLong() >> rnd.nextInt(40);
            data[i] = narrowUnscaled[i];
        }
        narrow = ArrowLayout.ofLongs(arena, data, null);

        wideUnscaled = new BigInteger[BATCH];
        for (int i = 0; i < BATCH; i++) {
            wideUnscaled[i] = new BigInteger(90 + rnd.nextInt(30), rnd).subtract(BigInteger.ONE.shiftLeft(100));
        }
        wide = ArrowLayout.ofDecimal128(arena, wideUnscaled, null);

        out = ArrowLayout.allocateData(arena, VecType.INT32, BATCH);

        // Guard: the lane path equals the old BigInteger path, bucket for bucket.
        for (int i = 0; i < BATCH; i++) {
            if (oldHashDecimal(narrowUnscaled[i]) != IcebergTransformKernels.hashDecimal(narrowUnscaled[i])) {
                throw new IllegalStateException("narrow hash mismatch at " + i);
            }
            int oldWide = IcebergTransformKernels.hashBytes(wideUnscaled[i].toByteArray());
            int newWide = IcebergTransformKernels.hashDecimal128(Decimal128.hiOf(wideUnscaled[i]), Decimal128.loOf(wideUnscaled[i]));
            if (oldWide != newWide) {
                throw new IllegalStateException("wide hash mismatch at " + i);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    /** The pre-#901 path: a BigInteger and a byte[] per row. */
    private static int oldHashDecimal(long unscaled) {
        return IcebergTransformKernels.hashBytes(BigInteger.valueOf(unscaled)
                .toByteArray());
    }

    @Benchmark
    public MemorySegment narrowOldBigInteger() {
        for (int i = 0; i < BATCH; i++) {
            out.setAtIndex(VectorBuffers.LE_INT, i, IcebergTransformKernels.bucket(oldHashDecimal(narrowUnscaled[i]), BUCKETS));
        }
        return out;
    }

    @Benchmark
    public MemorySegment narrowLane() {
        IcebergTransformKernels.bucketDecimal(narrow, BUCKETS, out);
        return out;
    }

    @Benchmark
    public MemorySegment wideOldBigInteger() {
        for (int i = 0; i < BATCH; i++) {
            out.setAtIndex(VectorBuffers.LE_INT, i, IcebergTransformKernels.bucket(IcebergTransformKernels.hashBytes(wideUnscaled[i].toByteArray()), BUCKETS));
        }
        return out;
    }

    @Benchmark
    public MemorySegment wideLane() {
        IcebergTransformKernels.bucketDecimal128(wide, BUCKETS, out);
        return out;
    }
}
