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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.SequenceKernels;
import io.vecruntime.kernels.VecType;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The fill behind the columnar {@code range()} leaf: one batch of {@code
 * start + i * step} written into a native INT64 buffer by {@link
 * SequenceKernels#range} (lane offsets plus a broadcast base per block) against
 * the scalar oracle {@link ScalarReference#range}. Reported as operations per
 * millisecond where one operation is one row, so the number is rows per
 * millisecond. {@code step = 1} takes the {@code iota} path that
 * {@code monotonically_increasing_id()} also uses.
 *
 * <pre>
 * java --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
 *   -jar benchmarks/target/benchmarks.jar RangeFillBenchmark -wi 2 -i 3 -w 1 -r 1 -f 1
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(RangeFillBenchmark.N)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class RangeFillBenchmark {

    /** One {@code spark.sql.inMemoryColumnarStorage.batchSize} batch. */
    static final int N = 10000;

    @Param({"1", "3", "-7"})
    long step;

    Arena arena;
    MemorySegment out;
    long start;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        out = ArrowLayout.allocateData(arena, VecType.INT64, N);
        start = 1L << 40;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public long vector() {
        return SequenceKernels.range(out, N, start, step);
    }

    @Benchmark
    public long scalar() {
        return ScalarReference.range(out, N, start, step);
    }
}
