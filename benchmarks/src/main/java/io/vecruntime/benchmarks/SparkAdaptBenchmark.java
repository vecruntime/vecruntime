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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.spark.adapter.SparkColumnVectorBuffers;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * {@link SparkColumnVectorBuffers#copy} of a 4096-row BIGINT vector from
 * Spark's Parquet scan into our layout, on heap and off heap, with a share of
 * null rows (#541): the null bytes become a validity bitmap, the values one
 * bulk copy.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class SparkAdaptBenchmark {

    static final int N = 4096;

    @Param({"true", "false"})
    boolean onHeap;

    @Param({"0.01", "0.5"})
    double nullShare;

    WritableColumnVector cv;

    @Setup(Level.Trial)
    public void setup() {
        cv = onHeap ? new OnHeapColumnVector(N, DataTypes.LongType) : new OffHeapColumnVector(N, DataTypes.LongType);
        Random r = new Random(5412);
        for (int i = 0; i < N; i++) {
            if (r.nextDouble() < nullShare) {
                cv.putNull(i);
            } else {
                cv.putLong(i, r.nextLong());
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cv.close();
    }

    @Benchmark
    public long copy() {
        try (Arena a = Arena.ofConfined()) {
            VectorBuffers v = SparkColumnVectorBuffers.copy(cv, N, a);
            // Consume the result so none of the work can be dropped.
            return Bitmap.popcount(v.validity(), N) + v.getLong(1);
        }
    }
}
