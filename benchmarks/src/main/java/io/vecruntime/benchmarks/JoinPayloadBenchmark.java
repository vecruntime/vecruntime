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

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.vecruntime.bench.JoinPayloadBench;
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
 * A broadcast hash join's probe and output with and without an
 * {@code array<int>} payload on the build side (#547). {@code lanes} is the
 * lane-only join every build side had before (it must not move with the row
 * store in the code); {@code array} adds a four-element array per build row,
 * read from the row store through a remapped view. Half of the streamed keys
 * match; {@code outer} pads the rest with nulls. The rate is streamed rows per
 * millisecond; every output value is read into the returned checksum.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class JoinPayloadBenchmark {

    static final int ROWS_PER_BATCH = 4096;
    static final int BATCHES = 32;

    @Param({"lanes", "array"})
    String payload;

    @Param({"inner", "outer"})
    String join;

    JoinPayloadBench bench;

    @Setup(Level.Trial)
    public void setup() {
        bench = new JoinPayloadBench(20_000, ROWS_PER_BATCH, BATCHES, payload.equals("array"),
                join.equals("outer"), 7L);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        bench.close();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS_PER_BATCH * BATCHES)
    public long probe() {
        return bench.run();
    }
}
