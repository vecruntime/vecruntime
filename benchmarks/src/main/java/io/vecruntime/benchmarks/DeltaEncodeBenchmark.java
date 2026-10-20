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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.parquet.write.ByteSink;
import io.vecruntime.parquet.write.DeltaBinaryPackedEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
 * The columnar writer's {@code DELTA_BINARY_PACKED} encoder (#64), scalar loops
 * against the Vector API block (deltas, minimum, reduction and miniblock
 * widths): one 20,000-value page, values per microsecond.
 *
 * <p>Shapes: {@code sorted} (a ticket number, runs of equal values), {@code
 * keys} (random surrogate keys under a million), {@code wide} (full-range
 * random longs). {@code int32} encodes the low 32 bits (INT32 columns).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@OperationsPerInvocation(DeltaEncodeBenchmark.N)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class DeltaEncodeBenchmark {

    static final int N = 20_000;

    @Param({"scalar", "vector"})
    public String impl;

    @Param({"sorted", "keys", "wide"})
    public String shape;

    @Param({"int32", "int64"})
    public String width;

    private final long[] values = new long[N];
    private final ByteSink out = new ByteSink(N * 9);
    private DeltaBinaryPackedEncoder encoder;

    @Setup
    public void setUp() {
        Random r = new Random(17);
        for (int i = 0; i < N; i++) {
            values[i] = switch (shape) {
                        case "sorted" -> i / 10 + 1;
                        case "keys" -> 1 + r.nextInt(1_000_000);
                        default -> width.equals("int32") ? r.nextInt() : r.nextLong();
                    };
        }
        encoder = new DeltaBinaryPackedEncoder(impl.equals("vector"));
    }

    @Benchmark
    public int encode() {
        out.reset();
        if (width.equals("int32")) {
            encoder.encodeInts(values, N, out);
        } else {
            encoder.encodeLongs(values, N, out);
        }
        return out.size();
    }
}
