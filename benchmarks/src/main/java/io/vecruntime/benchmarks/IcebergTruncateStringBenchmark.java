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
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.IcebergTransformKernels;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Grouping a batch by a string partition transform (#901 follow-up): the old
 * per-row path, which decoded each value to a {@code String} and (for
 * truncate) took a code-point {@code substring} before hashing, against the new
 * lane path, which truncates on the UTF-8 bytes
 * ({@link IcebergTransformKernels#truncateUtf8ByteLength}) and hashes the bytes
 * with no {@code String} per row. Covers {@code truncate(N, s)} and {@code
 * identity(s)} over a batch of multi-byte strings. Grouping only needs the
 * value's hash, so the lane path never materialises a {@code String}; the old
 * path allocated one (and a substring) per row.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class IcebergTruncateStringBenchmark {

    static final int BATCH = 4096;
    static final int WIDTH = 4;

    Arena arena;
    VectorBuffers lane;
    byte[][] rowBytes;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(11);
        String[] values = new String[BATCH];
        rowBytes = new byte[BATCH][];
        for (int i = 0; i < BATCH; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("é".repeat(rnd.nextInt(3)));
            sb.append("日".repeat(rnd.nextInt(3)));
            sb.append("\uD83D\uDE00".repeat(rnd.nextInt(2))); // U+1F600, a 4-byte / surrogate-pair code point
            sb.append("s").append(rnd.nextInt(1000));
            values[i] = sb.toString();
            rowBytes[i] = values[i].getBytes(StandardCharsets.UTF_8);
        }
        lane = ArrowLayout.ofStrings(arena, values);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public void truncateOldPerRowString(Blackhole bh) {
        for (int i = 0; i < BATCH; i++) {
            String s = lane.getString(i); // a String per row
            int cp = Math.min(WIDTH, s.codePointCount(0, s.length()));
            String truncated = s.substring(0, s.offsetByCodePoints(0, cp)); // a substring per row
            bh.consume(truncated.hashCode());
        }
    }

    @Benchmark
    public void truncateLaneBytes(Blackhole bh) {
        for (int i = 0; i < BATCH; i++) {
            byte[] b = lane.getUtf8Bytes(i);
            int len = IcebergTransformKernels.truncateUtf8ByteLength(java.lang.foreign.MemorySegment.ofArray(b), 0, b.length, WIDTH);
            bh.consume(IcebergTransformKernels.hashBytes(java.lang.foreign.MemorySegment.ofArray(b), 0, len));
        }
    }

    @Benchmark
    public void identityOldPerRowString(Blackhole bh) {
        for (int i = 0; i < BATCH; i++) {
            bh.consume(lane.getString(i)
                           .hashCode()); // a String per row, then its hash
        }
    }

    @Benchmark
    public void identityLaneBytes(Blackhole bh) {
        for (int i = 0; i < BATCH; i++) {
            byte[] b = lane.getUtf8Bytes(i);
            bh.consume(IcebergTransformKernels.hashBytes(java.lang.foreign.MemorySegment.ofArray(b), 0, b.length));
        }
    }
}
