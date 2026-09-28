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
import java.lang.foreign.ValueLayout;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.SelectKernels;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
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
 * {@link SelectKernels#select}, the {@code CASE WHEN} blend: two branches and
 * an {@code ELSE} over 8192 rows, with branch masks either random per row
 * ({@code mixed}) or taking long runs of rows ({@code runs}, 1024 rows each in
 * turn), and branch values with or without nulls (#541).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class SelectBenchmark {

    static final int N = 8192;

    @Param({"INT64", "BOOL"})
    String type;

    @Param({"false", "true"})
    boolean nulls;

    @Param({"mixed", "runs"})
    String masks;

    Arena arena;
    VecType vecType;
    MemorySegment[] wins;
    VectorBuffers[] branches;
    VectorBuffers otherwise;

    private static MemorySegment random(Arena a, long bytes, Random r) {
        MemorySegment s = a.allocate(Math.max(8, bytes), 8);
        for (long i = 0; i < s.byteSize(); i++) {
            s.set(ValueLayout.JAVA_BYTE, i, (byte) r.nextInt(256));
        }
        return s;
    }

    private VectorBuffers column(Random r) {
        long bytes = vecType == VecType.BOOL ? Bitmap.bytesFor(N) : (long) N * vecType.byteWidth();
        MemorySegment validity = nulls ? random(arena, Bitmap.bytesFor(N), r) : null;
        return SegmentVectorBuffers.fixedWidth(vecType, N, validity, random(arena, bytes, r));
    }

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random r = new Random(5413);
        vecType = VecType.valueOf(type);
        wins = new MemorySegment[2];
        for (int k = 0; k < 2; k++) {
            MemorySegment m = Bitmap.allocate(arena, N);
            if (masks.equals("mixed")) {
                m.copyFrom(random(arena, m.byteSize(), r));
            } else {
                // Branch k wins the runs [k*1024, (k+1)*1024) mod 3072: runs of 1024 rows each in turn.
                for (int i = 0; i < N; i++) {
                    if ((i / 1024) % 3 == k) {
                        Bitmap.set(m, i);
                    }
                }
            }
            wins[k] = m;
        }
        branches = new VectorBuffers[] {column(r), column(r)};
        otherwise = column(r);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public VectorBuffers select() {
        try (Arena a = Arena.ofConfined()) {
            VectorBuffers v = SelectKernels.select(vecType, N, wins, branches, otherwise, null,
                    a);
            return v.length() == N ? null : v;
        }
    }
}
