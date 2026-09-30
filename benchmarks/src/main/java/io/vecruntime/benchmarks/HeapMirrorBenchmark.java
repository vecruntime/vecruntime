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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.HeapMirror;
import io.vecruntime.kernels.VectorBuffers;
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
 * {@link HeapMirror}'s validity moves (#555): mirroring a nullable INT64 column
 * ({@code of}) and gathering it with an output bitmap ({@code gather}), each
 * against the per-word {@link Bitmap#wordAt} / {@link Bitmap#setWord} loop it
 * replaced. Rows per ms.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(HeapMirrorBenchmark.N)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class HeapMirrorBenchmark {
    static final int N = 4096;

    /** Share of null rows, in percent. */
    @Param({"5"})
    int nullPercent;

    Arena arena;
    VectorBuffers in;
    HeapMirror mirror;
    int[] idx;
    MemorySegment outData;
    MemorySegment outValidity;
    HeapMirror.GatherScratch scratch;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(11);
        long[] values = new long[N];
        boolean[] nulls = new boolean[N];
        for (int i = 0; i < N; i++) {
            values[i] = rnd.nextLong();
            nulls[i] = rnd.nextInt(100) < nullPercent;
        }
        in = ArrowLayout.ofLongs(arena, values, nulls);
        mirror = HeapMirror.of(in);
        idx = new int[N];
        for (int i = 0; i < N; i++) {
            idx[i] = rnd.nextInt(N);
        }
        outData = ArrowLayout.allocateData(arena, in.type(), N);
        outValidity = ArrowLayout.allocateBitmap(arena, N);
        scratch = new HeapMirror.GatherScratch();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public long of_bulk() {
        return HeapMirror.of(in).validity[0];
    }

    /**
     * {@code of} as it ran before #555: the same data copy, then a {@code
     * wordAt} per validity word.
     */
    @Benchmark
    public long of_perWord() {
        long[] longs = new long[N];
        MemorySegment.copy(in.data(), VectorBuffers.LE_LONG, 0L, longs, 0,
                N);
        int words = Bitmap.wordsFor(N);
        long[] v = new long[words];
        MemorySegment bm = in.validity();
        for (int w = 0; w < words; w++) {
            v[w] = Bitmap.wordAt(bm, w, N);
        }
        return v[0] ^ longs[0];
    }

    /** The whole gather, values and output bitmap, as it runs now. */
    @Benchmark
    public long gather_bulk() {
        mirror.gather(idx, 0, N, outData, outValidity, scratch);
        return outValidity.get(VectorBuffers.LE_LONG, 0L);
    }

    /**
     * The output-bitmap store alone, as {@code gather} now does it: one bulk
     * copy.
     */
    @Benchmark
    public long store_bulk() {
        MemorySegment.copy(mirror.validity, 0, outValidity, VectorBuffers.LE_LONG, 0L,
                Bitmap.wordsFor(N));
        return outValidity.get(VectorBuffers.LE_LONG, 0L);
    }

    /**
     * The output-bitmap store as {@code gather} ran it before #555: a {@code
     * setWord} per word.
     */
    @Benchmark
    public long store_perWord() {
        int words = Bitmap.wordsFor(N);
        long[] bits = mirror.validity;
        for (int w = 0; w < words; w++) {
            Bitmap.setWord(outValidity, w, N, bits[w]);
        }
        return outValidity.get(VectorBuffers.LE_LONG, 0L);
    }
}
