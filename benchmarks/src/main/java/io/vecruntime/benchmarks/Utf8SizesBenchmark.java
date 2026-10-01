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
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.Utf8Sizes;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.kernels.reference.ScalarReference;
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
 * The shuffle writer's per-batch string-size estimate (#565): {@code Utf8Sizes}
 * over heap copies against the per-row segment reads it replaced ({@code
 * ScalarReference.paddedUtf8Bytes} is that loop). {@code segments=mixed} reads
 * a native and a heap-backed column per call, so the accessors' receiver
 * profile is polluted the way an executor's is; {@code native} is the friendly
 * single-type shape JMH otherwise measures.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class Utf8SizesBenchmark {

    @Param({"plain", "dictionary"})
    String encoding;

    @Param({"0.0", "0.1"})
    double nulls;

    @Param({"native", "mixed"})
    String segments;

    static final int ROWS = 8192;

    Arena arena;
    VectorBuffers nativeCol;
    VectorBuffers heapCol;
    Utf8Sizes.Scratch scratch;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(565);
        String[] dict = new String[64];
        for (int e = 0; e < dict.length; e++) {
            dict[e] = word(rnd);
        }
        if (encoding.equals("plain")) {
            String[] v = new String[ROWS];
            for (int i = 0; i < ROWS; i++) {
                v[i] = rnd.nextDouble() < nulls ? null : dict[rnd.nextInt(dict.length)];
            }
            nativeCol = ArrowLayout.ofStrings(arena, v);
            heapCol = SegmentVectorBuffers.utf8(ROWS, heap(nativeCol.validity()), heap(nativeCol.offsets()),
                    heap(nativeCol.data()));
        } else {
            int[] ids = new int[ROWS];
            boolean[] isNull = new boolean[ROWS];
            for (int i = 0; i < ROWS; i++) {
                ids[i] = rnd.nextInt(dict.length);
                isNull[i] = rnd.nextDouble() < nulls;
            }
            VectorBuffers idx = ArrowLayout.ofInts(arena, ids, isNull);
            VectorBuffers d = ArrowLayout.ofStrings(arena, dict);
            nativeCol = SegmentVectorBuffers.dictionaryUtf8(ROWS, idx.validity(), idx.data(), d);
            VectorBuffers hd = SegmentVectorBuffers.utf8(dict.length, null, heap(d.offsets()), heap(d.data()));
            heapCol = SegmentVectorBuffers.dictionaryUtf8(ROWS, heap(idx.validity()), heap(idx.data()), hd);
        }
        scratch = new Utf8Sizes.Scratch();
    }

    private static String word(Random rnd) {
        StringBuilder sb = new StringBuilder();
        int len = rnd.nextInt(3, 40);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + rnd.nextInt(26)));
        }
        return new String(sb.toString()
                            .getBytes(StandardCharsets.UTF_8),
                StandardCharsets.UTF_8);
    }

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public long perRowSegmentReads() {
        long t = ScalarReference.paddedUtf8Bytes(nativeCol, ROWS);
        if (segments.equals("mixed")) {
            t += ScalarReference.paddedUtf8Bytes(heapCol, ROWS);
        }
        return t;
    }

    @Benchmark
    public long heapCopies() {
        long t = Utf8Sizes.paddedBytes(nativeCol, ROWS, scratch);
        if (segments.equals("mixed")) {
            t += Utf8Sizes.paddedBytes(heapCol, ROWS, scratch);
        }
        return t;
    }
}
