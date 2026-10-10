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
import java.lang.foreign.ValueLayout;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.GatherKernels;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.Utf8Mirror;
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
 * A hash join's build-side string gather (#565): one output batch of
 * {@code OUT} rows gathered at random build rows, through the column's
 * segments ({@code GatherKernels.gatherUtf8Bytes} + {@code gatherUtf8}, what the
 * join did) against a {@code Utf8Mirror} made once per build side. {@code
 * segments=mixed} alternates a native and a heap-backed build column per call,
 * so the segment accessors' receiver profile is polluted the way an executor's
 * is. The mirror's one-off copy is in setup: a build side is gathered for every
 * output batch of every probe batch.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class JoinUtf8GatherBenchmark {

    /** Average string length in the build column. */
    @Param({"8", "40"})
    int avgLen;

    @Param({"0.0", "0.1"})
    double nulls;

    @Param({"native", "mixed"})
    String segments;

    static final int BUILD_ROWS = 100_000;
    static final int OUT = 8192;

    Arena arena;
    VectorBuffers nativeCol;
    VectorBuffers heapCol;
    Utf8Mirror nativeMirror;
    Utf8Mirror heapMirror;
    Utf8Mirror.Scratch scratch;
    int[] idx;
    MemorySegment outOffsets;
    MemorySegment outData;
    MemorySegment outValidity;
    boolean flip;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(565);
        String[] values = new String[BUILD_ROWS];
        for (int i = 0; i < BUILD_ROWS; i++) {
            if (rnd.nextDouble() < nulls) {
                continue;
            }
            int len = Math.max(0, avgLen + rnd.nextInt(-avgLen / 2, avgLen / 2 + 1));
            StringBuilder sb = new StringBuilder(len);
            for (int k = 0; k < len; k++) {
                sb.append((char) ('a' + rnd.nextInt(26)));
            }
            values[i] = sb.toString();
        }
        nativeCol = ArrowLayout.ofStrings(arena, values);
        heapCol = SegmentVectorBuffers.utf8(nativeCol.length(), heap(nativeCol.validity()), heap(nativeCol.offsets()),
                heap(nativeCol.data()));
        nativeMirror = Utf8Mirror.of(nativeCol);
        heapMirror = Utf8Mirror.of(heapCol);
        scratch = new Utf8Mirror.Scratch();
        idx = new int[OUT];
        for (int o = 0; o < OUT; o++) {
            idx[o] = rnd.nextInt(BUILD_ROWS);
        }
        outOffsets = ArrowLayout.allocateOffsets(arena, OUT);
        outData = ArrowLayout.allocateBytes(arena, (long) OUT * (avgLen * 2L + 2));
        outValidity = ArrowLayout.allocateBitmap(arena, OUT);
    }

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    private boolean useHeap() {
        if (segments.equals("native")) {
            return false;
        }
        flip = !flip;
        return flip;
    }

    @Benchmark
    public long segmentGather() {
        VectorBuffers in = useHeap() ? heapCol : nativeCol;
        long bytes = GatherKernels.gatherUtf8Bytes(in, idx, 0, OUT);
        GatherKernels.gatherUtf8(in, idx, 0, OUT, outOffsets, outData,
                in.hasNulls() ? outValidity : null);
        return bytes;
    }

    @Benchmark
    public long mirrorGather() {
        Utf8Mirror m = useHeap() ? heapMirror : nativeMirror;
        long bytes = m.bytes(idx, 0, OUT);
        m.gather(idx, 0, OUT, outOffsets, outData,
                m.hasNulls() ? outValidity : null, scratch);
        return bytes;
    }
}
