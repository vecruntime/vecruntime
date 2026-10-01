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

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.GatherKernels;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
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
 * The output gathers of a sort or a join (#565) -- {@code gatherUtf8Bytes},
 * {@code gatherUtf8} with its validity, and a BOOL {@code gatherFixed} -- over
 * native columns, after the kernels have also seen heap-backed columns. On an
 * executor the same kernels gather from native batches and from heap segments
 * (group-table records, dictionaries), so their accessors' receiver profile is
 * mixed; {@code profile=mixed} reproduces that by gathering from a heap copy of
 * each column in every invocation as well. {@code native} is the single-type
 * shape. Compare a score across builds at the same {@code profile}: under
 * {@code mixed} an invocation also does the heap gathers.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(GatherSegmentProfileBenchmark.N)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class GatherSegmentProfileBenchmark {
    static final int N = 4096;
    static final int SOURCE_ROWS = 65536;

    @Param({"native", "mixed"})
    String profile;

    Arena arena;
    VectorBuffers strings;
    VectorBuffers heapStrings;
    VectorBuffers bools;
    VectorBuffers heapBools;
    int[] idx;
    MemorySegment outOffsets;
    MemorySegment outData;
    MemorySegment outValidity;
    MemorySegment outBits;
    MemorySegment heapOffsets;
    MemorySegment heapData;
    MemorySegment heapValidity;
    MemorySegment heapBits;

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(565);
        String[] values = new String[SOURCE_ROWS];
        boolean[] flags = new boolean[SOURCE_ROWS];
        boolean[] nulls = new boolean[SOURCE_ROWS];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < SOURCE_ROWS; i++) {
            sb.setLength(0);
            int len = rnd.nextInt(4, 30);
            for (int k = 0; k < len; k++) {
                sb.append((char) ('a' + rnd.nextInt(26)));
            }
            values[i] = rnd.nextInt(10) == 0 ? null : sb.toString();
            flags[i] = rnd.nextBoolean();
            nulls[i] = rnd.nextInt(10) == 0;
        }
        strings = ArrowLayout.ofStrings(arena, values);
        heapStrings = SegmentVectorBuffers.utf8(SOURCE_ROWS, heap(strings.validity()), heap(strings.offsets()),
                heap(strings.data()));
        bools = ArrowLayout.ofBooleans(arena, flags, nulls);
        heapBools = SegmentVectorBuffers.fixedWidth(VecType.BOOL, SOURCE_ROWS, heap(bools.validity()), heap(bools.data()));
        idx = new int[N];
        for (int i = 0; i < N; i++) {
            idx[i] = rnd.nextInt(SOURCE_ROWS);
        }
        outOffsets = ArrowLayout.allocateOffsets(arena, N);
        outData = ArrowLayout.allocateBytes(arena, (long) N * 32);
        outValidity = ArrowLayout.allocateBitmap(arena, N);
        outBits = ArrowLayout.allocateBitmap(arena, N);
        heapOffsets = heap(outOffsets);
        heapData = heap(outData);
        heapValidity = heap(outValidity);
        heapBits = heap(outBits);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public long gathers() {
        long bytes = GatherKernels.gatherUtf8Bytes(strings, idx, 0, N);
        GatherKernels.gatherUtf8(strings, idx, 0, N, outOffsets, outData,
                outValidity);
        GatherKernels.gatherFixed(bools, idx, 0, N, outBits, outValidity);
        if (profile.equals("mixed")) {
            bytes += GatherKernels.gatherUtf8Bytes(heapStrings, idx, 0, N);
            GatherKernels.gatherUtf8(heapStrings, idx, 0, N, heapOffsets, heapData,
                    heapValidity);
            GatherKernels.gatherFixed(heapBools, idx, 0, N, heapBits, heapValidity);
        }
        return bytes + Bitmap.wordAt(outBits, 0, N);
    }
}
