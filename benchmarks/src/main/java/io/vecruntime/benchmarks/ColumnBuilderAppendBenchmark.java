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

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.ColumnBuilder;
import io.vecruntime.kernels.SegmentVectorBuffers;
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
 * {@link ColumnBuilder#append} of 16 batches of 4096 rows behind a 3-row head
 * (so every append starts at a bit offset that is not byte aligned, as the join
 * and sort builders' do): the validity bitmap, and a BOOL column's values,
 * written per row before #541 and per 64-bit word after.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class ColumnBuilderAppendBenchmark {

    static final int ROWS = 4096;
    static final int BATCHES = 16;

    @Param({"INT64", "BOOL"})
    String type;

    @Param({"false", "true"})
    boolean nulls;

    @Param({"false", "true"})
    boolean selected;

    Arena arena;
    VecType vecType;
    VectorBuffers head;
    VectorBuffers batch;
    MemorySegment selection;
    int selectedCount;

    private static MemorySegment random(Arena a, long bytes, Random r) {
        MemorySegment s = a.allocate(Math.max(8, bytes), 8);
        for (long i = 0; i < s.byteSize(); i++) {
            s.set(ValueLayout.JAVA_BYTE, i, (byte) r.nextInt(256));
        }
        return s;
    }

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random r = new Random(541);
        vecType = VecType.valueOf(type);
        long dataBytes = vecType == VecType.BOOL ? Bitmap.bytesFor(ROWS) : (long) ROWS * vecType.byteWidth();
        MemorySegment validity = nulls ? random(arena, Bitmap.bytesFor(ROWS), r) : null;
        batch = SegmentVectorBuffers.fixedWidth(vecType, ROWS, validity, random(arena, dataBytes, r));
        head = SegmentVectorBuffers.fixedWidth(vecType, 3, null, random(arena, 3L * 16, r));
        selection = selected ? random(arena, Bitmap.bytesFor(ROWS), r) : null;
        selectedCount = selected ? Bitmap.popcount(selection, ROWS) : ROWS;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public int append() {
        try (ColumnBuilder b = ColumnBuilder.owning(vecType, 3 + ROWS * BATCHES)) {
            b.append(head);
            for (int i = 0; i < BATCHES; i++) {
                b.append(batch, selection, selectedCount);
            }
            return b.length();
        }
    }
}
