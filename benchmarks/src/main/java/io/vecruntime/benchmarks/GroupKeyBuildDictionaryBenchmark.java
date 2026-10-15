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
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.GroupKeyTable;
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
 * Group-id assignment over dictionary-encoded UTF8 keys whose dictionary is a
 * join's build side (#692): every key column of the batch indexes the same
 * {@code entries} build rows (a join output gathers its build-side strings by
 * build row), each row a random build row, and each column's value a function
 * of the build row with {@code distinct} values per column, so the groups are
 * a bounded set however large the build side is. q24a's partial aggregate has
 * eight such string keys over a multi-million-row build side.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class GroupKeyBuildDictionaryBenchmark {

    static final int N = 4096;
    static final int BATCHES = 64;

    @Param({"65536", "4194304"})
    int entries;

    @Param({"4"})
    int columns;

    @Param({"30"})
    int distinct;

    Arena arena;
    VectorBuffers[][] batches;
    int[] ids;
    GroupKeyTable table;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        SplittableRandom rnd = new SplittableRandom(692);
        VectorBuffers[] dicts = new VectorBuffers[columns];
        for (int c = 0; c < columns; c++) {
            String[] values = new String[entries];
            long stride = 1;
            for (int s = 0; s < c; s++) {
                stride *= distinct;
            }
            for (int e = 0; e < entries; e++) {
                values[e] = "value-of-column-" + c + "-" + ((e / stride) % distinct);
            }
            dicts[c] = ArrowLayout.ofStrings(arena, values);
        }
        batches = new VectorBuffers[BATCHES][columns];
        for (int b = 0; b < BATCHES; b++) {
            MemorySegment idx = arena.allocate(ValueLayout.JAVA_INT, N);
            for (int i = 0; i < N; i++) {
                idx.setAtIndex(ValueLayout.JAVA_INT, i, rnd.nextInt(entries));
            }
            for (int c = 0; c < columns; c++) {
                batches[b][c] = SegmentVectorBuffers.dictionaryUtf8(N, null, idx, dicts[c]);
            }
        }
        ids = new int[N];
        VecType[] types = new VecType[columns];
        java.util.Arrays.fill(types, VecType.UTF8);
        table = new GroupKeyTable(types);
        // Warm the table and the entry maps over every batch once: the measured loop is the steady state of a
        // long-running partial aggregate, its groups and build entries already seen.
        for (VectorBuffers[] batch : batches) {
            table.assign(batch, N, ids);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        GroupKeyTable.releaseThreadScratch();
        arena.close();
    }

    /**
     * All batches into the warmed table; the reported rate is rows per
     * millisecond.
     */
    @Benchmark
    @OperationsPerInvocation(N * BATCHES)
    public int assignBuildDictionary() {
        int groups = 0;
        for (VectorBuffers[] batch : batches) {
            groups = table.assign(batch, N, ids);
        }
        return groups;
    }
}
