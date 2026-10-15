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
 * q24a's partial aggregate (#692): ten keys -- eight strings gathered from a
 * join's build side (dictionary encoded over its {@code entries} rows), an
 * INT32 and an INT64 -- with {@code groups} distinct key tuples, the columns'
 * cardinalities q24's (last and first name and price near {@code groups},
 * store, state, color, units and size a handful each). Steady state: the table
 * already holds every group. Run with
 * {@code -jvmArgsAppend -Dvecruntime.kernels.packedGroupKeys=false} for the
 * general probe.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class GroupKeyPackedBenchmark {

    static final int N = 4096;
    static final int BATCHES = 64;
    static final int STRINGS = 8;

    @Param({"1000", "50000"})
    int groups;

    @Param({"1048576"})
    int entries;

    Arena arena;
    VectorBuffers[][] batches;
    int[] ids;
    GroupKeyTable table;

    private static int value(int c, int g, int groups) {
        return switch (c) {
            case 0 -> g; // c_last_name
            case 1 -> (int) ((g * 7919L) % groups); // c_first_name
            case 2 -> g % 3; // s_store_name
            case 3 -> g % 5; // ca_state
            case 4 -> g % 3; // s_state
            case 5 -> g % 92; // i_color
            case 6 -> g % 21; // i_units
            case 7 -> g % 7; // i_size
            case 8 -> g % 100; // i_manager_id
            default -> (int) ((g * 13L) % groups); // i_current_price
        };
    }

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        SplittableRandom rnd = new SplittableRandom(692);
        int[] groupOfEntry = new int[entries];
        for (int e = 0; e < entries; e++) {
            groupOfEntry[e] = rnd.nextInt(groups);
        }
        VectorBuffers[] dicts = new VectorBuffers[STRINGS];
        for (int c = 0; c < STRINGS; c++) {
            String[] values = new String[entries];
            for (int e = 0; e < entries; e++) {
                values[e] = "column-" + c + "-value-" + value(c, groupOfEntry[e], groups);
            }
            dicts[c] = ArrowLayout.ofStrings(arena, values);
        }
        batches = new VectorBuffers[BATCHES][STRINGS + 2];
        for (int b = 0; b < BATCHES; b++) {
            MemorySegment idx = arena.allocate(ValueLayout.JAVA_INT, N);
            int[] mgr = new int[N];
            long[] price = new long[N];
            for (int i = 0; i < N; i++) {
                int e = rnd.nextInt(entries);
                idx.setAtIndex(ValueLayout.JAVA_INT, i, e);
                mgr[i] = value(8, groupOfEntry[e], groups);
                price[i] = 100L * value(9, groupOfEntry[e], groups) + 99L;
            }
            for (int c = 0; c < STRINGS; c++) {
                batches[b][c] = SegmentVectorBuffers.dictionaryUtf8(N, null, idx, dicts[c]);
            }
            batches[b][STRINGS] = ArrowLayout.ofInts(arena, mgr, null);
            batches[b][STRINGS + 1] = ArrowLayout.ofLongs(arena, price, null);
        }
        ids = new int[N];
        VecType[] types = new VecType[STRINGS + 2];
        java.util.Arrays.fill(types, VecType.UTF8);
        types[STRINGS] = VecType.INT32;
        types[STRINGS + 1] = VecType.INT64;
        table = new GroupKeyTable(types);
        for (int r = 0; r < 4; r++) {
            for (VectorBuffers[] batch : batches) {
                table.assign(batch, N, ids);
            }
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
    public int assignQ24Keys() {
        int groupsSeen = 0;
        for (VectorBuffers[] batch : batches) {
            groupsSeen = table.assign(batch, N, ids);
        }
        return groupsSeen;
    }
}
