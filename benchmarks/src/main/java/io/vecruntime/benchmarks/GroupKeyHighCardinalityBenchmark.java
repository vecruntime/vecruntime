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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.GroupKeyTable;
import io.vecruntime.kernels.HashKernels;
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
 * Group-id assignment with many groups per task (#677): the partial aggregates
 * of TPC-DS q23a at 1 TB, where {@code GroupKeyTable.assign} was 36-74 % of
 * the stage CPU. One task there sees ~2.2 M rows; {@code keys=1} is
 * {@code max_store_sales} by {@code ss_customer_sk} (~390 K groups per task),
 * {@code keys=2} is {@code frequent_ss_items} by {@code (i_item_sk, d_date)}
 * (~1.1 M groups per task, rows reduce 2x). Integer keys drawn uniformly from
 * {@code distinct} values over {@link #ROWS} rows.
 *
 * <ul>
 *   <li>{@code hashOnly}: the hashing pass alone, the floor of any table;</li>
 *   <li>{@code assignFresh}: a new table per invocation, inserts included --
 *       what a partial aggregate does;</li>
 *   <li>{@code assignWarm}: the same rows into a table that already holds
 *       every group -- probes only, so the gap to {@code assignFresh} is the
 *       insert and growth cost, and the gap to {@code hashOnly} the probe.</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "-Xmx4g"})
@State(Scope.Thread)
public class GroupKeyHighCardinalityBenchmark {

    static final int N = 4096;

    /** Rows per invocation: one task's share of a 1 TB q23a stage. */
    static final int BATCHES = 512;

    static final int ROWS = N * BATCHES;

    @Param({"1", "2"})
    int keys;

    @Param({"4096", "400000", "1100000"})
    int distinct;

    Arena arena;
    VectorBuffers[][] batches;
    VecType[] types;
    int[] ids;
    int[] hashes;
    GroupKeyTable warm;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(7);
        types = keys == 1 ? new VecType[] {VecType.INT32} : new VecType[] {VecType.INT32, VecType.INT32};
        // Two keys: an item (300 K) and a date (~1,460 days) whose combinations number `distinct`.
        int[] item = new int[distinct];
        int[] date = new int[distinct];
        for (int g = 0; g < distinct; g++) {
            item[g] = 1 + rnd.nextInt(300_000);
            date[g] = 2_451_545 + rnd.nextInt(1_461);
        }
        batches = new VectorBuffers[BATCHES][];
        for (int b = 0; b < BATCHES; b++) {
            int[] c0 = new int[N];
            int[] c1 = new int[N];
            for (int i = 0; i < N; i++) {
                int g = rnd.nextInt(distinct);
                if (keys == 1) {
                    c0[i] = 1 + g * 31 % 12_000_000;
                } else {
                    c0[i] = item[g];
                    c1[i] = date[g];
                }
            }
            batches[b] = keys == 1 ? new VectorBuffers[] {ArrowLayout.ofInts(arena, c0, null)} : new VectorBuffers[] {ArrowLayout.ofInts(arena, c0, null), ArrowLayout.ofInts(arena, c1, null)};
        }
        ids = new int[N];
        hashes = new int[N];
        warm = new GroupKeyTable(types);
        for (VectorBuffers[] batch : batches) {
            warm.assign(batch, N, ids);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public int hashOnly() {
        int acc = 0;
        for (VectorBuffers[] batch : batches) {
            HashKernels.init(hashes, N);
            for (VectorBuffers key : batch) {
                HashKernels.mixColumn(key, hashes);
            }
            acc += hashes[N - 1];
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public int assignFresh() {
        GroupKeyTable table = new GroupKeyTable(types);
        int groups = 0;
        for (VectorBuffers[] batch : batches) {
            groups = table.assign(batch, N, ids);
        }
        return groups;
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public int assignWarm() {
        int groups = 0;
        for (VectorBuffers[] batch : batches) {
            groups = warm.assign(batch, N, ids);
        }
        return groups;
    }
}
