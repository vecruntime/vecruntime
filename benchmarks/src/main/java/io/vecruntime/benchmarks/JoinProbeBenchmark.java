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
import io.vecruntime.kernels.DenseKeyIndex;
import io.vecruntime.kernels.GroupKeyTable;
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
 * A hash join's probe over one integer key against a small dimension (#546):
 * TPC-DS q88 probes 2.65 B `store_sales` rows per branch against
 * `household_demographics`, whose filtered build side holds about 27 % of the
 * keys 1..7,200. `dense` is that shape (build keys a subset of a small range);
 * `sparse` spreads the same number of build keys over 2^30 values, a table no
 * dense index could cover. The probe keys are drawn from the build keys' range,
 * so about `hit` of them match. The reported rate is probed rows per
 * millisecond; the sum of the ids is returned so the lookup cannot be
 * eliminated.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class JoinProbeBenchmark {

    static final int N = 4096;
    static final int BATCHES = 64;

    /** The key range of the dimension (q88's household_demographics: 1..7,200). */
    static final int RANGE = 7200;

    @Param({"INT32", "INT64"})
    String type;

    @Param({"dense", "sparse"})
    String layout;

    /**
     * The fraction of the range's keys the build side holds, hence of probe
     * rows that match.
     */
    @Param({"0.27"})
    double hit;

    Arena arena;
    GroupKeyTable table;
    DenseKeyIndex dense;
    DenseKeyIndex.Scratch scratch;
    VectorBuffers[][] probes;
    int[] ids;
    int[] hashes;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(7);
        boolean wide = type.equals("INT64");
        boolean sparse = layout.equals("sparse");
        // The range's values, spread over 2^30 in the sparse layout; the build side keeps a `hit` share.
        long[] domain = new long[RANGE];
        for (int k = 0; k < RANGE; k++) {
            domain[k] = sparse ? (long) k * 149_101 + 1 : k + 1;
        }
        int builds = 0;
        long[] buildKeys = new long[RANGE];
        for (int k = 0; k < RANGE; k++) {
            if (rnd.nextDouble() < hit) {
                buildKeys[builds++] = domain[k];
            }
        }
        VecType vt = wide ? VecType.INT64 : VecType.INT32;
        table = new GroupKeyTable(new VecType[] {vt}, true);
        int[] buildIds = new int[builds];
        table.assign(new VectorBuffers[] {column(buildKeys, builds, wide)}, builds, buildIds);
        dense = DenseKeyIndex.tryBuild(column(buildKeys, builds, wide), builds, buildIds, builds);
        if (sparse != (dense == null)) {
            throw new IllegalStateException("layout " + layout + " but dense index " + dense);
        }
        scratch = new DenseKeyIndex.Scratch();
        probes = new VectorBuffers[BATCHES][];
        for (int b = 0; b < BATCHES; b++) {
            long[] keys = new long[N];
            for (int i = 0; i < N; i++) {
                keys[i] = domain[rnd.nextInt(RANGE)];
            }
            probes[b] = new VectorBuffers[] {column(keys, N, wide)};
        }
        ids = new int[N];
        hashes = new int[N];
    }

    private VectorBuffers column(long[] values, int n, boolean wide) {
        if (wide) {
            return ArrowLayout.ofLongs(arena, java.util.Arrays.copyOf(values, n), null);
        }
        int[] ints = new int[n];
        for (int i = 0; i < n; i++) {
            ints[i] = (int) values[i];
        }
        return ArrowLayout.ofInts(arena, ints, null);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    /**
     * The current probe: hash, slot, compare, per row ({@code
     * GroupKeyTable.lookup}).
     */
    @Benchmark
    @OperationsPerInvocation(N * BATCHES)
    public long hashLookup() {
        long sum = 0;
        for (VectorBuffers[] batch : probes) {
            table.lookup(batch, N, ids, null, hashes);
            for (int i = 0; i < N; i++) {
                sum += ids[i];
            }
        }
        return sum;
    }

    /**
     * The dense probe: a range check and one array load per row ({@code
     * DenseKeyIndex.lookup}). On the sparse layout no index is built and this
     * falls back to the hash probe, as the join does.
     */
    @Benchmark
    @OperationsPerInvocation(N * BATCHES)
    public long denseLookup() {
        long sum = 0;
        for (VectorBuffers[] batch : probes) {
            if (dense != null) {
                dense.lookup(batch[0], N, ids, null, scratch);
            } else {
                table.lookup(batch, N, ids, null, hashes);
            }
            for (int i = 0; i < N; i++) {
                sum += ids[i];
            }
        }
        return sum;
    }
}
