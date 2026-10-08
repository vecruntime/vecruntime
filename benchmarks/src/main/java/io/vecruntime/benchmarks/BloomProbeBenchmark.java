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

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.BloomKernels;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * FactBloomFilter's split-block probe (#664): the scalar reference against the
 * branch-free scalar batch and the vector batch (512-bit remix, a 256-bit test
 * per key). Sub-filters hold {@code keysPerPart} keys at 8 bits a key, the
 * layout FactBloomFilter builds (2M keys = a 2 MB sub-filter, beyond L2);
 * {@code parts} of them make the partitioned filter. Probes are a batch of
 * {@code 4096} hashes of which {@code hitPercent} are keys of the filter, the
 * rest random.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class BloomProbeBenchmark {

    static final int BATCH = 4096;

    @Param({"262144", "2097152"})
    int keysPerPart;

    @Param({"1", "8"})
    int parts;

    @Param({"3", "50"})
    int hitPercent;

    int[][] filters;
    long[] hashes;
    long[] out = new long[BATCH / 64];
    long[] xs = new long[BATCH];

    /**
     * Many batches of probes, cycled, so the filter's cache state is not one
     * batch's.
     */
    long[][] batches;

    int next;

    @Setup(Level.Trial)
    public void setup() {
        SplittableRandom rnd = new SplittableRandom(42);
        filters = new int[parts][];
        long total = (long) keysPerPart * parts;
        long[] keySet = new long[(int) Math.min(total, 1 << 24)];
        for (int p = 0; p < parts; p++) {
            filters[p] = BloomKernels.create(keysPerPart * 8L);
        }
        for (long k = 0; k < total; k++) {
            long h = rnd.nextLong();
            if (k < keySet.length) {
                keySet[(int) k] = h;
            }
            int p = (int) (parts == 1 ? 0 : Math.floorMod(h, (long) parts));
            BloomKernels.put(filters[p], h);
        }
        batches = new long[256][BATCH];
        for (long[] b : batches) {
            for (int i = 0; i < BATCH; i++) {
                b[i] = rnd.nextInt(100) < hitPercent ? keySet[rnd.nextInt(keySet.length)] : rnd.nextLong();
            }
        }
        // Every variant agrees with the reference, bit for bit.
        long[] ref = new long[BATCH / 64];
        long[] got = new long[BATCH / 64];
        for (long[] b : batches) {
            BloomKernels.probeReference(filters, b, BATCH, ref);
            BloomKernels.probeScalar(filters, b, BATCH, got);
            check(ref, got, "scalar");
            BloomKernels.probeVector(filters, b, BATCH, got, xs);
            check(ref, got, "vector");
            BloomKernels.probeVector128(filters, b, BATCH, got);
            check(ref, got, "vector128");
            BloomKernels.probe(filters, b, BATCH, got, xs);
            check(ref, got, "probe");
        }
    }

    private static void check(long[] ref, long[] got, String what) {
        if (!Arrays.equals(ref, got)) {
            throw new IllegalStateException(what + " disagrees with the reference");
        }
    }

    private long[] batch() {
        long[] b = batches[next];
        next = (next + 1) & (batches.length - 1);
        return b;
    }

    @Benchmark
    public long[] reference() {
        BloomKernels.probeReference(filters, batch(), BATCH, out);
        return out;
    }

    @Benchmark
    public long[] scalarBranchFree() {
        BloomKernels.probeScalar(filters, batch(), BATCH, out);
        return out;
    }

    @Benchmark
    public long[] vector() {
        BloomKernels.probeVector(filters, batch(), BATCH, out, xs);
        return out;
    }

    @Benchmark
    public long[] vector128() {
        BloomKernels.probeVector128(filters, batch(), BATCH, out);
        return out;
    }

    /** The dispatched probe: what the engine runs on this host. */
    @Benchmark
    public long[] probe() {
        BloomKernels.probe(filters, batch(), BATCH, out, xs);
        return out;
    }
}
