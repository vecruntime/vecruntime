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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.SparkBloomKernels;
import org.apache.spark.util.sketch.BloomFilter;
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
 * Spark's runtime bloom filter probe (#692): Spark's own per-key
 * {@code mightContainLong} against {@code SparkBloomKernels.probe}, which tests
 * each hash function over the batch's keys still alive. The filter is what
 * Spark's runtime filter builds by default: {@code numItems} keys in
 * {@code BloomFilter.create(numItems, bits)} capped at 8 MB (64M bits), so a
 * 4M-key filter is beyond L2 and most reads miss. Probes are batches of
 * {@code 4096} hashes of which {@code hitPercent} are keys.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class SparkBloomProbeBenchmark {

    static final int BATCH = 4096;

    @Param({"100000", "4000000"})
    int numItems;

    @Param({"3", "50"})
    int hitPercent;

    BloomFilter spark;
    SparkBloomKernels.Filter ours;
    long[][] batches;
    int next;
    long[] out = new long[BATCH / 64];
    long[] combined = new long[BATCH];
    int[] alive = new int[BATCH];
    int[] h2s = new int[BATCH];

    @Setup(Level.Trial)
    public void setup() throws IOException {
        SplittableRandom rnd = new SplittableRandom(692);
        long bits = Math.min(BloomFilter.optimalNumOfBits(numItems, 0.03), 64L << 20);
        spark = BloomFilter.create(numItems, bits);
        long[] keys = new long[numItems];
        for (int i = 0; i < numItems; i++) {
            keys[i] = rnd.nextLong();
            spark.putLong(keys[i]);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        spark.writeTo(bytes);
        ours = SparkBloomKernels.parse(bytes.toByteArray());
        batches = new long[256][BATCH];
        for (long[] b : batches) {
            for (int i = 0; i < BATCH; i++) {
                b[i] = rnd.nextInt(100) < hitPercent ? keys[rnd.nextInt(numItems)] : rnd.nextLong();
            }
        }
        long[] ref = new long[BATCH / 64];
        for (long[] b : batches) {
            Arrays.fill(ref, 0L);
            for (int i = 0; i < BATCH; i++) {
                if (spark.mightContainLong(b[i])) {
                    ref[i >>> 6] |= 1L << i;
                }
            }
            SparkBloomKernels.probe(ours, b, BATCH, out, combined, alive,
                    h2s);
            if (!Arrays.equals(ref, out)) {
                throw new IllegalStateException("batch probe disagrees with Spark's");
            }
        }
    }

    private long[] batch() {
        long[] b = batches[next];
        next = (next + 1) & (batches.length - 1);
        return b;
    }

    /** Spark's per-key probe, as BloomProbeExpr ran it before #692. */
    @Benchmark
    public long[] sparkPerKey() {
        long[] b = batch();
        Arrays.fill(out, 0L);
        for (int i = 0; i < BATCH; i++) {
            if (spark.mightContainLong(b[i])) {
                out[i >>> 6] |= 1L << i;
            }
        }
        return out;
    }

    @Benchmark
    public long[] batchProbe() {
        SparkBloomKernels.probe(ours, batch(), BATCH, out, combined,
                alive, h2s);
        return out;
    }
}
