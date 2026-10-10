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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import io.vecruntime.kernels.parquet.DeltaBinaryPackedReader;
import io.vecruntime.kernels.parquet.GroupUnpacker;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedDeltaBinaryPackedReader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
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
 * Sanity JMH for {@code DELTA_BINARY_PACKED} decoding (#559 slice 2): our
 * {@link DeltaBinaryPackedReader} with parquet-java's {@code BytePacker}
 * injected (the scan node's path) against Spark's own {@link
 * VectorizedDeltaBinaryPackedReader} filling an on-heap column, over a page
 * written by parquet-java's writer. Not a verdict -- the decision is a run on
 * real data -- only a check that the reader is in Spark's range and not
 * accidentally slow. Convention: {@code -wi 2 -i 3 -w 1 -r 1 -f 1}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class DeltaBinaryPackedBenchmark {

    /** Values per page. */
    @Param({"20000"})
    int n;

    /**
     * Value shape: {@code seq-int} sorted INT32 keys (narrow deltas), {@code rand-int}
     * full-range INT32 (32-bit deltas), {@code ts-long} INT64 timestamps with jitter,
     * {@code rand-long} 44-bit INT64 (widths above 32, the scalar long unpack).
     */
    @Param({"seq-int", "rand-int", "ts-long", "rand-long"})
    String shape;

    private byte[] page;
    private boolean int64;
    private int[] ints;
    private long[] longs;
    private IntFunction<GroupUnpacker> unpackers;
    private WritableColumnVector sparkCol;

    @Setup(Level.Trial)
    @SuppressWarnings("deprecation") // BytePacker.unpack8Values(byte[]...) is the seam's byte[] path
    public void setup() {
        Random rnd = new Random(5599);
        int64 = shape.endsWith("long");
        if (int64) {
            DeltaBinaryPackingValuesWriterForLong w = new DeltaBinaryPackingValuesWriterForLong(128, 4, 64, 1 << 20,
                    HeapByteBufferAllocator.getInstance());
            long t = 1_700_000_000_000_000L;
            for (int i = 0; i < n; i++) {
                w.writeLong(shape.equals("ts-long") ? (t += 1000 + rnd.nextInt(50)) : rnd.nextLong() >> 20);
            }
            page = bytes(w.getBytes());
            longs = new long[n];
            sparkCol = new OnHeapColumnVector(n, DataTypes.LongType);
        } else {
            DeltaBinaryPackingValuesWriterForInteger w = new DeltaBinaryPackingValuesWriterForInteger(128, 4, 64, 1 << 20,
                    HeapByteBufferAllocator.getInstance());
            for (int i = 0; i < n; i++) {
                w.writeInteger(shape.equals("seq-int") ? 1_000_000 + i * 3 + rnd.nextInt(3) : rnd.nextInt());
            }
            page = bytes(w.getBytes());
            ints = new int[n];
            sparkCol = new OnHeapColumnVector(n, DataTypes.IntegerType);
        }
        GroupUnpacker[] cache = new GroupUnpacker[33];
        unpackers = width -> {
            if (cache[width] == null) {
                BytePacker p = Packer.LITTLE_ENDIAN.newBytePacker(width);
                cache[width] = p::unpack8Values;
            }
            return cache[width];
        };
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        sparkCol.close();
    }

    /**
     * Ours, BytePacker injected, one read of the whole page into the lane's
     * array.
     */
    @Benchmark
    public long ours() {
        DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(page, 0, page.length, int64, unpackers);
        if (int64) {
            r.readLongs(longs, 0, n);
            return longs[n - 1];
        }
        r.readInts(ints, 0, n);
        return ints[n - 1];
    }

    /** Ours, read in 1024-row batches, as the scan reads it. */
    @Benchmark
    public long oursBatched() {
        DeltaBinaryPackedReader r = new DeltaBinaryPackedReader(page, 0, page.length, int64, unpackers);
        for (int done = 0; done < n; done += 1024) {
            int take = Math.min(1024, n - done);
            if (int64) {
                r.readLongs(longs, done, take);
            } else {
                r.readInts(ints, done, take);
            }
        }
        return int64 ? longs[n - 1] : ints[n - 1];
    }

    /** Spark's own reader, as its vectorized Parquet reader uses it. */
    @Benchmark
    public long spark() throws java.io.IOException {
        VectorizedDeltaBinaryPackedReader r = new VectorizedDeltaBinaryPackedReader();
        r.initFromPage(n, ByteBufferInputStream.wrap(java.nio.ByteBuffer.wrap(page)));
        sparkCol.reset();
        if (int64) {
            r.readLongs(n, sparkCol, 0);
            return sparkCol.getLong(n - 1);
        }
        r.readIntegers(n, sparkCol, 0);
        return sparkCol.getInt(n - 1);
    }

    private static byte[] bytes(org.apache.parquet.bytes.BytesInput in) {
        try {
            return in.toByteArray();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
