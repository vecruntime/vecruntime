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

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.parquet.RleBitPackingReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
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
 * Sanity JMH for the Parquet page decoder's hot inner loop: the RLE/bit-packed
 * hybrid reader ({@link RleBitPackingReader}), used for both definition levels
 * (bit width 1, mostly present) and dictionary ids (a wider bit width, mixed).
 * Not a verdict -- per AGENTS.md the decision is a 1 TB A/B on real executors
 * -- only a check that the loop is not accidentally quadratic and a baseline
 * for a later Vector-API bit-unpacking version. Convention: {@code -wi 2 -i 3
 * -w 1 -r 1 -f 1}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class ParquetDecodeBenchmark {

    /** Values per page. */
    @Param({"8192"})
    int n;

    /**
     * Bit width: 1 models definition levels, 10 models dictionary ids of a
     * ~1000-entry dictionary.
     */
    @Param({"1", "10"})
    int bitWidth;

    private Arena arena;
    private MemorySegment page;
    private long length;
    private byte[] packedBytes; // bit-packed group bytes without the ULEB128 header (BytePacker path)
    private byte[] pageBytesFull; // the whole hybrid stream as a byte[] (injected-reader path)
    private int paddedN;
    private int[] idOut;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(559);
        int[] values = new int[n];
        long max = bitWidth == 32 ? 0xFFFFFFFFL : ((1L << bitWidth) - 1);
        for (int i = 0; i < n; i++) {
            // Level-like: mostly the max (present). Id-like: uniform over the width.
            values[i] = bitWidth == 1
                    ? (rnd.nextInt(10) == 0 ? 0 : 1)
                    : (int) (Math.floorMod(rnd.nextLong(), max + 1));
        }
        byte[] bytes = encode(values, bitWidth);
        length = bytes.length;
        pageBytesFull = bytes;
        page = arena.allocate(bytes.length + 8L);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, page, 0, bytes.length);
        paddedN = ((n + 7) / 8) * 8;
        idOut = new int[paddedN];
        int header = (((n + 7) / 8) << 1) | 1;
        int headerLen = header < 0x80
                ? 1
                : (header < 0x4000 ? 2 : 3);
        packedBytes = new byte[bytes.length - headerLen];
        System.arraycopy(bytes, headerLen, packedBytes, 0, packedBytes.length);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    /** Ours, value-at-a-time. */
    @Benchmark
    public long readIntScalar() {
        RleBitPackingReader r = new RleBitPackingReader(page, 0, length, bitWidth);
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += r.readInt();
        }
        return sum;
    }

    /** Ours, batch. */
    @Benchmark
    public long readIntsBatch() {
        RleBitPackingReader r = new RleBitPackingReader(page, 0, length, bitWidth);
        r.readInts(idOut, 0, n);
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += idOut[i];
        }
        return sum;
    }

    /**
     * Ours, batch, with parquet-java's BytePacker injected for bit-packed
     * groups (the node's path).
     */
    @Benchmark
    public long readIntsBatchInjected() {
        if (bitWidth == 0) {
            return 0;
        }
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        RleBitPackingReader r = RleBitPackingReader.overArray(pageBytesFull, 0, pageBytesFull.length, bitWidth, packer::unpack8Values);
        r.readInts(idOut, 0, n);
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += idOut[i];
        }
        return sum;
    }

    /**
     * parquet-java's generated unrolled BytePacker, group by group (Spark's
     * reference path).
     */
    @Benchmark
    public long parquetBytePacker() {
        if (bitWidth == 0) {
            return 0;
        }
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        int bytesPerGroup = bitWidth;
        int pos = 0;
        for (int g = 0; g < paddedN; g += 8) {
            packer.unpack8Values(packedBytes, pos, idOut, g);
            pos += bytesPerGroup;
        }
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += idOut[i];
        }
        return sum;
    }

    // A minimal bit-packed encoder (whole groups of 8, zero-padded last group), enough to feed the reader.
    private static byte[] encode(int[] values, int bitWidth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (bitWidth == 0) {
            return out.toByteArray();
        }
        int groups = (values.length + 7) / 8;
        writeUleb128(out, (groups << 1) | 1);
        long buffer = 0;
        int bits = 0;
        long mask = bitWidth == 32 ? 0xFFFFFFFFL : ((1L << bitWidth) - 1);
        int padded = groups * 8;
        for (int j = 0; j < padded; j++) {
            int v = j < values.length ? values[j] : 0;
            buffer |= (v & mask) << bits;
            bits += bitWidth;
            while (bits >= 8) {
                out.write((int) (buffer & 0xFF));
                buffer >>>= 8;
                bits -= 8;
            }
        }
        if (bits > 0) {
            out.write((int) (buffer & 0xFF));
        }
        return out.toByteArray();
    }

    private static void writeUleb128(ByteArrayOutputStream out, int value) {
        int v = value;
        while (true) {
            int b = v & 0x7F;
            v >>>= 7;
            if (v != 0) {
                out.write(b | 0x80);
            } else {
                out.write(b);
                return;
            }
        }
    }
}
