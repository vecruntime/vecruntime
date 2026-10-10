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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.parquet.ColumnChunkDecoder;
import io.vecruntime.kernels.parquet.GroupUnpacker;
import io.vecruntime.kernels.parquet.ParquetPageDecoder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForDouble;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForInteger;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForLong;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.io.api.Binary;
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
 * Sanity JMH for the {@code BYTE_STREAM_SPLIT}, {@code DELTA_BYTE_ARRAY},
 * {@code DELTA_LENGTH_BYTE_ARRAY} and BOOLEAN ({@code PLAIN} and {@code RLE})
 * pages of the native scan (#559): one page decoded
 * through {@link ColumnChunkDecoder} in 1024-row batches into an Arrow buffer,
 * the scan's path, against parquet-java's own value readers over the same page
 * (Spark's vectorized reader rejects {@code BYTE_STREAM_SPLIT} on INT64 and has
 * no public DLBA reader to drive alone). Not a verdict. Convention: {@code -wi
 * 2 -i 3 -w 1 -r 1 -f 1}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class V2EncodingsBenchmark {

    /** Values per page. */
    @Param({"20000"})
    int n;

    /**
     * {@code bss-double}, {@code bss-long}, {@code bss-int}, {@code
     * dlba-string} (short random strings) or {@code dba-string} (sorted
     * URL-like keys).
     */
    @Param({"bss-double", "bss-long", "bss-int", "dlba-string", "dba-string", "bool-plain",
                "bool-rle", "flba-dec38"})
    String shape;

    private byte[] page;
    private VecType lane;
    private ParquetPageDecoder.Encoding encoding;
    private Arena arena;
    private MemorySegment out;
    private MemorySegment outOffsets;
    private MemorySegment outBytes;
    private ColumnChunkDecoder decoder;

    @Setup(Level.Trial)
    @SuppressWarnings("deprecation") // BytePacker.unpack8Values(byte[]...) is the seam's byte[] path
    public void setup() throws java.io.IOException {
        Random rnd = new Random(55914);
        HeapByteBufferAllocator alloc = HeapByteBufferAllocator.getInstance();
        switch (shape) {
            case "bss-double" -> {
                ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter w = new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(64, 1 << 20, alloc);
                for (int i = 0; i < n; i++) {
                    w.writeDouble(rnd.nextGaussian() * 1e6);
                }
                page = w.getBytes().toByteArray();
                lane = VecType.FLOAT64;
                encoding = ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT;
            }
            case "bss-long" -> {
                ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter w = new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(64, 1 << 20, alloc);
                for (int i = 0; i < n; i++) {
                    w.writeLong(rnd.nextLong());
                }
                page = w.getBytes().toByteArray();
                lane = VecType.INT64;
                encoding = ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT;
            }
            case "bss-int" -> {
                ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter w = new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(64, 1 << 20, alloc);
                for (int i = 0; i < n; i++) {
                    w.writeInteger(rnd.nextInt());
                }
                page = w.getBytes().toByteArray();
                lane = VecType.INT32;
                encoding = ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT;
            }
            case "bool-plain" -> {
                // Independent coin flips: the bit-packed PLAIN layout parquet-mr v1 writes.
                BooleanPlainValuesWriter w = new BooleanPlainValuesWriter();
                for (int i = 0; i < n; i++) {
                    w.writeBoolean(rnd.nextBoolean());
                }
                page = w.getBytes().toByteArray();
                lane = VecType.BOOL;
                encoding = ParquetPageDecoder.Encoding.PLAIN;
            }
            case "bool-rle" -> {
                // Runs of 1..64 equal values: the 4-byte-length-prefixed RLE / bit-packed hybrid v2 writes,
                // with both run kinds in the stream.
                RunLengthBitPackingHybridValuesWriter w = new RunLengthBitPackingHybridValuesWriter(1, 64, 1 << 20, alloc);
                boolean v = false;
                for (int i = 0; i < n; ) {
                    int run = 1 + rnd.nextInt(64);
                    for (int j = 0;
                         j < run && i < n;
                         j++, i++) {
                        w.writeBoolean(v);
                    }
                    v = !v;
                }
                page = w.getBytes().toByteArray();
                lane = VecType.BOOL;
                encoding = ParquetPageDecoder.Encoding.RLE;
            }
            case "flba-dec38" -> {
                // decimal(38, s) as Spark writes it: 16-byte big-endian two's complement, PLAIN.
                FixedLenByteArrayPlainValuesWriter w = new FixedLenByteArrayPlainValuesWriter(16, 64, 1 << 20, alloc);
                for (int i = 0; i < n; i++) {
                    java.math.BigInteger x = new java.math.BigInteger(120, rnd);
                    if (i % 2 == 0) {
                        x = x.negate();
                    }
                    byte[] b = x.toByteArray();
                    byte[] v = new byte[16];
                    java.util.Arrays.fill(v, x.signum() < 0 ? (byte) -1 : 0);
                    System.arraycopy(b, 0, v, 16 - b.length, b.length);
                    w.writeBytes(Binary.fromConstantByteArray(v));
                }
                page = w.getBytes().toByteArray();
                lane = VecType.DECIMAL128;
                encoding = ParquetPageDecoder.Encoding.PLAIN;
            }
            case "dba-string" -> {
                DeltaByteArrayWriter w = new DeltaByteArrayWriter(64, 1 << 20, alloc);
                int key = 0;
                for (int i = 0; i < n; i++) {
                    key += 1 + rnd.nextInt(5);
                    w.writeBytes(Binary.fromString(String.format("https://example.com/catalog/item/%09d", key)));
                }
                page = w.getBytes().toByteArray();
                lane = VecType.UTF8;
                encoding = ParquetPageDecoder.Encoding.DELTA_BYTE_ARRAY;
            }
            default -> {
                DeltaLengthByteArrayValuesWriter w = new DeltaLengthByteArrayValuesWriter(64, 1 << 20, alloc);
                for (int i = 0; i < n; i++) {
                    w.writeBytes(Binary.fromString("value-" + rnd.nextInt(1_000_000)));
                }
                page = w.getBytes().toByteArray();
                lane = VecType.UTF8;
                encoding = ParquetPageDecoder.Encoding.DELTA_LENGTH_BYTE_ARRAY;
            }
        }
        arena = Arena.ofShared();
        GroupUnpacker[] cache = new GroupUnpacker[33];
        decoder = new ColumnChunkDecoder(lane == VecType.DECIMAL128 ? VecType.UTF8 : lane, 0, 1024,
                width -> {
                    if (cache[width] == null) {
                        BytePacker p = Packer.LITTLE_ENDIAN.newBytePacker(width);
                        cache[width] = p::unpack8Values;
                    }
                    return cache[width];
                });
        if (lane == VecType.DECIMAL128) {
            decoder.setFixedLength(16); // the bytes are staged, then converted into two limbs per row
            out = ArrowLayout.allocateData(arena, VecType.DECIMAL128, 1024);
        } else if (lane == VecType.UTF8) {
            outOffsets = ArrowLayout.allocateOffsets(arena, 1024);
            outBytes = ArrowLayout.allocateBytes(arena, 1 << 20);
        } else if (lane == VecType.BOOL) {
            out = arena.allocate(1024 / 8, 8); // one batch's value bitmap
        } else {
            out = ArrowLayout.allocateData(arena, lane, n);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    /**
     * Ours: the page through ColumnChunkDecoder in 1024-row batches, as the
     * scan reads it.
     */
    @Benchmark
    public long ours() {
        ColumnChunkDecoder d = decoder;
        d.startChunk(n);
        long sum = 0;
        for (int done = 0; done < n; done += 1024) {
            int want = Math.min(1024, n - done);
            d.startBatch(want);
            int filled = 0;
            while (filled < want) {
                if (d.needsPage()) {
                    d.feedPage(ColumnChunkDecoder.Page.v1(page, page.length, n, encoding));
                }
                filled += lane == VecType.UTF8 || lane == VecType.BOOL || lane == VecType.DECIMAL128
                        ? d.readBatch(want - filled, filled)
                        : d.readBatchDirectA(want - filled, done + filled, out, null);
            }
            if (lane == VecType.DECIMAL128) {
                d.flushDecimal(want, out, null, true);
                sum += out.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 8);
            } else if (lane == VecType.BOOL) {
                d.flushBool(want, out, null);
                sum += out.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 0);
            } else if (lane == VecType.UTF8) {
                d.flushUtf8(want, outOffsets, outBytes, null);
                sum += d.utf8Bytes();
            }
        }
        return sum;
    }

    /** parquet-java's own value reader over the same page, value at a time. */
    @Benchmark
    public long parquetJava() throws java.io.IOException {
        ValuesReader r = switch (shape) {
            case "bss-double" -> new ByteStreamSplitValuesReaderForDouble();
            case "bss-long" -> new ByteStreamSplitValuesReaderForLong();
            case "bss-int" -> new ByteStreamSplitValuesReaderForInteger();
            case "dba-string" -> new DeltaByteArrayReader();
            case "bool-plain" -> new BooleanPlainValuesReader();
            case "bool-rle" -> new RunLengthBitPackingHybridValuesReader(1);
            case "flba-dec38" -> new FixedLenByteArrayPlainValuesReader(16);
            default -> new DeltaLengthByteArrayValuesReader();
        };
        r.initFromPage(n, ByteBufferInputStream.wrap(java.nio.ByteBuffer.wrap(page)));
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += switch (shape) {
                        case "bss-double" -> (long) r.readDouble();
                        case "bss-long" -> r.readLong();
                        case "bss-int" -> r.readInteger();
                        case "bool-plain", "bool-rle" -> r.readBoolean() ? 1 : 0;
                        default -> r.readBytes().length();
                    };
        }
        return sum;
    }
}
