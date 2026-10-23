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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.NestedVectorBuffers;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.parquet.write.ColumnarParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
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
 * The Parquet encoding cost of a nested write and a boolean/decimal38 write
 * (#901), without I/O: the same rows written to a discarding output by the
 * columnar writer ({@link ColumnarParquetWriter}, from the engine's {@link
 * VectorBuffers}, Parquet v2, our own chunk/leaf writers) and by parquet-java's
 * record writer ({@code ExampleParquetWriter} over prebuilt {@link Group}s, its
 * per-value column writer), both as Parquet v2. {@code shape=nested} measures
 * NestedLeafWriter against parquet-java's column writer on struct/array/map
 * leaves; {@code shape=booldecimal} measures BooleanChunkWriter and
 * FixedBinaryChunkWriter (decimal38) against it on flat columns that previously
 * went through parquet-java's writer.
 *
 * <pre>
 * java -jar benchmarks/target/benchmarks.jar ParquetWriteNestedBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(ParquetWriteNestedBenchmark.ROWS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class ParquetWriteNestedBenchmark {

    static final int ROWS = 131_072;
    static final int BATCH = 8192;

    private static final MessageType NESTED = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional int64 id;
              optional group arr (LIST) {
                repeated group list {
                  optional int64 element;
                }
              }
              optional group m (MAP) {
                repeated group key_value {
                  required binary key (STRING);
                  optional int32 value;
                }
              }
            }
            """);

    private static final MessageType BOOL_DECIMAL = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional boolean flag;
              optional fixed_len_byte_array(16) amount (DECIMAL(38,6));
            }
            """);

    @Param({"nested", "booldecimal"})
    public String shape;

    @Param({"columnar", "parquet-java-v2"})
    public String writer;

    private final Configuration conf = new Configuration();
    private CodecFactory codecs;
    private ParquetProperties props;
    private Arena arena;
    private MessageType schema;
    private List<List<VectorBuffers>> batches;
    private List<Group> groups;

    @Setup(Level.Trial)
    public void setUp() {
        codecs = new CodecFactory(conf, 1 << 20);
        props = ParquetProperties.builder().build();
        arena = Arena.ofShared();
        schema = shape.equals("nested") ? NESTED : BOOL_DECIMAL;
        if (shape.equals("nested")) {
            setUpNested();
        } else {
            setUpBoolDecimal();
        }
    }

    private void setUpNested() {
        Random r = new Random(42);
        batches = new ArrayList<>();
        groups = new ArrayList<>(ROWS);
        SimpleGroupFactory f = new SimpleGroupFactory(schema);
        for (int from = 0; from < ROWS; from += BATCH) {
            int to = Math.min(ROWS, from + BATCH);
            int n = to - from;
            long[] id = new long[n];
            // arr: list of longs, with nulls and empties.
            MemorySegment arrOffsets = ArrowLayout.allocateOffsets(arena, n);
            List<Long> arrElems = new ArrayList<>();
            boolean[] arrNull = new boolean[n];
            // m: map string -> int, with nulls and empties.
            MemorySegment mOffsets = ArrowLayout.allocateOffsets(arena, n);
            List<String> keys = new ArrayList<>();
            List<Integer> vals = new ArrayList<>();
            boolean[] mNull = new boolean[n];
            for (int k = 0; k < n; k++) {
                id[k] = from + k;
                arrOffsets.set(VectorBuffers.LE_INT, (long) k << 2, arrElems.size());
                arrNull[k] = r.nextInt(8) == 0;
                if (!arrNull[k]) {
                    int c = r.nextInt(4);
                    for (int j = 0; j < c; j++) {
                        arrElems.add((long) r.nextInt(1_000_000));
                    }
                }
                mOffsets.set(VectorBuffers.LE_INT, (long) k << 2, keys.size());
                mNull[k] = r.nextInt(8) == 0;
                if (!mNull[k]) {
                    int c = r.nextInt(4);
                    for (int j = 0; j < c; j++) {
                        keys.add("k" + r.nextInt(100));
                        vals.add(r.nextInt(5) == 0 ? null : r.nextInt(10_000));
                    }
                }
            }
            arrOffsets.set(VectorBuffers.LE_INT, (long) n << 2, arrElems.size());
            mOffsets.set(VectorBuffers.LE_INT, (long) n << 2, keys.size());
            long[] av = new long[arrElems.size()];
            for (int i = 0; i < av.length; i++) {
                av[i] = arrElems.get(i);
            }
            int[] vv = new int[vals.size()];
            boolean[] vn = new boolean[vals.size()];
            for (int i = 0; i < vals.size(); i++) {
                vn[i] = vals.get(i) == null;
                vv[i] = vn[i] ? 0 : vals.get(i);
            }
            VectorBuffers arr = NestedVectorBuffers.list(n, ArrowLayout.validityFrom(arena, arrNull, n), arrOffsets, ArrowLayout.ofLongs(arena, av, null));
            VectorBuffers map = NestedVectorBuffers.map(n, ArrowLayout.validityFrom(arena, mNull, n), mOffsets,
                    ArrowLayout.ofStrings(arena, keys.toArray(new String[0])), ArrowLayout.ofInts(arena, vv, vn));
            batches.add(List.of(ArrowLayout.ofLongs(arena, id, null), arr, map));
        }
        // parquet-java groups for the same rows.
        Random rg = new Random(42);
        for (int k = 0; k < ROWS; k++) {
            Group g = f.newGroup();
            g.add("id", (long) k);
            boolean arrNull = rg.nextInt(8) == 0;
            Group arr = g.addGroup("arr");
            if (!arrNull) {
                int c = rg.nextInt(4);
                for (int j = 0; j < c; j++) {
                    arr.addGroup("list").add("element", (long) rg.nextInt(1_000_000));
                }
            }
            boolean mNull = rg.nextInt(8) == 0;
            Group m = g.addGroup("m");
            if (!mNull) {
                int c = rg.nextInt(4);
                for (int j = 0; j < c; j++) {
                    Group kv = m.addGroup("key_value");
                    kv.add("key", "k" + rg.nextInt(100));
                    boolean vNull = rg.nextInt(5) == 0;
                    int val = rg.nextInt(10_000);
                    if (!vNull) {
                        kv.add("value", val);
                    }
                }
            }
            groups.add(g);
        }
    }

    private static byte[] be(BigInteger v) {
        byte[] s = v.toByteArray();
        byte[] o = new byte[16];
        byte pad = (byte) (v.signum() < 0 ? -1 : 0);
        int off = 16 - s.length;
        for (int k = 0; k < 16; k++) {
            o[k] = k < off ? pad : s[k - off];
        }
        return o;
    }

    private void setUpBoolDecimal() {
        Random r = new Random(42);
        boolean[] flag = new boolean[ROWS];
        boolean[] flagNull = new boolean[ROWS];
        BigInteger[] amount = new BigInteger[ROWS];
        boolean[] amtNull = new boolean[ROWS];
        for (int k = 0; k < ROWS; k++) {
            flagNull[k] = r.nextInt(12) == 0;
            flag[k] = r.nextBoolean();
            amtNull[k] = r.nextInt(10) == 0;
            amount[k] = amtNull[k] ? null : new BigInteger(90, r).subtract(BigInteger.ONE.shiftLeft(89));
        }
        batches = new ArrayList<>();
        for (int from = 0; from < ROWS; from += BATCH) {
            int to = Math.min(ROWS, from + BATCH);
            batches.add(
                    List.of(
                            ArrowLayout.ofBooleans(arena, java.util.Arrays.copyOfRange(flag, from, to), java.util.Arrays.copyOfRange(flagNull, from, to)),
                            ArrowLayout.ofDecimal128(arena, java.util.Arrays.copyOfRange(amount, from, to), java.util.Arrays.copyOfRange(amtNull, from, to))));
        }
        SimpleGroupFactory f = new SimpleGroupFactory(schema);
        groups = new ArrayList<>(ROWS);
        for (int k = 0; k < ROWS; k++) {
            Group g = f.newGroup();
            if (!flagNull[k]) {
                g.add("flag", flag[k]);
            }
            if (!amtNull[k]) {
                g.add("amount", Binary.fromConstantByteArray(be(amount[k])));
            }
            groups.add(g);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        codecs.release();
        arena.close();
    }

    @Benchmark
    public long write() throws IOException {
        ParquetWriteBenchmark.DiscardFile out = new ParquetWriteBenchmark.DiscardFile();
        if (writer.equals("columnar")) {
            try (ColumnarParquetWriter w = new ColumnarParquetWriter(out, schema, props, codecs.getCompressor(CompressionCodecName.UNCOMPRESSED),
                    128L << 20, Map.of())) {
                for (List<VectorBuffers> b : batches) {
                    w.write(b, b.get(0)
                                .length());
                }
            }
        } else {
            try (ParquetWriter<Group> w = ExampleParquetWriter.builder(out)
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withRowGroupSize(128L << 20)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .build()) {
                for (Group g : groups) {
                    w.write(g);
                }
            }
        }
        return out.bytes;
    }
}
