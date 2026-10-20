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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
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
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
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
 * The Parquet encoding cost of a {@code store_sales}-shaped write (#64),
 * without I/O: the same rows written to a discarding {@link OutputFile} by the
 * columnar writer ({@link ColumnarParquetWriter}, from the engine's {@link
 * VectorBuffers}, in 8,192-row batches, Parquet v2) and by parquet-java's
 * record writer ({@code ExampleParquetWriter} over prebuilt {@link Group}s,
 * the record path Spark's {@code ParquetWriteSupport} also takes), as v2 (the
 * same encodings as ours) and as v1 (Spark's default).
 *
 * <p>Columns: six int surrogate keys (one with 4.5 % nulls, as {@code
 * ss_sold_date_sk}), a long ticket number, a decimal(7,2) quantity-like value
 * as INT32, three decimal(7,2) prices in an INT64 lane written as INT32, a
 * dictionary-encoded string (ids into 500 entries) and a plain string.
 * Uncompressed: the 1 TB profile put compression at 0.7 % of the write's CPU.
 *
 * <pre>
 * java -jar benchmarks/target/benchmarks.jar ParquetWriteBenchmark -prof jfr
 * java -jar benchmarks/target/benchmarks.jar ParquetWriteBenchmark -prof async:libPath=...;output=flamegraph
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(ParquetWriteBenchmark.ROWS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class ParquetWriteBenchmark {

    static final int ROWS = 262_144;
    static final int BATCH = 8192;

    static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional int32 ss_sold_date_sk;
              optional int32 ss_item_sk;
              optional int32 ss_customer_sk;
              optional int32 ss_store_sk;
              optional int32 ss_promo_sk;
              optional int32 ss_quantity;
              optional int64 ss_ticket_number;
              optional int32 ss_list_price (DECIMAL(7,2));
              optional int32 ss_sales_price (DECIMAL(7,2));
              optional int32 ss_net_profit (DECIMAL(7,2));
              optional binary s_dict (STRING);
              optional binary s_plain (STRING);
            }
            """);

    /** The writer under test. */
    @Param({"columnar", "parquet-java-v2", "parquet-java-v1"})
    public String writer;

    private final Configuration conf = new Configuration();
    private CodecFactory codecs;
    private ParquetProperties props;
    private Arena arena;
    private List<List<VectorBuffers>> batches;
    private List<Group> groups;

    @Setup(Level.Trial)
    public void setUp() {
        codecs = new CodecFactory(conf, 1 << 20);
        props = ParquetProperties.builder().build();
        arena = Arena.ofShared();
        Random r = new Random(42);
        int[][] ints = new int[6][ROWS];
        boolean[] dateNulls = new boolean[ROWS];
        long[] ticket = new long[ROWS];
        long[][] prices = new long[3][ROWS];
        int[] dict = new int[ROWS];
        String[] plain = new String[ROWS];
        for (int k = 0; k < ROWS; k++) {
            dateNulls[k] = r.nextInt(1000) < 45;
            ints[0][k] = 2_450_816 + r.nextInt(1823);
            ints[1][k] = 1 + r.nextInt(300_000);
            ints[2][k] = 1 + r.nextInt(12_000_000);
            ints[3][k] = 1 + r.nextInt(1002);
            ints[4][k] = 1 + r.nextInt(1500);
            ints[5][k] = 1 + r.nextInt(100);
            ticket[k] = k / 10 + 1;
            for (int p = 0; p < 3; p++) {
                prices[p][k] = r.nextInt(30_000) - (p == 2 ? 10_000 : 0);
            }
            dict[k] = r.nextInt(500);
            plain[k] = "p" + r.nextInt(1_000_000);
        }
        String[] dictionary = new String[500];
        for (int k = 0; k < dictionary.length; k++) {
            dictionary[k] = "entry-" + k;
        }
        VectorBuffers dictBuffers = ArrowLayout.ofStrings(arena, dictionary);
        batches = new ArrayList<>();
        for (int from = 0; from < ROWS; from += BATCH) {
            int to = Math.min(ROWS, from + BATCH);
            List<VectorBuffers> cols = new ArrayList<>();
            for (int c = 0; c < 6; c++) {
                cols.add(ArrowLayout.ofInts(arena, java.util.Arrays.copyOfRange(ints[c], from, to),
                        c == 0 ? java.util.Arrays.copyOfRange(dateNulls, from, to) : null));
            }
            cols.add(ArrowLayout.ofLongs(arena, java.util.Arrays.copyOfRange(ticket, from, to), null));
            for (int p = 0; p < 3; p++) {
                cols.add(ArrowLayout.ofLongs(arena, java.util.Arrays.copyOfRange(prices[p], from, to), null));
            }
            SegmentVectorBuffers ids = ArrowLayout.ofInts(arena, java.util.Arrays.copyOfRange(dict, from, to), null);
            cols.add(new SegmentVectorBuffers(VecType.UTF8, to - from, null, ids.data(),
                    null, dictBuffers));
            cols.add(ArrowLayout.ofStrings(arena, java.util.Arrays.copyOfRange(plain, from, to)));
            batches.add(cols);
        }
        SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
        groups = new ArrayList<>(ROWS);
        String[] names = {"ss_sold_date_sk", "ss_item_sk", "ss_customer_sk",
                "ss_store_sk", "ss_promo_sk", "ss_quantity"};
        for (int k = 0; k < ROWS; k++) {
            Group g = f.newGroup();
            for (int c = 0; c < 6; c++) {
                if (c != 0 || !dateNulls[k]) {
                    g.add(names[c], ints[c][k]);
                }
            }
            g.add("ss_ticket_number", ticket[k]);
            g.add("ss_list_price", (int) prices[0][k]);
            g.add("ss_sales_price", (int) prices[1][k]);
            g.add("ss_net_profit", (int) prices[2][k]);
            g.add("s_dict", dictionary[dict[k]]);
            g.add("s_plain", plain[k]);
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
        DiscardFile out = new DiscardFile();
        if (writer.equals("columnar")) {
            try (ColumnarParquetWriter w = new ColumnarParquetWriter(out, SCHEMA, props, codecs.getCompressor(CompressionCodecName.UNCOMPRESSED),
                    128L << 20, Map.of())) {
                for (List<VectorBuffers> b : batches) {
                    w.write(b, b.get(0)
                                .length());
                }
            }
        } else {
            try (ParquetWriter<Group> w = ExampleParquetWriter.builder(out)
                    .withType(SCHEMA)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withRowGroupSize(128L << 20)
                    .withWriterVersion(writer.equals("parquet-java-v1") ? ParquetProperties.WriterVersion.PARQUET_1_0 : ParquetProperties.WriterVersion.PARQUET_2_0)
                    .build()) {
                for (Group g : groups) {
                    w.write(g);
                }
            }
        }
        return out.bytes;
    }

    /** An {@link OutputFile} that counts and drops every byte. */
    static final class DiscardFile implements OutputFile {
        long bytes;

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {
                @Override
                public long getPos() {
                    return bytes;
                }

                @Override
                public void write(int b) {
                    bytes++;
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    bytes += len;
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}
