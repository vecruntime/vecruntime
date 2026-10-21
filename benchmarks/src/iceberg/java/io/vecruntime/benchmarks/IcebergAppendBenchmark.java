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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.iceberg.write.IcebergParquetAppender;
import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.parquet.write.ColumnarParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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

import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * Writing one Iceberg Parquet data file of a {@code store_sales}-like table (#842, W1), rows per
 * millisecond, three ways:
 * <ul>
 *   <li>{@code columnar}: {@link ColumnarParquetWriter} alone, on the schema Iceberg writes (field ids
 *       included) and with Iceberg's default page and dictionary sizes, with no metrics -- the floor;</li>
 *   <li>{@code iceberg-appender}: {@link IcebergParquetAppender} over the same writer, which adds the
 *       float/double field metrics, the footer conversion and {@code ParquetUtil.footerMetrics} -- the
 *       overhead of being an Iceberg appender, which should be within noise of the floor;</li>
 *   <li>{@code iceberg-generic}: Iceberg's own Parquet appender over prebuilt {@link Record}s, the row
 *       path the appender replaces (Spark's Iceberg writer takes the same path over {@code InternalRow}).</li>
 * </ul>
 * The same 262,144 rows each way, in 8,192-row batches for the columnar legs; uncompressed, as in
 * {@link ParquetWriteBenchmark}. The Iceberg runtime is provided, so run it with that jar on the
 * class path:
 *
 * <pre>
 * java -cp benchmarks/target/benchmarks.jar:iceberg-spark-runtime-4.1_2.13-&lt;v&gt;.jar org.openjdk.jmh.Main IcebergAppendBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(IcebergAppendBenchmark.ROWS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Thread)
public class IcebergAppendBenchmark {

    static final int ROWS = 262_144;
    static final int BATCH = 8192;

    static final Schema SCHEMA = new Schema(
            optional(1, "ss_sold_date_sk", Types.IntegerType.get()),
            optional(2, "ss_item_sk", Types.IntegerType.get()),
            optional(3, "ss_customer_sk", Types.IntegerType.get()),
            optional(4, "ss_store_sk", Types.IntegerType.get()),
            optional(5, "ss_ticket_number", Types.LongType.get()),
            optional(6, "ss_quantity", Types.IntegerType.get()),
            optional(7, "ss_list_price", Types.DecimalType.of(7, 2)),
            optional(8, "ss_sales_price", Types.DecimalType.of(7, 2)),
            optional(9, "ss_net_profit", Types.DecimalType.of(7, 2)),
            optional(10, "ss_ext_tax", Types.DoubleType.get()),
            optional(11, "s_dict", Types.StringType.get()),
            optional(12, "s_plain", Types.StringType.get()));

    private static final Map<String, String> PROPS = Map.of(TableProperties.PARQUET_COMPRESSION, "uncompressed");

    /** The writer under test. */
    @Param({"columnar", "iceberg-appender", "iceberg-generic"})
    public String writer;

    private final Configuration conf = new Configuration();
    private CodecFactory codecs;
    private MessageType fileSchema;
    private Arena arena;
    private List<List<VectorBuffers>> batches;
    private List<Record> records;

    @Setup(Level.Trial)
    public void setUp() {
        codecs = new CodecFactory(conf, 1 << 20);
        fileSchema = MessageTypeParser.parseMessageType(ParquetSchemaUtil.convert(SCHEMA, "table").toString());
        arena = Arena.ofShared();
        Random r = new Random(42);
        int[][] ints = new int[5][ROWS];
        boolean[] dateNulls = new boolean[ROWS];
        long[] ticket = new long[ROWS];
        long[][] prices = new long[3][ROWS];
        double[] tax = new double[ROWS];
        int[] dict = new int[ROWS];
        String[] plain = new String[ROWS];
        for (int k = 0; k < ROWS; k++) {
            dateNulls[k] = r.nextInt(1000) < 45;
            ints[0][k] = 2_450_816 + r.nextInt(1823);
            ints[1][k] = 1 + r.nextInt(300_000);
            ints[2][k] = 1 + r.nextInt(12_000_000);
            ints[3][k] = 1 + r.nextInt(1002);
            ints[4][k] = 1 + r.nextInt(100);
            ticket[k] = k / 10 + 1;
            for (int p = 0; p < 3; p++) {
                prices[p][k] = r.nextInt(30_000) - (p == 2 ? 10_000 : 0);
            }
            tax[k] = r.nextInt(100_000) / 100.0;
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
            for (int c = 0; c < 4; c++) {
                cols.add(ArrowLayout.ofInts(arena, java.util.Arrays.copyOfRange(ints[c], from, to),
                        c == 0 ? java.util.Arrays.copyOfRange(dateNulls, from, to) : null));
            }
            cols.add(ArrowLayout.ofLongs(arena, java.util.Arrays.copyOfRange(ticket, from, to), null));
            cols.add(ArrowLayout.ofInts(arena, java.util.Arrays.copyOfRange(ints[4], from, to), null));
            for (int p = 0; p < 3; p++) {
                cols.add(ArrowLayout.ofLongs(arena, java.util.Arrays.copyOfRange(prices[p], from, to), null));
            }
            cols.add(ArrowLayout.ofDoubles(arena, java.util.Arrays.copyOfRange(tax, from, to), null));
            SegmentVectorBuffers ids = ArrowLayout.ofInts(arena, java.util.Arrays.copyOfRange(dict, from, to), null);
            cols.add(new SegmentVectorBuffers(VecType.UTF8, to - from, null, ids.data(), null, dictBuffers));
            cols.add(ArrowLayout.ofStrings(arena, java.util.Arrays.copyOfRange(plain, from, to)));
            batches.add(cols);
        }
        records = new ArrayList<>(ROWS);
        String[] names = {"ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk"};
        for (int k = 0; k < ROWS; k++) {
            Record g = GenericRecord.create(SCHEMA);
            for (int c = 0; c < 4; c++) {
                g.setField(names[c], c == 0 && dateNulls[k] ? null : ints[c][k]);
            }
            g.setField("ss_ticket_number", ticket[k]);
            g.setField("ss_quantity", ints[4][k]);
            g.setField("ss_list_price", BigDecimal.valueOf(prices[0][k], 2));
            g.setField("ss_sales_price", BigDecimal.valueOf(prices[1][k], 2));
            g.setField("ss_net_profit", BigDecimal.valueOf(prices[2][k], 2));
            g.setField("ss_ext_tax", tax[k]);
            g.setField("s_dict", dictionary[dict[k]]);
            g.setField("s_plain", plain[k]);
            records.add(g);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        codecs.release();
        arena.close();
    }

    @Benchmark
    public long write() throws IOException {
        DiscardOutput out = new DiscardOutput();
        switch (writer) {
            case "columnar" -> {
                ParquetWriteBenchmark.DiscardFile file = new ParquetWriteBenchmark.DiscardFile();
                try (ColumnarParquetWriter w = new ColumnarParquetWriter(file,
                        fileSchema, icebergDefaults(),
                        codecs.getCompressor(CompressionCodecName.UNCOMPRESSED), 128L << 20, Map.of())) {
                    for (List<VectorBuffers> b : batches) {
                        w.write(b, b.get(0).length());
                    }
                }
                return file.bytes;
            }
            case "iceberg-appender" -> {
                IcebergParquetAppender a = IcebergParquetAppender.create(out, SCHEMA, PROPS, MetricsConfig.getDefault(),
                        null, conf);
                for (List<VectorBuffers> b : batches) {
                    a.write(b, 0, b.get(0).length());
                }
                a.close();
                return a.metrics().recordCount() + out.bytes;
            }
            default -> {
                FileAppender<Record> a = Parquet.write(out)
                        .schema(SCHEMA)
                        .setAll(PROPS)
                        .createWriterFunc(GenericParquetWriter::create)
                        .build();
                for (Record rec : records) {
                    a.add(rec);
                }
                a.close();
                return a.metrics().recordCount() + out.bytes;
            }
        }
    }

    /**
     * The page, page-row and dictionary sizes Iceberg's {@code write.parquet.*} defaults give the appender
     * (Iceberg's dictionary page is 2 MB where parquet-java's default is 1 MB), so the floor writes the same
     * pages and the comparison isolates what being an appender costs.
     */
    private static ParquetProperties icebergDefaults() {
        return ParquetProperties.builder()
                .withPageSize(TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT)
                .withPageRowCountLimit(TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT)
                .withDictionaryPageSize(TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT)
                .build();
    }

    /** An Iceberg {@link OutputFile} that counts and drops every byte. */
    static final class DiscardOutput implements OutputFile {
        long bytes;

        @Override
        public PositionOutputStream create() {
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
        public PositionOutputStream createOrOverwrite() {
            return create();
        }

        @Override
        public String location() {
            return "discard:///iceberg-append-benchmark.parquet";
        }

        @Override
        public InputFile toInputFile() {
            throw new UnsupportedOperationException("a discarded file cannot be read");
        }
    }
}
