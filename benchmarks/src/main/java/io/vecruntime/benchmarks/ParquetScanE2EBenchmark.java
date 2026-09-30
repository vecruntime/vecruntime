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

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.VecType;
import io.vecruntime.spark.adapter.TypeMapping;
import io.vecruntime.spark.arrow.ArrowSegments;
import io.vecruntime.spark.arrow.VectorAllocators;
import io.vecruntime.spark.parquet.NativeParquetColumnReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * The end-to-end verdict for #559 slice 1's scan node (STATUS-1b: "before any
 * commit -- end-to-end perf comparison"): decode a real 4M-row Parquet file's
 * decompressed pages into an Arrow batch through OUR chunk reader
 * ({@link NativeParquetColumnReader}), against Spark's own
 * {@link VectorizedParquetRecordReader} reading the same file into a
 * {@link ColumnarBatch}, same JVM, same file. The file mixes the encodings the
 * node must handle: INT32 dictionary, INT64 PLAIN, DECIMAL(7,2) (int32-backed),
 * a UTF8 dictionary column, each with ~10% nulls; a small {@code parquet.block.size}
 * gives several row groups and pages.
 *
 * <p>Both benchmarks compute the same checksum over every decoded value so a
 * silent wrong decode cannot look fast, and JMH's dead-code elimination cannot
 * drop the reads. Per AGENTS.md JMH is a sanity/relative check, not the 1 TB
 * verdict -- but the maintainer requires ours be competitive end to end before
 * the node is wired, and this is the measurement that decides it.
 *
 * <p>Not run from {@code benchmarks.jar} (Spark is a provided dependency): use
 * {@code benchmarks/scripts/run-parquet-e2e.sh}, which builds the Spark
 * classpath the way {@code run-flight-bench.sh} does.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class ParquetScanE2EBenchmark {

    private static final int ROWS = 4_000_000;
    private static final int BATCH = 4096;

    private SparkSession spark;
    private File dir;
    private String filePath;
    private StructType schema;
    private Configuration hadoopConf;
    private BufferAllocator allocator;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        spark = SparkSession.builder()
                .appName("parquet-e2e")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.parquet.compression.codec", "snappy")
                // Small blocks/pages so the 4M rows span many row groups and pages (the node's real shape).
                .config("parquet.block.size", String.valueOf(8 << 20))
                .config("parquet.page.size", String.valueOf(256 << 10))
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        schema = new StructType(
                new StructField[] {new StructField("i32", DataTypes.IntegerType, true, org.apache.spark.sql.types.Metadata.empty()), new StructField("i64", DataTypes.LongType, true, org.apache.spark.sql.types.Metadata.empty()),
                        new StructField("dec", DataTypes.createDecimalType(7, 2), true, org.apache.spark.sql.types.Metadata.empty()), new StructField("s", DataTypes.StringType, true, org.apache.spark.sql.types.Metadata.empty())});

        dir = Files.createTempDirectory("parquet-e2e").toFile();
        String out = new File(dir, "data").getAbsolutePath();
        writeData(out);
        // Spark writes a directory of part files; the vectorized reader reads one file at a time, so
        // pick the single part file the local write produced.
        File[] parts = new File(out).listFiles((d, nm) -> nm.endsWith(".parquet"));
        if (parts == null || parts.length == 0) {
            throw new IllegalStateException("no parquet part file produced under " + out);
        }
        Arrays.sort(parts);
        filePath = parts[0].getAbsolutePath();
        hadoopConf = spark.sessionState().newHadoopConf();
        allocator = VectorAllocators.newChild("parquet-e2e");

        // Correctness gate: ours and Spark must agree on the total checksum, or a fast-but-wrong decode
        // could look like a win. Both checksums are pure additive, per-value polynomial hashes, so the
        // traversal order (column-major vs batch-major) does not matter.
        long ours = oursChecksum();
        long theirs = sparkChecksum();
        if (ours != theirs) {
            throw new IllegalStateException("decode mismatch: ours=" + ours + " spark=" + theirs);
        }
    }

    private long oursChecksum() throws Exception {
        java.util.concurrent.atomic.AtomicLong acc = new java.util.concurrent.atomic.AtomicLong();
        oursDecodeInto(acc);
        return acc.get();
    }

    private long sparkChecksum() throws Exception {
        VectorizedParquetRecordReader reader = new VectorizedParquetRecordReader(true, BATCH);
        try {
            reader.initialize(filePath, columnNames());
            reader.initBatch(new StructType(), org.apache.spark.sql.catalyst.InternalRow.empty());
            reader.enableReturningBatches();
            long checksum = 0;
            while (reader.nextBatch()) {
                ColumnarBatch batch = reader.resultBatch();
                int n = batch.numRows();
                for (int c = 0; c < schema.fields().length; c++) {
                    checksum += checksumSpark(batch.column(c), schema.fields()[c].dataType(), n);
                }
            }
            return checksum;
        } finally {
            reader.close();
        }
    }

    private void writeData(String out) {
        Random rnd = new Random(559);
        // Small domains -> Spark dictionary-encodes i32 and s; i64 is high-cardinality -> PLAIN.
        int[] i32domain = new int[500];
        for (int i = 0; i < i32domain.length; i++) {
            i32domain[i] = rnd.nextInt(1_000_000);
        }
        String[] sdomain = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta",
                "eta", "theta", "a longer string value for width", ""};
        List<Row> rows = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            Integer i32 = rnd.nextInt(10) == 0 ? null : i32domain[rnd.nextInt(i32domain.length)];
            Long i64 = rnd.nextInt(10) == 0 ? null : rnd.nextLong();
            java.math.BigDecimal dec = rnd.nextInt(10) == 0 ? null : java.math.BigDecimal.valueOf(rnd.nextInt(100_000), 2);
            String s = rnd.nextInt(10) == 0 ? null : sdomain[rnd.nextInt(sdomain.length)];
            rows.add(RowFactory.create(i32, i64, dec, s));
        }
        Dataset<Row> df = spark.createDataFrame(rows, schema).coalesce(1);
        df.write()
          .mode("overwrite")
          .parquet(out);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (allocator != null) {
            allocator.close();
        }
        if (spark != null) {
            spark.stop();
        }
        if (dir != null) {
            deleteRec(dir);
        }
    }

    // ------------------------------------------------------------------ ours

    @Benchmark
    public void oursDecode(Blackhole bh) throws Exception {
        java.util.concurrent.atomic.AtomicLong acc = new java.util.concurrent.atomic.AtomicLong();
        oursDecodeInto(acc);
        bh.consume(acc.get());
    }

    /**
     * Decode-only: the whole file through our reader, consuming only the
     * vectors and their row counts (no per-value read). This times the DECODE,
     * not our Arrow output format -- the per-value checksum reads Arrow
     * segments through FFM and inflated {@code oursDecode} relative to {@code
     * sparkDecode}, which reads Spark's on-heap columns.
     */
    @Benchmark
    public void oursDecodeOnly(Blackhole bh) throws Exception {
        Path path = new Path(filePath);
        try (ParquetFileReader reader = ParquetFileReader.open(hadoopConf, path)) {
            List<ColumnDescriptor> columns = reader.getFooter()
                    .getFileMetaData()
                    .getSchema()
                    .getColumns();
            NativeParquetColumnReader[] readers = new NativeParquetColumnReader[columns.size()];
            for (int c = 0; c < columns.size(); c++) {
                ColumnDescriptor cd = columns.get(c);
                readers[c] = new NativeParquetColumnReader(cd, TypeMapping.vecTypeOf(schema.fields()[c].dataType()), schema.fields()[c].dataType(), cd.getPath()[0],
                        allocator);
            }
            long rows = 0;
            PageReadStore rg;
            List<BlockMetaData> blocks = reader.getFooter().getBlocks();
            int rgIndex = 0;
            while ((rg = reader.readNextRowGroup()) != null) {
                int rgRows = (int) blocks.get(rgIndex++).getRowCount();
                for (int c = 0; c < columns.size(); c++) {
                    FieldVector v = readers[c].readRowGroup(rg.getPageReader(columns.get(c)), rgRows);
                    bh.consume(v);
                    rows += v.getValueCount();
                    v.close();
                }
            }
            for (NativeParquetColumnReader r : readers) {
                r.close();
            }
            bh.consume(rows);
        }
    }

    private void oursDecodeInto(java.util.concurrent.atomic.AtomicLong acc) throws Exception {
        Path path = new Path(filePath);
        try (ParquetFileReader reader = ParquetFileReader.open(hadoopConf, path)) {
            MessageType fileSchema = reader.getFooter()
                    .getFileMetaData()
                    .getSchema();
            List<ColumnDescriptor> columns = fileSchema.getColumns();
            NativeParquetColumnReader[] readers = new NativeParquetColumnReader[columns.size()];
            for (int c = 0; c < columns.size(); c++) {
                ColumnDescriptor cd = columns.get(c);
                DataType dt = schema.fields()[c].dataType();
                VecType vt = TypeMapping.vecTypeOf(dt);
                readers[c] = new NativeParquetColumnReader(cd, vt, dt, cd.getPath()[0], allocator);
            }
            long checksum = 0;
            PageReadStore rg;
            List<BlockMetaData> blocks = reader.getFooter().getBlocks();
            int rgIndex = 0;
            while ((rg = reader.readNextRowGroup()) != null) {
                int rgRows = (int) blocks.get(rgIndex++).getRowCount();
                for (int c = 0; c < columns.size(); c++) {
                    PageReader pr = rg.getPageReader(columns.get(c));
                    DataType dt = schema.fields()[c].dataType();
                    VecType vt = TypeMapping.vecTypeOf(dt);
                    FieldVector v = readers[c].readRowGroup(pr, rgRows);
                    checksum += checksum(v, vt, dt, rgRows);
                    v.close();
                }
            }
            for (NativeParquetColumnReader r : readers) {
                r.close();
            }
            acc.set(checksum);
        }
    }

    private static long checksum(FieldVector v, VecType vt, DataType dt,
            int rows) {
        long sum = 0;
        boolean hasNulls = v.getNullCount() > 0;
        java.lang.foreign.MemorySegment validity = hasNulls ? ArrowSegments.of(v.getValidityBuffer()) : null;
        java.lang.foreign.MemorySegment data = ArrowSegments.of(v.getDataBuffer());
        var leInt = java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
        var leLong = java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
        if (dt instanceof org.apache.spark.sql.types.StringType) {
            java.lang.foreign.MemorySegment off = ArrowSegments.of(v.getOffsetBuffer());
            for (int i = 0; i < rows; i++) {
                if (validity != null && !io.vecruntime.kernels.Bitmap.isSet(validity, i)) {
                    continue;
                }
                int s = off.get(leInt, (long) i << 2);
                int e = off.get(leInt, (long) (i + 1) << 2);
                long h = 1;
                for (int b = s; b < e; b++) {
                    h = h * 31 + (data.get(java.lang.foreign.ValueLayout.JAVA_BYTE, b) & 0xFF);
                }
                sum += h;
            }
            return sum;
        }
        for (int i = 0; i < rows; i++) {
            if (validity != null && !io.vecruntime.kernels.Bitmap.isSet(validity, i)) {
                continue;
            }
            // INT32 (also DATE) and int32-backed narrow decimal read from the INT32 lane; INT64 / decimal(10..18)
            // / double bits from the INT64 lane. dec(7,2) is int32-backed -> INT32 lane -> unscaled value.
            if (vt == VecType.INT32) {
                sum += data.get(leInt, (long) i << 2);
            } else {
                sum += data.get(leLong, (long) i << 3);
            }
        }
        return sum;
    }

    // ------------------------------------------------------------------ spark

    @Benchmark
    public void sparkDecode(Blackhole bh) throws Exception {
        bh.consume(sparkChecksum());
    }

    /**
     * Decode-only counterpart of {@link #oursDecodeOnly}: batches consumed, row
     * counts summed, no per-value read.
     */
    @Benchmark
    public void sparkDecodeOnly(Blackhole bh) throws Exception {
        VectorizedParquetRecordReader reader = new VectorizedParquetRecordReader(true, BATCH);
        try {
            reader.initialize(filePath, columnNames());
            reader.initBatch(new StructType(), org.apache.spark.sql.catalyst.InternalRow.empty());
            reader.enableReturningBatches();
            long rows = 0;
            while (reader.nextBatch()) {
                ColumnarBatch batch = reader.resultBatch();
                bh.consume(batch);
                rows += batch.numRows();
            }
            bh.consume(rows);
        } finally {
            reader.close();
        }
    }

    private static long checksumSpark(ColumnVector col, DataType dt, int rows) {
        long sum = 0;
        if (dt instanceof org.apache.spark.sql.types.StringType) {
            for (int i = 0; i < rows; i++) {
                if (col.isNullAt(i)) {
                    continue;
                }
                byte[] bytes = col.getUTF8String(i).getBytes();
                long h = 1;
                for (byte b : bytes) {
                    h = h * 31 + (b & 0xFF);
                }
                sum += h;
            }
            return sum;
        }
        if (dt instanceof DecimalType d) {
            for (int i = 0; i < rows; i++) {
                if (col.isNullAt(i)) {
                    continue;
                }
                Decimal dec = col.getDecimal(i, d.precision(), d.scale());
                sum += dec.toUnscaledLong();
            }
            return sum;
        }
        if (dt instanceof org.apache.spark.sql.types.IntegerType) {
            for (int i = 0; i < rows; i++) {
                if (!col.isNullAt(i)) {
                    sum += col.getInt(i);
                }
            }
            return sum;
        }
        for (int i = 0; i < rows; i++) {
            if (!col.isNullAt(i)) {
                sum += col.getLong(i);
            }
        }
        return sum;
    }

    private static java.util.List<String> columnNames() {
        return Arrays.asList("i32", "i64", "dec", "s");
    }

    private static void deleteRec(File f) {
        File[] kids = f.listFiles();
        if (kids != null) {
            for (File k : kids) {
                deleteRec(k);
            }
        }
        // best effort
        f.delete();
    }
}
