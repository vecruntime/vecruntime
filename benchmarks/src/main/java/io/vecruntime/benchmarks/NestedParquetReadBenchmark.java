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

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import io.vecruntime.spark.arrow.VectorAllocators;
import io.vecruntime.spark.parquet.NativeParquetColumn;
import io.vecruntime.spark.parquet.NativeParquetListReader;
import io.vecruntime.spark.parquet.NativeParquetStructReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * The nested readers of the native Parquet scan (#568) against Spark's own
 * {@link VectorizedParquetRecordReader}, one nested column at a time, same
 * file, same JVM: a list of ints ({@code NativeParquetListReader}), a list of
 * strings, a struct of an int and a string ({@code NativeParquetStructReader}),
 * a map of strings to ints, a list of lists of ints ({@code
 * NativeParquetNestedListReader}) and a list of structs holding a list of
 * strings ({@code NativeParquetTreeReader}). Each benchmark reads the whole
 * column and folds every value into a checksum through Spark's {@link
 * ColumnVector} API -- the way an operator above the scan reads it -- and the
 * setup checks that both readers agree on it. One file of 1M rows, snappy,
 * Spark-written, several row groups.
 *
 * <p>Run with {@code benchmarks/scripts/run-parquet-e2e.sh
 * NestedParquetReadBenchmark} (Spark is a provided dependency, so not from
 * benchmarks.jar).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class NestedParquetReadBenchmark {

    private static final int ROWS = 1_000_000;
    private static final int BATCH = 4096;

    @Param({"li", "ls", "st", "m", "ll", "tree"})
    public String column;

    private SparkSession spark;
    private File dir;
    private String filePath;
    private StructType fileSparkSchema;
    private Configuration hadoopConf;
    private BufferAllocator allocator;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        spark = SparkSession.builder()
                .appName("nested-parquet")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("parquet.block.size", String.valueOf(16 << 20))
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        dir = Files.createTempDirectory("nested-parquet").toFile();
        String out = new File(dir, "data").getAbsolutePath();
        spark.range(0, ROWS)
             .selectExpr("case when id % 17 = 0 then null else sequence(0, cast(id % 7 as int)) end as li", "case when id % 13 = 0 then null else transform(sequence(0, cast(id % 5 as int)), x -> concat('s', (id + x) % 1009)) end as ls", "case when id % 11 = 0 then null else named_struct('a', cast(id as int), 'b', concat('b', id % 211)) end as st",
                     "map_from_arrays(transform(sequence(0, cast(id % 3 as int)), x -> concat('k', x)), sequence(0, cast(id % 3 as int))) as m", "array(sequence(0, cast(id % 4 as int)), cast(null as array<int>), sequence(1, 2)) as ll", "case when id % 9 = 0 then null else array(named_struct('x', cast(id as int), 'y', array(concat('y', id % 97), 'z')), named_struct('x', 1, 'y', cast(null as array<string>))) end as tree")
             .coalesce(1)
             .write()
             .mode("overwrite")
             .parquet(out);
        File[] parts = new File(out).listFiles((d, nm) -> nm.endsWith(".parquet"));
        Arrays.sort(parts);
        filePath = parts[0].getAbsolutePath();
        fileSparkSchema = spark.read()
                               .parquet(out)
                               .schema();
        hadoopConf = spark.sessionState().newHadoopConf();
        allocator = VectorAllocators.newChild("nested-parquet");
        long ours = ours();
        long theirs = spark();
        if (ours != theirs) {
            throw new IllegalStateException(column + ": ours=" + ours + " spark=" + theirs);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        allocator.close();
        spark.stop();
        deleteRecursively(dir);
    }

    private DataType type() {
        return fileSparkSchema.apply(column).dataType();
    }

    /**
     * Our nested reader for the column, over the file's own schema (which is
     * what the scan clips to).
     */
    private NativeParquetColumn reader(MessageType file) {
        DataType dt = type();
        if (dt instanceof StructType st) {
            return NativeParquetStructReader.create(file, new String[] {column}, st, column, BATCH,
                    allocator);
        }
        return NativeParquetListReader.create(file, column, dt, column, BATCH, allocator);
    }

    @Benchmark
    public long ours() throws Exception {
        try (ParquetFileReader r = ParquetFileReader.open(hadoopConf, new Path(filePath))) {
            MessageType file = r.getFooter()
                                .getFileMetaData()
                                .getSchema();
            MessageType requested = new MessageType(file.getName(), file.getType(column));
            r.setRequestedSchema(requested);
            NativeParquetColumn c = reader(requested);
            long sum = 0;
            try {
                PageReadStore rg;
                while ((rg = r.readNextRowGroup()) != null) {
                    int rows = (int) rg.getRowCount();
                    c.startRowGroup(rg, rows);
                    int done = 0;
                    while (done < rows) {
                        int n = Math.min(BATCH, rows - done);
                        Object b = c.readBatch(n);
                        sum += checksum(c.wrap(b), type(), n);
                        c.release(b);
                        done += n;
                    }
                }
            } finally {
                c.close();
            }
            return sum;
        }
    }

    @Benchmark
    public long spark() throws Exception {
        VectorizedParquetRecordReader reader = new VectorizedParquetRecordReader(true, BATCH);
        try {
            reader.initialize(filePath, java.util.List.of(column));
            reader.initBatch(new StructType(), org.apache.spark.sql.catalyst.InternalRow.empty());
            reader.enableReturningBatches();
            long sum = 0;
            while (reader.nextBatch()) {
                ColumnarBatch batch = reader.resultBatch();
                sum += checksum(batch.column(0), type(), batch.numRows());
            }
            return sum;
        } finally {
            reader.close();
        }
    }

    /**
     * Every value of `rows` rows folded into one number through the
     * ColumnVector API.
     */
    private static long checksum(ColumnVector v, DataType dt, int rows) {
        long sum = 0;
        for (int i = 0; i < rows; i++) {
            sum = sum * 31
                  + (v.isNullAt(i) ? 7 : value(top(v, dt, i), dt));
        }
        return sum;
    }

    private static Object top(ColumnVector v, DataType dt, int i) {
        if (dt instanceof ArrayType) {
            return v.getArray(i);
        }
        if (dt instanceof MapType) {
            return v.getMap(i);
        }
        if (dt instanceof StructType) {
            return v.getStruct(i);
        }
        if (dt instanceof org.apache.spark.sql.types.StringType) {
            return v.getUTF8String(i);
        }
        return dt instanceof org.apache.spark.sql.types.LongType
                ? (Object) v.getLong(i)
                : (Object) v.getInt(i);
    }

    /**
     * One value as an operator above the scan reads it: arrays, maps and
     * structs element by element.
     */
    private static long value(Object x, DataType dt) {
        if (dt instanceof ArrayType at) {
            ColumnarArray a = (ColumnarArray) x;
            long s = a.numElements();
            for (int k = 0; k < a.numElements(); k++) {
                s = s * 17
                    + (a.isNullAt(k) ? 7 : value(a.get(k, at.elementType()), at.elementType()));
            }
            return s;
        }
        if (dt instanceof MapType mt) {
            ColumnarMap m = (ColumnarMap) x;
            long s = m.numElements();
            for (int k = 0; k < m.numElements(); k++) {
                s = s * 17 + value(m.keyArray().get(k, mt.keyType()),
                        mt.keyType());
                s = s * 17
                    + (m.valueArray().isNullAt(k) ? 7 : value(m.valueArray().get(k, mt.valueType()),
                        mt.valueType()));
            }
            return s;
        }
        if (dt instanceof StructType st) {
            org.apache.spark.sql.catalyst.InternalRow r = (org.apache.spark.sql.catalyst.InternalRow) x;
            long s = 3;
            for (int f = 0; f < st.fields().length; f++) {
                DataType ft = st.fields()[f].dataType();
                s = s * 17
                    + (r.isNullAt(f) ? 7 : value(r.get(f, ft), ft));
            }
            return s;
        }
        return x.hashCode(); // an int, a long or a UTF8String
    }

    private static void deleteRecursively(File f) {
        File[] kids = f.listFiles();
        if (kids != null) {
            for (File k : kids) {
                deleteRecursively(k);
            }
        }
        f.delete();
    }
}
