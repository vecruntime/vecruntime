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
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.vecruntime.spark.adapter.TypeMapping;
import io.vecruntime.spark.arrow.VectorAllocators;
import io.vecruntime.spark.parquet.NativeParquetColumnReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * #786: the native reader decoding one file written with v1 or v2 data pages
 * under a codec, all columns, whole file per invocation. Run with {@code -prof
 * gc}: {@code gc.alloc.rate.norm} is the bytes allocated per file, so the page
 * copies the reader no longer makes (v1 pages and v2 sections read in place
 * when parquet-java hands back a heap buffer) show up there. Columns: a
 * nullable INT32 with a small dictionary, a high-cardinality INT64, a nullable
 * string dictionary.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class ParquetPageVersionBenchmark {

    private static final int ROWS = 2_000_000;
    private static final int BATCH_ROWS = 4096;

    @Param({"v1", "v2"})
    public String version;

    @Param({"zstd", "uncompressed", "snappy"})
    public String codec;

    // #735: off = parquet-java's default codec factory + allocator (a fresh array per page, per row group);
    // on = our ReusingCodecFactory + PooledHeapAllocator through the shared scan helper. The before/after.
    @Param({"off", "on"})
    public String tuning;

    private SparkSession spark;
    private File dir;
    private String filePath;
    private StructType schema;
    private Configuration hadoopConf;
    private BufferAllocator allocator;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        spark = SparkSession.builder()
                .appName("parquet-page-version")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.parquet.compression.codec", codec)
                .config("parquet.writer.version", version)
                .config("parquet.block.size", String.valueOf(8 << 20))
                .config("parquet.page.size", String.valueOf(64 << 10))
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        dir = Files.createTempDirectory("parquet-page-version").toFile();
        String out = new File(dir, "data").getAbsolutePath();
        spark.range(0, ROWS)
             .selectExpr("IF(id % 10 = 0, NULL, CAST(id % 500 AS INT)) AS i32", "id * 2654435761 AS i64", "IF(id % 7 = 0, NULL, concat('value-', CAST(id % 37 AS STRING))) AS s")
             .coalesce(1)
             .write()
             .mode("overwrite")
             .parquet(out);
        File[] parts = new File(out).listFiles((d, nm) -> nm.endsWith(".parquet"));
        if (parts == null || parts.length == 0) {
            throw new IllegalStateException("no parquet part file produced under " + out);
        }
        Arrays.sort(parts);
        filePath = parts[0].getAbsolutePath();
        schema = spark.read()
                      .parquet(filePath)
                      .schema();
        hadoopConf = spark.sessionState().newHadoopConf();
        allocator = VectorAllocators.newChild("parquet-page-version");
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
            try (var walk = Files.walk(dir.toPath())) {
                walk.sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
            }
        }
    }

    /**
     * The whole file through our reader, every column; consumes the vectors and
     * their row counts.
     */
    @Benchmark
    public long decode(Blackhole bh) throws Exception {
        boolean on = "on".equals(tuning);
        org.apache.parquet.hadoop.util.HadoopInputFile inputFile = org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new Path(filePath), hadoopConf);
        org.apache.parquet.ParquetReadOptions options = io.vecruntime.spark.parquet.NativeParquetReaderOptions.apply(org.apache.parquet.HadoopReadOptions.builder(hadoopConf, new Path(filePath)),
                hadoopConf, on, on)
                .build();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
            List<ColumnDescriptor> columns = reader.getFooter()
                    .getFileMetaData()
                    .getSchema()
                    .getColumns();
            NativeParquetColumnReader[] readers = new NativeParquetColumnReader[columns.size()];
            for (int c = 0; c < columns.size(); c++) {
                ColumnDescriptor cd = columns.get(c);
                readers[c] = new NativeParquetColumnReader(cd, TypeMapping.vecTypeOf(schema.fields()[c].dataType()), schema.fields()[c].dataType(), cd.getPath()[0],
                        BATCH_ROWS, allocator);
                readers[c].setReuseDecompress(on);
            }
            long rows = 0;
            PageReadStore rg;
            List<BlockMetaData> blocks = reader.getFooter().getBlocks();
            int rgIndex = 0;
            while ((rg = reader.readNextRowGroup()) != null) {
                int rgRows = (int) blocks.get(rgIndex++).getRowCount();
                for (int c = 0; c < columns.size(); c++) {
                    readers[c].startRowGroup(rg.getPageReader(columns.get(c)), rgRows);
                    int done = 0;
                    while (done < rgRows) {
                        int n = Math.min(BATCH_ROWS, rgRows - done);
                        FieldVector v = readers[c].readBatch(n);
                        bh.consume(v);
                        rows += v.getValueCount();
                        readers[c].release(v);
                        done += n;
                    }
                }
            }
            for (NativeParquetColumnReader r : readers) {
                r.close();
            }
            return rows;
        }
    }
}
