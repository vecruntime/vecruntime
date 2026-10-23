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
package io.vecruntime.parquet.write;

import java.lang.foreign.Arena;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The flat types #901 item 13 moved from parquet-java's per-value column writer
 * onto {@link FixedWidthChunkWriter}: a FLOAT narrowed from a FLOAT64 lane (also
 * covered by {@code ColumnarParquetWriterTest}), a millisecond timestamp carried
 * in an INT64 lane of microseconds (divided), and decimals of precision up to 9
 * and up to 18 held as the low limb of a DECIMAL128 lane and written as INT32 and
 * INT64. The same rows are written by our writer and by parquet-java's v2 record
 * writer and compared value by value and in each chunk's statistics, value count
 * and encodings.
 */
class FlatExtraTypesTest {

    // tsMicros holds microseconds; the column is TIMESTAMP_MILLIS, so the writer divides by 1000.
    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional int64 ts (TIMESTAMP(MILLIS,true));
              optional int32 d9 (DECIMAL(9,2));
              optional int64 d18 (DECIMAL(18,4));
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();
    private int files;

    private record Row(Long tsMicros, Integer d9, Long d18) {}

    private static List<Row> rows(int n, long seed) {
        Random r = new Random(seed);
        List<Row> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            out.add(
                    new Row(
                            r.nextInt(12) == 0 ? null : (long) r.nextInt(2_000_000_000) * 1000L,
                            r.nextInt(10) == 0 ? null : r.nextInt(1_000_000_000) - 500_000_000,
                            r.nextInt(8) == 0 ? null : (r.nextLong() % 500_000_000_000_000L)));
        }
        return out;
    }

    private static boolean[] nulls(List<Row> rows, java.util.function.Function<Row, Object> f) {
        boolean[] out = new boolean[rows.size()];
        for (int k = 0; k < out.length; k++) {
            out[k] = f.apply(rows.get(k)) == null;
        }
        return out;
    }

    private static List<VectorBuffers> columns(Arena a, List<Row> rows) {
        int n = rows.size();
        long[] ts = new long[n];
        BigInteger[] d9 = new BigInteger[n];
        BigInteger[] d18 = new BigInteger[n];
        for (int k = 0; k < n; k++) {
            Row row = rows.get(k);
            ts[k] = row.tsMicros() == null ? 0 : row.tsMicros();
            d9[k] = row.d9() == null ? null : BigInteger.valueOf(row.d9());
            d18[k] = row.d18() == null ? null : BigInteger.valueOf(row.d18());
        }
        return List.of(ArrowLayout.ofLongs(a, ts, nulls(rows, Row::tsMicros)), ArrowLayout.ofDecimal128(a, d9, nulls(rows, Row::d9)),
                ArrowLayout.ofDecimal128(a, d18, nulls(rows, Row::d18)));
    }

    private ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(16 * 1024)
                .withDictionaryPageSize(16 * 1024)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Row> rows, int batch, long rowGroupSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours-" + (files++) + ".parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 16 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), rowGroupSize, Map.of())) {
            for (int from = 0; from < rows.size(); from += batch) {
                List<Row> slice = rows.subList(from,
                        Math.min(rows.size(), from + batch));
                try (Arena a = Arena.ofConfined()) {
                    w.write(columns(a, slice), slice.size());
                }
            }
        } finally {
            codecs.release();
        }
        return p;
    }

    private org.apache.hadoop.fs.Path writeParquetJava(List<Row> rows, long rowGroupSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("parquet-java-" + (files++) + ".parquet")
                .toString());
        SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> w = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(p, conf))
                .withType(SCHEMA)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(16 * 1024)
                .withDictionaryPageSize(16 * 1024)
                .build()) {
            for (Row r : rows) {
                Group g = f.newGroup();
                if (r.tsMicros() != null) {
                    g.add("ts", r.tsMicros() / 1000L);
                }
                if (r.d9() != null) {
                    g.add("d9", r.d9());
                }
                if (r.d18() != null) {
                    g.add("d18", r.d18());
                }
                w.write(g);
            }
        }
        return p;
    }

    private List<String> readAll(org.apache.hadoop.fs.Path p) throws Exception {
        List<String> out = new ArrayList<>();
        try (ParquetReader<Group> r = ParquetReader.builder(new GroupReadSupport(), p)
                .withConf(conf)
                .build()) {
            for (Group g = r.read();
                 g != null;
                 g = r.read()) {
                out.add(g.toString());
            }
        }
        return out;
    }

    private ParquetMetadata footer(org.apache.hadoop.fs.Path p) throws Exception {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, conf))) {
            return r.getFooter();
        }
    }

    @Test
    void sameValuesAsParquetJava() throws Exception {
        List<Row> rows = rows(20_000, 1);
        for (int batch : new int[] {1, 777, rows.size()}) {
            assertEquals(readAll(writeParquetJava(rows, 128L << 20)), readAll(writeOurs(rows, batch, 128L << 20)),
                    "batch " + batch);
        }
    }

    @Test
    void sameChunkStatisticsAndEncodings() throws Exception {
        List<Row> rows = rows(20_000, 2);
        ParquetMetadata ours = footer(writeOurs(rows, 4096, 128L << 20));
        ParquetMetadata theirs = footer(writeParquetJava(rows, 128L << 20));
        assertEquals(1, ours.getBlocks()
                            .size());
        BlockMetaData a = ours.getBlocks().get(0);
        BlockMetaData b = theirs.getBlocks().get(0);
        assertEquals(b.getRowCount(), a.getRowCount());
        for (int c = 0; c < b.getColumns().size(); c++) {
            ColumnChunkMetaData x = a.getColumns().get(c);
            ColumnChunkMetaData y = b.getColumns().get(c);
            String col = y.getPath().toDotString();
            assertEquals(y.getValueCount(), x.getValueCount(), col);
            assertEquals(y.getEncodings(), x.getEncodings(), col);
            Statistics<?> sx = x.getStatistics();
            Statistics<?> sy = y.getStatistics();
            assertEquals(sy.getNumNulls(), sx.getNumNulls(), col);
            assertEquals(sy.genericGetMin(), sx.genericGetMin(), col);
            assertEquals(sy.genericGetMax(), sx.genericGetMax(), col);
        }
    }
}
