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
import org.apache.parquet.example.data.simple.NanoTime;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * INT96 (#901 item 13) on {@link FixedBinaryChunkWriter} (Kind.INT96): Spark's
 * INT96 timestamp carried in an INT64 lane of microseconds, written as 12 PLAIN
 * bytes (8 nanoseconds of the day, 4 Julian day, little-endian). The same rows
 * are written by our writer and by parquet-java's v2 record writer (NanoTime)
 * and compared value by value and in each chunk's value count, encodings
 * (RLE_DICTIONARY then PLAIN fallback) and null count. INT96 carries no min/max.
 */
class Int96ChunkWriterTest {

    private static final long MICROS_PER_DAY = 86_400_000_000L;
    private static final int JULIAN_DAY_OF_EPOCH = 2_440_588;

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional int96 ts;
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();
    private int files;

    private record Row(Long micros) {}

    private static List<Row> rows(int n, long seed, boolean unique) {
        Random r = new Random(seed);
        List<Row> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            Long micros;
            if (r.nextInt(9) == 0) {
                micros = null;
            } else if (unique) {
                micros = (long) r.nextInt(100) * MICROS_PER_DAY + (long) (r.nextDouble() * MICROS_PER_DAY);
            } else {
                // A small domain so the dictionary path fires.
                micros = (long) (2_450_000 + r.nextInt(20)) * MICROS_PER_DAY / 1000 * 1000;
            }
            out.add(new Row(micros));
        }
        return out;
    }

    private static boolean[] nulls(List<Row> rows) {
        boolean[] out = new boolean[rows.size()];
        for (int k = 0; k < out.length; k++) {
            out[k] = rows.get(k).micros() == null;
        }
        return out;
    }

    private static List<VectorBuffers> columns(Arena a, List<Row> rows) {
        int n = rows.size();
        long[] ts = new long[n];
        for (int k = 0; k < n; k++) {
            ts[k] = rows.get(k).micros() == null ? 0 : rows.get(k).micros();
        }
        return List.of(ArrowLayout.ofLongs(a, ts, nulls(rows)));
    }

    private static NanoTime nanoTime(long micros) {
        long day = Math.floorDiv(micros, MICROS_PER_DAY);
        long nanos = Math.floorMod(micros, MICROS_PER_DAY) * 1000L;
        return new NanoTime((int) (day + JULIAN_DAY_OF_EPOCH), nanos);
    }

    private ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(8 * 1024)
                .withBloomFilterEnabled(true)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Row> rows, int batch, long rgSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours-" + (files++) + ".parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 8 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), rgSize, Map.of())) {
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

    private org.apache.hadoop.fs.Path writeParquetJava(List<Row> rows, long rgSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("parquet-java-" + (files++) + ".parquet")
                .toString());
        SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> w = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(p, conf))
                .withType(SCHEMA)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(rgSize)
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(8 * 1024)
                .withBloomFilterEnabled(true)
                .build()) {
            for (Row r : rows) {
                Group g = f.newGroup();
                if (r.micros() != null) {
                    g.add("ts", nanoTime(r.micros()));
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
        List<Row> rows = rows(20_000, 1, true);
        for (int batch : new int[] {1, 512, rows.size()}) {
            assertEquals(readAll(writeParquetJava(rows, 128L << 20)), readAll(writeOurs(rows, batch, 128L << 20)),
                    "batch " + batch);
        }
    }

    @Test
    void sameChunkValueCountEncodingsAndNulls() throws Exception {
        // Small domain -> dictionary; a long run -> an RLE_DICTIONARY page then PLAIN fallback across the chunk.
        List<Row> rows = rows(30_000, 2, false);
        ParquetMetadata ours = footer(writeOurs(rows, 4096, 128L << 20));
        ParquetMetadata theirs = footer(writeParquetJava(rows, 128L << 20));
        BlockMetaData a = ours.getBlocks().get(0);
        BlockMetaData b = theirs.getBlocks().get(0);
        assertEquals(b.getRowCount(), a.getRowCount());
        ColumnChunkMetaData x = a.getColumns().get(0);
        ColumnChunkMetaData y = b.getColumns().get(0);
        assertEquals(y.getValueCount(), x.getValueCount());
        assertEquals(y.getEncodings(), x.getEncodings());
        Statistics<?> sx = x.getStatistics();
        Statistics<?> sy = y.getStatistics();
        assertEquals(sy.getNumNulls(), sx.getNumNulls());
        // INT96 has no min/max on either side.
        assertEquals(sy.hasNonNullValue(), sx.hasNonNullValue());
    }

    @Test
    void uniqueValuesFallBackToPlain() throws Exception {
        List<Row> rows = rows(20_000, 3, true);
        org.apache.hadoop.fs.Path path = writeOurs(rows, 4096, 128L << 20);
        assertEquals(readAll(writeParquetJava(rows, 128L << 20)), readAll(path));
        java.util.Set<org.apache.parquet.column.Encoding> enc = new java.util.HashSet<>();
        for (BlockMetaData blk : footer(path).getBlocks()) {
            for (ColumnChunkMetaData c : blk.getColumns()) {
                enc.addAll(c.getEncodings());
            }
        }
        assertTrue(enc.contains(org.apache.parquet.column.Encoding.PLAIN), enc::toString);
    }
}
