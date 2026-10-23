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
import org.apache.parquet.column.Encoding;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BOOLEAN (#901 item 13) on {@link BooleanChunkWriter}: an optional and a
 * required boolean column, with all-true, all-false, mixed and (optional) all-
 * null runs, written by our writer and by parquet-java's v2 record writer and
 * compared value by value and in each chunk's statistics and value count. Our
 * writer encodes booleans as PLAIN (not the RLE / bit-packing hybrid parquet-java
 * picks for v2 booleans), because Iceberg's vectorized reader rejects RLE-encoded
 * boolean values; parquet-java reads either, so the read-back comparison still
 * holds. As parquet-java does, no Bloom filter is written for a boolean column.
 */
class BooleanChunkWriterTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional boolean opt;
              required boolean req;
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();
    private int files;

    private record Row(Boolean opt, boolean req) {}

    private static List<Row> rows(int n, long seed) {
        Random r = new Random(seed);
        List<Row> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            // Long runs of the same value, plus nulls, so the RLE path and bit-packed groups both fire.
            Boolean opt;
            int bucket = (k / 37) % 4;
            opt = switch (bucket) {
                        case 0 -> Boolean.TRUE;
                        case 1 -> Boolean.FALSE;
                        case 2 -> null;
                        default -> r.nextBoolean();
                    };
            out.add(new Row(opt, r.nextInt(5) != 0));
        }
        return out;
    }

    private static boolean[] optNulls(List<Row> rows) {
        boolean[] out = new boolean[rows.size()];
        for (int k = 0; k < out.length; k++) {
            out[k] = rows.get(k).opt() == null;
        }
        return out;
    }

    private static List<VectorBuffers> columns(Arena a, List<Row> rows) {
        int n = rows.size();
        boolean[] opt = new boolean[n];
        boolean[] req = new boolean[n];
        for (int k = 0; k < n; k++) {
            opt[k] = rows.get(k).opt() != null && rows.get(k).opt();
            req[k] = rows.get(k).req();
        }
        return List.of(ArrowLayout.ofBooleans(a, opt, optNulls(rows)), ArrowLayout.ofBooleans(a, req, null));
    }

    private ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(4 * 1024)
                .withBloomFilterEnabled(true)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Row> rows, int batch, long rgSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours-" + (files++) + ".parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 4 * 1024);
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
                .withPageSize(4 * 1024)
                .withBloomFilterEnabled(true)
                .build()) {
            for (Row r : rows) {
                Group g = f.newGroup();
                if (r.opt() != null) {
                    g.add("opt", r.opt());
                }
                g.add("req", r.req());
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
        for (int batch : new int[] {1, 300, rows.size()}) {
            assertEquals(readAll(writeParquetJava(rows, 128L << 20)), readAll(writeOurs(rows, batch, 128L << 20)),
                    "batch " + batch);
        }
    }

    @Test
    void sameChunkStatisticsAndEncodings() throws Exception {
        List<Row> rows = rows(20_000, 2);
        ParquetMetadata ours = footer(writeOurs(rows, 4096, 128L << 20));
        ParquetMetadata theirs = footer(writeParquetJava(rows, 128L << 20));
        BlockMetaData a = ours.getBlocks().get(0);
        BlockMetaData b = theirs.getBlocks().get(0);
        assertEquals(b.getRowCount(), a.getRowCount());
        for (int c = 0; c < b.getColumns().size(); c++) {
            ColumnChunkMetaData x = a.getColumns().get(c);
            ColumnChunkMetaData y = b.getColumns().get(c);
            String col = y.getPath().toDotString();
            assertEquals(y.getValueCount(), x.getValueCount(), col);
            // Our booleans are PLAIN, not the RLE parquet-java emits (Iceberg's vectorized reader rejects RLE
            // booleans). The value count and statistics still match parquet-java.
            assertEquals(java.util.Set.of(Encoding.PLAIN), java.util.Set.copyOf(x.getEncodings()), col);
            Statistics<?> sx = x.getStatistics();
            Statistics<?> sy = y.getStatistics();
            assertEquals(sy.getNumNulls(), sx.getNumNulls(), col);
            assertEquals(sy.genericGetMin(), sx.genericGetMin(), col);
            assertEquals(sy.genericGetMax(), sx.genericGetMax(), col);
        }
    }

    @Test
    void sameBloomFilterAsParquetJava() throws Exception {
        List<Row> rows = rows(5_000, 3);
        org.apache.hadoop.fs.Path ours = writeOurs(rows, 4096, 128L << 20);
        org.apache.hadoop.fs.Path theirs = writeParquetJava(rows, 128L << 20);
        try (ParquetFileReader a = ParquetFileReader.open(HadoopInputFile.fromPath(ours, conf));
             ParquetFileReader b = ParquetFileReader.open(HadoopInputFile.fromPath(theirs, conf))) {
            BlockMetaData ba = a.getFooter()
                                .getBlocks()
                                .get(0);
            BlockMetaData bb = b.getFooter()
                                .getBlocks()
                                .get(0);
            var ra = a.getBloomFilterDataReader(ba);
            var rb = b.getBloomFilterDataReader(bb);
            // parquet-java creates a filter for a boolean column but inserts nothing; ours must be the same
            // empty bitset.
            for (int c = 0; c < bb.getColumns().size(); c++) {
                String col = bb.getColumns()
                               .get(c)
                               .getPath()
                               .toDotString();
                assertEquals(
                        rb.readBloomFilter(bb.getColumns().get(c)),
                        ra.readBloomFilter(ba.getColumns().get(c)),
                        col + ": bloom filter");
            }
        }
    }

    @Test
    void severalRowGroups() throws Exception {
        List<Row> rows = rows(200_000, 4);
        org.apache.hadoop.fs.Path path = writeOurs(rows, 2048, 4 * 1024);
        ParquetMetadata m = footer(path);
        assertTrue(m.getBlocks().size() > 1, "several row groups, got " + m.getBlocks().size());
        assertEquals(readAll(writeParquetJava(rows, 4 * 1024)), readAll(path));
    }
}
