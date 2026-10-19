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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
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
 * Dictionary fallback against parquet-java: high-cardinality strings and longs
 * outgrow the dictionary page size in the middle of a chunk, so pages switch to
 * PLAIN; low-cardinality columns stay dictionary-encoded. Values, encodings and
 * chunk statistics must equal parquet-java's.
 */
class DictionaryFallbackTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional binary hc (STRING);
              optional int64 big;
              optional binary lc (STRING);
              required int32 small;
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();

    private static ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(16 * 1024)
                .withDictionaryPageSize(32 * 1024)
                .build();
    }

    private record Row(String hc, Long big, String lc,
                       int small) {}

    private static List<Row> rows(int n) {
        Random r = new Random(5);
        List<Row> out = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            out.add(
                    new Row(
                            r.nextInt(20) == 0 ? null : Long.toHexString(r.nextLong()) + "-" + k,
                            r.nextInt(20) == 0 ? null : r.nextLong(),
                            r.nextInt(5) == 0 ? null : "lc" + r.nextInt(30),
                            r.nextInt(50)));
        }
        return out;
    }

    private org.apache.hadoop.fs.Path ours(List<Row> rows) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours.parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 64 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.UNCOMPRESSED), 128L << 20, Map.of())) {
            for (int from = 0; from < rows.size(); from += 3000) {
                List<Row> s = rows.subList(from,
                        Math.min(rows.size(), from + 3000));
                try (Arena a = Arena.ofConfined()) {
                    String[] hc = s.stream()
                            .map(Row::hc)
                            .toArray(String[]::new);
                    String[] lc = s.stream()
                            .map(Row::lc)
                            .toArray(String[]::new);
                    long[] big = s.stream()
                            .mapToLong(x -> x.big() == null ? 0 : x.big())
                            .toArray();
                    boolean[] bigNull = new boolean[s.size()];
                    for (int k = 0; k < s.size(); k++) {
                        bigNull[k] = s.get(k).big() == null;
                    }
                    int[] small = s.stream()
                            .mapToInt(Row::small)
                            .toArray();
                    List<VectorBuffers> cols = List.of(ArrowLayout.ofStrings(a, hc), ArrowLayout.ofLongs(a, big, bigNull), ArrowLayout.ofStrings(a, lc),
                            ArrowLayout.ofInts(a, small, null));
                    w.write(cols, s.size());
                }
            }
        } finally {
            codecs.release();
        }
        return p;
    }

    private org.apache.hadoop.fs.Path parquetJava(List<Row> rows) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("theirs.parquet")
                .toString());
        SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> w = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(p, conf))
                .withType(SCHEMA)
                .withPageSize(16 * 1024)
                .withDictionaryPageSize(32 * 1024)
                .build()) {
            for (Row r : rows) {
                Group g = f.newGroup();
                if (r.hc() != null) {
                    g.add("hc", r.hc());
                }
                if (r.big() != null) {
                    g.add("big", r.big());
                }
                if (r.lc() != null) {
                    g.add("lc", r.lc());
                }
                g.add("small", r.small());
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
    @SuppressWarnings("deprecation") // PLAIN_DICTIONARY is the name parquet-java's v1 writer records
    void fallsBackWhereParquetJavaDoes() throws Exception {
        List<Row> rows = rows(30_000);
        org.apache.hadoop.fs.Path a = ours(rows);
        org.apache.hadoop.fs.Path b = parquetJava(rows);
        assertEquals(readAll(b), readAll(a));
        ColumnChunkMetaData[] x = footer(a).getBlocks()
                .get(0)
                .getColumns()
                .toArray(new ColumnChunkMetaData[0]);
        ColumnChunkMetaData[] y = footer(b).getBlocks()
                .get(0)
                .getColumns()
                .toArray(new ColumnChunkMetaData[0]);
        for (int c = 0; c < y.length; c++) {
            String col = y[c].getPath().toDotString();
            assertEquals(y[c].getEncodings(), x[c].getEncodings(), col);
            assertEquals(y[c].getStatistics().getNumNulls(),
                    x[c].getStatistics().getNumNulls(), col);
            assertEquals(y[c].getStatistics().genericGetMin(),
                    x[c].getStatistics().genericGetMin(), col);
            assertEquals(y[c].getStatistics().genericGetMax(),
                    x[c].getStatistics().genericGetMax(), col);
        }
        // hc and big fall back to PLAIN; lc and small stay dictionary-encoded only.
        assertTrue(x[0].getEncodings().contains(Encoding.PLAIN),
                Arrays.toString(x));
        assertTrue(x[1].getEncodings().contains(Encoding.PLAIN),
                Arrays.toString(x));
        assertTrue(!x[2].getEncodings().contains(Encoding.PLAIN) && x[2].getEncodings().contains(Encoding.PLAIN_DICTIONARY));
    }
}
