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
import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.NestedVectorBuffers;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nested leaves of every remaining primitive type (#901 item 13): a list of
 * doubles, a list of booleans, a struct of a float and a fixed decimal(38,6),
 * and a map of string to long. Random rows with nulls at every level and empty
 * lists and maps are written by our writer (through NestedLeafWriter) and by
 * parquet-java, read back by parquet-java's reader to the same records -- so the
 * repetition and definition levels and the per-type value encodings our leaves
 * write are correct for a standard reader.
 */
class NestedLeafTypesTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional group ds (LIST) {
                repeated group list {
                  optional double element;
                }
              }
              optional group bs (LIST) {
                repeated group list {
                  optional boolean element;
                }
              }
              optional group s {
                optional float f;
                optional fixed_len_byte_array(16) dec (DECIMAL(38,6));
              }
              optional group m (MAP) {
                repeated group key_value {
                  required binary key (STRING);
                  optional int64 value;
                }
              }
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();
    private int files;

    // A row: doubles, booleans, {Float f, BigInteger dec}, Map<String,Long>. null = absent.
    private static final class Row {
        List<Double> ds;
        List<Boolean> bs;
        Float f;
        BigInteger dec;
        Map<String, Long> m;
    }

    private static List<Row> rows(int n, long seed) {
        Random r = new Random(seed);
        List<Row> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            Row row = new Row();
            if (r.nextInt(6) != 0) {
                row.ds = new ArrayList<>();
                int c = r.nextInt(4);
                for (int j = 0; j < c; j++) {
                    row.ds.add(r.nextInt(5) == 0 ? null : r.nextGaussian());
                }
            }
            if (r.nextInt(6) != 0) {
                row.bs = new ArrayList<>();
                int c = r.nextInt(4);
                for (int j = 0; j < c; j++) {
                    row.bs.add(r.nextInt(5) == 0 ? null : r.nextBoolean());
                }
            }
            if (r.nextInt(5) != 0) {
                row.f = r.nextInt(4) == 0 ? null : (float) r.nextGaussian();
                row.dec = r.nextInt(4) == 0 ? null : new BigInteger(90, r).subtract(BigInteger.ONE.shiftLeft(89));
            }
            if (r.nextInt(6) != 0) {
                row.m = new java.util.LinkedHashMap<>();
                int c = r.nextInt(4);
                for (int j = 0; j < c; j++) {
                    row.m.put("k" + r.nextInt(40),
                            r.nextInt(5) == 0 ? null : r.nextLong() % 1_000_000);
                }
            }
            out.add(row);
        }
        return out;
    }

    private static byte[] be(BigInteger v, int length) {
        byte[] s = v.toByteArray();
        byte[] o = new byte[length];
        byte pad = (byte) (v.signum() < 0 ? -1 : 0);
        int off = length - s.length;
        for (int k = 0; k < length; k++) {
            o[k] = k < off ? pad : s[k - off];
        }
        return o;
    }

    // ---- our lanes ----

    private static MemorySegment validity(Arena a, boolean[] present) {
        boolean[] nulls = new boolean[present.length];
        for (int i = 0; i < present.length; i++) {
            nulls[i] = !present[i];
        }
        return ArrowLayout.validityFrom(a, nulls, present.length);
    }

    private static VectorBuffers listOfDoubles(Arena a, List<Row> rows) {
        int n = rows.size();
        boolean[] present = new boolean[n];
        MemorySegment offsets = ArrowLayout.allocateOffsets(a, n);
        List<Double> elems = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            offsets.set(VectorBuffers.LE_INT, (long) i << 2, elems.size());
            present[i] = rows.get(i).ds != null;
            if (present[i]) {
                elems.addAll(rows.get(i).ds);
            }
        }
        offsets.set(VectorBuffers.LE_INT, (long) n << 2, elems.size());
        double[] v = new double[elems.size()];
        boolean[] en = new boolean[elems.size()];
        for (int i = 0; i < elems.size(); i++) {
            en[i] = elems.get(i) == null;
            v[i] = en[i] ? 0 : elems.get(i);
        }
        return NestedVectorBuffers.list(n, validity(a, present), offsets, ArrowLayout.ofDoubles(a, v, en));
    }

    private static VectorBuffers listOfBooleans(Arena a, List<Row> rows) {
        int n = rows.size();
        boolean[] present = new boolean[n];
        MemorySegment offsets = ArrowLayout.allocateOffsets(a, n);
        List<Boolean> elems = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            offsets.set(VectorBuffers.LE_INT, (long) i << 2, elems.size());
            present[i] = rows.get(i).bs != null;
            if (present[i]) {
                elems.addAll(rows.get(i).bs);
            }
        }
        offsets.set(VectorBuffers.LE_INT, (long) n << 2, elems.size());
        boolean[] v = new boolean[elems.size()];
        boolean[] en = new boolean[elems.size()];
        for (int i = 0; i < elems.size(); i++) {
            en[i] = elems.get(i) == null;
            v[i] = !en[i] && elems.get(i);
        }
        return NestedVectorBuffers.list(n, validity(a, present), offsets, ArrowLayout.ofBooleans(a, v, en));
    }

    private static VectorBuffers structFloatDec(Arena a, List<Row> rows) {
        int n = rows.size();
        boolean[] present = new boolean[n];
        double[] f = new double[n];
        boolean[] fn = new boolean[n];
        BigInteger[] dec = new BigInteger[n];
        boolean[] dn = new boolean[n];
        for (int i = 0; i < n; i++) {
            Row row = rows.get(i);
            present[i] = row.f != null || row.dec != null || isStructPresent(row);
            fn[i] = row.f == null;
            f[i] = fn[i] ? 0 : row.f;
            dn[i] = row.dec == null;
            dec[i] = row.dec;
        }
        List<VectorBuffers> fields = List.of(ArrowLayout.ofDoubles(a, f, fn), ArrowLayout.ofDecimal128(a, dec, dn));
        return NestedVectorBuffers.struct(n, validity(a, present), fields);
    }

    // The struct is present when the row set either field (the test marks it present iff it populated f/dec).
    private static boolean isStructPresent(Row row) {
        return row.f != null || row.dec != null;
    }

    private static VectorBuffers mapStringLong(Arena a, List<Row> rows) {
        int n = rows.size();
        boolean[] present = new boolean[n];
        MemorySegment offsets = ArrowLayout.allocateOffsets(a, n);
        List<String> keys = new ArrayList<>();
        List<Long> vals = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            offsets.set(VectorBuffers.LE_INT, (long) i << 2, keys.size());
            present[i] = rows.get(i).m != null;
            if (present[i]) {
                for (Map.Entry<String, Long> e : rows.get(i).m.entrySet()) {
                    keys.add(e.getKey());
                    vals.add(e.getValue());
                }
            }
        }
        offsets.set(VectorBuffers.LE_INT, (long) n << 2, keys.size());
        long[] lv = new long[vals.size()];
        boolean[] ln = new boolean[vals.size()];
        for (int i = 0; i < vals.size(); i++) {
            ln[i] = vals.get(i) == null;
            lv[i] = ln[i] ? 0 : vals.get(i);
        }
        return NestedVectorBuffers.map(n, validity(a, present), offsets,
                ArrowLayout.ofStrings(a, keys.toArray(new String[0])), ArrowLayout.ofLongs(a, lv, ln));
    }

    private static List<VectorBuffers> columns(Arena a, List<Row> rows) {
        return List.of(listOfDoubles(a, rows), listOfBooleans(a, rows), structFloatDec(a, rows),
                mapStringLong(a, rows));
    }

    private ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(8 * 1024)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Row> rows, int batch) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours-" + (files++) + ".parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 8 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), 64L << 10, Map.of())) {
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

    private org.apache.hadoop.fs.Path writeParquetJava(List<Row> rows) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("parquet-java-" + (files++) + ".parquet")
                .toString());
        SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> w = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(p, conf))
                .withType(SCHEMA)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(8 * 1024)
                .build()) {
            for (Row row : rows) {
                Group g = f.newGroup();
                if (row.ds != null) {
                    Group lst = g.addGroup("ds");
                    for (Double d : row.ds) {
                        Group e = lst.addGroup("list");
                        if (d != null) {
                            e.add("element", d);
                        }
                    }
                }
                if (row.bs != null) {
                    Group lst = g.addGroup("bs");
                    for (Boolean b : row.bs) {
                        Group e = lst.addGroup("list");
                        if (b != null) {
                            e.add("element", b);
                        }
                    }
                }
                if (row.f != null || row.dec != null) {
                    Group st = g.addGroup("s");
                    if (row.f != null) {
                        st.add("f", row.f);
                    }
                    if (row.dec != null) {
                        st.add("dec", Binary.fromConstantByteArray(be(row.dec, 16)));
                    }
                }
                if (row.m != null) {
                    Group mp = g.addGroup("m");
                    for (Map.Entry<String, Long> e : row.m.entrySet()) {
                        Group kv = mp.addGroup("key_value");
                        kv.add("key", e.getKey());
                        if (e.getValue() != null) {
                            kv.add("value", e.getValue());
                        }
                    }
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

    @Test
    void sameRecordsAsParquetJava() throws Exception {
        List<Row> rows = rows(6_000, 21);
        List<String> expected = readAll(writeParquetJava(rows));
        assertEquals(rows.size(), expected.size());
        assertTrue(expected.stream().anyMatch(s -> s.contains("element") && s.contains("E")),
                "has doubles");
        assertTrue(expected.stream()
                .anyMatch(s -> s.contains("key_value")),
                "has map entries");
        for (int batch : new int[] {1, 257, rows.size()}) {
            assertEquals(expected, readAll(writeOurs(rows, batch)), "batch " + batch);
        }
    }
}
