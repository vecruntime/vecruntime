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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Structs, arrays and maps (#842), in the layout Spark writes them: random rows
 * with nulls at every level and empty arrays and maps, written by our writer
 * from Arrow nested lanes and by parquet-java from the same rows, read back by
 * parquet-java's reader to the same records.
 */
class NestedWriteTest {

    // Spark's layout: SparkToParquetSchemaConverter outside writeLegacyFormat.
    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional int32 id;
              optional group s {
                optional int32 a;
                optional binary b (STRING);
              }
              optional group arr (LIST) {
                repeated group list {
                  optional int64 element;
                }
              }
              optional group m (MAP) {
                repeated group key_value {
                  required binary key (STRING);
                  optional int32 value;
                }
              }
              optional group deep (LIST) {
                repeated group list {
                  optional group element {
                    optional int32 x;
                    optional group ys (LIST) {
                      repeated group list {
                        optional binary element (STRING);
                      }
                    }
                    required group r {
                      optional int64 z;
                    }
                  }
                }
              }
              required group req {
                required int32 k;
                optional group inner (LIST) {
                  repeated group list {
                    required int32 element;
                  }
                }
              }
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();
    private int files;

    // ---- random values: null, Integer, Long, String, List (an array), Map (a map), Object[] (a struct) ----

    private static Object random(Type t, Random r, int depth) {
        if (!t.isRepetition(Type.Repetition.REQUIRED) && r.nextInt(7) == 0) {
            return null;
        }
        if (t.isPrimitive()) {
            PrimitiveTypeName p = t.asPrimitiveType().getPrimitiveTypeName();
            return switch (p) {
                case INT32 -> r.nextInt(1000) - 500;
                case INT64 -> r.nextLong() % 100_000;
                case BINARY -> "v" + r.nextInt(300);
                default -> throw new IllegalArgumentException(p.toString());
            };
        }
        GroupType g = t.asGroupType();
        LogicalTypeAnnotation a = g.getLogicalTypeAnnotation();
        int n = r.nextInt(4) == 0 ? 0 : r.nextInt(depth > 1 ? 3 : 6);
        if (a instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            Type element = g.getType(0)
                            .asGroupType()
                            .getType(0);
            List<Object> out = new ArrayList<>();
            for (int k = 0; k < n; k++) {
                out.add(random(element, r, depth + 1));
            }
            return out;
        }
        if (a instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            Type value = g.getType(0)
                          .asGroupType()
                          .getType(1);
            Map<String, Object> out = new LinkedHashMap<>();
            for (int k = 0; k < n; k++) {
                out.put("k" + r.nextInt(50), random(value, r, depth + 1));
            }
            return out;
        }
        Object[] out = new Object[g.getFieldCount()];
        for (int f = 0; f < out.length; f++) {
            out[f] = random(g.getType(f), r, depth + 1);
        }
        return out;
    }

    // ---- the same values as Arrow lanes ----

    private static VectorBuffers lane(Arena a, Type t, List<Object> values) {
        int n = values.size();
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            nulls[i] = values.get(i) == null;
        }
        if (t.isPrimitive()) {
            switch (t.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32 -> {
                    int[] v = new int[n];
                    for (int i = 0; i < n; i++) {
                        v[i] = nulls[i] ? 0 : (Integer) values.get(i);
                    }
                    return ArrowLayout.ofInts(a, v, nulls);
                }
                case INT64 -> {
                    long[] v = new long[n];
                    for (int i = 0; i < n; i++) {
                        v[i] = nulls[i] ? 0 : (Long) values.get(i);
                    }
                    return ArrowLayout.ofLongs(a, v, nulls);
                }
                default -> {
                    String[] v = new String[n];
                    for (int i = 0; i < n; i++) {
                        v[i] = (String) values.get(i);
                    }
                    return ArrowLayout.ofStrings(a, v);
                }
            }
        }
        MemorySegment validity = ArrowLayout.validityFrom(a, nulls, n);
        GroupType g = t.asGroupType();
        LogicalTypeAnnotation ann = g.getLogicalTypeAnnotation();
        boolean list = ann instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
        boolean map = ann instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation;
        if (list || map) {
            GroupType repeated = g.getType(0).asGroupType();
            MemorySegment offsets = ArrowLayout.allocateOffsets(a, n);
            List<Object> keys = new ArrayList<>();
            List<Object> elems = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                offsets.set(VectorBuffers.LE_INT, (long) i << 2, elems.size());
                Object v = values.get(i);
                if (v instanceof List<?> l) {
                    elems.addAll(l);
                } else if (v instanceof Map<?, ?> m) {
                    for (Map.Entry<?, ?> e : m.entrySet()) {
                        keys.add(e.getKey());
                        elems.add(e.getValue());
                    }
                }
            }
            offsets.set(VectorBuffers.LE_INT, (long) n << 2, elems.size());
            if (list) {
                return NestedVectorBuffers.list(n, validity, offsets, lane(a, repeated.getType(0), elems));
            }
            return NestedVectorBuffers.map(n, validity, offsets, lane(a, repeated.getType(0), keys),
                    lane(a, repeated.getType(1), elems));
        }
        List<VectorBuffers> fields = new ArrayList<>();
        for (int f = 0; f < g.getFieldCount(); f++) {
            List<Object> column = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                Object v = values.get(i);
                column.add(v == null ? null : ((Object[]) v)[f]);
            }
            fields.add(lane(a, g.getType(f), column));
        }
        return NestedVectorBuffers.struct(n, validity, fields);
    }

    // ---- the same values as parquet-java groups ----

    private static void add(Group parent, Type t, Object v) {
        if (v == null) {
            return;
        }
        String name = t.getName();
        if (t.isPrimitive()) {
            switch (v) {
                case Integer i -> parent.add(name, i);
                case Long l -> parent.add(name, l);
                default -> parent.add(name, (String) v);
            }
            return;
        }
        GroupType g = t.asGroupType();
        Group group = parent.addGroup(name);
        if (v instanceof List<?> l) {
            GroupType repeated = g.getType(0).asGroupType();
            for (Object e : l) {
                add(group.addGroup(repeated.getName()), repeated.getType(0), e);
            }
        } else if (v instanceof Map<?, ?> m) {
            GroupType repeated = g.getType(0).asGroupType();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                Group kv = group.addGroup(repeated.getName());
                add(kv, repeated.getType(0), e.getKey());
                add(kv, repeated.getType(1), e.getValue());
            }
        } else {
            Object[] fields = (Object[]) v;
            for (int f = 0; f < fields.length; f++) {
                add(group, g.getType(f), fields[f]);
            }
        }
    }

    private static List<Object[]> rows(int n, long seed) {
        Random r = new Random(seed);
        List<Object[]> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Object[] row = new Object[SCHEMA.getFieldCount()];
            for (int f = 0; f < row.length; f++) {
                row[f] = random(SCHEMA.getType(f), r, 0);
            }
            out.add(row);
        }
        return out;
    }

    private static ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(8 * 1024)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Object[]> rows, int batch) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours-" + (files++) + ".parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 64 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), 64L << 10, Map.of())) {
            for (int from = 0; from < rows.size(); from += batch) {
                List<Object[]> slice = rows.subList(from,
                        Math.min(rows.size(), from + batch));
                try (Arena a = Arena.ofConfined()) {
                    List<VectorBuffers> cols = new ArrayList<>();
                    for (int f = 0; f < SCHEMA.getFieldCount(); f++) {
                        List<Object> column = new ArrayList<>(slice.size());
                        for (Object[] row : slice) {
                            column.add(row[f]);
                        }
                        cols.add(lane(a, SCHEMA.getType(f), column));
                    }
                    w.write(cols, slice.size());
                }
            }
        } finally {
            codecs.release();
        }
        return p;
    }

    private org.apache.hadoop.fs.Path writeParquetJava(List<Object[]> rows) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("parquet-java.parquet")
                .toString());
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> w = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(p, conf))
                .withType(SCHEMA)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(8 * 1024)
                .build()) {
            for (Object[] row : rows) {
                Group g = factory.newGroup();
                for (int f = 0; f < row.length; f++) {
                    add(g, SCHEMA.getType(f), row[f]);
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
        List<Object[]> rows = rows(5_000, 11);
        List<String> expected = readAll(writeParquetJava(rows));
        assertEquals(rows.size(), expected.size());
        // The rows reach every level: an element of an array inside a struct inside an array, a map entry.
        org.junit.jupiter.api.Assertions.assertTrue(expected.stream()
                .anyMatch(s -> s.contains("ys") && s.contains("element: v")));
        org.junit.jupiter.api.Assertions.assertTrue(expected.stream()
                .anyMatch(s -> s.contains("key_value")));
        for (int batch : new int[] {1, 333, rows.size()}) {
            assertEquals(expected, readAll(writeOurs(rows, batch)), "batch " + batch);
        }
    }
}
