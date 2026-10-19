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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round trips through parquet-java: the same rows written by {@link
 * ArrowParquetWriter} and by parquet-java's own {@code ExampleParquetWriter},
 * read back with parquet-java's reader, must agree value by value and in each
 * column chunk's statistics, value count and encodings.
 */
class ArrowParquetWriterTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional int32 i;
              optional int64 l;
              optional double d;
              optional boolean b;
              optional binary s (STRING);
              optional int32 day (DATE);
              optional int64 dec (DECIMAL(15,2));
              optional fixed_len_byte_array(16) wide (DECIMAL(38,6));
            }
            """);

    @TempDir
    Path dir;

    private BufferAllocator allocator;
    private final Configuration conf = new Configuration();

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    /** One row of the test data; null fields are null. */
    private record Row(
            Integer i,
            Long l,
            Double d,
            Boolean b,
            String s,
            Integer day,
            Long dec,
            BigDecimal wide) {}

    private static List<Row> rows(int n, long seed) {
        Random r = new Random(seed);
        List<Row> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            boolean nulls = r.nextInt(10) == 0;
            out.add(
                    new Row(
                            nulls ? null : r.nextInt(),
                            nulls ? null : r.nextLong(),
                            r.nextInt(17) == 0 ? null : r.nextGaussian(),
                            r.nextInt(13) == 0 ? null : r.nextBoolean(),
                            r.nextInt(11) == 0 ? null : "v" + r.nextInt(500), // few distinct: dictionary-encoded
                            nulls ? null : 10_000 + r.nextInt(5000),
                            r.nextInt(7) == 0 ? null : (long) r.nextInt(1_000_000_000)
                                    * (r.nextBoolean() ? 1 : -1),
                            r.nextInt(9) == 0 ? null : new BigDecimal(new java.math.BigInteger(100, r).subtract(java.math.BigInteger.ONE.shiftLeft(99)), 6)));
        }
        return out;
    }

    private List<FieldVector> vectors(List<Row> rows) {
        int n = rows.size();
        IntVector i = new IntVector("i", allocator);
        BigIntVector l = new BigIntVector("l", allocator);
        Float8Vector d = new Float8Vector("d", allocator);
        BitVector b = new BitVector("b", allocator);
        VarCharVector s = new VarCharVector("s", allocator);
        DateDayVector day = new DateDayVector("day", allocator);
        DecimalVector dec = new DecimalVector("dec", allocator, 15, 2);
        DecimalVector wide = new DecimalVector("wide", allocator, 38, 6);
        List<FieldVector> all = List.of(i, l, d, b, s, day,
                dec, wide);
        all.forEach(FieldVector::allocateNew);
        for (int k = 0; k < n; k++) {
            Row row = rows.get(k);
            if (row.i() != null) {
                i.setSafe(k, row.i());
            } else {
                i.setNull(k);
            }
            if (row.l() != null) {
                l.setSafe(k, row.l());
            } else {
                l.setNull(k);
            }
            if (row.d() != null) {
                d.setSafe(k, row.d());
            } else {
                d.setNull(k);
            }
            if (row.b() != null) {
                b.setSafe(k, row.b() ? 1 : 0);
            } else {
                b.setNull(k);
            }
            if (row.s() != null) {
                s.setSafe(k, row.s()
                                .getBytes(StandardCharsets.UTF_8));
            } else {
                s.setNull(k);
            }
            if (row.day() != null) {
                day.setSafe(k, row.day());
            } else {
                day.setNull(k);
            }
            if (row.dec() != null) {
                dec.setSafe(k, BigDecimal.valueOf(row.dec(), 2));
            } else {
                dec.setNull(k);
            }
            if (row.wide() != null) {
                wide.setSafe(k, row.wide());
            } else {
                wide.setNull(k);
            }
        }
        all.forEach(v -> v.setValueCount(n));
        return all;
    }

    private static Binary fixed(BigDecimal v, int length) {
        byte[] be = v.unscaledValue().toByteArray();
        byte[] out = new byte[length];
        byte pad = (byte) (v.signum() < 0 ? -1 : 0);
        int off = length - be.length;
        for (int k = 0; k < length; k++) {
            out[k] = k < off ? pad : be[k - off];
        }
        return Binary.fromConstantByteArray(out);
    }

    private ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(64 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Row> rows, int batch, long rowGroupSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours.parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 64 * 1024);
        try (ArrowParquetWriter w = new ArrowParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), rowGroupSize, Map.of("writer", "vecruntime"))) {
            for (int from = 0; from < rows.size(); from += batch) {
                List<Row> slice = rows.subList(from,
                        Math.min(rows.size(), from + batch));
                List<FieldVector> vs = vectors(slice);
                try {
                    w.write(vs, slice.size());
                } finally {
                    vs.forEach(FieldVector::close);
                }
            }
        } finally {
            codecs.release();
        }
        return p;
    }

    private org.apache.hadoop.fs.Path writeParquetJava(List<Row> rows, long rowGroupSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("parquet-java.parquet")
                .toString());
        SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> w = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(p, conf))
                .withType(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(64 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .build()) {
            for (Row r : rows) {
                Group g = f.newGroup();
                if (r.i() != null) {
                    g.add("i", r.i());
                }
                if (r.l() != null) {
                    g.add("l", r.l());
                }
                if (r.d() != null) {
                    g.add("d", r.d());
                }
                if (r.b() != null) {
                    g.add("b", r.b());
                }
                if (r.s() != null) {
                    g.add("s", r.s());
                }
                if (r.day() != null) {
                    g.add("day", r.day());
                }
                if (r.dec() != null) {
                    g.add("dec", r.dec());
                }
                if (r.wide() != null) {
                    g.add("wide", fixed(r.wide(), 16));
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
        org.apache.hadoop.fs.Path ours = writeOurs(rows, 4096, 128L << 20);
        org.apache.hadoop.fs.Path theirs = writeParquetJava(rows, 128L << 20);
        assertEquals(readAll(theirs), readAll(ours));
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
        assertEquals("vecruntime",
                ours.getFileMetaData()
                    .getKeyValueMetaData()
                    .get("writer"));
    }

    @Test
    void rowGroupsAreCutByBufferedSize() throws Exception {
        List<Row> rows = rows(60_000, 3);
        ParquetMetadata ours = footer(writeOurs(rows, 2048, 256 * 1024));
        assertTrue(ours.getBlocks().size() > 1, "expected several row groups, got " + ours.getBlocks().size());
        long total = ours.getBlocks().stream()
                .mapToLong(BlockMetaData::getRowCount)
                .sum();
        assertEquals(rows.size(), total);
    }

    @Test
    void emptyFileHasSchemaOnly() throws Exception {
        ParquetMetadata ours = footer(writeOurs(List.of(), 1024, 128L << 20));
        assertEquals(0, ours.getBlocks()
                            .size());
        assertEquals(SCHEMA, ours.getFileMetaData()
                .getSchema());
    }
}
