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
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round trips through parquet-java: the same rows written by {@link
 * ColumnarParquetWriter} and by parquet-java's own {@code
 * ExampleParquetWriter}, read back with parquet-java's reader, must agree value
 * by value and in each column chunk's statistics, value count and encodings.
 * The columns are built in the engine's layout with {@link ArrowLayout},
 * including a dictionary-encoded string column (ids into a dictionary) and a
 * float column carried in a FLOAT64 lane, as the scan and the operators hand
 * them over.
 */
class ColumnarParquetWriterTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional int32 i;
              optional int64 l;
              optional double d;
              optional float f;
              optional boolean b;
              optional binary s (STRING);
              optional binary ds (STRING);
              optional int32 day (DATE);
              optional int64 dec (DECIMAL(15,2));
              optional fixed_len_byte_array(16) wide (DECIMAL(38,6));
            }
            """);

    private static final String[] DICTIONARY = dictionary();

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();

    private int files;

    private static String[] dictionary() {
        String[] d = new String[200];
        for (int k = 0; k < d.length; k++) {
            d[k] = "dict-" + k;
        }
        return d;
    }

    /**
     * One row of the test data; null fields are null. {@code ds} is an id into
     * {@link #DICTIONARY}.
     */
    private record Row(
            Integer i,
            Long l,
            Double d,
            Float f,
            Boolean b,
            String s,
            Integer ds,
            Integer day,
            Long dec,
            BigInteger wide) {}

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
                            r.nextInt(19) == 0 ? null : (float) r.nextGaussian(),
                            r.nextInt(13) == 0 ? null : r.nextBoolean(),
                            r.nextInt(11) == 0 ? null : "v" + r.nextInt(500),
                            r.nextInt(8) == 0 ? null : r.nextInt(DICTIONARY.length),
                            nulls ? null : 10_000 + r.nextInt(5000),
                            r.nextInt(7) == 0 ? null : (long) r.nextInt(1_000_000_000)
                                    * (r.nextBoolean() ? 1 : -1),
                            r.nextInt(9) == 0 ? null : new BigInteger(100, r).subtract(BigInteger.ONE.shiftLeft(99))));
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
        int[] i = new int[n];
        long[] l = new long[n];
        double[] d = new double[n];
        double[] f = new double[n];
        boolean[] b = new boolean[n];
        String[] s = new String[n];
        int[] ds = new int[n];
        int[] day = new int[n];
        long[] dec = new long[n];
        BigInteger[] wide = new BigInteger[n];
        for (int k = 0; k < n; k++) {
            Row r = rows.get(k);
            i[k] = r.i() == null ? 0 : r.i();
            l[k] = r.l() == null ? 0 : r.l();
            d[k] = r.d() == null ? 0 : r.d();
            f[k] = r.f() == null ? 0 : r.f();
            b[k] = r.b() != null && r.b();
            s[k] = r.s();
            ds[k] = r.ds() == null ? 0 : r.ds();
            day[k] = r.day() == null ? 0 : r.day();
            dec[k] = r.dec() == null ? 0 : r.dec();
            wide[k] = r.wide();
        }
        SegmentVectorBuffers ids = ArrowLayout.ofInts(a, ds, nulls(rows, Row::ds));
        VectorBuffers dict = ArrowLayout.ofStrings(a, DICTIONARY);
        VectorBuffers dsColumn = new SegmentVectorBuffers(VecType.UTF8, n, ids.validity(), ids.data(),
                null, dict);
        return List.of(
                ArrowLayout.ofInts(a, i, nulls(rows, Row::i)),
                ArrowLayout.ofLongs(a, l, nulls(rows, Row::l)),
                ArrowLayout.ofDoubles(a, d, nulls(rows, Row::d)),
                ArrowLayout.ofDoubles(a, f, nulls(rows, Row::f)),
                ArrowLayout.ofBooleans(a, b, nulls(rows, Row::b)),
                ArrowLayout.ofStrings(a, s),
                dsColumn,
                ArrowLayout.ofInts(a, day, nulls(rows, Row::day)),
                ArrowLayout.ofLongs(a, dec, nulls(rows, Row::dec)),
                ArrowLayout.ofDecimal128(a, wide, nulls(rows, Row::wide)));
    }

    private static Binary fixed(BigInteger unscaled, int length) {
        byte[] be = unscaled.toByteArray();
        byte[] out = new byte[length];
        byte pad = (byte) (unscaled.signum() < 0 ? -1 : 0);
        int off = length - be.length;
        for (int k = 0; k < length; k++) {
            out[k] = k < off ? pad : be[k - off];
        }
        return Binary.fromConstantByteArray(out);
    }

    private static ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(64 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Row> rows, int batch, long rowGroupSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours-" + (files++) + ".parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 64 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), rowGroupSize, Map.of("writer", "vecruntime"))) {
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
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("parquet-java.parquet")
                .toString());
        SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
        try (ParquetWriter<Group> w = ExampleParquetWriter.builder(HadoopOutputFile.fromPath(p, conf))
                .withType(SCHEMA)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
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
                if (r.f() != null) {
                    g.add("f", r.f());
                }
                if (r.b() != null) {
                    g.add("b", r.b());
                }
                if (r.s() != null) {
                    g.add("s", r.s());
                }
                if (r.ds() != null) {
                    g.add("ds", DICTIONARY[r.ds()]);
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

    /**
     * Hardwood (an independent reader, no parquet-java code) reads our files to
     * the written values: dictionary pages, and -- with unique strings and
     * several row groups -- the fallback encodings ({@code
     * DELTA_BINARY_PACKED}, {@code DELTA_BYTE_ARRAY}, {@code PLAIN}).
     */
    @Test
    void hardwoodReadsOurFiles() throws Exception {
        List<Row> dictionaryRows = rows(20_000, 3);
        List<Row> uniqueRows = new ArrayList<>();
        for (Row r : rows(50_000, 4)) {
            uniqueRows.add(
                    new Row(
                            r.i(),
                            r.l(),
                            r.d(),
                            r.f(),
                            r.b(),
                            r.s() == null ? null : r.s()
                                    + "-"
                                    + Integer.toHexString(r.hashCode())
                                    + "-"
                                    + uniqueRows.size(),
                            r.ds(),
                            r.day(),
                            r.dec(),
                            r.wide()));
        }
        org.apache.hadoop.fs.Path dictionaryFile = writeOurs(dictionaryRows, 4096, 128L << 20);
        org.apache.hadoop.fs.Path uniqueFile = writeOurs(uniqueRows, 3000, 1L << 20);
        assertEquals(dictionaryRows, readHardwood(dictionaryFile));
        assertEquals(uniqueRows, readHardwood(uniqueFile));
        java.util.Set<org.apache.parquet.column.Encoding> encodings = new java.util.HashSet<>();
        ParquetMetadata f = footer(uniqueFile);
        assertTrue(f.getBlocks().size() > 1, "several row groups");
        for (BlockMetaData b : f.getBlocks()) {
            for (ColumnChunkMetaData c : b.getColumns()) {
                encodings.addAll(c.getEncodings());
            }
        }
        assertTrue(encodings.contains(org.apache.parquet.column.Encoding.DELTA_BINARY_PACKED), encodings::toString);
        assertTrue(encodings.contains(org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY), encodings::toString);
        assertTrue(encodings.contains(org.apache.parquet.column.Encoding.RLE_DICTIONARY), encodings::toString);
    }

    /**
     * The rows of {@code p} as Hardwood reads them, back in the shape of {@link
     * Row}.
     */
    private static List<Row> readHardwood(org.apache.hadoop.fs.Path p) throws Exception {
        List<Row> out = new ArrayList<>();
        try (dev.hardwood.reader.ParquetFileReader file = dev.hardwood.reader.ParquetFileReader.open(dev.hardwood.InputFile.of(Path.of(p.toUri()
                     .getPath())));
             dev.hardwood.reader.RowReader r = file.rowReader()) {
            while (r.hasNext()) {
                r.next();
                String ds = r.isNull(6) ? null : r.getString(6);
                out.add(
                        new Row(
                                r.isNull(0) ? null : r.getInt(0),
                                r.isNull(1) ? null : r.getLong(1),
                                r.isNull(2) ? null : r.getDouble(2),
                                r.isNull(3) ? null : r.getFloat(3),
                                r.isNull(4) ? null : r.getBoolean(4),
                                r.isNull(5) ? null : r.getString(5),
                                ds == null ? null : java.util.Arrays.asList(DICTIONARY).indexOf(ds),
                                r.isNull(7) ? null : (Integer) r.getRawValue(7),
                                r.isNull(8) ? null : (Long) r.getRawValue(8),
                                r.isNull(9) ? null : new BigInteger((byte[]) r.getRawValue(9))));
            }
        }
        return out;
    }

    private static String d(org.apache.parquet.column.statistics.SizeStatistics s) {
        return s == null ? "null" : s.getUnencodedByteArrayDataBytes() + " rep=" + s.getRepetitionLevelHistogram() + " def=" + s.getDefinitionLevelHistogram();
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
            assertEquals(d(y.getSizeStatistics()), d(x.getSizeStatistics()), col + ": size statistics");
        }
        assertEquals("vecruntime",
                ours.getFileMetaData()
                    .getKeyValueMetaData()
                    .get("writer"));
    }

    @Test
    void rowGroupsAreCutByBufferedSize() throws Exception {
        List<Row> rows = rows(60_000, 3);
        org.apache.hadoop.fs.Path path = writeOurs(rows, 2048, 256 * 1024);
        ParquetMetadata ours = footer(path);
        assertTrue(ours.getBlocks().size() > 1, "expected several row groups, got " + ours.getBlocks().size());
        long total = ours.getBlocks().stream()
                .mapToLong(BlockMetaData::getRowCount)
                .sum();
        assertEquals(rows.size(), total);
        assertEquals(readAll(writeParquetJava(rows, 128L << 20)), readAll(path));
    }

    @Test
    void emptyFileHasSchemaOnly() throws Exception {
        ParquetMetadata ours = footer(writeOurs(List.of(), 1024, 128L << 20));
        assertEquals(0, ours.getBlocks()
                            .size());
        assertEquals(SCHEMA, ours.getFileMetaData()
                .getSchema());
    }

    @Test
    void batchSizeDoesNotChangeTheValues() throws Exception {
        List<Row> rows = rows(9_999, 4);
        List<String> a = readAll(writeOurs(rows, 1, 128L << 20));
        List<String> b = readAll(writeOurs(rows, 777, 128L << 20));
        List<String> c = readAll(writeOurs(rows, 9_999, 128L << 20));
        assertEquals(a, b);
        assertEquals(a, c);
        assertEquals(rows.size(), a.size());
    }

    /**
     * Runs of repeated values (a ticket's rows, a sorted key), with nulls
     * inside the runs and runs that span batch and page boundaries: the
     * dictionary ids the writer reuses for a repeated neighbour give the values
     * parquet-java writes.
     */
    @Test
    void repeatedRunsAcrossBatchesMatchParquetJava() throws Exception {
        List<Row> base = rows(3_000, 5);
        Random r = new Random(6);
        List<Row> rows = new ArrayList<>();
        for (Row b : base) {
            int run = 1 + r.nextInt(r.nextBoolean() ? 3 : 40);
            for (int k = 0; k < run; k++) {
                boolean hole = r.nextInt(6) == 0;
                rows.add(hole ? new Row(null, null, null, null, null, null,
                        null, null, null, null)
                        : b);
            }
        }
        org.apache.hadoop.fs.Path theirs = writeParquetJava(rows, 128L << 20);
        List<String> expected = readAll(theirs);
        for (int batch : new int[] {1, 7, 1000, rows.size()}) {
            assertEquals(expected, readAll(writeOurs(rows, batch, 128L << 20)), "batch " + batch);
        }
    }
}
