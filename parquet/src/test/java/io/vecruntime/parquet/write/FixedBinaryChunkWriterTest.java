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
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.FixedBinaryVectorBuffers;
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
 * FIXED_LEN_BYTE_ARRAY (#901 item 13) on {@link FixedBinaryChunkWriter}: a
 * decimal of precision beyond 18 carried in a DECIMAL128 lane and written
 * big-endian two's-complement (signed comparator), a plain fixed binary carried
 * in a FIXED_BINARY lane (unsigned comparator), and a UUID fixed(16). The same
 * rows are written by our writer and by parquet-java's v2 record writer and
 * compared value by value and in each chunk's statistics, value count,
 * encodings and Bloom filter, with dictionary and plain pages and dictionary
 * fallback (unique values force DELTA_BYTE_ARRAY).
 */
class FixedBinaryChunkWriterTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              optional fixed_len_byte_array(16) dec (DECIMAL(38,6));
              optional fixed_len_byte_array(10) fx;
              optional fixed_len_byte_array(16) id (UUID);
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();
    private int files;

    private static final class Row {
        final BigInteger dec;
        final byte[] fx;
        final byte[] id;

        Row(BigInteger dec, byte[] fx, byte[] id) {
            this.dec = dec;
            this.fx = fx;
            this.id = id;
        }

        BigInteger dec() {
            return dec;
        }

        byte[] fx() {
            return fx;
        }

        byte[] id() {
            return id;
        }
    }

    private static List<Row> rows(int n, long seed, boolean unique) {
        Random r = new Random(seed);
        List<Row> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            BigInteger dec = r.nextInt(9) == 0 ? null : new BigInteger(120, r).subtract(BigInteger.ONE.shiftLeft(119));
            byte[] fx = r.nextInt(8) == 0 ? null : new byte[10];
            if (fx != null) {
                if (unique) {
                    r.nextBytes(fx);
                } else {
                    // A small domain, so the dictionary path and repeated values fire.
                    fx[0] = (byte) r.nextInt(6);
                    fx[9] = (byte) r.nextInt(4);
                }
            }
            byte[] id = r.nextInt(10) == 0 ? null : new byte[16];
            if (id != null) {
                r.nextBytes(id);
            }
            out.add(new Row(dec, fx, id));
        }
        return out;
    }

    private static byte[] be(BigInteger v, int length) {
        byte[] src = v.toByteArray();
        byte[] out = new byte[length];
        byte pad = (byte) (v.signum() < 0 ? -1 : 0);
        int off = length - src.length;
        for (int k = 0; k < length; k++) {
            out[k] = k < off ? pad : src[k - off];
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

    private static VectorBuffers fixedLane(Arena a, int width, List<Row> rows,
            java.util.function.Function<Row, byte[]> f) {
        int n = rows.size();
        MemorySegment validity = ArrowLayout.validityFrom(a, nulls(rows, f::apply), n);
        FixedBinaryVectorBuffers lane = FixedBinaryVectorBuffers.allocate(a, n, width, validity);
        MemorySegment data = lane.data();
        for (int i = 0; i < n; i++) {
            byte[] b = f.apply(rows.get(i));
            if (b != null) {
                MemorySegment.copy(b, 0, data, ValueLayout.JAVA_BYTE, (long) i * width,
                        width);
            }
        }
        return lane;
    }

    private static List<VectorBuffers> columns(Arena a, List<Row> rows) {
        int n = rows.size();
        BigInteger[] dec = new BigInteger[n];
        for (int k = 0; k < n; k++) {
            dec[k] = rows.get(k).dec();
        }
        return List.of(ArrowLayout.ofDecimal128(a, dec, nulls(rows, Row::dec)), fixedLane(a, 10, rows, Row::fx),
                fixedLane(a, 16, rows, Row::id));
    }

    private ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(16 * 1024)
                .withDictionaryPageSize(16 * 1024)
                .withBloomFilterEnabled(true)
                .build();
    }

    private org.apache.hadoop.fs.Path writeOurs(List<Row> rows, int batch, long rgSize) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("ours-" + (files++) + ".parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 16 * 1024);
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
                .withPageSize(16 * 1024)
                .withDictionaryPageSize(16 * 1024)
                .withBloomFilterEnabled(true)
                .build()) {
            for (Row r : rows) {
                Group g = f.newGroup();
                if (r.dec() != null) {
                    g.add("dec", Binary.fromConstantByteArray(be(r.dec(), 16)));
                }
                if (r.fx() != null) {
                    g.add("fx", Binary.fromConstantByteArray(r.fx()));
                }
                if (r.id() != null) {
                    g.add("id", Binary.fromConstantByteArray(r.id()));
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
        List<Row> rows = rows(20_000, 1, false);
        for (int batch : new int[] {1, 512, rows.size()}) {
            assertEquals(readAll(writeParquetJava(rows, 128L << 20)), readAll(writeOurs(rows, batch, 128L << 20)),
                    "batch " + batch);
        }
    }

    @Test
    void sameChunkStatisticsEncodingsAndBloom() throws Exception {
        List<Row> rows = rows(20_000, 2, false);
        org.apache.hadoop.fs.Path ours = writeOurs(rows, 4096, 128L << 20);
        org.apache.hadoop.fs.Path theirs = writeParquetJava(rows, 128L << 20);
        ParquetMetadata mo = footer(ours);
        ParquetMetadata mt = footer(theirs);
        BlockMetaData a = mo.getBlocks().get(0);
        BlockMetaData b = mt.getBlocks().get(0);
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
            assertEquals(sy.genericGetMin(), sx.genericGetMin(), col + " min");
            assertEquals(sy.genericGetMax(), sx.genericGetMax(), col + " max");
        }
        try (ParquetFileReader ra = ParquetFileReader.open(HadoopInputFile.fromPath(ours, conf));
             ParquetFileReader rb = ParquetFileReader.open(HadoopInputFile.fromPath(theirs, conf))) {
            BlockMetaData ba = ra.getFooter()
                                 .getBlocks()
                                 .get(0);
            BlockMetaData bb = rb.getFooter()
                                 .getBlocks()
                                 .get(0);
            var da = ra.getBloomFilterDataReader(ba);
            var db = rb.getBloomFilterDataReader(bb);
            for (int c = 0; c < bb.getColumns().size(); c++) {
                String col = bb.getColumns()
                               .get(c)
                               .getPath()
                               .toDotString();
                assertEquals(
                        db.readBloomFilter(bb.getColumns().get(c)),
                        da.readBloomFilter(ba.getColumns().get(c)),
                        col + ": bloom");
            }
        }
    }

    @Test
    void dictionaryFallbackToDeltaByteArray() throws Exception {
        // Unique fixed values past the dictionary page size -> DELTA_BYTE_ARRAY pages.
        List<Row> rows = rows(60_000, 3, true);
        org.apache.hadoop.fs.Path path = writeOurs(rows, 3000, 1L << 20);
        assertEquals(readAll(writeParquetJava(rows, 1L << 20)), readAll(path));
        java.util.Set<org.apache.parquet.column.Encoding> encodings = new java.util.HashSet<>();
        ParquetMetadata m = footer(path);
        for (BlockMetaData blk : m.getBlocks()) {
            for (ColumnChunkMetaData c : blk.getColumns()) {
                encodings.addAll(c.getEncodings());
            }
        }
        assertTrue(encodings.contains(org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY), encodings::toString);
        assertTrue(encodings.contains(org.apache.parquet.column.Encoding.RLE_DICTIONARY), encodings::toString);
    }
}
