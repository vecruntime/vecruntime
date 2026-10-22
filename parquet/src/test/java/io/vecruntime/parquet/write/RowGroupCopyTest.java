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

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ColumnarParquetWriter#copyRowGroup}, the M4a primitive (#901): a row
 * group with no deleted rows is copied byte for byte into the new file instead
 * of being decoded and re-encoded. A copy-on-write {@code DELETE} rewrite is
 * modelled here at the row-group level -- a source file of several groups is
 * rewritten into a new file, copying the groups a delete did not touch and
 * decoding and re-encoding only the group it did.
 *
 * <p>The checks are the ones the write path relies on: the new file's rows and
 * order equal the surviving rows, a copied group's footer statistics are the
 * source's byte for byte (so Iceberg's footer metrics come out the same as a
 * full rewrite would produce), the copied groups were really copied (their
 * chunk byte sizes equal the source's, where a re-encode would differ), and a
 * file whose schema differs is reported not copyable so the caller falls back.
 */
class RowGroupCopyTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message table {
              optional int32 id = 1;
              optional binary name (STRING) = 2;
            }
            """);

    @TempDir
    Path dir;

    private final Configuration conf = new Configuration();

    private ParquetProperties props() {
        return ParquetProperties.builder()
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(4 * 1024)
                .build();
    }

    private static List<VectorBuffers> columns(Arena a, int[] id, String[] name) {
        boolean[] noNulls = new boolean[id.length];
        return List.of(ArrowLayout.ofInts(a, id, noNulls), ArrowLayout.ofStrings(a, name));
    }

    /** Writes rows [first, end) of the generated data into {@code w} as one call. */
    private void writeRange(ColumnarParquetWriter w, int first, int end) throws Exception {
        int n = end - first;
        int[] id = new int[n];
        String[] name = new String[n];
        for (int k = 0; k < n; k++) {
            id[k] = first + k;
            name[k] = "row-" + (first + k);
        }
        try (Arena a = Arena.ofConfined()) {
            w.write(columns(a, id, name), 0, n);
        }
    }

    /**
     * A source file of {@code groups} row groups of {@code perGroup} rows each,
     * cut by a small row-group size so each write-range becomes its own group.
     */
    private org.apache.hadoop.fs.Path writeSource(int groups, int perGroup) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("source.parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 4 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(p, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), 1L << 20, Map.of("writer", "vecruntime"))) {
            for (int g = 0; g < groups; g++) {
                writeRange(w, g * perGroup, (g + 1) * perGroup);
                // Force the open group to flush, so the file has exactly `groups` groups of `perGroup` rows.
                w.endRowGroup();
            }
        } finally {
            codecs.release();
        }
        return p;
    }

    private ParquetMetadata footer(org.apache.hadoop.fs.Path p) throws Exception {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, conf))) {
            return r.getFooter();
        }
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

    /**
     * A copy-on-write DELETE modelled at the row-group level: group {@code
     * deletedGroup} has a deleted row (decoded, filtered, re-encoded), every
     * other group has none (copied byte for byte). {@code deletedGroup = -1}
     * copies all groups (DELETE touches none); {@code deletedGroup} in every
     * group would re-encode all.
     */
    private void rewrite(int groups, int perGroup, int deletedGroup,
                         int deletedRowInGroup)
            throws Exception {
        org.apache.hadoop.fs.Path source = writeSource(groups, perGroup);
        ParquetMetadata srcFooter = footer(source);
        assertEquals(groups, srcFooter.getBlocks().size(),
                "source groups");

        org.apache.hadoop.fs.Path out = new org.apache.hadoop.fs.Path(dir.resolve("rewritten.parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 4 * 1024);
        int copied = 0;
        List<String> expected = new ArrayList<>();
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(out, conf), SCHEMA, props(),
                     codecs.getCompressor(CompressionCodecName.SNAPPY), 1L << 20, Map.of("writer", "vecruntime"));
             SeekableInputStream in = HadoopInputFile.fromPath(source, conf).newStream()) {
            MessageType fileSchema = srcFooter.getFileMetaData().getSchema();
            assertTrue(w.copyableFrom(fileSchema), "same-schema file is copyable");
            for (int g = 0; g < groups; g++) {
                if (g == deletedGroup) {
                    // Decode, filter (drop one row), re-encode this group through the columnar writer.
                    int first = g * perGroup;
                    int[] id = new int[perGroup - 1];
                    String[] name = new String[perGroup - 1];
                    int k = 0;
                    for (int r = 0; r < perGroup; r++) {
                        if (r == deletedRowInGroup) {
                            continue;
                        }
                        id[k] = first + r;
                        name[k] = "row-" + (first + r);
                        expected.add(groupString(id[k], name[k]));
                        k++;
                    }
                    try (Arena a = Arena.ofConfined()) {
                        w.write(columns(a, id, name), 0, id.length);
                    }
                    w.endRowGroup();
                } else {
                    w.copyRowGroup(in, srcFooter.getBlocks()
                            .get(g));
                    copied++;
                    for (int r = 0; r < perGroup; r++) {
                        expected.add(groupString(g * perGroup + r, "row-" + (g * perGroup + r)));
                    }
                }
            }
        } finally {
            codecs.release();
        }

        // (1) rows and order equal the surviving rows.
        assertEquals(expected, readAll(out));

        ParquetMetadata outFooter = footer(out);
        // (4) the copied groups were really copied: a copied group's chunk byte sizes equal the source's.
        int copiedSeen = 0;
        int outGroup = 0;
        for (int g = 0; g < groups; g++) {
            if (g == deletedGroup) {
                outGroup++; // the re-encoded group occupies one output group too
                continue;
            }
            BlockMetaData src = srcFooter.getBlocks().get(g);
            BlockMetaData dst = outFooter.getBlocks().get(outGroup++);
            assertEquals(src.getRowCount(), dst.getRowCount(), "copied group row count");
            for (int c = 0; c < src.getColumns().size(); c++) {
                ColumnChunkMetaData sx = src.getColumns().get(c);
                ColumnChunkMetaData dx = dst.getColumns().get(c);
                String col = sx.getPath().toDotString();
                // (3) copied bytes: total compressed size identical (a re-encode would almost never match exactly).
                assertEquals(sx.getTotalSize(), dx.getTotalSize(), col + ": copied chunk compressed size");
                assertEquals(sx.getTotalUncompressedSize(), dx.getTotalUncompressedSize(), col + ": uncompressed");
                assertEquals(sx.getEncodings(), dx.getEncodings(), col + ": encodings");
                // (2) copied statistics preserved byte for byte -> footer metrics match a full rewrite.
                Statistics<?> ss = sx.getStatistics();
                Statistics<?> ds = dx.getStatistics();
                assertEquals(ss.getNumNulls(), ds.getNumNulls(), col + ": nulls");
                assertEquals(ss.genericGetMin(), ds.genericGetMin(), col + ": min");
                assertEquals(ss.genericGetMax(), ds.genericGetMax(), col + ": max");
            }
            copiedSeen++;
        }
        assertEquals(copied, copiedSeen, "number of copied groups");
    }

    private static String groupString(int id, String name) {
        return "id: " + id + "\nname: " + name + "\n";
    }

    /** DELETE touches no group: every group is copied (the cheapest case). */
    @Test
    void copyAllGroups() throws Exception {
        rewrite(4, 500, -1, -1);
    }

    /**
     * DELETE touches one group of several: that one is re-encoded, the rest
     * copied.
     */
    @Test
    void copySomeGroups() throws Exception {
        rewrite(4, 500, 2, 123);
    }

    /**
     * DELETE touches the only group: nothing is copied (equivalent to a full
     * rewrite).
     */
    @Test
    void copyNoGroups() throws Exception {
        rewrite(1, 500, 0, 7);
    }

    /**
     * A file whose schema differs (a dropped column) is not copyable -> caller
     * falls back.
     */
    @Test
    void schemaEvolvedFileIsNotCopyable() throws Exception {
        org.apache.hadoop.fs.Path out = new org.apache.hadoop.fs.Path(dir.resolve("probe.parquet")
                .toString());
        CodecFactory codecs = new CodecFactory(conf, 4 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(out, conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), 1L << 20, Map.of())) {
            // A file with an extra/renamed/dropped column, or different field ids, must not be copyable.
            MessageType evolvedDropped = MessageTypeParser.parseMessageType("message table { optional int32 id = 1; }");
            MessageType evolvedRenamed = MessageTypeParser.parseMessageType("message table { optional int32 id = 1; optional binary label (STRING) = 2; }");
            MessageType evolvedFieldId = MessageTypeParser.parseMessageType("message table { optional int32 id = 1; optional binary name (STRING) = 7; }");
            assertFalse(w.copyableFrom(evolvedDropped), "dropped column");
            assertFalse(w.copyableFrom(evolvedRenamed), "renamed column");
            assertFalse(w.copyableFrom(evolvedFieldId), "different field id");
            assertTrue(w.copyableFrom(SCHEMA), "identical schema");
        } finally {
            codecs.release();
        }
    }

    /**
     * Benchmark smoke (not an assertion on time): a copy-on-write DELETE
     * touching one row group of {@code N}, done two ways -- the full rewrite
     * (decode and re-encode every group) and M4a (copy {@code N-1} groups, only
     * re-encode the touched one). Prints both wall times; the M4a path should
     * be the faster, and the point here is only to show the shape, with the 1
     * TB A/B left to the cluster. A single modest warm iteration keeps it
     * inside the unit suite.
     */
    @Test
    void benchmarkSmokeFullRewriteVsCopy() throws Exception {
        int groups = 8;
        int perGroup = 2000;
        org.apache.hadoop.fs.Path source = writeSource(groups, perGroup);
        ParquetMetadata srcFooter = footer(source);
        int deletedGroup = 3;
        int deletedRow = 111;

        long full = time(() -> fullRewrite(srcFooter, deletedGroup, deletedRow, "full"));
        long m4a = time(() -> m4aRewrite(source, srcFooter, deletedGroup, deletedRow, "m4a"));
        System.out.println("M4a benchmark smoke (DELETE touches 1 of "
                + groups
                + " groups, "
                + perGroup
                + " rows each): full rewrite "
                + full / 1_000_000.0
                + " ms, M4a copy "
                + m4a / 1_000_000.0
                + " ms");

        // Both produce the same rows, so correctness is not traded for speed.
        assertEquals(readAll(rewriteOut("full")), readAll(rewriteOut("m4a")));
    }

    private interface Work {
        void run() throws Exception;
    }

    private static long time(Work w) throws Exception {
        long t0 = System.nanoTime();
        w.run();
        return System.nanoTime() - t0;
    }

    private org.apache.hadoop.fs.Path rewriteOut(String tag) {
        return new org.apache.hadoop.fs.Path(dir.resolve("bench-" + tag + ".parquet")
                .toString());
    }

    /**
     * Full rewrite: decode and re-encode every group, dropping the one deleted
     * row.
     */
    private void fullRewrite(ParquetMetadata srcFooter, int deletedGroup, int deletedRow,
            String tag)
            throws Exception {
        CodecFactory codecs = new CodecFactory(conf, 4 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(rewriteOut(tag), conf), SCHEMA, props(),
                codecs.getCompressor(CompressionCodecName.SNAPPY), 1L << 20, Map.of())) {
            int groups = srcFooter.getBlocks().size();
            int perGroup = (int) srcFooter.getBlocks()
                    .get(0)
                    .getRowCount();
            for (int g = 0; g < groups; g++) {
                int drop = g == deletedGroup ? deletedRow : -1;
                reencodeGroup(w, g, perGroup, drop);
                w.endRowGroup();
            }
        } finally {
            codecs.release();
        }
    }

    /**
     * M4a: copy every delete-free group byte for byte, only re-encode the
     * touched one.
     */
    private void m4aRewrite(org.apache.hadoop.fs.Path source, ParquetMetadata srcFooter, int deletedGroup,
                            int deletedRow, String tag)
            throws Exception {
        CodecFactory codecs = new CodecFactory(conf, 4 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(HadoopOutputFile.fromPath(rewriteOut(tag), conf), SCHEMA, props(),
                     codecs.getCompressor(CompressionCodecName.SNAPPY), 1L << 20, Map.of());
             SeekableInputStream in = HadoopInputFile.fromPath(source, conf).newStream()) {
            int groups = srcFooter.getBlocks().size();
            int perGroup = (int) srcFooter.getBlocks()
                    .get(0)
                    .getRowCount();
            for (int g = 0; g < groups; g++) {
                if (g == deletedGroup) {
                    reencodeGroup(w, g, perGroup, deletedRow);
                    w.endRowGroup();
                } else {
                    w.copyRowGroup(in, srcFooter.getBlocks()
                            .get(g));
                }
            }
        } finally {
            codecs.release();
        }
    }

    /**
     * Re-encodes group {@code g}'s surviving rows (dropping row {@code drop},
     * or none when -1).
     */
    private void reencodeGroup(ColumnarParquetWriter w, int g, int perGroup,
            int drop)
            throws Exception {
        int first = g * perGroup;
        int keep = drop < 0 ? perGroup : perGroup - 1;
        int[] id = new int[keep];
        String[] name = new String[keep];
        int k = 0;
        for (int r = 0; r < perGroup; r++) {
            if (r == drop) {
                continue;
            }
            id[k] = first + r;
            name[k] = "row-" + (first + r);
            k++;
        }
        try (Arena a = Arena.ofConfined()) {
            w.write(columns(a, id, name), 0, keep);
        }
    }
}
