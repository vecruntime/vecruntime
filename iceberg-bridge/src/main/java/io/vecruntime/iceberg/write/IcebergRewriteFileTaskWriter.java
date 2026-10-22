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
package io.vecruntime.iceberg.write;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

/**
 * One task's output for a copy-on-write {@code DELETE} rewritten with row-group
 * copies (#901 M4a). The write's input is the surviving rows of the files the
 * command rewrites, clustered by {@code _file} (Iceberg's copy-on-write
 * distribution), each row carrying its source file path ({@code _file}) and its
 * position in that file ({@code _pos}) alongside the data columns.
 *
 * <p>Rows are buffered per source file. On {@link #close}, each source file is
 * rewritten into a new data file:
 *
 * <ul>
 *   <li>its footer is read and {@link RowGroupCopyPlanner} decides, per row
 *       group, whether it survives whole (copy its bytes), partly (re-encode
 *       its surviving rows, in {@code _pos} order) or not at all (skip);
 *   <li>a group with no deleted rows is copied byte for byte with {@link
 *       ColumnarParquetWriter#copyRowGroup}; the rest go through the columnar
 *       writer, which also carries the v3 lineage columns of the re-encoded
 *       rows;
 *   <li>a file the planner rejects (schema evolution, or a tracked float/double
 *       column whose NaN count a copy cannot supply) is rewritten in full by
 *       re-encoding every surviving row, exactly as the streaming writer would.
 * </ul>
 *
 * <p>The new file's metrics come from {@link IcebergParquetAppender}, i.e. the
 * combined footer the writer wrote (copied groups' statistics included) plus
 * the re-encoded rows' float/double metrics -- the same path the full rewrite
 * uses, so a copy and a full rewrite report the same metrics.
 *
 * <p>Only the unpartitioned spec is handled here; a partitioned copy-on-write
 * write takes the full-rewrite task writer. {@code copiedRowGroups} counts the
 * groups copied, for tests and metrics.
 */
public final class IcebergRewriteFileTaskWriter implements AutoCloseable {

    /**
     * Groups copied byte for byte across all tasks of a write (test/metric
     * counter).
     */
    public static final LongAdder COPIED_ROW_GROUPS = new LongAdder();

    /**
     * Buffered surviving rows of one source file: their gathered data lanes and
     * their positions.
     */
    private static final class FileBuffer {
        final List<List<VectorBuffers>> dataChunks = new ArrayList<>();
        final List<long[]> posChunks = new ArrayList<>();
        long rows;

        void add(List<VectorBuffers> data, long[] pos) {
            dataChunks.add(data);
            posChunks.add(pos);
            rows += pos.length;
        }
    }

    private final IcebergWriteSpec spec;
    private final OutputFileFactory files;
    private final MetricsConfig metricsConfig;
    private final NameMapping nameMapping;
    private final Configuration conf;
    private final MessageType writeSchema;
    private final int fileOrdinal;
    private final int posOrdinal;
    private final int[] dataOrdinals;
    private final Arena arena = Arena.ofShared();
    private final Map<String, FileBuffer> buffers = new LinkedHashMap<>();
    private final List<DataFile> done = new ArrayList<>();
    private final List<String> written = new ArrayList<>();
    private boolean closed;

    /**
     * @param spec the write spec (table, write schema, properties, target size)
     * @param fileOrdinal the {@code _file} column's ordinal in the batches
     * @param posOrdinal the {@code _pos} column's ordinal in the batches
     * @param dataOrdinals the write-schema columns' ordinals in the batches
     *     (data columns and, on v3, the row-lineage columns)
     */
    public IcebergRewriteFileTaskWriter(
            IcebergWriteSpec spec,
            int partitionId,
            long taskId,
            Configuration conf,
            int fileOrdinal,
            int posOrdinal,
            int[] dataOrdinals) {
        this.spec = spec;
        this.files = OutputFileFactory.builderFor(spec.table(), partitionId, taskId)
                .format(FileFormat.PARQUET)
                .operationId(spec.operationId())
                .defaultSpec(spec.spec())
                .build();
        this.metricsConfig = MetricsConfig.forTable(spec.table());
        String mapping = spec.table()
                             .properties()
                             .get(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING);
        this.nameMapping = mapping == null ? null : NameMappingParser.fromJson(mapping);
        this.conf = conf;
        this.writeSchema = IcebergParquetInterop.fileSchema(spec.writeSchema(), "table");
        this.fileOrdinal = fileOrdinal;
        this.posOrdinal = posOrdinal;
        this.dataOrdinals = dataOrdinals;
    }

    /**
     * Buffers rows {@code [first, end)} of {@code columns}: the data columns at
     * the write-schema ordinals and the {@code _file} / {@code _pos} columns.
     * Rows are grouped by {@code _file} into per-file buffers. {@code columns}
     * is read, not retained, so the picked rows are gathered into this writer's
     * own arena.
     */
    public void write(List<? extends VectorBuffers> columns, int first, int end) {
        if (end <= first) {
            return;
        }
        VectorBuffers fileCol = columns.get(fileOrdinal);
        VectorBuffers posCol = columns.get(posOrdinal);
        // Group the batch's rows by their _file value (runs reuse the previous row's string).
        int n = end - first;
        Map<String, List<Integer>> byFile = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            int r = first + i;
            String file = fileCol.getString(r);
            byFile.computeIfAbsent(file, k -> new ArrayList<>()).add(r);
        }
        for (Map.Entry<String, List<Integer>> e : byFile.entrySet()) {
            List<Integer> rowsList = e.getValue();
            int[] rows = new int[rowsList.size()];
            long[] pos = new long[rowsList.size()];
            for (int k = 0; k < rows.length; k++) {
                rows[k] = rowsList.get(k);
                pos[k] = posCol.data().get(VectorBuffers.LE_LONG, (long) rows[k] << 3);
            }
            List<VectorBuffers> data = new ArrayList<>(dataOrdinals.length);
            List<VectorBuffers> picked = new ArrayList<>(dataOrdinals.length);
            for (int o : dataOrdinals) {
                picked.add(columns.get(o));
            }
            data.addAll(Gather.rows(picked, rows, arena));
            buffers.computeIfAbsent(e.getKey(), k -> new FileBuffer()).add(data, pos);
        }
    }

    /** Finishes all files and returns the task's data files once. */
    public DataFile[] dataFiles() {
        close();
        return done.toArray(new DataFile[0]);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            for (Map.Entry<String, FileBuffer> e : buffers.entrySet()) {
                rewriteFile(e.getKey(), e.getValue());
            }
        } finally {
            arena.close();
        }
    }

    private void rewriteFile(String sourceLocation, FileBuffer buffer) {
        // Concatenate the buffered chunks and order the surviving rows by _pos (the file's row order).
        int total = (int) buffer.rows;
        long[] pos = new long[total];
        int[] chunkOf = new int[total];
        int[] rowInChunk = new int[total];
        int k = 0;
        for (int c = 0; c < buffer.posChunks.size(); c++) {
            long[] cp = buffer.posChunks.get(c);
            for (int r = 0; r < cp.length; r++) {
                pos[k] = cp[r];
                chunkOf[k] = c;
                rowInChunk[k] = r;
                k++;
            }
        }
        Integer[] order = new Integer[total];
        for (int i = 0; i < total; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (x, y) -> Long.compare(pos[x], pos[y]));

        org.apache.hadoop.fs.Path sourcePath = new org.apache.hadoop.fs.Path(sourceLocation);
        HadoopInputFile in;
        ParquetMetadata footer;
        try {
            in = HadoopInputFile.fromPath(sourcePath, conf);
            try (ParquetFileReader reader = ParquetFileReader.open(in)) {
                footer = reader.getFooter();
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }

        // The deleted positions of this file: those in [0, sourceRows) not present among the survivors.
        long sourceRows = footer.getBlocks().stream()
                .mapToLong(b -> b.getRowCount())
                .sum();
        boolean[] survives = new boolean[(int) sourceRows];
        for (long p : pos) {
            if (p >= 0 && p < sourceRows) {
                survives[(int) p] = true;
            }
        }
        long deletedCount = sourceRows - total;
        long[] deleted = new long[(int) deletedCount];
        int di = 0;
        for (int p = 0; p < sourceRows; p++) {
            if (!survives[p]) {
                deleted[di++] = p;
            }
        }

        RowGroupCopyPlanner plan = RowGroupCopyPlanner.plan(footer, writeSchema, deleted, spec.writeSchema(), metricsConfig);

        EncryptedOutputFile out = files.newOutputFile();
        String location = out.encryptingOutputFile().location();
        written.add(location);
        IcebergParquetAppender appender;
        try {
            appender = IcebergParquetAppender.create(out.encryptingOutputFile(), spec.writeSchema(), spec.properties(),
                    metricsConfig, nameMapping, conf);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }

        // Map each source position to the (chunk,row) that holds its buffered data, for re-encoding in order.
        long[] posToPacked = new long[(int) sourceRows];
        Arrays.fill(posToPacked, -1L);
        for (int i = 0; i < total; i++) {
            posToPacked[(int) pos[i]] = ((long) chunkOf[i] << 32) | (rowInChunk[i] & 0xffffffffL);
        }

        try (SeekableInputStream stream = plan.copyable() ? in.newStream() : null) {
            for (RowGroupCopyPlanner.Group g : plan.groups()) {
                RowGroupCopyPlanner.Action action = plan.copyable() ? g.action() : RowGroupCopyPlanner.Action.REENCODE;
                if (action == RowGroupCopyPlanner.Action.COPY) {
                    appender.copyRowGroup(stream, footer.getBlocks()
                            .get(g.ordinal()));
                    COPIED_ROW_GROUPS.increment();
                } else if (action == RowGroupCopyPlanner.Action.REENCODE) {
                    reencodeGroup(appender, buffer, g, posToPacked);
                }
                // SKIP: every row of the group was deleted -- nothing written.
            }
            if (!plan.copyable()) {
                appender.endRowGroup();
            }
            appender.close();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }

        DataFiles.Builder b = DataFiles.builder(spec.spec())
                .withPath(location)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(appender.length())
                .withMetrics(appender.metrics())
                .withSplitOffsets(appender.splitOffsets());
        done.add(b.build());
    }

    /**
     * Re-encodes the surviving rows of one row group, in {@code _pos} order,
     * through the appender.
     */
    private void reencodeGroup(IcebergParquetAppender appender, FileBuffer buffer, RowGroupCopyPlanner.Group g,
            long[] posToPacked)
            throws IOException {
        long start = g.start();
        long endPos = start + g.rowCount();
        // Collect the packed (chunk,row) of the surviving positions of this group, in position order.
        List<Long> packed = new ArrayList<>();
        for (long p = start; p < endPos; p++) {
            long pk = posToPacked[(int) p];
            if (pk >= 0) {
                packed.add(pk);
            }
        }
        if (packed.isEmpty()) {
            return;
        }
        try (Arena a = Arena.ofConfined()) {
            // Gather, per source chunk, the rows that fall in this group; append in position order by walking
            // runs of the same chunk.
            int i = 0;
            while (i < packed.size()) {
                int chunk = (int) (packed.get(i) >>> 32);
                int j = i;
                while (j < packed.size() && (int) (packed.get(j) >>> 32) == chunk) {
                    j++;
                }
                int[] rows = new int[j - i];
                for (int t = 0; t < rows.length; t++) {
                    rows[t] = (int) (packed.get(i + t) & 0xffffffffL);
                }
                List<VectorBuffers> gathered = Gather.rows(buffer.dataChunks.get(chunk), rows, a);
                appender.write(gathered, 0, rows.length);
                i = j;
            }
            appender.endRowGroup();
        }
    }

    /** Abandons the task, deleting whatever files it wrote. */
    public void abort(Throwable cause) {
        closed = true;
        try {
            arena.close();
        } catch (RuntimeException e) {
            cause.addSuppressed(e);
        }
        for (String path : written) {
            try {
                spec.table()
                    .io()
                    .deleteFile(path);
            } catch (RuntimeException e) {
                cause.addSuppressed(e);
            }
        }
        done.clear();
    }
}
