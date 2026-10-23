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
    // Per write-schema column, true to RE-ENCODE the column (a changed UPDATE column, or a v3 lineage column),
    // false to COPY its chunk byte for byte. Length == dataOrdinals.length.
    private final boolean[] encodeColumns;
    private final PartitionKeys keys;
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
     * @param encodeColumns per write-schema column, true to re-encode the
     *     column (a changed UPDATE column, or a v3 lineage column), false to
     *     copy its chunk byte for byte
     */
    public IcebergRewriteFileTaskWriter(
            IcebergWriteSpec spec,
            int partitionId,
            long taskId,
            Configuration conf,
            int fileOrdinal,
            int posOrdinal,
            int[] dataOrdinals,
            boolean[] encodeColumns) {
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
        this.encodeColumns = encodeColumns == null ? new boolean[dataOrdinals.length] : encodeColumns;
        this.keys = spec.spec().isUnpartitioned() ? null : new PartitionKeys(spec.spec(), spec.writeSchema());
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
        // Runtime safety guard: a copy is only valid if this task holds the file's rows exactly once, within
        // range. If a position repeats or is out of range (a split file routed part elsewhere, or a corrupt
        // _pos), the survivor set is not this file's true survivor set, so we must not copy -- force a full
        // rewrite of whatever rows this task does hold (correct data; just no byte copy).
        boolean[] survives = new boolean[(int) sourceRows];
        boolean safe = total <= sourceRows;
        for (long p : pos) {
            if (p < 0 || p >= sourceRows || survives[(int) p]) {
                safe = false;
                break;
            }
            survives[(int) p] = true;
        }
        long[] deleted;
        if (safe) {
            long deletedCount = sourceRows - total;
            deleted = new long[(int) deletedCount];
            int di = 0;
            for (int p = 0; p < sourceRows; p++) {
                if (!survives[p]) {
                    deleted[di++] = p;
                }
            }
        } else {
            // Treat every row this task holds as surviving and re-encode them all (no copy): mark all deleted
            // so the planner yields REENCODE/SKIP only, and copyable() still gates the copy off below.
            deleted = new long[0];
        }

        RowGroupCopyPlanner plan = safe ? RowGroupCopyPlanner.plan(footer, writeSchema, deleted, spec.writeSchema(), metricsConfig,
                maskOrNull())
                : null;
        boolean copyable = safe && plan.copyable();

        // #901 M4a/M4b required-column guard: a COPIED column keeps the SOURCE's repetition, which may be
        // REQUIRED (Iceberg's writer marks an always-non-null column REQUIRED). A re-encoded group writes all
        // columns under the output schema, so a null in a REQUIRED copied column would be invalid. This cannot
        // happen for a survivor whose value came from that very column -- a REQUIRED column holds no nulls --
        // but guard anyway. Only COPIED columns matter: an ENCODED (changed) column uses the write schema's
        // OPTIONAL field, so a null there is fine.
        if (copyable && survivorHasNullInRequiredCopied(buffer, plan.fileSchema(), plan.copyMask())) {
            copyable = false;
        }

        // #901 M4b partition-change guard: if an UPDATE changes a column the table partitions by, a rewritten
        // row may move partition, but the copy path keeps the source file's partition. Decline the copy and
        // fall back to the full rewrite (which routes rows to the right partition). Only relevant when a data
        // column is re-encoded (an UPDATE); a DELETE never changes a value.
        if (copyable && changesPartitionColumn()) {
            copyable = false;
        }

        // The partition of this file's rows (all share one partition), from the first buffered row. Null for an
        // unpartitioned spec; used so a rewritten file keeps its source file's partition.
        org.apache.iceberg.PartitionKey partition = partitionOf(buffer);

        EncryptedOutputFile out = partition == null ? files.newOutputFile() : files.newOutputFile(spec.spec(), partition);
        String location = out.encryptingOutputFile().location();
        written.add(location);
        IcebergParquetAppender appender;
        try {
            // #901 M4a: on the copy path the output file uses the planner's copy output schema -- the SOURCE
            // file's own Parquet schema for a v2 whole-group copy, or the full write schema (data + the two v3
            // lineage columns) for a v3 mixed-group copy, where the data chunks are copied and the lineage
            // chunks encoded. Either carries Iceberg's field ids, so footer metrics stay keyed by the same ids.
            // The full-rewrite path keeps Iceberg's own conversion of the write schema (outputSchema = null).
            MessageType outputSchema = copyable ? plan.copyOutputSchema() : null;
            appender = IcebergParquetAppender.create(out.encryptingOutputFile(), spec.writeSchema(), outputSchema,
                    spec.properties(), metricsConfig, nameMapping, conf);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }

        // Map each source position to the (chunk,row) that holds its buffered data, for re-encoding in order.
        long[] posToPacked = new long[(int) sourceRows];
        Arrays.fill(posToPacked, -1L);
        for (int i = 0; i < total; i++) {
            if (pos[i] >= 0 && pos[i] < sourceRows) {
                posToPacked[(int) pos[i]] = ((long) chunkOf[i] << 32) | (rowInChunk[i] & 0xffffffffL);
            }
        }

        if (copyable) {
            // How many columns a re-encoded group writes: the output schema's field count. On a v2 whole-group
            // copy that is the source field count (no lineage); on a v3 mixed-group copy it is the write
            // schema's (data + the two lineage columns), so a re-encoded group matches the copied groups' layout.
            int outCols = plan.outputFieldCount();
            try (SeekableInputStream stream = in.newStream();
                 ParquetFileReader indexReader = ParquetFileReader.open(in)) {
                for (RowGroupCopyPlanner.Group g : plan.groups()) {
                    if (g.action() == RowGroupCopyPlanner.Action.COPY) {
                        if (g.copiesWholeGroup()) {
                            appender.copyRowGroup(stream, footer.getBlocks()
                                    .get(g.ordinal()));
                        } else {
                            // Mixed group (v3): copy the data chunks, encode the lineage chunks from the
                            // group's surviving (here: all) rows, resolved by the scan into the batch.
                            copyMixedGroup(
                                    appender,
                                    indexReader,
                                    stream,
                                    footer.getBlocks().get(g.ordinal()),
                                    g,
                                    plan.copyMask(),
                                    buffer,
                                    posToPacked,
                                    outCols);
                        }
                        COPIED_ROW_GROUPS.increment();
                    } else if (g.action() == RowGroupCopyPlanner.Action.REENCODE) {
                        reencodeGroup(appender, buffer, g, posToPacked, outCols);
                    }
                    // SKIP: every row of the group was deleted -- nothing written.
                }
                appender.close();
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        } else {
            // Full rewrite: re-encode every surviving row this task holds, in _pos order, no copy.
            try {
                reencodeAll(appender, buffer, pos, chunkOf, rowInChunk, total);
                appender.close();
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        }

        DataFiles.Builder b = DataFiles.builder(spec.spec())
                .withPath(location)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(appender.length())
                .withMetrics(appender.metrics())
                .withSplitOffsets(appender.splitOffsets());
        if (partition != null) {
            b.withPartition(partition);
        }
        done.add(b.build());
    }

    /**
     * The partition of a file's buffered rows (they share one), or null for an
     * unpartitioned spec.
     */
    private org.apache.iceberg.PartitionKey partitionOf(FileBuffer buffer) {
        if (keys == null) {
            return null;
        }
        List<VectorBuffers> first = buffer.dataChunks.get(0);
        Object[][] values = keys.compute(first, 0, 1);
        return keys.key(values, 0);
    }

    /**
     * Whether any buffered survivor is null in a top-level column the source
     * schema marks REQUIRED (#901 M4a guard). The buffered data columns are in
     * write-schema field order, which {@link RowGroupCopyPlanner#compatibleForCopy}
     * has verified equals the source schema's top-level field order, so index
     * {@code i} of {@code dataChunks} is top-level field {@code i} of {@code
     * fileSchema}. A REQUIRED column that a survivor is null in cannot be
     * written under the source schema, so the file must take the full rewrite
     * instead. Checks only the lineage-free data columns (ids below the write
     * schema's data field count); v3 lineage columns are the writer's own.
     */
    private boolean survivorHasNullInRequiredCopied(FileBuffer buffer, MessageType fileSchema, boolean[] copyMask) {
        int fields = Math.min(fileSchema.getFieldCount(), dataOrdinals.length);
        for (int f = 0; f < fields; f++) {
            // Only a COPIED REQUIRED column matters; an ENCODED column uses the write schema's OPTIONAL field.
            if (copyMask != null && f < copyMask.length && !copyMask[f]) {
                continue;
            }
            org.apache.parquet.schema.Type type = fileSchema.getType(f);
            if (!type.isRepetition(org.apache.parquet.schema.Type.Repetition.REQUIRED)) {
                continue;
            }
            for (List<VectorBuffers> chunk : buffer.dataChunks) {
                VectorBuffers col = chunk.get(f);
                if (!col.hasNulls()) {
                    continue;
                }
                int rows = col.length();
                for (int r = 0; r < rows; r++) {
                    if (col.isNull(r)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Whether an UPDATE re-encodes (changes) a column the table partitions by,
     * so a rewritten row might move partition. The write-schema Parquet fields
     * carry Iceberg's source field ids; a partition field's {@code sourceId}
     * names the column it transforms. Only meaningful when a data column is
     * re-encoded (an UPDATE); a DELETE changes no value.
     */
    private boolean changesPartitionColumn() {
        if (maskOrNull() == null || spec.spec().isUnpartitioned()) {
            return false;
        }
        java.util.Set<Integer> partitionSourceIds = new java.util.HashSet<>();
        for (org.apache.iceberg.PartitionField pf : spec.spec().fields()) {
            partitionSourceIds.add(pf.sourceId());
        }
        int dataCols = writeSchema.getFieldCount();
        for (int i = 0;
             i < dataCols && i < encodeColumns.length;
             i++) {
            if (!encodeColumns[i]) {
                continue;
            }
            org.apache.parquet.schema.Type t = writeSchema.getType(i);
            if (t.getId() != null && partitionSourceIds.contains(t.getId()
                    .intValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The encode mask to hand the planner, or {@code null} to keep the M4a copy
     * logic (copy every data column, encode only v3 lineage). Returns the mask
     * only when a DATA column (not just lineage) is marked for re-encoding --
     * i.e. an UPDATE whose changed columns the strategy identified. A DELETE
     * (lineage-only mask) returns {@code null} so M4a's whole-group / lineage
     * copy runs unchanged.
     */
    private boolean[] maskOrNull() {
        // The two v3 lineage columns are the trailing write-schema columns; a mask that marks only those (or
        // nothing) is a DELETE-shaped mask. Treat the leading (data) columns only.
        int dataCols = writeSchema.getFieldCount();
        // Lineage columns are those whose field id is one of the reserved lineage ids.
        for (int i = 0;
             i < dataCols && i < encodeColumns.length;
             i++) {
            org.apache.parquet.schema.Type t = writeSchema.getType(i);
            boolean lineage = t.getId() != null && (t.getId().intValue() == Integer.MAX_VALUE - 107 || t.getId().intValue() == Integer.MAX_VALUE - 108);
            if (encodeColumns[i] && !lineage) {
                return encodeColumns;
            }
        }
        return null;
    }

    /**
     * Re-encodes every surviving row (across all chunks) in {@code _pos} order,
     * as one group stream.
     */
    private void reencodeAll(IcebergParquetAppender appender, FileBuffer buffer, long[] pos,
            int[] chunkOf, int[] rowInChunk, int total)
            throws IOException {
        Integer[] order = new Integer[total];
        for (int i = 0; i < total; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (x, y) -> Long.compare(pos[x], pos[y]));
        try (Arena a = Arena.ofConfined()) {
            int i = 0;
            while (i < total) {
                int chunk = chunkOf[order[i]];
                int j = i;
                while (j < total && chunkOf[order[j]] == chunk) {
                    j++;
                }
                int[] rows = new int[j - i];
                for (int t = 0; t < rows.length; t++) {
                    rows[t] = rowInChunk[order[i + t]];
                }
                List<VectorBuffers> gathered = Gather.rows(buffer.dataChunks.get(chunk), rows, a);
                appender.write(gathered, 0, rows.length);
                i = j;
            }
        }
    }

    /**
     * Copies one row group as a MIX of copied and encoded column chunks (#901
     * M4a v3 lineage): the data chunks are copied byte for byte from {@code
     * sourceBlock} and the columns the mask marks for encoding (the two v3
     * lineage columns) are encoded from the group's rows. Every row of the
     * group survives (this is a COPY group, no deletions), so the rows are the
     * whole group in {@code _pos} order. The group may span several buffered
     * batches, so its rows are gathered into one or more sub-runs (one per
     * source chunk run) and the writer concatenates them into one group.
     */
    private void copyMixedGroup(
            IcebergParquetAppender appender,
            ParquetFileReader indexReader,
            SeekableInputStream stream,
            org.apache.parquet.hadoop.metadata.BlockMetaData sourceBlock,
            RowGroupCopyPlanner.Group g,
            boolean[] copyMask,
            FileBuffer buffer,
            long[] posToPacked,
            int outCols)
            throws IOException {
        long start = g.start();
        long endPos = start + g.rowCount();
        // The (chunk,row) of every position in this group, in position order. A COPY group has every position.
        List<Long> packed = new ArrayList<>((int) g.rowCount());
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
            // Gather, per source chunk, the rows that fall in this group, in position order: one sub-run per
            // contiguous run of the same chunk. Each run is the full column set (data + lineage), sliced to the
            // output columns.
            List<List<VectorBuffers>> runs = new ArrayList<>();
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
                runs.add(new ArrayList<>(Gather.rows(buffer.dataChunks.get(chunk), rows, a)
                        .subList(0, outCols)));
                i = j;
            }
            appender.copyRowGroupColumns(indexReader, stream, sourceBlock, copyMask, runs,
                    packed.size());
        }
    }

    /**
     * Re-encodes the surviving rows of one row group, in {@code _pos} order,
     * through the appender. {@code dataCols} is how many leading columns to
     * write: the source file's own field count on the copy path, which on a v3
     * table drops the trailing lineage columns the source does not materialise,
     * so a re-encoded group matches the copied groups' (source) schema.
     */
    private void reencodeGroup(IcebergParquetAppender appender, FileBuffer buffer, RowGroupCopyPlanner.Group g,
            long[] posToPacked, int dataCols)
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
                appender.write(gathered.subList(0, dataCols), 0, rows.length);
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
