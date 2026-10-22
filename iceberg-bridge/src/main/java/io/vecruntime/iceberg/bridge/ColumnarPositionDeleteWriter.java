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
package io.vecruntime.iceberg.bridge;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import io.vecruntime.iceberg.write.IcebergParquetAppender;
import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteIndexFactory;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.CharSequenceSet;

/**
 * A v2 table's position delete files, written columnar (#901 M2): the same
 * files Iceberg's Spark delta writer writes ({@code
 * FanoutPositionOnlyDeleteWriter} over {@code
 * SortingPositionOnlyDeleteWriter}), with the {@code (file_path, pos)} rows
 * encoded by the columnar Parquet writer ({@link IcebergParquetAppender}) a
 * chunk of positions at a time instead of one {@code PositionDelete} row at a
 * time.
 *
 * <p>As Iceberg does, a task collects each data file's positions in a bitmap,
 * per partition, and at close writes, per partition, the data files in path
 * order and each file's positions in ascending order: with {@code FILE}
 * granularity one delete file per data file, its previous file-scoped deletes
 * merged in and reported as rewritten; with {@code PARTITION} granularity one
 * per partition. A file rolls at the target delete file size. A delete file
 * referencing several data files keeps no counts or bounds for {@code
 * file_path} and {@code pos}, as {@code PositionDeleteWriter} drops them; one
 * referencing a single data file keeps them in full ({@code
 * MetricsConfig.forPositionDelete}), so readers can tell which data file it
 * applies to.
 */
public final class ColumnarPositionDeleteWriter implements DeleteSink {

    /** Positions encoded per appender call. */
    static final int CHUNK = 64 * 1024;

    /** Delete files written in this JVM (read by tests). */
    public static final java.util.concurrent.atomic.AtomicLong FILES_WRITTEN = new java.util.concurrent.atomic.AtomicLong();

    private static final Schema SCHEMA = DeleteSchemaUtil.pathPosSchema();
    private static final Set<Integer> FILE_AND_POS = Set.of(MetadataColumns.DELETE_FILE_PATH.fieldId(), MetadataColumns.DELETE_FILE_POS.fieldId());

    /**
     * One partition's deletes: its spec and tuple, and each data file's
     * positions, in path order.
     */
    private record Group(PartitionSpec spec, StructLike partition, TreeMap<String, PositionDeleteIndex> files) {}

    private record Key(int specId, PartitionData partition) {}

    private final Table table;
    private final OutputFileFactory files;
    private final PositionDeleteSpec deleteSpec;
    private final Function<CharSequence, PositionDeleteIndex> previousDeletes;
    private final MetricsConfig metricsConfig;
    private final Configuration conf;
    private final Map<Key, Group> groups = new HashMap<>();
    private DeleteWriteResult result;

    public ColumnarPositionDeleteWriter(Table table, OutputFileFactory files, PositionDeleteSpec deleteSpec,
            Function<CharSequence, PositionDeleteIndex> previousDeletes, Configuration conf) {
        this.table = table;
        this.files = files;
        this.deleteSpec = deleteSpec;
        this.previousDeletes = previousDeletes;
        this.metricsConfig = MetricsConfig.forPositionDelete();
        this.conf = conf;
    }

    @Override
    public void deleteFile(String dataFilePath, long[] positions, int count,
                           PartitionSpec spec, StructLike partition) {
        if (count <= 0) {
            return;
        }
        PartitionData tuple = copy(spec, partition);
        Group group = groups.computeIfAbsent(
                new Key(spec.specId(), tuple),
                k -> new Group(spec, spec.isUnpartitioned() ? null : tuple,
                        new TreeMap<>()));
        PositionDeleteIndex run = PositionDeleteIndexFactory.fromPositions(positions, count);
        PositionDeleteIndex known = group.files().putIfAbsent(dataFilePath, run);
        if (known != null) {
            known.merge(run);
        }
    }

    private static PartitionData copy(PartitionSpec spec, StructLike partition) {
        PartitionData data = new PartitionData(spec.partitionType());
        if (partition != null) {
            for (int i = 0; i < data.size(); i++) {
                data.set(i, partition.get(i, Object.class));
            }
        }
        return data;
    }

    @Override
    public DeleteWriteResult result() {
        if (result == null) {
            throw new IllegalStateException("the result is available once the writer is closed");
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (result != null) {
            return;
        }
        List<DeleteFile> written = new ArrayList<>();
        CharSequenceSet referenced = CharSequenceSet.empty();
        List<DeleteFile> rewritten = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            long[] chunk = new long[CHUNK];
            MemorySegment pos = ArrowLayout.allocateData(arena, VecType.INT64, CHUNK);
            // Every row of a chunk is the same data file: index 0 of a one-entry dictionary.
            MemorySegment indices = ArrowLayout.allocateData(arena, VecType.INT32, CHUNK);
            for (Group group : groups.values()) {
                FileOut out = null;
                for (Map.Entry<String, PositionDeleteIndex> e : group.files().entrySet()) {
                    String path = e.getKey();
                    PositionDeleteIndex index = e.getValue();
                    if (deleteSpec.fileGranularity() && previousDeletes != null) {
                        PositionDeleteIndex previous = previousDeletes.apply(path);
                        if (previous != null && previous.isNotEmpty()) {
                            index.merge(previous);
                            rewritten.addAll(previous.deleteFiles());
                        }
                    }
                    referenced.add(path);
                    if (out == null || deleteSpec.fileGranularity()) {
                        if (out != null) {
                            written.add(out.finish());
                        }
                        out = new FileOut(group);
                    }
                    VectorBuffers dictionary = utf8(arena, path);
                    int[] n = {0};
                    FileOut[] holder = {out};
                    index.forEach(p -> {
                        chunk[n[0]++] = p;
                        if (n[0] == CHUNK) {
                            holder[0] = write(holder[0], group, path, chunk, n[0], pos,
                                    indices, dictionary, written);
                            n[0] = 0;
                        }
                    });
                    if (n[0] > 0) {
                        holder[0] = write(holder[0], group, path, chunk, n[0], pos,
                                indices, dictionary, written);
                    }
                    out = holder[0];
                }
                if (out != null) {
                    written.add(out.finish());
                }
            }
        } catch (IOException | RuntimeException e) {
            for (DeleteFile f : written) {
                table.io().deleteFile(f.location());
            }
            throw e;
        }
        result = new DeleteWriteResult(written, referenced, rewritten);
    }

    /**
     * Appends one chunk of {@code path}'s positions, rolling to a new file
     * first when the open one is full.
     */
    private FileOut write(
            FileOut out,
            Group group,
            String path,
            long[] chunk,
            int n,
            MemorySegment pos,
            MemorySegment indices,
            VectorBuffers dictionary,
            List<DeleteFile> written) {
        FileOut target = out;
        if (target.appender.rowCount() > 0 && target.appender.length() >= deleteSpec.targetFileSize()) {
            try {
                written.add(target.finish());
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            target = new FileOut(group);
        }
        MemorySegment.copy(chunk, 0, pos, ValueLayout.JAVA_LONG_UNALIGNED, 0, n);
        List<VectorBuffers> columns = List.of(SegmentVectorBuffers.dictionaryUtf8(n, null, indices, dictionary), SegmentVectorBuffers.fixedWidth(VecType.INT64, n, null, pos));
        target.appender.write(columns, 0, n);
        target.paths.add(path);
        return target;
    }

    private static VectorBuffers utf8(Arena arena, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        MemorySegment data = ArrowLayout.allocateBytes(arena, bytes.length);
        MemorySegment.copy(bytes, 0, data, ValueLayout.JAVA_BYTE, 0, bytes.length);
        MemorySegment offsets = ArrowLayout.allocateOffsets(arena, 1);
        offsets.set(ValueLayout.JAVA_INT_UNALIGNED, 0, 0);
        offsets.set(ValueLayout.JAVA_INT_UNALIGNED, 4, bytes.length);
        return SegmentVectorBuffers.utf8(1, null, offsets, data);
    }

    /** One open delete file of a partition. */
    private final class FileOut {
        final PartitionSpec spec;
        final StructLike partition;
        final OutputFile file;
        final IcebergParquetAppender appender;
        final CharSequenceSet paths = CharSequenceSet.empty();

        FileOut(Group group) {
            this.spec = group.spec();
            this.partition = group.partition();
            this.file = (spec.isUnpartitioned() ? files.newOutputFile() : files.newOutputFile(spec, partition)).encryptingOutputFile();
            try {
                this.appender = IcebergParquetAppender.create(file, SCHEMA, deleteSpec.properties(), metricsConfig, null,
                        conf);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }

        DeleteFile finish() throws IOException {
            appender.close();
            FILES_WRITTEN.incrementAndGet();
            Metrics metrics = appender.metrics();
            if (paths.size() > 1) {
                metrics = MetricsUtil.copyWithoutFieldCountsAndBounds(metrics, FILE_AND_POS);
            }
            return FileMetadata.deleteFileBuilder(spec)
                    .ofPositionDeletes()
                    .withFormat(FileFormat.PARQUET)
                    .withPath(file.location())
                    .withPartition(partition)
                    .withSplitOffsets(appender.splitOffsets())
                    .withFileSizeInBytes(appender.length())
                    .withMetrics(metrics)
                    .build();
        }
    }
}
