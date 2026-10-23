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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;

/**
 * One task's data files for an Iceberg write (#842, W2-W3): batches go to
 * {@link IcebergParquetAppender}s, a new file is started once the open one
 * reaches the write's target file size, and {@link #dataFiles} returns the
 * files as Iceberg {@link DataFile}s. File names and locations come from
 * Iceberg's own {@link OutputFileFactory} (the table's location provider, the
 * partition and task ids, the operation id), so they are the names Iceberg's
 * writer gives its files.
 *
 * <p>For a partitioned spec each row's partition comes from {@link
 * PartitionKeys}, and the writer is the one Iceberg's own Spark write would use
 * ({@code SparkWrite.useFanoutWriter}):
 *
 * <ul>
 *   <li><b>clustered</b> (the default): the rows arrive clustered by partition
 *       -- Iceberg requests that ordering of every write distribution mode when
 *       fanout is off -- so one file is open at a time and each run of a
 *       partition is written as a slice of its batch; a partition met again
 *       after it was closed is an error, as in Iceberg's {@code
 *       ClusteredWriter};
 *   <li><b>fanout</b>: rows in any order; one open file per partition, each
 *       batch's rows gathered per partition, nested columns included.
 * </ul>
 *
 * Like Iceberg's {@code RollingDataWriter}, the size is checked between writes:
 * a file ends at most one batch past the target.
 */
public final class IcebergDataTaskWriter implements AutoCloseable {

    /**
     * The rolling data files of one partition (or of the unpartitioned spec,
     * with a null key).
     */
    private final class Rolling {
        final PartitionKey key;
        IcebergParquetAppender appender;
        String location;

        Rolling(PartitionKey key) {
            this.key = key;
        }

        void write(List<? extends VectorBuffers> columns, int first, int end) {
            if (end <= first) {
                return;
            }
            if (appender == null) {
                EncryptedOutputFile out = key == null ? files.newOutputFile() : files.newOutputFile(spec.spec(), key);
                location = out.encryptingOutputFile().location();
                written.add(location);
                try {
                    appender = IcebergParquetAppender.create(out.encryptingOutputFile(), spec.writeSchema(), spec.properties(),
                            metricsConfig, nameMapping, conf);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            appender.write(columns, first, end);
            if (appender.length() >= spec.targetFileSize()) {
                finish();
            }
        }

        void finish() {
            if (appender == null) {
                return;
            }
            try {
                appender.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            DataFiles.Builder b = DataFiles.builder(spec.spec())
                    .withPath(location)
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(appender.length())
                    .withMetrics(appender.metrics())
                    .withSortOrderId(spec.sortOrderId())
                    .withSplitOffsets(appender.splitOffsets());
            if (key != null) {
                b.withPartition(key);
            }
            done.add(b.build());
            appender = null;
            location = null;
        }

        void abort(Throwable cause) {
            if (appender != null) {
                try {
                    appender.close();
                } catch (IOException | RuntimeException e) {
                    cause.addSuppressed(e);
                }
                appender = null;
            }
        }
    }

    private final IcebergWriteSpec spec;
    private final OutputFileFactory files;
    private final MetricsConfig metricsConfig;
    private final NameMapping nameMapping;
    private final Configuration conf;
    private final PartitionKeys keys;
    private final boolean arrowLanes;
    private final List<DataFile> done = new ArrayList<>();
    private final List<String> written = new ArrayList<>();
    // Unpartitioned, or clustered: the one open partition.
    private Rolling current;
    // Clustered: the partitions already closed (a key met again is an error).
    private final Set<PartitionKey> closedKeys = new HashSet<>();
    // Fanout: the open partitions.
    private final Map<PartitionKey, Rolling> open = new LinkedHashMap<>();
    private boolean closed;

    public IcebergDataTaskWriter(IcebergWriteSpec spec, int partitionId, long taskId,
            Configuration conf) {
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
        this.keys = spec.spec().isUnpartitioned() ? null : new PartitionKeys(spec.spec(), spec.writeSchema());
        this.arrowLanes = SparkLanes.needed(spec.writeSchema());
    }

    /**
     * Appends rows {@code [first, end)} of {@code columns}, one vector per
     * top-level write-schema field.
     */
    public void write(List<? extends VectorBuffers> columns, int first, int end) {
        if (end <= first) {
            return;
        }
        if (arrowLanes) {
            // Spark's lanes as the Arrow lanes of the Iceberg types (SparkLanes), for the length of this call: the
            // encoders copy every value into their pages before it returns.
            try (java.lang.foreign.Arena arena = java.lang.foreign.Arena.ofConfined()) {
                writeArrow(SparkLanes.toArrow(spec.writeSchema(), columns, arena), first, end);
            }
            return;
        }
        writeArrow(columns, first, end);
    }

    private void writeArrow(List<? extends VectorBuffers> columns, int first, int end) {
        if (keys == null) {
            if (current == null) {
                current = new Rolling(null);
            }
            current.write(columns, first, end);
        } else if (spec.fanout()) {
            writeFanout(columns, first, end);
        } else {
            writeClustered(columns, first, end);
        }
    }

    private void writeClustered(List<? extends VectorBuffers> columns, int first, int end) {
        Object[][] values = keys.compute(columns, first, end);
        int n = end - first;
        int start = 0;
        while (start < n) {
            int stop = start + 1;
            while (stop < n && PartitionKeys.samePartition(values, start, stop)) {
                stop++;
            }
            if (current == null || !PartitionKeys.inPartition(values, start, current.key)) {
                PartitionKey key = keys.key(values, start);
                if (closedKeys.contains(key)) {
                    throw new IllegalStateException("Incoming records violate the writer assumption that records are"
                            + " clustered by spec and by partition within each spec. Either cluster the incoming"
                            + " records or switch to fanout writers.\nEncountered records that belong to already"
                            + " closed files:\npartition '"
                            + key.toPath()
                            + "' in spec "
                            + spec.spec());
                }
                if (current != null) {
                    current.finish();
                    closedKeys.add(current.key);
                }
                current = new Rolling(key);
            }
            current.write(columns, first + start, first + stop);
            start = stop;
        }
    }

    private void writeFanout(List<? extends VectorBuffers> columns, int first, int end) {
        Object[][] values = keys.compute(columns, first, end);
        int n = end - first;
        // Each row's partition as an index into `partitions` (first-seen order); runs reuse the previous row's.
        List<PartitionKey> partitions = new ArrayList<>();
        Map<PartitionKey, Integer> index = new HashMap<>();
        int[] group = new int[n];
        for (int r = 0; r < n; r++) {
            if (r > 0 && PartitionKeys.samePartition(values, r - 1, r)) {
                group[r] = group[r - 1];
            } else {
                PartitionKey k = keys.key(values, r);
                Integer g = index.get(k);
                if (g == null) {
                    g = partitions.size();
                    partitions.add(k);
                    index.put(k, g);
                }
                group[r] = g;
            }
        }
        if (partitions.size() == 1) {
            open.computeIfAbsent(partitions.get(0), Rolling::new).write(columns, first, end);
            return;
        }
        int[] counts = new int[partitions.size()];
        for (int g : group) {
            counts[g]++;
        }
        int[][] rows = new int[partitions.size()][];
        for (int g = 0; g < rows.length; g++) {
            rows[g] = new int[counts[g]];
            counts[g] = 0;
        }
        for (int r = 0; r < n; r++) {
            rows[group[r]][counts[group[r]]++] = first + r;
        }
        try (Arena arena = Arena.ofConfined()) {
            for (int g = 0; g < rows.length; g++) {
                List<VectorBuffers> gathered = Gather.rows(columns, rows[g], arena);
                open.computeIfAbsent(partitions.get(g), Rolling::new).write(gathered, 0, rows[g].length);
            }
        }
    }

    /** The data files this task wrote, once; open files are finished first. */
    public DataFile[] dataFiles() {
        close();
        return done.toArray(new DataFile[0]);
    }

    /** Finishes the open files, if any. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (current != null) {
            current.finish();
        }
        for (Rolling r : open.values()) {
            r.finish();
        }
    }

    /**
     * Abandons the task: closes the open files and deletes every file it wrote,
     * as Iceberg's writer cleans up a failed task. Errors while closing and
     * deleting are suppressed onto {@code cause}.
     */
    public void abort(Throwable cause) {
        closed = true;
        if (current != null) {
            current.abort(cause);
        }
        for (Rolling r : open.values()) {
            r.abort(cause);
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
