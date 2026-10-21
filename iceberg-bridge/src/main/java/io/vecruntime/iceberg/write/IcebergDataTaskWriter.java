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
import java.util.ArrayList;
import java.util.List;

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

/**
 * One task's data files for an Iceberg write into an unpartitioned spec (#842,
 * W2): batches go to an {@link IcebergParquetAppender}, a new file is started
 * once the open one reaches the write's target file size, and {@link #close}
 * returns the files as Iceberg {@link DataFile}s. File names and locations come
 * from Iceberg's own {@link OutputFileFactory} (the table's location provider,
 * the partition and task ids, the operation id), so they are the names
 * Iceberg's writer gives its files.
 *
 * <p>Like Iceberg's {@code RollingDataWriter}, the size is checked between
 * writes: here once per batch, so a file ends at most one batch past the
 * target.
 */
public final class IcebergDataTaskWriter implements AutoCloseable {

    private final IcebergWriteSpec spec;
    private final OutputFileFactory files;
    private final MetricsConfig metricsConfig;
    private final NameMapping nameMapping;
    private final Configuration conf;
    private final List<DataFile> done = new ArrayList<>();
    private final List<String> written = new ArrayList<>();
    private IcebergParquetAppender appender;
    private String location;
    private boolean closed;

    public IcebergDataTaskWriter(IcebergWriteSpec spec, int partitionId, long taskId,
            Configuration conf) {
        if (!spec.spec().isUnpartitioned()) {
            throw new UnsupportedOperationException("partitioned specs are not supported yet: " + spec.spec());
        }
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
    }

    /**
     * Appends rows {@code [first, end)} of {@code columns}, one vector per
     * top-level write-schema field.
     */
    public void write(List<? extends VectorBuffers> columns, int first, int end) {
        if (end <= first) {
            return;
        }
        if (appender == null) {
            open();
        }
        appender.write(columns, first, end);
        if (appender.length() >= spec.targetFileSize()) {
            finishFile();
        }
    }

    private void open() {
        EncryptedOutputFile out = files.newOutputFile();
        location = out.encryptingOutputFile().location();
        written.add(location);
        try {
            appender = IcebergParquetAppender.create(out.encryptingOutputFile(), spec.writeSchema(), spec.properties(),
                    metricsConfig, nameMapping, conf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void finishFile() {
        try {
            appender.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        done.add(
                DataFiles.builder(spec.spec())
                        .withPath(location)
                        .withFormat(FileFormat.PARQUET)
                        .withFileSizeInBytes(appender.length())
                        .withMetrics(appender.metrics())
                        .withSplitOffsets(appender.splitOffsets())
                        .build());
        appender = null;
        location = null;
    }

    /** The data files this task wrote, once; an open file is finished first. */
    public DataFile[] dataFiles() {
        close();
        return done.toArray(new DataFile[0]);
    }

    /** Finishes the open file, if any. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (appender != null) {
            finishFile();
        }
    }

    /**
     * Abandons the task: closes the open file and deletes every file it wrote,
     * as Iceberg's writer cleans up a failed task. Errors while deleting are
     * suppressed onto {@code cause}.
     */
    public void abort(Throwable cause) {
        closed = true;
        if (appender != null) {
            try {
                appender.close();
            } catch (IOException | RuntimeException e) {
                cause.addSuppressed(e);
            }
            appender = null;
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
