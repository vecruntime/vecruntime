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
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

/**
 * Bin-packs the copyable source files of one {@code rewrite_data_files} file
 * group into target-sized output data files by copying their Parquet row groups
 * byte for byte (#957 M4c). All the files handed to one instance share the same
 * partition (the runner groups by partition first) and the same Parquet schema
 * (each was proven copyable against the table write schema by
 * {@link BinPackCopyPlanner}), so their compressed pages are all valid under one
 * output footer and can be concatenated with no decode or re-encode.
 *
 * <p>Row groups are appended through {@link IcebergParquetAppender#copyRowGroup}
 * -- i.e. parquet-java's {@code ParquetFileWriter.appendRowGroup}, which keeps
 * each chunk's codec, statistics, column index, offset index, Bloom filter and
 * dictionary pages. When the current output file reaches {@code
 * targetFileSize}, it is closed into a {@link DataFile} (metrics, split offsets
 * and size come from {@link IcebergParquetAppender}, the same footer path
 * Iceberg's own writer uses) and a fresh one is opened. The output file's
 * schema is the table write schema; because every input matches it, a copied
 * group's statistics land under the same field ids Iceberg would use.
 *
 * <p>Files the planner declined are NOT handled here; the runner routes those
 * to the columnar decode/encode path and merges the two output sets.
 */
public final class BinPackCopyWriter implements AutoCloseable {

    /**
     * Row groups copied byte for byte across all bin-pack tasks (test/metric
     * counter).
     */
    public static final LongAdder COPIED_ROW_GROUPS = new LongAdder();

    /**
     * Output files produced by copy across all bin-pack tasks (test/metric
     * counter).
     */
    public static final LongAdder COPIED_OUTPUT_FILES = new LongAdder();

    private final IcebergWriteSpec spec;
    private final OutputFileFactory files;
    private final MetricsConfig metricsConfig;
    private final NameMapping nameMapping;
    private final Configuration conf;
    private final Schema writeSchema;
    private final long targetFileSize;
    private final StructLike partition;

    private final List<DataFile> done = new ArrayList<>();
    private final List<String> written = new ArrayList<>();

    private IcebergParquetAppender appender;
    private String openLocation;
    private boolean closed;

    /**
     * @param spec the write spec (table, write schema, properties)
     * @param files the output-file factory for this task (already defaulted to
     *     the output spec)
     * @param targetFileSize the byte size at which an output file is rolled
     * @param partition the shared partition tuple of this writer's files (the
     *     source files' own {@code DataFile.partition()}), or null for an
     *     unpartitioned spec. A partitioned table does NOT store the partition
     *     columns in the data file; the value is carried in the manifest, so a
     *     copied file MUST keep its source file's partition tuple or the reader
     *     reconstructs the partition columns as null.
     * @param conf the Hadoop configuration the readers and codecs use
     */
    public BinPackCopyWriter(IcebergWriteSpec spec, OutputFileFactory files, long targetFileSize,
            StructLike partition, Configuration conf) {
        this.spec = spec;
        this.files = files;
        this.metricsConfig = MetricsConfig.forTable(spec.table());
        String mapping = spec.table()
                             .properties()
                             .get(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING);
        this.nameMapping = mapping == null ? null : NameMappingParser.fromJson(mapping);
        this.conf = conf;
        this.writeSchema = spec.writeSchema();
        this.targetFileSize = targetFileSize;
        this.partition = partition;
    }

    /**
     * Copies every row group of one source file into the open output, rolling a
     * new output file whenever the open one reaches the target size. The file's
     * footer is passed in so the caller (which already read it to plan) does not
     * read it twice. The output file uses the SOURCE file's OWN Parquet schema
     * (read from the footer), which the planner proved equals the table write
     * schema's physical form -- using the source schema verbatim guarantees a
     * copied chunk's pages, logical-type annotations and column order are valid
     * under the output footer (a mismatch in a timestamp unit or decimal scale
     * annotation would silently reinterpret the copied bytes).
     *
     * <p>All copyable files handed to one writer share one schema (same
     * partition group, each equal to the write schema), so the first file's
     * schema is the output schema for every one; a later file whose schema
     * somehow differs would fail parquet-java's {@code appendRowGroup} identity
     * check, which the runner catches and falls back from.
     */
    public void copyFile(String location, ParquetMetadata footer) {
        MessageType sourceSchema = footer.getFileMetaData().getSchema();
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
        try {
            HadoopInputFile in = HadoopInputFile.fromPath(path, conf);
            try (SeekableInputStream stream = in.newStream()) {
                for (BlockMetaData block : footer.getBlocks()) {
                    ensureOpen(sourceSchema);
                    appender.copyRowGroup(stream, block);
                    COPIED_ROW_GROUPS.increment();
                    // Roll after a group if the file has reached the target size (whole groups only -- a row
                    // group is the copy unit, so an output file lands at or just over the target).
                    if (appender.length() >= targetFileSize) {
                        rollOutput();
                    }
                }
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private void ensureOpen(MessageType outputSchema) {
        if (appender != null) {
            return;
        }
        EncryptedOutputFile out = partition == null ? files.newOutputFile() : files.newOutputFile(spec.spec(), partition);
        openLocation = out.encryptingOutputFile().location();
        written.add(openLocation);
        try {
            // The copy path writes under the SOURCE file's physical schema (== table write schema's physical
            // form), so a copied group's pages and statistics are valid under this footer.
            appender = IcebergParquetAppender.create(out.encryptingOutputFile(), writeSchema, outputSchema, spec.properties(),
                    metricsConfig, nameMapping, conf);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private void rollOutput() {
        if (appender == null) {
            return;
        }
        try {
            appender.close();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
        DataFiles.Builder b = DataFiles.builder(spec.spec())
                .withPath(openLocation)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(appender.length())
                .withMetrics(appender.metrics())
                // sortOrderId stays 0 (unsorted): a bin-pack copy proves no sort order.
                .withSortOrder(org.apache.iceberg.SortOrder.unsorted())
                .withSplitOffsets(appender.splitOffsets());
        if (partition != null) {
            b.withPartition(partition);
        }
        done.add(b.build());
        COPIED_OUTPUT_FILES.increment();
        appender = null;
        openLocation = null;
    }

    /** Finishes any open output and returns the copied data files. */
    public List<DataFile> dataFiles() {
        close();
        return done;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        rollOutput();
    }

    /** Abandons the task, deleting whatever output files it wrote. */
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

    /**
     * Reads a source file's Parquet footer (shared helper for the runner's
     * planning).
     */
    public static ParquetMetadata readFooter(String location, Configuration conf) {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
        try {
            HadoopInputFile in = HadoopInputFile.fromPath(path, conf);
            try (ParquetFileReader reader = ParquetFileReader.open(in)) {
                return reader.getFooter();
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    /**
     * The partition of a source file, computed from its scan-task partition
     * struct.
     */
    public static StructLike taskPartition(org.apache.iceberg.FileScanTask task) {
        return task.file().partition();
    }

    /**
     * The vanilla (un-shaded) Parquet form of the table write schema, field ids
     * and all, as the engine's writer produces it -- exposed for the bin-pack
     * runner (a different package) so its copyable comparison uses the exact
     * {@link MessageType} the writer and {@link BinPackCopyPlanner} use.
     */
    public static MessageType writeSchema(Schema schema) {
        return IcebergParquetInterop.fileSchema(schema, "table");
    }
}
