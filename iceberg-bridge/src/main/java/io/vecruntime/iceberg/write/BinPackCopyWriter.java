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
import java.util.List;
import java.util.function.LongConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

/**
 * Copies one pre-planned bin -- an ordered list of (source file, row-group
 * ordinals) all sharing one partition -- into a SINGLE output data file by
 * copying the Parquet row groups byte for byte (#957 M4c). One bin becomes one
 * output file, so the driver decides the bin boundaries (each bin filled to the
 * target size) and this writer runs on the EXECUTOR, one Spark task per bin, so
 * the copy is distributed exactly like Iceberg's own runner rather than
 * streamed through the driver.
 *
 * <p>Source files are opened through the table's {@link FileIO} ({@link
 * IcebergParquetInterop#parquetInput}), not Hadoop, so the copy uses the
 * table's configured IO wherever Spark places the task. Row groups are appended
 * through {@link IcebergParquetAppender#copyRowGroup} -- parquet-java's {@code
 * ParquetFileWriter.appendRowGroup}, which keeps each chunk's codec,
 * statistics, column/offset index, Bloom filter and dictionary pages. All files
 * in a bin share one Parquet schema (the planner proved each equals the table
 * write schema), and the output uses the first source file's OWN schema so a
 * copied chunk's pages and logical-type annotations are valid under the output
 * footer. Metrics, split offsets and size come from {@link
 * IcebergParquetAppender}, the same footer path Iceberg's own writer uses; the
 * output keeps the bin's partition tuple (a partitioned table does not store
 * partition columns in the file) and sortOrderId 0.
 */
public final class BinPackCopyWriter implements AutoCloseable {

    private final FileIO io;
    private final PartitionSpec outputSpec;
    private final OutputFileFactory files;
    private final MetricsConfig metricsConfig;
    private final NameMapping nameMapping;
    private final java.util.Map<String, String> properties;
    private final Configuration conf;
    private final Schema writeSchema;
    private final StructLike partition;
    // Called once per copied row group, on the executor, so the task can tally into a Spark accumulator.
    private final LongConsumer onCopiedGroup;

    private IcebergParquetAppender appender;
    private String openLocation;
    private DataFile result;
    private boolean closed;

    /**
     * @param table the (serializable) table -- its {@code FileIO} opens source
     *     and output files, its properties drive {@code write.parquet.*}, and
     *     its {@code MetricsConfig} keys the output metrics
     * @param outputSpec the spec the rewrite writes under
     * @param writeSchema the table's Iceberg write schema
     * @param files the output-file factory for this task
     * @param partition the bin's partition tuple (all its files share it), or
     *     null for an unpartitioned spec
     * @param conf the Hadoop configuration the page codecs are created with
     * @param onCopiedGroup called once per copied row group (executor-side
     *     counter), or null
     */
    public BinPackCopyWriter(
            org.apache.iceberg.Table table,
            PartitionSpec outputSpec,
            Schema writeSchema,
            OutputFileFactory files,
            StructLike partition,
            Configuration conf,
            LongConsumer onCopiedGroup) {
        this.io = table.io();
        this.outputSpec = outputSpec;
        this.writeSchema = writeSchema;
        this.properties = table.properties();
        this.files = files;
        this.metricsConfig = MetricsConfig.forTable(table);
        String mapping = properties.get(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING);
        this.nameMapping = mapping == null ? null : NameMappingParser.fromJson(mapping);
        this.conf = conf;
        this.partition = partition;
        this.onCopiedGroup = onCopiedGroup;
    }

    /**
     * Copies the given row groups of one source file into the open output. The
     * row groups are named by ordinal (their position in the source footer), so
     * the driver's bin plan controls exactly which groups go where; an empty or
     * null {@code ordinals} copies every group.
     */
    public void copyGroups(String location, int[] ordinals) {
        InputFile in = io.newInputFile(location);
        org.apache.parquet.io.InputFile parquetIn = IcebergParquetInterop.parquetInput(in);
        try (ParquetFileReader reader = ParquetFileReader.open(parquetIn)) {
            ParquetMetadata footer = reader.getFooter();
            MessageType sourceSchema = footer.getFileMetaData().getSchema();
            List<BlockMetaData> blocks = footer.getBlocks();
            ensureOpen(sourceSchema);
            try (SeekableInputStream stream = parquetIn.newStream()) {
                if (ordinals == null || ordinals.length == 0) {
                    for (BlockMetaData block : blocks) {
                        appender.copyRowGroup(stream, block);
                        if (onCopiedGroup != null) {
                            onCopiedGroup.accept(1L);
                        }
                    }
                } else {
                    for (int ordinal : ordinals) {
                        appender.copyRowGroup(stream, blocks.get(ordinal));
                        if (onCopiedGroup != null) {
                            onCopiedGroup.accept(1L);
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private void ensureOpen(MessageType sourceSchema) {
        if (appender != null) {
            return;
        }
        EncryptedOutputFile out = partition == null ? files.newOutputFile() : files.newOutputFile(outputSpec, partition);
        openLocation = out.encryptingOutputFile().location();
        try {
            // Output under the SOURCE file's own schema so a copied group's pages/annotations are valid.
            appender = IcebergParquetAppender.create(out.encryptingOutputFile(), writeSchema, sourceSchema, properties, metricsConfig,
                    nameMapping, conf);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    /** Finishes the single output file of this bin and returns its DataFile. */
    public DataFile finish() {
        close();
        return result;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (appender == null) {
            return;
        }
        try {
            appender.close();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
        DataFiles.Builder b = DataFiles.builder(outputSpec)
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
        result = b.build();
    }

    /** Abandons the output, deleting it if written. */
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
        if (openLocation != null) {
            try {
                io.deleteFile(openLocation);
            } catch (RuntimeException e) {
                cause.addSuppressed(e);
            }
        }
    }

    /**
     * Reads a source file's Parquet footer through the table's FileIO
     * (driver-side planning helper).
     */
    public static ParquetMetadata readFooter(FileIO io, String location) {
        try (ParquetFileReader reader = ParquetFileReader.open(IcebergParquetInterop.parquetInput(io.newInputFile(location)))) {
            return reader.getFooter();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
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
