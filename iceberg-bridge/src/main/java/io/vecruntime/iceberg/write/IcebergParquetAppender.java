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
import java.util.Locale;
import java.util.Map;

import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.parquet.write.ColumnarParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * An Iceberg {@link FileAppender} for Parquet data files over the engine's columnar
 * writer, {@link ColumnarParquetWriter} (#842, #901). It takes batches in the engine's
 * Arrow layout instead of rows. Everything Iceberg-specific is Iceberg's own:
 * <ul>
 *   <li>the file schema is {@code ParquetSchemaUtil.convert(schema)}, so the field ids
 *       and type annotations are the ones Iceberg's writer puts in the file, and the
 *       footer carries the {@code iceberg.schema} key as Iceberg's does;</li>
 *   <li>{@link #metrics} is {@code ParquetUtil.footerMetrics} over the footer the writer
 *       wrote, under the table's {@link MetricsConfig}, so value and null counts and the
 *       truncated bounds are computed by the code that computes them for Iceberg's own
 *       files; the NaN counts and NaN-free bounds of float and double fields, which a
 *       footer does not have, are collected from the batches with Iceberg's own field
 *       metrics builders ({@link FloatingMetrics}), as Iceberg's writer collects them;</li>
 *   <li>the {@code write.parquet.*} table properties set row-group, page and dictionary
 *       sizes, the page row limit, the codec and the Bloom filter columns.</li>
 * </ul>
 * The values are encoded by the writer the Spark write path uses; nothing here encodes.
 * The file is written with Parquet v2 data pages and encodings, which the writer always
 * uses, where Iceberg's own writer defaults to v1 pages; every Parquet reader Iceberg
 * uses reads both.
 */
public final class IcebergParquetAppender implements FileAppender<IcebergParquetAppender.Batch> {

    /** Rows {@code [first, end)} of a batch, one vector per top-level field of the schema, in order. */
    public record Batch(List<? extends VectorBuffers> columns, int first, int end) {
        public Batch(List<? extends VectorBuffers> columns, int rows) {
            this(columns, 0, rows);
        }
    }

    private static final String BLOOM_ENABLED_PREFIX = TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
    private static final String BLOOM_FPP_PREFIX = TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX;
    private static final String BLOOM_NDV_PREFIX = TableProperties.PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX;

    private final ColumnarParquetWriter writer;
    private final MetricsConfig metricsConfig;
    private final NameMapping nameMapping;
    private final FloatingMetrics floating;
    private Metrics metrics;
    private List<Long> splitOffsets;

    private IcebergParquetAppender(ColumnarParquetWriter writer, MetricsConfig metricsConfig,
            NameMapping nameMapping, FloatingMetrics floating) {
        this.writer = writer;
        this.floating = floating;
        this.metricsConfig = metricsConfig;
        this.nameMapping = nameMapping;
    }

    /**
     * A new data file at {@code file} (which must not exist) for rows of {@code schema}.
     *
     * @param properties the table's properties ({@code write.parquet.*} are read)
     * @param metricsConfig the table's metrics modes ({@code MetricsConfig.forTable})
     * @param nameMapping the table's name mapping, or {@code null}
     * @param conf the Hadoop configuration the page codecs are created with
     */
    public static IcebergParquetAppender create(OutputFile file, Schema schema, Map<String, String> properties,
            MetricsConfig metricsConfig, NameMapping nameMapping, Configuration conf) throws IOException {
        if (!FloatingMetrics.check(schema)) {
            throw new UnsupportedOperationException(
                    "a float or double inside a list or a map is not supported: " + schema);
        }
        MessageType fileSchema = IcebergParquetInterop.fileSchema(schema, "table");
        int pageSize = intProp(properties, TableProperties.PARQUET_PAGE_SIZE_BYTES,
                TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT);
        ParquetProperties.Builder props = ParquetProperties.builder()
                .withPageSize(pageSize)
                .withPageRowCountLimit(intProp(properties, TableProperties.PARQUET_PAGE_ROW_LIMIT,
                        TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT))
                .withDictionaryPageSize(intProp(properties, TableProperties.PARQUET_DICT_SIZE_BYTES,
                        TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT))
                .withDictionaryEncoding(true)
                .withMaxBloomFilterBytes(intProp(properties, TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES,
                        TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT));
        for (Map.Entry<String, String> e : properties.entrySet()) {
            if (e.getKey().startsWith(BLOOM_ENABLED_PREFIX)) {
                props.withBloomFilterEnabled(e.getKey().substring(BLOOM_ENABLED_PREFIX.length()),
                        Boolean.parseBoolean(e.getValue().trim()));
            } else if (e.getKey().startsWith(BLOOM_FPP_PREFIX)) {
                props.withBloomFilterFPP(e.getKey().substring(BLOOM_FPP_PREFIX.length()),
                        Double.parseDouble(e.getValue().trim()));
            } else if (e.getKey().startsWith(BLOOM_NDV_PREFIX)) {
                props.withBloomFilterNDV(e.getKey().substring(BLOOM_NDV_PREFIX.length()),
                        Long.parseLong(e.getValue().trim()));
            }
        }
        String codecName = properties.getOrDefault(TableProperties.PARQUET_COMPRESSION,
                TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
        CompressionCodecName codec = CompressionCodecName.fromConf(codecName.toUpperCase(Locale.ROOT));
        String level = properties.get(TableProperties.PARQUET_COMPRESSION_LEVEL);
        Configuration codecConf = conf;
        if (level != null) {
            codecConf = new Configuration(conf);
            // The keys parquet-java's codecs read their level from, as Iceberg's writer sets them.
            if (codec == CompressionCodecName.ZSTD) {
                codecConf.set("parquet.compression.codec.zstd.level", level);
            } else if (codec == CompressionCodecName.GZIP) {
                codecConf.set("zlib.compress.level", level);
            }
        }
        CodecFactory codecs = new CodecFactory(codecConf, pageSize);
        long rowGroupSize = longProp(properties, TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
        ColumnarParquetWriter writer = new ColumnarParquetWriter(IcebergParquetInterop.parquetOutput(file),
                fileSchema, props.build(), codecs.getCompressor(codec), rowGroupSize,
                Map.of("iceberg.schema", SchemaParser.toJson(schema)));
        return new IcebergParquetAppender(writer, metricsConfig, nameMapping, new FloatingMetrics(schema));
    }

    private static int intProp(Map<String, String> p, String key, int dflt) {
        String v = p.get(key);
        return v == null ? dflt : Integer.parseInt(v.trim());
    }

    private static long longProp(Map<String, String> p, String key, long dflt) {
        String v = p.get(key);
        return v == null ? dflt : Long.parseLong(v.trim());
    }

    @Override
    public void add(Batch batch) {
        write(batch.columns(), batch.first(), batch.end());
    }

    /** Appends rows {@code [first, end)} of {@code columns}, one vector per top-level schema field. */
    public void write(List<? extends VectorBuffers> columns, int first, int end) {
        try {
            writer.write(columns, first, end);
            floating.add(columns, first, end);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Rows appended so far. */
    public long rowCount() {
        return writer.rowCount();
    }

    /** The file's metrics, from its footer; available once {@link #close} has returned. */
    @Override
    public Metrics metrics() {
        if (metrics == null) {
            throw new IllegalStateException("metrics are available once the appender is closed");
        }
        return metrics;
    }

    /** Bytes written plus the open row group's estimate; the exact size once closed. */
    @Override
    public long length() {
        try {
            return writer.length();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Each row group's starting offset, the file's split points; available once closed. */
    @Override
    public List<Long> splitOffsets() {
        return splitOffsets;
    }

    @Override
    public void close() throws IOException {
        if (metrics != null) {
            return;
        }
        writer.close();
        ParquetMetadata footer = writer.footer();
        List<Long> offsets = new ArrayList<>(footer.getBlocks().size());
        for (BlockMetaData block : footer.getBlocks()) {
            offsets.add(block.getStartingPos());
        }
        offsets.sort(null);
        splitOffsets = List.copyOf(offsets);
        metrics = ParquetUtil.footerMetrics(IcebergParquetInterop.shadedFooter(footer), floating.metrics(),
                metricsConfig, nameMapping);
    }
}
