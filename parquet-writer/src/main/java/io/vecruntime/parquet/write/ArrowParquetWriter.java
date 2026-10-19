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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Writes Arrow column vectors to one Parquet file, column by column (#64,
 * #842).
 *
 * <p>The rows never exist: each batch is written one column at a time into
 * parquet-java's {@link ColumnWriteStore}, whose pages go through a {@link
 * ColumnChunkPageWriteStore} to a {@link ParquetFileWriter}, the same page,
 * chunk and footer code parquet-java's own writer uses. What it skips is the
 * record path: no {@code RecordConsumer}, no {@code WriteSupport}, and the
 * row-group size is checked once per batch instead of once per record.
 *
 * <p>The schema is a flat {@link MessageType} whose fields are in the order of
 * the vectors handed to {@link #write}; the caller derives it (the Spark
 * adapter uses Spark's own schema converter, so the file's schema is the one
 * Spark would write). Each column is bound to a {@link ColumnEncoder} by its
 * Arrow vector type and Parquet primitive type.
 *
 * <p>Pages are cut by parquet-java's own size checks, which run when a record
 * ends: a batch is written in slices of {@link #SLICE} rows, every column's
 * slice first, then the slice's records are ended, so a page is at most one
 * slice past the page-size threshold.
 */
public final class ArrowParquetWriter implements AutoCloseable {

    /** Rows per slice: the granularity of the page-size checks. */
    static final int SLICE = 1024;

    private final ParquetFileWriter file;
    private final MessageType schema;
    private final ParquetProperties props;
    private final CompressionCodecFactory.BytesInputCompressor compressor;
    private final long rowGroupSize;
    private final Map<String, String> footer;
    private final List<ColumnDescriptor> columns;
    private final ColumnEncoder[] encoders;

    private ColumnChunkPageWriteStore pageStore;
    private ColumnWriteStore columnStore;
    private long rowGroupRows;
    private long totalRows;
    private boolean closed;

    /**
     * @param out the file to write (created, not overwritten)
     * @param schema the flat file schema, one field per vector in {@link
     *     #write}'s order
     * @param props page size, dictionary, writer version, statistics and index
     *     settings
     * @param compressor the page compressor (parquet-java's {@code
     *     CodecFactory})
     * @param rowGroupSize the buffered bytes at which a row group is flushed
     *     ({@code parquet.block.size})
     * @param footer key-value metadata for the footer (Spark's version and
     *     schema keys)
     */
    public ArrowParquetWriter(OutputFile out, MessageType schema, ParquetProperties props,
            CompressionCodecFactory.BytesInputCompressor compressor, long rowGroupSize, Map<String, String> footer)
            throws IOException {
        this.schema = schema;
        this.props = props;
        this.compressor = compressor;
        this.rowGroupSize = rowGroupSize;
        this.footer = footer;
        this.columns = schema.getColumns();
        for (ColumnDescriptor c : columns) {
            if (c.getMaxRepetitionLevel() != 0 || c.getPath().length != 1) {
                throw new IllegalArgumentException("nested column not supported yet: " + String.join(".", c.getPath()));
            }
        }
        this.encoders = new ColumnEncoder[columns.size()];
        this.file = new ParquetFileWriter(out, schema, ParquetFileWriter.Mode.CREATE, rowGroupSize, 0, null,
                props);
        this.file.start();
        newRowGroup();
    }

    private void newRowGroup() {
        pageStore = new ColumnChunkPageWriteStore(compressor, schema, new HeapByteBufferAllocator(), props.getColumnIndexTruncateLength(),
                props.getPageWriteChecksumEnabled());
        columnStore = props.newColumnWriteStore(schema, pageStore, pageStore);
        rowGroupRows = 0;
    }

    /**
     * Appends the first {@code rowCount} rows of {@code vectors}, one vector
     * per schema field, in order. The vectors are read, not retained.
     */
    public void write(List<? extends FieldVector> vectors, int rowCount) throws IOException {
        if (vectors.size() != columns.size()) {
            throw new IllegalArgumentException("expected " + columns.size() + " vectors, got " + vectors.size());
        }
        for (int i = 0; i < encoders.length; i++) {
            FieldVector v = vectors.get(i);
            if (encoders[i] == null || !encoders[i].accepts(v)) {
                encoders[i] = ColumnEncoder.forVector(v, columns.get(i));
            }
        }
        for (int from = 0; from < rowCount; from += SLICE) {
            int to = Math.min(rowCount, from + SLICE);
            for (int i = 0; i < encoders.length; i++) {
                encoders[i].write(vectors.get(i), from, to, columnStore.getColumnWriter(columns.get(i)));
            }
            for (int r = from; r < to; r++) {
                columnStore.endRecord();
            }
        }
        rowGroupRows += rowCount;
        totalRows += rowCount;
        if (columnStore.getBufferedSize() >= rowGroupSize) {
            flushRowGroup();
        }
    }

    private void flushRowGroup() throws IOException {
        if (rowGroupRows == 0) {
            return;
        }
        file.startBlock(rowGroupRows);
        columnStore.flush();
        pageStore.flushToFileWriter(file);
        file.endBlock();
        columnStore.close();
        pageStore.close();
        newRowGroup();
    }

    /** Rows written so far. */
    public long rowCount() {
        return totalRows;
    }

    /** Flushes the open row group and writes the footer. */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            flushRowGroup();
            file.end(footer);
        } finally {
            columnStore.close();
            pageStore.close();
        }
    }
}
