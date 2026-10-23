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

import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Writes columns in the engine's Arrow layout ({@link VectorBuffers}) to one
 * Parquet file, column by column (#64, #842).
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
 * lane ({@link VecType}) and Parquet primitive type.
 *
 * <p>Pages are cut by parquet-java's own size checks, which run when a record
 * ends: a batch is written in slices of {@link #SLICE} rows, every column's
 * slice first, then the slice's records are ended, so a page is at most one
 * slice past the page-size threshold.
 */
public final class ColumnarParquetWriter implements AutoCloseable {

    /** Rows per slice: the granularity of the page-size checks. */
    static final int SLICE = 1024;

    private final ParquetFileWriter file;
    private final MessageType schema;
    private final ParquetProperties props;
    private final CompressionCodecFactory.BytesInputCompressor compressor;
    private final long rowGroupSize;
    private final Map<String, String> footer;
    private final List<ColumnDescriptor> columns;
    private final NestedFieldWriter[] nested;
    private final ColumnEncoder[] encoders;
    private final VecType[] lanes;
    private final ChunkWriter[] fast;

    private ColumnChunkPageWriteStore pageStore;
    private ColumnWriteStore columnStore;
    // The columns that go through parquet-java's column writers, once the first batch has bound every column; a
    // column store covers only these, so it never cuts pages for a column written by a FixedWidthChunkWriter.
    private MessageType slowSchema;
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
    public ColumnarParquetWriter(OutputFile out, MessageType schema, ParquetProperties props,
            CompressionCodecFactory.BytesInputCompressor compressor, long rowGroupSize, Map<String, String> footer)
            throws IOException {
        this.schema = schema;
        // The writer writes Parquet v2 only (#64): data page v2 and the v2 encodings, also on the column-store path.
        this.props = ParquetProperties.copy(props)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .build();
        this.compressor = compressor;
        this.rowGroupSize = rowGroupSize;
        this.footer = footer;
        // One entry per top-level field: a flat field's column, or, for a struct, array or map, its nested writer
        // (always on the column-store path).
        this.columns = new java.util.ArrayList<>(schema.getFieldCount());
        this.nested = new NestedFieldWriter[schema.getFieldCount()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            org.apache.parquet.schema.Type f = schema.getType(i);
            if (f.isPrimitive() && !f.isRepetition(org.apache.parquet.schema.Type.Repetition.REPEATED)) {
                columns.add(schema.getColumnDescription(new String[] {f.getName()}));
            } else if (f.isPrimitive()) {
                throw new IllegalArgumentException("repeated primitive field not supported: " + f.getName());
            } else {
                columns.add(null);
                nested[i] = new NestedFieldWriter(schema, f, this.props);
            }
        }
        this.encoders = new ColumnEncoder[columns.size()];
        this.lanes = new VecType[columns.size()];
        this.fast = new ChunkWriter[columns.size()];
        this.file = new ParquetFileWriter(out, schema, ParquetFileWriter.Mode.CREATE, rowGroupSize, 0, null,
                props);
        this.file.start();
        newRowGroup();
    }

    /**
     * Where the row group's Bloom filter for {@code column} goes, or {@code
     * null} when it has none (#842).
     */
    private org.apache.parquet.column.values.bloomfilter.BloomFilterWriter blooms(ColumnDescriptor column) {
        return props.isBloomFilterEnabled(column) ? pageStore.getBloomFilterWriter(column) : null;
    }

    /**
     * A flat chunk writer for a {@code lane} column not covered by {@link
     * FixedWidthChunkWriter} -- a string/binary BINARY, a BOOLEAN, an INT96 or a
     * FIXED_LEN_BYTE_ARRAY -- or {@code null} when the column (nested, or a lane
     * with no flat writer) goes through parquet-java's column writer.
     */
    private ChunkWriter flatChunkWriter(VecType lane, ColumnDescriptor column) {
        if (column.getMaxRepetitionLevel() != 0) {
            return null;
        }
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName p = column.getPrimitiveType().getPrimitiveTypeName();
        if (lane == VecType.UTF8 && p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY) {
            return new BinaryChunkWriter(column, props);
        }
        if (lane == VecType.BOOL && p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            return new BooleanChunkWriter(column, props);
        }
        if (lane == VecType.INT64 && p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96) {
            return new FixedBinaryChunkWriter(FixedBinaryChunkWriter.Kind.INT96, 12, column, props);
        }
        if (p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            int fixedWidth = column.getPrimitiveType().getTypeLength();
            if (lane == VecType.FIXED_BINARY) {
                return new FixedBinaryChunkWriter(FixedBinaryChunkWriter.Kind.FIXED, fixedWidth, column, props);
            }
            if (lane == VecType.DECIMAL128) {
                return new FixedBinaryChunkWriter(FixedBinaryChunkWriter.Kind.DECIMAL, fixedWidth, column, props);
            }
        }
        return null;
    }

    private void newRowGroup() {
        pageStore = new ColumnChunkPageWriteStore(compressor, schema, new HeapByteBufferAllocator(), props.getColumnIndexTruncateLength(),
                props.getPageWriteChecksumEnabled());
        columnStore = slowSchema == null || slowSchema.getFieldCount() == 0
                ? null
                : props.newColumnWriteStore(slowSchema, pageStore, pageStore);
        rowGroupRows = 0;
        if (fast != null) {
            for (int i = 0; i < fast.length; i++) {
                if (fast[i] != null) {
                    fast[i].startChunk(pageStore.getPageWriter(columns.get(i)), blooms(columns.get(i)));
                }
            }
        }
        if (nested != null) {
            for (NestedFieldWriter n : nested) {
                if (n != null) {
                    n.startRowGroup(pageStore);
                }
            }
        }
    }

    /**
     * Appends the first {@code rowCount} rows of {@code columns}, one per
     * schema field, in order. The buffers are read, not retained past the
     * call (a dictionary may be kept, by identity, to reuse its converted
     * entries).
     */
    public void write(List<? extends VectorBuffers> vectors, int rowCount) throws IOException {
        write(vectors, 0, rowCount);
    }

    /**
     * Appends rows {@code [first, end)} of {@code columns}, one per schema
     * field, in order: a range of a batch (a file rolled at {@code
     * maxRecordsPerFile} in the middle of a batch).
     */
    public void write(List<? extends VectorBuffers> vectors, int first, int end) throws IOException {
        int rowCount = end - first;
        if (vectors.size() != columns.size()) {
            throw new IllegalArgumentException("expected " + columns.size() + " columns, got " + vectors.size());
        }
        for (int i = 0; i < encoders.length; i++) {
            VecType lane = vectors.get(i).type();
            if (lanes[i] == null) {
                lanes[i] = lane;
                if (nested[i] != null) {
                    continue;
                }
                FixedWidthChunkWriter.Kind kind = FixedWidthChunkWriter.kindFor(lane, columns.get(i));
                if (kind != null) {
                    fast[i] = new FixedWidthChunkWriter(kind, columns.get(i), props);
                    fast[i].startChunk(pageStore.getPageWriter(columns.get(i)), blooms(columns.get(i)));
                } else {
                    ChunkWriter flat = flatChunkWriter(lane, columns.get(i));
                    if (flat != null) {
                        fast[i] = flat;
                        fast[i].startChunk(pageStore.getPageWriter(columns.get(i)), blooms(columns.get(i)));
                    } else {
                        encoders[i] = ColumnEncoder.forColumn(lane, columns.get(i));
                    }
                }
            } else if (lanes[i] != lane) {
                throw new IllegalArgumentException("column "
                        + columns.get(i)
                        + " changed lane from "
                        + lanes[i]
                        + " to "
                        + lane);
            }
        }
        if (slowSchema == null) {
            List<org.apache.parquet.schema.Type> slow = new java.util.ArrayList<>();
            for (int i = 0; i < encoders.length; i++) {
                // Nested fields write their leaves themselves; only a flat column with neither a fast writer nor
                // a nested writer is left on parquet-java's column store.
                if (fast[i] == null && nested[i] == null) {
                    slow.add(schema.getType(i));
                }
            }
            slowSchema = new MessageType(schema.getName(), slow);
            if (!slow.isEmpty()) {
                columnStore = props.newColumnWriteStore(slowSchema, pageStore, pageStore);
            }
        }
        for (int from = first; from < end; from += SLICE) {
            int to = Math.min(end, from + SLICE);
            for (int i = 0; i < encoders.length; i++) {
                if (fast[i] != null) {
                    fast[i].write(vectors.get(i), from, to);
                } else if (nested[i] != null) {
                    nested[i].write(vectors.get(i), from, to);
                } else {
                    encoders[i].write(vectors.get(i), from, to, columnStore.getColumnWriter(columns.get(i)));
                }
            }
            if (columnStore != null) {
                for (int r = from; r < to; r++) {
                    columnStore.endRecord();
                }
            }
        }
        rowGroupRows += rowCount;
        totalRows += rowCount;
        if (bufferedSize() >= rowGroupSize) {
            flushRowGroup();
        }
    }

    /**
     * Copies one row group {@code block} of {@code source} into this file
     * byte for byte, without decoding it (#901 M4a). The compressed pages are
     * appended as they are, each chunk keeping its codec, statistics, column
     * index, offset index and Bloom filter, through parquet-java's {@code
     * ParquetFileWriter.appendRowGroup}. Any rows buffered in the open row
     * group are flushed first, so copied and encoded groups keep their order
     * in the file.
     *
     * <p>{@code source} must be positioned-readable for the life of the call
     * and describe a file whose schema equals {@link #copyableFrom this
     * writer's schema} (same columns, physical and logical types, repetition
     * and field ids); the caller checks that with {@link #copyableFrom} and
     * falls back to decoding the group when it does not hold. The copied
     * group's statistics reach {@link #footer}, so a metrics builder combines
     * them with the encoded groups' without reading the file again.
     *
     * @param source a seekable stream on the file the group comes from
     * @param block the group's {@code BlockMetaData} from {@code source}'s
     *     footer
     */
    public void copyRowGroup(org.apache.parquet.io.SeekableInputStream source, org.apache.parquet.hadoop.metadata.BlockMetaData block) throws IOException {
        if (closed) {
            throw new IllegalStateException("writer is closed");
        }
        flushRowGroup();
        // dropColumnIndexes = false: keep each chunk's column index, offset index and Bloom filter.
        file.appendRowGroup(source, block, false);
        totalRows += block.getRowCount();
    }

    /**
     * Whether a file with schema {@code fileSchema} can have its row groups
     * copied into this file without decoding (#901 M4a): its columns must be
     * exactly this writer's schema -- same names, order, physical and logical
     * types, repetition and field ids -- so a copied chunk's pages and
     * statistics are valid under this file's footer schema. A file that
     * differs (schema evolution: a renamed, added, dropped, reordered or
     * promoted column, a different field id) is not copyable and the caller
     * rewrites it the normal way. The text form is the contract parquet-java
     * uses between two schemas (see {@code fileSchema} in the Iceberg interop),
     * and it carries the id and logical-type annotations.
     */
    public boolean copyableFrom(MessageType fileSchema) {
        return schema.equals(fileSchema);
    }

    /**
     * Ends the open row group now, if it holds any rows, cutting a row-group
     * boundary at this point (#901 M4a). The M4a rewrite uses it so a group of
     * re-encoded surviving rows is closed before a byte-for-byte copied group
     * follows, keeping the groups separate and in order; {@link #copyRowGroup}
     * calls it for the same reason. A no-op when the open group is empty.
     */
    public void endRowGroup() throws IOException {
        if (closed) {
            throw new IllegalStateException("writer is closed");
        }
        flushRowGroup();
    }

    private void flushRowGroup() throws IOException {
        if (rowGroupRows == 0) {
            return;
        }
        file.startBlock(rowGroupRows);
        for (ChunkWriter f : fast) {
            if (f != null) {
                f.endChunk();
            }
        }
        for (NestedFieldWriter n : nested) {
            if (n != null) {
                n.endRowGroup();
            }
        }
        if (columnStore != null) {
            columnStore.flush();
        }
        pageStore.flushToFileWriter(file);
        file.endBlock();
        if (columnStore != null) {
            columnStore.close();
        }
        pageStore.close();
        newRowGroup();
    }

    private long bufferedSize() {
        long size = columnStore == null ? 0 : columnStore.getBufferedSize();
        for (ChunkWriter f : fast) {
            if (f != null) {
                size += f.bufferedBytes();
            }
        }
        for (NestedFieldWriter n : nested) {
            if (n != null) {
                size += n.bufferedBytes();
            }
        }
        return size;
    }

    /** Rows written so far. */
    public long rowCount() {
        return totalRows;
    }

    /**
     * The file's length so far: the bytes already written plus the open row
     * group's buffered estimate, and the exact file size once {@link #close}
     * has returned. Like Iceberg's {@code FileAppender.length()}, it is what a
     * caller rolls files on ({@code write.target-file-size-bytes}).
     */
    public long length() throws IOException {
        return closed ? file.getPos() : file.getPos() + bufferedSize();
    }

    /**
     * The footer this writer wrote -- schema, row groups, and each column
     * chunk's statistics, encodings, sizes and value counts -- once {@link
     * #close} has returned (#900, #901). It is what a table format needs to
     * describe the file without reading it back: Iceberg builds its {@code
     * Metrics} from it ({@code ParquetUtil.footerMetrics}), keyed by the field
     * ids the schema carries. Like parquet-java's {@code
     * ParquetWriter.getFooter()}.
     *
     * @throws IllegalStateException before {@link #close}, or when close failed
     *     before the footer was written
     */
    public ParquetMetadata footer() {
        if (!closed) {
            throw new IllegalStateException("the footer is written by close()");
        }
        return file.getFooter();
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
            if (columnStore != null) {
                columnStore.close();
            }
            pageStore.close();
        }
    }
}
