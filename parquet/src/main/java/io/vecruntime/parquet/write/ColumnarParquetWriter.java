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
    private final WriteRebase rebase;

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
        this(out, schema, props, compressor, rowGroupSize, footer,
                WriteRebase.NONE);
    }

    /**
     * As above, applying {@code rebase} to the file's date and timestamp values
     * (#842): a {@code LEGACY} or {@code EXCEPTION} write rebases or refuses
     * values below the Gregorian calendar switch, as Spark's {@code
     * ParquetWriteSupport} does. {@link WriteRebase#NONE} (the default
     * constructor) writes the proleptic Gregorian calendar as stored.
     */
    public ColumnarParquetWriter(
            OutputFile out,
            MessageType schema,
            ParquetProperties props,
            CompressionCodecFactory.BytesInputCompressor compressor,
            long rowGroupSize,
            Map<String, String> footer,
            WriteRebase rebase)
            throws IOException {
        this.rebase = rebase == null ? WriteRebase.NONE : rebase;
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
                nested[i] = new NestedFieldWriter(schema, f, this.props, rebase);
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
            return new FixedBinaryChunkWriter(FixedBinaryChunkWriter.Kind.INT96, 12, column, props, rebase);
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
                    fast[i] = new FixedWidthChunkWriter(kind, columns.get(i), props, rebase);
                    fast[i].startChunk(pageStore.getPageWriter(columns.get(i)), blooms(columns.get(i)));
                } else {
                    ChunkWriter flat = flatChunkWriter(lane, columns.get(i));
                    if (flat != null) {
                        fast[i] = flat;
                        fast[i].startChunk(pageStore.getPageWriter(columns.get(i)), blooms(columns.get(i)));
                    } else {
                        encoders[i] = ColumnEncoder.forColumn(lane, columns.get(i), rebase);
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
            rowGroupRows += to - from;
            totalRows += to - from;
            // Check the row-group boundary once per slice, not once per batch. A caller that hands the whole
            // file in one write() call (the common unpartitioned path) would otherwise roll only a single row
            // group however small write.parquet.row-group-size-bytes is, because the check ran after the whole
            // batch was buffered. Iceberg's own writer checks the block size as records accrue, so this matches
            // its cadence (a group ends at most one slice past the target) and gives a file the several groups
            // the copy-on-write row-group copy path (#901 M4a) needs to copy the delete-free ones.
            if (bufferedSize() >= rowGroupSize) {
                flushRowGroup();
            }
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
     * Writes one row group whose columns are a mix of chunks copied byte for
     * byte from a source row group and chunks freshly encoded from buffered
     * values (#901 M4a v3 lineage, M4b column-chunk copy). Every column holds
     * the same {@code rowCount} rows, in source order, so the group is valid.
     *
     * <p>This is the partial-columnar-merge unit: a copy-on-write rewrite that
     * drops no row from this group (a v3 DELETE copy, or an UPDATE/MERGE group
     * whose rows all survive) copies the chunks it does not touch and re-encodes
     * only the rest. On a v3 DELETE copy the two row-lineage chunks are encoded
     * (their values come from the scan, which already resolved {@code _row_id}
     * and {@code _last_updated_sequence_number} per surviving row) while every
     * data chunk is copied. On an UPDATE/MERGE the changed columns are encoded
     * and the untouched ones copied.
     *
     * <p>The columns are written in this writer's (output) schema order. A
     * copied chunk keeps its codec, statistics, column index, offset index and
     * Bloom filter, read from {@code source}. An encoded chunk goes through the
     * same page, encoding and statistics code {@link #write} uses, so its
     * statistics reach the footer the same way. Any rows buffered in the open
     * row group are flushed first, so this group keeps its order in the file.
     *
     * <p>Only flat top-level columns are supported here (the copy-on-write
     * write schema is flat: data columns and, on v3, two lineage longs); a
     * nested column makes the caller fall back to the full rewrite, as the
     * schema guard already does.
     *
     * @param reader a reader on the source file, for its column index, offset
     *     index and Bloom filter (read per copied chunk)
     * @param source a seekable stream on the same file, positioned by {@code
     *     appendColumnChunk} to each copied chunk's pages
     * @param sourceBlock the source group whose chunks are copied; its columns
     *     are in the source schema's order, which equals this writer's data
     *     columns' order (the schema guard verified it)
     * @param copy per output column, whether to copy its chunk from {@code
     *     sourceBlock} ({@code true}) or encode it from {@code encodedRuns}
     *     ({@code false})
     * @param encodedRuns the group's values as one or more schema-ordered
     *     column sets whose rows concatenate to the whole group (a group may
     *     span several buffered batches); the encoded columns are read, a
     *     copied column's entry is ignored
     * @param rowCount the group's row count (every column's)
     */
    public void copyRowGroupColumns(org.apache.parquet.hadoop.ParquetFileReader reader, org.apache.parquet.io.SeekableInputStream source, org.apache.parquet.hadoop.metadata.BlockMetaData sourceBlock,
            boolean[] copy, List<? extends List<? extends VectorBuffers>> encodedRuns, int rowCount)
            throws IOException {
        if (closed) {
            throw new IllegalStateException("writer is closed");
        }
        if (copy.length != columns.size()) {
            throw new IllegalArgumentException("expected " + columns.size() + " columns, got " + copy.length);
        }
        flushRowGroup();

        // The source block's per-LEAF chunk metadata, in leaf order (depth-first over the schema). The source
        // schema equals this writer's copied fields (the schema guard verified it), so the copied fields' leaf
        // chunks are exactly the leading source chunks, in order. A nested copied field copies ALL its leaf
        // chunks; a flat copied field copies its one chunk.
        List<org.apache.parquet.hadoop.metadata.ColumnChunkMetaData> srcChunks = sourceBlock.getColumns();
        // All output leaf descriptors in schema (leaf) order, so a copied chunk is appended under the right one.
        List<ColumnDescriptor> outLeaves = schema.getColumns();

        file.startBlock(rowCount);
        // Copied chunks first (in leaf order), then encoded chunks: the physical order of chunks within a block
        // is free (the footer records each chunk's offset and a reader seeks by it), and keeping copies and
        // encodes in two passes avoids interleaving a direct file append with a page-store flush.
        //
        // The source file holds a chunk for every DATA column (copied OR changed/encoded), in schema order, but
        // NOT the v3 lineage columns (which the source never materialises). So the source leaf cursor `srcIdx`
        // must advance over a changed data column's leaves too -- otherwise a later copied column would read the
        // wrong source chunk. It must NOT advance over a source-absent (lineage) encoded column. A column is
        // source-absent iff it is a v3 lineage column (reserved field id).
        int leaf = 0;
        int srcIdx = 0;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            int leaves = leafCount(schema.getType(i));
            boolean sourceAbsent = isLineageField(schema.getType(i));
            if (copy[i]) {
                for (int l = 0; l < leaves; l++) {
                    ColumnDescriptor desc = outLeaves.get(leaf + l);
                    org.apache.parquet.hadoop.metadata.ColumnChunkMetaData src = srcChunks.get(srcIdx + l);
                    file.appendColumnChunk(desc, source, src, reader.readBloomFilter(src),
                            reader.readColumnIndex(src), reader.readOffsetIndex(src));
                }
            }
            if (!sourceAbsent) {
                srcIdx += leaves; // this data column occupies leaves in the source, whether copied or re-encoded
            }
            leaf += leaves;
        }
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (copy[i]) {
                continue;
            }
            if (nested[i] != null) {
                throw new IllegalArgumentException("mixed-group copy can ENCODE only flat columns; field " + schema.getType(i).getName() + " is nested (a changed nested column must take the full rewrite)");
            }
            encodeColumnChunk(i, encodedRuns, rowCount);
        }
        file.endBlock();
        totalRows += rowCount;
    }

    /**
     * Whether a schema field is a v3 row-lineage column (reserved field id),
     * which a source file never holds.
     */
    private static boolean isLineageField(org.apache.parquet.schema.Type type) {
        if (type.getId() == null) {
            return false;
        }
        int id = type.getId().intValue();
        return id == Integer.MAX_VALUE - 107 || id == Integer.MAX_VALUE - 108;
    }

    /**
     * The number of leaf (primitive) columns under a schema field: 1 for a flat
     * field, more for a nested one.
     */
    private static int leafCount(org.apache.parquet.schema.Type type) {
        if (type.isPrimitive()) {
            return 1;
        }
        int n = 0;
        for (org.apache.parquet.schema.Type f : type.asGroupType().getFields()) {
            n += leafCount(f);
        }
        return n;
    }

    /**
     * Encodes one output column {@code i} into its own single-column chunk and
     * writes it to the file at the current position (within an open block). The
     * column's values come from {@code runs}: each run is a schema-ordered
     * column set holding a contiguous slice of the group's rows (a group may
     * span several buffered batches), and they are written in order so the one
     * chunk holds the whole group. The column is bound and written exactly as
     * {@link #write} binds and writes it, into a one-column page store so the
     * one chunk can be flushed on its own.
     */
    private void encodeColumnChunk(int i, List<? extends List<? extends VectorBuffers>> runs,
            int rowCount)
            throws IOException {
        if (nested[i] != null) {
            throw new IllegalArgumentException("mixed-group copy supports flat columns only; field " + schema.getType(i).getName() + " is nested");
        }
        int runTotal = 0;
        for (List<? extends VectorBuffers> run : runs) {
            runTotal += run.get(i).length();
        }
        if (runTotal != rowCount) {
            throw new IllegalArgumentException("encoded column "
                    + columns.get(i)
                    + " has "
                    + runTotal
                    + " rows across runs, expected "
                    + rowCount);
        }
        ColumnDescriptor desc = columns.get(i);
        VecType lane = runs.get(0)
                           .get(i)
                           .type();
        MessageType oneCol = new MessageType(schema.getName(), schema.getType(i));
        ColumnChunkPageWriteStore pages = new ColumnChunkPageWriteStore(compressor, oneCol, new HeapByteBufferAllocator(), props.getColumnIndexTruncateLength(),
                props.getPageWriteChecksumEnabled());
        org.apache.parquet.column.values.bloomfilter.BloomFilterWriter bloom = props.isBloomFilterEnabled(desc) ? pages.getBloomFilterWriter(desc) : null;
        FixedWidthChunkWriter.Kind kind = FixedWidthChunkWriter.kindFor(lane, desc);
        if (kind != null) {
            ChunkWriter w = new FixedWidthChunkWriter(kind, desc, props);
            w.startChunk(pages.getPageWriter(desc), bloom);
            for (List<? extends VectorBuffers> run : runs) {
                writeInSlices(w, run.get(i),
                        run.get(i).length());
            }
            w.endChunk();
        } else if (lane == VecType.UTF8 && desc.getMaxRepetitionLevel() == 0 && desc.getPrimitiveType().getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY) {
            ChunkWriter w = new BinaryChunkWriter(desc, props);
            w.startChunk(pages.getPageWriter(desc), bloom);
            for (List<? extends VectorBuffers> run : runs) {
                writeInSlices(w, run.get(i),
                        run.get(i).length());
            }
            w.endChunk();
        } else {
            ColumnEncoder enc = ColumnEncoder.forColumn(lane, desc);
            ColumnWriteStore store = props.newColumnWriteStore(oneCol, pages, pages);
            org.apache.parquet.column.ColumnWriter cw = store.getColumnWriter(desc);
            for (List<? extends VectorBuffers> run : runs) {
                VectorBuffers values = run.get(i);
                int n = values.length();
                for (int from = 0; from < n; from += SLICE) {
                    int to = Math.min(n, from + SLICE);
                    enc.write(values, from, to, cw);
                    for (int r = from; r < to; r++) {
                        store.endRecord();
                    }
                }
            }
            store.flush();
            store.close();
        }
        pages.flushToFileWriter(file);
        pages.close();
    }

    /**
     * Writes rows {@code [0, rowCount)} of {@code values} to a fast {@link
     * ChunkWriter} in slices.
     */
    private void writeInSlices(ChunkWriter w, VectorBuffers values, int rowCount) throws IOException {
        for (int from = 0; from < rowCount; from += SLICE) {
            w.write(values, from, Math.min(rowCount, from + SLICE));
        }
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
