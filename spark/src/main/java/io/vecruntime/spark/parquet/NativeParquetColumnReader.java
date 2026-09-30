/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
package io.vecruntime.spark.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.kernels.parquet.ColumnChunkDecoder;
import io.vecruntime.kernels.parquet.GroupUnpacker;
import io.vecruntime.kernels.parquet.ParquetPageDecoder;
import io.vecruntime.spark.arrow.ArrowOutput;
import io.vecruntime.spark.arrow.ArrowSegments;
import io.vecruntime.spark.arrow.ArrowVectorBuffers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.spark.sql.types.DataType;

/**
 * Reads one Parquet column chunk of a row group -- its dictionary page and every
 * data page -- straight into a single Arrow {@link FieldVector} sized for the
 * whole row group, through {@link ColumnChunkDecoder} (the kernels' dependency-
 * free page decoder). The node ({@code VectorParquetScanExec}) then emits
 * {@code columnBatchSize} batches as offset views over this vector, so no
 * per-batch copy happens (STATUS-1b binding contract).
 *
 * <p>parquet-java hands us the decompressed page bytes ({@link DataPage}); the
 * definition levels and the {@code RLE_DICTIONARY} ids are decoded through
 * parquet-java's generated {@code BytePacker}, injected into the decoder as a
 * {@link GroupUnpacker} cached per bit width (requirement 1: no scalar/{@code
 * MemorySegment} reader on this path). The id/level scratch (inside the decoder)
 * and the UTF8 sink are reused for the reader lifetime (requirement 3).
 *
 * <p>Not thread safe: one reader per column per task. Slice 1 supports {@code
 * PLAIN} and {@code RLE_DICTIONARY}/{@code PLAIN_DICTIONARY} value encodings and
 * throws on any other (delta, byte-stream-split); the planner keeps the flag off
 * by default and the column-index page filter still applies.
 */
public final class NativeParquetColumnReader {

    private final ColumnDescriptor column;
    private final VecType type;
    private final VecType physicalType;
    private final DataType sparkType;
    private final String name;
    private final int maxDefLevel;
    private final BufferAllocator allocator;

    /**
     * Cached per bit width for the lifetime of the reader (levels + every id
     * width seen).
     */
    private final GroupUnpacker[] unpackers = new GroupUnpacker[33];

    /** Reused UTF8 sink + offsets scratch across row groups (UTF8 columns only). */
    private final Arena scratch;

    private NativeUtf8Sink utf8Sink;
    private MemorySegment utf8Offsets;
    private int utf8OffsetsCapacity;

    public NativeParquetColumnReader(ColumnDescriptor column, VecType type, DataType sparkType,
            String name, BufferAllocator allocator) {
        this.column = column;
        this.type = type;
        this.physicalType = physicalTypeOf(column, type);
        this.sparkType = sparkType;
        this.name = name;
        this.maxDefLevel = column.getMaxDefinitionLevel();
        this.allocator = allocator;
        this.scratch = Arena.ofShared();
    }

    /**
     * The Parquet <em>physical</em> read type. It equals the output lane except
     * for a narrow decimal whose Parquet physical type is {@code INT32}
     * (precision at most 9) while its output lane is the {@code INT64} unscaled
     * value: those values are read as ints and sign-extended into the long
     * lane.
     */
    private static VecType physicalTypeOf(ColumnDescriptor column, VecType lane) {
        switch (column.getPrimitiveType().getPrimitiveTypeName()) {
            case INT32:
                return VecType.INT32;
            case INT64:
                return VecType.INT64;
            case DOUBLE:
                return VecType.FLOAT64;
            case BINARY:
                return VecType.UTF8;
            default:
                // FLOAT, INT96, FIXED_LEN_BYTE_ARRAY, BOOLEAN: not reached (the planner refuses the column).
                return lane;
        }
    }

    /** The BytePacker factory the decoder injects, cached per bit width. */
    @SuppressWarnings("deprecation") // BytePacker.unpack8Values(byte[]...) is the byte[] path the seam needs
    private GroupUnpacker unpackerFor(int bitWidth) {
        if (bitWidth == 0) {
            return null;
        }
        GroupUnpacker u = unpackers[bitWidth];
        if (u == null) {
            u = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth)::unpack8Values;
            unpackers[bitWidth] = u;
        }
        return u;
    }

    /**
     * Decodes the whole column chunk of {@code pages} ({@code rowGroupRows}
     * rows) into one finished Arrow {@link FieldVector}. The caller owns the
     * returned vector and closes it when the row group's batches are done.
     */
    public FieldVector readRowGroup(PageReader pages, int rowGroupRows) {
        VectorBuffers dictionary = decodeDictionary(pages);
        if (type == VecType.UTF8) {
            return readUtf8RowGroup(pages, rowGroupRows, dictionary);
        }
        return readFixedRowGroup(pages, rowGroupRows, dictionary);
    }

    // ------------------------------------------------------------------ fixed

    private FieldVector readFixedRowGroup(PageReader pages, int rowGroupRows, VectorBuffers dictionary) {
        ArrowVectorBuffers out = ArrowOutput.allocateFixed(name, sparkType, rowGroupRows, allocator);
        ColumnChunkDecoder decoder = new ColumnChunkDecoder(
                physicalType,
                type,
                maxDefLevel,
                rowGroupRows,
                out.data(),
                out.validity(),
                null,
                null,
                this::unpackerFor);
        if (dictionary != null) {
            decoder.setDictionary(dictionary);
        }
        drivePages(pages, decoder, rowGroupRows);
        // allValid when the column has no null definition levels: the validity buffer is already all ones.
        ArrowOutput.finish(out, rowGroupRows, maxDefLevel == 0);
        return (FieldVector) out.vector();
    }

    // ------------------------------------------------------------------ utf8

    private FieldVector readUtf8RowGroup(PageReader pages, int rowGroupRows, VectorBuffers dictionary) {
        // The output byte total is unknown up front (as it is for Spark's reader): decode into a reusable
        // native sink + offsets scratch, then size the VarCharVector exactly and copy once.
        NativeUtf8Sink sink = utf8Sink();
        sink.reset();
        MemorySegment offsets = utf8Offsets(rowGroupRows);
        MemorySegment validity = ArrowLayout.allocateBitmap(scratch, rowGroupRows);
        ColumnChunkDecoder decoder = new ColumnChunkDecoder(VecType.UTF8, maxDefLevel, rowGroupRows, sink.segment(), validity,
                offsets, sink, this::unpackerFor);
        if (dictionary != null) {
            decoder.setDictionary(dictionary);
        }
        drivePages(pages, decoder, rowGroupRows);
        long bytes = sink.length();
        VarCharVector v = (VarCharVector) ArrowOutput.newVector(name, sparkType, allocator);
        v.allocateNew(Math.max(bytes, 1L), rowGroupRows);
        ArrowVectorBuffers vb = ArrowVectorBuffers.forWrite(v, rowGroupRows);
        MemorySegment.copy(offsets, 0L, vb.offsets(), 0L,
                ((long) rowGroupRows + 1) << 2);
        MemorySegment.copy(sink.segment(), 0L, vb.data(), 0L,
                bytes);
        if (maxDefLevel == 0) {
            Bitmap.fill(vb.validity(), rowGroupRows, true);
        } else {
            MemorySegment.copy(validity, 0L, vb.validity(), 0L,
                    Bitmap.bytesFor(rowGroupRows));
        }
        v.setLastSet(rowGroupRows - 1);
        v.setValueCount(rowGroupRows);
        return v;
    }

    // ------------------------------------------------------------------ page loop

    private void drivePages(PageReader pages, ColumnChunkDecoder decoder, int rowGroupRows) {
        while (decoder.rowsWritten() < rowGroupRows) {
            DataPage page = pages.readPage();
            if (page == null) {
                throw new IllegalStateException("ran out of pages at "
                        + decoder.rowsWritten()
                        + " of "
                        + rowGroupRows
                        + " for "
                        + java.util.Arrays.toString(column.getPath()));
            }
            decoder.decodePage(toKernelPage(page));
        }
    }

    private ColumnChunkDecoder.Page toKernelPage(DataPage page) {
        if (page instanceof DataPageV1 v1) {
            byte[] data = bytes(v1.getBytes());
            return ColumnChunkDecoder.Page.v1(data, v1.getValueCount(), encoding(v1.getValueEncoding()));
        }
        DataPageV2 v2 = (DataPageV2) page;
        byte[] levels = bytes(v2.getDefinitionLevels());
        byte[] values = bytes(v2.getData());
        // Lay the definition-level slice and the value slice end to end into one buffer for the reader
        // offsets (repetition levels are absent on a flat column).
        byte[] data = new byte[levels.length + values.length];
        System.arraycopy(levels, 0, data, 0, levels.length);
        System.arraycopy(values, 0, data, levels.length, values.length);
        return ColumnChunkDecoder.Page.v2(data, levels.length, v2.getValueCount(), encoding(v2.getDataEncoding()));
    }

    @SuppressWarnings("deprecation") // PLAIN_DICTIONARY is the legacy data-page dictionary encoding
    private static ParquetPageDecoder.Encoding encoding(Encoding e) {
        if (e == Encoding.PLAIN) {
            return ParquetPageDecoder.Encoding.PLAIN;
        }
        if (e == Encoding.RLE_DICTIONARY || e == Encoding.PLAIN_DICTIONARY) {
            return ParquetPageDecoder.Encoding.RLE_DICTIONARY;
        }
        throw new UnsupportedOperationException("unsupported Parquet value encoding " + e + " (slice 1 decodes PLAIN and dictionary only)");
    }

    // ------------------------------------------------------------------ dictionary

    private VectorBuffers decodeDictionary(PageReader pages) {
        DictionaryPage dp = pages.readDictionaryPage();
        if (dp == null) {
            return null;
        }
        byte[] data = bytes(dp.getBytes());
        int numValues = dp.getDictionarySize();
        return ParquetPageDecoder.decodeDictionary(MemorySegment.ofArray(data), 0L, data.length, numValues, physicalType,
                scratch);
    }

    // ------------------------------------------------------------------ helpers

    private NativeUtf8Sink utf8Sink() {
        if (utf8Sink == null) {
            utf8Sink = new NativeUtf8Sink(scratch);
        }
        return utf8Sink;
    }

    private MemorySegment utf8Offsets(int rowGroupRows) {
        if (utf8OffsetsCapacity < rowGroupRows + 1) {
            utf8OffsetsCapacity = Math.max(rowGroupRows + 1, 2 * utf8OffsetsCapacity);
            utf8Offsets = scratch.allocate((long) utf8OffsetsCapacity << 2, 8);
        }
        return utf8Offsets;
    }

    @SuppressWarnings("deprecation") // BytesInput.toByteArray(): the decoder needs a byte[] for the injected unpacker
    private static byte[] bytes(org.apache.parquet.bytes.BytesInput in) {
        try {
            return in.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Releases the reader's scratch arena (UTF8 sink, offsets, dictionary).
     * Call at task end.
     */
    public void close() {
        scratch.close();
    }

    /** The descriptor this reader was built for. */
    public ColumnDescriptor column() {
        return column;
    }

    /**
     * A trivially-correct {@link VectorBuffers} over the finished vector, for
     * tests.
     */
    static VectorBuffers viewOf(FieldVector v, VecType type, int rows,
            boolean hasNulls) {
        MemorySegment validity = hasNulls ? ArrowSegments.of(v.getValidityBuffer()) : null;
        MemorySegment data = ArrowSegments.of(v.getDataBuffer());
        if (type == VecType.UTF8) {
            return SegmentVectorBuffers.utf8(rows, validity, ArrowSegments.of(v.getOffsetBuffer()), data);
        }
        return SegmentVectorBuffers.fixedWidth(type, rows, validity, data);
    }
}
