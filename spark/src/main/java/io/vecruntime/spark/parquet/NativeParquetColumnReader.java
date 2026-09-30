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
import org.apache.parquet.bytes.BytesInput;
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
 * data page -- into a single Arrow {@link FieldVector} sized for the whole row
 * group, through {@link ColumnChunkDecoder}. The decoder stages values into
 * reused heap primitive arrays with plain stores and this reader flushes the
 * finished row group into the Arrow buffers with ONE bulk {@code MemorySegment.copy}
 * per buffer (the write-path rewrite: per-value FFM segment writes were the
 * measured e2e cost). The node ({@code VectorParquetScanExec}) then emits
 * {@code columnBatchSize} batches as offset views over the finished vector.
 *
 * <p>parquet-java hands us the decompressed page bytes ({@link DataPage}); the
 * definition levels and the dictionary ids are decoded through parquet-java's
 * generated {@code BytePacker}, injected as a {@link GroupUnpacker} cached per
 * bit width. The page bytes are read into a reused {@link ReusableByteOut} (no
 * per-page {@code toByteArray}); the level/id/value staging and the dictionary
 * are reused for the reader lifetime.
 *
 * <p>Not thread safe: one reader per column per task. Slice 1 supports {@code
 * PLAIN} and {@code RLE_DICTIONARY}/{@code PLAIN_DICTIONARY} value encodings and
 * throws on any other; the planner keeps the flag off by default.
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

    /**
     * Reused decoder + decompression buffer + dictionary scratch across row
     * groups.
     */
    private final Arena scratch;

    private ColumnChunkDecoder decoder;
    private final ReusableByteOut pageOut = new ReusableByteOut(1 << 16);

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
                return lane;
        }
    }

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
        if (decoder == null) {
            decoder = new ColumnChunkDecoder(physicalType, type, maxDefLevel, rowGroupRows, this::unpackerFor);
        } else {
            decoder.reset(rowGroupRows);
        }
        // The dictionary is per COLUMN CHUNK (per row group), not per file: decode it fresh each row group.
        VectorBuffers dict = decodeDictionary(pages);
        if (dict != null) {
            decoder.setDictionary(dict);
        }
        drivePages(pages, rowGroupRows);
        if (type == VecType.UTF8) {
            return flushUtf8(rowGroupRows);
        }
        return flushFixed(rowGroupRows);
    }

    private FieldVector flushFixed(int rowGroupRows) {
        ArrowVectorBuffers out = ArrowOutput.allocateFixed(name, sparkType, rowGroupRows, allocator);
        decoder.flushFixed(out.data(), out.validity());
        ArrowOutput.finish(out, rowGroupRows, maxDefLevel == 0);
        return (FieldVector) out.vector();
    }

    private FieldVector flushUtf8(int rowGroupRows) {
        long bytes = decoder.utf8Bytes();
        VarCharVector v = (VarCharVector) ArrowOutput.newVector(name, sparkType, allocator);
        v.allocateNew(Math.max(bytes, 1L), rowGroupRows);
        ArrowVectorBuffers vb = ArrowVectorBuffers.forWrite(v, rowGroupRows);
        decoder.flushUtf8(vb.offsets(), vb.data(), vb.validity());
        v.setLastSet(rowGroupRows - 1);
        v.setValueCount(rowGroupRows);
        return v;
    }

    // ------------------------------------------------------------------ page loop

    private void drivePages(PageReader pages, int rowGroupRows) {
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
            pageOut.reset();
            writeInto(v1.getBytes(), pageOut);
            return ColumnChunkDecoder.Page.v1(pageOut.array(), v1.getValueCount(), encoding(v1.getValueEncoding()));
        }
        DataPageV2 v2 = (DataPageV2) page;
        // Levels then values, back to back into the reused buffer -- no per-page toByteArray, no concat alloc.
        pageOut.reset();
        writeInto(v2.getDefinitionLevels(), pageOut);
        int levelsLength = pageOut.size();
        writeInto(v2.getData(), pageOut);
        return ColumnChunkDecoder.Page.v2(pageOut.array(), levelsLength, v2.getValueCount(),
                encoding(v2.getDataEncoding()));
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
        // The dictionary is decoded ONCE per chunk; a plain toByteArray here is not a hot path.
        byte[] data = bytes(dp.getBytes());
        int numValues = dp.getDictionarySize();
        return ParquetPageDecoder.decodeDictionary(MemorySegment.ofArray(data), 0L, data.length, numValues, physicalType,
                scratch);
    }

    // ------------------------------------------------------------------ helpers

    private static void writeInto(BytesInput in, ReusableByteOut out) {
        try {
            in.writeAllTo(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("deprecation") // BytesInput.toByteArray(): dictionary page only, decoded once
    private static byte[] bytes(BytesInput in) {
        try {
            return in.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Releases the reader's scratch arena (dictionary). Call at task end. */
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
