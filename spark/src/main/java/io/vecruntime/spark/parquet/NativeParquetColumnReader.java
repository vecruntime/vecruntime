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
import java.util.ArrayDeque;
import java.util.Deque;

import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.kernels.parquet.ColumnChunkDecoder;
import io.vecruntime.kernels.parquet.GroupUnpacker;
import io.vecruntime.kernels.parquet.ParquetPageDecoder;
import io.vecruntime.spark.arrow.ArrowOutput;
import io.vecruntime.spark.arrow.ArrowVectorBuffers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
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
 * Streaming reader for one Parquet column chunk: {@link #readBatch} decodes the
 * next {@code n} rows of the current row group STRAIGHT into a fresh (or pooled)
 * batch-sized Arrow {@link FieldVector} that the emitted batch owns -- no
 * row-group-sized vector is ever materialised and there is no per-batch copy of
 * the row group. This is #559 slice 1's scan decode core (the streaming rewrite;
 * the earlier row-group-at-a-time version staged the whole group and the node
 * sliced+copied each batch out, which walked the group's null count per batch --
 * 32% of the 1 TB CPU -- and copied twice).
 *
 * <h2>How a batch is produced</h2>
 * The reader holds the chunk's {@link PageReader}, a resumable {@link
 * ColumnChunkDecoder}, and the once-decoded dictionary. {@link #readBatch}
 * borrows a batch-sized vector from a small pool (or allocates one), then loops:
 * whenever the decoder {@linkplain ColumnChunkDecoder#needsPage() needs a page}
 * it pulls and decompresses the next {@link DataPage} lazily and feeds it, and
 * the decoder fills the batch staging arrays for as many rows as the current
 * page holds. A batch therefore spans any number of pages and resumes mid-run
 * (the decoder keeps the RLE/bit-packed id-reader state). The finished batch
 * staging copies into the vector's Arrow buffers in one bulk {@code
 * MemorySegment.copy} per buffer; the null count comes from the definition-level
 * pass, so validity is written (and the buffer kept) only when the batch has
 * nulls -- Arrow's {@code getNullCount} is never called.
 *
 * <h2>Batch ownership and the pool</h2>
 * Each emitted batch OWNS its vector; the consumer releases it (which returns the
 * vector to this reader's pool for reuse) on batch release / close. Nothing is
 * shared across batches, so a retained or serialized batch (a broadcast build,
 * the shuffle writer) ships only its own rows.
 *
 * <p>Not thread safe: one reader per column per task. Slice 1 supports {@code
 * PLAIN} and {@code RLE_DICTIONARY}/{@code PLAIN_DICTIONARY} value encodings.
 */
public final class NativeParquetColumnReader {

    private final ColumnDescriptor column;
    private final VecType type;
    private final VecType physicalType;
    private final DataType sparkType;
    private final String name;
    private final int maxDefLevel;
    private final BufferAllocator allocator;
    // TINYINT / SMALLINT: the INT32 lane's values are narrowed to these many bits (0: not narrow).
    private final int narrowBits;
    // A FIXED_LEN_BYTE_ARRAY / BINARY decimal, staged as bytes and converted at flush; its FLBA width (0: BINARY).
    private final boolean binaryDecimal;
    private final int fixedLength;
    // TIMESTAMP stored as MILLIS: the lane holds micros, so each present value is scaled by 1000.
    private final boolean millis;
    private static final java.lang.foreign.ValueLayout.OfLong LONG_LE = java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final java.lang.foreign.ValueLayout.OfInt INT_LE = java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private final GroupUnpacker[] unpackers = new GroupUnpacker[33];
    private final Arena scratch;
    private final ColumnChunkDecoder decoder;
    private final ReusableByteOut pageOut = new ReusableByteOut(1 << 16);

    // Per-chunk (row-group) state.
    private PageReader pages;
    private int rowGroupRows;

    // A small pool of released batch vectors to reuse (bounded; extras are closed). Guarded by its own
    // monitor: with decode-ahead (#606) the producer thread borrows while the task thread releases. On JDK 25 a
    // monitor no longer pins a virtual thread (JEP 491).
    private final Deque<FieldVector> pool = new ArrayDeque<>();
    private int maxPool = 4;
    private boolean closed;

    public NativeParquetColumnReader(ColumnDescriptor column, VecType type, DataType sparkType,
            String name, int batchRows, BufferAllocator allocator) {
        this.column = column;
        this.type = type;
        this.physicalType = physicalTypeOf(column, type);
        this.sparkType = sparkType;
        this.name = name;
        this.maxDefLevel = column.getMaxDefinitionLevel();
        this.allocator = allocator;
        this.narrowBits = sparkType instanceof org.apache.spark.sql.types.ByteType
                ? 8
                : sparkType instanceof org.apache.spark.sql.types.ShortType ? 16 : 0;
        this.millis = (sparkType instanceof org.apache.spark.sql.types.TimestampType || sparkType instanceof org.apache.spark.sql.types.TimestampNTZType)
                      && column.getPrimitiveType().getLogicalTypeAnnotation() instanceof org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation t
                      && t.getUnit() == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
        // Dictionary scratch is GC-managed: closing a shared Arena runs a JVM-wide handshake that walks every
        // thread's stack, and a reader is made per column per file -- on 1 TB TPC-DS (14,594 store_sales
        // files) those handshakes were 8-9% of executor CPU in q88. An automatic arena has no close.
        this.scratch = Arena.ofAuto();
        // A decimal stored as FIXED_LEN_BYTE_ARRAY or BINARY: its big-endian bytes are staged on the decoder's
        // UTF8 path (PLAIN, dictionary, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY) and converted into the INT64
        // lane (p <= 18) or the DECIMAL128 lane by flushDecimal.
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName ptn = column.getPrimitiveType().getPrimitiveTypeName();
        this.binaryDecimal = sparkType instanceof org.apache.spark.sql.types.DecimalType && (ptn == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY || ptn == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY);
        this.fixedLength = ptn == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ? column.getPrimitiveType().getTypeLength() : 0;
        if (binaryDecimal) {
            this.decoder = new ColumnChunkDecoder(VecType.UTF8, VecType.UTF8, maxDefLevel, batchRows, this::unpackerFor);
            this.decoder.setFixedLength(fixedLength);
        } else {
            this.decoder = new ColumnChunkDecoder(physicalType, type, maxDefLevel, batchRows, this::unpackerFor);
            if (type == VecType.UTF8 && fixedLength > 0) {
                // A FIXED_LEN_BYTE_ARRAY column read as BINARY: fixed-length values with no length prefix.
                this.decoder.setFixedLength(fixedLength);
            }
        }
    }

    private static VecType physicalTypeOf(ColumnDescriptor column, VecType lane) {
        switch (column.getPrimitiveType().getPrimitiveTypeName()) {
            case INT32:
                return VecType.INT32;
            case INT64:
                return VecType.INT64;
            case DOUBLE:
                return VecType.FLOAT64;
            case BOOLEAN:
                return VecType.BOOL;
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
     * Begin a new row group of {@code rowGroupRows} rows on this column's {@code
     * pages}. Decodes the chunk's dictionary page once (if present) and resets the
     * decoder; {@link #readBatch} then streams the pages.
     */
    public void startRowGroup(PageReader pages, int rowGroupRows) {
        this.pages = pages;
        this.rowGroupRows = rowGroupRows;
        decoder.startChunk(rowGroupRows);
        releaseChunkDictionary();
        VectorBuffers dict = decodeDictionary(pages);
        if (dict != null) {
            decoder.setDictionary(dict);
            if (dictionaryIds) {
                // #612: the row group's dictionary as one Arrow vector, shared by every batch emitted as ids.
                chunkDict = new SharedDictionary(io.vecruntime.spark.arrow.ArrowOutput.copyDictionary(name + ".dictionary", dict, allocator));
            }
        }
    }

    // ---- dictionary-encoded string output (#612) ----

    /**
     * A row group's dictionary, shared by its id batches; closed when the
     * reader and every batch let go.
     */
    private static final class SharedDictionary {
        final org.apache.arrow.vector.VarCharVector vector;
        private int refs = 1; // the reader's own, dropped at the next row group / close

        SharedDictionary(org.apache.arrow.vector.VarCharVector vector) {
            this.vector = vector;
        }

        synchronized void retain() {
            refs++;
        }

        synchronized void release() {
            if (--refs == 0) {
                vector.close();
            }
        }
    }

    private boolean dictionaryIds;
    private SharedDictionary chunkDict;
    private final java.util.IdentityHashMap<FieldVector, SharedDictionary> idBatches = new java.util.IdentityHashMap<>();
    private final Deque<FieldVector> idPool = new ArrayDeque<>();

    /**
     * Emit a string column's dictionary-encoded batches as dictionary ids over
     * the row group's dictionary ({@link #wrap} gives the {@code
     * VectorDictionaryColumnVector}), so the operators above compute on ids, as
     * they do over Spark's scan. Only for a UTF8 string column read through the
     * UTF8 path (not binary, not a fixed-length or byte-array decimal).
     */
    public void setDictionaryIds(boolean enabled) {
        boolean ok = enabled
                && type == VecType.UTF8
                && !binaryDecimal
                && fixedLength == 0
                && sparkType instanceof org.apache.spark.sql.types.StringType;
        this.dictionaryIds = ok;
        if (type == VecType.UTF8) {
            decoder.setEmitDictionaryIds(ok);
        }
    }

    /**
     * The batch vector as Spark's column: a dictionary view for an id batch,
     * else the plain Arrow wrapper.
     */
    public org.apache.spark.sql.vectorized.ColumnVector wrap(FieldVector v) {
        SharedDictionary d;
        synchronized (pool) {
            d = idBatches.get(v);
        }
        if (d != null) {
            return io.vecruntime.spark.arrow.VectorDictionaryColumnVector.borrowed((org.apache.arrow.vector.IntVector) v, d.vector);
        }
        return io.vecruntime.spark.arrow.ArrowOutput.wrap(v, sparkType);
    }

    private FieldVector flushIds(int rows, boolean hasNulls) {
        org.apache.arrow.vector.IntVector v;
        synchronized (pool) {
            v = (org.apache.arrow.vector.IntVector) idPool.pollFirst();
        }
        long needValidity = io.vecruntime.kernels.Bitmap.bytesFor(rows);
        if (v != null && (v.getDataBuffer().capacity() < (long) rows * 4 || v.getValidityBuffer().capacity() < needValidity)) {
            v.close();
            v = null;
        }
        if (v == null) {
            v = new org.apache.arrow.vector.IntVector(name, allocator);
            v.allocateNew(rows);
        }
        ArrowVectorBuffers out = ArrowVectorBuffers.forWrite(v, rows, org.apache.spark.sql.types.DataTypes.IntegerType);
        decoder.flushIds(rows, out.data(),
                hasNulls ? out.validity() : null);
        io.vecruntime.spark.arrow.ArrowOutput.finish(out, rows, !hasNulls);
        chunkDict.retain();
        synchronized (pool) {
            idBatches.put(v, chunkDict);
        }
        return v;
    }

    private void releaseChunkDictionary() {
        if (chunkDict != null) {
            chunkDict.release();
            chunkDict = null;
        }
    }

    /** Rows still unemitted in the current row group. */
    public int rowsRemaining() {
        return rowGroupRows - decoder.rowsDone();
    }

    /**
     * Decode the next {@code n} rows of the current row group into a fresh (or
     * pooled) batch-owned Arrow vector and return it. The caller owns the vector
     * and releases it with {@link #release}. {@code n} must be <= the rows
     * remaining in the row group.
     *
     * <p>Fixed-width lanes (INT32/INT64/FLOAT64) are decoded STRAIGHT into the
     * vector's off-heap Arrow data and validity buffers -- no heap staging, no
     * bulk copy ({@link #fillFixed}, decoder's {@code readBatchDirectA}). The
     * #559 JMH A/B (ParquetScanE2EBenchmark) measured this direct write within
     * noise of the earlier staging + one bulk copy (135.9 vs 135.2 ms/op), so the
     * direct form is kept to meet the "decode straight into the output, no
     * intermediate copies" requirement. UTF8 stays on the staging path (its two
     * passes already size one data buffer and bulk-copy offsets+bytes once).
     */
    public FieldVector readBatch(int n) {
        decoder.startBatch(n);
        if (type == VecType.UTF8) {
            int filled = 0;
            while (filled < n) {
                if (decoder.needsPage()) {
                    decoder.feedPage(toKernelPage(requirePage()));
                }
                filled += decoder.readBatch(n - filled, filled);
            }
            if (decoder.batchIsDictionaryIds()) {
                return flushIds(n, decoder.batchNullCount() > 0);
            }
            return flushUtf8(n, decoder.batchNullCount() > 0);
        }
        if (type == VecType.BOOL || binaryDecimal) {
            int filled = 0;
            while (filled < n) {
                if (decoder.needsPage()) {
                    decoder.feedPage(toKernelPage(requirePage()));
                }
                filled += decoder.readBatch(n - filled, filled);
            }
            boolean nulls = decoder.batchNullCount() > 0;
            return binaryDecimal ? flushDecimal(n, nulls) : flushBool(n, nulls);
        }
        return fillFixed(n, /* modeA= */ true);
    }

    /**
     * A FIXED_LEN_BYTE_ARRAY / BINARY decimal batch: the staged big-endian
     * bytes into the INT64 or DECIMAL128 lane.
     */
    private FieldVector flushDecimal(int rows, boolean hasNulls) {
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(rows);
        ArrowVectorBuffers out = ArrowVectorBuffers.forWrite(v, rows, sparkType);
        decoder.flushDecimal(rows, out.data(),
                hasNulls ? out.validity() : null, type == VecType.DECIMAL128);
        ArrowOutput.finish(out, rows, !hasNulls);
        return v;
    }

    private DataPage requirePage() {
        DataPage page = pages.readPage();
        if (page == null) {
            throw new IllegalStateException("ran out of pages at "
                    + decoder.rowsDone()
                    + " of "
                    + rowGroupRows
                    + " for "
                    + java.util.Arrays.toString(column.getPath()));
        }
        return page;
    }

    /**
     * Decode a fixed-width batch straight into the borrowed vector's off-heap
     * buffers. {@code modeA}: hoisted {@code MemorySegment} + counted loop (the
     * production write); otherwise a {@code ByteBuffer} LE view -- the #559 A/B's
     * second variant, kept only for the benchmark.
     */
    private FieldVector fillFixed(int n, boolean modeA) {
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(n);
        ArrowVectorBuffers out = ArrowVectorBuffers.forWrite(v, n, sparkType);
        java.lang.foreign.MemorySegment data = out.data();
        java.lang.foreign.MemorySegment validity = maxDefLevel > 0 ? out.validity() : null;
        if (validity != null) {
            // The decoder read-modify-writes each 64-bit validity word; zero the words it will touch first.
            int words = (n + 63) >>> 6;
            validity.asSlice(0L, (long) words * 8).fill((byte) 0);
        }
        java.nio.ByteBuffer nio = modeA ? null : v.getDataBuffer().nioBuffer(0L, n * v.getTypeWidth());
        int filled = 0;
        while (filled < n) {
            if (decoder.needsPage()) {
                decoder.feedPage(toKernelPage(requirePage()));
            }
            filled += modeA ? decoder.readBatchDirectA(n - filled, filled, data, validity) : decoder.readBatchDirectB(n - filled, filled, nio, validity);
        }
        boolean hasNulls = decoder.batchNullCount() > 0;
        if (narrowBits != 0) {
            narrow(data, n, narrowBits);
        }
        if (millis) {
            millisToMicros(data, hasNulls ? validity : null, n);
        }
        io.vecruntime.spark.arrow.ArrowOutput.finish(out, n, !hasNulls);
        return v;
    }

    /**
     * TIMESTAMP(MILLIS) on the INT64 lane of micros: Spark's {@code
     * LongAsMicrosUpdater} multiplies each value by 1000 with {@code
     * Math.multiplyExact}, so an out-of-range value fails the read with an
     * {@link ArithmeticException}, never a wrapped instant. Same here, on the
     * present rows only: a null slot holds whatever the reused buffer held,
     * which must not raise.
     */
    private static void millisToMicros(java.lang.foreign.MemorySegment data, java.lang.foreign.MemorySegment validity, int n) {
        for (int i = 0; i < n; i++) {
            if (validity != null && (validity.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i >>> 3) & (1 << (i & 7))) == 0) {
                continue;
            }
            long at = (long) i << 3;
            data.set(LONG_LE, at, Math.multiplyExact(data.get(LONG_LE, at), 1000L));
        }
    }

    /**
     * TINYINT / SMALLINT on the INT32 lane: Spark's readers (vectorized and
     * row-based) narrow each INT32 value with a {@code (byte)} / {@code (short)}
     * cast, so a file value outside the declared range wraps. The lane must hold
     * the narrowed value too -- operators compute on the {@code int}, not on
     * {@code getByte} -- so sign-extend the low {@code bits} of each slot in
     * place. Null slots are narrowed as well, harmlessly.
     */
    private static void narrow(java.lang.foreign.MemorySegment data, int n, int bits) {
        int shift = 32 - bits;
        for (int i = 0; i < n; i++) {
            long at = (long) i << 2;
            int x = data.get(INT_LE, at);
            data.set(INT_LE, at, (x << shift) >> shift);
        }
    }

    // ----- #559 JMH A/B entry points (ParquetScanE2EBenchmark only) -----
    // readBatchStaging: the earlier path (heap staging arrays + one bulk MemorySegment.copy per buffer).
    // readBatchDirectA: production (hoisted MemorySegment + counted loop). readBatchDirectB: ByteBuffer LE
    // view. All three decode identically; the benchmark compares their ms/op. Not for production callers.

    public FieldVector readBatchStaging(int n) {
        decoder.startBatch(n);
        int filled = 0;
        while (filled < n) {
            if (decoder.needsPage()) {
                decoder.feedPage(toKernelPage(requirePage()));
            }
            filled += decoder.readBatch(n - filled, filled);
        }
        boolean hasNulls = decoder.batchNullCount() > 0;
        if (binaryDecimal) {
            return flushDecimal(n, hasNulls);
        }
        return type == VecType.UTF8
                ? flushUtf8(n, hasNulls)
                : type == VecType.BOOL ? flushBool(n, hasNulls) : flushFixed(n, hasNulls);
    }

    public FieldVector readBatchDirectA(int n) {
        return readBatch(n);
    }

    public FieldVector readBatchDirectB(int n) {
        if (type == VecType.UTF8 || type == VecType.BOOL || binaryDecimal) {
            return readBatchStaging(n);
        }
        decoder.startBatch(n);
        return fillFixed(n, /* modeA= */ false);
    }

    private FieldVector flushFixed(int rows, boolean hasNulls) {
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(rows);
        ArrowVectorBuffers out = ArrowVectorBuffers.forWrite(v, rows, sparkType);
        decoder.flushFixed(rows, out.data(),
                hasNulls ? out.validity() : null);
        if (narrowBits != 0) {
            narrow(out.data(), rows, narrowBits);
        }
        if (millis) {
            millisToMicros(out.data(), hasNulls ? out.validity() : null,
                    rows);
        }
        // finish: set the Arrow value count and (when no nulls) mark all valid without a validity scan.
        ArrowOutput.finish(out, rows, !hasNulls);
        return v;
    }

    private FieldVector flushBool(int rows, boolean hasNulls) {
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(rows);
        ArrowVectorBuffers out = ArrowVectorBuffers.forWrite(v, rows, sparkType);
        decoder.flushBool(rows, out.data(),
                hasNulls ? out.validity() : null);
        ArrowOutput.finish(out, rows, !hasNulls);
        return v;
    }

    private FieldVector flushUtf8(int rows, boolean hasNulls) {
        long bytes = decoder.utf8Bytes();
        org.apache.arrow.vector.BaseVariableWidthVector v = (org.apache.arrow.vector.BaseVariableWidthVector) borrowUtf8(rows, bytes);
        ArrowVectorBuffers vb = ArrowVectorBuffers.forWrite(v, rows, sparkType);
        decoder.flushUtf8(rows, vb.offsets(), vb.data(),
                hasNulls ? vb.validity() : null);
        if (!hasNulls) {
            io.vecruntime.kernels.Bitmap.fill(vb.validity(), rows, true);
        }
        v.setLastSet(rows - 1);
        v.setValueCount(rows);
        return v;
    }

    // ------------------------------------------------------------------ batch vector pool

    private FieldVector borrowFixed(int rows) {
        long needValidity = io.vecruntime.kernels.Bitmap.bytesFor(rows);
        FieldVector v = pooled();
        while (v != null) {
            BaseFixedWidthVector fv = (BaseFixedWidthVector) v;
            // A BitVector has type width 0: its data is a bitmap, written by flushBool in whole 64-bit words.
            long needData = type == VecType.BOOL ? (long) ((rows + 63) >>> 6) << 3 : (long) rows * fv.getTypeWidth();
            if (v.getDataBuffer().capacity() >= needData && v.getValidityBuffer().capacity() >= needValidity) {
                fv.setValueCount(0);
                return v;
            }
            v.close();
            v = pooled();
        }
        BaseFixedWidthVector nv = (BaseFixedWidthVector) ArrowOutput.newVector(name, sparkType, allocator);
        nv.allocateNew(rows);
        return nv;
    }

    private FieldVector borrowUtf8(int rows, long bytes) {
        FieldVector v = pooled();
        while (v != null) {
            if (v.getValidityBuffer().capacity() >= io.vecruntime.kernels.Bitmap.bytesFor(rows) && v.getOffsetBuffer().capacity() >= (long) (rows + 1) * 4 && v.getDataBuffer().capacity() >= Math.max(bytes, 1L)) {
                ((org.apache.arrow.vector.BaseVariableWidthVector) v).setValueCount(0);
                return v;
            }
            v.close();
            v = pooled();
        }
        org.apache.arrow.vector.BaseVariableWidthVector nv = (org.apache.arrow.vector.BaseVariableWidthVector) ArrowOutput.newVector(name, sparkType, allocator);
        nv.allocateNew(Math.max(bytes, 1L), rows);
        return nv;
    }

    /**
     * Return a batch vector to the pool for reuse (or close it if the pool is
     * full).
     */
    public void release(FieldVector v) {
        if (v == null) {
            return;
        }
        SharedDictionary d;
        synchronized (pool) {
            d = idBatches.remove(v);
        }
        if (d != null) {
            // An id batch (#612): drop its hold on the row group's dictionary, pool the ids.
            d.release();
            synchronized (pool) {
                if (!closed && idPool.size() < maxPool) {
                    idPool.addLast(v);
                    return;
                }
            }
            v.close();
            return;
        }
        synchronized (pool) {
            if (!closed && pool.size() < maxPool) {
                pool.addLast(v);
                return;
            }
        }
        v.close();
    }

    /**
     * How many released vectors the pool keeps (decode-ahead keeps more batches
     * in flight).
     */
    public void setPoolLimit(int limit) {
        synchronized (pool) {
            maxPool = Math.max(1, limit);
        }
    }

    private FieldVector pooled() {
        synchronized (pool) {
            return pool.pollFirst();
        }
    }

    private ColumnChunkDecoder.Page toKernelPage(DataPage page) {
        if (page instanceof DataPageV1 v1) {
            pageOut.reset();
            writeInto(v1.getBytes(), pageOut);
            return ColumnChunkDecoder.Page.v1(pageOut.array(), pageOut.size(), v1.getValueCount(),
                    encoding(v1.getValueEncoding()));
        }
        DataPageV2 v2 = (DataPageV2) page;
        pageOut.reset();
        writeInto(v2.getDefinitionLevels(), pageOut);
        int levelsLength = pageOut.size();
        writeInto(v2.getData(), pageOut);
        // The reused buffer is larger than the page: pass the page's real extent.
        return ColumnChunkDecoder.Page.v2At(pageOut.array(), 0, levelsLength, levelsLength,
                pageOut.size() - levelsLength, v2.getValueCount(), encoding(v2.getDataEncoding()));
    }

    @SuppressWarnings("deprecation") // PLAIN_DICTIONARY is the legacy data-page dictionary encoding
    private static ParquetPageDecoder.Encoding encoding(Encoding e) {
        if (e == Encoding.PLAIN) {
            return ParquetPageDecoder.Encoding.PLAIN;
        }
        if (e == Encoding.RLE_DICTIONARY || e == Encoding.PLAIN_DICTIONARY) {
            return ParquetPageDecoder.Encoding.RLE_DICTIONARY;
        }
        if (e == Encoding.RLE) {
            return ParquetPageDecoder.Encoding.RLE; // BOOLEAN values (the only RLE value encoding in data pages)
        }
        if (e == Encoding.DELTA_BINARY_PACKED) {
            return ParquetPageDecoder.Encoding.DELTA_BINARY_PACKED;
        }
        if (e == Encoding.DELTA_LENGTH_BYTE_ARRAY) {
            return ParquetPageDecoder.Encoding.DELTA_LENGTH_BYTE_ARRAY;
        }
        if (e == Encoding.DELTA_BYTE_ARRAY) {
            return ParquetPageDecoder.Encoding.DELTA_BYTE_ARRAY;
        }
        if (e == Encoding.BYTE_STREAM_SPLIT) {
            return ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT;
        }
        throw new UnsupportedOperationException("unsupported Parquet value encoding " + e + " (decodes PLAIN, dictionary, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT)");
    }

    private VectorBuffers decodeDictionary(PageReader pages) {
        DictionaryPage dp = pages.readDictionaryPage();
        if (dp == null) {
            return null;
        }
        byte[] data = bytes(dp.getBytes());
        int numValues = dp.getDictionarySize();
        return ParquetPageDecoder.decodeDictionary(MemorySegment.ofArray(data), 0L, data.length, numValues,
                binaryDecimal ? VecType.UTF8 : physicalType, fixedLength, scratch);
    }

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

    /**
     * Releases the pool. The scratch arena is automatic: the GC frees it once
     * the reader is unreachable. A vector released after this is closed, not pooled.
     */
    public void close() {
        synchronized (pool) {
            closed = true;
            for (FieldVector v : pool) {
                v.close();
            }
            pool.clear();
            for (FieldVector v : idPool) {
                v.close();
            }
            idPool.clear();
        }
        // Batches still out keep the dictionary until they are released; the reader's own hold goes now.
        releaseChunkDictionary();
    }

    public ColumnDescriptor column() {
        return column;
    }
}
