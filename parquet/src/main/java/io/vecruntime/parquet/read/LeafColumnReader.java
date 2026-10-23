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
package io.vecruntime.parquet.read;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Deque;

import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.parquet.codec.ColumnChunkDecoder;
import io.vecruntime.parquet.codec.GroupUnpacker;
import io.vecruntime.parquet.codec.ParquetPageDecoder;
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
public class LeafColumnReader implements ColumnReader, RowLevels {

    private final ColumnDescriptor column;
    private final VecType type;
    private final VecType physicalType;
    private final String name;
    private final int maxDefLevel;
    private final BufferAllocator allocator;
    // TINYINT / SMALLINT: the INT32 lane's values are narrowed to these many bits (0: not narrow).
    private final int narrowBits;
    // #900: FLOAT kept as its own 4-byte bits (a Float4Vector); a string whose dictionary batches may be ids.
    private final boolean float32;
    // A uuid: a FIXED_LEN_BYTE_ARRAY(16) staged on the UTF8 path whose 16 big-endian bytes are formatted into the
    // canonical lowercase 8-4-4-4-12 text at flush, as Iceberg's Spark reader produces it.
    private final boolean uuid;
    private final boolean dictionaryIdsAllowed;
    // #900: the Arrow type of this leaf's batch vectors (the spec's), so vectors are made without the Spark type.
    private final org.apache.arrow.vector.types.pojo.ArrowType arrowType;
    // A FIXED_LEN_BYTE_ARRAY / BINARY decimal, staged as bytes and converted at flush; its FLBA width (0: BINARY).
    private final boolean binaryDecimal;
    private final int fixedLength;
    // TIMESTAMP stored as MILLIS (the lane holds micros) or TIME stored as MICROS (the lane holds nanos, #787):
    // each present value is scaled by 1000.
    private final boolean millis;
    // #568 item 6: a TIMESTAMP stored as INT96 (12 bytes, staged like a fixed-length binary and converted at
    // flush), and the lane's kind for the file's calendar rebase (a date's days, or a timestamp's micros).
    private final boolean int96;
    private final boolean dateLane;
    private final boolean timestampLane;
    private io.vecruntime.parquet.read.CalendarRebase rebase = io.vecruntime.parquet.read.CalendarRebase.NONE;
    // #568 schema evolution: a column read as a wider Spark type than it is stored as, the way Spark's
    // ParquetVectorUpdaterFactory widens it. INT32 or FLOAT values are decoded widened into INT64 slots
    // and converted in place (WIDEN_*); a decimal read at a larger scale has its unscaled values multiplied
    // by 10^k (rescaleFactor for the INT64 lane, rescale digits for the DECIMAL128 lane).
    private static final int WIDEN_NONE = io.vecruntime.parquet.read.LeafReadSpec.WIDEN_NONE;
    private static final int WIDEN_INT_TO_DOUBLE = io.vecruntime.parquet.read.LeafReadSpec.WIDEN_INT_TO_DOUBLE;
    private static final int WIDEN_FLOAT_TO_DOUBLE = io.vecruntime.parquet.read.LeafReadSpec.WIDEN_FLOAT_TO_DOUBLE;
    private static final int WIDEN_DAYS_TO_MICROS = io.vecruntime.parquet.read.LeafReadSpec.WIDEN_DAYS_TO_MICROS;
    // #812: an unsigned INT32 (UINT_32) read as a long, zero-extended (Spark's UnsignedIntegerUpdater).
    private static final int WIDEN_UINT32_TO_LONG = io.vecruntime.parquet.read.LeafReadSpec.WIDEN_UINT32_TO_LONG;
    // #812: an INT32 / INT64 read as a decimal wider than 18 digits: each 8-byte slot sign-extended to a
    // 16-byte DECIMAL128 slot in place (Spark's IntegerToBinaryUpdater / LongToBinaryUpdater, or their
    // ToDecimal forms when rescaled).
    private static final int WIDEN_LONG_TO_DEC128 = io.vecruntime.parquet.read.LeafReadSpec.WIDEN_LONG_TO_DEC128;
    // #812: an unsigned INT64 (UINT_64) read as decimal(20, 0), zero-extended (Spark's UnsignedLongUpdater).
    private static final int WIDEN_ULONG_TO_DEC128 = io.vecruntime.parquet.read.LeafReadSpec.WIDEN_ULONG_TO_DEC128;
    private final int widen;
    private final int rescale;
    private final long rescaleFactor;
    private static final java.lang.foreign.ValueLayout.OfLong LONG_LE = java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final java.lang.foreign.ValueLayout.OfInt INT_LE = java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private final GroupUnpacker[] unpackers = new GroupUnpacker[33];
    private final Arena scratch;
    private final ColumnChunkDecoder decoder;
    private final ReusableByteOut pageOut = new ReusableByteOut(1 << 16);
    // #786: a v2 page's definition levels when they are not one heap buffer (copied, as the values are into pageOut).
    private final ReusableByteOut levelsOut = new ReusableByteOut(1 << 12);

    // Per-chunk (row-group) state.
    private PageReader pages;
    private int rowGroupRows;

    // #568 column-index page filtering: the row group's selected rows as ranges [start, end) of absolute row
    // indexes, when the store was filtered by the column index (its pages are only those overlapping the
    // ranges, each with its first row index); null otherwise. `gap` is the absolute index of the decoder's next
    // row less decoder.rowsDone() (the rows of the pages dropped before it).
    private long[] rangeStart;
    private long[] rangeEnd;
    private int range;
    private long inRange;
    private long gap;
    private int selectedRows;
    private int selectedDone;

    // #568: a repeated leaf -- a list's element, a map's key or value. The decoder then walks ENTRIES, one per
    // (repetition, definition) level pair, not rows; NativeParquetListReader maps entries to rows from the
    // levels kept here. Pages are pulled ahead of the decoder (a batch's rows may run past the page in hand),
    // each into its own buffer, and queued for it.
    private final boolean repeated;
    private final int repBitWidth;
    private final ArrayDeque<ColumnChunkDecoder.Page> pendingPages = new ArrayDeque<>();
    private int[] reps = new int[0];
    private int[] defs = new int[0];
    private int levelStart; // the first entry not decoded yet
    private int levelEnd; // entries whose levels have been read
    private boolean chunkHead; // the chunk's first entry is not checked yet
    private int rowsEmitted; // rows of the row group emitted
    // #626: a repeated leaf of a row group filtered by the column index. Each pulled page's first entry in the
    // kept levels and its first row index, oldest first ([pageHead, pageTail)), and the absolute row of the
    // entry at levelStart, which is always a row's first entry between calls. A page begins on a row boundary
    // wherever an offset index exists (the format requires it), so a page's first entry is its first row's.
    private int[] pageLevelAt = new int[8];
    private long[] pageFirstRow = new long[8];
    private int pageHead;
    private int pageTail;
    private long levelRow;

    // A small pool of released batch vectors to reuse (bounded; extras are closed). Guarded by its own
    // monitor: with decode-ahead (#606) the producer thread borrows while the task thread releases. On JDK 25 a
    // monitor no longer pins a virtual thread (JEP 491).
    private final Deque<FieldVector> pool = new ArrayDeque<>();
    private int maxPool = 4;
    private boolean closed;
    // #735: the scan installed a ReusingCodecFactory, so a decompressed page lives in a buffer the next page
    // reuses. A repeated leaf queues several pages at once (pullPage), so it must copy each out of that buffer;
    // a flat leaf reads one page at a time in place. Off by default (default codec factory: a fresh array a page).
    private boolean reuseDecompress;

    /**
     * #900: a leaf read by {@code spec}. The engine's adapter (Spark's {@code NativeParquetColumnReader})
     * builds the spec and wraps the batches as its own columns.
     */
    public LeafColumnReader(ColumnDescriptor column, LeafReadSpec spec, String name,
                            int batchRows, BufferAllocator allocator) {
        this.column = column;
        this.type = spec.lane;
        this.widen = spec.widen;
        this.rescale = spec.rescale;
        this.rescaleFactor = rescale > 0 && rescale <= 18
                ? LONG_TEN_POWERS[rescale]
                : 1L;
        // INT32 and FLOAT bits are decoded sign-extended into INT64 slots, then converted (widenLane).
        this.physicalType = widen == WIDEN_INT_TO_DOUBLE || widen == WIDEN_FLOAT_TO_DOUBLE
                ? VecType.INT32
                : physicalTypeOf(column, type);
        this.name = name;
        this.maxDefLevel = column.getMaxDefinitionLevel();
        this.repeated = column.getMaxRepetitionLevel() > 0;
        this.repBitWidth = ParquetPageDecoder.bitWidth(column.getMaxRepetitionLevel());
        this.allocator = allocator;
        this.narrowBits = spec.narrowBits;
        this.millis = spec.millis;
        this.float32 = spec.float32;
        this.uuid = spec.uuid;
        this.dictionaryIdsAllowed = spec.dictionaryIdsAllowed;
        this.arrowType = spec.arrowType();
        // Dictionary scratch is GC-managed: closing a shared Arena runs a JVM-wide handshake that walks every
        // thread's stack, and a reader is made per column per file -- on 1 TB TPC-DS (14,594 store_sales
        // files) those handshakes were 8-9% of executor CPU in q88. An automatic arena has no close.
        this.scratch = Arena.ofAuto();
        // A decimal stored as FIXED_LEN_BYTE_ARRAY or BINARY: its big-endian bytes are staged on the decoder's
        // UTF8 path (PLAIN, dictionary, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY) and converted into the INT64
        // lane (p <= 18) or the DECIMAL128 lane by flushDecimal.
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName ptn = column.getPrimitiveType().getPrimitiveTypeName();
        this.binaryDecimal = spec.decimal && (ptn == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY || ptn == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY);
        this.fixedLength = ptn == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ? column.getPrimitiveType().getTypeLength() : 0;
        this.int96 = ptn == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
        this.dateLane = spec.dateLane;
        this.timestampLane = spec.timestampLane;
        if (binaryDecimal) {
            this.decoder = new ColumnChunkDecoder(VecType.UTF8, VecType.UTF8, maxDefLevel, batchRows, this::unpackerFor);
            this.decoder.setFixedLength(fixedLength);
        } else if (int96) {
            // Staged as 12-byte values on the UTF8 path (PLAIN and dictionary), converted by flushInt96.
            this.decoder = new ColumnChunkDecoder(VecType.UTF8, VecType.UTF8, maxDefLevel, batchRows, this::unpackerFor);
            this.decoder.setFixedLength(12);
        } else {
            this.decoder = new ColumnChunkDecoder(physicalType, widen != WIDEN_NONE ? VecType.INT64 : type, maxDefLevel,
                    batchRows, this::unpackerFor);
            if (type == VecType.UTF8 && fixedLength > 0) {
                // A FIXED_LEN_BYTE_ARRAY column read as BINARY: fixed-length values with no length prefix.
                this.decoder.setFixedLength(fixedLength);
            }
        }
    }

    private static final long[] LONG_TEN_POWERS = new long[19];

    static {
        LONG_TEN_POWERS[0] = 1L;
        for (int i = 1; i < LONG_TEN_POWERS.length; i++) {
            LONG_TEN_POWERS[i] = LONG_TEN_POWERS[i - 1] * 10L;
        }
    }

    private static boolean isNull(java.lang.foreign.MemorySegment validity, int i) {
        return validity != null && (validity.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i >>> 3) & (1 << (i & 7))) == 0;
    }

    /**
     * The widening conversion over a decoded batch of {@code n} slots (no-op
     * when the column is read as stored). Null slots are skipped: a pooled
     * vector's stale value must not raise.
     */
    private void widenLane(java.lang.foreign.MemorySegment data, java.lang.foreign.MemorySegment validity, int n) {
        if (widen == WIDEN_DAYS_TO_MICROS) {
            rebase.daysToMicros(data, validity, n);
            return;
        }
        if (widen == WIDEN_UINT32_TO_LONG) {
            for (int i = 0; i < n; i++) {
                long at = (long) i << 3;
                data.set(LONG_LE, at, data.get(LONG_LE, at) & 0xFFFFFFFFL);
            }
            return;
        }
        if (widen == WIDEN_LONG_TO_DEC128 || widen == WIDEN_ULONG_TO_DEC128) {
            // From the last slot down, so no 8-byte value is overwritten before it is moved.
            boolean signed = widen == WIDEN_LONG_TO_DEC128;
            for (int i = n - 1; i >= 0; i--) {
                long v = data.get(LONG_LE, (long) i << 3);
                long at = (long) i << 4;
                data.set(LONG_LE, at, v);
                data.set(LONG_LE, at + 8,
                        signed ? v >> 63 : 0L);
            }
            if (rescale > 0) {
                rescale128(data, validity, n);
            }
            return;
        }
        if (widen != WIDEN_NONE) {
            for (int i = 0; i < n; i++) {
                if (isNull(validity, i)) {
                    continue;
                }
                long at = (long) i << 3;
                long v = data.get(LONG_LE, at);
                double d = widen == WIDEN_INT_TO_DOUBLE
                        ? (double) v
                        : (double) Float.intBitsToFloat((int) v);
                data.set(LONG_LE, at, Double.doubleToRawLongBits(d));
            }
            return;
        }
        if (rescale > 0) {
            if (type == VecType.DECIMAL128) {
                rescale128(data, validity, n);
                return;
            }
            // The low 64 bits of the exact product, as Spark's BigDecimal.setScale(...).unscaledValue().longValue().
            for (int i = 0; i < n; i++) {
                if (isNull(validity, i)) {
                    continue;
                }
                long at = (long) i << 3;
                data.set(LONG_LE, at, data.get(LONG_LE, at) * rescaleFactor);
            }
        }
    }

    /** DECIMAL128 slots (little-endian two's complement) times 10^rescale. */
    private void rescale128(java.lang.foreign.MemorySegment data, java.lang.foreign.MemorySegment validity, int n) {
        java.math.BigInteger factor = java.math.BigInteger.TEN.pow(rescale);
        java.math.BigInteger mask = java.math.BigInteger.ONE.shiftLeft(64).subtract(java.math.BigInteger.ONE);
        for (int i = 0; i < n; i++) {
            if (isNull(validity, i)) {
                continue;
            }
            long at = (long) i << 4;
            long lo = data.get(LONG_LE, at);
            long hi = data.get(LONG_LE, at + 8);
            java.math.BigInteger v = java.math.BigInteger.valueOf(hi)
                    .shiftLeft(64)
                    .or(java.math.BigInteger.valueOf(lo).and(mask))
                    .multiply(factor);
            data.set(LONG_LE, at, v.longValue());
            data.set(LONG_LE, at + 8,
                    v.shiftRight(64).longValue());
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

    @Override
    public void startRowGroup(org.apache.parquet.column.page.PageReadStore store, int rows) {
        setRanges(store.getRowIndexes(), rows);
        startPages(store.getPageReader(column), rows);
    }

    /**
     * The selected rows of a row group filtered by the column index (#568), as
     * ranges; none when every row is read.
     */
    private void setRanges(java.util.Optional<java.util.PrimitiveIterator.OfLong> indexes, int rows) {
        rangeStart = null;
        rangeEnd = null;
        range = 0;
        inRange = 0;
        gap = 0;
        selectedRows = rows;
        selectedDone = 0;
        if (indexes.isEmpty()) {
            return;
        }
        java.util.PrimitiveIterator.OfLong it = indexes.get();
        long[] starts = new long[16];
        long[] ends = new long[16];
        int k = 0;
        while (it.hasNext()) {
            long r = it.nextLong();
            if (k > 0 && ends[k - 1] == r) {
                ends[k - 1] = r + 1;
                continue;
            }
            if (k == starts.length) {
                starts = java.util.Arrays.copyOf(starts, 2 * k);
                ends = java.util.Arrays.copyOf(ends, 2 * k);
            }
            starts[k] = r;
            ends[k] = r + 1;
            k++;
        }
        rangeStart = java.util.Arrays.copyOf(starts, k);
        rangeEnd = java.util.Arrays.copyOf(ends, k);
    }

    /**
     * Moves the decoder to absolute row {@code target}: skips rows within pages,
     * and over pages the column index dropped.
     */
    private void alignTo(long target) {
        while (decoder.rowsDone() + gap < target) {
            if (decoder.needsPage()) {
                decoder.feedPage(nextKernelPage());
                continue;
            }
            decoder.skip((int) Math.min(target - decoder.rowsDone() - gap, Integer.MAX_VALUE));
        }
    }

    /**
     * Walks the next {@code n} selected rows run by run: aligns the decoder to
     * each run's first row and calls {@code read} (rows read, page-bounded) to
     * decode it at its batch position.
     */
    private void forRanges(int n, java.util.function.IntBinaryOperator read) {
        int at = 0;
        while (at < n) {
            long next = rangeStart[range] + inRange;
            alignTo(next);
            int len = (int) Math.min(n - at, rangeEnd[range] - next);
            int done = 0;
            while (done < len) {
                if (decoder.needsPage()) {
                    decoder.feedPage(nextKernelPage());
                }
                done += read.applyAsInt(len - done, at + done);
            }
            at += len;
            advance(len);
        }
    }

    /**
     * Moves the selected-row cursor {@code n} rows on (the decoder follows at
     * the next read).
     */
    private void advance(int n) {
        long left = n;
        while (left > 0) {
            long room = rangeEnd[range] - rangeStart[range] - inRange;
            if (left < room) {
                inRange += left;
                break;
            }
            left -= room;
            range++;
            inRange = 0;
        }
        selectedDone += n;
    }

    /**
     * {@link #readBatch} over a row group filtered by the column index: its
     * next {@code n} selected rows.
     */
    private FieldVector readFiltered(int n) {
        decoder.startBatch(n);
        if (type == VecType.UTF8 || type == VecType.BOOL || binaryDecimal || int96) {
            forRanges(n, decoder::readBatch);
            boolean nulls = decoder.batchNullCount() > 0;
            if (type == VecType.UTF8) {
                return decoder.batchIsDictionaryIds()
                        ? flushIds(n, nulls)
                        : uuid ? flushUuid(n, nulls) : flushUtf8(n, nulls);
            }
            return int96
                    ? flushInt96(n, nulls)
                    : binaryDecimal ? flushDecimal(n, nulls) : flushBool(n, nulls);
        }
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(n);
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, n);
        java.lang.foreign.MemorySegment data = out.data();
        java.lang.foreign.MemorySegment validity = maxDefLevel > 0 ? out.validity() : null;
        if (validity != null) {
            validity.asSlice(0L, (long) ((n + 63) >>> 6) * 8).fill((byte) 0);
        }
        forRanges(n, (len, at) -> decoder.readBatchDirectA(len, at, data, validity));
        boolean hasNulls = decoder.batchNullCount() > 0;
        if (narrowBits != 0) {
            narrow(data, n, narrowBits);
        }
        if (millis) {
            millisToMicros(data, hasNulls ? validity : null, n);
        }
        rebaseLane(data, hasNulls ? validity : null, n);
        widenLane(data, hasNulls ? validity : null, n);
        ArrowWrite.finish(out, n, !hasNulls);
        return v;
    }

    /** {@link #release(FieldVector)} for a batch handle. */
    @Override
    public void release(Object batch) {
        release((FieldVector) batch);
    }

    @Override
    public int[] rowLevels() {
        return batchLevels();
    }

    @Override
    public void recordRowLevels() {
        setRecordLevels(true);
    }

    /**
     * Keep each batch's definition levels (#568): a struct reader derives the
     * struct's validity from its first leaf's levels.
     */
    public void setRecordLevels(boolean on) {
        decoder.setRecordLevels(on);
    }

    /**
     * The last batch's definition levels, rows {@code [0, n)}; see {@link
     * #setRecordLevels}.
     */
    public int[] batchLevels() {
        return decoder.batchLevels();
    }

    public int maxDefinitionLevel() {
        return maxDefLevel;
    }

    /**
     * Begin a new row group of {@code rowGroupRows} rows on this column's {@code
     * pages}. Decodes the chunk's dictionary page once (if present) and resets the
     * decoder; {@link #readBatch} then streams the pages.
     */
    public void startRowGroup(PageReader pages, int rowGroupRows) {
        setRanges(java.util.Optional.empty(), rowGroupRows);
        startPages(pages, rowGroupRows);
    }

    private void startPages(PageReader pages, int rowGroupRows) {
        this.pages = pages;
        this.rowGroupRows = rowGroupRows;
        startRepeatedChunk();
        // Filtered by the column index, the pages hold more rows than the selection: no row-count bound.
        decoder.startChunk(repeated
                ? (int) pages.getTotalValueCount()
                : rangeStart != null ? Integer.MAX_VALUE : rowGroupRows);
        releaseChunkDictionary();
        VectorBuffers dict = decodeDictionary(pages);
        if (dict != null) {
            decoder.setDictionary(dict);
            if (dictionaryIds) {
                // #612: the row group's dictionary as one Arrow vector, shared by every batch emitted as ids.
                chunkDict = new SharedDictionary(ArrowWrite.copyDictionary(name + ".dictionary", dict, allocator));
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
     * the row group's dictionary ({@link #dictionaryOf} names it; Spark wraps
     * an id batch as a {@code VectorDictionaryColumnVector}), so the operators
     * above compute on ids, as they do over Spark's scan. Only for a UTF8
     * string column read through the UTF8 path (not binary, not a fixed-length
     * or byte-array decimal).
     */
    @Override
    public void setDictionaryIds(boolean enabled) {
        boolean ok = enabled
                && type == VecType.UTF8
                && !binaryDecimal
                && fixedLength == 0
                && dictionaryIdsAllowed; // a UTF8-binary string: a collated one (#568) is compared by its values
        this.dictionaryIds = ok;
        if (type == VecType.UTF8) {
            decoder.setEmitDictionaryIds(ok);
        }
    }

    /**
     * The row group's dictionary when {@code v} is a batch of dictionary ids
     * (#612; an {@code IntVector} of ids into it), else null: the adapter wraps
     * an id batch as a dictionary view.
     */
    public org.apache.arrow.vector.VarCharVector dictionaryOf(FieldVector v) {
        SharedDictionary d;
        synchronized (pool) {
            d = idBatches.get(v);
        }
        return d == null ? null : d.vector;
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
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, rows);
        decoder.flushIds(rows, out.data(),
                hasNulls ? out.validity() : null);
        ArrowWrite.finish(out, rows, !hasNulls);
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
    @Override
    public int rowsRemaining() {
        if (rangeStart != null) {
            return selectedRows - selectedDone;
        }
        if (repeated) {
            return rowGroupRows - rowsEmitted;
        }
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
    @Override
    public FieldVector readBatch(int n) {
        if (rangeStart != null) {
            return readFiltered(n);
        }
        decoder.startBatch(n);
        if (type == VecType.UTF8) {
            int filled = 0;
            while (filled < n) {
                if (decoder.needsPage()) {
                    decoder.feedPage(nextKernelPage());
                }
                filled += decoder.readBatch(n - filled, filled);
            }
            if (decoder.batchIsDictionaryIds()) {
                return flushIds(n, decoder.batchNullCount() > 0);
            }
            return uuid ? flushUuid(n, decoder.batchNullCount() > 0) : flushUtf8(n, decoder.batchNullCount() > 0);
        }
        if (type == VecType.BOOL || binaryDecimal || int96) {
            int filled = 0;
            while (filled < n) {
                if (decoder.needsPage()) {
                    decoder.feedPage(nextKernelPage());
                }
                filled += decoder.readBatch(n - filled, filled);
            }
            boolean nulls = decoder.batchNullCount() > 0;
            return int96
                    ? flushInt96(n, nulls)
                    : binaryDecimal ? flushDecimal(n, nulls) : flushBool(n, nulls);
        }
        return fillFixed(n, /* modeA= */ true);
    }

    // ----- repeated leaves (#568: lists and maps) -----

    private void startRepeatedChunk() {
        if (!repeated) {
            return;
        }
        pendingPages.clear();
        levelStart = 0;
        levelEnd = 0;
        rowsEmitted = 0;
        chunkHead = true;
        pageHead = 0;
        pageTail = 0;
        levelRow = 0;
    }

    /**
     * The next page for the decoder: a queued one for a repeated leaf, else the
     * next of the chunk.
     */
    private ColumnChunkDecoder.Page nextKernelPage() {
        if (!repeated) {
            DataPage page = requirePage();
            if (rangeStart != null && page.getFirstRowIndex().isPresent()) {
                // Every earlier page is consumed here, so the decoder's next row is this page's first.
                gap = page.getFirstRowIndex().get() - decoder.rowsDone();
            }
            return toKernelPage(page);
        }
        if (pendingPages.isEmpty() && !pullPage()) {
            throw new IllegalStateException("ran out of pages for " + java.util.Arrays.toString(column.getPath()));
        }
        return pendingPages.poll();
    }

    /**
     * Reads the chunk's next page into its own buffer: its repetition and
     * definition levels are appended to the kept levels, and the page past its
     * repetition levels is queued for the decoder. False at the chunk's end.
     */
    private boolean pullPage() {
        DataPage page = pages.readPage();
        if (page == null) {
            return false;
        }
        int n = page.getValueCount();
        makeRoom(n);
        int firstEntry = levelEnd;
        ColumnChunkDecoder.Page kernel;
        if (page instanceof DataPageV1 v1) {
            byte[] buf = bytes(v1.getBytes());
            int repLen = readLeInt(buf, 0);
            readLevels(buf, 4, repLen, repBitWidth, reps, n);
            int defAt = 4 + repLen;
            readLevels(buf, defAt + 4, readLeInt(buf, defAt),
                    ParquetPageDecoder.bitWidth(maxDefLevel), defs, n);
            kernel = ColumnChunkDecoder.Page.v1At(buf, defAt, buf.length - defAt, n,
                    encoding(v1.getValueEncoding()));
        } else {
            DataPageV2 v2 = (DataPageV2) page;
            // The page is queued past this call, so each section is its own array: parquet-java's own buffer when
            // it is one on the heap (#786), else a copy. Level and value sections are never joined into one.
            java.nio.ByteBuffer rep = ownedView(v2.getRepetitionLevels());
            java.nio.ByteBuffer def = ownedView(v2.getDefinitionLevels());
            java.nio.ByteBuffer data = ownedView(v2.getData());
            readLevels(rep.array(), rep.arrayOffset() + rep.position(), rep.remaining(),
                    repBitWidth, reps, n);
            readLevels(def.array(), def.arrayOffset() + def.position(), def.remaining(),
                    ParquetPageDecoder.bitWidth(maxDefLevel), defs, n);
            kernel = ColumnChunkDecoder.Page.v2Split(
                    def.array(),
                    def.arrayOffset() + def.position(),
                    def.remaining(),
                    data.array(),
                    data.arrayOffset() + data.position(),
                    data.remaining(),
                    n,
                    encoding(v2.getDataEncoding()));
        }
        levelEnd += n;
        pendingPages.add(kernel);
        if (rangeStart != null) {
            keepPageStart(page, firstEntry, n);
        }
        return true;
    }

    /**
     * #626: records where a pulled page of a column-index-filtered row group
     * starts, in the kept levels and in rows, so {@link #alignRepeated} can
     * cross the pages the column index dropped.
     */
    private void keepPageStart(DataPage page, int firstEntry, int n) {
        if (page.getFirstRowIndex().isEmpty()) {
            throw new IllegalStateException("a page of " + java.util.Arrays.toString(column.getPath()) + " filtered by the column index has no first row index");
        }
        if (n > 0 && reps[firstEntry] != 0 && pageTail > 0) {
            throw new IllegalStateException("a page of " + java.util.Arrays.toString(column.getPath()) + " does not begin on a row boundary, which a file with an offset index must");
        }
        if (pageHead > 0) {
            int live = pageTail - pageHead;
            System.arraycopy(pageLevelAt, pageHead, pageLevelAt, 0, live);
            System.arraycopy(pageFirstRow, pageHead, pageFirstRow, 0, live);
            pageHead = 0;
            pageTail = live;
        }
        if (pageTail == pageLevelAt.length) {
            pageLevelAt = java.util.Arrays.copyOf(pageLevelAt, 2 * pageTail);
            pageFirstRow = java.util.Arrays.copyOf(pageFirstRow, 2 * pageTail);
        }
        pageLevelAt[pageTail] = firstEntry;
        pageFirstRow[pageTail] = page.getFirstRowIndex().get();
        pageTail++;
    }

    /**
     * #626: moves a repeated leaf of a column-index-filtered row group to its
     * next selected row, skipping the entries of the rows before it in the
     * pages it holds and jumping over the pages the column index dropped. A
     * batch never crosses a selected range (the scan bounds it), so the rows
     * of one call are consecutive in the row group.
     */
    private void alignRepeated() {
        if (rangeStart == null || range >= rangeStart.length) {
            return;
        }
        startChunkHead();
        long target = rangeStart[range] + inRange;
        while (true) {
            if (levelStart == levelEnd && !pullPage()) {
                throw new IllegalStateException("ran out of pages for " + java.util.Arrays.toString(column.getPath()) + " before row " + target);
            }
            while (pageHead + 1 < pageTail && pageLevelAt[pageHead + 1] <= levelStart) {
                pageHead++;
            }
            if (pageLevelAt[pageHead] == levelStart) {
                levelRow = pageFirstRow[pageHead];
            }
            if (levelRow >= target) {
                if (levelRow > target) {
                    throw new IllegalStateException("row " + target + " of " + java.util.Arrays.toString(column.getPath()) + " is in no page the column index kept");
                }
                return;
            }
            int pageEnd = pageHead + 1 < pageTail ? pageLevelAt[pageHead + 1] : levelEnd;
            long want = target - levelRow;
            long starts = 0;
            int j = levelStart + 1;
            while (j < pageEnd) {
                if (reps[j] == 0 && ++starts == want) {
                    break;
                }
                j++;
            }
            int entries = j - levelStart;
            int done = 0;
            while (done < entries) {
                if (decoder.needsPage()) {
                    decoder.feedPage(nextKernelPage());
                }
                done += decoder.skip(entries - done);
            }
            levelStart = j;
            levelRow += j < pageEnd ? want : starts + 1;
        }
    }

    /** #626: the cursor and row of a filtered repeated leaf move {@code rows} on. */
    private void advanceRepeated(int rows) {
        if (rangeStart != null) {
            levelRow += rows;
            advance(rows);
        }
    }

    /**
     * #626: a filtered repeated leaf's {@code n} rows must lie in its current
     * range.
     */
    private void requireInRange(int n) {
        if (rangeStart != null
                && n > 0
                && (range >= rangeStart.length || n > rangeEnd[range] - rangeStart[range] - inRange)) {
            throw new IllegalStateException(n + " rows of " + java.util.Arrays.toString(column.getPath()) + " cross a range the column index selected");
        }
    }

    /**
     * {@code in} as a heap buffer the caller may keep PAST the next page: #786's
     * own buffer when it is one on the heap AND nothing reuses it, else a copy.
     * A repeated leaf queues several pages, so when the scan reuses the
     * decompressor's buffer (#735) every page here must be copied out -- the
     * reused buffer would otherwise be overwritten by the next page while this
     * one is still queued.
     */
    private java.nio.ByteBuffer ownedView(BytesInput in) {
        if (!reuseDecompress) {
            java.nio.ByteBuffer view = heapView(in);
            if (view != null) {
                return view;
            }
        }
        return java.nio.ByteBuffer.wrap(bytes(in));
    }

    /** Room for {@code n} more kept levels: drops the decoded ones, then grows. */
    private void makeRoom(int n) {
        if (levelStart > 0) {
            int live = levelEnd - levelStart;
            System.arraycopy(reps, levelStart, reps, 0, live);
            System.arraycopy(defs, levelStart, defs, 0, live);
            for (int q = pageHead; q < pageTail; q++) {
                pageLevelAt[q] -= levelStart;
            }
            levelStart = 0;
            levelEnd = live;
        }
        if (reps.length < levelEnd + n) {
            int cap = Math.max(levelEnd + n, 2 * reps.length);
            reps = java.util.Arrays.copyOf(reps, cap);
            defs = java.util.Arrays.copyOf(defs, cap);
        }
    }

    /**
     * Decodes {@code n} levels of an RLE/bit-packed stream into {@code dst} at
     * the kept levels' end.
     */
    private void readLevels(byte[] src, int offset, int length,
                            int bitWidth, int[] dst, int n) {
        if (bitWidth == 0) {
            java.util.Arrays.fill(dst, levelEnd, levelEnd + n, 0);
            return;
        }
        io.vecruntime.parquet.codec.RleBitPackingReader.overArray(src, offset, length, bitWidth, unpackerFor(bitWidth)).readInts(dst, levelEnd, n);
    }

    private static int readLeInt(byte[] b, int at) {
        return (b[at] & 0xFF)
                | (b[at + 1] & 0xFF) << 8
                | (b[at + 2] & 0xFF) << 16
                | (b[at + 3] & 0xFF) << 24;
    }

    /**
     * A chunk's first entry starts a row whatever its repetition level: a
     * malformed chunk that opens at level 1 (parquet-testing's
     * bad_data/ARROW-GH-45185) is read as parquet-java's record reader reads it.
     */
    private void startChunkHead() {
        if (!chunkHead) {
            return;
        }
        chunkHead = false;
        if (levelStart < levelEnd || pullPage()) {
            reps[levelStart] = 0;
        }
    }

    /**
     * The entries that make up the next {@code n} rows of a repeated leaf: an
     * entry with repetition level 0 starts a row. Reads pages ahead until the
     * row after them starts or the chunk ends; their levels are then at {@link
     * #levelOffset} of {@link #repLevels} / {@link #defLevels}.
     */
    public int entriesForRows(int n) {
        startChunkHead();
        alignRepeated();
        requireInRange(n);
        int rows = 0;
        int seen = 0; // entries scanned, from levelStart (pullPage may move levelStart to 0)
        while (true) {
            int[] r = reps;
            int i = levelStart + seen;
            int end = levelEnd;
            while (i < end) {
                if (r[i] == 0) {
                    if (rows == n) {
                        return i - levelStart;
                    }
                    rows++;
                }
                i++;
            }
            seen = i - levelStart;
            if (!pullPage()) {
                return seen;
            }
        }
    }

    public int[] repLevels() {
        return reps;
    }

    public int[] defLevels() {
        return defs;
    }

    public int levelOffset() {
        return levelStart;
    }

    /**
     * Decodes the next {@code entries} entries of a repeated leaf -- {@code
     * rows} rows, as {@link #entriesForRows} counted them -- keeping only the
     * entries that are elements (definition level {@code >= elementDef}), back
     * to back: a null or empty list has an entry but no element. An element is
     * null where its level is below the leaf's maximum.
     */
    public FieldVector readElements(int rows, int entries, int elementDef) {
        startChunkHead();
        alignRepeated();
        int from = levelStart;
        int[] d = defs;
        int elements = 0;
        for (int k = 0; k < entries; k++) {
            if (d[from + k] >= elementDef) {
                elements++;
            }
        }
        decoder.startBatch(elements);
        boolean staged = type == VecType.UTF8 || type == VecType.BOOL || binaryDecimal || int96;
        BaseFixedWidthVector fixed = null;
        ArrowWrite.Buffers out = null;
        MemorySegment data = null;
        MemorySegment validity = null;
        if (!staged) {
            fixed = (BaseFixedWidthVector) borrowFixed(elements);
            out = ArrowWrite.forWrite(fixed, elements);
            data = out.data();
            validity = out.validity();
            validity.asSlice(0L, (long) ((elements + 63) >>> 6) * 8).fill((byte) 0);
        }
        int k = 0;
        int at = 0;
        while (k < entries) {
            boolean element = d[from + k] >= elementDef;
            int j = k + 1;
            while (j < entries && (d[from + j] >= elementDef) == element) {
                j++;
            }
            int len = j - k;
            int done = 0;
            while (done < len) {
                if (decoder.needsPage()) {
                    decoder.feedPage(nextKernelPage());
                }
                int m;
                if (!element) {
                    m = decoder.skip(len - done);
                } else if (staged) {
                    m = decoder.readBatch(len - done, at);
                } else {
                    m = decoder.readBatchDirectA(len - done, at, data, validity);
                }
                if (element) {
                    at += m;
                }
                done += m;
            }
            k = j;
        }
        levelStart += entries;
        rowsEmitted += rows;
        advanceRepeated(rows);
        boolean nulls = decoder.batchNullCount() > 0;
        if (type == VecType.UTF8) {
            // #568: a list's or map's strings are dictionary ids too, where the leaf allows it (#612).
            return decoder.batchIsDictionaryIds()
                    ? flushIds(elements, nulls)
                    : uuid ? flushUuid(elements, nulls) : flushUtf8(elements, nulls);
        }
        if (staged) {
            return int96
                    ? flushInt96(elements, nulls)
                    : binaryDecimal ? flushDecimal(elements, nulls) : flushBool(elements, nulls);
        }
        if (narrowBits != 0) {
            narrow(data, elements, narrowBits);
        }
        if (millis) {
            millisToMicros(data, nulls ? validity : null, elements);
        }
        rebaseLane(data, nulls ? validity : null, elements);
        widenLane(data, nulls ? validity : null, elements);
        ArrowWrite.finish(out, elements, !nulls);
        return fixed;
    }

    /**
     * Skips the next {@code entries} entries ({@code rows} rows) of a repeated
     * leaf.
     */
    public void skipEntries(int rows, int entries) {
        startChunkHead();
        alignRepeated();
        int done = 0;
        while (done < entries) {
            if (decoder.needsPage()) {
                decoder.feedPage(nextKernelPage());
            }
            done += decoder.skip(entries - done);
        }
        levelStart += entries;
        rowsEmitted += rows;
        advanceRepeated(rows);
    }

    // ----- late materialization (#611) -----

    /**
     * Whether decoding only a batch's selected rows can beat decoding it whole:
     * for a string column (its bytes are copied per value). A fixed-width
     * column, a boolean or a binary decimal is unpacked a whole batch at a
     * time, so a run-by-run walk of a scattered selection -- a decoder call per
     * run of one or a few rows -- costs more than it saves (1 TB q44: twice the
     * executor time at ~0.1% scattered survivors).
     */
    @Override
    public boolean decodesSelectedFaster() {
        return type == VecType.UTF8 && rangeStart == null;
    }

    /**
     * Skips the next {@code n} rows of the row group without decoding them into
     * any vector.
     */
    @Override
    public void skipRows(int n) {
        if (repeated) {
            skipEntries(n, entriesForRows(n));
            return;
        }
        if (rangeStart != null) {
            advance(n);
            return;
        }
        int done = 0;
        while (done < n) {
            if (decoder.needsPage()) {
                decoder.feedPage(nextKernelPage());
            }
            done += decoder.skip(n - done);
        }
    }

    /**
     * {@link #readBatch} decoding only the rows set in {@code selection}
     * ({@code n} bits): the vector has {@code n} slots, the others null for a
     * nullable column (zero, false or empty otherwise), and the batch's
     * selection excludes them. Runs of unselected rows are skipped in the page
     * streams.
     */
    @Override
    public FieldVector readBatchSelected(int n, java.lang.foreign.MemorySegment selection) {
        if (rangeStart != null) {
            return readBatch(n); // the batch's selection excludes the other rows
        }
        decoder.startBatch(n);
        if (type == VecType.UTF8 || type == VecType.BOOL || binaryDecimal || int96) {
            forRuns(n, selection,
                    (sel, len, at) -> sel ? decoder.readBatch(len, at) : decoder.skipIntoBatch(len, at));
            boolean nulls = decoder.batchNullCount() > 0;
            if (type == VecType.UTF8) {
                return decoder.batchIsDictionaryIds()
                        ? flushIds(n, nulls)
                        : uuid ? flushUuid(n, nulls) : flushUtf8(n, nulls);
            }
            return int96
                    ? flushInt96(n, nulls)
                    : binaryDecimal ? flushDecimal(n, nulls) : flushBool(n, nulls);
        }
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(n);
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, n);
        java.lang.foreign.MemorySegment data = out.data();
        java.lang.foreign.MemorySegment validity = maxDefLevel > 0 ? out.validity() : null;
        if (validity != null) {
            validity.asSlice(0L, (long) ((n + 63) >>> 6) * 8).fill((byte) 0);
        }
        int width = v.getTypeWidth();
        forRuns(
                n,
                selection,
                (sel, len, at) -> {
                    if (sel) {
                        return decoder.readBatchDirectA(len, at, data, validity);
                    }
                    int m = decoder.skip(len);
                    // Defined values in the skipped slots (a pooled vector holds an earlier batch's): the
                    // conversions below run over every slot, and a stale micros value times 1000 could overflow.
                    data.asSlice((long) at * width, (long) m * width).fill((byte) 0);
                    return m;
                });
        boolean hasNulls = decoder.batchNullCount() > 0 || (validity != null && hasUnselected(n, selection));
        if (narrowBits != 0) {
            narrow(data, n, narrowBits);
        }
        if (millis) {
            millisToMicros(data, hasNulls ? validity : null, n);
        }
        rebaseLane(data, hasNulls ? validity : null, n);
        widenLane(data, hasNulls ? validity : null, n);
        ArrowWrite.finish(out, n, !hasNulls);
        return v;
    }

    @FunctionalInterface
    private interface RunStep {
        /**
         * Decodes or skips up to {@code len} rows at batch row {@code at};
         * returns the rows done.
         */
        int apply(boolean selected, int len, int at);
    }

    /**
     * Walks the runs of equal bits of {@code selection}, feeding pages, calling
     * {@code step} until each run is done.
     */
    private void forRuns(int n, java.lang.foreign.MemorySegment selection, RunStep step) {
        int i = 0;
        while (i < n) {
            boolean sel = bit(selection, i);
            int j = nextDifferent(selection, i, n, sel);
            int done = 0;
            while (done < j - i) {
                if (decoder.needsPage()) {
                    decoder.feedPage(nextKernelPage());
                }
                done += step.apply(sel, j - i - done, i + done);
            }
            i = j;
        }
    }

    /**
     * The first row at or after {@code from} whose bit is not {@code sel}, or
     * {@code n}: a word at a time.
     */
    private static int nextDifferent(java.lang.foreign.MemorySegment bits, int from, int n,
            boolean sel) {
        int w = from >>> 6;
        int words = io.vecruntime.kernels.Bitmap.wordsFor(n);
        long word = io.vecruntime.kernels.Bitmap.wordAt(bits, w, n);
        if (sel) {
            word = ~word;
        }
        word &= -1L << (from & 63);
        while (word == 0L) {
            if (++w >= words) {
                return n;
            }
            word = io.vecruntime.kernels.Bitmap.wordAt(bits, w, n);
            if (sel) {
                word = ~word;
            }
        }
        return Math.min(n, (w << 6) + Long.numberOfTrailingZeros(word));
    }

    private static boolean bit(java.lang.foreign.MemorySegment bits, int i) {
        return ((bits.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i >>> 3) >>> (i & 7)) & 1) != 0;
    }

    private static boolean hasUnselected(int n, java.lang.foreign.MemorySegment selection) {
        for (int i = 0; i < n; i++) {
            if (!bit(selection, i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A FIXED_LEN_BYTE_ARRAY / BINARY decimal batch: the staged big-endian
     * bytes into the INT64 or DECIMAL128 lane.
     */
    private FieldVector flushDecimal(int rows, boolean hasNulls) {
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(rows);
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, rows);
        decoder.flushDecimal(rows, out.data(),
                hasNulls ? out.validity() : null, type == VecType.DECIMAL128);
        widenLane(out.data(), hasNulls ? out.validity() : null,
                rows);
        ArrowWrite.finish(out, rows, !hasNulls);
        return v;
    }

    /**
     * An INT96 timestamp batch: the staged 12-byte values into micros, then the
     * file's INT96 rebase.
     */
    private FieldVector flushInt96(int rows, boolean hasNulls) {
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(rows);
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, rows);
        decoder.flushInt96(rows, out.data(),
                hasNulls ? out.validity() : null);
        rebase.fixMicros(out.data(), hasNulls ? out.validity() : null,
                rows, true);
        ArrowWrite.finish(out, rows, !hasNulls);
        return v;
    }

    /**
     * The file's calendar rebase over a decoded date or timestamp batch (no-op
     * for other lanes).
     */
    private void rebaseLane(java.lang.foreign.MemorySegment data, java.lang.foreign.MemorySegment validity, int n) {
        if (dateLane) {
            rebase.fixDays(data, validity, n);
        } else if (timestampLane) {
            rebase.fixMicros(data, validity, n, false);
        }
    }

    /**
     * This file's calendar rebase and INT96 conversion (#568 item 6); {@link
     * CalendarRebase#NONE} by default.
     */
    @Override
    public void setRebase(CalendarRebase rebase) {
        this.rebase = rebase;
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
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, n);
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
                decoder.feedPage(nextKernelPage());
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
        rebaseLane(data, hasNulls ? validity : null, n);
        widenLane(data, hasNulls ? validity : null, n);
        ArrowWrite.finish(out, n, !hasNulls);
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
                decoder.feedPage(nextKernelPage());
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
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, rows);
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
        ArrowWrite.finish(out, rows, !hasNulls);
        return v;
    }

    private FieldVector flushBool(int rows, boolean hasNulls) {
        BaseFixedWidthVector v = (BaseFixedWidthVector) borrowFixed(rows);
        ArrowWrite.Buffers out = ArrowWrite.forWrite(v, rows);
        decoder.flushBool(rows, out.data(),
                hasNulls ? out.validity() : null);
        ArrowWrite.finish(out, rows, !hasNulls);
        return v;
    }

    /**
     * A uuid batch: the staged 16-byte values formatted into canonical text,
     * then emitted as the UTF8 batch of that text.
     */
    private FieldVector flushUuid(int rows, boolean hasNulls) {
        decoder.toUuidText(rows, null);
        return flushUtf8(rows, hasNulls);
    }

    private FieldVector flushUtf8(int rows, boolean hasNulls) {
        long bytes = decoder.utf8Bytes();
        org.apache.arrow.vector.BaseVariableWidthVector v = (org.apache.arrow.vector.BaseVariableWidthVector) borrowUtf8(rows, bytes);
        ArrowWrite.Buffers vb = ArrowWrite.forWrite(v, rows);
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
        // FLOAT is decoded as its own 4-byte bits (#559) into a Float4Vector; the operators widen it to the
        // FLOAT64 lane on the way in (#712), and every other type is the lane's own vector.
        BaseFixedWidthVector nv = float32 ? new org.apache.arrow.vector.Float4Vector(name, allocator) : (BaseFixedWidthVector) ArrowWrite.newVector(name, arrowType, allocator);
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
        org.apache.arrow.vector.BaseVariableWidthVector nv = (org.apache.arrow.vector.BaseVariableWidthVector) ArrowWrite.newVector(name, arrowType, allocator);
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
    @Override
    public void setPoolLimit(int limit) {
        synchronized (pool) {
            maxPool = Math.max(1, limit);
        }
    }

    /** #735: see {@link #reuseDecompress} and {@link #ownedView}. */
    @Override
    public void setReuseDecompress(boolean reuse) {
        this.reuseDecompress = reuse;
    }

    private FieldVector pooled() {
        synchronized (pool) {
            return pool.pollFirst();
        }
    }

    /**
     * The page for the decoder. #786: decoded where parquet-java left it -- a
     * heap buffer (a decompressed page, an uncompressed one in the chunk's
     * buffer) is read in place; anything else (a stream from the decompressor,
     * a direct buffer, a list of buffers) is copied into a reused buffer, as
     * every page was before. A v2 page's level and value sections are read
     * where each is, never copied into one buffer.
     */
    private ColumnChunkDecoder.Page toKernelPage(DataPage page) {
        if (page instanceof DataPageV1 v1) {
            java.nio.ByteBuffer view = heapView(v1.getBytes());
            if (view != null) {
                return ColumnChunkDecoder.Page.v1At(view.array(), view.arrayOffset() + view.position(), view.remaining(),
                        v1.getValueCount(), encoding(v1.getValueEncoding()));
            }
            pageOut.reset();
            writeInto(v1.getBytes(), pageOut);
            return ColumnChunkDecoder.Page.v1(pageOut.array(), pageOut.size(), v1.getValueCount(),
                    encoding(v1.getValueEncoding()));
        }
        DataPageV2 v2 = (DataPageV2) page;
        byte[] levels;
        int levelsAt;
        int levelsLength;
        java.nio.ByteBuffer lv = heapView(v2.getDefinitionLevels());
        if (lv != null) {
            levels = lv.array();
            levelsAt = lv.arrayOffset() + lv.position();
            levelsLength = lv.remaining();
        } else {
            levelsOut.reset();
            writeInto(v2.getDefinitionLevels(), levelsOut);
            levels = levelsOut.array();
            levelsAt = 0;
            levelsLength = levelsOut.size();
        }
        byte[] values;
        int valuesAt;
        int valuesLength;
        java.nio.ByteBuffer vv = heapView(v2.getData());
        if (vv != null) {
            values = vv.array();
            valuesAt = vv.arrayOffset() + vv.position();
            valuesLength = vv.remaining();
        } else {
            pageOut.reset();
            writeInto(v2.getData(), pageOut);
            values = pageOut.array();
            valuesAt = 0;
            valuesLength = pageOut.size();
        }
        // Each section's real extent: a reused or shared buffer is larger than the section.
        return ColumnChunkDecoder.Page.v2Split(levels, levelsAt, levelsLength, values, valuesAt, valuesLength,
                v2.getValueCount(), encoding(v2.getDataEncoding()));
    }

    /**
     * #786: {@code in}'s bytes as the heap buffer that already holds them, or
     * null when they are not one heap buffer. parquet-java hands its own buffer
     * back when it is of the allocator's kind; for anything else it would
     * allocate and copy, which this allocator refuses, so the caller copies
     * into its reused buffer instead.
     */
    private static java.nio.ByteBuffer heapView(BytesInput in) {
        try {
            java.nio.ByteBuffer b = in.toByteBuffer(NO_COPY, buf -> {});
            return b.hasArray() ? b : null;
        } catch (NotOneHeapBuffer e) {
            return null;
        }
    }

    /**
     * Refuses to allocate: {@link #heapView} only takes a buffer parquet-java
     * already has.
     */
    private static final org.apache.parquet.bytes.ByteBufferAllocator NO_COPY = new org.apache.parquet.bytes.ByteBufferAllocator() {
        @Override
        public java.nio.ByteBuffer allocate(int size) {
            throw new NotOneHeapBuffer();
        }

        @Override
        public void release(java.nio.ByteBuffer b) {}

        @Override
        public boolean isDirect() {
            return false;
        }
    };

    /**
     * {@link #NO_COPY}'s refusal: no stack trace, one instance (it is control
     * flow, once per such page).
     */
    private static final class NotOneHeapBuffer extends RuntimeException {
        private static final long serialVersionUID = 1L;

        NotOneHeapBuffer() {
            super("not one heap buffer", null, false, false);
        }
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
        return ParquetPageDecoder.decodeDictionary(
                MemorySegment.ofArray(data),
                0L,
                data.length,
                numValues,
                binaryDecimal || int96 ? VecType.UTF8 : physicalType,
                int96 ? 12 : fixedLength,
                scratch);
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
    @Override
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
