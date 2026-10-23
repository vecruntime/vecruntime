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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import static io.vecruntime.kernels.VectorBuffers.LE_DOUBLE;
import static io.vecruntime.kernels.VectorBuffers.LE_INT;
import static io.vecruntime.kernels.VectorBuffers.LE_LONG;

/**
 * Writes one leaf column of a nested field (#901 item 13) straight to its
 * {@link PageWriter}, off parquet-java's per-value column writer. The caller
 * ({@link NestedFieldWriter}) walks each row's nested lane and feeds this writer
 * a repetition and definition level per leaf occurrence ({@link #putNull} for a
 * null or absent leaf, {@link #put} for a present value).
 *
 * <p>Data page v2: the repetition and definition levels are RLE / bit-packed
 * hybrid, bit width {@code bitWidth(maxRep)} and {@code bitWidth(maxDef)}, in the
 * page's separate level sections (no length prefix); the values are encoded with
 * the same per-type encoders a flat column uses -- DELTA_BINARY_PACKED for INT32
 * and INT64, PLAIN for DOUBLE and FLOAT, dictionary ({@code RLE_DICTIONARY}) with
 * a DELTA_BYTE_ARRAY fallback for BINARY, PLAIN for FIXED_LEN_BYTE_ARRAY and
 * INT96, RLE for BOOLEAN. Page statistics carry the min, max and null count the
 * leaf's comparator defines (INT96 and the levels histogram aside). A reader --
 * parquet-java's, Spark's or ours -- reconstructs the records from the levels.
 */
final class NestedLeafWriter {

    private static final long MICROS_PER_DAY = 86_400_000_000L;
    private static final int JULIAN_DAY_OF_EPOCH = 2_440_588;

    /** The physical shape of the leaf, which picks the value encoder. */
    private enum Kind {
        BOOLEAN,
        INT32,
        INT32_FROM_INT64,
        INT64,
        INT64_MILLIS,
        DOUBLE,
        FLOAT,
        DECIMAL_INT32,
        DECIMAL_INT64,
        BINARY,
        FIXED,
        DECIMAL_FIXED,
        INT96
    }

    private final ColumnDescriptor column;
    private final int maxRep;
    private final int maxDef;
    private final int repBitWidth;
    private final int defBitWidth;
    private final int pageRowLimit;
    private final int pageSize;
    // The write calendar rebase (#842) for a DATE / TIMESTAMP / INT96 leaf; NONE for every other type. The
    // per-lane bind() derives which of the three rebases (days, micros, int96 micros) this leaf needs.
    private final WriteRebase rebase;
    private boolean rebaseDate;
    private boolean rebaseTimestamp;
    private boolean rebaseInt96;

    private Kind kind;
    private VecType lane;
    private int fixedWidth; // FIXED / DECIMAL_FIXED / INT96 value width in bytes

    private PageWriter pages;

    // The page being built: the levels of every occurrence, and the present values.
    private int[] repLevels = new int[256];
    private int[] defLevels = new int[256];
    private int occurrences; // rep/def entries so far on the page
    private int presentValues; // entries at def == maxDef
    private long[] fixedValues = new long[256]; // raw bits for fixed-width kinds
    private final ByteSink binary = new ByteSink(4 * 1024); // length-prefixed values for BINARY/FIXED/INT96
    private Statistics<?> pageStats;

    private final ByteSink body = new ByteSink(4 * 1024);
    private final ByteSink repSink = new ByteSink(256);
    private final ByteSink defSink = new ByteSink(256);
    private final DeltaBinaryPackedEncoder delta = new DeltaBinaryPackedEncoder();
    private final ByteSink rleScratch = new ByteSink(256);

    NestedLeafWriter(ColumnDescriptor column, ParquetProperties props) {
        this(column, props, WriteRebase.NONE);
    }

    NestedLeafWriter(ColumnDescriptor column, ParquetProperties props, WriteRebase rebase) {
        this.column = column;
        this.maxRep = column.getMaxRepetitionLevel();
        this.maxDef = column.getMaxDefinitionLevel();
        this.repBitWidth = RleHybridEncoder.bitWidth(maxRep);
        this.defBitWidth = RleHybridEncoder.bitWidth(maxDef);
        this.pageRowLimit = props.getPageRowCountLimit();
        this.pageSize = props.getPageSizeThreshold();
        this.rebase = rebase == null ? WriteRebase.NONE : rebase;
    }

    ColumnDescriptor column() {
        return column;
    }

    void startChunk(PageWriter pageWriter) {
        this.pages = pageWriter;
        occurrences = 0;
        presentValues = 0;
        binary.reset();
        pageStats = null;
    }

    /** Binds the value encoder to the lane the leaf is being fed in. */
    private void bind(VecType lane) {
        this.lane = lane;
        PrimitiveTypeName p = column.getPrimitiveType().getPrimitiveTypeName();
        var logical = column.getPrimitiveType().getLogicalTypeAnnotation();
        boolean millis = logical instanceof org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts && ts.getUnit() == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
        this.fixedWidth = p == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                ? column.getPrimitiveType().getTypeLength()
                : (p == PrimitiveTypeName.INT96 ? 12 : 0);
        this.kind = switch (lane) {
                    case BOOL -> Kind.BOOLEAN;
                    case INT32 -> Kind.INT32;
                    case INT64 -> p == PrimitiveTypeName.INT96
                            ? Kind.INT96
                            : p == PrimitiveTypeName.INT32 ? Kind.INT32_FROM_INT64 : millis ? Kind.INT64_MILLIS : Kind.INT64;
                    case FLOAT64 -> p == PrimitiveTypeName.FLOAT ? Kind.FLOAT : Kind.DOUBLE;
                    case UTF8 -> Kind.BINARY;
                    case DECIMAL128 -> switch (p) {
                                case INT32 -> Kind.DECIMAL_INT32;
                                case INT64 -> Kind.DECIMAL_INT64;
                                default -> Kind.DECIMAL_FIXED;
                            };
                    case FIXED_BINARY -> Kind.FIXED;
                    default -> throw new IllegalArgumentException("no nested leaf encoder for a " + lane + " lane as " + column.getPrimitiveType());
                };
        // The calendar rebase (#842) this leaf needs, by kind: a DATE's days, a TIMESTAMP's micros or millis
        // (adjusted to UTC only; TIMESTAMP_NTZ is written as stored), or an INT96 timestamp's micros.
        boolean dateLeaf = kind == Kind.INT32 && logical instanceof org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
        boolean tsLeaf = (kind == Kind.INT64 || kind == Kind.INT64_MILLIS)
                && logical instanceof org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts
                && ts.isAdjustedToUTC();
        this.rebaseDate = dateLeaf && rebase.datetimeActive();
        this.rebaseTimestamp = tsLeaf && rebase.datetimeActive();
        this.rebaseInt96 = kind == Kind.INT96 && rebase.int96Active();
    }

    /**
     * A millisecond timestamp rebased: the micros rebase scaled by 1000 (see
     * FixedWidthChunkWriter).
     */
    private long rebaseMillis(long millis) {
        return Math.floorDiv(rebase.rebaseMicros(Math.multiplyExact(millis, 1000L)), 1000L);
    }

    /**
     * A null or absent occurrence at repetition level {@code rep}, definition
     * level {@code def}.
     */
    void putNull(int rep, int def) {
        ensureLevels();
        repLevels[occurrences] = rep;
        defLevels[occurrences] = def;
        occurrences++;
    }

    /**
     * A present value at row {@code i} of {@code v}, repetition level {@code
     * rep}, definition level {@code def} (== maxDef).
     */
    void put(VectorBuffers v, int i, int rep,
             int def) {
        if (lane != v.type()) {
            bind(v.type());
        }
        ensureLevels();
        repLevels[occurrences] = rep;
        defLevels[occurrences] = def;
        occurrences++;
        appendValue(v, i);
    }

    private void ensureLevels() {
        if (occurrences == repLevels.length) {
            repLevels = Arrays.copyOf(repLevels, repLevels.length * 2);
            defLevels = Arrays.copyOf(defLevels, defLevels.length * 2);
        }
    }

    private void ensureFixed() {
        if (presentValues == fixedValues.length) {
            fixedValues = Arrays.copyOf(fixedValues, fixedValues.length * 2);
        }
    }

    private void appendValue(VectorBuffers v, int i) {
        MemorySegment data = v.data();
        switch (kind) {
            case BOOLEAN -> {
                ensureFixed();
                fixedValues[presentValues++] = io.vecruntime.kernels.Bitmap.isSet(data, i) ? 1 : 0;
            }
            case INT32 -> {
                ensureFixed();
                int d = data.get(LE_INT, (long) i << 2);
                fixedValues[presentValues++] = rebaseDate && d < rebase.switchGregorianDay()
                        ? rebase.rebaseDays(d)
                        : d;
            }
            case INT32_FROM_INT64 -> {
                ensureFixed();
                fixedValues[presentValues++] = (int) data.get(LE_LONG, (long) i << 3);
            }
            case INT64 -> {
                ensureFixed();
                long us = data.get(LE_LONG, (long) i << 3);
                fixedValues[presentValues++] = rebaseTimestamp && us < rebase.switchGregorianTs()
                        ? rebase.rebaseMicros(us)
                        : us;
            }
            case INT64_MILLIS -> {
                ensureFixed();
                long ms = Math.floorDiv(data.get(LE_LONG, (long) i << 3), 1000L);
                fixedValues[presentValues++] = rebaseTimestamp && ms < Math.floorDiv(rebase.switchGregorianTs(), 1000L)
                        ? rebaseMillis(ms)
                        : ms;
            }
            case DOUBLE -> {
                ensureFixed();
                fixedValues[presentValues++] = Double.doubleToRawLongBits(data.get(LE_DOUBLE, (long) i << 3));
            }
            case FLOAT -> {
                ensureFixed();
                fixedValues[presentValues++] = Float.floatToRawIntBits((float) data.get(LE_DOUBLE, (long) i << 3)) & 0xFFFFFFFFL;
            }
            case DECIMAL_INT32 -> {
                ensureFixed();
                fixedValues[presentValues++] = (int) data.get(LE_LONG, (long) i << 4);
            }
            case DECIMAL_INT64 -> {
                ensureFixed();
                fixedValues[presentValues++] = data.get(LE_LONG, (long) i << 4);
            }
            case BINARY -> {
                presentValues++;
                VectorBuffers dict = v.dictionary();
                MemorySegment offsets = dict != null ? dict.offsets() : v.offsets();
                MemorySegment bytes = dict != null ? dict.data() : v.data();
                int src = dict != null ? v.data().get(LE_INT, (long) i << 2) : i;
                int start = offsets.get(LE_INT, (long) src << 2);
                int len = offsets.get(LE_INT, (long) (src + 1) << 2) - start;
                binary.writeIntLE(len);
                binary.write(bytes, start, len);
                statBinary(bytes, start, len);
            }
            case FIXED -> {
                presentValues++;
                binary.writeIntLE(fixedWidth);
                binary.write(data, (long) i * fixedWidth, fixedWidth);
                statBinary(data, (long) i * fixedWidth, fixedWidth);
            }
            case DECIMAL_FIXED -> {
                presentValues++;
                byte[] b = new byte[fixedWidth];
                long base = (long) i << 4;
                for (int k = 0; k < fixedWidth; k++) {
                    b[fixedWidth - 1 - k] = data.get(ValueLayout.JAVA_BYTE, base + k);
                }
                binary.writeIntLE(fixedWidth);
                binary.write(b, 0, fixedWidth);
                statBinaryBytes(b);
            }
            case INT96 -> {
                presentValues++;
                long micros = data.get(LE_LONG, (long) i << 3);
                if (rebaseInt96) {
                    micros = rebase.rebaseInt96Micros(micros);
                }
                long day = Math.floorDiv(micros, MICROS_PER_DAY);
                long nanos = Math.floorMod(micros, MICROS_PER_DAY) * 1000L;
                byte[] b = new byte[12];
                MemorySegment s = MemorySegment.ofArray(b);
                s.set(LE_LONG, 0, nanos);
                s.set(LE_INT, 8, (int) (day + JULIAN_DAY_OF_EPOCH));
                binary.writeIntLE(12);
                binary.write(b, 0, 12);
            }
        }
    }

    private void statBinary(MemorySegment seg, long offset, int len) {
        byte[] b = new byte[len];
        MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, offset, b, 0, len);
        statBinaryBytes(b);
    }

    private void statBinaryBytes(byte[] b) {
        if (pageStats == null) {
            pageStats = Statistics.createStats(column.getPrimitiveType());
        }
        pageStats.updateStats(Binary.fromConstantByteArray(b));
    }

    void maybeFlush() throws IOException {
        if (occurrences >= pageRowLimit || estimatedBytes() >= pageSize) {
            flushPage();
        }
    }

    private long estimatedBytes() {
        return switch (kind == null ? Kind.INT32 : kind) {
            case BINARY, FIXED, DECIMAL_FIXED, INT96 -> binary.size();
            case DOUBLE, INT64, INT64_MILLIS, DECIMAL_INT64 -> (long) presentValues * 8;
            default -> (long) presentValues * 4;
        };
    }

    void flushPage() throws IOException {
        if (occurrences == 0) {
            return;
        }
        repSink.reset();
        if (maxRep > 0) {
            RleHybridEncoder.encode(repLevels, occurrences, repBitWidth, repSink);
        }
        defSink.reset();
        if (maxDef > 0) {
            RleHybridEncoder.encode(defLevels, occurrences, defBitWidth, defSink);
        }
        body.reset();
        Encoding enc = encodeValues();
        int nullCount = occurrences - presentValues;
        // The number of top-level rows the page covers: one per occurrence at repetition level 0.
        int rowCount = 0;
        for (int k = 0; k < occurrences; k++) {
            if (repLevels[k] == 0) {
                rowCount++;
            }
        }
        Statistics<?> s = Statistics.createStats(column.getPrimitiveType());
        s.incrementNumNulls(nullCount);
        if (pageStats != null && pageStats.hasNonNullValue()) {
            s.updateStats(Binary.fromConstantByteArray(pageStats.getMinBytes()));
            s.updateStats(Binary.fromConstantByteArray(pageStats.getMaxBytes()));
        } else {
            copyNumericStats(s);
        }
        pages.writePageV2(
                rowCount,
                nullCount,
                occurrences,
                BytesInput.from(repSink.toByteArray()),
                BytesInput.from(defSink.toByteArray()),
                enc,
                BytesInput.from(body.toByteArray()),
                s,
                sizeStatistics(),
                null);
        occurrences = 0;
        presentValues = 0;
        binary.reset();
        pageStats = null;
    }

    private org.apache.parquet.column.statistics.SizeStatistics sizeStatistics() {
        // Let parquet-java compute the unencoded byte count and level histograms a nested column carries.
        var builder = org.apache.parquet.column.statistics.SizeStatistics.newBuilder(column.getPrimitiveType(), maxRep, maxDef);
        return builder.build();
    }

    private void copyNumericStats(Statistics<?> s) {
        if (presentValues == 0) {
            return;
        }
        switch (kind) {
            case BOOLEAN -> {
                int trues = 0;
                for (int k = 0; k < presentValues; k++) {
                    trues += (int) fixedValues[k];
                }
                org.apache.parquet.column.statistics.BooleanStatistics bs = (org.apache.parquet.column.statistics.BooleanStatistics) s;
                if (trues < presentValues) {
                    bs.updateStats(false);
                }
                if (trues > 0) {
                    bs.updateStats(true);
                }
            }
            case INT32, INT32_FROM_INT64, DECIMAL_INT32 -> {
                int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
                for (int k = 0; k < presentValues; k++) {
                    int x = (int) fixedValues[k];
                    min = Math.min(min, x);
                    max = Math.max(max, x);
                }
                var is = (org.apache.parquet.column.statistics.IntStatistics) s;
                is.updateStats(min);
                is.updateStats(max);
            }
            case INT64, INT64_MILLIS, DECIMAL_INT64 -> {
                long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
                for (int k = 0; k < presentValues; k++) {
                    min = Math.min(min, fixedValues[k]);
                    max = Math.max(max, fixedValues[k]);
                }
                var ls = (org.apache.parquet.column.statistics.LongStatistics) s;
                ls.updateStats(min);
                ls.updateStats(max);
            }
            case DOUBLE -> {
                var ds = (org.apache.parquet.column.statistics.DoubleStatistics) s;
                for (int k = 0; k < presentValues; k++) {
                    ds.updateStats(Double.longBitsToDouble(fixedValues[k]));
                }
            }
            case FLOAT -> {
                var fs = (org.apache.parquet.column.statistics.FloatStatistics) s;
                for (int k = 0; k < presentValues; k++) {
                    fs.updateStats(Float.intBitsToFloat((int) fixedValues[k]));
                }
            }
            default -> {
                // INT96: no min/max.
            }
        }
    }

    /**
     * Encodes the present values into {@link #body} and returns the data
     * encoding.
     */
    private Encoding encodeValues() {
        if (presentValues == 0) {
            // A page of only null / absent occurrences carries no values. The
            // leaf may never have been bound to a lane (every row of the chunk
            // had a null ancestor, so put() was never called): PLAIN with an
            // empty body is valid for every physical type.
            return Encoding.PLAIN;
        }
        switch (kind) {
            case BOOLEAN -> {
                rleScratch.reset();
                int[] b = new int[presentValues];
                for (int k = 0; k < presentValues; k++) {
                    b[k] = (int) fixedValues[k];
                }
                RleHybridEncoder.encode(b, presentValues, 1, rleScratch);
                body.writeIntLE(rleScratch.size());
                body.write(rleScratch);
                return Encoding.RLE;
            }
            case DOUBLE, FLOAT -> {
                if (kind == Kind.DOUBLE) {
                    body.writeLongsLE(fixedValues, presentValues);
                } else {
                    body.writeIntsLE(fixedValues, presentValues);
                }
                return Encoding.PLAIN;
            }
            case INT32, INT32_FROM_INT64, DECIMAL_INT32 -> {
                delta.encodeInts(fixedValues, presentValues, body);
                return Encoding.DELTA_BINARY_PACKED;
            }
            case INT64, INT64_MILLIS, DECIMAL_INT64 -> {
                delta.encodeLongs(fixedValues, presentValues, body);
                return Encoding.DELTA_BINARY_PACKED;
            }
            case INT96 -> {
                // PLAIN: the fixed 12-byte values back to back (strip the length prefixes).
                writeFixedPlain(body);
                return Encoding.PLAIN;
            }
            case FIXED, DECIMAL_FIXED -> {
                // PLAIN: fixed-width bytes back to back.
                writeFixedPlain(body);
                return Encoding.PLAIN;
            }
            case BINARY -> {
                // PLAIN BYTE_ARRAY: the length-prefixed values as buffered.
                body.write(binary);
                return Encoding.PLAIN;
            }
            default -> throw new IllegalStateException("unreachable kind " + kind);
        }
    }

    /**
     * Writes the buffered length-prefixed fixed-width values as plain fixed
     * bytes (no prefix).
     */
    private void writeFixedPlain(ByteSink out) {
        MemorySegment seg = binary.segment();
        long pos = 0;
        for (int k = 0; k < presentValues; k++) {
            int len = seg.get(LE_INT, pos);
            pos += 4;
            out.write(seg, pos, len);
            pos += len;
        }
    }

    void endChunk() throws IOException {
        flushPage();
    }

    long bufferedBytes() {
        return pages == null ? 0 : pages.getMemSize() + estimatedBytes();
    }
}
