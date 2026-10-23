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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.FixedBinaryVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import static io.vecruntime.kernels.VectorBuffers.LE_DOUBLE;
import static io.vecruntime.kernels.VectorBuffers.LE_INT;
import static io.vecruntime.kernels.VectorBuffers.LE_LONG;

/**
 * Writes a row range of one column, in the engine's Arrow layout ({@link
 * VectorBuffers}), into a parquet-java {@link ColumnWriter}. One implementation
 * per (lane, Parquet physical type) pair; {@link #forColumn} binds a column.
 *
 * <p>The values are the ones Spark's {@code ParquetWriteSupport} writes for the
 * same Spark type: byte, short, int and date as {@code INT32}; long as {@code
 * INT64}; timestamps (an {@code INT64} lane of microseconds) as {@code INT64}
 * micros or millis, or as Spark's {@code INT96} (Julian day and nanoseconds of
 * the day, proleptic Gregorian, no rebase); float (a {@code FLOAT64} lane) as
 * {@code FLOAT}; decimals as {@code INT32} / {@code INT64} unscaled values or a
 * big-endian {@code FIXED_LEN_BYTE_ARRAY} of the precision's minimal width. A
 * dictionary-encoded string column is written from its ids, each dictionary
 * entry converted to a {@link Binary} once per dictionary.
 */
abstract class ColumnEncoder {

    private static final long MICROS_PER_DAY = 86_400_000_000L;
    private static final int JULIAN_DAY_OF_EPOCH = 2_440_588;

    final int maxDef;

    ColumnEncoder(ColumnDescriptor column) {
        this.maxDef = column.getMaxDefinitionLevel();
    }

    /** Writes rows {@code [from, to)} of {@code v}. */
    abstract void write(VectorBuffers v, int from, int to,
                        ColumnWriter w);

    /**
     * Writes the non-null value at row {@code i} of {@code v} at repetition
     * level {@code rep} and definition level {@code def}: a leaf of a nested
     * column, whose levels {@link NestedFieldWriter} works out.
     */
    abstract void put(VectorBuffers v, int i, int rep,
                      int def, ColumnWriter w);

    final boolean isNull(MemorySegment validity, int i) {
        return validity != null && !Bitmap.isSet(validity, i);
    }

    final void writeNull(ColumnWriter w) {
        if (maxDef == 0) {
            throw new IllegalStateException("null in a required column");
        }
        w.writeNull(0, maxDef - 1);
    }

    static ColumnEncoder forColumn(VecType lane, ColumnDescriptor column) {
        return forColumn(lane, column, WriteRebase.NONE);
    }

    /**
     * Binds an encoder for a (lane, column) pair, applying {@code rebase} to the
     * values of a DATE, a non-INT96 TIMESTAMP and an INT96 TIMESTAMP column as
     * Spark's {@code ParquetWriteSupport} applies its rebase functions.
     */
    static ColumnEncoder forColumn(VecType lane, ColumnDescriptor column, WriteRebase rebase) {
        PrimitiveType type = column.getPrimitiveType();
        PrimitiveTypeName p = type.getPrimitiveTypeName();
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        boolean decimal = logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
        boolean date = logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation;
        switch (lane) {
            case BOOL:
                if (p == PrimitiveTypeName.BOOLEAN) {
                    return new Bool(column);
                }
                break;
            case INT32:
                if (p == PrimitiveTypeName.INT32) {
                    // A DATE's days are rebased; a plain byte/short/int is written as stored.
                    return new Int32(column, date ? rebase : WriteRebase.NONE);
                }
                break;
            case INT64:
                if (p == PrimitiveTypeName.INT64) {
                    long divisor = logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS
                            ? 1000
                            : 1;
                    // A TIMESTAMP's micros are rebased before the millis division (Spark:
                    // microsToMillis(timestampRebaseFunc(micros))); a long / TIMESTAMP_NTZ / TIME is written as stored.
                    boolean timestamp = logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts && ts.isAdjustedToUTC();
                    return new Int64(column, divisor, timestamp ? rebase : WriteRebase.NONE);
                }
                if (p == PrimitiveTypeName.INT32 && decimal) {
                    return new LongToInt32(column);
                }
                // writeLegacyFormat stores a decimal of any precision as FIXED_LEN_BYTE_ARRAY; precision <= 18 is
                // carried on the INT64 lane of unscaled values (p > 18 on the DECIMAL128 lane, Decimal128Fixed).
                if (p == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && decimal) {
                    return new DecimalLongFixed(column, type.getTypeLength());
                }
                if (p == PrimitiveTypeName.INT96) {
                    return new Int96(column, rebase);
                }
                break;
            case FLOAT64:
                if (p == PrimitiveTypeName.DOUBLE) {
                    return new Float64(column, false);
                }
                if (p == PrimitiveTypeName.FLOAT) {
                    return new Float64(column, true);
                }
                break;
            case UTF8:
                if (p == PrimitiveTypeName.BINARY) {
                    return new Bytes(column);
                }
                break;
            case DECIMAL128:
                if (p == PrimitiveTypeName.INT32 || p == PrimitiveTypeName.INT64) {
                    return new Decimal128Long(column, p == PrimitiveTypeName.INT32);
                }
                if (p == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                    return new Decimal128Fixed(column, type.getTypeLength());
                }
                break;
            case FIXED_BINARY:
                if (p == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && !decimal) {
                    return new FixedBinary(column, type.getTypeLength());
                }
                break;
            default:
                break;
        }
        throw new IllegalArgumentException("no encoder for a " + lane + " lane as " + type);
    }

    /**
     * A {@link VecType#FIXED_BINARY} lane (Arrow FixedSizeBinary) as a {@code
     * FIXED_LEN_BYTE_ARRAY} of the same width: each value's bytes as they are.
     */
    private static final class FixedBinary extends ColumnEncoder {
        private final int length;

        FixedBinary(ColumnDescriptor c, int length) {
            super(c);
            this.length = length;
        }

        private byte[] value(VectorBuffers v, int i) {
            FixedBinaryVectorBuffers f = (FixedBinaryVectorBuffers) v;
            if (f.width() != length) {
                throw new IllegalArgumentException("a FIXED_BINARY(" + f.width() + ") lane for a fixed[" + length + "] column");
            }
            return f.bytes(i);
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            w.write(Binary.fromConstantByteArray(value(v, i)), rep, def);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                } else {
                    // Constant: the dictionary and the statistics may keep the value past this call.
                    w.write(Binary.fromConstantByteArray(value(v, i)), 0, maxDef);
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------

    private static final class Bool extends ColumnEncoder {
        Bool(ColumnDescriptor c) {
            super(c);
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            w.write(Bitmap.isSet(v.data(), i), rep, def);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                } else {
                    w.write(Bitmap.isSet(data, i), 0, maxDef);
                }
            }
        }
    }

    /**
     * INT32 from an INT32 lane (byte, short, int, date); a DATE's days are
     * rebased by {@code rebase}.
     */
    private static final class Int32 extends ColumnEncoder {
        private final WriteRebase rebase;

        Int32(ColumnDescriptor c, WriteRebase rebase) {
            super(c);
            this.rebase = rebase;
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            w.write(rebase.rebaseDays(v.data().get(LE_INT, (long) i << 2)),
                    rep, def);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                } else {
                    w.write(rebase.rebaseDays(data.get(LE_INT, (long) i << 2)), 0, maxDef);
                }
            }
        }
    }

    /**
     * INT64 from an INT64 lane (long, timestamp micros), rebased by {@code
     * rebase} (a TIMESTAMP, else {@link WriteRebase#NONE}) then divided by
     * {@code divisor} (1000 for millis), as Spark's {@code
     * microsToMillis(timestampRebaseFunc(micros))}.
     */
    private static final class Int64 extends ColumnEncoder {
        private final long divisor;
        private final WriteRebase rebase;

        Int64(ColumnDescriptor c, long divisor, WriteRebase rebase) {
            super(c);
            this.divisor = divisor;
            this.rebase = rebase;
        }

        private long value(long x) {
            long r = rebase.rebaseMicros(x);
            return divisor == 1 ? r : Math.floorDiv(r, divisor);
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            w.write(value(v.data().get(LE_LONG, (long) i << 3)),
                    rep, def);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                } else {
                    w.write(value(data.get(LE_LONG, (long) i << 3)), 0, maxDef);
                }
            }
        }
    }

    /**
     * A decimal of precision up to 9 held as an INT64 unscaled lane, written as
     * INT32.
     */
    private static final class LongToInt32 extends ColumnEncoder {
        LongToInt32(ColumnDescriptor c) {
            super(c);
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            w.write((int) v.data().get(LE_LONG, (long) i << 3), rep, def);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                } else {
                    w.write((int) data.get(LE_LONG, (long) i << 3), 0, maxDef);
                }
            }
        }
    }

    /**
     * Spark's INT96 timestamp: 8 bytes of nanoseconds of the day, then 4 bytes
     * of Julian day, little-endian. The micros are rebased by {@code rebase}
     * (Spark's {@code int96RebaseFunc}) before the Julian-day conversion.
     */
    private static final class Int96 extends ColumnEncoder {
        private final WriteRebase rebase;

        Int96(ColumnDescriptor c, WriteRebase rebase) {
            super(c);
            this.rebase = rebase;
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            w.write(int96(rebase.rebaseInt96Micros(v.data().get(LE_LONG, (long) i << 3))),
                    rep, def);
        }

        private static Binary int96(long micros) {
            long day = Math.floorDiv(micros, MICROS_PER_DAY);
            long nanos = Math.floorMod(micros, MICROS_PER_DAY) * 1000L;
            byte[] b = new byte[12];
            MemorySegment s = MemorySegment.ofArray(b);
            s.set(LE_LONG, 0, nanos);
            s.set(LE_INT, 8, (int) (day + JULIAN_DAY_OF_EPOCH));
            return Binary.fromConstantByteArray(b);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                    continue;
                }
                w.write(int96(rebase.rebaseInt96Micros(data.get(LE_LONG, (long) i << 3))), 0,
                        maxDef);
            }
        }
    }

    /**
     * DOUBLE, or FLOAT narrowed from the FLOAT64 lane a float column is carried
     * in (#712).
     */
    private static final class Float64 extends ColumnEncoder {
        private final boolean narrow;

        Float64(ColumnDescriptor c, boolean narrow) {
            super(c);
            this.narrow = narrow;
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            double x = v.data().get(LE_DOUBLE, (long) i << 3);
            if (narrow) {
                w.write((float) x, rep, def);
            } else {
                w.write(x, rep, def);
            }
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                } else {
                    double x = data.get(LE_DOUBLE, (long) i << 3);
                    if (narrow) {
                        w.write((float) x, 0, maxDef);
                    } else {
                        w.write(x, 0, maxDef);
                    }
                }
            }
        }
    }

    /**
     * BINARY from a UTF8 lane (string or binary): plain offsets and bytes, or
     * dictionary ids into a UTF8 dictionary, whose entries are converted to
     * {@link Binary} once and reused for every id that names them.
     */
    private static final class Bytes extends ColumnEncoder {
        private VectorBuffers dictionary;
        private Binary[] entries;

        Bytes(ColumnDescriptor c) {
            super(c);
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            VectorBuffers dict = v.dictionary();
            if (dict != null) {
                w.write(entries(dict)[v.data().get(LE_INT, (long) i << 2)], rep, def);
            } else {
                w.write(Binary.fromConstantByteArray(bytes(v.offsets(), v.data(), i)),
                        rep, def);
            }
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            VectorBuffers dict = v.dictionary();
            if (dict != null) {
                Binary[] values = entries(dict);
                MemorySegment ids = v.data();
                for (int i = from; i < to; i++) {
                    if (isNull(validity, i)) {
                        writeNull(w);
                    } else {
                        w.write(values[ids.get(LE_INT, (long) i << 2)], 0, maxDef);
                    }
                }
                return;
            }
            MemorySegment offsets = v.offsets();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                } else {
                    // Constant: the dictionary and the statistics may keep the value past this call.
                    w.write(Binary.fromConstantByteArray(bytes(offsets, data, i)), 0, maxDef);
                }
            }
        }

        @SuppressWarnings("ReferenceEquality") // the converted entries are keyed on the dictionary object itself
        private Binary[] entries(VectorBuffers dict) {
            if (dict != dictionary) {
                int n = dict.length();
                Binary[] out = new Binary[n];
                MemorySegment offsets = dict.offsets();
                MemorySegment data = dict.data();
                for (int k = 0; k < n; k++) {
                    out[k] = Binary.fromConstantByteArray(bytes(offsets, data, k));
                }
                dictionary = dict;
                entries = out;
            }
            return entries;
        }

        private static byte[] bytes(MemorySegment offsets, MemorySegment data, int i) {
            int start = offsets.get(LE_INT, (long) i << 2);
            int end = offsets.get(LE_INT, (long) (i + 1) << 2);
            byte[] b = new byte[end - start];
            MemorySegment.copy(data, ValueLayout.JAVA_BYTE, start, b, 0, b.length);
            return b;
        }
    }

    /**
     * A DECIMAL128 lane of precision up to 18 as its unscaled value, INT32 or
     * INT64: the low limb.
     */
    private static final class Decimal128Long extends ColumnEncoder {
        private final boolean int32;

        Decimal128Long(ColumnDescriptor c, boolean int32) {
            super(c);
            this.int32 = int32;
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            long unscaled = v.data().get(LE_LONG, (long) i << 4);
            if (int32) {
                w.write((int) unscaled, rep, def);
            } else {
                w.write(unscaled, rep, def);
            }
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                    continue;
                }
                long unscaled = data.get(LE_LONG, (long) i << 4);
                if (int32) {
                    w.write((int) unscaled, 0, maxDef);
                } else {
                    w.write(unscaled, 0, maxDef);
                }
            }
        }
    }

    /**
     * A DECIMAL128 lane as a big-endian FIXED_LEN_BYTE_ARRAY of {@code length}
     * bytes, as Spark writes it.
     */
    private static final class Decimal128Fixed extends ColumnEncoder {
        private final int length;

        Decimal128Fixed(ColumnDescriptor c, int length) {
            super(c);
            this.length = length;
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            MemorySegment data = v.data();
            long base = (long) i << 4;
            byte[] b = new byte[length];
            for (int k = 0; k < length; k++) {
                b[length - 1 - k] = data.get(ValueLayout.JAVA_BYTE, base + k);
            }
            w.write(Binary.fromConstantByteArray(b), rep, def);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                    continue;
                }
                long base = (long) i << 4;
                byte[] b = new byte[length];
                // Little-endian 16 bytes to big-endian: the low `length` bytes, most significant first.
                for (int k = 0; k < length; k++) {
                    b[length - 1 - k] = data.get(ValueLayout.JAVA_BYTE, base + k);
                }
                w.write(Binary.fromConstantByteArray(b), 0, maxDef);
            }
        }
    }

    /**
     * An INT64 lane of unscaled decimal values as a big-endian {@code
     * FIXED_LEN_BYTE_ARRAY} of {@code length} bytes: writeLegacyFormat stores a
     * decimal of any precision as FLBA, and a decimal of precision up to 18 is
     * carried on the INT64 lane (a DECIMAL128 lane for p > 18 goes to {@link
     * Decimal128Fixed}). The 8-byte value is sign-extended to {@code length}
     * bytes, most significant first, as Spark's {@code ParquetWriteSupport}
     * writes its {@code decimalBuffer}.
     */
    private static final class DecimalLongFixed extends ColumnEncoder {
        private final int length;

        DecimalLongFixed(ColumnDescriptor c, int length) {
            super(c);
            this.length = length;
        }

        private byte[] bytes(long unscaled) {
            byte[] b = new byte[length];
            for (int k = 0; k < length; k++) {
                b[length - 1 - k] = (byte) (unscaled >> (8 * k));
            }
            return b;
        }

        @Override
        void put(VectorBuffers v, int i, int rep,
                 int def, ColumnWriter w) {
            w.write(Binary.fromConstantByteArray(bytes(v.data().get(LE_LONG, (long) i << 3))),
                    rep, def);
        }

        @Override
        void write(VectorBuffers v, int from, int to,
                   ColumnWriter w) {
            MemorySegment validity = v.validity();
            MemorySegment data = v.data();
            for (int i = from; i < to; i++) {
                if (isNull(validity, i)) {
                    writeNull(w);
                    continue;
                }
                w.write(Binary.fromConstantByteArray(bytes(data.get(LE_LONG, (long) i << 3))), 0,
                        maxDef);
            }
        }
    }
}
