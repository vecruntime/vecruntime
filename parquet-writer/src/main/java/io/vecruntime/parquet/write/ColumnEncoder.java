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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Writes a row range of one Arrow vector into a parquet-java {@link
 * ColumnWriter}, reading the vector's buffers directly. One implementation per
 * (Arrow vector, Parquet physical type) pair; {@link #forVector} binds a
 * column.
 *
 * <p>The values are the ones Spark's {@code ParquetWriteSupport} writes for the
 * same Spark type: dates as {@code INT32} days, timestamps as {@code INT64}
 * micros or millis or as Spark's {@code INT96} (Julian day and nanoseconds of
 * day, proleptic Gregorian, no rebase), decimals as {@code INT32} / {@code
 * INT64} unscaled values or big-endian {@code FIXED_LEN_BYTE_ARRAY} of the
 * precision's minimal width.
 */
abstract class ColumnEncoder {

    private static final long MICROS_PER_DAY = 86_400_000_000L;
    private static final int JULIAN_DAY_OF_EPOCH = 2_440_588;

    final int maxDef;

    ColumnEncoder(ColumnDescriptor column) {
        this.maxDef = column.getMaxDefinitionLevel();
    }

    /** Whether this encoder was bound to this vector's class. */
    abstract boolean accepts(FieldVector v);

    /** Writes rows {@code [from, to)} of {@code v}. */
    abstract void write(FieldVector v, int from, int to,
                        ColumnWriter w);

    static boolean isNull(FieldVector v, int i) {
        ArrowBuf validity = v.getValidityBuffer();
        return validity.capacity() > 0 && BitVectorHelper.get(validity, i) == 0;
    }

    static ColumnEncoder forVector(FieldVector v, ColumnDescriptor column) {
        PrimitiveType type = column.getPrimitiveType();
        PrimitiveTypeName p = type.getPrimitiveTypeName();
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (v instanceof BitVector && p == PrimitiveTypeName.BOOLEAN) {
            return new Bool(column);
        }
        if (p == PrimitiveTypeName.INT32 && (v instanceof IntVector || v instanceof DateDayVector)) {
            return new Int32(column, v.getClass(), 4);
        }
        if (p == PrimitiveTypeName.INT32 && v instanceof SmallIntVector) {
            return new Int32(column, v.getClass(), 2);
        }
        if (p == PrimitiveTypeName.INT32 && v instanceof TinyIntVector) {
            return new Int32(column, v.getClass(), 1);
        }
        if (p == PrimitiveTypeName.INT64 && v instanceof BigIntVector) {
            return new Int64(column, v.getClass(), 1);
        }
        if (v instanceof TimeStampMicroTZVector || v instanceof TimeStampMicroVector) {
            if (p == PrimitiveTypeName.INT96) {
                return new Int96(column, v.getClass());
            }
            if (p == PrimitiveTypeName.INT64 && logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
                return new Int64(column, v.getClass(),
                        ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS ? 1000 : 1);
            }
        }
        if (p == PrimitiveTypeName.FLOAT && v instanceof Float4Vector) {
            return new Float32(column);
        }
        if (p == PrimitiveTypeName.DOUBLE && v instanceof Float8Vector) {
            return new Float64(column);
        }
        if (p == PrimitiveTypeName.BINARY && (v instanceof VarCharVector || v instanceof VarBinaryVector)) {
            return new Bytes(column, v.getClass());
        }
        if (v instanceof DecimalVector) {
            switch (p) {
                case INT32:
                case INT64:
                    return new DecimalLong(column, p == PrimitiveTypeName.INT32);
                case FIXED_LEN_BYTE_ARRAY:
                    return new DecimalFixed(column, type.getTypeLength());
                default:
                    break;
            }
        }
        throw new IllegalArgumentException("no encoder for " + v.getClass().getSimpleName() + " as " + type);
    }

    // ------------------------------------------------------------------------------------------------------------

    private static final class Bool extends ColumnEncoder {
        Bool(ColumnDescriptor c) {
            super(c);
        }

        @Override
        boolean accepts(FieldVector v) {
            return v instanceof BitVector;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                } else {
                    w.write(BitVectorHelper.get(data, i) != 0, 0, maxDef);
                }
            }
        }
    }

    /** INT32 from a 1-, 2- or 4-byte signed vector (byte, short, int, date). */
    private static final class Int32 extends ColumnEncoder {
        private final Class<?> cls;
        private final int width;

        Int32(ColumnDescriptor c, Class<?> cls, int width) {
            super(c);
            this.cls = cls;
            this.width = width;
        }

        @Override
        boolean accepts(FieldVector v) {
            return v.getClass() == cls;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                    continue;
                }
                int x = switch (width) {
                    case 4 -> data.getInt((long) i * 4);
                    case 2 -> data.getShort((long) i * 2);
                    default -> data.getByte(i);
                };
                w.write(x, 0, maxDef);
            }
        }
    }

    /**
     * INT64 from a long or microsecond-timestamp vector, divided by {@code
     * divisor} (1000 for millis).
     */
    private static final class Int64 extends ColumnEncoder {
        private final Class<?> cls;
        private final long divisor;

        Int64(ColumnDescriptor c, Class<?> cls, long divisor) {
            super(c);
            this.cls = cls;
            this.divisor = divisor;
        }

        @Override
        boolean accepts(FieldVector v) {
            return v.getClass() == cls;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                } else {
                    long x = data.getLong((long) i * 8);
                    w.write(divisor == 1 ? x : Math.floorDiv(x, divisor), 0,
                            maxDef);
                }
            }
        }
    }

    /**
     * Spark's INT96 timestamp: 8 bytes of nanoseconds of the day, then 4 bytes
     * of Julian day, little-endian.
     */
    private static final class Int96 extends ColumnEncoder {
        private final Class<?> cls;

        Int96(ColumnDescriptor c, Class<?> cls) {
            super(c);
            this.cls = cls;
        }

        @Override
        boolean accepts(FieldVector v) {
            return v.getClass() == cls;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                    continue;
                }
                long micros = data.getLong((long) i * 8);
                long day = Math.floorDiv(micros, MICROS_PER_DAY);
                long nanos = Math.floorMod(micros, MICROS_PER_DAY) * 1000L;
                byte[] b = new byte[12];
                for (int k = 0; k < 8; k++) {
                    b[k] = (byte) (nanos >>> (8 * k));
                }
                int julian = (int) (day + JULIAN_DAY_OF_EPOCH);
                for (int k = 0; k < 4; k++) {
                    b[8 + k] = (byte) (julian >>> (8 * k));
                }
                w.write(Binary.fromConstantByteArray(b), 0, maxDef);
            }
        }
    }

    private static final class Float32 extends ColumnEncoder {
        Float32(ColumnDescriptor c) {
            super(c);
        }

        @Override
        boolean accepts(FieldVector v) {
            return v instanceof Float4Vector;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                } else {
                    w.write(data.getFloat((long) i * 4), 0, maxDef);
                }
            }
        }
    }

    private static final class Float64 extends ColumnEncoder {
        Float64(ColumnDescriptor c) {
            super(c);
        }

        @Override
        boolean accepts(FieldVector v) {
            return v instanceof Float8Vector;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                } else {
                    w.write(data.getDouble((long) i * 8), 0, maxDef);
                }
            }
        }
    }

    /** BINARY from a string or binary vector (32-bit offsets). */
    private static final class Bytes extends ColumnEncoder {
        private final Class<?> cls;

        Bytes(ColumnDescriptor c, Class<?> cls) {
            super(c);
            this.cls = cls;
        }

        @Override
        boolean accepts(FieldVector v) {
            return v.getClass() == cls;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf offsets = v.getOffsetBuffer();
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                    continue;
                }
                int start = offsets.getInt((long) i * 4);
                int end = offsets.getInt((long) (i + 1) * 4);
                byte[] b = new byte[end - start];
                data.getBytes(start, b);
                // Constant: the dictionary and the statistics may keep the value past this call.
                w.write(Binary.fromConstantByteArray(b), 0, maxDef);
            }
        }
    }

    /** A decimal of precision up to 18 as its unscaled value, INT32 or INT64. */
    private static final class DecimalLong extends ColumnEncoder {
        private final boolean int32;

        DecimalLong(ColumnDescriptor c, boolean int32) {
            super(c);
            this.int32 = int32;
        }

        @Override
        boolean accepts(FieldVector v) {
            return v instanceof DecimalVector;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                    continue;
                }
                // Arrow decimals are 16-byte little-endian two's complement: a value of precision <= 18 is its
                // low 8 bytes.
                long unscaled = data.getLong((long) i * 16);
                if (int32) {
                    w.write((int) unscaled, 0, maxDef);
                } else {
                    w.write(unscaled, 0, maxDef);
                }
            }
        }
    }

    /**
     * A decimal as a big-endian FIXED_LEN_BYTE_ARRAY of {@code length} bytes,
     * as Spark writes it.
     */
    private static final class DecimalFixed extends ColumnEncoder {
        private final int length;

        DecimalFixed(ColumnDescriptor c, int length) {
            super(c);
            this.length = length;
        }

        @Override
        boolean accepts(FieldVector v) {
            return v instanceof DecimalVector;
        }

        @Override
        void write(FieldVector v, int from, int to,
                   ColumnWriter w) {
            ArrowBuf data = v.getDataBuffer();
            for (int i = from; i < to; i++) {
                if (isNull(v, i)) {
                    w.writeNull(0, maxDef - 1);
                    continue;
                }
                long base = (long) i * 16;
                byte[] b = new byte[length];
                // Little-endian 16 bytes to big-endian: the low `length` bytes, most significant first.
                for (int k = 0; k < length; k++) {
                    b[length - 1 - k] = data.getByte(base + k);
                }
                w.write(Binary.fromConstantByteArray(b), 0, maxDef);
            }
        }
    }
}
