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
package io.vecruntime.spark.arrow;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * One build-side column the kernels have no lane for (an array, map or
 * struct payload, #547), held as Spark's on-heap column vector. The build rows
 * are appended in build order; {@link #seal()} adds one trailing null row, so a
 * joined batch reads the column as a {@link RemappedColumnVector} over the
 * build row ids the probe emits, a padded outer-join row ({@code -1}) mapped to
 * that null row. Once sealed the vector is only read, which is what lets the
 * tasks of an executor share it through a broadcast table.
 */
public final class BuildPayloadColumn implements AutoCloseable {

    private final DataType type;
    private final OnHeapColumnVector vector;
    private int rows;
    private int nullRow = -1;
    private long bytes;

    public BuildPayloadColumn(DataType type, int capacity) {
        this.type = type;
        this.vector = new OnHeapColumnVector(Math.max(capacity, 16), type);
    }

    /**
     * Whether a value of {@code dt} can be carried: the nested types built
     * from Spark's physical types a {@link WritableColumnVector} stores. A
     * type outside that set (an interval, a variant, a user-defined type)
     * keeps the join with Spark.
     */
    public static boolean carries(DataType dt) {
        if (dt instanceof ArrayType a) {
            return carries(a.elementType());
        }
        if (dt instanceof MapType m) {
            return carries(m.keyType()) && carries(m.valueType());
        }
        if (dt instanceof StructType s) {
            for (StructField f : s.fields()) {
                if (!carries(f.dataType())) {
                    return false;
                }
            }
            return true;
        }
        return dt instanceof BooleanType
                || dt instanceof ByteType
                || dt instanceof ShortType
                || dt instanceof IntegerType
                || dt instanceof LongType
                || dt instanceof FloatType
                || dt instanceof DoubleType
                || dt instanceof DateType
                || dt instanceof TimestampType
                || dt instanceof TimestampNTZType
                || dt instanceof YearMonthIntervalType
                || dt instanceof DayTimeIntervalType
                || dt instanceof StringType
                || dt instanceof BinaryType
                || dt instanceof DecimalType
                || dt instanceof NullType;
    }

    public DataType dataType() {
        return type;
    }

    /** The build rows appended (the trailing null row not counted). */
    public int numRows() {
        return rows;
    }

    /** An estimate of the heap the column holds, for the build budget. */
    public long bytes() {
        return bytes;
    }

    /**
     * Appends {@code src}'s value at {@code ordinal} (a row's column, an
     * array's element).
     */
    public void add(SpecializedGetters src, int ordinal) {
        if (nullRow >= 0) {
            throw new IllegalStateException("column is sealed");
        }
        append(vector, type, src, ordinal);
        rows++;
    }

    /** Ends the appends: adds the null row that padded outer-join rows read. */
    public BuildPayloadColumn seal() {
        if (nullRow < 0) {
            appendNull(vector, type);
            nullRow = rows;
        }
        return this;
    }

    /**
     * The underlying vector as a Spark column over exactly the {@code
     * numRows()} appended rows, no null-padding row (#57): the aggregate output
     * of {@code collect_list} / {@code collect_set} / {@code bloom_filter_agg},
     * read back as its declared array or binary type. Not sealed -- this view
     * is not the broadcast join's, which needs the trailing null row.
     */
    public ColumnVector plain() {
        return vector;
    }

    /**
     * The column over build rows {@code buildRows[from, to)}, in that order; a
     * row id of {@code -1} reads as null.
     */
    public ColumnVector view(int[] buildRows, int from, int to) {
        if (nullRow < 0) {
            throw new IllegalStateException("column is not sealed");
        }
        int[] rowIds = new int[to - from];
        for (int i = from; i < to; i++) {
            int r = buildRows[i];
            rowIds[i - from] = r < 0 ? nullRow : r;
        }
        return RemappedColumnVector.of(vector, rowIds);
    }

    @Override
    public void close() {
        vector.close();
    }

    private void append(WritableColumnVector v, DataType dt, SpecializedGetters src,
                        int ordinal) {
        if (src.isNullAt(ordinal)) {
            appendNull(v, dt);
            bytes += 1;
            return;
        }
        if (dt instanceof BooleanType) {
            v.appendBoolean(src.getBoolean(ordinal));
            bytes += 1;
        } else if (dt instanceof ByteType) {
            v.appendByte(src.getByte(ordinal));
            bytes += 1;
        } else if (dt instanceof ShortType) {
            v.appendShort(src.getShort(ordinal));
            bytes += 2;
        } else if (dt instanceof IntegerType || dt instanceof DateType || dt instanceof YearMonthIntervalType) {
            v.appendInt(src.getInt(ordinal));
            bytes += 4;
        } else if (dt instanceof LongType
                || dt instanceof TimestampType
                || dt instanceof TimestampNTZType
                || dt instanceof DayTimeIntervalType) {
            v.appendLong(src.getLong(ordinal));
            bytes += 8;
        } else if (dt instanceof FloatType) {
            v.appendFloat(src.getFloat(ordinal));
            bytes += 4;
        } else if (dt instanceof DoubleType) {
            v.appendDouble(src.getDouble(ordinal));
            bytes += 8;
        } else if (dt instanceof StringType) {
            UTF8String s = src.getUTF8String(ordinal);
            byte[] b = s.getBytes();
            v.appendByteArray(b, 0, b.length);
            bytes += 8 + b.length;
        } else if (dt instanceof BinaryType) {
            byte[] b = src.getBinary(ordinal);
            v.appendByteArray(b, 0, b.length);
            bytes += 8 + b.length;
        } else if (dt instanceof DecimalType d) {
            // The layout WritableColumnVector.putDecimal uses: unscaled int, long, or big-endian bytes.
            Decimal x = src.getDecimal(ordinal, d.precision(), d.scale());
            if (d.precision() <= Decimal.MAX_INT_DIGITS()) {
                v.appendInt((int) x.toUnscaledLong());
                bytes += 4;
            } else if (d.precision() <= Decimal.MAX_LONG_DIGITS()) {
                v.appendLong(x.toUnscaledLong());
                bytes += 8;
            } else {
                byte[] b = x.toJavaBigDecimal()
                            .unscaledValue()
                            .toByteArray();
                v.appendByteArray(b, 0, b.length);
                bytes += 8 + b.length;
            }
        } else if (dt instanceof ArrayType a) {
            ArrayData arr = src.getArray(ordinal);
            int n = arr.numElements();
            v.appendArray(n);
            bytes += 8;
            WritableColumnVector elements = v.arrayData();
            for (int i = 0; i < n; i++) {
                append(elements, a.elementType(), arr, i);
            }
        } else if (dt instanceof MapType m) {
            MapData map = src.getMap(ordinal);
            int n = map.numElements();
            // A map vector's offsets index its key child, which its value child shares.
            v.appendArray(n);
            bytes += 8;
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();
            WritableColumnVector keyVector = v.getChild(0);
            WritableColumnVector valueVector = v.getChild(1);
            for (int i = 0; i < n; i++) {
                append(keyVector, m.keyType(), keys, i);
                append(valueVector, m.valueType(), values, i);
            }
        } else if (dt instanceof StructType s) {
            StructField[] fields = s.fields();
            InternalRow row = src.getStruct(ordinal, fields.length);
            v.appendStruct(false);
            for (int i = 0; i < fields.length; i++) {
                append(v.getChild(i), fields[i].dataType(), row, i);
            }
        } else {
            throw new UnsupportedOperationException("build payload type " + dt.simpleString());
        }
    }

    private static void appendNull(WritableColumnVector v, DataType dt) {
        // A null struct appends a null to each of its fields too (appendNull refuses structs).
        if (dt instanceof StructType) {
            v.appendStruct(true);
        } else {
            v.appendNull();
        }
    }
}
