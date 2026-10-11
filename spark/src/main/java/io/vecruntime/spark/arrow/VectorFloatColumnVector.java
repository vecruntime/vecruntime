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

import org.apache.arrow.vector.Float8Vector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A FLOAT (REAL) column over a FLOAT64 lane (#712): the kernels compute on
 * doubles and every value is a float widened exactly (the compiler rounds float
 * arithmetic and casts back to single precision after every operation), so
 * Spark reads the declared type back through {@link #getFloat} -- an exact
 * narrowing -- while the operators see an ordinary {@link Float8Vector}. The
 * counterpart of {@link VectorNarrowIntColumnVector} for floats.
 */
public final class VectorFloatColumnVector extends ColumnVector {

    private final Float8Vector vector;
    private final boolean owns;

    public VectorFloatColumnVector(Float8Vector vector) {
        this(vector, true);
    }

    private VectorFloatColumnVector(Float8Vector vector, boolean owns) {
        super(FloatType$.MODULE$);
        this.vector = vector;
        this.owns = owns;
    }

    public Float8Vector vector() {
        return vector;
    }

    public boolean ownsMemory() {
        return owns;
    }

    /** Same vector, not owned. */
    public VectorFloatColumnVector borrow() {
        return new VectorFloatColumnVector(vector, false);
    }

    @Override
    public void close() {
        if (owns) {
            vector.close();
        }
    }

    @Override
    public boolean hasNull() {
        return vector.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
        return vector.getNullCount();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return vector.isNull(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return (float) vector.get(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return vector.get(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        throw new UnsupportedOperationException();
    }
}
