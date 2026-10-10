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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A zero-copy view of rows {@code [offset, offset + length)} of another {@link
 * ColumnVector}: every read delegates to {@code inner} at {@code offset +
 * rowId}. Used by the native Parquet scan to emit {@code
 * columnarReaderBatchSize}-row batches over a whole row group decoded into one
 * reused vector, without copying (the alternative -- a {@code
 * TransferPair.splitAndTransfer} per batch -- copies).
 *
 * <p>{@link #close()} is a no-op: the underlying row-group vector is owned by
 * the reader, which recycles it for the next row group and frees it at task
 * end. A batch of these is closed by Spark's {@code ColumnarToRowExec} after it
 * reads it, which must not free the reader's vector.
 */
public final class SlicedColumnVector extends ColumnVector {

    private final ColumnVector inner;
    private final int offset;
    private final int innerLength;

    private SlicedColumnVector(ColumnVector inner, int offset, int innerLength) {
        super(inner.dataType());
        this.inner = inner;
        this.offset = offset;
        this.innerLength = innerLength;
    }

    /**
     * A view of {@code length} rows of {@code inner} (whose full length is
     * {@code innerLength}) at {@code offset}.
     */
    public static ColumnVector of(ColumnVector inner, int offset, int length,
            int innerLength) {
        if (inner instanceof SlicedColumnVector s) {
            return new SlicedColumnVector(s.inner, s.offset + offset, s.innerLength);
        }
        return new SlicedColumnVector(inner, offset, innerLength);
    }

    @Override
    public void close() {
        // No-op: the producer owns and recycles the underlying vector.
    }

    /** The wrapped (row-group) column this is a view of. */
    public ColumnVector inner() {
        return inner;
    }

    /** The row offset of this view into {@link #inner()}. */
    public int offset() {
        return offset;
    }

    /**
     * The wrapped column's full length (row-group rows), for adapting the
     * underlying vector.
     */
    public int innerLength() {
        return innerLength;
    }

    @Override
    public boolean hasNull() {
        return inner.hasNull();
    }

    @Override
    public int numNulls() {
        return inner.numNulls();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return inner.isNullAt(offset + rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return inner.getBoolean(offset + rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return inner.getByte(offset + rowId);
    }

    @Override
    public short getShort(int rowId) {
        return inner.getShort(offset + rowId);
    }

    @Override
    public int getInt(int rowId) {
        return inner.getInt(offset + rowId);
    }

    @Override
    public long getLong(int rowId) {
        return inner.getLong(offset + rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return inner.getFloat(offset + rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return inner.getDouble(offset + rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        return inner.getArray(offset + rowId);
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        return inner.getMap(offset + ordinal);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        return inner.getDecimal(offset + rowId, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        return inner.getUTF8String(offset + rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        return inner.getBinary(offset + rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        return inner.getChild(ordinal);
    }
}
