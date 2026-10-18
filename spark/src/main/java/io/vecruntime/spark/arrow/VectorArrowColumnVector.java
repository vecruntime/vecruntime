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

import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

/**
 * Spark's {@link ArrowColumnVector} hides the wrapped {@link ValueVector}. Our
 * operators emit this subclass so downstream spark-vector operators can get
 * back to the Arrow buffers without a copy.
 *
 * <p>A column may be <em>borrowed</em>: a projection that forwards an input
 * column unchanged wraps the child's vector without taking ownership, so
 * closing the output batch leaves the child's memory alone (the child releases
 * it when it produces its next batch).
 *
 * <p>A column may be <em>reusable</em>: a producer that refills the same vector
 * for every batch (the range leaf) owns it and releases it at task end, and
 * Spark's {@code ColumnarToRowExec} must not free it after reading a batch --
 * it calls {@link #closeIfFreeable()} on every batch it has consumed, which
 * Spark's own reusable {@code WritableColumnVector}s answer with a no-op, and
 * so does a reusable column here. {@link #close()} still frees it.
 */
public final class VectorArrowColumnVector extends ArrowColumnVector {

    private final ValueVector valueVector;
    private final boolean owns;
    private final boolean reusable;

    public VectorArrowColumnVector(ValueVector vector) {
        this(vector, true, false);
    }

    public VectorArrowColumnVector(ValueVector vector, boolean owns) {
        this(vector, owns, false);
    }

    private VectorArrowColumnVector(ValueVector vector, boolean owns, boolean reusable) {
        super(vector);
        this.valueVector = vector;
        this.owns = owns;
        this.reusable = reusable;
        this.bitValidity = vector instanceof org.apache.arrow.vector.BaseFixedWidthVector || vector instanceof org.apache.arrow.vector.BaseVariableWidthVector;
    }

    /**
     * Whether the vector keeps one validity bit per slot in its validity buffer
     * (read by {@link #isNullAt}).
     */
    private final boolean bitValidity;

    /**
     * #812: the validity bit read straight from the buffer's address. Arrow's
     * {@code isNull} goes through the accessor and a bounds-checked {@code
     * ArrowBuf.getByte}; in a nested read that is a quarter of the time, once per
     * element (the map benchmark). The buffer is read on every call, so a vector
     * refilled or reallocated between batches is still read where it lives now.
     */
    @Override
    public boolean isNullAt(int rowId) {
        if (bitValidity) {
            long addr = valueVector.getValidityBuffer().memoryAddress();
            return ((org.apache.spark.unsafe.Platform.getByte(null, addr + (rowId >>> 3)) >> (rowId & 7)) & 1) == 0;
        }
        return super.isNullAt(rowId);
    }

    /**
     * #812: Arrow's {@code IntVector.get} checks the validity bit again (and
     * throws on a null slot) before reading; Spark only reads a value it has
     * checked with {@link #isNullAt} or does not care about, so the value is read
     * straight from the data buffer, as Spark's own vectors read it.
     */
    @Override
    public int getInt(int rowId) {
        if (valueVector instanceof org.apache.arrow.vector.IntVector v) {
            return org.apache.spark.unsafe.Platform.getInt(null, v.getDataBuffer().memoryAddress() + 4L * rowId);
        }
        return super.getInt(rowId);
    }

    /** #812: as {@link #getInt}, for a {@code BigIntVector}. */
    @Override
    public long getLong(int rowId) {
        if (valueVector instanceof org.apache.arrow.vector.BigIntVector v) {
            return org.apache.spark.unsafe.Platform.getLong(null, v.getDataBuffer().memoryAddress() + 8L * rowId);
        }
        return super.getLong(rowId);
    }

    /**
     * #812: a string read as Spark's accessor reads it (null for a null slot,
     * else the bytes in place, no copy), without Arrow's bounds-checked buffer
     * reads: the offsets and bytes straight from their addresses.
     */
    @Override
    public org.apache.spark.unsafe.types.UTF8String getUTF8String(int rowId) {
        if (valueVector instanceof org.apache.arrow.vector.VarCharVector v) {
            if (isNullAt(rowId)) {
                return null;
            }
            long offsets = v.getOffsetBuffer().memoryAddress() + 4L * rowId;
            int start = org.apache.spark.unsafe.Platform.getInt(null, offsets);
            int end = org.apache.spark.unsafe.Platform.getInt(null, offsets + 4);
            return org.apache.spark.unsafe.types.UTF8String.fromAddress(null, v.getDataBuffer().memoryAddress() + start, end - start);
        }
        return super.getUTF8String(rowId);
    }

    /**
     * An owned column over a vector its producer refills for every batch:
     * survives Spark's per-batch {@link #closeIfFreeable()}, freed by {@link
     * #close()}.
     */
    public static VectorArrowColumnVector reusable(ValueVector vector) {
        return new VectorArrowColumnVector(vector, true, true);
    }

    @Override
    public ValueVector getValueVector() {
        return valueVector;
    }

    /**
     * A BINARY column rides the UTF8 lane (#705), so an output written by a
     * string kernel is a {@code VarCharVector} even when the attribute is
     * binary; Spark then reads it with {@code getBinary}, which its string
     * accessor does not implement. The bytes are the value.
     */
    @Override
    public byte[] getBinary(int rowId) {
        if (valueVector instanceof org.apache.arrow.vector.VarCharVector v) {
            return v.isNull(rowId) ? null : v.get(rowId);
        }
        return super.getBinary(rowId);
    }

    public boolean ownsMemory() {
        return owns;
    }

    /** Same vector, not owned. */
    public VectorArrowColumnVector borrow() {
        return new VectorArrowColumnVector(valueVector, false, false);
    }

    @Override
    public void closeIfFreeable() {
        if (!reusable) {
            close();
        }
    }

    @Override
    public void close() {
        if (owns) {
            super.close();
        }
    }
}
