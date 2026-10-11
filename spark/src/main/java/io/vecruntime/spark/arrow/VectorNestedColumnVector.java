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

import java.lang.foreign.MemorySegment;
import java.util.List;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.NestedVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A nested lane (#705) handed to Spark: an array, map or struct column over
 * child column vectors. A leaf child is the Arrow vector {@link ArrowOutput}
 * emits for its lane, read back through the same wrappers as any output column
 * (narrow ints, decimals, binary); a nested child is another of these. {@code
 * getArray} / {@code getMap} slice the children by the offsets, and Spark's own
 * {@code getStruct} reads the fields through {@link #getChild}.
 */
public final class VectorNestedColumnVector extends ColumnVector {

    private final int length;
    private final boolean[] nulls;
    private final int nullCount;
    private final int[] offsets;
    private final ColumnVector[] children;

    /**
     * The Arrow vector this column reads (its own validity and offsets),
     * released with it; null over a lane.
     */
    private final FieldVector owner;

    private VectorNestedColumnVector(DataType type, int length, boolean[] nulls,
            int[] offsets, ColumnVector[] children) {
        this(type, length, nulls, offsets, children, null);
    }

    private VectorNestedColumnVector(DataType type, int length, boolean[] nulls,
            int[] offsets, ColumnVector[] children, FieldVector owner) {
        super(type);
        this.owner = owner;
        this.length = length;
        this.nulls = nulls;
        int c = 0;
        if (nulls != null) {
            for (boolean b : nulls) {
                if (b) {
                    c++;
                }
            }
        }
        this.nullCount = c;
        this.offsets = offsets;
        this.children = children;
    }

    /**
     * The Spark column of the nested lane {@code in}, of Spark type {@code dt};
     * owns its children's memory.
     */
    public static ColumnVector of(String name, DataType dt, VectorBuffers in,
            BufferAllocator allocator) {
        if (!in.type().isNested()) {
            // A leaf under a nested value: Spark's ColumnarArray.copy reads primitives in bulk, nulls
            // included, and Arrow's getters raise on a null slot -- answer a null slot with zero.
            return new NullSafe(ArrowOutput.copy(name, dt, in, allocator), dt);
        }
        NestedVectorBuffers n = (NestedVectorBuffers) in;
        int len = n.length();
        boolean[] nulls = null;
        if (n.validity() != null) {
            nulls = new boolean[len];
            MemorySegment v = n.validity();
            for (int i = 0; i < len; i++) {
                nulls[i] = !Bitmap.isSet(v, i);
            }
        }
        int[] offsets = null;
        if (n.type() != VecType.STRUCT) {
            offsets = new int[len + 1];
            MemorySegment.copy(n.offsets(), VectorBuffers.LE_INT, 0, offsets, 0,
                    len + 1);
        }
        List<VectorBuffers> kids = n.children();
        ColumnVector[] children = new ColumnVector[kids.size()];
        if (dt instanceof ArrayType a) {
            children[0] = of(name + ".element", a.elementType(), kids.get(0),
                    allocator);
        } else if (dt instanceof MapType m) {
            children[0] = of(name + ".key", m.keyType(), kids.get(0),
                    allocator);
            children[1] = of(name + ".value", m.valueType(), kids.get(1),
                    allocator);
        } else if (dt instanceof StructType s) {
            StructField[] fs = s.fields();
            for (int f = 0; f < fs.length; f++) {
                children[f] = of(name + "." + fs[f].name(), fs[f].dataType(), kids.get(f),
                        allocator);
            }
        } else {
            throw new IllegalArgumentException("a nested lane of Spark type " + dt.simpleString());
        }
        return new VectorNestedColumnVector(dt, len, nulls, offsets, children);
    }

    /**
     * The Spark column over an Arrow list, map or struct vector of Spark type
     * {@code dt} (the shuffle's read side, #705), its leaves read through
     * {@link ArrowOutput#wrap}; takes ownership of {@code v}.
     */
    public static ColumnVector ofArrow(FieldVector v, DataType dt) {
        if (isLeaf(dt)) {
            return new NullSafe(ArrowOutput.wrap(v, dt), dt);
        }
        int len = v.getValueCount();
        boolean[] nulls = null;
        if (v.getNullCount() > 0) {
            nulls = new boolean[len];
            for (int i = 0; i < len; i++) {
                nulls[i] = v.isNull(i);
            }
        }
        if (dt instanceof StructType s) {
            StructVector sv = (StructVector) v;
            StructField[] fs = s.fields();
            ColumnVector[] children = new ColumnVector[fs.length];
            for (int f = 0; f < fs.length; f++) {
                children[f] = ofArrow((FieldVector) sv.getChildByOrdinal(f), fs[f].dataType());
            }
            return new VectorNestedColumnVector(dt, len, nulls, null, children, v);
        }
        ListVector lv = (ListVector) v;
        int[] offsets = new int[len + 1];
        if (len > 0) {
            for (int i = 0; i <= len; i++) {
                offsets[i] = lv.getOffsetBuffer().getInt((long) i * ListVector.OFFSET_WIDTH);
            }
        }
        ColumnVector[] children;
        if (dt instanceof MapType m) {
            StructVector entries = (StructVector) lv.getDataVector();
            children = new ColumnVector[] {ofArrow((FieldVector) entries.getChildByOrdinal(0), m.keyType()), ofArrow((FieldVector) entries.getChildByOrdinal(1), m.valueType())};
        } else if (dt instanceof ArrayType a) {
            children = new ColumnVector[] {ofArrow(lv.getDataVector(), a.elementType())};
        } else {
            throw new IllegalArgumentException("an Arrow nested vector of Spark type " + dt.simpleString());
        }
        return new VectorNestedColumnVector(dt, len, nulls, offsets, children, v);
    }

    /** Whether a Spark type is a leaf (not an array, map or struct). */
    private static boolean isLeaf(DataType dt) {
        return !(dt instanceof ArrayType || dt instanceof MapType || dt instanceof StructType);
    }

    public int length() {
        return length;
    }

    @Override
    public void close() {
        for (ColumnVector c : children) {
            c.close();
        }
        if (owner != null) {
            owner.close();
        }
    }

    @Override
    public boolean hasNull() {
        return nullCount > 0;
    }

    @Override
    public int numNulls() {
        return nullCount;
    }

    @Override
    public boolean isNullAt(int rowId) {
        return nulls != null && nulls[rowId];
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return new ColumnarArray(children[0], offsets[rowId], offsets[rowId + 1] - offsets[rowId]);
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        if (isNullAt(ordinal)) {
            return null;
        }
        return new ColumnarMap(children[0], children[1], offsets[ordinal], offsets[ordinal + 1] - offsets[ordinal]);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        return children[ordinal];
    }

    private UnsupportedOperationException notScalar() {
        return new UnsupportedOperationException("a " + dataType().simpleString() + " column has no scalar values");
    }

    @Override
    public boolean getBoolean(int rowId) {
        throw notScalar();
    }

    @Override
    public byte getByte(int rowId) {
        throw notScalar();
    }

    @Override
    public short getShort(int rowId) {
        throw notScalar();
    }

    @Override
    public int getInt(int rowId) {
        throw notScalar();
    }

    @Override
    public long getLong(int rowId) {
        throw notScalar();
    }

    @Override
    public float getFloat(int rowId) {
        throw notScalar();
    }

    @Override
    public double getDouble(int rowId) {
        throw notScalar();
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        throw notScalar();
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        throw notScalar();
    }

    @Override
    public byte[] getBinary(int rowId) {
        throw notScalar();
    }

    /**
     * A leaf column under a nested value whose primitive getters answer a null
     * slot with zero instead of raising (Arrow's {@code get} checks nulls;
     * Spark's bulk array copy reads every slot and then sets the null bits
     * itself).
     */
    static final class NullSafe extends ColumnVector {
        private final ColumnVector inner;

        NullSafe(ColumnVector inner, DataType type) {
            super(type); // the declared type: an Arrow vector infers StringType for a binary lane
            this.inner = inner;
        }

        @Override
        public void close() {
            inner.close();
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
            return inner.isNullAt(rowId);
        }

        @Override
        public boolean getBoolean(int rowId) {
            return !inner.isNullAt(rowId) && inner.getBoolean(rowId);
        }

        @Override
        public byte getByte(int rowId) {
            return inner.isNullAt(rowId) ? 0 : inner.getByte(rowId);
        }

        @Override
        public short getShort(int rowId) {
            return inner.isNullAt(rowId) ? 0 : inner.getShort(rowId);
        }

        @Override
        public int getInt(int rowId) {
            return inner.isNullAt(rowId) ? 0 : inner.getInt(rowId);
        }

        @Override
        public long getLong(int rowId) {
            return inner.isNullAt(rowId) ? 0L : inner.getLong(rowId);
        }

        @Override
        public float getFloat(int rowId) {
            return inner.isNullAt(rowId) ? 0f : inner.getFloat(rowId);
        }

        @Override
        public double getDouble(int rowId) {
            return inner.isNullAt(rowId) ? 0.0 : inner.getDouble(rowId);
        }

        @Override
        public Decimal getDecimal(int rowId, int precision, int scale) {
            return inner.isNullAt(rowId) ? null : inner.getDecimal(rowId, precision, scale);
        }

        @Override
        public UTF8String getUTF8String(int rowId) {
            return inner.isNullAt(rowId) ? null : inner.getUTF8String(rowId);
        }

        @Override
        public byte[] getBinary(int rowId) {
            return inner.isNullAt(rowId) ? null : inner.getBinary(rowId);
        }

        @Override
        public ColumnarArray getArray(int rowId) {
            return inner.getArray(rowId);
        }

        @Override
        public ColumnarMap getMap(int ordinal) {
            return inner.getMap(ordinal);
        }

        @Override
        public ColumnVector getChild(int ordinal) {
            return inner.getChild(ordinal);
        }
    }
}
