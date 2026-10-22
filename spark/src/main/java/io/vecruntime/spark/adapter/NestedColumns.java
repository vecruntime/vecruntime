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
package io.vecruntime.spark.adapter;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.Decimal128;
import io.vecruntime.kernels.NestedVectorBuffers;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;

/**
 * Spark's array, map and struct column vectors as nested lanes (#705): a {@link
 * VecType#LIST}, {@link VecType#MAP} or {@link VecType#STRUCT} {@link
 * NestedVectorBuffers} whose leaves are ordinary lanes. The values are read
 * through Spark's own accessors ({@code getArray}, {@code getMap}, {@code
 * getStruct}), which every Spark column vector implements -- the writable
 * vectors of Spark's Parquet and ORC readers, Arrow vectors, ours -- so this
 * works on any input; the leaves are written once into {@code arena}.
 */
public final class NestedColumns {

    private NestedColumns() {}

    /**
     * The nested lane of {@code numRows} values of {@code cv} for the columnar
     * writer (#842): a column of our native scan ({@link
     * io.vecruntime.spark.arrow.VectorNestedColumnVector}) is read in place --
     * its null flags and offsets copied, its leaves adapted as flat columns,
     * zero-copy over Arrow -- and any other column through {@link #adapt}. The
     * leaves' lanes are the flat adapter's (a decimal of precision up to 18 is
     * an INT64 lane), which the writer's encoders take; the sort and the
     * expressions keep {@link #adapt}'s lanes.
     */
    public static VectorBuffers adaptForWrite(ColumnVector cv, DataType dt, int numRows,
            Arena arena) {
        if (dt instanceof org.apache.spark.sql.types.VariantType) {
            // A variant (#842): the struct of its value and metadata binaries, from Spark's getters.
            return adapt(cv, dt, numRows, arena);
        }
        boolean nested = dt instanceof StructType || dt instanceof ArrayType || dt instanceof MapType;
        if (!nested) {
            return ColumnVectorAdapters.adapt(cv, numRows, arena);
        }
        if (!(cv instanceof io.vecruntime.spark.arrow.VectorNestedColumnVector n)) {
            return adapt(cv, dt, numRows, arena);
        }
        boolean[] nulls = n.nullFlags();
        MemorySegment validity = null;
        if (nulls != null) {
            boolean any = false;
            for (int i = 0;
                 i < numRows && !any;
                 i++) {
                any = nulls[i];
            }
            if (any) {
                validity = ArrowLayout.validityFrom(arena, Arrays.copyOf(nulls, numRows), numRows);
            }
        }
        ColumnVector[] kids = n.childColumns();
        if (dt instanceof StructType s) {
            StructField[] fs = s.fields();
            List<VectorBuffers> fields = new ArrayList<>(fs.length);
            for (int f = 0; f < fs.length; f++) {
                fields.add(adaptForWrite(kids[f], fs[f].dataType(), numRows, arena));
            }
            return NestedVectorBuffers.struct(numRows, validity, fields);
        }
        int[] off = n.offsetArray();
        MemorySegment offsets = ArrowLayout.allocateOffsets(arena, numRows);
        MemorySegment.copy(off, 0, offsets, VectorBuffers.LE_INT, 0,
                numRows + 1);
        int childRows = off[numRows];
        if (dt instanceof ArrayType a) {
            return NestedVectorBuffers.list(numRows, validity, offsets, adaptForWrite(kids[0], a.elementType(), childRows, arena));
        }
        MapType m = (MapType) dt;
        return NestedVectorBuffers.map(numRows, validity, offsets, adaptForWrite(kids[0], m.keyType(), childRows, arena),
                adaptForWrite(kids[1], m.valueType(), childRows, arena));
    }

    /**
     * The nested lane of {@code numRows} values of {@code cv}, of type {@code
     * dt}.
     */
    public static NestedVectorBuffers adapt(ColumnVector cv, DataType dt, int numRows,
            Arena arena) {
        Node node = node(dt, numRows);
        // The whole column seen as one array: getArray(i) / getMap(i) / getStruct(i) per row.
        SpecializedGetters rows = new ColumnarArray(cv, 0, numRows);
        for (int i = 0; i < numRows; i++) {
            if (cv.isNullAt(i)) {
                node.appendNull();
            } else {
                node.append(rows, i);
            }
        }
        return (NestedVectorBuffers) node.build(arena);
    }

    private static Node node(DataType dt, int expected) {
        if (dt instanceof org.apache.spark.sql.types.VariantType) {
            return new VariantNode(expected);
        }
        if (dt instanceof ArrayType a) {
            return new ListNode(node(a.elementType(), expected), expected);
        }
        if (dt instanceof MapType m) {
            return new MapNode(node(m.keyType(), expected), node(m.valueType(), expected), expected);
        }
        if (dt instanceof StructType s) {
            StructField[] fs = s.fields();
            Node[] children = new Node[fs.length];
            for (int i = 0; i < fs.length; i++) {
                children[i] = node(fs[i].dataType(), expected);
            }
            return new StructNode(children, expected);
        }
        VecType t = TypeMapping.vecTypeOf(dt);
        if (t == null) {
            throw new UnsupportedOperationException("no lane for nested leaf type " + dt.simpleString());
        }
        return switch (t) {
            case INT32 -> new IntNode(dt, expected);
            case INT64 -> new LongNode(dt, expected);
            case FLOAT64 -> new DoubleNode(dt instanceof org.apache.spark.sql.types.FloatType, expected);
            case BOOL -> new BoolNode(expected);
            case UTF8 -> new BytesNode(dt instanceof BinaryType, expected);
            case DECIMAL128 -> new Decimal128Node((DecimalType) dt, expected);
            case LIST, MAP, STRUCT, FIXED_BINARY ->
                    throw new IllegalStateException("vecTypeOf is never nested or FIXED_BINARY");
        };
    }

    /** Growable validity, one entry per value of a node. */
    private abstract static class Node {
        int rows;
        boolean[] nulls;
        boolean anyNull;

        Node(int expected) {
            nulls = new boolean[Math.max(expected, 16)];
        }

        final void appendNull() {
            grow();
            nulls[rows] = true;
            anyNull = true;
            appendDefault();
            rows++;
        }

        final void append(SpecializedGetters g, int ordinal) {
            grow();
            appendValue(g, ordinal);
            rows++;
        }

        private void grow() {
            if (rows == nulls.length) {
                nulls = Arrays.copyOf(nulls, rows * 2);
                growValues(rows * 2);
            }
        }

        /** Element {@code ordinal} of {@code g}, or a null element. */
        final void appendElement(SpecializedGetters g, int ordinal) {
            if (g.isNullAt(ordinal)) {
                appendNull();
            } else {
                append(g, ordinal);
            }
        }

        MemorySegment validity(Arena arena) {
            return anyNull ? ArrowLayout.validityFrom(arena, nulls, rows) : null;
        }

        abstract void growValues(int capacity);

        abstract void appendDefault();

        abstract void appendValue(SpecializedGetters g, int ordinal);

        abstract VectorBuffers build(Arena arena);
    }

    private static final class IntNode extends Node {
        private final DataType dt;
        private int[] values;

        IntNode(DataType dt, int expected) {
            super(expected);
            this.dt = dt;
            values = new int[nulls.length];
        }

        @Override
        void growValues(int capacity) {
            values = Arrays.copyOf(values, capacity);
        }

        @Override
        void appendDefault() {
            values[rows] = 0;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            values[rows] = dt instanceof ByteType
                    ? g.getByte(ordinal)
                    : dt instanceof ShortType ? g.getShort(ordinal) : g.getInt(ordinal);
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment data = ArrowLayout.allocateData(arena, VecType.INT32, rows);
            MemorySegment.copy(values, 0, data, VectorBuffers.LE_INT, 0, rows);
            return SegmentVectorBuffers.fixedWidth(VecType.INT32, rows, validity(arena), data);
        }
    }

    private static final class LongNode extends Node {
        private final DecimalType decimal;
        private long[] values;

        LongNode(DataType dt, int expected) {
            super(expected);
            this.decimal = dt instanceof DecimalType d ? d : null;
            values = new long[nulls.length];
        }

        @Override
        void growValues(int capacity) {
            values = Arrays.copyOf(values, capacity);
        }

        @Override
        void appendDefault() {
            values[rows] = 0L;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            values[rows] = decimal == null ? g.getLong(ordinal) : g.getDecimal(ordinal, decimal.precision(), decimal.scale()).toUnscaledLong();
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment data = ArrowLayout.allocateData(arena, VecType.INT64, rows);
            MemorySegment.copy(values, 0, data, VectorBuffers.LE_LONG, 0, rows);
            return SegmentVectorBuffers.fixedWidth(VecType.INT64, rows, validity(arena), data);
        }
    }

    private static final class DoubleNode extends Node {
        private double[] values;

        /** A FLOAT leaf (#712): read as float, widened into the FLOAT64 lane. */
        private final boolean single;

        DoubleNode(boolean single, int expected) {
            super(expected);
            this.single = single;
            values = new double[nulls.length];
        }

        @Override
        void growValues(int capacity) {
            values = Arrays.copyOf(values, capacity);
        }

        @Override
        void appendDefault() {
            values[rows] = 0.0;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            values[rows] = single ? g.getFloat(ordinal) : g.getDouble(ordinal);
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment data = ArrowLayout.allocateData(arena, VecType.FLOAT64, rows);
            MemorySegment.copy(values, 0, data, VectorBuffers.LE_DOUBLE, 0, rows);
            return SegmentVectorBuffers.fixedWidth(VecType.FLOAT64, rows, validity(arena), data);
        }
    }

    private static final class BoolNode extends Node {
        private boolean[] values;

        BoolNode(int expected) {
            super(expected);
            values = new boolean[nulls.length];
        }

        @Override
        void growValues(int capacity) {
            values = Arrays.copyOf(values, capacity);
        }

        @Override
        void appendDefault() {
            values[rows] = false;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            values[rows] = g.getBoolean(ordinal);
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment data = ArrowLayout.allocateBitmap(arena, rows);
            for (int i = 0; i < rows; i++) {
                if (values[i]) {
                    Bitmap.set(data, i);
                }
            }
            return SegmentVectorBuffers.fixedWidth(VecType.BOOL, rows, validity(arena), data);
        }
    }

    /** Strings and binary: offsets and bytes (#705: binary rides the UTF8 lane). */
    private static final class BytesNode extends Node {
        private final boolean binary;
        private int[] offsets;
        private byte[] bytes = new byte[256];
        private int used;

        BytesNode(boolean binary, int expected) {
            super(expected);
            this.binary = binary;
            offsets = new int[nulls.length + 1];
        }

        @Override
        void growValues(int capacity) {
            offsets = Arrays.copyOf(offsets, capacity + 1);
        }

        @Override
        void appendDefault() {
            offsets[rows + 1] = used;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            byte[] v = binary ? g.getBinary(ordinal) : g.getUTF8String(ordinal).getBytes();
            if (used + v.length > bytes.length) {
                bytes = Arrays.copyOf(bytes,
                        Math.max(bytes.length * 2, used + v.length));
            }
            System.arraycopy(v, 0, bytes, used, v.length);
            used += v.length;
            offsets[rows + 1] = used;
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment off = ArrowLayout.allocateOffsets(arena, rows);
            MemorySegment.copy(offsets, 0, off, VectorBuffers.LE_INT, 0,
                    rows + 1);
            MemorySegment data = ArrowLayout.allocateBytes(arena, used);
            MemorySegment.copy(bytes, 0, data, ValueLayout.JAVA_BYTE, 0, used);
            return SegmentVectorBuffers.utf8(rows, validity(arena), off, data);
        }
    }

    private static final class Decimal128Node extends Node {
        private final DecimalType decimal;
        private long[] hi;
        private long[] lo;

        Decimal128Node(DecimalType decimal, int expected) {
            super(expected);
            this.decimal = decimal;
            hi = new long[nulls.length];
            lo = new long[nulls.length];
        }

        @Override
        void growValues(int capacity) {
            hi = Arrays.copyOf(hi, capacity);
            lo = Arrays.copyOf(lo, capacity);
        }

        @Override
        void appendDefault() {
            hi[rows] = 0L;
            lo[rows] = 0L;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            java.math.BigInteger v = g.getDecimal(ordinal, decimal.precision(), decimal.scale())
                    .toJavaBigDecimal()
                    .unscaledValue();
            hi[rows] = Decimal128.hiOf(v);
            lo[rows] = Decimal128.loOf(v);
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment data = ArrowLayout.allocateData(arena, VecType.DECIMAL128, rows);
            for (int i = 0; i < rows; i++) {
                Decimal128.set(data, i, hi[i], lo[i]);
            }
            return SegmentVectorBuffers.fixedWidth(VecType.DECIMAL128, rows, validity(arena), data);
        }
    }

    private static final class ListNode extends Node {
        private final Node elements;
        private int[] offsets;

        ListNode(Node elements, int expected) {
            super(expected);
            this.elements = elements;
            offsets = new int[nulls.length + 1];
        }

        @Override
        void growValues(int capacity) {
            offsets = Arrays.copyOf(offsets, capacity + 1);
        }

        @Override
        void appendDefault() {
            offsets[rows + 1] = elements.rows;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            ArrayData a = g.getArray(ordinal);
            int n = a.numElements();
            for (int j = 0; j < n; j++) {
                elements.appendElement(a, j);
            }
            offsets[rows + 1] = elements.rows;
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment off = ArrowLayout.allocateOffsets(arena, rows);
            MemorySegment.copy(offsets, 0, off, VectorBuffers.LE_INT, 0,
                    rows + 1);
            return NestedVectorBuffers.list(rows, validity(arena), off, elements.build(arena));
        }
    }

    private static final class MapNode extends Node {
        private final Node keys;
        private final Node values;
        private int[] offsets;

        MapNode(Node keys, Node values, int expected) {
            super(expected);
            this.keys = keys;
            this.values = values;
            offsets = new int[nulls.length + 1];
        }

        @Override
        void growValues(int capacity) {
            offsets = Arrays.copyOf(offsets, capacity + 1);
        }

        @Override
        void appendDefault() {
            offsets[rows + 1] = keys.rows;
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            MapData m = g.getMap(ordinal);
            ArrayData k = m.keyArray();
            ArrayData v = m.valueArray();
            int n = m.numElements();
            for (int j = 0; j < n; j++) {
                keys.appendElement(k, j);
                values.appendElement(v, j);
            }
            offsets[rows + 1] = keys.rows;
        }

        @Override
        VectorBuffers build(Arena arena) {
            MemorySegment off = ArrowLayout.allocateOffsets(arena, rows);
            MemorySegment.copy(offsets, 0, off, VectorBuffers.LE_INT, 0,
                    rows + 1);
            return NestedVectorBuffers.map(rows, validity(arena), off, keys.build(arena),
                    values.build(arena));
        }
    }

    /**
     * A variant, as the struct of its two binaries {@code (value, metadata)} the
     * engine carries it in ({@code NativeParquetSupport.VARIANT_READ_TYPE}, the
     * order of Spark's own variant vectors); a null variant's binaries are
     * null.
     */
    private static final class VariantNode extends Node {
        private final BytesNode value;
        private final BytesNode metadata;

        VariantNode(int expected) {
            super(expected);
            this.value = new BytesNode(true, expected);
            this.metadata = new BytesNode(true, expected);
        }

        @Override
        void growValues(int capacity) {}

        @Override
        void appendDefault() {
            value.appendNull();
            metadata.appendNull();
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            org.apache.spark.unsafe.types.VariantVal v = g.getVariant(ordinal);
            org.apache.spark.sql.catalyst.expressions.GenericInternalRow both = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(new Object[] {v.getValue(), v.getMetadata()});
            value.append(both, 0);
            metadata.append(both, 1);
        }

        @Override
        VectorBuffers build(Arena arena) {
            return NestedVectorBuffers.struct(rows, validity(arena),
                    List.of(value.build(arena), metadata.build(arena)));
        }
    }

    private static final class StructNode extends Node {
        private final Node[] fields;

        StructNode(Node[] fields, int expected) {
            super(expected);
            this.fields = fields;
        }

        @Override
        void growValues(int capacity) {}

        @Override
        void appendDefault() {
            // A null struct's fields are null, as Arrow and Spark both read them.
            for (Node f : fields) {
                f.appendNull();
            }
        }

        @Override
        void appendValue(SpecializedGetters g, int ordinal) {
            SpecializedGetters row = g.getStruct(ordinal, fields.length);
            for (int f = 0; f < fields.length; f++) {
                fields[f].appendElement(row, f);
            }
        }

        @Override
        VectorBuffers build(Arena arena) {
            List<VectorBuffers> built = new ArrayList<>(fields.length);
            for (Node f : fields) {
                built.add(f.build(arena));
            }
            return NestedVectorBuffers.struct(rows, validity(arena), built);
        }
    }
}
