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

import java.lang.foreign.MemorySegment;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Writing decoded batches into Arrow vectors, with no engine above it (#900
 * phase 2): the readers' view of a vector's buffers as {@code MemorySegment}s,
 * its lane, and the step that marks it written. The Spark module's {@code
 * ArrowVectorBuffers} / {@code ArrowOutput} do the same for the operators and
 * add the Spark column wrappers; the readers need only this part.
 */
public final class ArrowWrite {

    private ArrowWrite() {}

    /** A view over {@code [0, buf.capacity())} of an Arrow buffer. */
    public static MemorySegment segment(ArrowBuf buf) {
        long size = buf.capacity();
        if (size == 0) {
            return MemorySegment.NULL.reinterpret(0);
        }
        return MemorySegment.ofAddress(buf.memoryAddress()).reinterpret(size);
    }

    /**
     * The lane a vector's data buffer has the layout of: its own for the
     * engine's vector classes, and for the classes the scan writes with no
     * engine lane -- FLOAT's 4-byte bits on INT32, TIME's nanoseconds and a
     * day-time interval's micros on INT64, a year-month interval's months on
     * INT32. Null for any other.
     */
    public static VecType laneOf(ValueVector v) {
        if (v instanceof IntVector
                || v instanceof DateDayVector
                || v instanceof IntervalYearVector
                || v instanceof Float4Vector) {
            return VecType.INT32;
        }
        if (v instanceof BigIntVector
                || v instanceof TimeStampVector
                || v instanceof TimeNanoVector
                || v instanceof DurationVector) {
            return VecType.INT64;
        }
        if (v instanceof Float8Vector) {
            return VecType.FLOAT64;
        }
        if (v instanceof BitVector) {
            return VecType.BOOL;
        }
        if (v instanceof DecimalVector) {
            return VecType.DECIMAL128; // Arrow Decimal128: 16-byte little-endian, the lane's layout
        }
        if (v instanceof VarCharVector || v instanceof VarBinaryVector) {
            return VecType.UTF8;
        }
        return null;
    }

    /** An empty vector of {@code type} named {@code name}. */
    public static FieldVector newVector(String name, ArrowType type, BufferAllocator allocator) {
        return new Field(name, FieldType.nullable(type), null).createVector(allocator);
    }

    /**
     * Writable buffers over {@code v}, already sized for {@code length} values
     * ({@code allocateNew}); write the values and validity, then {@link
     * #finish}.
     */
    public static Buffers forWrite(ValueVector v, int length) {
        VecType lane = laneOf(v);
        if (lane == null) {
            throw new IllegalArgumentException("unsupported Arrow vector " + v.getClass().getName());
        }
        if (v.getValidityBuffer().capacity() < Bitmap.bytesFor(length)) {
            throw new IllegalStateException("vector not allocated for " + length + " elements");
        }
        MemorySegment offsets = null;
        if (v instanceof BaseVariableWidthVector) {
            offsets = segment(v.getOffsetBuffer());
        } else if (!(v instanceof BaseFixedWidthVector)) {
            throw new IllegalArgumentException("unexpected vector class " + v.getClass());
        }
        return new Buffers(v, lane, length, segment(v.getValidityBuffer()),
                segment(v.getDataBuffer()), offsets);
    }

    /**
     * Marks {@code length} values of {@code v} written; {@code allValid} sets
     * the validity to all ones (the decoder wrote no validity because the batch
     * has no nulls).
     */
    public static void finish(Buffers out, int length, boolean allValid) {
        if (allValid) {
            Bitmap.fill(out.validity, length, true);
        }
        ValueVector v = out.vector;
        if (v instanceof BaseVariableWidthVector vw) {
            // setValueCount "fills holes" in the offsets from lastSet+1; every offset was written directly.
            vw.setLastSet(length - 1);
        }
        v.setValueCount(length);
    }

    /**
     * A UTF8 dictionary as one Arrow vector, every entry valid unless {@code
     * dict} says otherwise.
     */
    public static VarCharVector copyDictionary(String name, VectorBuffers dict, BufferAllocator allocator) {
        int n = dict.length();
        int end = dict.offsets().get(VectorBuffers.LE_INT, (long) n << 2);
        VarCharVector v = new VarCharVector(name, allocator);
        v.allocateNew(Math.max(end, 1L), n);
        Buffers out = forWrite(v, n);
        MemorySegment.copy(dict.offsets(), 0, out.offsets, 0,
                ((long) n + 1) << 2);
        MemorySegment.copy(dict.data(), 0, out.data, 0, end);
        if (dict.hasNulls()) {
            MemorySegment.copy(dict.validity(), 0, out.validity, 0,
                    Bitmap.bytesFor(n));
        } else {
            Bitmap.fill(out.validity, n, true);
        }
        v.setLastSet(n - 1);
        v.setValueCount(n);
        return v;
    }

    /** The writable buffers of one vector, as {@link VectorBuffers}. */
    public static final class Buffers implements VectorBuffers {
        private final ValueVector vector;
        private final VecType lane;
        private final int length;
        private final MemorySegment validity;
        private final MemorySegment data;
        private final MemorySegment offsets;

        private Buffers(ValueVector vector, VecType lane, int length,
                        MemorySegment validity, MemorySegment data, MemorySegment offsets) {
            this.vector = vector;
            this.lane = lane;
            this.length = length;
            this.validity = validity;
            this.data = data;
            this.offsets = offsets;
        }

        public ValueVector vector() {
            return vector;
        }

        @Override
        public VecType type() {
            return lane;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public MemorySegment validity() {
            return validity;
        }

        @Override
        public MemorySegment data() {
            return data;
        }

        @Override
        public MemorySegment offsets() {
            return offsets;
        }

        @Override
        public VectorBuffers dictionary() {
            return null;
        }
    }
}
