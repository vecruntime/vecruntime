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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowWriteTest {

    /**
     * Every vector class the leaf readers write, from its Arrow type, and the
     * lane of its layout.
     */
    @Test
    void newVectorAndLanes() {
        try (BufferAllocator a = new RootAllocator()) {
            check(a, new ArrowType.Int(32, true), IntVector.class, VecType.INT32);
            check(a, new ArrowType.Date(DateUnit.DAY), DateDayVector.class, VecType.INT32);
            check(a, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), Float4Vector.class, VecType.INT32);
            check(a, new ArrowType.Interval(IntervalUnit.YEAR_MONTH), IntervalYearVector.class, VecType.INT32);
            check(a, new ArrowType.Int(64, true), BigIntVector.class, VecType.INT64);
            check(a, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), TimeStampMicroTZVector.class, VecType.INT64);
            check(a, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), TimeStampMicroVector.class, VecType.INT64);
            check(a, new ArrowType.Duration(TimeUnit.MICROSECOND), DurationVector.class, VecType.INT64);
            check(a, new ArrowType.Time(TimeUnit.NANOSECOND, 64), TimeNanoVector.class, VecType.INT64);
            check(a, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), Float8Vector.class, VecType.FLOAT64);
            check(a, ArrowType.Bool.INSTANCE, BitVector.class, VecType.BOOL);
            check(a, new ArrowType.Decimal(30, 4, 128), DecimalVector.class, VecType.DECIMAL128);
            check(a, ArrowType.Utf8.INSTANCE, VarCharVector.class, VecType.UTF8);
        }
    }

    private static void check(BufferAllocator a, ArrowType t, Class<?> cls,
            VecType lane) {
        try (FieldVector v = ArrowWrite.newVector("c", t, a)) {
            assertInstanceOf(cls, v, t.toString());
            assertEquals(lane, ArrowWrite.laneOf(v), t.toString());
            assertTrue(v.getField()
                        .isNullable());
        }
    }

    @Test
    void writeFixedThenFinish() {
        try (BufferAllocator a = new RootAllocator();
             BigIntVector v = new BigIntVector("l", a)) {
            v.allocateNew(100);
            ArrowWrite.Buffers out = ArrowWrite.forWrite(v, 100);
            assertEquals(VecType.INT64, out.type());
            assertNull(out.offsets());
            for (int i = 0; i < 100; i++) {
                out.data().setAtIndex(VectorBuffers.LE_LONG, i, i * 7L);
            }
            ArrowWrite.finish(out, 100, true);
            assertEquals(100, v.getValueCount());
            assertEquals(0, v.getNullCount());
            assertEquals(693L, v.get(99));

            v.allocateNew(10);
            ArrowWrite.Buffers again = ArrowWrite.forWrite(v, 10);
            again.validity().fill((byte) 0);
            Bitmap.set(again.validity(), 3);
            again.data().setAtIndex(VectorBuffers.LE_LONG, 3, 42L);
            ArrowWrite.finish(again, 10, false);
            assertEquals(9, v.getNullCount());
            assertEquals(42L, v.get(3));
        }
    }

    @Test
    void refusesWhatTheReadersNeverWrite() {
        try (BufferAllocator a = new RootAllocator();
             org.apache.arrow.vector.SmallIntVector s = new org.apache.arrow.vector.SmallIntVector("s", a)) {
            s.allocateNew(4);
            assertNull(ArrowWrite.laneOf(s));
            assertThrows(IllegalArgumentException.class, () -> ArrowWrite.forWrite(s, 4));
        }
        try (BufferAllocator a = new RootAllocator();
             IntVector v = new IntVector("i", a)) {
            v.allocateNew(8);
            assertThrows(IllegalStateException.class, () -> ArrowWrite.forWrite(v, 1 << 20));
        }
    }

    @Test
    void copyDictionaryKeepsEntriesAndNulls() {
        String[] words = {"", "a", "bc", "def", "ghij"};
        try (Arena arena = Arena.ofConfined();
             BufferAllocator a = new RootAllocator()) {
            int n = words.length;
            MemorySegment offsets = arena.allocate(4L * (n + 1));
            MemorySegment data = arena.allocate(64);
            MemorySegment validity = arena.allocate(8);
            int pos = 0;
            for (int i = 0; i < n; i++) {
                offsets.setAtIndex(VectorBuffers.LE_INT, i, pos);
                byte[] b = words[i].getBytes(StandardCharsets.UTF_8);
                MemorySegment.copy(MemorySegment.ofArray(b), 0, data, pos, b.length);
                pos += b.length;
                if (i != 2) {
                    Bitmap.set(validity, i);
                }
            }
            offsets.setAtIndex(VectorBuffers.LE_INT, n, pos);
            VectorBuffers dict = SegmentVectorBuffers.utf8(n, validity, offsets, data);
            try (VarCharVector v = ArrowWrite.copyDictionary("d", dict, a)) {
                assertEquals(n, v.getValueCount());
                assertEquals(1, v.getNullCount());
                assertTrue(v.isNull(2));
                assertArrayEquals("def".getBytes(StandardCharsets.UTF_8), v.get(3));
                assertArrayEquals(new byte[0], v.get(0));
            }
        }
    }
}
