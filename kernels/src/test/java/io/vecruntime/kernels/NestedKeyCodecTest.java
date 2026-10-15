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
package io.vecruntime.kernels;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NestedKeyCodecTest {

    private static MemorySegment offsets(Arena arena, int... o) {
        MemorySegment s = ArrowLayout.allocateOffsets(arena, o.length - 1);
        MemorySegment.copy(o, 0, s, VectorBuffers.LE_INT, 0, o.length);
        return s;
    }

    private static final NestedKeyCodec.Shape SHAPE = new NestedKeyCodec.Shape(
            VecType.LIST,
            List.of(
                    new NestedKeyCodec.Shape(
                            VecType.STRUCT,
                            List.of(NestedKeyCodec.Shape.leaf(VecType.INT64), NestedKeyCodec.Shape.leaf(VecType.UTF8), NestedKeyCodec.Shape.leaf(VecType.DECIMAL128),
                                    NestedKeyCodec.Shape.leaf(VecType.BOOL)))));

    /**
     * array<struct<a:bigint, b:string, c:decimal(30,0), d:boolean>> over 6
     * rows: [s0, s1], null, [], [s0, s1] again, [s1, s0] (order matters), [null
     * struct, s2 with null fields].
     */
    private static VectorBuffers lane(Arena arena) {
        int n = 9;
        boolean[] structNulls = {false, false, false, false, false, false,
                true, false, false};
        VectorBuffers a = ArrowLayout.ofLongs(
                arena,
                new long[] {1, 2, 1, 2, 2, 1,
                        0, 0, 0},
                new boolean[] {false, false, false, false, false, false,
                        true, true, false});
        VectorBuffers b = ArrowLayout.ofStrings(
                arena,
                new String[] {"x", "", "x", "", "", "x",
                        null, "é", null});
        BigInteger big = BigInteger.TEN.pow(29).negate();
        VectorBuffers c = ArrowLayout.ofDecimal128(
                arena,
                new BigInteger[] {big, BigInteger.ONE, big, BigInteger.ONE, BigInteger.ONE, big,
                        null, null, BigInteger.ZERO},
                new boolean[] {false, false, false, false, false, false,
                        true, true, false});
        VectorBuffers d = ArrowLayout.ofBooleans(
                arena,
                new boolean[] {true, false, true, false, false, true,
                        false, false, true},
                null);
        VectorBuffers structs = NestedVectorBuffers.struct(n, ArrowLayout.validityFrom(arena, structNulls, n), List.of(a, b, c, d));
        boolean[] listNulls = {false, true, false, false, false, false};
        return NestedVectorBuffers.list(
                6,
                ArrowLayout.validityFrom(arena, listNulls, 6),
                offsets(arena, 0, 2, 2, 2, 4,
                        6, 8),
                structs);
    }

    @Test
    void equalValuesEncodeEquallyAndDistinctOnesDoNot() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers v = lane(arena);
            assertArrayEquals(NestedKeyCodec.encodeValue(v, 0), NestedKeyCodec.encodeValue(v, 3));
            assertFalse(java.util.Arrays.equals(NestedKeyCodec.encodeValue(v, 0), NestedKeyCodec.encodeValue(v, 4)));
            assertFalse(java.util.Arrays.equals(NestedKeyCodec.encodeValue(v, 2), NestedKeyCodec.encodeValue(v, 5)));
            VectorBuffers encoded = NestedKeyCodec.encode(arena, v, 6);
            assertTrue(encoded.isNull(1));
            assertFalse(encoded.isNull(2)); // an empty array is a value, not a null
            assertNotEquals(0, encoded.getUtf8Bytes(2).length);
        }
    }

    @Test
    void decodeRebuildsTheLaneAndReEncodesToTheSameBytes() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers v = lane(arena);
            byte[][] bytes = new byte[6][];
            for (int i = 0; i < 6; i++) {
                bytes[i] = v.isNull(i) ? null : NestedKeyCodec.encodeValue(v, i);
            }
            VectorBuffers back = NestedKeyCodec.decode(arena, SHAPE, bytes);
            assertEquals(VecType.LIST, back.type());
            assertEquals(6, back.length());
            for (int i = 0; i < 6; i++) {
                assertEquals(v.isNull(i), back.isNull(i));
                if (!v.isNull(i)) {
                    assertArrayEquals(bytes[i], NestedKeyCodec.encodeValue(back, i));
                }
            }
            NestedVectorBuffers list = (NestedVectorBuffers) back;
            VectorBuffers structs = list.children().get(0);
            assertEquals(8, structs.length());
            assertTrue(structs.isNull(6));
            VectorBuffers fields = structs.children().get(2);
            assertEquals(BigInteger.TEN.pow(29).negate(),
                    fields.getDecimal128(0));
            assertEquals("é",
                    structs.children()
                           .get(1)
                           .getString(7));
        }
    }
}
