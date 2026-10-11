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
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nested lanes (#705): construction rules, per-value offsets, and slices of
 * LIST, MAP and STRUCT.
 */
class NestedVectorBuffersTest {

    private static MemorySegment offsets(Arena arena, int... o) {
        MemorySegment s = ArrowLayout.allocateOffsets(arena, o.length - 1);
        MemorySegment.copy(o, 0, s, VectorBuffers.LE_INT, 0, o.length);
        return s;
    }

    @Test
    void typesAreNestedAndHaveNoDataBuffer() {
        assertTrue(VecType.LIST.isNested());
        assertTrue(VecType.MAP.isNested());
        assertTrue(VecType.STRUCT.isNested());
        assertFalse(VecType.UTF8.isNested());
        assertFalse(VecType.LIST.isFixedWidth());
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers elements = ArrowLayout.ofInts(arena, new int[] {1, 2, 3}, null);
            NestedVectorBuffers list = NestedVectorBuffers.list(2, null, offsets(arena, 0, 1, 3), elements);
            assertEquals(MemorySegment.NULL, list.data());
            assertEquals(List.of(elements), list.children());
            assertTrue(elements.children()
                               .isEmpty());
            assertEquals(1, list.size(0));
            assertEquals(2, list.size(1));
            assertEquals(1, list.start(1));
            assertEquals(3, list.end(1));
        }
    }

    @Test
    void constructionChecksTheShape() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers ints = ArrowLayout.ofInts(arena, new int[] {1, 2}, null);
            VectorBuffers one = ArrowLayout.ofInts(arena, new int[] {1}, null);
            assertThrows(IllegalArgumentException.class, () -> NestedVectorBuffers.list(1, null, null, ints));
            assertThrows(IllegalArgumentException.class, () -> NestedVectorBuffers.map(1, null, offsets(arena, 0, 2), ints, one));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new NestedVectorBuffers(VecType.STRUCT, 1, null, offsets(arena, 0, 1),
                            List.of(ints)));
            assertThrows(IllegalArgumentException.class, () -> NestedVectorBuffers.struct(3, null, List.of(ints)));
            assertThrows(IllegalArgumentException.class, () -> new NestedVectorBuffers(VecType.INT32, 1, null, null, List.of()));
        }
    }

    @Test
    void slicesKeepTheChildrenAndShiftTheOffsetsOrTheFields() {
        try (Arena arena = Arena.ofConfined()) {
            int n = 200;
            int[] o = new int[n + 1];
            int[] values = new int[n * 2];
            for (int i = 0; i < n; i++) {
                o[i + 1] = o[i] + (i % 3);
                for (int j = o[i];
                     j < o[i + 1];
                     j++) {
                    values[j] = i * 10 + (j - o[i]);
                }
            }
            VectorBuffers elements = ArrowLayout.ofInts(arena, java.util.Arrays.copyOf(values, o[n]), null);
            boolean[] nulls = new boolean[n];
            nulls[70] = true;
            NestedVectorBuffers list = NestedVectorBuffers.list(n, ArrowLayout.validityFrom(arena, nulls, n), offsets(arena, o), elements);
            NestedVectorBuffers s = (NestedVectorBuffers) list.slice(64, 192);
            assertEquals(128, s.length());
            assertEquals(VecType.LIST, s.type());
            assertTrue(s.isNull(70 - 64));
            for (int i = 0; i < 128; i++) {
                assertEquals((64 + i) % 3, s.size(i));
                for (int j = s.start(i);
                     j < s.end(i);
                     j++) {
                    assertEquals((64 + i) * 10 + (j - s.start(i)),
                            s.children()
                             .get(0)
                             .getInt(j));
                }
            }
            VectorBuffers a = ArrowLayout.ofLongs(arena, new long[n], null);
            NestedVectorBuffers struct = NestedVectorBuffers.struct(n, null, List.of(list, a));
            NestedVectorBuffers ss = (NestedVectorBuffers) struct.slice(128, 200);
            assertEquals(72, ss.length());
            assertEquals(72,
                    ss.children()
                      .get(0)
                      .length());
            assertEquals(72,
                    ss.children()
                      .get(1)
                      .length());
            assertEquals((128 + 5) % 3, ((NestedVectorBuffers) ss.children().get(0)).size(5));
        }
    }
}
