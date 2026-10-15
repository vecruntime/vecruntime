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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ColumnBuilder} over nested lanes (#705): appends under selections,
 * across calls, both memory modes.
 */
class ColumnBuilderNestedTest {

    private static MemorySegment offsets(Arena arena, int... o) {
        MemorySegment s = ArrowLayout.allocateOffsets(arena, o.length - 1);
        MemorySegment.copy(o, 0, s, VectorBuffers.LE_INT, 0, o.length);
        return s;
    }

    /**
     * array<struct<a:int, b:string>> over 5 rows: [x0, x1], null, [], [x2],
     * [null struct, x4].
     */
    private static VectorBuffers lane(Arena arena, int base) {
        VectorBuffers a = ArrowLayout.ofInts(
                arena,
                new int[] {base, base + 1, base + 2, 0,
                        base + 4},
                new boolean[] {false, false, false, true, false});
        VectorBuffers b = ArrowLayout.ofStrings(arena,
                new String[] {"p" + base, null, "", null, "q"});
        VectorBuffers structs = NestedVectorBuffers.struct(
                5,
                ArrowLayout.validityFrom(
                        arena,
                        new boolean[] {false, false, false, true, false},
                        5),
                List.of(a, b));
        return NestedVectorBuffers.list(
                5,
                ArrowLayout.validityFrom(
                        arena,
                        new boolean[] {false, true, false, false, false},
                        5),
                offsets(arena, 0, 2, 2, 2, 3,
                        5),
                structs);
    }

    private static byte[] value(VectorBuffers v, int i) {
        return v.isNull(i) ? null : NestedKeyCodec.encodeValue(v, i);
    }

    private static MemorySegment rows(Arena arena, int n, int... set) {
        MemorySegment sel = Bitmap.allocate(arena, n);
        for (int i : set) {
            Bitmap.set(sel, i);
        }
        return sel;
    }

    private static void check(ColumnBuilder b, Arena arena) {
        VectorBuffers in1 = lane(arena, 10);
        VectorBuffers in2 = lane(arena, 20);
        b.append(in1); // rows 0..4 of in1
        b.append(in2, rows(arena, 5, 1, 3, 4), 3); // null, [x2], [null, x4] of in2
        b.append(in1, rows(arena, 5, 2), 1); // [] of in1
        assertEquals(9, b.length());
        VectorBuffers v = b.view();
        assertEquals(VecType.LIST, v.type());
        int[][] expected = {
            {1, 0},
            {1, 1},
            {1, 2},
            {1, 3},
            {1, 4},
            {2, 1},
            {2, 3},
            {2, 4},
            {1, 2}
        };
        for (int r = 0; r < expected.length; r++) {
            VectorBuffers src = expected[r][0] == 1 ? in1 : in2;
            int row = expected[r][1];
            assertEquals(src.isNull(row), v.isNull(r), "row " + r);
            byte[] want = value(src, row);
            if (want != null) {
                assertArrayEquals(want, value(v, r), "row " + r);
            }
        }
        NestedVectorBuffers list = (NestedVectorBuffers) v;
        assertEquals(0, list.size(8)); // the empty array stays empty
        assertTrue(list.children()
                       .get(0)
                       .isNull(list.start(7))); // the null struct inside stays null
    }

    @Test
    void appendsNestedRowsUnderSelectionsOverACallersArena() {
        try (Arena arena = Arena.ofConfined()) {
            check(new ColumnBuilder(arena, VecType.LIST, 1), arena);
        }
    }

    @Test
    void appendsNestedRowsUnderSelectionsOwningItsMemory() {
        try (Arena arena = Arena.ofConfined();
             ColumnBuilder b = ColumnBuilder.owning(VecType.LIST, 1)) {
            check(b, arena);
        }
    }

    @Test
    void appendsAStructUnderASelection() {
        try (Arena arena = Arena.ofConfined()) {
            VectorBuffers a = ArrowLayout.ofLongs(arena, new long[] {1, 2, 3}, null);
            VectorBuffers in = NestedVectorBuffers.struct(3, ArrowLayout.validityFrom(arena, new boolean[] {false, true, false}, 3),
                    List.of(a));
            ColumnBuilder b = new ColumnBuilder(arena, VecType.STRUCT, 1);
            b.append(in, rows(arena, 3, 1, 2), 2);
            VectorBuffers v = b.view();
            assertEquals(2, v.length());
            assertTrue(v.isNull(0));
            assertEquals(3L,
                    v.children()
                     .get(0)
                     .getLong(1));
        }
    }
}
