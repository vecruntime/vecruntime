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
import java.lang.foreign.ValueLayout;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Utf8Mirror} (#565) gathers exactly what {@link GatherKernels#gatherUtf8}
 * gathers from the segments: offsets, bytes and validity, for columns with and
 * without nulls, a slice whose offsets do not start at zero, heap-backed
 * columns, empty and long strings, padded (negative) indices, ranges that do not
 * start at zero, counts around word boundaries and one reused scratch.
 */
class Utf8MirrorTest {

    private static String word(Random rnd) {
        int len = switch (rnd.nextInt(5)) {
            case 0 -> 0;
            case 1 -> rnd.nextInt(1, 4);
            case 2 -> rnd.nextInt(4, 16);
            case 3 -> rnd.nextInt(16, 40);
            default -> rnd.nextInt(40, 300);
        };
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < len; k++) {
            sb.append(rnd.nextInt(9) == 0 ? 'ñ' : (char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    @Test
    void gathersLikeTheSegmentGather() {
        Random rnd = new Random(5651);
        Utf8Mirror.Scratch scratch = new Utf8Mirror.Scratch();
        try (Arena arena = Arena.ofConfined()) {
            int[] sizes = {1, 63, 64, 65, 1000, 4097};
            for (int round = 0; round < sizes.length * 4; round++) {
                int n = sizes[round % sizes.length];
                boolean withNulls = round % 2 == 0;
                String[] wide = new String[n + 64];
                for (int i = 0; i < wide.length; i++) {
                    wide[i] = withNulls && rnd.nextInt(5) == 0
                            ? null
                            : word(rnd);
                }
                VectorBuffers in = ArrowLayout.ofStrings(arena, wide).slice(64, 64 + n);
                if (round % 4 == 3) {
                    in = SegmentVectorBuffers.utf8(in.length(), heap(in.validity()), heap(in.offsets()),
                            heap(in.data()));
                }
                assertTrue(Utf8Mirror.mirrors(in));
                Utf8Mirror m = Utf8Mirror.of(in);
                assertEquals(in.hasNulls(), m.hasNulls(), "round " + round);
                int count = rnd.nextInt(3 * n + 1) + 1;
                int[] idx = new int[count + 5];
                for (int o = 0; o < idx.length; o++) {
                    idx[o] = rnd.nextInt(8) == 0 ? -1 : rnd.nextInt(n);
                }
                int from = 5;
                int to = idx.length;
                int rows = to - from;
                long bytes = GatherKernels.gatherUtf8Bytes(in, idx, from, to);
                assertEquals(bytes, m.bytes(idx, from, to), "bytes round " + round);
                MemorySegment eo = ArrowLayout.allocateOffsets(arena, rows);
                MemorySegment ed = ArrowLayout.allocateBytes(arena, Math.max(bytes, 1));
                MemorySegment ev = ArrowLayout.allocateBitmap(arena, rows);
                GatherKernels.gatherUtf8(in, idx, from, to, eo, ed,
                        ev);
                MemorySegment ao = ArrowLayout.allocateOffsets(arena, rows);
                MemorySegment ad = ArrowLayout.allocateBytes(arena, Math.max(bytes, 1));
                MemorySegment av = ArrowLayout.allocateBitmap(arena, rows);
                m.gather(idx, from, to, ao, ad, av,
                        scratch);
                String what = "round " + round + " n=" + n;
                assertArrayEquals(
                        eo.asSlice(0, (long) (rows + 1) << 2).toArray(ValueLayout.JAVA_INT_UNALIGNED),
                        ao.asSlice(0, (long) (rows + 1) << 2).toArray(ValueLayout.JAVA_INT_UNALIGNED),
                        what + " offsets");
                assertArrayEquals(
                        ed.asSlice(0, bytes).toArray(ValueLayout.JAVA_BYTE),
                        ad.asSlice(0, bytes).toArray(ValueLayout.JAVA_BYTE),
                        what + " bytes");
                for (int o = 0; o < rows; o++) {
                    assertEquals(Bitmap.isSet(ev, o), Bitmap.isSet(av, o), what + " validity @" + o);
                }
            }
        }
    }

    @Test
    void aColumnOverTheCapIsNotMirrored() {
        try (Arena arena = Arena.ofConfined()) {
            int n = 3;
            MemorySegment off = ArrowLayout.allocateOffsets(arena, n);
            off.set(VectorBuffers.LE_INT, 0L, 0);
            off.set(VectorBuffers.LE_INT, 4L, 10);
            off.set(VectorBuffers.LE_INT, 8L, 20);
            off.set(VectorBuffers.LE_INT, 12L, (int) Utf8Mirror.MAX_BYTES + 21);
            // Only the offsets are read to decide; the data segment is never touched.
            VectorBuffers big = SegmentVectorBuffers.utf8(n, null, off, MemorySegment.NULL);
            assertFalse(Utf8Mirror.mirrors(big));
        }
    }
}
