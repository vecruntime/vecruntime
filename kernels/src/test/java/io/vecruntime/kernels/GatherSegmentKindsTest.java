/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import io.vecruntime.kernels.reference.ScalarReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The gathers that dispatch on {@link MemorySegment#isNative} (#565) give the
 * reference's result on native, heap and mixed segments: every input and output
 * segment of a call is native, or any one of them is on the heap, so both bodies
 * of each kernel run. Output bitmaps are allocated at exactly the bytes the rows
 * need, so the tail-word store is exercised as well as the whole-word one.
 */
class GatherSegmentKindsTest {

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    private static VectorBuffers strings(Arena arena, Random rnd, int n,
            double nullFraction) {
        String[] v = new String[n];
        for (int i = 0; i < n; i++) {
            v[i] = rnd.nextDouble() < nullFraction ? null : switch (rnd.nextInt(7)) {
                        case 0 -> "";
                        case 1 -> "a";
                        case 2 -> "é" + rnd.nextInt(3);
                        case 3 -> "eight888";
                        case 4 -> "a string of more than sixteen bytes " + rnd.nextInt(100);
                        case 5 -> "four";
                        default -> "s" + rnd.nextInt(30);
                    };
        }
        return ArrowLayout.ofStrings(arena, v);
    }

    private static int[] indices(Random rnd, int count, int n) {
        int[] idx = new int[count];
        for (int i = 0; i < count; i++) {
            idx[i] = n == 0 || rnd.nextInt(6) == 0
                    ? -1
                    : rnd.nextInt(n);
        }
        return idx;
    }

    /** 0: all native; 1: heap input; 2: heap output; 3: heap validity only. */
    private static final int KINDS = 4;

    @Test
    void utf8GathersMatchTheReferenceOnEverySegmentKind() {
        Random rnd = new Random(565);
        for (int n : TestData.LENGTHS) {
            for (int kind = 0; kind < KINDS; kind++) {
                try (Arena arena = Arena.ofConfined()) {
                    VectorBuffers nat = strings(arena, rnd, n, 0.2);
                    VectorBuffers in = switch (kind) {
                        case 1 -> SegmentVectorBuffers.utf8(n, heap(nat.validity()), heap(nat.offsets()),
                                heap(nat.data()));
                        case 3 -> SegmentVectorBuffers.utf8(n, heap(nat.validity()), nat.offsets(),
                                nat.data());
                        default -> nat;
                    };
                    int count = rnd.nextInt(2 * n + 2);
                    int[] idx = indices(rnd, count + 4, n);
                    int from = 2, to = 2 + count;
                    long bytes = GatherKernels.gatherUtf8Bytes(in, idx, from, to);
                    MemorySegment eo = ArrowLayout.allocateOffsets(arena, count);
                    MemorySegment ed = ArrowLayout.allocateBytes(arena, Math.max(bytes, 1));
                    MemorySegment ev = arena.allocate(Math.max(1, Bitmap.bytesFor(count)));
                    ScalarReference.gatherUtf8(nat, idx, from, to, eo, ed,
                            ev);
                    long expBytes = eo.get(VectorBuffers.LE_INT, (long) count << 2);
                    assertEquals(expBytes, bytes, "bytes n=" + n + " kind=" + kind);
                    MemorySegment ao = ArrowLayout.allocateOffsets(arena, count);
                    MemorySegment ad = ArrowLayout.allocateBytes(arena, Math.max(bytes, 1));
                    MemorySegment av = arena.allocate(Math.max(1, Bitmap.bytesFor(count)));
                    if (kind == 2) {
                        ao = heap(ao);
                        ad = heap(ad);
                        av = heap(av);
                    }
                    GatherKernels.gatherUtf8(in, idx, from, to, ao, ad,
                            av);
                    assertArrayEquals(
                            eo.asSlice(0, ((long) count + 1) << 2).toArray(ValueLayout.JAVA_INT_UNALIGNED),
                            ao.asSlice(0, ((long) count + 1) << 2).toArray(ValueLayout.JAVA_INT_UNALIGNED),
                            "offsets n=" + n + " kind=" + kind);
                    assertArrayEquals(
                            ed.asSlice(0, expBytes).toArray(ValueLayout.JAVA_BYTE),
                            ad.asSlice(0, expBytes).toArray(ValueLayout.JAVA_BYTE),
                            "bytes n=" + n + " kind=" + kind);
                    TestData.assertBitmapEquals(ev, av, count, "validity n=" + n + " kind=" + kind);
                }
            }
        }
    }

    @Test
    void bitGathersMatchTheReferenceOnEverySegmentKind() {
        Random rnd = new Random(7);
        for (int n : TestData.LENGTHS) {
            for (int kind = 0; kind < KINDS; kind++) {
                try (Arena arena = Arena.ofConfined()) {
                    boolean[] values = new boolean[n];
                    for (int i = 0; i < n; i++) {
                        values[i] = rnd.nextBoolean();
                    }
                    VectorBuffers nat = ArrowLayout.ofBooleans(arena, values, TestData.nulls(rnd, n, 0.3));
                    VectorBuffers in = kind == 1 || kind == 3
                            ? SegmentVectorBuffers.fixedWidth(VecType.BOOL, n, heap(nat.validity()),
                                    kind == 1 ? heap(nat.data()) : nat.data())
                            : nat;
                    int count = rnd.nextInt(2 * n + 2);
                    int[] idx = indices(rnd, count + 1, n);
                    MemorySegment exp = arena.allocate(Math.max(1, Bitmap.bytesFor(count)));
                    MemorySegment ev = arena.allocate(Math.max(1, Bitmap.bytesFor(count)));
                    ScalarReference.gatherFixed(nat, idx, 1, 1 + count, exp,
                            ev);
                    MemorySegment act = arena.allocate(Math.max(1, Bitmap.bytesFor(count)));
                    MemorySegment av = arena.allocate(Math.max(1, Bitmap.bytesFor(count)));
                    if (kind == 2) {
                        act = heap(act);
                        av = heap(av);
                    }
                    GatherKernels.gatherFixed(in, idx, 1, 1 + count, act,
                            av);
                    TestData.assertBitmapEquals(ev, av, count, "validity n=" + n + " kind=" + kind);
                    for (int o = 0; o < count; o++) {
                        if (Bitmap.isSet(ev, o)) {
                            assertEquals(Bitmap.isSet(exp, o), Bitmap.isSet(act, o), "@" + o + " n=" + n + " kind=" + kind);
                        }
                    }
                }
            }
        }
    }

    @Test
    void copyNativeMatchesCopyAtEveryShortLength() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(128);
            for (int b = 0; b < 128; b++) {
                src.set(ValueLayout.JAVA_BYTE, b, (byte) (b * 31 + 7));
            }
            for (int len = 0; len <= 40; len++) {
                for (int srcPos = 0; srcPos < 5; srcPos++) {
                    MemorySegment a = arena.allocate(64);
                    MemorySegment e = arena.allocate(64);
                    ByteCopy.copy(src, srcPos, e, 3, len);
                    ByteCopy.copyNative(src, srcPos, a, 3, len);
                    assertArrayEquals(e.toArray(ValueLayout.JAVA_BYTE), a.toArray(ValueLayout.JAVA_BYTE), "len=" + len + " srcPos=" + srcPos);
                }
            }
        }
    }
}
