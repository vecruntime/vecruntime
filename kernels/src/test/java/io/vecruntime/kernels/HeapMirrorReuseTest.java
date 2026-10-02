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
import java.util.Random;

import io.vecruntime.kernels.reference.ScalarReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HeapMirror#reuse} (#565): a mirror built into the previous batch's
 * arrays gathers exactly what {@link GatherKernels#gatherFixed} gathers from the
 * segments, over a stream of batches that grows, shrinks, switches type (INT32,
 * INT64, DECIMAL128, FLOAT64) and gains and loses nulls -- so stale words or
 * values left in the reused arrays past the batch's rows must never show.
 */
class HeapMirrorReuseTest {

    @Test
    void reusedMirrorsGatherLikeTheSegments() {
        runStream(false);
    }

    /**
     * The same stream with every batch's mirror handed to the thread's
     * {@link HeapMirror.Stash} and the next one built with no {@code prev}, as
     * across two map tasks' writers: the stashed arrays come back dirty and
     * longer than the batch.
     */
    @Test
    void stashedArraysGatherLikeTheSegments() {
        runStream(true);
    }

    /** The stash holds no more than its budget, and hands an array out only once. */
    @Test
    void stashIsBoundedAndHandsOutOnce() throws Exception {
        java.util.concurrent.atomic.AtomicReference<Throwable> err = new java.util.concurrent.atomic.AtomicReference<>();
        Thread worker = new Thread(
                () -> {
                    HeapMirror.Stash s = HeapMirror.Stash.local();
                    int big = (int) (HeapMirror.Stash.MAX_BYTES / 8 / 3);
                    HeapMirror[] ms = new HeapMirror[5];
                    try (Arena arena = Arena.ofConfined()) {
                        Random rnd = new Random(1);
                        for (int i = 0; i < ms.length; i++) {
                            ms[i] = HeapMirror.reuse(TestData.longs(arena, rnd, big, null), null);
                        }
                    }
                    s.give(ms);
                    assertTrue(s.bytes() <= HeapMirror.Stash.MAX_BYTES, "bytes " + s.bytes());
                    long[] a = s.takeLongs(big);
                    long[] b = s.takeLongs(big);
                    org.junit.jupiter.api.Assertions.assertNotSame(a, b, "same array handed out twice");
                    assertTrue(a.length >= big && b.length >= big);
                });
        worker.setUncaughtExceptionHandler((th, e) -> err.set(e));
        worker.start();
        worker.join();
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    private static void runStream(boolean viaStash) {
        Random rnd = new Random(565);
        int[] sizes = {4097, 10, 1000, 64, 65, 5000,
                3, 4097};
        HeapMirror prev = null;
        HeapMirror.GatherScratch scratch = new HeapMirror.GatherScratch();
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < sizes.length * 3; b++) {
                int n = sizes[b % sizes.length];
                boolean[] nulls = b % 2 == 0 ? TestData.nulls(rnd, n, 0.2) : null;
                VectorBuffers in = switch (b % 4) {
                    case 0 -> TestData.ints(arena, rnd, n, nulls);
                    case 1 -> TestData.longs(arena, rnd, n, nulls);
                    case 2 -> TestData.decimal128s(arena, rnd, n, nulls);
                    default -> TestData.doubles(arena, rnd, n, nulls);
                };
                assertTrue(HeapMirror.mirrorsForGather(in),
                        in.type().toString());
                HeapMirror m;
                if (viaStash) {
                    if (prev != null) {
                        HeapMirror.Stash.local().give(new HeapMirror[] {prev});
                    }
                    m = HeapMirror.reuse(in, null);
                } else {
                    m = HeapMirror.reuse(in, prev);
                }
                prev = m;
                int count = rnd.nextInt(2 * n + 1);
                int[] idx = new int[count];
                for (int o = 0; o < count; o++) {
                    idx[o] = rnd.nextInt(n);
                }
                int from = count / 3;
                int to = count;
                int rows = to - from;
                MemorySegment exp = ArrowLayout.allocateData(arena, in.type(), Math.max(rows, 1));
                MemorySegment ev = ArrowLayout.allocateBitmap(arena, Math.max(rows, 1));
                ScalarReference.gatherFixed(in, idx, from, to, exp, ev);
                MemorySegment act = ArrowLayout.allocateData(arena, in.type(), Math.max(rows, 1));
                MemorySegment av = ArrowLayout.allocateBitmap(arena, Math.max(rows, 1));
                m.gather(idx, from, to, act, av, scratch);
                String what = "batch "
                        + b
                        + " "
                        + in.type()
                        + " n="
                        + n;
                TestData.assertBitmapEquals(ev, av, rows, what + " validity");
                int width = in.type().byteWidth();
                for (int o = 0; o < rows; o++) {
                    if (!Bitmap.isSet(ev, o)) {
                        continue;
                    }
                    if (width == 4) {
                        assertEquals(exp.get(VectorBuffers.LE_INT, (long) o << 2),
                                act.get(VectorBuffers.LE_INT, (long) o << 2), what + " @" + o);
                    } else if (width == 16) {
                        assertEquals(exp.get(VectorBuffers.LE_LONG, (long) o << 4),
                                act.get(VectorBuffers.LE_LONG, (long) o << 4), what + " lo @" + o);
                        assertEquals(exp.get(VectorBuffers.LE_LONG, ((long) o << 4) + 8),
                                act.get(VectorBuffers.LE_LONG, ((long) o << 4) + 8), what + " hi @" + o);
                    } else {
                        assertEquals(exp.get(VectorBuffers.LE_LONG, (long) o << 3),
                                act.get(VectorBuffers.LE_LONG, (long) o << 3), what + " @" + o);
                    }
                }
            }
        }
    }
}
