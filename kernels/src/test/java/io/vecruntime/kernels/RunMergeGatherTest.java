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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link RunMerge}'s gathers -- over the runs' segments and over their {@link
 * RunMirrors} (#565) -- against per-row reads through the accessors: every
 * fixed-width type and plain UTF8, with nulls, runs with and without validity,
 * native and heap runs mixed in one call, {@code runOf} patterns from one run
 * per row to long blocks, output counts around word boundaries, and mirrors
 * rebound after one run's column is replaced.
 */
class RunMergeGatherTest {

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    private static VectorBuffers onHeap(VectorBuffers c) {
        if (c.type() == VecType.UTF8) {
            return SegmentVectorBuffers.utf8(c.length(), heap(c.validity()), heap(c.offsets()),
                    heap(c.data()));
        }
        return SegmentVectorBuffers.fixedWidth(c.type(), c.length(), heap(c.validity()),
                heap(c.data()));
    }

    private static VectorBuffers column(Arena arena, Random rnd, VecType t,
            int n, boolean nulls) {
        boolean[] isNull = nulls ? TestData.nulls(rnd, n, 0.25) : null;
        return switch (t) {
            case INT32 -> TestData.ints(arena, rnd, n, isNull);
            case INT64 -> TestData.longs(arena, rnd, n, isNull);
            case FLOAT64 -> TestData.doubles(arena, rnd, n, isNull);
            case DECIMAL128 -> TestData.decimal128s(arena, rnd, n, isNull);
            case BOOL -> {
                boolean[] v = new boolean[n];
                for (int i = 0; i < n; i++) {
                    v[i] = rnd.nextBoolean();
                }
                yield ArrowLayout.ofBooleans(arena, v, isNull);
            }
            case UTF8 -> {
                String[] v = new String[n];
                for (int i = 0; i < n; i++) {
                    v[i] = isNull != null && isNull[i]
                            ? null
                            : "v" + rnd.nextInt(1000) + "x".repeat(rnd.nextInt(25));
                }
                yield ArrowLayout.ofStrings(arena, v);
            }
        };
    }

    private static Object value(VectorBuffers c, int i) {
        return switch (c.type()) {
            case INT32 -> c.getInt(i);
            case INT64 -> c.getLong(i);
            case FLOAT64 -> Double.doubleToRawLongBits(c.getDouble(i));
            case BOOL -> c.getBoolean(i);
            case DECIMAL128 -> c.getDecimal128(i);
            case UTF8 -> c.getString(i);
        };
    }

    private static VectorBuffers gatherWithMirrors(
            Arena arena,
            VecType t,
            VectorBuffers[] sources,
            RunMirrors mirrors,
            int[] runOf,
            int[] idx,
            int count) {
        MemorySegment validity = ArrowLayout.allocateBitmap(arena, count);
        if (t == VecType.UTF8) {
            long bytes = RunMerge.gatherUtf8Bytes(mirrors, runOf, idx, count);
            MemorySegment offsets = ArrowLayout.allocateOffsets(arena, count);
            MemorySegment data = ArrowLayout.allocateBytes(arena, Math.max(1, bytes));
            RunMerge.gatherUtf8(sources, mirrors, runOf, idx, count,
                    new int[count + 1], offsets, data, validity);
            return SegmentVectorBuffers.utf8(count, validity, offsets, data);
        }
        MemorySegment data = t == VecType.BOOL ? ArrowLayout.allocateBitmap(arena, count) : ArrowLayout.allocateData(arena, t, count);
        RunMerge.gatherFixed(sources, mirrors, runOf, idx, count, data,
                validity);
        return SegmentVectorBuffers.fixedWidth(t, count, validity, data);
    }

    @Test
    void gathersMatchPerRowReadsForEveryTypeAndStretchShape() {
        Random rnd = new Random(565);
        VecType[] types = {VecType.INT32, VecType.INT64, VecType.FLOAT64, VecType.BOOL, VecType.DECIMAL128, VecType.UTF8};
        int[] counts = {1, 63, 64, 65, 200, 1000};
        try (Arena arena = Arena.ofConfined()) {
            for (VecType t : types) {
                for (int count : counts) {
                    for (int block : new int[] {1, 3, 64, 1000}) {
                        int runs = 4;
                        VectorBuffers[] sources = new VectorBuffers[runs];
                        for (int r = 0; r < runs; r++) {
                            VectorBuffers c = column(arena, rnd, t, 50 + rnd.nextInt(300),
                                    r != 1);
                            sources[r] = r == 2 ? onHeap(c) : c;
                        }
                        int[] runOf = new int[count];
                        int[] idx = new int[count];
                        int r = rnd.nextInt(runs);
                        for (int o = 0; o < count; o++) {
                            if (o % block == 0) {
                                r = rnd.nextInt(runs);
                            }
                            runOf[o] = r;
                            idx[o] = rnd.nextInt(sources[r].length());
                        }
                        String what = t + " count=" + count + " block=" + block;
                        VectorBuffers out;
                        MemorySegment validity = ArrowLayout.allocateBitmap(arena, count);
                        if (t == VecType.UTF8) {
                            long bytes = RunMerge.gatherUtf8Bytes(sources, runOf, idx, count);
                            long expectedBytes = 0;
                            for (int o = 0; o < count; o++) {
                                VectorBuffers s = sources[runOf[o]];
                                if (!s.isNull(idx[o])) {
                                    expectedBytes += s.getUtf8Bytes(idx[o]).length;
                                }
                            }
                            assertEquals(expectedBytes, bytes, what + " bytes");
                            MemorySegment offsets = ArrowLayout.allocateOffsets(arena, count);
                            MemorySegment data = ArrowLayout.allocateBytes(arena, Math.max(1, bytes));
                            RunMerge.gatherUtf8(sources, runOf, idx, count, offsets, data,
                                    validity);
                            out = SegmentVectorBuffers.utf8(count, validity, offsets, data);
                        } else {
                            MemorySegment data = t == VecType.BOOL ? ArrowLayout.allocateBitmap(arena, count) : ArrowLayout.allocateData(arena, t, count);
                            RunMerge.gatherFixed(sources, runOf, idx, count, data, validity);
                            out = SegmentVectorBuffers.fixedWidth(t, count, validity, data);
                        }
                        for (int o = 0; o < count; o++) {
                            VectorBuffers s = sources[runOf[o]];
                            boolean isNull = s.isNull(idx[o]);
                            assertEquals(isNull, out.isNull(o), what + " null @" + o);
                            if (!isNull) {
                                assertEquals(value(s, idx[o]), value(out, o), what + " @" + o);
                            }
                        }
                        // The same gather over heap mirrors, bound twice: once fresh, once after one run's
                        // column object is replaced (a spilled run's refill), which must be mirrored again.
                        RunMirrors mirrors = new RunMirrors();
                        for (int pass = 0; pass < 2; pass++) {
                            if (pass == 1) {
                                VectorBuffers c = column(arena, rnd, t, sources[3].length(), true);
                                sources[3] = c;
                            }
                            mirrors.bind(sources);
                            VectorBuffers m = gatherWithMirrors(arena, t, sources, mirrors, runOf, idx,
                                    count);
                            for (int o = 0; o < count; o++) {
                                VectorBuffers s = sources[runOf[o]];
                                boolean isNull = s.isNull(idx[o]);
                                assertEquals(isNull, m.isNull(o), what + " mirrors pass " + pass + " null @" + o);
                                if (!isNull) {
                                    assertEquals(value(s, idx[o]), value(m, o), what + " mirrors pass " + pass + " @" + o);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
