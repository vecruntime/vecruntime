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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link GroupKeyTable#assign} and {@link GroupKeyTable#lookup} against a
 * {@code HashMap} of the rows' key tuples (#565: the per-row key reads come from
 * heap mirrors of the batch's columns). Batches of varying sizes, so the mirror
 * arrays are reused, grown and read past a shorter batch's rows; INT32, INT64,
 * FLOAT64 and UTF8 keys (plain and dictionary encoded) with nulls; every column
 * native, every column on the heap, or alternating; dictionary-mode and
 * record-mode string keys.
 */
class GroupKeyTableMirrorTest {

    private static MemorySegment heap(MemorySegment s) {
        return s == null ? null : MemorySegment.ofArray(s.toArray(ValueLayout.JAVA_BYTE));
    }

    private static VectorBuffers onHeap(VectorBuffers c) {
        if (c.isDictionaryEncoded()) {
            VectorBuffers d = c.dictionary();
            return SegmentVectorBuffers.dictionaryUtf8(
                    c.length(),
                    heap(c.validity()),
                    heap(c.data()),
                    SegmentVectorBuffers.utf8(d.length(), heap(d.validity()), heap(d.offsets()),
                            heap(d.data())));
        }
        if (c.type() == VecType.UTF8) {
            return SegmentVectorBuffers.utf8(c.length(), heap(c.validity()), heap(c.offsets()),
                    heap(c.data()));
        }
        return SegmentVectorBuffers.fixedWidth(c.type(), c.length(), heap(c.validity()),
                heap(c.data()));
    }

    private static final String[] WORDS = {"", "a", "bb", "ccc", "a key longer than sixteen bytes", "é",
            "x1", "x2"};

    /**
     * One batch: columns and, per row, the key tuple (null entries for null
     * keys).
     */
    private static VectorBuffers[] batch(Arena arena, Random rnd, int n,
            List<List<Object>> tuples) {
        int[] i32 = new int[n];
        long[] i64 = new long[n];
        double[] f64 = new double[n];
        String[] plain = new String[n];
        int[] dictIdx = new int[n];
        boolean[] n32 = new boolean[n], n64 = new boolean[n], nf = new boolean[n], nd = new boolean[n];
        String[] dictEntries = {"R", "A", "N", "a key longer than sixteen bytes"};
        for (int i = 0; i < n; i++) {
            i32[i] = rnd.nextInt(-3, 4);
            i64[i] = rnd.nextBoolean() ? rnd.nextInt(3) : (1L << 40) + rnd.nextInt(2);
            f64[i] = switch (rnd.nextInt(4)) {
                        case 0 -> -0.0;
                        case 1 -> 0.0;
                        case 2 -> Double.NaN;
                        default -> 1.5;
                    };
            plain[i] = rnd.nextInt(8) == 0 ? null : WORDS[rnd.nextInt(WORDS.length)];
            n32[i] = rnd.nextInt(7) == 0;
            n64[i] = rnd.nextInt(9) == 0;
            nf[i] = rnd.nextInt(11) == 0;
            nd[i] = rnd.nextInt(6) == 0;
            dictIdx[i] = nd[i] ? 999_999 : rnd.nextInt(dictEntries.length); // a null row's index is never read
            List<Object> t = new ArrayList<>();
            t.add(n32[i] ? null : i32[i]);
            t.add(n64[i] ? null : i64[i]);
            t.add(nf[i] ? null : Double.doubleToRawLongBits(f64[i]));
            t.add(plain[i]);
            t.add(nd[i] ? null : dictEntries[dictIdx[i]]);
            tuples.add(t);
        }
        VectorBuffers idx = ArrowLayout.ofInts(arena, dictIdx, nd);
        return new VectorBuffers[] {ArrowLayout.ofInts(arena, i32, n32), ArrowLayout.ofLongs(arena, i64, n64), ArrowLayout.ofDoubles(arena, f64, nf),
                ArrowLayout.ofStrings(arena, plain), SegmentVectorBuffers.dictionaryUtf8(n, idx.validity(), idx.data(),
                ArrowLayout.ofStrings(arena, dictEntries))};
    }

    @ParameterizedTest
    @CsvSource({"native, true", "heap, true", "mixed, true", "native, false", "heap, false", "mixed, false"})
    void groupsMatchTheReference(String segments, boolean dictionaryStrings) {
        VecType[] types = {VecType.INT32, VecType.INT64, VecType.FLOAT64, VecType.UTF8, VecType.UTF8};
        GroupKeyTable table = new GroupKeyTable(types, dictionaryStrings);
        Map<List<Object>, Integer> reference = new HashMap<>();
        Map<Integer, List<Object>> keyOfGroup = new HashMap<>();
        Random rnd = new Random(565 + segments.hashCode());
        int[] sizes = {1000, 3, 4097, 64, 65, 1000,
                7};
        try (Arena arena = Arena.ofConfined()) {
            for (int b = 0; b < sizes.length; b++) {
                int n = sizes[b];
                List<List<Object>> tuples = new ArrayList<>();
                VectorBuffers[] cols = batch(arena, rnd, n, tuples);
                for (int c = 0; c < cols.length; c++) {
                    boolean toHeap = segments.equals("heap") || (segments.equals("mixed") && (c + b) % 2 == 0);
                    if (toHeap) {
                        cols[c] = onHeap(cols[c]);
                    }
                }
                int[] ids = new int[n];
                int groups = table.assign(cols, n, ids);
                for (int i = 0; i < n; i++) {
                    List<Object> t = tuples.get(i);
                    Integer known = reference.get(t);
                    if (known == null) {
                        assertEquals(null, keyOfGroup.get(ids[i]), "row "
                                + i
                                + " of batch "
                                + b
                                + " "
                                + t
                                + " joined the group of "
                                + keyOfGroup.get(ids[i]));
                        reference.put(t, ids[i]);
                        keyOfGroup.put(ids[i], t);
                    } else {
                        assertEquals(known.intValue(), ids[i], "row " + i + " of batch " + b + " " + t);
                    }
                }
                assertEquals(reference.size(), groups, "groups after batch " + b);
                // Probing the same rows finds every one of them.
                int[] probe = new int[n];
                assertEquals(n, table.lookup(cols, n, probe, null), "lookup batch " + b);
                assertEquals(Arrays.toString(ids), Arrays.toString(probe), "lookup ids batch " + b);
            }
        }
    }
}
