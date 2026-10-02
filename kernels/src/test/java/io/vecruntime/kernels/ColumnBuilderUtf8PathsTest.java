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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link ColumnBuilder#append} of UTF8 batches (#565: the selected and the
 * dictionary paths read indices, offsets, validity and selection from heap
 * copies) against the expected strings: plain batches with and without a
 * selection, dictionary-encoded batches over small dictionaries and over one
 * more than twice the rows (read per row), nulls (a null dictionary row's index
 * points outside the dictionary), sliced columns with absolute offsets, heap
 * segments, and many appends so the data buffer and the scratch grow.
 */
class ColumnBuilderUtf8PathsTest {

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
        return SegmentVectorBuffers.utf8(c.length(), heap(c.validity()), heap(c.offsets()),
                heap(c.data()));
    }

    private static String word(Random rnd) {
        int len = rnd.nextInt(0, 30);
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < len; k++) {
            sb.append(rnd.nextInt(7) == 0 ? 'ñ' : (char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    @Test
    void everyAppendPathKeepsTheValues() {
        Random rnd = new Random(565);
        try (Arena arena = Arena.ofConfined()) {
            ColumnBuilder b = new ColumnBuilder(arena, VecType.UTF8, 4);
            List<String> expected = new ArrayList<>();
            int[] sizes = {1, 64, 65, 1000, 3, 4097,
                    129, 7};
            for (int round = 0; round < sizes.length * 6; round++) {
                int n = sizes[round % sizes.length];
                int shape = round % 6;
                boolean dict = shape >= 2;
                boolean withSelection = shape % 2 == 1;
                String[] values = new String[n];
                VectorBuffers col;
                if (!dict) {
                    // Plain, as a slice when long enough so its offsets do not start at zero.
                    String[] wide = new String[n + 64];
                    for (int i = 0; i < wide.length; i++) {
                        wide[i] = rnd.nextInt(6) == 0 ? null : word(rnd);
                    }
                    VectorBuffers whole = ArrowLayout.ofStrings(arena, wide);
                    col = whole.slice(64, 64 + n);
                    System.arraycopy(wide, 64, values, 0, n);
                } else {
                    int entries = shape >= 4 ? ColumnBuilder.DICTIONARY_COPY_FACTOR * n + 1 : Math.max(1, n / 3);
                    String[] entry = new String[entries];
                    for (int e = 0; e < entries; e++) {
                        entry[e] = word(rnd);
                    }
                    int[] ids = new int[n];
                    boolean[] nulls = new boolean[n];
                    for (int i = 0; i < n; i++) {
                        nulls[i] = rnd.nextInt(5) == 0;
                        ids[i] = nulls[i] ? 1_000_000_007 : rnd.nextInt(entries);
                        values[i] = nulls[i] ? null : entry[ids[i]];
                    }
                    VectorBuffers idx = ArrowLayout.ofInts(arena, ids, nulls);
                    col = SegmentVectorBuffers.dictionaryUtf8(n, idx.validity(), idx.data(),
                            ArrowLayout.ofStrings(arena, entry));
                }
                if (round % 4 == 3) {
                    col = onHeap(col);
                }
                MemorySegment selection = null;
                int count = n;
                if (withSelection) {
                    selection = ArrowLayout.allocateBitmap(arena, n);
                    count = 0;
                    for (int i = 0; i < n; i++) {
                        if (rnd.nextInt(3) != 0) {
                            Bitmap.set(selection, i);
                            count++;
                        }
                    }
                }
                b.append(col, selection, count);
                for (int i = 0; i < n; i++) {
                    if (selection == null || Bitmap.isSet(selection, i)) {
                        expected.add(values[i]);
                    }
                }
                VectorBuffers v = b.view();
                assertEquals(expected.size(), v.length(), "rows after round " + round);
                for (int i = 0; i < expected.size(); i++) {
                    String e = expected.get(i);
                    if (e == null) {
                        assertEquals(true, v.isNull(i), "null @" + i + " round " + round);
                    } else {
                        assertEquals(false, v.isNull(i), "valid @" + i + " round " + round);
                        assertEquals(e, v.getString(i), "@" + i + " round " + round);
                    }
                }
            }
        }
    }

    /**
     * An all-null batch over a dictionary with no entries and an empty offsets
     * buffer (what an all-null dictionary column arrives as in a sort): nothing
     * is read from the dictionary. A first version copied its offsets
     * regardless and failed out of bounds on TPC-DS q67 at SF10.
     */
    @Test
    void allNullBatchOverAnEmptyDictionary() {
        try (Arena arena = Arena.ofConfined()) {
            ColumnBuilder b = new ColumnBuilder(arena, VecType.UTF8, 4);
            b.append(ArrowLayout.ofStrings(arena, new String[] {"x", null}), null, 2);
            for (boolean withSelection : new boolean[] {false, true}) {
                int n = 70;
                int[] ids = new int[n];
                boolean[] nulls = new boolean[n];
                java.util.Arrays.fill(nulls, true);
                VectorBuffers idx = ArrowLayout.ofInts(arena, ids, nulls);
                VectorBuffers empty = SegmentVectorBuffers.utf8(0, null, MemorySegment.NULL, MemorySegment.NULL);
                VectorBuffers col = SegmentVectorBuffers.dictionaryUtf8(n, idx.validity(), idx.data(), empty);
                MemorySegment selection = null;
                int count = n;
                if (withSelection) {
                    selection = ArrowLayout.allocateBitmap(arena, n);
                    count = 0;
                    for (int i = 0; i < n; i += 3) {
                        Bitmap.set(selection, i);
                        count++;
                    }
                }
                int before = b.view().length();
                b.append(col, selection, count);
                VectorBuffers v = b.view();
                assertEquals(before + count, v.length());
                for (int i = before; i < v.length(); i++) {
                    assertEquals(true, v.isNull(i), "null @" + i);
                }
            }
            assertEquals("x", b.view()
                               .getString(0));
        }
    }
}
