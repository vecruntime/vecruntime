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

import java.lang.foreign.MemorySegment;

/**
 * The bytes {@code UnsafeRow} gives the string values of a UTF8 column: each
 * non-null value's UTF-8 length rounded up to a word, dictionary-encoded
 * columns resolved through their dictionary.
 *
 * <p>The shuffle writer sums this per written batch for AQE's map-size scaling.
 * Read row by row through {@link MemorySegment} accessors -- a validity bit, an
 * index and two offsets per row -- it was 14 % of the FFM liveness and bounds
 * checks on TPC-DS q67 at 1 TB (#565): the accessors' receiver profile mixes
 * native and heap segments, so the reads stay calls and each pays its check.
 * Here the offsets, the indices and the validity words are copied into reused
 * Java arrays once per call (bulk moves) and the per-row loop reads only arrays.
 */
public final class Utf8Sizes {

    /**
     * A dictionary larger than this many times the rows is not copied whole: its
     * offsets are read per row instead, so a small batch over a large dictionary
     * costs no more than it did.
     */
    static final int DICTIONARY_COPY_FACTOR = 2;

    private Utf8Sizes() {}

    /** Reusable arrays for {@link #paddedBytes}: one per writer, grown on demand. */
    public static final class Scratch {
        private int[] offsets = new int[0];
        private int[] ids = new int[0];
        private int[] padded = new int[0];
        private long[] words = new long[0];

        int[] offsets(int n) {
            if (offsets.length < n) {
                offsets = new int[Math.max(n, offsets.length * 2)];
            }
            return offsets;
        }

        int[] ids(int n) {
            if (ids.length < n) {
                ids = new int[Math.max(n, ids.length * 2)];
            }
            return ids;
        }

        int[] padded(int n) {
            if (padded.length < n) {
                padded = new int[Math.max(n, padded.length * 2)];
            }
            return padded;
        }

        long[] words(int n) {
            if (words.length < n) {
                words = new long[Math.max(n, words.length * 2)];
            }
            return words;
        }
    }

    /**
     * Sum over the first {@code n} rows of {@code col} of {@code (len + 7) & ~7}
     * for every non-null row, {@code len} its UTF-8 byte length; 0 for a column
     * that is not UTF8.
     */
    public static long paddedBytes(VectorBuffers col, int n, Scratch scratch) {
        if (n <= 0 || col.type() != VecType.UTF8) {
            return 0L;
        }
        long[] valid = null;
        MemorySegment validity = col.validity();
        if (validity != null) {
            int words = Bitmap.wordsFor(n);
            valid = scratch.words(words);
            for (int w = 0; w < words; w++) {
                valid[w] = Bitmap.wordAt(validity, w, n);
            }
        }
        VectorBuffers dict = col.dictionary();
        if (dict == null) {
            int[] off = scratch.offsets(n + 1);
            MemorySegment.copy(col.offsets(), VectorBuffers.LE_INT, 0L, off, 0,
                    n + 1);
            return plain(off, valid, n);
        }
        int[] ids = scratch.ids(n);
        MemorySegment.copy(col.data(), VectorBuffers.LE_INT, 0L, ids, 0,
                n);
        int entries = dict.length();
        if (entries > DICTIONARY_COPY_FACTOR * n) {
            return dictionaryPerRow(ids, dict.offsets(), valid, n);
        }
        int[] off = scratch.offsets(entries + 1);
        MemorySegment.copy(dict.offsets(), VectorBuffers.LE_INT, 0L, off, 0,
                entries + 1);
        int[] padded = scratch.padded(entries);
        for (int e = 0; e < entries; e++) {
            padded[e] = (off[e + 1] - off[e] + 7) & ~7;
        }
        long total = 0L;
        if (valid == null) {
            for (int i = 0; i < n; i++) {
                total += padded[ids[i]];
            }
        } else {
            // A null row's index is not guaranteed to be in range: read it only under its bit.
            for (int i = 0; i < n; i++) {
                if (((valid[i >>> 6] >>> i) & 1L) != 0L) {
                    total += padded[ids[i]];
                }
            }
        }
        return total;
    }

    private static long plain(int[] off, long[] valid, int n) {
        long total = 0L;
        if (valid == null) {
            for (int i = 0; i < n; i++) {
                total += (off[i + 1] - off[i] + 7) & ~7;
            }
        } else {
            for (int i = 0; i < n; i++) {
                total += ((valid[i >>> 6] >>> i) & 1L) * ((off[i + 1] - off[i] + 7) & ~7);
            }
        }
        return total;
    }

    private static long dictionaryPerRow(int[] ids, MemorySegment off, long[] valid,
            int n) {
        long total = 0L;
        for (int i = 0; i < n; i++) {
            if (valid == null || ((valid[i >>> 6] >>> i) & 1L) != 0L) {
                int e = ids[i];
                int len = off.get(VectorBuffers.LE_INT, (long) (e + 1) << 2) - off.get(VectorBuffers.LE_INT, (long) e << 2);
                total += (len + 7) & ~7;
            }
        }
        return total;
    }
}
