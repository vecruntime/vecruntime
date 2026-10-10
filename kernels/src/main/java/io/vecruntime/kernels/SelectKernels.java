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

/**
 * Row selection between several same-typed columns, the kernel behind {@code
 * CASE WHEN}, {@code IF} and {@code COALESCE}: for each row the first branch
 * whose {@code wins} bit is set supplies the value, else the {@code otherwise}
 * column does, else the row is null. Branch masks are expected to be disjoint
 * (the caller carries "already decided" forward so a later branch only wins
 * where no earlier one did); a branch column may be {@code null} to mean a
 * {@code NULL} literal.
 *
 * <p>The blend is a scalar pass over the rows: the decision per row is a
 * handful of bit tests and one copy, and unlike the compare and arithmetic
 * kernels there is no per-lane arithmetic for SIMD to speed up. A {@code
 * VectorMask.blend} formulation for the fixed-width types is the obvious next
 * step if a profile ever shows this loop.
 */
public final class SelectKernels {

    private SelectKernels() {}

    /**
     * Selects per row among {@code branches} (a column and its winning-rows
     * bitmap each) and {@code otherwise} (may be {@code null}: no ELSE),
     * producing a new column of {@code type} in {@code arena}. Rows outside
     * {@code active} (may be {@code null}: all rows) are produced as null,
     * since nothing downstream reads them.
     */
    public static SegmentVectorBuffers select(
            VecType type,
            int n,
            MemorySegment[] wins,
            VectorBuffers[] branches,
            VectorBuffers otherwise,
            MemorySegment active,
            Arena arena) {
        if (wins.length != branches.length) {
            throw new IllegalArgumentException("one mask per branch");
        }
        MemorySegment validity = ArrowLayout.allocateBitmap(arena, n);
        if (type == VecType.UTF8) {
            return selectUtf8(n, wins, branches, otherwise, active, arena,
                    validity);
        }
        MemorySegment data = type == VecType.BOOL ? ArrowLayout.allocateBitmap(arena, n) : ArrowLayout.allocateData(arena, type, n);
        // A word of rows at a time (#541): each branch takes the still-undecided rows its mask wins, the
        // ELSE what is left; validity and BOOL values are whole words, fixed-width values a bulk copy when
        // one source takes all 64 rows and a per-row copy of just the taken rows otherwise. Row by row this
        // paid a checked bitmap read per branch, per row, before touching the value.
        int width = type == VecType.BOOL ? 0 : type.byteWidth();
        for (int w = 0, words = Bitmap.wordsFor(n);
             w < words;
             w++) {
            int base = w << 6;
            long rows = Bitmap.lowBits(Math.min(64, n - base));
            long undecided = active == null ? rows : Bitmap.wordAt(active, w, n);
            long valid = 0L;
            long bools = 0L;
            for (int k = 0;
                 k <= wins.length && undecided != 0L;
                 k++) {
                VectorBuffers src;
                long take;
                if (k < wins.length) {
                    take = Bitmap.wordAt(wins[k], w, n) & undecided;
                    src = branches[k];
                } else {
                    take = undecided;
                    src = otherwise;
                }
                if (take == 0L) {
                    continue;
                }
                undecided &= ~take;
                // A winning branch whose value is null makes the row null: it does not fall through.
                take &= validWord(src, w, n);
                if (take == 0L) {
                    continue;
                }
                valid |= take;
                if (width == 0) {
                    bools |= take & Bitmap.wordAt(src.data(), w, n);
                } else if (take == rows && rows == -1L) {
                    MemorySegment.copy(src.data(), (long) base * width, data,
                            (long) base * width, 64L * width);
                } else {
                    copyTaken(type, src, data, base, take);
                }
            }
            Bitmap.setWord(validity, w, n, valid);
            if (width == 0) {
                Bitmap.setWord(data, w, n, bools);
            }
        }
        return SegmentVectorBuffers.fixedWidth(type, n, validity, data);
    }

    /**
     * The rows of word {@code w} that hold a value: none for a missing source,
     * all without a validity.
     */
    private static long validWord(VectorBuffers src, int w, int n) {
        if (src == null) {
            return 0L;
        }
        MemorySegment v = src.validity();
        return v == null ? -1L : Bitmap.wordAt(v, w, n);
    }

    /**
     * Copies the values of the rows set in {@code take} (row {@code base +
     * bit}) from {@code src} to {@code data}.
     */
    private static void copyTaken(VecType type, VectorBuffers src, MemorySegment data,
            int base, long take) {
        MemorySegment in = src.data();
        while (take != 0L) {
            int i = base + Long.numberOfTrailingZeros(take);
            take &= take - 1;
            switch (type) {
                case INT32 -> data.setAtIndex(VectorBuffers.LE_INT, i, in.getAtIndex(VectorBuffers.LE_INT, i));
                case INT64 -> data.setAtIndex(VectorBuffers.LE_LONG, i, in.getAtIndex(VectorBuffers.LE_LONG, i));
                case FLOAT64 -> data.setAtIndex(VectorBuffers.LE_DOUBLE, i, in.getAtIndex(VectorBuffers.LE_DOUBLE, i));
                case DECIMAL128 -> Decimal128.copy(in, i, data, i); // both limbs (#326)
                default -> throw new IllegalArgumentException("unsupported type " + type);
            }
        }
    }

    /**
     * The column row {@code i} takes its value from, or {@code null} for a null
     * result.
     */
    private static VectorBuffers source(int i, MemorySegment[] wins, VectorBuffers[] branches,
            VectorBuffers otherwise, MemorySegment active) {
        if (active != null && !Bitmap.isSet(active, i)) {
            return null;
        }
        for (int k = 0; k < wins.length; k++) {
            if (Bitmap.isSet(wins[k], i)) {
                return branches[k];
            }
        }
        return otherwise;
    }

    private static SegmentVectorBuffers selectUtf8(
            int n,
            MemorySegment[] wins,
            VectorBuffers[] branches,
            VectorBuffers otherwise,
            MemorySegment active,
            Arena arena,
            MemorySegment validity) {
        // Two passes over the same decisions: sizes first, then one contiguous copy.
        long total = 0;
        for (int i = 0; i < n; i++) {
            VectorBuffers src = source(i, wins, branches, otherwise, active);
            if (src != null && !src.isNull(i)) {
                total += utf8Length(src, i);
            }
        }
        MemorySegment offsets = ArrowLayout.allocateOffsets(arena, n);
        MemorySegment data = ArrowLayout.allocateBytes(arena, total);
        int pos = 0;
        offsets.setAtIndex(VectorBuffers.LE_INT, 0, 0);
        for (int i = 0; i < n; i++) {
            VectorBuffers src = source(i, wins, branches, otherwise, active);
            if (src != null && !src.isNull(i)) {
                Bitmap.set(validity, i);
                pos += copyUtf8(src, i, data, pos);
            }
            offsets.setAtIndex(VectorBuffers.LE_INT, i + 1, pos);
        }
        return SegmentVectorBuffers.utf8(n, validity, offsets, data);
    }

    /**
     * Byte length of row {@code i}, through the dictionary when the column is
     * encoded.
     */
    private static int utf8Length(VectorBuffers src, int i) {
        VectorBuffers plain = src.isDictionaryEncoded() ? src.dictionary() : src;
        int row = src.isDictionaryEncoded() ? src.data().getAtIndex(VectorBuffers.LE_INT, i) : i;
        MemorySegment off = plain.offsets();
        return off.getAtIndex(VectorBuffers.LE_INT, row + 1) - off.getAtIndex(VectorBuffers.LE_INT, row);
    }

    private static int copyUtf8(VectorBuffers src, int i, MemorySegment out,
            int pos) {
        VectorBuffers plain = src.isDictionaryEncoded() ? src.dictionary() : src;
        int row = src.isDictionaryEncoded() ? src.data().getAtIndex(VectorBuffers.LE_INT, i) : i;
        MemorySegment off = plain.offsets();
        int start = off.getAtIndex(VectorBuffers.LE_INT, row);
        int len = off.getAtIndex(VectorBuffers.LE_INT, row + 1) - start;
        MemorySegment.copy(plain.data(), ValueLayout.JAVA_BYTE, start, out, ValueLayout.JAVA_BYTE,
                pos, len);
        return len;
    }
}
