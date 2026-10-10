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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * A plain UTF8 column copied into heap arrays -- offsets, bytes and validity --
 * for a caller that gathers from it many times: a hash join's build side,
 * gathered once per output batch of every probe batch (#565). The string
 * gather through the column's segments reads two offsets, a validity bit and
 * the bytes per output row, each a checked segment access ({@code ByteCopy}
 * under {@link GatherKernels#gatherUtf8} was the largest FFM check site left
 * locally once the fixed-width gathers had mirrors). Here the per-row work
 * reads and writes arrays only, and the gathered chunk goes to the Arrow
 * output in two bulk moves.
 *
 * <p>A column of more than {@link #MAX_BYTES} string bytes is not mirrored, so
 * a large build side does not double its heap footprint; it is gathered the
 * usual way.
 */
public final class Utf8Mirror {

    /**
     * The largest column, in string bytes, that is mirrored: a broadcast build
     * side is shared by the executor's tasks, but a shuffled one is per task,
     * so the cap bounds what a task can add to the heap per string column.
     */
    public static final long MAX_BYTES = 16L << 20;

    public final int length;

    /**
     * The column's offsets as they are ({@code length + 1}, absolute into its
     * data).
     */
    private final int[] offsets;

    /** The bytes {@code [offsets[0], offsets[length])} of the column's data. */
    private final byte[] data;

    /** {@code offsets[0]}: where {@link #data} starts in the column's data. */
    private final int base;

    /** Validity as 64-row words, or null when every row is valid. */
    final long[] validity;

    private Utf8Mirror(int length, int[] offsets, byte[] data,
                       long[] validity) {
        this.length = length;
        this.offsets = offsets;
        this.data = data;
        this.base = offsets[0];
        this.validity = validity;
    }

    /**
     * Whether {@code in} is a column this mirrors: plain UTF8 of at most {@link
     * #MAX_BYTES} bytes.
     */
    public static boolean mirrors(VectorBuffers in) {
        if (in.type() != VecType.UTF8 || in.isDictionaryEncoded()) {
            return false;
        }
        int n = in.length();
        MemorySegment off = in.offsets();
        long bytes = (long) off.get(VectorBuffers.LE_INT, (long) n << 2) - off.get(VectorBuffers.LE_INT, 0L);
        return bytes <= MAX_BYTES;
    }

    /**
     * Copies the column's offsets, bytes and (when it has nulls) validity: one
     * bulk move each.
     */
    public static Utf8Mirror of(VectorBuffers in) {
        int n = in.length();
        int[] offsets = new int[n + 1];
        MemorySegment.copy(in.offsets(), VectorBuffers.LE_INT, 0L, offsets, 0,
                n + 1);
        int bytes = offsets[n] - offsets[0];
        byte[] data = new byte[bytes];
        MemorySegment.copy(in.data(), ValueLayout.JAVA_BYTE, offsets[0], data, 0,
                bytes);
        long[] validity = null;
        if (in.hasNulls()) {
            int words = Bitmap.wordsFor(n);
            validity = new long[words];
            for (int w = 0; w < words; w++) {
                validity[w] = Bitmap.wordAt(in.validity(), w, n);
            }
        }
        return new Utf8Mirror(n, offsets, data, validity);
    }

    /** Whether row {@code i} is valid. */
    public boolean isValid(int i) {
        return validity == null || ((validity[i >>> 6] >>> (i & 63)) & 1L) != 0L;
    }

    /**
     * The column dictionary encoded (#603: a join's build-side string, emitted
     * as ids over its distinct values so a consumer such as an aggregate maps
     * the values once instead of the rows): {@code ids[i]} is row {@code i}'s
     * value id, -1 for a null row; the values are {@code dictionary}'s entries
     * 0 until {@code size()}.
     */
    public static final class Encoded {
        private final int[] ids;
        private final StringDictionary dictionary;

        Encoded(int[] ids, StringDictionary dictionary) {
            this.ids = ids;
            this.dictionary = dictionary;
        }

        public int[] ids() {
            return ids;
        }

        public StringDictionary dictionary() {
            return dictionary;
        }
    }

    /**
     * {@link Encoded} when the column has at most {@code maxDistinct} distinct
     * values, else null. One pass over the rows; stops at the first value past
     * the limit.
     */
    public Encoded encode(int maxDistinct) {
        StringDictionary dict = new StringDictionary();
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            if (!isValid(i)) {
                ids[i] = -1;
                continue;
            }
            int from = offsets[i] - base;
            int len = offsets[i + 1] - offsets[i];
            int id = dict.indexOf(StringDictionary.fingerprint(data, from, len),
                    len, data, from, true);
            if (dict.size() > maxDistinct) {
                return null;
            }
            ids[i] = id;
        }
        return new Encoded(ids, dict);
    }

    /** Whether the column has a null row. */
    public boolean hasNulls() {
        return validity != null;
    }

    /**
     * The string bytes of rows {@code idx[from..to)} (a negative index or a
     * null row counts 0).
     */
    public long bytes(int[] idx, int from, int to) {
        long total = 0;
        for (int o = from; o < to; o++) {
            int i = idx[o];
            if (i >= 0 && isValid(i)) {
                total += offsets[i + 1] - offsets[i];
            }
        }
        return total;
    }

    /**
     * Gathers rows {@code idx[from..to)} (a negative index pads a null, empty
     * row) into Arrow UTF8 buffers: {@code outOffsets} takes {@code to - from +
     * 1} offsets from zero, {@code outData} must hold {@link #bytes} of them,
     * and {@code outValidity} may be null when the caller knows every gathered
     * row is valid.
     */
    public void gather(
            int[] idx,
            int from,
            int to,
            MemorySegment outOffsets,
            MemorySegment outData,
            MemorySegment outValidity,
            Scratch scratch) {
        gather(idx, from, to, bytes(idx, from, to), outOffsets,
                outData, outValidity, scratch);
    }

    /**
     * As {@link #gather(int[], int, int, MemorySegment, MemorySegment,
     * MemorySegment, Scratch)}, with {@code bytes} the caller's {@link #bytes}
     * of the same rows, so the offsets are not summed a second time.
     */
    public void gather(
            int[] idx,
            int from,
            int to,
            long bytes,
            MemorySegment outOffsets,
            MemorySegment outData,
            MemorySegment outValidity,
            Scratch scratch) {
        int count = to - from;
        int[] outOff = scratch.ints(count + 1);
        byte[] out = scratch.bytes(Math.toIntExact(bytes));
        int pos = 0;
        for (int o = 0; o < count; o++) {
            int i = idx[from + o];
            outOff[o] = pos;
            if (i >= 0 && isValid(i)) {
                int start = offsets[i];
                int len = offsets[i + 1] - start;
                copy(data, start - base, out, pos, len);
                pos += len;
            }
        }
        outOff[count] = pos;
        MemorySegment.copy(outOff, 0, outOffsets, VectorBuffers.LE_INT, 0L,
                count + 1);
        if (pos > 0) {
            MemorySegment.copy(out, 0, outData, ValueLayout.JAVA_BYTE, 0L, pos);
        }
        if (outValidity != null) {
            int words = Bitmap.wordsFor(count);
            for (int w = 0; w < words; w++) {
                int limit = Math.min(64, count - (w << 6));
                long word = 0L;
                int at = from + (w << 6);
                for (int j = 0; j < limit; j++) {
                    int i = idx[at + j];
                    if (i >= 0 && isValid(i)) {
                        word |= 1L << j;
                    }
                }
                Bitmap.setWord(outValidity, w, count, word);
            }
        }
    }

    private static final VarHandle LONGS = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INTS = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    /**
     * {@code len} bytes from {@code src[from..]} to {@code dst[to..]}. Most
     * strings a join gathers are short, and {@code System.arraycopy} per row
     * costs about what the segment checks it replaced did (#565, local JFR);
     * up to 16 bytes the bytes move as two overlapping words, as
     * {@link ByteCopy} does on segments.
     */
    private static void copy(byte[] src, int from, byte[] dst,
            int to, int len) {
        if (len > 16) {
            System.arraycopy(src, from, dst, to, len);
        } else if (len >= 8) {
            long head = (long) LONGS.get(src, from);
            long tail = (long) LONGS.get(src, from + len - 8);
            LONGS.set(dst, to, head);
            LONGS.set(dst, to + len - 8, tail);
        } else if (len >= 4) {
            int head = (int) INTS.get(src, from);
            int tail = (int) INTS.get(src, from + len - 4);
            INTS.set(dst, to, head);
            INTS.set(dst, to + len - 4, tail);
        } else {
            for (int k = 0; k < len; k++) {
                dst[to + k] = src[from + k];
            }
        }
    }

    /**
     * Reusable heap arrays for {@link #gather}: one per iterator, grown on
     * demand.
     */
    public static final class Scratch {
        private int[] ints = new int[0];
        private byte[] bytes = new byte[0];

        int[] ints(int n) {
            if (ints.length < n) {
                ints = new int[Math.max(n, ints.length * 2)];
            }
            return ints;
        }

        byte[] bytes(int n) {
            if (bytes.length < n) {
                bytes = new byte[Math.max(n, bytes.length * 2)];
            }
            return bytes;
        }
    }
}
