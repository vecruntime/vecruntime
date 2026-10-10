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
import java.nio.ByteOrder;

/**
 * Utilities over Arrow-layout bitmaps (validity bitmaps and boolean/selection
 * masks).
 *
 * <p>Arrow bitmaps are LSB-first: bit {@code i} lives in byte {@code i >>> 3}
 * at bit position {@code i & 7}. Reading the bitmap as little-endian 64-bit
 * words therefore gives a word whose bit {@code k} is element {@code wordIndex
 * * 64 + k}, which is exactly the layout expected by {@code
 * VectorMask.fromLong}.
 *
 * <p>Bitmaps may be allocated with padding beyond {@code numBits}; padding bits
 * are never assumed to be zero and every bulk operation masks the tail word.
 */
public final class Bitmap {

    /**
     * Little-endian unaligned long view, independent of the platform's native
     * byte order.
     */
    static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private Bitmap() {}

    /** Number of bytes needed to hold {@code numBits} bits. */
    public static long bytesFor(int numBits) {
        return ((long) numBits + 7) >>> 3;
    }

    /** Number of 64-bit words needed to hold {@code numBits} bits. */
    public static int wordsFor(int numBits) {
        return (numBits + 63) >>> 6;
    }

    /**
     * Allocates a zeroed bitmap for {@code numBits} bits, rounded up to a
     * multiple of 8 bytes so that whole-word reads never run past the end of
     * the segment.
     */
    public static MemorySegment allocate(Arena arena, int numBits) {
        long bytes = Math.max(8L, (long) wordsFor(numBits) * 8L);
        return arena.allocate(bytes, 8);
    }

    public static boolean isSet(MemorySegment bm, int index) {
        return ((bm.get(BYTE, index >>> 3) >>> (index & 7)) & 1) != 0;
    }

    public static void set(MemorySegment bm, int index) {
        int byteIndex = index >>> 3;
        bm.set(BYTE, byteIndex, (byte) (bm.get(BYTE, byteIndex) | (1 << (index & 7))));
    }

    public static void clear(MemorySegment bm, int index) {
        int byteIndex = index >>> 3;
        bm.set(BYTE, byteIndex, (byte) (bm.get(BYTE, byteIndex) & ~(1 << (index & 7))));
    }

    public static void setTo(MemorySegment bm, int index, boolean value) {
        if (value) {
            set(bm, index);
        } else {
            clear(bm, index);
        }
    }

    /**
     * Sets bits {@code [from, from + count)} to {@code value}; the rest of the
     * bitmap is left as it is.
     */
    public static void fillRange(MemorySegment bm, int from, int count,
            boolean value) {
        int i = from;
        int end = from + count;
        // Leading partial byte, whole bytes, trailing partial byte; the partial ones a word write each.
        int head = Math.min(end - i, (8 - (i & 7)) & 7);
        if (head > 0) {
            putBits(bm, i, value ? -1L : 0L, head);
            i += head;
        }
        byte b = (byte) (value ? 0xFF : 0);
        int bytes = (end - i) >>> 3;
        if (bytes > 0) {
            bm.asSlice(i >>> 3, bytes).fill(b);
            i += bytes << 3;
        }
        if (i < end) {
            putBits(bm, i, value ? -1L : 0L,
                    end - i);
        }
    }

    /**
     * Copies bits {@code [0, count)} of {@code src} to {@code dst} at bit
     * offset {@code dstFrom} -- the append of a compacted validity or BOOL
     * slice at a row offset (#351). Word at a time at any offset (#541): a bit
     * at a time paid a checked byte read and write per row.
     */
    public static void copyBits(MemorySegment src, MemorySegment dst, int dstFrom,
            int count) {
        if ((dstFrom & 7) == 0) {
            int whole = count >>> 3;
            if (whole > 0) {
                MemorySegment.copy(src, 0, dst, dstFrom >>> 3, whole);
            }
            int rest = count & 7;
            if (rest != 0) {
                putBits(dst, dstFrom + (whole << 3), wordAt(src, whole >>> 3, count) >>> ((whole & 7) << 3),
                        rest);
            }
            return;
        }
        for (int w = 0, words = wordsFor(count);
             w < words;
             w++) {
            int n = Math.min(64, count - (w << 6));
            putBits(dst, dstFrom + (w << 6), wordAt(src, w, count), n);
        }
    }

    /**
     * Writes the low {@code n} bits ({@code 1 <= n <= 64}) of {@code bits} to
     * bits {@code [dstBit, dstBit + n)} of {@code dst}, leaving every other bit
     * as it is: at most two read-modify-writes of a 64-bit word (#541).
     */
    public static void putBits(MemorySegment dst, long dstBit, long bits,
            int n) {
        long mask = lowBits(n);
        bits &= mask;
        int shift = (int) (dstBit & 63);
        long wordByte = (dstBit >>> 6) << 3;
        putMasked(dst, wordByte, bits << shift, mask << shift);
        if (shift + n > 64) {
            putMasked(dst, wordByte + 8, bits >>> (64 - shift),
                    mask >>> (64 - shift));
        }
    }

    /**
     * {@code word = (word & ~mask) | (bits & mask)} at byte offset {@code at},
     * byte-wise past the end.
     */
    private static void putMasked(MemorySegment dst, long at, long bits,
            long mask) {
        if (at + 8 <= dst.byteSize()) {
            long old = dst.get(LE_LONG, at);
            dst.set(LE_LONG, at, (old & ~mask) | (bits & mask));
            return;
        }
        for (int b = 0;
             b < 8 && mask >>> (b << 3) != 0L;
             b++) {
            int m = (int) (mask >>> (b << 3)) & 0xFF;
            if (m != 0) {
                long off = at + b;
                byte old = dst.get(BYTE, off);
                dst.set(BYTE, off, (byte) ((old & ~m) | ((int) (bits >>> (b << 3)) & m)));
            }
        }
    }

    /**
     * Appends to {@code dst} at bit {@code dstFrom} the bits of {@code src} at
     * the positions set in {@code selection} (its first {@code srcLen} bits),
     * in order; returns how many were written. Per 64 rows: one word of each,
     * {@link Long#compress} (a single instruction where the JIT has one) and one
     * {@link #putBits} (#541).
     */
    public static int appendSelectedBits(MemorySegment src, MemorySegment selection, int srcLen,
            MemorySegment dst, int dstFrom) {
        int o = dstFrom;
        for (int w = 0, words = wordsFor(srcLen);
             w < words;
             w++) {
            long sel = wordAt(selection, w, srcLen);
            if (sel == 0L) {
                continue;
            }
            int n = Long.bitCount(sel);
            putBits(dst, o, Long.compress(wordAt(src, w, srcLen), sel), n);
            o += n;
        }
        return o - dstFrom;
    }

    /**
     * Copies bits {@code srcFrom .. srcFrom + count} of {@code src} to bits
     * {@code dstFrom ..} of {@code dst}: whole bytes when both offsets are byte
     * aligned, a word at a time from the source otherwise.
     */
    public static void copyBitsFrom(MemorySegment src, int srcFrom, MemorySegment dst,
            int dstFrom, int count) {
        if ((srcFrom & 7) == 0) {
            if (srcFrom == 0) {
                copyBits(src, dst, dstFrom, count);
            } else {
                copyBits(src.asSlice(srcFrom >>> 3), dst, dstFrom, count);
            }
            return;
        }
        int end = srcFrom + count;
        for (int o = 0; o < count; ) {
            int s = srcFrom + o;
            int shift = s & 63;
            long word = wordAt(src, s >>> 6, end) >>> shift;
            int take = Math.min(64 - shift, count - o);
            putBits(dst, dstFrom + o, word, take);
            o += take;
        }
    }

    public static void fill(MemorySegment bm, int numBits, boolean value) {
        long bytes = bytesFor(numBits);
        if (bytes == 0) {
            return;
        }
        bm.asSlice(0, bytes).fill(value ? (byte) 0xFF : (byte) 0);
        if (value) {
            int rem = numBits & 7;
            if (rem != 0) {
                long last = bytes - 1;
                bm.set(BYTE, last, (byte) (bm.get(BYTE, last) & ((1 << rem) - 1)));
            }
        }
    }

    /**
     * Returns the 64-bit word at {@code wordIndex}, with any bits at or beyond
     * {@code numBits} cleared. Safe to call on a bitmap whose backing segment
     * holds only {@code bytesFor(numBits)} bytes: the tail word is assembled
     * byte by byte when a full 8-byte read would overrun.
     */
    public static long wordAt(MemorySegment bm, int wordIndex, int numBits) {
        long byteOffset = (long) wordIndex << 3;
        int bitsInWord = numBits - (wordIndex << 6);
        if (bitsInWord >= 64) {
            return bm.get(LE_LONG, byteOffset);
        }
        if (bitsInWord <= 0) {
            return 0L;
        }
        long word;
        if (byteOffset + 8 <= bm.byteSize()) {
            word = bm.get(LE_LONG, byteOffset);
        } else {
            word = 0L;
            long bytes = bytesFor(bitsInWord);
            for (int b = 0; b < bytes; b++) {
                word |= (bm.get(BYTE, byteOffset + b) & 0xFFL) << (b << 3);
            }
        }
        return word & ((1L << bitsInWord) - 1);
    }

    /** A word with the low {@code count} bits set ({@code 1 <= count <= 64}). */
    public static long lowBits(int count) {
        return count >= 64 ? -1L : (1L << count) - 1;
    }

    /**
     * Writes the low {@code min(64, numBits - wordIndex*64)} bits of {@code
     * word}.
     */
    public static void setWord(MemorySegment bm, int wordIndex, int numBits,
            long word) {
        long byteOffset = (long) wordIndex << 3;
        int bitsInWord = numBits - (wordIndex << 6);
        if (bitsInWord >= 64 || byteOffset + 8 <= bm.byteSize()) {
            bm.set(LE_LONG, byteOffset, word);
            return;
        }
        long bytes = bytesFor(bitsInWord);
        for (int b = 0; b < bytes; b++) {
            bm.set(BYTE, byteOffset + b, (byte) (word >>> (b << 3)));
        }
    }

    /** Number of set bits among the first {@code numBits}. */
    public static int popcount(MemorySegment bm, int numBits) {
        int words = wordsFor(numBits);
        int count = 0;
        for (int w = 0; w < words; w++) {
            count += Long.bitCount(wordAt(bm, w, numBits));
        }
        return count;
    }

    public static boolean allSet(MemorySegment bm, int numBits) {
        return popcount(bm, numBits) == numBits;
    }

    public static boolean noneSet(MemorySegment bm, int numBits) {
        int words = wordsFor(numBits);
        for (int w = 0; w < words; w++) {
            if (wordAt(bm, w, numBits) != 0L) {
                return false;
            }
        }
        return true;
    }
}
