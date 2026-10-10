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
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BitmapTest {

    @Test
    void bytesForRoundsUpToWholeBytes() {
        assertEquals(0, Bitmap.bytesFor(0));
        assertEquals(1, Bitmap.bytesFor(1));
        assertEquals(1, Bitmap.bytesFor(8));
        assertEquals(2, Bitmap.bytesFor(9));
        assertEquals(8, Bitmap.bytesFor(64));
        assertEquals(9, Bitmap.bytesFor(65));
    }

    @Test
    void setClearAndIsSetUseArrowLsbBitOrder() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bm = Bitmap.allocate(arena, 16);
            Bitmap.set(bm, 0);
            Bitmap.set(bm, 9);
            assertEquals(0b0000_0001, bm.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0) & 0xFF);
            assertEquals(0b0000_0010, bm.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 1) & 0xFF);
            assertTrue(Bitmap.isSet(bm, 0));
            assertTrue(Bitmap.isSet(bm, 9));
            assertFalse(Bitmap.isSet(bm, 1));
            Bitmap.clear(bm, 9);
            assertFalse(Bitmap.isSet(bm, 9));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {
        0,
        1,
        7,
        8,
        63,
        64,
        65,
        127,
        128,
        129,
        1000,
        4097
    })
    void popcountMatchesScalarReferenceAndIgnoresTrailingGarbage(int numBits) {
        Random rnd = new Random(42L + numBits);
        try (Arena arena = Arena.ofConfined()) {
            // Allocate extra bytes filled with garbage to make sure the tail is masked.
            long bytes = Bitmap.bytesFor(numBits) + 16;
            MemorySegment bm = arena.allocate(bytes, 8);
            for (long i = 0; i < bytes; i++) {
                bm.set(java.lang.foreign.ValueLayout.JAVA_BYTE,
                        i, (byte) rnd.nextInt(256));
            }
            int expected = 0;
            for (int i = 0; i < numBits; i++) {
                if (Bitmap.isSet(bm, i)) {
                    expected++;
                }
            }
            assertEquals(expected, Bitmap.popcount(bm, numBits));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 63, 64, 65, 130})
    void wordAtMasksBitsBeyondLength(int numBits) {
        try (Arena arena = Arena.ofConfined()) {
            long bytes = Bitmap.bytesFor(numBits) + 8;
            MemorySegment bm = arena.allocate(bytes, 8);
            bm.fill((byte) 0xFF);
            int words = (numBits + 63) >>> 6;
            for (int w = 0; w < words; w++) {
                long word = Bitmap.wordAt(bm, w, numBits);
                int validBitsInWord = Math.min(64, numBits - (w << 6));
                long expected = validBitsInWord == 64 ? -1L : (1L << validBitsInWord) - 1;
                assertEquals(expected, word, "word " + w);
            }
        }
    }

    @Test
    void allSetAndNoneSetFastPaths() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bm = Bitmap.allocate(arena, 100);
            assertTrue(Bitmap.noneSet(bm, 100));
            assertFalse(Bitmap.allSet(bm, 100));
            Bitmap.fill(bm, 100, true);
            assertTrue(Bitmap.allSet(bm, 100));
            assertFalse(Bitmap.noneSet(bm, 100));
            Bitmap.clear(bm, 99);
            assertFalse(Bitmap.allSet(bm, 100));
            assertEquals(99, Bitmap.popcount(bm, 100));
        }
    }

    // #541: the word-level writers against a bit-by-bit reference, at every offset and length that
    // crosses a byte and a word boundary, into destinations of exactly bytesFor(end) bytes (so the tail
    // takes the byte-wise path) and padded ones, with the bits around the written range random.

    private static MemorySegment randomBits(Arena a, Random r, long bytes) {
        MemorySegment s = a.allocate(Math.max(1, bytes));
        for (long i = 0; i < s.byteSize(); i++) {
            s.set(Bitmap.BYTE, i, (byte) r.nextInt());
        }
        return s;
    }

    private static MemorySegment copyOf(Arena a, MemorySegment s) {
        MemorySegment c = a.allocate(s.byteSize());
        c.copyFrom(s);
        return c;
    }

    private static void assertSameBits(MemorySegment expected, MemorySegment actual, int bits,
            String what) {
        for (int i = 0; i < bits; i++) {
            assertEquals(Bitmap.isSet(expected, i), Bitmap.isSet(actual, i), what + ": bit " + i);
        }
    }

    @Test
    void copyBitsMatchesBitByBitAtAnyOffset() {
        Random r = new Random(541);
        try (Arena a = Arena.ofConfined()) {
            for (int from = 0; from < 80; from++) {
                for (int count : new int[] {
                    0,
                    1,
                    7,
                    8,
                    9,
                    63,
                    64,
                    65,
                    127,
                    128,
                    129,
                    200
                }) {
                    for (boolean exact : new boolean[] {true, false}) {
                        int end = from + count;
                        long bytes = exact ? Bitmap.bytesFor(end) : Bitmap.bytesFor(end) + 16;
                        MemorySegment src = randomBits(a, r, Bitmap.bytesFor(count));
                        MemorySegment dst = randomBits(a, r, bytes);
                        MemorySegment ref = copyOf(a, dst);
                        for (int i = 0; i < count; i++) {
                            Bitmap.setTo(ref, from + i, Bitmap.isSet(src, i));
                        }
                        Bitmap.copyBits(src, dst, from, count);
                        assertSameBits(ref, dst, (int) (bytes * 8), "copyBits from=" + from + " count=" + count);
                    }
                }
            }
        }
    }

    @Test
    void fillRangeMatchesBitByBit() {
        Random r = new Random(5411);
        try (Arena a = Arena.ofConfined()) {
            for (int from = 0; from < 70; from++) {
                for (int count : new int[] {0, 1, 5, 8, 13, 64,
                        70, 150}) {
                    for (boolean value : new boolean[] {true, false}) {
                        long bytes = Bitmap.bytesFor(from + count);
                        MemorySegment dst = randomBits(a, r, bytes);
                        MemorySegment ref = copyOf(a, dst);
                        for (int i = 0; i < count; i++) {
                            Bitmap.setTo(ref, from + i, value);
                        }
                        Bitmap.fillRange(dst, from, count, value);
                        assertSameBits(ref, dst, (int) (bytes * 8), "fillRange from=" + from + " count=" + count);
                    }
                }
            }
        }
    }

    @Test
    void copyBitsFromAndAppendSelectedBitsMatchBitByBit() {
        Random r = new Random(5412);
        try (Arena a = Arena.ofConfined()) {
            for (int trial = 0; trial < 400; trial++) {
                int srcLen = r.nextInt(300);
                int srcFrom = srcLen == 0 ? 0 : r.nextInt(srcLen);
                int count = srcLen - srcFrom;
                int dstFrom = r.nextInt(130);
                MemorySegment src = randomBits(a, r, Bitmap.bytesFor(srcLen));
                // copyBitsFrom: bits [srcFrom, srcLen) of src to dstFrom.
                long bytes = Bitmap.bytesFor(dstFrom + count);
                MemorySegment dst = randomBits(a, r, bytes);
                MemorySegment ref = copyOf(a, dst);
                for (int i = 0; i < count; i++) {
                    Bitmap.setTo(ref, dstFrom + i, Bitmap.isSet(src, srcFrom + i));
                }
                Bitmap.copyBitsFrom(src, srcFrom, dst, dstFrom, count);
                assertSameBits(ref, dst, (int) (bytes * 8), "copyBitsFrom trial " + trial);
                // appendSelectedBits: the bits of src at the set positions of a random selection, in order.
                MemorySegment sel = randomBits(a, r, Bitmap.bytesFor(srcLen));
                int selected = Bitmap.popcount(sel, srcLen);
                long sbytes = Bitmap.bytesFor(dstFrom + selected);
                MemorySegment sdst = randomBits(a, r, sbytes);
                MemorySegment sref = copyOf(a, sdst);
                int o = dstFrom;
                for (int i = 0; i < srcLen; i++) {
                    if (Bitmap.isSet(sel, i)) {
                        Bitmap.setTo(sref, o++, Bitmap.isSet(src, i));
                    }
                }
                assertEquals(selected, Bitmap.appendSelectedBits(src, sel, srcLen, sdst, dstFrom));
                assertSameBits(sref, sdst, (int) (sbytes * 8), "appendSelectedBits trial " + trial);
            }
        }
    }
}
