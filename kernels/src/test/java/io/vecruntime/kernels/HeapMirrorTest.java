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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The bulk validity copies of {@link HeapMirror} (#555) against the per-word
 * {@link Bitmap#wordAt} / {@link Bitmap#setWord} loops they replace, on
 * word-padded segments and on segments of exactly {@code bytesFor(numBits)}
 * bytes (a partial tail word the bulk copy must not read or write past).
 */
class HeapMirrorTest {

    @ParameterizedTest
    @ValueSource(ints = {
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
        1000,
        4096,
        4097
    })
    void copyWordsMatchesWordAt(int numBits) {
        Random rnd = new Random(numBits);
        try (Arena arena = Arena.ofConfined()) {
            for (MemorySegment bm : new MemorySegment[] {
                Bitmap.allocate(arena, numBits), arena.allocate(Bitmap.bytesFor(numBits), 1),}) {
                for (long b = 0; b < bm.byteSize(); b++) {
                    bm.set(ValueLayout.JAVA_BYTE, b, (byte) rnd.nextInt());
                }
                long[] expected = new long[Bitmap.wordsFor(numBits)];
                for (int w = 0; w < expected.length; w++) {
                    expected[w] = Bitmap.wordAt(bm, w, numBits);
                }
                assertArrayEquals(expected, HeapMirror.copyWords(bm, numBits));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {
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
        1000,
        4096,
        4097
    })
    void storeWordsMatchesSetWord(int numBits) {
        Random rnd = new Random(31L * numBits);
        int words = Bitmap.wordsFor(numBits);
        long[] src = new long[words + 3];
        for (int w = 0; w < src.length; w++) {
            src[w] = rnd.nextLong();
        }
        src[words - 1] &= Bitmap.lowBits(numBits - ((words - 1) << 6));
        try (Arena arena = Arena.ofConfined()) {
            long[] sizes = {Bitmap.allocate(arena, numBits).byteSize(), Bitmap.bytesFor(numBits)};
            for (long size : sizes) {
                // One guard byte after the segment proves nothing is written past it.
                MemorySegment backing = arena.allocate(size + 1, 1);
                backing.set(ValueLayout.JAVA_BYTE, size, (byte) 0x5A);
                MemorySegment ours = backing.asSlice(0, size);
                MemorySegment theirs = arena.allocate(size, 1);
                HeapMirror.storeWords(ours, numBits, src);
                for (int w = 0; w < words; w++) {
                    Bitmap.setWord(theirs, w, numBits, src[w]);
                }
                assertEquals(-1L, ours.mismatch(theirs));
                assertEquals((byte) 0x5A, backing.get(ValueLayout.JAVA_BYTE, size));
            }
        }
    }
}
