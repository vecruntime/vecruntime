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
package io.vecruntime.kernels.parquet;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Scalar-oracle tests for {@link RleBitPackingReader}: values written with a
 * from-scratch encoder (whose output is trivially correct by construction) are
 * read back and compared. Widths 0..32, RLE and bit-packed runs and their
 * boundaries, lengths not a multiple of 8.
 */
class RleBitPackingReaderTest {

    /**
     * Encodes {@code values} at {@code bitWidth} using explicit RLE /
     * bit-packed runs.
     */
    static byte[] encode(int[] values, int bitWidth, boolean forceBitPacked) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (bitWidth == 0) {
            return out.toByteArray();
        }
        int byteWidth = (bitWidth + 7) / 8;
        int i = 0;
        while (i < values.length) {
            // Count a run of equal values.
            int runLen = 1;
            while (i + runLen < values.length && values[i + runLen] == values[i]) {
                runLen++;
            }
            if (!forceBitPacked && runLen >= 8) {
                // RLE run.
                writeUleb128(out, runLen << 1);
                for (int b = 0; b < byteWidth; b++) {
                    out.write((values[i] >>> (8 * b)) & 0xFF);
                }
                i += runLen;
            } else {
                // Bit-packed run: covers whole groups of 8. Accumulate values until a long RLE-worthy
                // run begins ON a group boundary (count % 8 == 0), or the array ends; the final group is
                // zero-padded to 8 (real Parquet framing -- the reader stops at the total value count).
                int start = i;
                int count = 0;
                while (i < values.length) {
                    int rl = 1;
                    while (i + rl < values.length && values[i + rl] == values[i]) {
                        rl++;
                    }
                    if (!forceBitPacked && rl >= 8 && (count % 8) == 0) {
                        break;
                    }
                    i += rl;
                    count += rl;
                }
                int groups = (count + 7) / 8;
                writeUleb128(out, (groups << 1) | 1);
                packBits(out, values, start, count, groups * 8,
                        bitWidth);
            }
        }
        return out.toByteArray();
    }

    private static void packBits(ByteArrayOutputStream out, int[] values, int start,
            int count, int padded, int bitWidth) {
        long buffer = 0;
        int bits = 0;
        for (int j = 0; j < padded; j++) {
            int v = j < count ? values[start + j] : 0;
            buffer |= (v & mask(bitWidth)) << bits;
            bits += bitWidth;
            while (bits >= 8) {
                out.write((int) (buffer & 0xFF));
                buffer >>>= 8;
                bits -= 8;
            }
        }
        if (bits > 0) {
            out.write((int) (buffer & 0xFF));
        }
    }

    private static long mask(int bitWidth) {
        return bitWidth == 32 ? 0xFFFFFFFFL : ((1L << bitWidth) - 1);
    }

    private static void writeUleb128(ByteArrayOutputStream out, int value) {
        int v = value;
        while (true) {
            int b = v & 0x7F;
            v >>>= 7;
            if (v != 0) {
                out.write(b | 0x80);
            } else {
                out.write(b);
                return;
            }
        }
    }

    private static void check(int[] values, int bitWidth, boolean forceBitPacked) {
        byte[] bytes = encode(values, bitWidth, forceBitPacked);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocate(Math.max(bytes.length, 1) + 8L);
            MemorySegment.copy(MemorySegment.ofArray(bytes), 0, seg, 0, bytes.length);
            RleBitPackingReader r = new RleBitPackingReader(seg, 0, bytes.length, bitWidth);
            for (int i = 0; i < values.length; i++) {
                assertEquals(values[i], r.readInt(), "value " + i + " at width " + bitWidth);
            }
        }
    }

    @Test
    void widthZeroAlwaysZero() {
        int[] v = new int[100];
        check(v, 0, false);
    }

    @Test
    void allWidthsRoundTrip() {
        Random rnd = new Random(559);
        for (int bitWidth = 1; bitWidth <= 32; bitWidth++) {
            long max = mask(bitWidth);
            for (int len : new int[] {1, 7, 8, 9, 63, 64,
                    65, 200}) {
                int[] v = new int[len];
                for (int i = 0; i < len; i++) {
                    v[i] = (int) (Math.floorMod(rnd.nextLong(), max + 1));
                }
                check(v, bitWidth, true); // bit-packed
                check(v, bitWidth, false); // mixed RLE + bit-packed
            }
        }
    }

    @Test
    void longRleRun() {
        int[] v = new int[1000];
        java.util.Arrays.fill(v, 12345);
        check(v, 20, false);
    }

    @Test
    void alternatingForcesBitPacked() {
        int[] v = new int[100];
        for (int i = 0; i < v.length; i++) {
            v[i] = i % 4;
        }
        check(v, 2, false);
    }

    @Test
    void runThenPackedThenRunBoundaries() {
        // 10 equal (RLE), 5 varied (bit-packed), 12 equal (RLE).
        int[] v = new int[27];
        for (int i = 0; i < 10; i++) {
            v[i] = 3;
        }
        for (int i = 10; i < 15; i++) {
            v[i] = i;
        }
        for (int i = 15; i < 27; i++) {
            v[i] = 7;
        }
        check(v, 6, false);
    }
}
