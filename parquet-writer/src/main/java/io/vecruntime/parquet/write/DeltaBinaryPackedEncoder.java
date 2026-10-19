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
package io.vecruntime.parquet.write;

/**
 * Parquet's {@code DELTA_BINARY_PACKED} encoding over whole arrays, the v2
 * encoding of INT32 and INT64 values without a dictionary (and of the lengths
 * inside {@code DELTA_BYTE_ARRAY}).
 *
 * <p>Layout, as parquet-java writes it: a header (block size 128, 4 miniblocks
 * per block, the value count, the first value zigzag-encoded), then per block
 * of 128 deltas the block's minimum delta (zigzag varint), one bit width per
 * miniblock, and the miniblocks of 32 values each, every value {@code delta -
 * minDelta} bit-packed at the miniblock's width, least significant bit first.
 * Miniblocks past the last value are not written; their widths are 0. INT32
 * deltas are computed and packed in 32-bit arithmetic, as readers decode them.
 */
final class DeltaBinaryPackedEncoder {

    static final int BLOCK = 128;
    static final int MINIBLOCKS = 4;
    static final int MINIBLOCK = BLOCK / MINIBLOCKS;

    private final long[] deltas = new long[BLOCK];

    /** Encodes {@code values[0, n)} as 32-bit integers (their low 32 bits). */
    void encodeInts(long[] values, int n, ByteSink out) {
        header(
                n,
                n == 0 ? 0 : (int) values[0],
                out);
        int i = 1;
        while (i < n) {
            int count = Math.min(BLOCK, n - i);
            int min = Integer.MAX_VALUE;
            for (int k = 0; k < count; k++) {
                int d = (int) values[i + k] - (int) values[i + k - 1];
                deltas[k] = d;
                min = Math.min(min, d);
            }
            for (int k = 0; k < count; k++) {
                deltas[k] = ((int) deltas[k] - min) & 0xFFFFFFFFL;
            }
            block(count, zigzag(min), out);
            i += count;
        }
    }

    /** Encodes {@code values[0, n)} as 64-bit integers. */
    void encodeLongs(long[] values, int n, ByteSink out) {
        header(n, n == 0 ? 0 : values[0], out);
        int i = 1;
        while (i < n) {
            int count = Math.min(BLOCK, n - i);
            long min = Long.MAX_VALUE;
            for (int k = 0; k < count; k++) {
                long d = values[i + k] - values[i + k - 1];
                deltas[k] = d;
                min = Math.min(min, d);
            }
            for (int k = 0; k < count; k++) {
                deltas[k] -= min;
            }
            block(count, zigzag(min), out);
            i += count;
        }
    }

    /** Encodes {@code values[0, n)} (non-negative lengths) as 32-bit integers. */
    void encodeInts(int[] values, int n, ByteSink out) {
        header(n, n == 0 ? 0 : values[0], out);
        int i = 1;
        while (i < n) {
            int count = Math.min(BLOCK, n - i);
            int min = Integer.MAX_VALUE;
            for (int k = 0; k < count; k++) {
                int d = values[i + k] - values[i + k - 1];
                deltas[k] = d;
                min = Math.min(min, d);
            }
            for (int k = 0; k < count; k++) {
                deltas[k] = ((int) deltas[k] - min) & 0xFFFFFFFFL;
            }
            block(count, zigzag(min), out);
            i += count;
        }
    }

    private static void header(int n, long first, ByteSink out) {
        writeVarLong(BLOCK, out);
        writeVarLong(MINIBLOCKS, out);
        writeVarLong(n, out);
        writeVarLong(zigzag(first), out);
    }

    private static long zigzag(long v) {
        return (v << 1) ^ (v >> 63);
    }

    private static void writeVarLong(long v, ByteSink out) {
        while ((v & ~0x7FL) != 0) {
            out.writeByte((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.writeByte((int) v);
    }

    /** One block of {@code count} deltas already reduced by the block minimum. */
    private void block(int count, long zigzagMin, ByteSink out) {
        writeVarLong(zigzagMin, out);
        int used = (count + MINIBLOCK - 1) / MINIBLOCK;
        int[] widths = new int[MINIBLOCKS];
        for (int m = 0; m < used; m++) {
            long or = 0;
            int end = Math.min(count, (m + 1) * MINIBLOCK);
            for (int k = m * MINIBLOCK; k < end; k++) {
                or |= deltas[k];
            }
            widths[m] = 64 - Long.numberOfLeadingZeros(or);
        }
        for (int m = 0; m < MINIBLOCKS; m++) {
            out.writeByte(widths[m]);
        }
        for (int m = 0; m < used; m++) {
            pack(m * MINIBLOCK, Math.min(count, (m + 1) * MINIBLOCK), widths[m], out);
        }
    }

    /**
     * Bit-packs deltas {@code [from, to)} as a full miniblock of 32 values
     * (padded with zeros).
     */
    private void pack(int from, int to, int width,
                      ByteSink out) {
        if (width == 0) {
            return;
        }
        out.packBits(deltas, from, to, MINIBLOCK, width);
    }
}
