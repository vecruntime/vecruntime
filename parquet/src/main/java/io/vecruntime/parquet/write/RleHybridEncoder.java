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

import java.lang.foreign.MemorySegment;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.parquet.codec.ParquetFraming;

/**
 * Parquet's RLE / bit-packing hybrid encoding over whole arrays: the definition
 * levels of a page and the dictionary ids of an {@code RLE_DICTIONARY} page.
 *
 * <p>A run of eight or more equal values becomes an RLE run (a varint header
 * {@code count << 1}, then the value in {@code ceil(bitWidth / 8)}
 * little-endian bytes); everything else is bit-packed in groups of eight values
 * (a varint header {@code groups << 1 | 1}, then {@code bitWidth} bytes per
 * group, least significant bit first). A bit-packed run must hold a multiple of
 * eight values unless it ends the stream, so before an RLE run the pending
 * bit-packed values are topped up to a multiple of eight from the run itself.
 *
 * <p>The definition levels of a flat optional column come from the validity
 * bitmap directly ({@link #encodeValidity}): runs of set and clear bits are
 * found a 64-bit word at a time, so a page without nulls is one RLE run
 * whatever its length, and a page with a few nulls is a few runs.
 */
final class RleHybridEncoder {

    private RleHybridEncoder() {}

    /** Bits needed for values {@code 0 .. maxValue}. */
    static int bitWidth(int maxValue) {
        return ParquetFraming.bitWidth(maxValue);
    }

    /** Encodes {@code values[0, n)} at {@code bitWidth} into {@code out}. */
    static void encode(int[] values, int n, int bitWidth,
                       ByteSink out) {
        int pendingStart = 0;
        int i = 0;
        while (i < n) {
            int v = values[i];
            int r = 1;
            while (i + r < n && values[i + r] == v) {
                r++;
            }
            if (r >= 8) {
                int fill = (8 - ((i - pendingStart) & 7)) & 7;
                if (r - fill >= 8) {
                    bitPack(values, pendingStart, i + fill, bitWidth, out);
                    writeRle(v, r - fill, bitWidth, out);
                    i += r;
                    pendingStart = i;
                    continue;
                }
            }
            i += r;
        }
        bitPack(values, pendingStart, n, bitWidth, out);
    }

    /**
     * Definition levels (bit width 1) of rows {@code [from, to)} whose validity
     * bitmap is {@code validity} ({@code null}: all valid): 1 for a value, 0
     * for a null.
     */
    static void encodeValidity(MemorySegment validity, int from, int to,
            ByteSink out) {
        int n = to - from;
        if (n == 0) {
            return;
        }
        if (validity == null) {
            writeRle(1, n, 1, out);
            return;
        }
        // Runs of equal bits, found a word at a time; short alternations collect into bit-packed groups.
        int[] pending = new int[64];
        int pendingCount = 0;
        int i = from;
        while (i < to) {
            boolean bit = Bitmap.isSet(validity, i);
            int r = runOfBit(validity, i, to, bit);
            int level = bit ? 1 : 0;
            if (r >= 8) {
                int fill = (8 - (pendingCount & 7)) & 7;
                if (r - fill >= 8) {
                    for (int k = 0; k < fill; k++) {
                        pending = append(pending, pendingCount++, level);
                    }
                    bitPack(pending, 0, pendingCount, 1, out);
                    pendingCount = 0;
                    writeRle(level, r - fill, 1, out);
                    i += r;
                    continue;
                }
            }
            for (int k = 0; k < r; k++) {
                pending = append(pending, pendingCount++, level);
            }
            i += r;
        }
        bitPack(pending, 0, pendingCount, 1, out);
    }

    private static int[] append(int[] a, int at, int v) {
        if (at == a.length) {
            a = java.util.Arrays.copyOf(a, a.length * 2);
        }
        a[at] = v;
        return a;
    }

    /**
     * Length of the run of {@code bit} starting at {@code i}, not past {@code
     * to}.
     */
    private static int runOfBit(MemorySegment bitmap, int i, int to,
            boolean bit) {
        int start = i;
        // Bit by bit up to the next word boundary, then whole words, then the tail.
        while (i < to && (i & 63) != 0) {
            if (Bitmap.isSet(bitmap, i) != bit) {
                return i - start;
            }
            i++;
        }
        while (i + 64 <= to) {
            long w = bitmap.get(io.vecruntime.kernels.VectorBuffers.LE_LONG, (long) (i >>> 6) << 3);
            long diff = bit ? ~w : w;
            if (diff != 0) {
                return i + Long.numberOfTrailingZeros(diff) - start;
            }
            i += 64;
        }
        while (i < to) {
            if (Bitmap.isSet(bitmap, i) != bit) {
                return i - start;
            }
            i++;
        }
        return i - start;
    }

    static void writeRle(int value, int count, int bitWidth,
                         ByteSink out) {
        out.writeUnsignedVarInt(ParquetFraming.rleRunHeader(count));
        int bytes = ParquetFraming.rleValueBytes(bitWidth);
        for (int b = 0; b < bytes; b++) {
            out.writeByte(value >>> (8 * b));
        }
    }

    /** Bit-packs {@code values[from, to)}, padding the last group with zeros. */
    static void bitPack(int[] values, int from, int to,
                        int bitWidth, ByteSink out) {
        int n = to - from;
        if (n == 0) {
            return;
        }
        int groups = ParquetFraming.bitPackedGroups(n);
        out.writeUnsignedVarInt(ParquetFraming.bitPackedRunHeader(groups));
        if (bitWidth == 0) {
            return;
        }
        out.packBits(values, from, to, groups << 3, bitWidth);
    }
}
