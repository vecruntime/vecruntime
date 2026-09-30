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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A decoder for Parquet's RLE / bit-packed hybrid encoding: the
 * {@code RLE_DICTIONARY} value encoding and the definition / repetition level
 * encoding. It reads one fixed bit width; runs alternate freely between a
 * run-length-encoded value and a bit-packed group as the stream dictates.
 *
 * <p>Format (parquet-format {@code Encodings.md}):
 *
 * <pre>
 *   hybrid  := (rle-run | bit-packed-run)*
 *   header  := ULEB128 varint; low bit 0 = RLE run, 1 = bit-packed run
 *   rle-run := header (= count &lt;&lt; 1) value  -- value is ceil(bitWidth/8) little-endian bytes
 *   bp-run  := header (= groups &lt;&lt; 1 | 1) (bitWidth-bit values, groups*8 of them)
 * </pre>
 *
 * <p>This reader is scalar and allocation-free. It walks a {@link MemorySegment}
 * over the decompressed page bytes; the caller owns the segment's lifetime. It
 * is the single source of level and id decoding for {@link ParquetPageDecoder}.
 * Kept scalar in this slice (correctness against Spark's reader is what slice 1
 * measures); the JMH benchmark decides any later Vector-API widening (AGENTS.md:
 * a kernel is not faster until a benchmark says so).
 */
public final class RleBitPackingReader {

    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private final MemorySegment page;
    private final long end;
    private final int bitWidth;
    private final int byteWidthOfValue; // ceil(bitWidth / 8), for an RLE run's repeated value
    private final int mask;

    private long pos;

    // Current RLE run.
    private int rleValue;
    private int rleRemaining;

    // Current bit-packed run: `packedRemaining` values remain across it; `packed` holds one group of 8.
    private final int[] packed = new int[8];
    private int packedIndex = 8; // index into `packed`; 8 forces a group refill
    private int packedRemaining;

    /**
     * @param page     the decompressed bytes
     * @param offset   first byte of the hybrid stream within {@code page}
     * @param length   number of bytes of the hybrid stream
     * @param bitWidth bits per value (0 is legal: every value is 0, no bytes read)
     */
    public RleBitPackingReader(MemorySegment page, long offset, long length,
            int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("bit width out of range: " + bitWidth);
        }
        this.page = page;
        this.pos = offset;
        this.end = offset + length;
        this.bitWidth = bitWidth;
        this.byteWidthOfValue = (bitWidth + 7) / 8;
        this.mask = bitWidth == 32 ? -1 : ((1 << bitWidth) - 1);
    }

    /** The next value, advancing the stream. A width of 0 always yields 0. */
    public int readInt() {
        if (bitWidth == 0) {
            return 0;
        }
        while (rleRemaining == 0 && packedRemaining == 0) {
            readRunHeader();
        }
        if (rleRemaining > 0) {
            rleRemaining--;
            return rleValue;
        }
        if (packedIndex == 8) {
            refillPackedGroup();
        }
        packedRemaining--;
        return packed[packedIndex++];
    }

    private void readRunHeader() {
        int header = readUleb128();
        boolean bitPacked = (header & 1) != 0;
        int count = header >>> 1;
        if (bitPacked) {
            packedRemaining = count * 8;
            packedIndex = 8; // force a refill on the first read of this run
        } else {
            rleRemaining = count;
            rleValue = readLittleEndian(byteWidthOfValue);
        }
    }

    /**
     * Unpacks the next group of 8 bit-packed values (LSB-first, values
     * low-to-high) into {@code packed}.
     */
    private void refillPackedGroup() {
        long bitBuffer = 0;
        int bitsInBuffer = 0;
        for (int i = 0; i < 8; i++) {
            while (bitsInBuffer < bitWidth) {
                int b = page.get(BYTE, pos++) & 0xFF;
                bitBuffer |= ((long) b) << bitsInBuffer;
                bitsInBuffer += 8;
            }
            packed[i] = (int) (bitBuffer & (mask & 0xFFFFFFFFL));
            bitBuffer >>>= bitWidth;
            bitsInBuffer -= bitWidth;
        }
        packedIndex = 0;
    }

    /** A little-endian value of {@code bytes} bytes (an RLE run's repeated value). */
    private int readLittleEndian(int bytes) {
        int v = 0;
        for (int i = 0; i < bytes; i++) {
            v |= (page.get(BYTE, pos++) & 0xFF) << (8 * i);
        }
        return v;
    }

    /** An unsigned LEB128 varint (the run header). */
    private int readUleb128() {
        int value = 0;
        int shift = 0;
        while (true) {
            if (pos >= end) {
                throw new IllegalStateException("RLE/bit-packed stream ended inside a run header");
            }
            int b = page.get(BYTE, pos++) & 0xFF;
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }
}
