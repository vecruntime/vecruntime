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
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    private final MemorySegment page;
    private final long end;
    private final int bitWidth;
    private final int byteWidthOfValue; // ceil(bitWidth / 8), for an RLE run's repeated value
    private final int mask;

    // Optional fast group unpacker (parquet-java's BytePacker) over the same bytes as a byte[]; null
    // means use the built-in scalar unpack. `pageArray` is `page` as an array (segment offset == array
    // index) so the unpacker reads a whole group at `pos` directly.
    private final GroupUnpacker unpacker;
    private final byte[] pageArray;

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
        this(page, null, null, offset, length, bitWidth);
    }

    /**
     * A reader over a {@code byte[]} with an injected fast group unpacker
     * (parquet-java's {@code BytePacker}). The array is wrapped as a segment for
     * the header, RLE-value and scalar paths; whole bit-packed groups in
     * {@link #readInts} go through {@code unpacker}. A {@code null} unpacker
     * falls back to the built-in scalar unpack (identical result).
     */
    public static RleBitPackingReader overArray(byte[] page, int offset, int length,
            int bitWidth, GroupUnpacker unpacker) {
        return new RleBitPackingReader(MemorySegment.ofArray(page), page, unpacker, offset, length,
                bitWidth);
    }

    private RleBitPackingReader(MemorySegment page, byte[] pageArray, GroupUnpacker unpacker,
            long offset, long length, int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("bit width out of range: " + bitWidth);
        }
        this.page = page;
        this.pageArray = pageArray;
        this.unpacker = unpacker;
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

    /**
     * Reads {@code n} values into {@code dst[off .. off+n)}. The batch path of
     * {@link #readInt}: RLE runs are a bulk {@link java.util.Arrays#fill}, and
     * bit-packed groups are unpacked eight at a time straight into {@code dst}
     * (no per-value dispatch, no {@code packed}/{@code packedIndex} bookkeeping),
     * so a call costs one word read per 64 bits of packed data plus the masks,
     * as parquet-java's generated {@code BytePacker} does. Leaves the reader
     * positioned exactly as {@code n} calls to {@link #readInt} would.
     */
    public void readInts(int[] dst, int off, int n) {
        if (bitWidth == 0) {
            java.util.Arrays.fill(dst, off, off + n, 0);
            return;
        }
        int remaining = n;
        int o = off;
        // Drain any partial bit-packed group left in `packed` first (from an earlier readInt).
        while (remaining > 0 && packedRemaining > 0 && packedIndex < 8) {
            dst[o++] = packed[packedIndex++];
            packedRemaining--;
            remaining--;
        }
        while (remaining > 0) {
            if (rleRemaining == 0 && packedRemaining == 0) {
                readRunHeader();
            }
            if (rleRemaining > 0) {
                int take = Math.min(remaining, rleRemaining);
                java.util.Arrays.fill(dst, o, o + take, rleValue);
                o += take;
                rleRemaining -= take;
                remaining -= take;
            } else {
                // Whole groups of 8 straight into dst.
                while (remaining >= 8 && packedRemaining >= 8) {
                    unpackGroup(dst, o);
                    o += 8;
                    packedRemaining -= 8;
                    remaining -= 8;
                }
                if (remaining > 0 && packedRemaining > 0) {
                    // A tail shorter than 8, or fewer than 8 left in the run: fall back to the buffered group.
                    refillPackedGroup();
                    int take = Math.min(remaining, Math.min(8, packedRemaining));
                    for (int i = 0; i < take; i++) {
                        dst[o++] = packed[packedIndex++];
                    }
                    packedRemaining -= take;
                    remaining -= take;
                }
            }
        }
    }

    /**
     * The width-1 batch path, for BOOLEAN pages: ORs the next {@code n} values
     * into the LSB-first bitmap {@code words} at bits {@code [dstBit, dstBit +
     * n)}, which must be zero. An RLE run of 1s sets its bit range a word at a
     * time (a run of 0s writes nothing), and a bit-packed run at width 1 is
     * already such a bitmap -- one byte per group of 8 -- so it moves 64 bits
     * per step instead of one value per {@code int}. Leaves the reader
     * positioned exactly as {@code n} calls to {@link #readInt} would.
     */
    public void readBits(long[] words, int dstBit, int n) {
        if (bitWidth != 1) {
            throw new IllegalStateException("readBits needs bit width 1, not " + bitWidth);
        }
        int remaining = n;
        int d = dstBit;
        // Drain any partial bit-packed group left in `packed` first (from an earlier readInt).
        while (remaining > 0 && packedRemaining > 0 && packedIndex < 8) {
            words[d >>> 6] |= (long) packed[packedIndex++] << (d & 63);
            d++;
            packedRemaining--;
            remaining--;
        }
        while (remaining > 0) {
            if (rleRemaining == 0 && packedRemaining == 0) {
                readRunHeader();
            }
            if (rleRemaining > 0) {
                int take = Math.min(remaining, rleRemaining);
                if ((rleValue & 1) != 0) {
                    setBits(words, d, take);
                }
                d += take;
                rleRemaining -= take;
                remaining -= take;
            } else {
                // Whole groups: each is one byte of 8 values, LSB first -- copy up to 8 bytes (64 values) a step.
                int bytes = Math.min(remaining, packedRemaining) >>> 3;
                while (bytes > 0) {
                    int k = Math.min(bytes, 8);
                    if (pos + k > end) {
                        throw new IllegalStateException("RLE/bit-packed stream ended inside a bit-packed run");
                    }
                    long v = loadLittleEndian(pos, k);
                    int bits = k << 3;
                    int w = d >>> 6;
                    int s = d & 63;
                    words[w] |= v << s;
                    if (s != 0 && s + bits > 64) {
                        words[w + 1] |= v >>> (64 - s);
                    }
                    pos += k;
                    d += bits;
                    bytes -= k;
                    packedRemaining -= bits;
                    remaining -= bits;
                }
                if (remaining > 0 && packedRemaining > 0) {
                    // A tail shorter than a group: fall back to the buffered group.
                    refillPackedGroup();
                    int take = Math.min(remaining, Math.min(8, packedRemaining));
                    for (int i = 0; i < take; i++) {
                        words[d >>> 6] |= (long) packed[packedIndex++] << (d & 63);
                        d++;
                    }
                    packedRemaining -= take;
                    remaining -= take;
                }
            }
        }
    }

    /**
     * Sets bits {@code [from, from + len)} of the LSB-first bitmap {@code
     * words}; {@code len > 0}.
     */
    private static void setBits(long[] words, int from, int len) {
        int last = from + len - 1;
        int w0 = from >>> 6;
        int w1 = last >>> 6;
        long head = -1L << (from & 63);
        long tail = -1L >>> (63 - (last & 63));
        if (w0 == w1) {
            words[w0] |= head & tail;
            return;
        }
        words[w0] |= head;
        java.util.Arrays.fill(words, w0 + 1, w1, -1L);
        words[w1] |= tail;
    }

    /**
     * {@code k} (1..8) little-endian bytes at {@code at}, as the low bytes of a
     * long.
     */
    private long loadLittleEndian(long at, int k) {
        if (k == 8 && at + 8 <= page.byteSize()) {
            return page.get(LE_LONG, at);
        }
        long v = 0L;
        for (int i = 0; i < k; i++) {
            v |= ((long) (page.get(BYTE, at + i) & 0xFF)) << (8 * i);
        }
        return v;
    }

    /**
     * Unpacks the 8 values of one bit-packed group at {@code pos} directly into
     * {@code dst[o .. o+8)} and advances {@code pos} by {@code bitWidth} bytes.
     * For {@code bitWidth <= 8} the whole group is {@code <= 64} bits, so it is
     * read as one little-endian {@code long} and the eight values fall out with
     * shifts -- no per-byte inner loop.
     */
    private void unpackGroup(int[] dst, int o) {
        long m = mask & 0xFFFFFFFFL;
        if (unpacker != null) {
            // parquet-java's generated BytePacker over the byte[] at the current position.
            unpacker.unpack8(pageArray, (int) pos, dst, o);
            pos += bitWidth;
            return;
        }
        if (bitWidth <= 8) {
            int nbytes = bitWidth; // 8 * bitWidth bits / 8
            long w = 0;
            for (int b = 0; b < nbytes; b++) {
                w |= ((long) (page.get(BYTE, pos + b) & 0xFF)) << (8 * b);
            }
            pos += nbytes;
            dst[o] = (int) (w & m);
            dst[o + 1] = (int) ((w >>> bitWidth) & m);
            dst[o + 2] = (int) ((w >>> (2 * bitWidth)) & m);
            dst[o + 3] = (int) ((w >>> (3 * bitWidth)) & m);
            dst[o + 4] = (int) ((w >>> (4 * bitWidth)) & m);
            dst[o + 5] = (int) ((w >>> (5 * bitWidth)) & m);
            dst[o + 6] = (int) ((w >>> (6 * bitWidth)) & m);
            dst[o + 7] = (int) ((w >>> (7 * bitWidth)) & m);
            return;
        }
        long bitBuffer = 0;
        int bitsInBuffer = 0;
        for (int i = 0; i < 8; i++) {
            while (bitsInBuffer < bitWidth) {
                int b = page.get(BYTE, pos++) & 0xFF;
                bitBuffer |= ((long) b) << bitsInBuffer;
                bitsInBuffer += 8;
            }
            dst[o + i] = (int) (bitBuffer & m);
            bitBuffer >>>= bitWidth;
            bitsInBuffer -= bitWidth;
        }
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
