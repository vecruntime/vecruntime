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

import java.util.function.IntFunction;

/**
 * Resumable reader of a Parquet {@code DELTA_BINARY_PACKED} value stream over a
 * {@code byte[]}, for INT32 and INT64 columns (#559 slice 2).
 *
 * <h2>Format</h2>
 * A header of four varints -- block size in values, miniblocks per block, the
 * total value count and the first value (zigzag) -- then blocks. Each block is a
 * zigzag varint minimum delta, one bit-width byte per miniblock, then the
 * miniblocks: {@code blockSize / miniblocks} deltas each (a multiple of 32), bit
 * packed LSB-first at that miniblock's width, the same packing as the
 * RLE/bit-packed hybrid's groups. Value {@code i} is value {@code i - 1} plus the
 * minimum delta plus the unpacked delta. The writer computes the deltas in the
 * column's own width, wrapping, so the reader adds in that width too: INT32 in
 * {@code int}, INT64 in {@code long}. Only the miniblocks that hold values are
 * written; the last of them is padded to its full size by the writer, but this
 * reader does not rely on that and unpacks a truncated tail from a zero-padded copy.
 *
 * <h2>Resumable</h2>
 * A batch may stop anywhere inside a miniblock, so the reader decodes one
 * miniblock at a time into a small buffer and hands values out of it; {@link
 * #readInts} / {@link #readLongs} pick up exactly where the previous call left off.
 *
 * <h2>Unpacking</h2>
 * Widths up to 32 unpack eight at a time through the injected {@link GroupUnpacker}
 * (parquet-java's {@code BytePacker} on the scan path; {@code null} falls back to
 * the built-in scalar unpack). INT64 widths above 32 use the scalar long unpack.
 */
public final class DeltaBinaryPackedReader {

    private final byte[] src;
    private final int end;
    private final boolean int64;
    private final IntFunction<GroupUnpacker> unpackers;

    private final int miniblocks;
    private final int perMini;
    private int pos;
    private int remaining; // values not yet handed out (including a pending first value)
    private boolean firstPending = true;
    private long last; // the last value decoded (INT32: sign-extended int)

    private long minDelta;
    private final int[] widths;
    private int miniIndex; // next miniblock within the current block; == miniblocks => read a block header

    private final long[] values; // decoded values of the current miniblock (or the first value)
    private int valuePos;
    private int valueLen;
    private final int[] deltas32; // unpack scratch for widths <= 32
    private byte[] padded = new byte[0]; // zero-padded copy of a truncated last miniblock

    /**
     * A reader of the stream at {@code src[offset .. offset + length)}. {@code
     * int64} selects 64-bit arithmetic (INT64 columns); otherwise deltas wrap
     * in 32 bits. {@code unpackers} returns the fast group unpacker for a width
     * {@code 1..32}, or {@code null} for the scalar one; it may itself be
     * {@code null}.
     */
    public DeltaBinaryPackedReader(byte[] src, int offset, int length,
            boolean int64, IntFunction<GroupUnpacker> unpackers) {
        this.src = src;
        this.pos = offset;
        this.end = offset + length;
        this.int64 = int64;
        this.unpackers = unpackers;
        int blockSize = readUleb32("block size");
        this.miniblocks = readUleb32("miniblock count");
        if (miniblocks <= 0
                || blockSize <= 0
                || blockSize % miniblocks != 0
                || (blockSize / miniblocks) % 8 != 0) {
            throw new IllegalStateException("DELTA_BINARY_PACKED: bad block layout " + blockSize + "/" + miniblocks);
        }
        this.perMini = blockSize / miniblocks;
        this.remaining = readUleb32("value count");
        long first = zigzag(readUleb64());
        this.last = int64 ? first : (int) first;
        this.widths = new int[miniblocks];
        this.miniIndex = miniblocks;
        this.values = new long[Math.max(perMini, 1)];
        this.deltas32 = new int[perMini];
    }

    /**
     * Once every value has been read, the offset just past the stream: where a
     * following region starts (the bytes of {@code DELTA_LENGTH_BYTE_ARRAY}). The
     * spec pads the last miniblock to its full size, so this is exact for
     * conforming writers.
     */
    public int position() {
        if (remaining != 0) {
            throw new IllegalStateException("DELTA_BINARY_PACKED: position() with " + remaining + " values unread");
        }
        return pos;
    }

    /** Total values in the stream not yet read. */
    public int remaining() {
        return remaining;
    }

    /**
     * Reads the next {@code n} values into {@code dst[off .. off+n)} (INT32
     * streams).
     */
    public void readInts(int[] dst, int off, int n) {
        int o = off;
        int left = checkAvailable(n);
        while (left > 0) {
            if (valuePos == valueLen) {
                fill();
            }
            int take = Math.min(left, valueLen - valuePos);
            long[] v = values;
            int p = valuePos;
            for (int k = 0; k < take; k++) {
                dst[o + k] = (int) v[p + k];
            }
            valuePos += take;
            o += take;
            left -= take;
        }
        remaining -= n;
    }

    /**
     * Reads the next {@code n} values into {@code dst[off .. off+n)} (INT64
     * streams, or INT32 widened).
     */
    public void readLongs(long[] dst, int off, int n) {
        int o = off;
        int left = checkAvailable(n);
        while (left > 0) {
            if (valuePos == valueLen) {
                fill();
            }
            int take = Math.min(left, valueLen - valuePos);
            System.arraycopy(values, valuePos, dst, o, take);
            valuePos += take;
            o += take;
            left -= take;
        }
        remaining -= n;
    }

    private int checkAvailable(int n) {
        if (n > remaining) {
            throw new IllegalStateException("DELTA_BINARY_PACKED: " + n + " values requested, " + remaining + " left");
        }
        return n;
    }

    /**
     * Decodes the next miniblock (or hands out the header's first value) into
     * {@link #values}.
     */
    private void fill() {
        valuePos = 0;
        if (firstPending) {
            firstPending = false;
            values[0] = last;
            valueLen = 1;
            return;
        }
        if (miniIndex == miniblocks) {
            minDelta = zigzag(readUleb64());
            if (pos + miniblocks > end) {
                throw new IllegalStateException("DELTA_BINARY_PACKED: block header past the end of the page");
            }
            for (int i = 0; i < miniblocks; i++) {
                widths[i] = src[pos + i] & 0xFF;
            }
            pos += miniblocks;
            miniIndex = 0;
        }
        int width = widths[miniIndex++];
        int limit = int64 ? 64 : 32;
        if (width > limit) {
            throw new IllegalStateException("DELTA_BINARY_PACKED: miniblock width " + width + " > " + limit);
        }
        int bytes = width * perMini / 8;
        byte[] in = src;
        int at = pos;
        if (pos + bytes + 8 > src.length || pos + bytes > end) {
            // A truncated last miniblock, or one too close to the array's end for the 8-byte reads:
            // unpack from a zero-padded copy.
            if (padded.length < bytes + 8) {
                padded = new byte[bytes + 8];
            }
            int avail = Math.max(0, Math.min(bytes, end - pos));
            System.arraycopy(src, pos, padded, 0, avail);
            java.util.Arrays.fill(padded, avail, bytes + 8, (byte) 0);
            in = padded;
            at = 0;
        }
        pos += bytes;
        int count = perMini;
        long[] v = values;
        long prev = last;
        if (int64) {
            long md = minDelta;
            if (width <= 32) {
                unpack32(in, at, width);
                int[] d = deltas32;
                for (int k = 0; k < count; k++) {
                    prev = prev + md + (d[k] & 0xFFFFFFFFL);
                    v[k] = prev;
                }
            } else {
                for (int k = 0; k < count; k++) {
                    prev = prev + md + unpackLong(in, at, k, width);
                    v[k] = prev;
                }
            }
        } else {
            int md = (int) minDelta;
            int p = (int) prev;
            unpack32(in, at, width);
            int[] d = deltas32;
            for (int k = 0; k < count; k++) {
                p = p + md + d[k];
                v[k] = p;
            }
            prev = p;
        }
        last = prev;
        valueLen = count;
    }

    /**
     * Unpacks {@link #perMini} values of {@code width <= 32} bits at {@code
     * in[at ..]} into {@link #deltas32}.
     */
    private void unpack32(byte[] in, int at, int width) {
        int[] d = deltas32;
        if (width == 0) {
            java.util.Arrays.fill(d, 0, perMini, 0);
            return;
        }
        GroupUnpacker u = unpackers == null ? null : unpackers.apply(width);
        if (u != null) {
            for (int g = 0; g < perMini; g += 8) {
                u.unpack8(in, at + (g / 8) * width, d, g);
            }
            return;
        }
        for (int k = 0; k < perMini; k++) {
            d[k] = (int) unpackLong(in, at, k, width);
        }
    }

    /**
     * The {@code k}-th value of {@code width} bits (1..64), LSB-first, from
     * {@code in[at ..]}; needs 8 readable bytes past it.
     */
    private static long unpackLong(byte[] in, int at, int k,
            int width) {
        long bit = (long) k * width;
        int b = at + (int) (bit >>> 3);
        int shift = (int) (bit & 7);
        long lo = leLong(in, b) >>> shift;
        if (shift + width > 64) {
            lo |= ((long) (in[b + 8] & 0xFF)) << (64 - shift);
        }
        return width == 64 ? lo : lo & ((1L << width) - 1);
    }

    // Little-endian long view over byte[]: one unaligned load. fill() guarantees 8 readable bytes past
    // the miniblock (or switches to the zero-padded copy), so b + 8 is always in bounds.
    private static final java.lang.invoke.VarHandle BA_LONG = java.lang.invoke.MethodHandles.byteArrayViewVarHandle(long[].class, java.nio.ByteOrder.LITTLE_ENDIAN);

    private static long leLong(byte[] a, int o) {
        return (long) BA_LONG.get(a, o);
    }

    private int readUleb32(String what) {
        long v = readUleb64();
        if (v < 0 || v > Integer.MAX_VALUE) {
            throw new IllegalStateException("DELTA_BINARY_PACKED: " + what + " out of range: " + v);
        }
        return (int) v;
    }

    private long readUleb64() {
        long value = 0;
        int shift = 0;
        while (true) {
            if (pos >= end) {
                throw new IllegalStateException("DELTA_BINARY_PACKED: stream ended inside a varint");
            }
            int b = src[pos++] & 0xFF;
            if (shift < 64) {
                value |= ((long) (b & 0x7F)) << shift;
            }
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }

    private static long zigzag(long v) {
        return (v >>> 1) ^ -(v & 1);
    }
}
