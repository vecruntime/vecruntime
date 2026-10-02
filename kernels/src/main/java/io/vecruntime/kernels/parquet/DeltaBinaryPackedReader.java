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

import io.vecruntime.kernels.Species;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Resumable reader of a Parquet {@code DELTA_BINARY_PACKED} value stream over a
 * {@code byte[]}, for INT32 and INT64 columns (#559 slice 2).
 *
 * <h2>Format</h2>
 *
 * A header of four varints -- block size in values, miniblocks per block, the
 * total value count and the first value (zigzag) -- then blocks. Each block is
 * a zigzag varint minimum delta, one bit-width byte per miniblock, then the
 * miniblocks: {@code blockSize / miniblocks} deltas each (a multiple of 32),
 * bit packed LSB-first at that miniblock's width, the same packing as the
 * RLE/bit-packed hybrid's groups. Value {@code i} is value {@code i - 1} plus
 * the minimum delta plus the unpacked delta. The writer computes the deltas in
 * the column's own width, wrapping, so the reader adds in that width too: INT32
 * in {@code int}, INT64 in {@code long}. Only the miniblocks that hold values
 * are written; the last of them is padded to its full size by the writer, but
 * this reader does not rely on that and unpacks a truncated tail from a
 * zero-padded copy.
 *
 * <h2>Resumable</h2>
 *
 * A batch may stop anywhere inside a miniblock, so the reader decodes whole
 * miniblocks straight into the caller's array and only a miniblock a read stops
 * inside through a small buffer; {@link #readInts} / {@link #readLongs} pick up
 * exactly where the previous call left off.
 *
 * <h2>Unpacking</h2>
 *
 * Widths up to 32 unpack eight at a time through the injected {@link
 * GroupUnpacker} (parquet-java's {@code BytePacker} on the scan path; {@code
 * null} falls back to the built-in scalar unpack). INT64 widths above 32 use
 * the scalar long unpack. The INT32 prefix sum runs a log-step Vector API scan
 * per register.
 */
public final class DeltaBinaryPackedReader {

    /**
     * Prefix sum over a miniblock: {@code scalar} (default) the serial add,
     * {@code vector} a log-step Vector API scan per register. On x86 with
     * AVX-512 the two measured the same (JMH, #559), so the simpler one is the
     * default; {@code -Dvecruntime.parquet.deltaScan=vector} is for A/B runs,
     * e.g. on Graviton4's 128-bit NEON / SVE.
     */
    static final boolean VECTOR_SCAN = "vector".equals(System.getProperty("vecruntime.parquet.deltaScan", "scalar"));

    private static final VectorSpecies<Integer> I = Species.I;

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

    // A miniblock decoded but only partly handed out (a read that stops inside one): INT32 streams use
    // intBuf, INT64 streams longBuf. A read of whole miniblocks decodes straight into the caller's array.
    private final int[] intBuf;
    private final long[] longBuf;
    private int valuePos;
    private int valueLen;
    private final int[] deltas32; // INT64 unpack scratch for widths <= 32
    private byte[] padded = new byte[0]; // zero-padded copy of a truncated last miniblock

    // The miniblock prepared by nextMiniblock(): its bytes and width.
    private byte[] curIn;
    private int curAt;
    private int curWidth;

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
        this.intBuf = int64 ? null : new int[perMini];
        this.longBuf = int64 ? new long[perMini] : null;
        this.deltas32 = int64 ? new int[perMini] : null;
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
     * streams). Whole miniblocks are unpacked and summed in place in {@code
     * dst}; only a miniblock a read stops inside goes through the buffer.
     */
    public void readInts(int[] dst, int off, int n) {
        if (int64) {
            throw new IllegalStateException("DELTA_BINARY_PACKED: readInts on an INT64 stream");
        }
        int o = off;
        int left = checkAvailable(n);
        while (left > 0) {
            if (valuePos < valueLen) {
                int take = Math.min(left, valueLen - valuePos);
                System.arraycopy(intBuf, valuePos, dst, o, take);
                valuePos += take;
                o += take;
                left -= take;
            } else if (firstPending) {
                firstPending = false;
                dst[o++] = (int) last;
                left--;
            } else if (left >= perMini) {
                decodeInts(dst, o);
                o += perMini;
                left -= perMini;
            } else {
                decodeInts(intBuf, 0);
                valuePos = 0;
                valueLen = perMini;
            }
        }
        remaining -= n;
    }

    /**
     * Reads the next {@code n} values into {@code dst[off .. off+n)}: an INT64
     * stream, decoded as {@link #readInts} does, or an INT32 stream widened
     * (sign-extended) to long.
     */
    public void readLongs(long[] dst, int off, int n) {
        int o = off;
        int left = checkAvailable(n);
        while (left > 0) {
            if (valuePos < valueLen) {
                int take = Math.min(left, valueLen - valuePos);
                if (int64) {
                    System.arraycopy(longBuf, valuePos, dst, o, take);
                } else {
                    int[] b = intBuf;
                    int p = valuePos;
                    for (int k = 0; k < take; k++) {
                        dst[o + k] = b[p + k];
                    }
                }
                valuePos += take;
                o += take;
                left -= take;
            } else if (firstPending) {
                firstPending = false;
                dst[o++] = last;
                left--;
            } else if (int64 && left >= perMini) {
                decodeLongs(dst, o);
                o += perMini;
                left -= perMini;
            } else {
                if (int64) {
                    decodeLongs(longBuf, 0);
                } else {
                    decodeInts(intBuf, 0);
                }
                valuePos = 0;
                valueLen = perMini;
            }
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
     * Positions on the next miniblock: reads a block header when one is due,
     * checks the width, and leaves its bytes in {@link #curIn} at {@link
     * #curAt} (a zero-padded copy for a truncated tail or one too close to the
     * array's end for the 8-byte reads).
     */
    private void nextMiniblock() {
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
        curIn = src;
        curAt = pos;
        curWidth = width;
        if (pos + bytes + 8 > src.length || pos + bytes > end) {
            if (padded.length < bytes + 8) {
                padded = new byte[bytes + 8];
            }
            int avail = Math.max(0, Math.min(bytes, end - pos));
            System.arraycopy(src, pos, padded, 0, avail);
            java.util.Arrays.fill(padded, avail, bytes + 8, (byte) 0);
            curIn = padded;
            curAt = 0;
        }
        pos += bytes;
    }

    /**
     * The next miniblock of an INT32 stream: unpacked into {@code out[off ..]},
     * then prefix-summed in place.
     */
    private void decodeInts(int[] out, int off) {
        nextMiniblock();
        unpack32(curIn, curAt, curWidth, out, off);
        last = scanInts(out, off, perMini, (int) minDelta,
                (int) last);
    }

    /** The next miniblock of an INT64 stream into {@code out[off .. off+perMini)}. */
    private void decodeLongs(long[] out, int off) {
        nextMiniblock();
        long md = minDelta;
        long prev = last;
        int count = perMini;
        if (curWidth <= 32) {
            unpack32(curIn, curAt, curWidth, deltas32, 0);
            int[] d = deltas32;
            for (int k = 0; k < count; k++) {
                prev = prev + md + (d[k] & 0xFFFFFFFFL);
                out[off + k] = prev;
            }
        } else {
            byte[] in = curIn;
            int at = curAt;
            int w = curWidth;
            for (int k = 0; k < count; k++) {
                prev = prev + md + unpackLong(in, at, k, w);
                out[off + k] = prev;
            }
        }
        last = prev;
    }

    /**
     * In place: {@code a[off+k] = carry + sum_{i<=k} (a[off+i] + md)}, wrapping
     * in 32 bits. Returns the last value. The vector form adds the minimum delta,
     * runs a log-step inclusive scan inside each register ({@code v + (v << s
     * lanes)} for s = 1, 2, 4, ...), adds the running carry and takes the top lane
     * as the next carry; it computes the same wrapping sums in a different order,
     * which int addition makes identical.
     */
    static int scanInts(int[] a, int off, int n,
                        int md, int carry) {
        return VECTOR_SCAN ? scanIntsVector(a, off, n, md, carry) : scanIntsScalar(a, off, n, md, carry);
    }

    /** The serial reference of {@link #scanInts}. */
    static int scanIntsScalar(int[] a, int off, int n,
            int md, int carry) {
        for (int k = 0; k < n; k++) {
            carry = carry + md + a[off + k];
            a[off + k] = carry;
        }
        return carry;
    }

    /**
     * The vector form of {@link #scanInts}; the tail past the last full
     * register is scalar.
     */
    static int scanIntsVector(int[] a, int off, int n,
            int md, int carry) {
        int k = 0;
        {
            int lanes = I.length();
            for (; k + lanes <= n; k += lanes) {
                IntVector v = IntVector.fromArray(I, a, off + k).add(md);
                for (int s = 1; s < lanes; s <<= 1) {
                    v = v.add(v.unslice(s));
                }
                v = v.add(carry);
                v.intoArray(a, off + k);
                carry = v.lane(lanes - 1);
            }
        }
        for (; k < n; k++) {
            carry = carry + md + a[off + k];
            a[off + k] = carry;
        }
        return carry;
    }

    /**
     * Unpacks {@link #perMini} values of {@code width <= 32} bits at {@code
     * in[at ..]} into {@code out[off ..]}.
     */
    private void unpack32(byte[] in, int at, int width,
                          int[] out, int off) {
        if (width == 0) {
            java.util.Arrays.fill(out, off, off + perMini, 0);
            return;
        }
        GroupUnpacker u = unpackers == null ? null : unpackers.apply(width);
        if (u != null) {
            for (int g = 0; g < perMini; g += 8) {
                u.unpack8(in, at + (g / 8) * width, out, off + g);
            }
            return;
        }
        for (int k = 0; k < perMini; k++) {
            out[off + k] = (int) unpackLong(in, at, k, width);
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

    // Little-endian long view over byte[]: one unaligned load. nextMiniblock() guarantees 8 readable bytes past
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
