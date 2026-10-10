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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import io.vecruntime.kernels.Species;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * The {@code BYTE_STREAM_SPLIT} transpose (#559): {@code n} values of {@code
 * K} bytes stored as K streams of {@code stride} bytes each, byte {@code j} of
 * value {@code i} at {@code start + j * stride + i}. Decoding gathers each
 * value's K bytes back together, a K x n byte-matrix transpose.
 *
 * <p>Three implementations of the same function, kept side by side so a
 * benchmark can pick one: a scalar byte gather (the reference), a Vector API
 * version that widens a run of bytes from each stream into long/int lanes,
 * shifts and ORs them together, and a SWAR version that loads eight bytes of
 * each stream as one {@code long} and transposes the 8x8 (or 4x8) byte block
 * in registers with three mask-and-shift stages. Every version produces the
 * same values; the tests compare them over every length and offset.
 */
public final class ByteStreamSplitKernels {

    private static final VarHandle BA_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle BA_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final VectorSpecies<Long> L = Species.L;
    private static final VectorSpecies<Integer> I = Species.I;

    /**
     * Bytes species with as many lanes as {@link #L}, widened by {@code B2L}
     * part 0; null when no such shape.
     */
    private static final VectorSpecies<Byte> BL = byteSpecies(L.length());

    private static final VectorSpecies<Byte> BI = byteSpecies(I.length());

    /**
     * Which transpose {@link #longs} / {@link #ints} run: {@code auto}
     * (default), {@code vector}, {@code swar} or {@code scalar}. Read once; the
     * property is for A/B runs. {@code auto} takes the Vector API for 4-byte
     * values at every width and for 8-byte values from 256 bits up, and SWAR
     * for 8-byte values on 128-bit vectors (NEON, SVE at 128 on Graviton4),
     * where widening a byte vector into two long lanes is not intrinsified and
     * ran ~30x slower than SWAR (#559: Graviton4 0.45 against 31 ops/ms; x86 at
     * -Dvecruntime.vectorBits=128 the same).
     */
    static final String MODE = System.getProperty("vecruntime.parquet.bssMode", "auto");

    /**
     * {@code auto}: the Vector API long transpose only when a register holds at
     * least 4 longs.
     */
    private static final boolean AUTO_LONG_VECTOR = Species.L.length() >= 4;

    private ByteStreamSplitKernels() {}

    /**
     * Values {@code from .. from+n} of an 8-stream split into {@code
     * dst[0..n)}, with the configured transpose.
     */
    public static void longs(byte[] src, int start, int stride,
            int from, long[] dst, int n) {
        switch (MODE) {
            case "scalar" -> longsScalar(src, start, stride, from, dst, n);
            case "swar" -> longsSwar(src, start, stride, from, dst, n);
            case "vector" -> longsVector(src, start, stride, from, dst, n);
            default -> {
                if (AUTO_LONG_VECTOR) {
                    longsVector(src, start, stride, from, dst, n);
                } else {
                    longsSwar(src, start, stride, from, dst, n);
                }
            }
        }
    }

    /**
     * Values {@code from .. from+n} of a 4-stream split into {@code dst[0..n)},
     * with the configured transpose.
     */
    public static void ints(byte[] src, int start, int stride,
                            int from, int[] dst, int n) {
        switch (MODE) {
            case "scalar" -> intsScalar(src, start, stride, from, dst, n);
            case "swar" -> intsSwar(src, start, stride, from, dst, n);
            default -> intsVector(src, start, stride, from, dst, n);
        }
    }

    private static VectorSpecies<Byte> byteSpecies(int lanes) {
        int bits = lanes * 8;
        if (bits < 64) {
            return ByteVector.SPECIES_64; // widen part 0 of an 8-byte vector: the first `lanes` bytes
        }
        return VectorSpecies.of(byte.class, jdk.incubator.vector.VectorShape.forBitSize(bits));
    }

    // ------------------------------------------------------------------ scalar reference

    /** Values {@code from .. from+n} of an 8-stream split into {@code dst[0..n)}. */
    public static void longsScalar(byte[] src, int start, int stride,
            int from, long[] dst, int n) {
        int b0 = start + from;
        for (int k = 0; k < n; k++) {
            long v = 0;
            int p = b0 + k;
            for (int j = 0; j < 8; j++) {
                v |= ((long) (src[p] & 0xFF)) << (8 * j);
                p += stride;
            }
            dst[k] = v;
        }
    }

    /** Values {@code from .. from+n} of a 4-stream split into {@code dst[0..n)}. */
    public static void intsScalar(byte[] src, int start, int stride,
            int from, int[] dst, int n) {
        int b0 = start + from;
        int b1 = b0 + stride;
        int b2 = b1 + stride;
        int b3 = b2 + stride;
        for (int k = 0; k < n; k++) {
            dst[k] = (src[b0 + k] & 0xFF)
                     | (src[b1 + k] & 0xFF) << 8
                     | (src[b2 + k] & 0xFF) << 16
                     | src[b3 + k] << 24;
        }
    }

    // ------------------------------------------------------------------ Vector API

    /**
     * 8-stream split, Vector API: per {@code L.length()} values, one byte vector
     * from each stream widened to long lanes, masked, shifted by its byte
     * position and ORed in. The tail is scalar.
     */
    public static void longsVector(byte[] src, int start, int stride,
            int from, long[] dst, int n) {
        int lanes = L.length();
        int bytesPerLoad = BL.length();
        int b0 = start + from;
        int k = 0;
        // A load of bytesPerLoad bytes at stream 7 must stay inside src.
        int last = b0 + 7 * stride;
        int bound = n - Math.max(lanes, bytesPerLoad);
        for (; k <= bound && last + k + bytesPerLoad <= src.length; k += lanes) {
            LongVector acc = LongVector.zero(L);
            int p = b0 + k;
            for (int j = 0; j < 8; j++) {
                LongVector w = (LongVector) ByteVector.fromArray(BL, src, p).convertShape(VectorOperators.B2L, L, 0);
                acc = acc.or(w.and(0xFFL)
                              .lanewise(VectorOperators.LSHL, 8L * j));
                p += stride;
            }
            acc.intoArray(dst, k);
        }
        if (k < n) {
            longsScalar(src, start, stride, from + k, dst,
                    k, n - k);
        }
    }

    /** 4-stream split, Vector API; see {@link #longsVector}. */
    public static void intsVector(byte[] src, int start, int stride,
            int from, int[] dst, int n) {
        int lanes = I.length();
        int bytesPerLoad = BI.length();
        int b0 = start + from;
        int k = 0;
        int last = b0 + 3 * stride;
        int bound = n - Math.max(lanes, bytesPerLoad);
        for (; k <= bound && last + k + bytesPerLoad <= src.length; k += lanes) {
            int p = b0 + k;
            IntVector v0 = (IntVector) ByteVector.fromArray(BI, src, p).convertShape(VectorOperators.B2I, I, 0);
            IntVector v1 = (IntVector) ByteVector.fromArray(BI, src, p + stride).convertShape(VectorOperators.B2I, I, 0);
            IntVector v2 = (IntVector) ByteVector.fromArray(BI, src, p + 2 * stride).convertShape(VectorOperators.B2I, I, 0);
            IntVector v3 = (IntVector) ByteVector.fromArray(BI, src, p + 3 * stride).convertShape(VectorOperators.B2I, I, 0);
            IntVector acc = v0.and(0xFF)
                              .or(v1.and(0xFF).lanewise(VectorOperators.LSHL, 8))
                              .or(v2.and(0xFF).lanewise(VectorOperators.LSHL, 16))
                              .or(v3.lanewise(VectorOperators.LSHL, 24));
            acc.intoArray(dst, k);
        }
        if (k < n) {
            intsScalar(src, start, stride, from + k, dst,
                    k, n - k);
        }
    }

    // ------------------------------------------------------------------ SWAR

    /**
     * 8-stream split, SWAR: eight values at a time. Row {@code j} is the
     * little-endian long of stream {@code j}'s next eight bytes; the 8x8 byte
     * transpose (three stages of swapping 1-, 2- and 4-byte blocks across
     * rows) turns row {@code i} into value {@code i}. The tail is scalar.
     */
    public static void longsSwar(byte[] src, int start, int stride,
            int from, long[] dst, int n) {
        int b0 = start + from;
        int k = 0;
        int last = b0 + 7 * stride;
        for (; k + 8 <= n && last + k + 8 <= src.length; k += 8) {
            int p = b0 + k;
            long r0 = (long) BA_LONG.get(src, p);
            long r1 = (long) BA_LONG.get(src, p + stride);
            long r2 = (long) BA_LONG.get(src, p + 2 * stride);
            long r3 = (long) BA_LONG.get(src, p + 3 * stride);
            long r4 = (long) BA_LONG.get(src, p + 4 * stride);
            long r5 = (long) BA_LONG.get(src, p + 5 * stride);
            long r6 = (long) BA_LONG.get(src, p + 6 * stride);
            long r7 = (long) BA_LONG.get(src, p + 7 * stride);
            // Stage 1: swap single bytes between row pairs (0,1), (2,3), ...
            long m1 = 0x00FF00FF00FF00FFL;
            long t;
            t = ((r0 >>> 8) ^ r1) & m1;
            r1 ^= t;
            r0 ^= t << 8;
            t = ((r2 >>> 8) ^ r3) & m1;
            r3 ^= t;
            r2 ^= t << 8;
            t = ((r4 >>> 8) ^ r5) & m1;
            r5 ^= t;
            r4 ^= t << 8;
            t = ((r6 >>> 8) ^ r7) & m1;
            r7 ^= t;
            r6 ^= t << 8;
            // Stage 2: swap 2-byte blocks between rows (0,2), (1,3), (4,6), (5,7).
            long m2 = 0x0000FFFF0000FFFFL;
            t = ((r0 >>> 16) ^ r2) & m2;
            r2 ^= t;
            r0 ^= t << 16;
            t = ((r1 >>> 16) ^ r3) & m2;
            r3 ^= t;
            r1 ^= t << 16;
            t = ((r4 >>> 16) ^ r6) & m2;
            r6 ^= t;
            r4 ^= t << 16;
            t = ((r5 >>> 16) ^ r7) & m2;
            r7 ^= t;
            r5 ^= t << 16;
            // Stage 3: swap 4-byte blocks between rows (0,4), (1,5), (2,6), (3,7).
            long m3 = 0x00000000FFFFFFFFL;
            t = ((r0 >>> 32) ^ r4) & m3;
            r4 ^= t;
            r0 ^= t << 32;
            t = ((r1 >>> 32) ^ r5) & m3;
            r5 ^= t;
            r1 ^= t << 32;
            t = ((r2 >>> 32) ^ r6) & m3;
            r6 ^= t;
            r2 ^= t << 32;
            t = ((r3 >>> 32) ^ r7) & m3;
            r7 ^= t;
            r3 ^= t << 32;
            dst[k] = r0;
            dst[k + 1] = r1;
            dst[k + 2] = r2;
            dst[k + 3] = r3;
            dst[k + 4] = r4;
            dst[k + 5] = r5;
            dst[k + 6] = r6;
            dst[k + 7] = r7;
        }
        if (k < n) {
            longsScalar(src, start, stride, from + k, dst,
                    k, n - k);
        }
    }

    /**
     * 4-stream split, SWAR: four values at a time from one int per stream, a
     * 4x4 byte transpose in two stages.
     */
    public static void intsSwar(byte[] src, int start, int stride,
            int from, int[] dst, int n) {
        int b0 = start + from;
        int k = 0;
        int last = b0 + 3 * stride;
        for (; k + 4 <= n && last + k + 4 <= src.length; k += 4) {
            int p = b0 + k;
            int r0 = (int) BA_INT.get(src, p);
            int r1 = (int) BA_INT.get(src, p + stride);
            int r2 = (int) BA_INT.get(src, p + 2 * stride);
            int r3 = (int) BA_INT.get(src, p + 3 * stride);
            int m1 = 0x00FF00FF;
            int t;
            t = ((r0 >>> 8) ^ r1) & m1;
            r1 ^= t;
            r0 ^= t << 8;
            t = ((r2 >>> 8) ^ r3) & m1;
            r3 ^= t;
            r2 ^= t << 8;
            int m2 = 0x0000FFFF;
            t = ((r0 >>> 16) ^ r2) & m2;
            r2 ^= t;
            r0 ^= t << 16;
            t = ((r1 >>> 16) ^ r3) & m2;
            r3 ^= t;
            r1 ^= t << 16;
            dst[k] = r0;
            dst[k + 1] = r1;
            dst[k + 2] = r2;
            dst[k + 3] = r3;
        }
        if (k < n) {
            intsScalar(src, start, stride, from + k, dst,
                    k, n - k);
        }
    }

    // ------------------------------------------------------------------ tails at a destination offset

    private static void longsScalar(
            byte[] src,
            int start,
            int stride,
            int from,
            long[] dst,
            int dstOff,
            int n) {
        int b0 = start + from;
        for (int k = 0; k < n; k++) {
            long v = 0;
            int p = b0 + k;
            for (int j = 0; j < 8; j++) {
                v |= ((long) (src[p] & 0xFF)) << (8 * j);
                p += stride;
            }
            dst[dstOff + k] = v;
        }
    }

    private static void intsScalar(
            byte[] src,
            int start,
            int stride,
            int from,
            int[] dst,
            int dstOff,
            int n) {
        int b0 = start + from;
        int b1 = b0 + stride;
        int b2 = b1 + stride;
        int b3 = b2 + stride;
        for (int k = 0; k < n; k++) {
            dst[dstOff + k] = (src[b0 + k] & 0xFF)
                              | (src[b1 + k] & 0xFF) << 8
                              | (src[b2 + k] & 0xFF) << 16
                              | src[b3 + k] << 24;
        }
    }
}
