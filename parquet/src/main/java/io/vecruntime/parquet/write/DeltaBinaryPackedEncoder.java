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

import java.util.Arrays;

import io.vecruntime.kernels.Species;
import io.vecruntime.parquet.codec.ParquetFraming;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

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
public final class DeltaBinaryPackedEncoder {

    static final int BLOCK = ParquetFraming.DELTA_BLOCK_SIZE;
    static final int MINIBLOCKS = ParquetFraming.DELTA_MINIBLOCKS;
    static final int MINIBLOCK = ParquetFraming.DELTA_MINIBLOCK_SIZE;

    /**
     * The vector path is the default;
     * -Dvecruntime.parquet.write.scalarDelta=true selects the scalar loops (JMH
     * A/B).
     */
    static final boolean VECTOR_DEFAULT = !Boolean.getBoolean("vecruntime.parquet.write.scalarDelta");

    private static final VectorSpecies<Long> L = Species.L;

    private final long[] deltas = new long[BLOCK];
    private final int[] widths = new int[MINIBLOCKS];
    private final boolean vector;

    public DeltaBinaryPackedEncoder() {
        this(VECTOR_DEFAULT);
    }

    public DeltaBinaryPackedEncoder(boolean vector) {
        this.vector = vector;
    }

    /** Encodes {@code values[0, n)} as 32-bit integers (their low 32 bits). */
    public void encodeInts(long[] values, int n, ByteSink out) {
        header(
                n,
                n == 0 ? 0 : (int) values[0],
                out);
        int i = 1;
        while (i < n) {
            int count = Math.min(BLOCK, n - i);
            if (vector && count == BLOCK) {
                long min = vectorBlock(values, i, true);
                emit(count, ParquetFraming.zigzagEncode(min), out);
                i += count;
                continue;
            }
            int min = Integer.MAX_VALUE;
            for (int k = 0; k < count; k++) {
                int d = (int) values[i + k] - (int) values[i + k - 1];
                deltas[k] = d;
                min = Math.min(min, d);
            }
            for (int k = 0; k < count; k++) {
                deltas[k] = ((int) deltas[k] - min) & 0xFFFFFFFFL;
            }
            block(count, ParquetFraming.zigzagEncode(min), out);
            i += count;
        }
    }

    /** Encodes {@code values[0, n)} as 64-bit integers. */
    public void encodeLongs(long[] values, int n, ByteSink out) {
        header(n, n == 0 ? 0 : values[0], out);
        int i = 1;
        while (i < n) {
            int count = Math.min(BLOCK, n - i);
            if (vector && count == BLOCK) {
                long min = vectorBlock(values, i, false);
                emit(count, ParquetFraming.zigzagEncode(min), out);
                i += count;
                continue;
            }
            long min = Long.MAX_VALUE;
            for (int k = 0; k < count; k++) {
                long d = values[i + k] - values[i + k - 1];
                deltas[k] = d;
                min = Math.min(min, d);
            }
            for (int k = 0; k < count; k++) {
                deltas[k] -= min;
            }
            block(count, ParquetFraming.zigzagEncode(min), out);
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
            block(count, ParquetFraming.zigzagEncode(min), out);
            i += count;
        }
    }

    private static void header(int n, long first, ByteSink out) {
        writeVarLong(BLOCK, out);
        writeVarLong(MINIBLOCKS, out);
        writeVarLong(n, out);
        writeVarLong(ParquetFraming.zigzagEncode(first), out);
    }

    private static void writeVarLong(long v, ByteSink out) {
        while ((v & ~0x7FL) != 0) {
            out.writeByte((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.writeByte((int) v);
    }

    /**
     * A full block of 128 deltas with the Vector API: the deltas of {@code
     * values[i - 1, i + 128)}, their minimum, the deltas reduced by it, and
     * each miniblock's bit width from the OR of its reduced deltas. For INT32
     * ({@code int32}) the deltas are the 32-bit wrapping differences,
     * sign-extended, and the reduced deltas their unsigned 32-bit values, as
     * {@link #encodeInts(long[], int, ByteSink)} computes them. Returns the
     * minimum; {@link #deltas} and {@link #widths} hold the rest.
     */
    private long vectorBlock(long[] values, int i, boolean int32) {
        int lanes = L.length();
        LongVector minV = LongVector.broadcast(L, Long.MAX_VALUE);
        for (int k = 0; k < BLOCK; k += lanes) {
            LongVector d = LongVector.fromArray(L, values, i + k).sub(LongVector.fromArray(L, values, i + k - 1));
            if (int32) {
                d = d.lanewise(VectorOperators.LSHL, 32).lanewise(VectorOperators.ASHR, 32);
            }
            d.intoArray(deltas, k);
            minV = minV.min(d);
        }
        long min = minV.reduceLanes(VectorOperators.MIN);
        LongVector minB = LongVector.broadcast(L, min);
        for (int m = 0; m < MINIBLOCKS; m++) {
            LongVector or = LongVector.zero(L);
            for (int k = m * MINIBLOCK; k < (m + 1) * MINIBLOCK; k += lanes) {
                LongVector d = LongVector.fromArray(L, deltas, k).sub(minB);
                if (int32) {
                    d = d.and(0xFFFFFFFFL);
                }
                d.intoArray(deltas, k);
                or = or.or(d);
            }
            widths[m] = 64 - Long.numberOfLeadingZeros(or.reduceLanes(VectorOperators.OR));
        }
        return min;
    }

    /**
     * Writes a block whose reduced deltas and widths {@link #vectorBlock}
     * computed.
     */
    private void emit(int count, long zigzagMin, ByteSink out) {
        writeVarLong(zigzagMin, out);
        for (int m = 0; m < MINIBLOCKS; m++) {
            out.writeByte(widths[m]);
        }
        for (int m = 0; m < MINIBLOCKS; m++) {
            pack(m * MINIBLOCK, Math.min(count, (m + 1) * MINIBLOCK), widths[m], out);
        }
    }

    /** One block of {@code count} deltas already reduced by the block minimum. */
    private void block(int count, long zigzagMin, ByteSink out) {
        writeVarLong(zigzagMin, out);
        int used = (count + MINIBLOCK - 1) / MINIBLOCK;
        Arrays.fill(widths, 0);
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
