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
package io.vecruntime.parquet.codec;

/**
 * The framing the Parquet encoders and decoders share (#900): one definition of
 * the parts of the {@code DELTA_BINARY_PACKED} and RLE / bit-packed hybrid
 * layouts that the writer ({@code io.vecruntime.parquet.write}) emits and this
 * package parses, so the two cannot drift. Only the framing lives here -- block
 * geometry, zigzag, run headers, widths. The value loops stay with their side:
 * the reader gathers into batches and the writer scatters from Arrow, and each
 * is tuned for its direction. Every method is a static one-liner that C2
 * inlines into its caller.
 *
 * <p>{@code DELTA_BINARY_PACKED}: a header (block size, miniblocks per block,
 * value count, first value zigzag-encoded, all ULEB128), then per block a
 * zigzag minimum delta, one width byte per miniblock, and each miniblock's
 * reduced deltas bit-packed LSB first. The RLE / bit-packed hybrid: runs, each
 * a ULEB128 header whose low bit is 0 for an RLE run ({@code count << 1}, then
 * the value in {@link #rleValueBytes} little-endian bytes) and 1 for a
 * bit-packed run ({@code groups << 1 | 1}, then {@code groups} groups of 8
 * values of {@code bitWidth} bits, i.e. {@code bitWidth} bytes per group).
 */
public final class ParquetFraming {

    private ParquetFraming() {}

    /**
     * Values per {@code DELTA_BINARY_PACKED} block the writer emits
     * (parquet-java's choice).
     */
    public static final int DELTA_BLOCK_SIZE = 128;

    /** Miniblocks per {@code DELTA_BINARY_PACKED} block the writer emits. */
    public static final int DELTA_MINIBLOCKS = 4;

    /**
     * Values per miniblock, {@link #DELTA_BLOCK_SIZE} / {@link
     * #DELTA_MINIBLOCKS}.
     */
    public static final int DELTA_MINIBLOCK_SIZE = DELTA_BLOCK_SIZE / DELTA_MINIBLOCKS;

    /**
     * Zigzag encoding of a signed value (the first value and each block's
     * minimum delta).
     */
    public static long zigzagEncode(long v) {
        return (v << 1) ^ (v >> 63);
    }

    /** The inverse of {@link #zigzagEncode}. */
    public static long zigzagDecode(long v) {
        return (v >>> 1) ^ -(v & 1);
    }

    /** The header of an RLE run of {@code count} equal values. */
    public static int rleRunHeader(int count) {
        return count << 1;
    }

    /** The header of a bit-packed run of {@code groups} groups of 8 values. */
    public static int bitPackedRunHeader(int groups) {
        return groups << 1 | 1;
    }

    /** Whether a run header announces a bit-packed run (otherwise an RLE run). */
    public static boolean isBitPackedRun(int header) {
        return (header & 1) != 0;
    }

    /**
     * A run header's count: values for an RLE run, groups of 8 for a bit-packed
     * one.
     */
    public static int runCount(int header) {
        return header >>> 1;
    }

    /**
     * Groups of 8 that hold {@code n} bit-packed values (the last one
     * zero-padded).
     */
    public static int bitPackedGroups(int n) {
        return (n + 7) >>> 3;
    }

    /**
     * Bytes of an RLE run's repeated value at {@code bitWidth}: {@code
     * ceil(bitWidth / 8)}.
     */
    public static int rleValueBytes(int bitWidth) {
        return (bitWidth + 7) >>> 3;
    }

    /**
     * Bits needed for values {@code 0 .. maxValue} (0 for 0): level and
     * dictionary-id widths.
     */
    public static int bitWidth(int maxValue) {
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }
}
