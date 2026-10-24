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
package io.vecruntime.iceberg.write;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.GatherKernels;
import io.vecruntime.kernels.NestedGatherKernels;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;

/**
 * Rows of a batch gathered into new columns, for the fanout writer (#842, W3):
 * the engine's gather kernels over fixed-width, boolean, decimal and string
 * lanes, a dictionary-encoded string keeping its dictionary, and lists, maps
 * and structs gathered recursively ({@link NestedGatherKernels}).
 */
final class Gather {

    private Gather() {}

    static List<VectorBuffers> rows(List<? extends VectorBuffers> columns, int[] idx, Arena arena) {
        List<VectorBuffers> out = new ArrayList<>(columns.size());
        for (VectorBuffers c : columns) {
            out.add(column(c, idx, arena));
        }
        return out;
    }

    /**
     * Gathers only the columns {@code gather[c]} marks, up to {@code width},
     * into a {@code width}-long list; un-gathered slots are {@code null} (#901
     * M4b). The mixed-group copy only needs the ENCODED columns' values -- the
     * copied columns' bytes come straight from the source file, so gathering
     * them is wasted work (it dominated the every-row UPDATE). A {@code null}
     * slot is never read: the writer encodes only the non-copied columns, and
     * the float/double metrics skip null columns (a tracked float/double is
     * always encoded, never copied, so its slot is populated).
     */
    static List<VectorBuffers> rowsSelected(List<? extends VectorBuffers> columns, int[] idx, boolean[] gather,
            int width, Arena arena) {
        List<VectorBuffers> out = new ArrayList<>(width);
        for (int c = 0; c < width; c++) {
            out.add(c < gather.length && gather[c]
                    ? column(columns.get(c), idx, arena)
                    : null);
        }
        return out;
    }

    private static VectorBuffers column(VectorBuffers in, int[] idx, Arena arena) {
        int n = idx.length;
        if (in.type().isNested()) {
            // Lists, maps and structs: their rows' elements and fields, gathered recursively.
            return NestedGatherKernels.gather(in, idx, n, arena);
        }
        MemorySegment validity = in.hasNulls() ? ArrowLayout.allocateBitmap(arena, n) : null;
        if (in.type() == VecType.UTF8 && !in.isDictionaryEncoded()) {
            long bytes = GatherKernels.gatherUtf8Bytes(in, idx, 0, n);
            MemorySegment offsets = ArrowLayout.allocateOffsets(arena, n);
            MemorySegment data = ArrowLayout.allocateBytes(arena, Math.max(1, bytes));
            GatherKernels.gatherUtf8(in, idx, 0, n, offsets, data,
                    validity);
            return SegmentVectorBuffers.utf8(n, validity, offsets, data);
        }
        VecType physical = in.isDictionaryEncoded() ? VecType.INT32 : in.type();
        MemorySegment data = physical == VecType.BOOL ? ArrowLayout.allocateBitmap(arena, n) : ArrowLayout.allocateData(arena, physical, n);
        GatherKernels.gatherFixed(in, idx, 0, n, data, validity);
        // A dictionary-encoded column gathers its int32 ids (above), but its DICTIONARY (the UTF8 values the
        // ids point into) is the producer's buffer. The native Iceberg scan reuses/releases that buffer when
        // it emits its next batch (#901 M4b), while the rewrite retains these gathered columns across every
        // batch until close(). Retaining the dictionary BY REFERENCE therefore leaves the ids pointing at a
        // dictionary whose offsets/bytes have since been overwritten -- at re-encode time BinaryChunkWriter
        // reads offsets[id+1]-offsets[id] and gets a negative length (IndexOutOfBoundsException: negative
        // size). Copy the dictionary into this arena so the retained column is self-contained.
        return in.isDictionaryEncoded() ? SegmentVectorBuffers.dictionaryUtf8(n, validity, data, copyUtf8(in.dictionary(), arena)) : SegmentVectorBuffers.fixedWidth(in.type(), n, validity, data);
    }

    /**
     * A deep copy of a plain UTF8 dictionary (its validity, offsets and bytes)
     * into {@code arena}, so a retained dictionary-encoded column no longer
     * aliases the producer's reusable buffer (#901 M4b).
     */
    private static VectorBuffers copyUtf8(VectorBuffers dict, Arena arena) {
        int d = dict.length();
        MemorySegment srcOffsets = dict.offsets();
        int base = srcOffsets.get(VectorBuffers.LE_INT, 0L);
        long byteLen = (long) srcOffsets.get(VectorBuffers.LE_INT, (long) d << 2) - base;
        // Rebase the offsets to the copied bytes buffer (which starts at 0), not the source's base: a
        // dictionary read from the producer need not start at offset 0.
        MemorySegment offsets = ArrowLayout.allocateOffsets(arena, d);
        for (int i = 0; i <= d; i++) {
            offsets.set(VectorBuffers.LE_INT, (long) i << 2, srcOffsets.get(VectorBuffers.LE_INT, (long) i << 2) - base);
        }
        MemorySegment bytes = ArrowLayout.allocateBytes(arena, Math.max(1, byteLen));
        if (byteLen > 0) {
            MemorySegment.copy(dict.data(), base, bytes, 0L, byteLen);
        }
        MemorySegment validity = null;
        if (dict.hasNulls() && dict.validity() != null) {
            long vbytes = io.vecruntime.kernels.Bitmap.bytesFor(d);
            validity = ArrowLayout.allocateBitmap(arena, d);
            MemorySegment.copy(dict.validity(), 0L, validity, 0L, vbytes);
        }
        return SegmentVectorBuffers.utf8(d, validity, offsets, bytes);
    }
}
