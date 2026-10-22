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
        return in.isDictionaryEncoded() ? SegmentVectorBuffers.dictionaryUtf8(n, validity, data, in.dictionary()) : SegmentVectorBuffers.fixedWidth(in.type(), n, validity, data);
    }
}
