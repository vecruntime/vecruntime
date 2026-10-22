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
package io.vecruntime.kernels;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/**
 * Rows of a lane, nested or not, gathered into a new lane (#705, #842): rows
 * {@code idx} ({@code -1} = a null row). A list's or a map's rows become new
 * offsets over its children gathered at the elements' positions, and a
 * struct's fields are gathered at the same rows, recursively down to the
 * leaves, which are {@link GatherKernels}.
 *
 * <p>
 * Shared by the element access expressions (an element that is itself nested)
 * and the fanout Iceberg writer (a batch's rows split per partition).
 */
public final class NestedGatherKernels {

    private NestedGatherKernels() {}

    /** Rows {@code idx[0..n)} of {@code v} as a lane of {@code n} rows. */
    public static VectorBuffers gather(VectorBuffers v, int[] idx, int n,
            Arena arena) {
        return v.type().isNested() ? nested((NestedVectorBuffers) v, idx, n, arena) : leaf(v, idx, n, arena);
    }

    private static VectorBuffers nested(NestedVectorBuffers v, int[] idx, int n,
            Arena arena) {
        MemorySegment validity = ArrowLayout.allocateBitmap(arena, Math.max(n, 1));
        for (int i = 0; i < n; i++) {
            if (idx[i] >= 0 && !v.isNull(idx[i])) {
                Bitmap.set(validity, i);
            }
        }
        if (v.type() == VecType.STRUCT) {
            List<VectorBuffers> fields = new ArrayList<>(v.children()
                    .size());
            for (VectorBuffers c : v.children()) {
                fields.add(gather(c, idx, n, arena));
            }
            return new NestedVectorBuffers(VecType.STRUCT, n, validity, null, fields);
        }
        MemorySegment offsets = ArrowLayout.allocateOffsets(arena, n);
        int total = 0;
        for (int i = 0; i < n; i++) {
            if (Bitmap.isSet(validity, i)) {
                total += v.size(idx[i]);
            }
        }
        int[] elems = new int[Math.max(total, 1)];
        int k = 0;
        for (int i = 0; i < n; i++) {
            if (Bitmap.isSet(validity, i)) {
                for (int j = v.start(idx[i]), end = v.end(idx[i]);
                     j < end;
                     j++) {
                    elems[k++] = j;
                }
            }
            offsets.set(VectorBuffers.LE_INT, (long) (i + 1) << 2, k);
        }
        List<VectorBuffers> kids = new ArrayList<>(v.children()
                .size());
        for (VectorBuffers c : v.children()) {
            kids.add(gather(c, elems, k, arena));
        }
        return new NestedVectorBuffers(v.type(), n, validity, offsets, kids);
    }

    private static VectorBuffers leaf(VectorBuffers child, int[] idx, int n,
            Arena arena) {
        MemorySegment validity = ArrowLayout.allocateBitmap(arena, n);
        VecType t = child.type();
        if (t == VecType.UTF8) {
            MemorySegment offsets = ArrowLayout.allocateOffsets(arena, n);
            MemorySegment data = ArrowLayout.allocateBytes(arena, Math.max(GatherKernels.gatherUtf8Bytes(child, idx, 0, n), 1L));
            GatherKernels.gatherUtf8(child, idx, 0, n, offsets, data,
                    validity);
            return SegmentVectorBuffers.utf8(n, validity, offsets, data);
        }
        MemorySegment data = t == VecType.BOOL ? ArrowLayout.allocateBitmap(arena, n) : ArrowLayout.allocateData(arena, t, n);
        if (child.length() == 0) {
            // No element anywhere: every row is null (and no position to read).
            Bitmap.fillRange(validity, 0, n, false);
        } else {
            GatherKernels.gatherFixed(child, idx, 0, n, data, validity);
        }
        return SegmentVectorBuffers.fixedWidth(t, n, validity, data);
    }
}
