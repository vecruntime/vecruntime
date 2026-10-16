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
import java.util.Arrays;
import java.util.List;

/**
 * The nested half of {@link ColumnBuilder} (#705): accumulates LIST, MAP and
 * STRUCT rows appended under a selection, as {@link ColumnBuilder} does lanes.
 * A list's or map's rows keep their validity and entry counts here; their
 * entries go to child builders in one append per call, under a selection of the
 * child rows the selected rows cover. A struct's fields are child builders
 * appended under the struct's own selection. Child builders are made on the
 * first append, from that input's child types, in this builder's memory mode.
 */
final class NestedColumnBuilder implements AutoCloseable {

    private final Arena arena; // the caller's, or null for an owning builder
    private final VecType type;
    private ColumnBuilder[] children;
    private boolean[] nulls;
    private int[] offsets; // LIST, MAP: offsets[i + 1] - offsets[i] entries of row i
    private int length;
    private Arena viewArena; // an owning builder's view buffers, replaced per view

    NestedColumnBuilder(Arena arena, VecType type, int expectedRows) {
        if (!type.isNested()) {
            throw new IllegalArgumentException("not a nested type: " + type);
        }
        this.arena = arena;
        this.type = type;
        int cap = Math.max(expectedRows, 16);
        nulls = new boolean[cap];
        offsets = type == VecType.STRUCT ? null : new int[cap + 1];
    }

    int length() {
        return length;
    }

    void append(VectorBuffers in, MemorySegment selection, int count) {
        NestedVectorBuffers nv = (NestedVectorBuffers) in;
        if (children == null) {
            children = new ColumnBuilder[nv.children().size()];
            for (int c = 0; c < children.length; c++) {
                VecType ct = nv.children()
                               .get(c)
                               .type();
                children[c] = arena == null ? ColumnBuilder.owning(ct, 1024) : new ColumnBuilder(arena, ct, 1024);
            }
        }
        if (count == 0) {
            return;
        }
        grow(length + count);
        int n = nv.length();
        if (type == VecType.STRUCT) {
            int r = length;
            for (int i = 0; i < n; i++) {
                if (selection == null || Bitmap.isSet(selection, i)) {
                    nulls[r++] = nv.isNull(i);
                }
            }
            for (int c = 0; c < children.length; c++) {
                children[c].append(nv.children().get(c), selection,
                        count);
            }
            length = r;
            return;
        }
        // LIST / MAP: the entries of the selected non-null rows, in row order.
        VectorBuffers first = nv.children().get(0);
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment entries = Bitmap.allocate(scratch, Math.max(first.length(), 1));
            int total = 0;
            int r = length;
            for (int i = 0; i < n; i++) {
                if (selection != null && !Bitmap.isSet(selection, i)) {
                    continue;
                }
                boolean isNull = nv.isNull(i);
                nulls[r] = isNull;
                int size = 0;
                if (!isNull) {
                    int s = nv.start(i);
                    int e = nv.end(i);
                    for (int j = s; j < e; j++) {
                        Bitmap.set(entries, j);
                    }
                    size = e - s;
                }
                offsets[r + 1] = offsets[r] + size;
                total += size;
                r++;
            }
            if (total > 0) {
                for (int c = 0; c < children.length; c++) {
                    children[c].append(nv.children().get(c), entries,
                            total);
                }
            }
            length = r;
        }
    }

    private void grow(int rows) {
        if (rows <= nulls.length) {
            return;
        }
        int cap = Math.max(rows, nulls.length * 2);
        nulls = Arrays.copyOf(nulls, cap);
        if (offsets != null) {
            offsets = Arrays.copyOf(offsets, cap + 1);
        }
    }

    /** The accumulated column; valid until the next append or close. */
    VectorBuffers view() {
        Arena a;
        if (arena != null) {
            a = arena;
        } else {
            if (viewArena != null) {
                viewArena.close();
            }
            viewArena = Arena.ofShared();
            a = viewArena;
        }
        MemorySegment validity = ArrowLayout.validityFrom(a, nulls, length);
        List<VectorBuffers> kids = new ArrayList<>();
        if (children == null) {
            // Nothing appended yet: no child types known; an empty nested lane has no children to read.
            if (type == VecType.STRUCT) {
                return NestedVectorBuffers.struct(0, null, kids);
            }
            throw new IllegalStateException("view of an empty " + type + " builder");
        }
        for (ColumnBuilder c : children) {
            kids.add(c.view());
        }
        if (type == VecType.STRUCT) {
            return NestedVectorBuffers.struct(length, validity, kids);
        }
        MemorySegment off = ArrowLayout.allocateOffsets(a, length);
        MemorySegment.copy(offsets, 0, off, VectorBuffers.LE_INT, 0,
                length + 1);
        return type == VecType.LIST ? NestedVectorBuffers.list(length, validity, off, kids.get(0)) : NestedVectorBuffers.map(length, validity, off, kids.get(0),
                kids.get(1));
    }

    long allocatedBytes() {
        long bytes = (long) nulls.length
                + (offsets == null ? 0 : 4L * offsets.length);
        if (children != null) {
            for (ColumnBuilder c : children) {
                bytes += c.allocatedBytes();
            }
        }
        return bytes;
    }

    @Override
    public void close() {
        if (children != null) {
            for (ColumnBuilder c : children) {
                c.close();
            }
        }
        if (viewArena != null) {
            viewArena.close();
            viewArena = null;
        }
    }
}
