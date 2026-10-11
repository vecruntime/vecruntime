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

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Objects;

/**
 * A nested lane (#705) in Arrow's layout: a {@link VecType#LIST} or {@link
 * VecType#MAP} is int32 {@code offsets} ({@code length + 1} entries, absolute
 * positions in the children) plus its children -- the elements, or the keys and
 * the values -- and a {@link VecType#STRUCT} is one child per field, each of
 * the struct's length, and no offsets. Validity is the nested value's own (a
 * null array, map or struct); the children carry their elements' validity.
 *
 * <p>A child is any {@link VectorBuffers}, a lane or another nested lane, so a
 * kernel over the elements of a list runs on its child unchanged, over {@code
 * offsets[length] - offsets[0]} rows.
 */
public record NestedVectorBuffers(VecType type, int length, MemorySegment validity,
        MemorySegment offsets, List<VectorBuffers> children)
        implements VectorBuffers {

    public NestedVectorBuffers {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(children, "children");
        if (!type.isNested()) {
            throw new IllegalArgumentException("not a nested lane: " + type);
        }
        if (length < 0) {
            throw new IllegalArgumentException("negative length: " + length);
        }
        switch (type) {
            case LIST -> {
                requireOffsets(type, offsets);
                if (children.size() != 1) {
                    throw new IllegalArgumentException("LIST has one child, not " + children.size());
                }
            }
            case MAP -> {
                requireOffsets(type, offsets);
                if (children.size() != 2 || children.get(0).length() != children.get(1).length()) {
                    throw new IllegalArgumentException("MAP has a keys and a values child of one length");
                }
            }
            default -> {
                if (offsets != null) {
                    throw new IllegalArgumentException("STRUCT has no offsets");
                }
                for (VectorBuffers f : children) {
                    if (f.length() < length) {
                        throw new IllegalArgumentException("STRUCT field of " + f.length() + " rows under " + length);
                    }
                }
            }
        }
        children = List.copyOf(children);
    }

    private static void requireOffsets(VecType type, MemorySegment offsets) {
        if (offsets == null) {
            throw new IllegalArgumentException(type + " requires offsets");
        }
    }

    public static NestedVectorBuffers list(int length, MemorySegment validity, MemorySegment offsets,
            VectorBuffers elements) {
        return new NestedVectorBuffers(VecType.LIST, length, validity, offsets, List.of(elements));
    }

    public static NestedVectorBuffers map(int length, MemorySegment validity, MemorySegment offsets,
            VectorBuffers keys, VectorBuffers values) {
        return new NestedVectorBuffers(VecType.MAP, length, validity, offsets, List.of(keys, values));
    }

    public static NestedVectorBuffers struct(int length, MemorySegment validity, List<VectorBuffers> fields) {
        return new NestedVectorBuffers(VecType.STRUCT, length, validity, null, fields);
    }

    /**
     * A nested lane has no data buffer of its own; its values are in {@link
     * #children()}.
     */
    @Override
    public MemorySegment data() {
        return MemorySegment.NULL;
    }

    @Override
    public VectorBuffers dictionary() {
        return null;
    }

    /** First child row of value {@code i} of a LIST or MAP. */
    public int start(int i) {
        return offsets.get(LE_INT, (long) i << 2);
    }

    /** One past the last child row of value {@code i} of a LIST or MAP. */
    public int end(int i) {
        return offsets.get(LE_INT, (long) (i + 1) << 2);
    }

    /**
     * Number of elements (or entries) of value {@code i} of a LIST or MAP; 0
     * for a null value.
     */
    public int size(int i) {
        return end(i) - start(i);
    }
}
