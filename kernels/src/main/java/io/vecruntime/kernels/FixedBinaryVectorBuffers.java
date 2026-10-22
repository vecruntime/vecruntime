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
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A {@link VecType#FIXED_BINARY} lane, Arrow's FixedSizeBinary: {@code length}
 * values of {@code width} bytes each, packed in {@code data} (value {@code i} at
 * byte {@code i * width}), with an optional validity bitmap. A null slot's
 * bytes are unspecified.
 */
public record FixedBinaryVectorBuffers(int length, int width, MemorySegment validity,
        MemorySegment data)
        implements VectorBuffers {

    public FixedBinaryVectorBuffers {
        Objects.requireNonNull(data, "data");
        if (length < 0) {
            throw new IllegalArgumentException("negative length: " + length);
        }
        if (width <= 0) {
            throw new IllegalArgumentException("FIXED_BINARY width must be positive: " + width);
        }
        if (data.byteSize() < (long) length * width) {
            throw new IllegalArgumentException("FIXED_BINARY data of "
                    + data.byteSize()
                    + " bytes for "
                    + length
                    + " values of "
                    + width);
        }
    }

    /**
     * An empty lane of {@code length} values of {@code width} bytes, all valid,
     * in {@code arena}.
     */
    public static FixedBinaryVectorBuffers allocate(Arena arena, int length, int width,
            MemorySegment validity) {
        return new FixedBinaryVectorBuffers(length, width, validity, ArrowLayout.allocateBytes(arena, Math.max(1L, (long) length * width)));
    }

    @Override
    public VecType type() {
        return VecType.FIXED_BINARY;
    }

    /** No offsets: values are {@link #width()} bytes apart. */
    @Override
    public MemorySegment offsets() {
        return null;
    }

    @Override
    public VectorBuffers dictionary() {
        return null;
    }

    /** The bytes of value {@code i} (a copy). */
    public byte[] bytes(int i) {
        byte[] b = new byte[width];
        MemorySegment.copy(data, ValueLayout.JAVA_BYTE, (long) i * width, b, 0,
                width);
        return b;
    }
}
