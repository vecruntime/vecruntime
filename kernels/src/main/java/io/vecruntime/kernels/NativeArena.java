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
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;

/**
 * An arena whose memory comes straight from the C allocator ({@code calloc} /
 * {@code free}) and whose {@link #close()} is a plain {@code free}.
 *
 * <p>#840: closing an {@link Arena#ofShared() shared} arena runs a handshake with
 * every thread of the JVM, so an owning {@link ColumnBuilder}, which gave each of
 * its buffers a shared arena and closed one on every doubling, stalled all task
 * threads of an executor many times per sort run (31-38 % of q14a/q14b's task
 * wall time at 10 TB). This arena has no memory session to close.
 *
 * <p>The price is the session's safety net: a segment of this arena stays
 * readable after {@link #close()} (its scope is the global one), so the owner
 * must not hand out a view that outlives it. {@link ColumnBuilder} documents
 * that already: its {@code view()} is valid until the next append or close.
 * Allocation and close are not thread-safe; one owner uses the arena.
 * Memory is zeroed, as an arena's is.
 */
final class NativeArena implements Arena {

    private static final MethodHandle CALLOC;
    private static final MethodHandle FREE;

    static {
        Linker linker = Linker.nativeLinker();
        CALLOC = linker.downcallHandle(linker.defaultLookup()
                .find("calloc")
                .orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        FREE = linker.downcallHandle(linker.defaultLookup()
                .find("free")
                .orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

    private final ArrayList<MemorySegment> blocks = new ArrayList<>(1); // as calloc returned them
    private boolean closed;

    @Override
    public MemorySegment allocate(long byteSize, long byteAlignment) {
        if (closed) {
            throw new IllegalStateException("arena closed");
        }
        if (byteSize < 0 || byteAlignment <= 0 || (byteAlignment & (byteAlignment - 1)) != 0) {
            throw new IllegalArgumentException("size " + byteSize + ", alignment " + byteAlignment);
        }
        long extra = byteAlignment - 1;
        MemorySegment raw;
        try {
            raw = (MemorySegment) CALLOC.invokeExact(1L, Math.max(byteSize + extra, 1L));
        } catch (Throwable t) {
            throw new IllegalStateException("calloc", t);
        }
        if (raw.address() == 0) {
            throw new OutOfMemoryError("calloc of " + byteSize + " bytes failed");
        }
        blocks.add(raw);
        long aligned = (raw.address() + extra) & -byteAlignment;
        return MemorySegment.ofAddress(aligned).reinterpret(byteSize);
    }

    @Override
    public MemorySegment.Scope scope() {
        return Arena.global().scope();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (MemorySegment raw : blocks) {
            try {
                FREE.invokeExact(raw);
            } catch (Throwable t) {
                throw new IllegalStateException("free", t);
            }
        }
        blocks.clear();
    }
}
