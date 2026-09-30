/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
package io.vecruntime.spark.parquet;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import io.vecruntime.kernels.parquet.ColumnChunkDecoder;

/**
 * A growable native byte buffer for a UTF8 column chunk's data, reused across
 * every row group of a column ({@link #reset()} keeps the buffer, discards the
 * contents). The buffer doubles only when a row group's data outgrows it, so a
 * steady-state column pays no allocation after the first large row group. The
 * node hands the finished bytes to Arrow with a single copy per row group (a
 * UTF8 column's data is one buffer either way).
 */
final class NativeUtf8Sink implements ColumnChunkDecoder.Utf8Sink {

    private final Arena arena;
    private MemorySegment buf;
    private long len;

    NativeUtf8Sink(Arena arena) {
        this.arena = arena;
        this.buf = arena.allocate(1 << 16, 8);
    }

    @Override
    public void append(MemorySegment src, long srcOffset, int n) {
        if (n == 0) {
            return;
        }
        long need = len + n;
        if (need > buf.byteSize()) {
            long cap = Math.max(need, buf.byteSize() * 2);
            MemorySegment bigger = arena.allocate(cap, 8);
            MemorySegment.copy(buf, 0L, bigger, 0L, len);
            buf = bigger;
        }
        MemorySegment.copy(src, srcOffset, buf, len, n);
        len += n;
    }

    @Override
    public long length() {
        return len;
    }

    @Override
    public MemorySegment segment() {
        return buf;
    }

    @Override
    public void reset() {
        len = 0;
    }
}
