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
package io.vecruntime.spark.parquet;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.parquet.bytes.ByteBufferAllocator;

/**
 * #735: a bounded HEAP {@link ByteBufferAllocator} the native scan installs
 * ({@code ParquetReadOptions.withAllocator}) so parquet-java's per-row-group
 * column-chunk buffers come from a small pool instead of a fresh array every
 * row group ({@code HeapByteBufferAllocator.allocate} was ~11-12 GB of the 3 TB
 * scan-only allocation per executor window). parquet-java releases a chunk
 * buffer with {@link #release} (through its {@code ByteBufferReleaser}, driven
 * by {@code ColumnChunkPageReadStore.close()} at row-group end); a released
 * buffer whose capacity is useful is kept for the next row group, up to a few
 * buffers and a total byte cap, and anything past that is dropped to the GC.
 *
 * <p>Heap, not direct: {@link #isDirect()} returns {@code false}. That keeps
 * parquet-java's page-decompress visitor on the {@code decompress(BytesInput,
 * int)} path (the direct 4-arg path is taken only for a direct allocator), which
 * is the path {@link ReusingCodecFactory} reuses and #786 reads in place.
 *
 * <p>Not thread safe: one allocator per {@code ParquetFileReader}, used by one
 * thread at a time (prefetch builds a reader on a background thread then hands it
 * over). A buffer that outlives the reader is never returned to the pool after
 * {@link #close()} empties it.
 */
final class PooledHeapAllocator implements ByteBufferAllocator {

    private final Deque<ByteBuffer> pool = new ArrayDeque<>();
    private final int maxBuffers;
    private final long maxTotalBytes;
    private long pooledBytes;

    PooledHeapAllocator(int maxBuffers, long maxTotalBytes) {
        this.maxBuffers = Math.max(1, maxBuffers);
        this.maxTotalBytes = Math.max(0L, maxTotalBytes);
    }

    @Override
    public ByteBuffer allocate(int size) {
        // Reuse the smallest pooled buffer that fits, so a run of same-sized chunk reads hits the pool and a
        // one-off large read does not evict every small buffer.
        ByteBuffer best = null;
        for (ByteBuffer b : pool) {
            if (b.capacity() >= size && (best == null || b.capacity() < best.capacity())) {
                best = b;
            }
        }
        if (best != null) {
            pool.remove(best);
            pooledBytes -= best.capacity();
            best.clear();
            best.limit(size);
            return best;
        }
        return ByteBuffer.allocate(size);
    }

    @Override
    public void release(ByteBuffer b) {
        if (b == null || b.isDirect()) {
            return;
        }
        int cap = b.capacity();
        if (pool.size() >= maxBuffers || pooledBytes + cap > maxTotalBytes) {
            return; // drop to the GC: the pool is at its buffer count or byte budget
        }
        b.clear();
        pool.addLast(b);
        pooledBytes += cap;
    }

    @Override
    public boolean isDirect() {
        return false;
    }

    /**
     * Drops the pool (nothing is leaked: the buffers are plain heap arrays the
     * GC reclaims).
     */
    void close() {
        pool.clear();
        pooledBytes = 0L;
    }
}
