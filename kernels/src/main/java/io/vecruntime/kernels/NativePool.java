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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * #869: a process-wide pool of native blocks in power-of-two size classes, and
 * the one place that counts every byte the kernels hold natively.
 *
 * <p>A request from {@link #MIN_CLASS} to {@link #MAX_CLASS} bytes is rounded up
 * to its class and served from that class's free list, or from {@code malloc}
 * when the list is empty. A released block goes back on its list while the pool
 * retains less than {@link #retainLimit()} free bytes, and to {@code free}
 * otherwise. Smaller and larger requests bypass the lists: {@code malloc} and
 * {@code free} directly, still counted.
 *
 * <p>Owners take blocks through a {@link #newArena() pooled arena}: an
 * {@link Arena} whose {@code close()} returns its blocks. That keeps the
 * property #840 needed -- no memory session, so a close is no handshake with
 * every JVM thread -- and adds reuse: a sort run's doubling buffers, built and
 * dropped thousands of times per executor, stop being a {@code malloc} (with
 * {@code MALLOC_MMAP_THRESHOLD_=0}, an {@code mmap}) each.
 *
 * <p>Segments are zeroed on every hand-out, as an arena's are. With the system
 * property {@code vecruntime.nativePool.poison=true} a released block is filled
 * with {@code 0xDB} first, so a read through a view that outlived its owner
 * sees garbage in tests instead of plausible data.
 *
 * <p>The pool is thread-safe: free lists are lock-free queues and the counters
 * atomics. One {@link Arena} it hands out is used by one owner at a time.
 */
public final class NativePool {

    /** Smallest pooled size: 64 KiB. */
    public static final long MIN_CLASS = 1L << 16;

    /** Largest pooled size: 64 MiB. */
    public static final long MAX_CLASS = 1L << 26;

    private static final int MIN_SHIFT = 16;
    private static final int CLASSES = 26 - MIN_SHIFT + 1;

    /** Alignment of every block: Arrow's buffer padding. */
    static final long ALIGN = 64;

    private static final byte POISON = (byte) 0xDB;

    private static final MethodHandle MALLOC;
    private static final MethodHandle FREE;

    static {
        Linker linker = Linker.nativeLinker();
        MALLOC = linker.downcallHandle(linker.defaultLookup()
                .find("malloc")
                .orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
        FREE = linker.downcallHandle(linker.defaultLookup()
                .find("free")
                .orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

    private static final NativePool SHARED = new NativePool(Long.getLong("vecruntime.nativePool.retainBytes", 2L << 30), Boolean.getBoolean("vecruntime.nativePool.poison"));

    /** The process-wide pool. */
    public static NativePool shared() {
        return SHARED;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final ConcurrentLinkedQueue<Block>[] free = new ConcurrentLinkedQueue[CLASSES];

    private final AtomicLong inUse = new AtomicLong();
    private final AtomicLong peakInUse = new AtomicLong();
    private final AtomicLong retained = new AtomicLong();
    private final AtomicLong mallocs = new AtomicLong();
    private final AtomicLong reuses = new AtomicLong();
    private final long retainLimit;
    private final boolean poison;

    /**
     * A pool that keeps at most {@code retainLimit} bytes of released blocks for
     * reuse; {@code poison} fills a released block with a marker byte.
     */
    public NativePool(long retainLimit, boolean poison) {
        if (retainLimit < 0) {
            throw new IllegalArgumentException("retainLimit " + retainLimit);
        }
        this.retainLimit = retainLimit;
        this.poison = poison;
        for (int c = 0; c < CLASSES; c++) {
            free[c] = new ConcurrentLinkedQueue<>();
        }
    }

    /**
     * A new owner: an arena whose blocks come from this pool and go back on
     * close.
     */
    public Arena newArena() {
        return new PooledArena(this);
    }

    /** Bytes handed out and not yet released, rounded to their classes. */
    public long inUseBytes() {
        return inUse.get();
    }

    /** The highest {@link #inUseBytes()} so far. */
    public long peakInUseBytes() {
        return peakInUse.get();
    }

    /** Bytes of released blocks the pool keeps for reuse. */
    public long retainedBytes() {
        return retained.get();
    }

    /** Most bytes of released blocks kept for reuse. */
    public long retainLimit() {
        return retainLimit;
    }

    /** Blocks that came from {@code malloc}. */
    public long mallocCount() {
        return mallocs.get();
    }

    /** Blocks served from a free list. */
    public long reuseCount() {
        return reuses.get();
    }

    /** Frees every retained block. */
    public void trim() {
        for (ConcurrentLinkedQueue<Block> q : free) {
            Block b;
            while ((b = q.poll()) != null) {
                retained.addAndGet(-b.size);
                freeRaw(b.raw);
            }
        }
    }

    /** The class index of a pooled size, or -1 when {@code size} is not pooled. */
    static int classOf(long size) {
        if (size < 1 || size > MAX_CLASS) {
            return -1;
        }
        long rounded = Math.max(MIN_CLASS, Long.highestOneBit(size - 1) << 1);
        if (size == 1) {
            rounded = MIN_CLASS;
        }
        return Long.numberOfTrailingZeros(rounded) - MIN_SHIFT;
    }

    /**
     * A block of at least {@code size} bytes, 64-byte aligned, zeroed over
     * {@code size}.
     */
    Block take(long size) {
        int c = size < MIN_CLASS ? -1 : classOf(size);
        Block b = c < 0 ? null : free[c].poll();
        if (b != null) {
            retained.addAndGet(-b.size);
            reuses.incrementAndGet();
        } else {
            long blockSize = c < 0 ? Math.max(size, 1) : MIN_CLASS << c;
            b = new Block(mallocRaw(blockSize + ALIGN - 1), blockSize, c);
            mallocs.incrementAndGet();
        }
        b.segment.asSlice(0, size).fill((byte) 0);
        long now = inUse.addAndGet(b.size);
        peakInUse.accumulateAndGet(now, Math::max);
        return b;
    }

    /** Takes {@code b} back: onto its free list, or to {@code free}. */
    void release(Block b) {
        inUse.addAndGet(-b.size);
        if (poison) {
            b.segment.fill(POISON);
        }
        if (b.cls >= 0 && retained.addAndGet(b.size) <= retainLimit) {
            free[b.cls].offer(b);
            return;
        }
        if (b.cls >= 0) {
            retained.addAndGet(-b.size);
        }
        freeRaw(b.raw);
    }

    private static MemorySegment mallocRaw(long bytes) {
        MemorySegment raw;
        try {
            raw = (MemorySegment) MALLOC.invokeExact(bytes);
        } catch (Throwable t) {
            throw new IllegalStateException("malloc", t);
        }
        if (raw.address() == 0) {
            throw new OutOfMemoryError("malloc of " + bytes + " bytes failed");
        }
        return raw;
    }

    private static void freeRaw(MemorySegment raw) {
        try {
            FREE.invokeExact(raw);
        } catch (Throwable t) {
            throw new IllegalStateException("free", t);
        }
    }

    /**
     * One native block: the pointer malloc returned, and its aligned usable
     * segment.
     */
    static final class Block {
        final MemorySegment raw;
        final MemorySegment segment;
        final long size;
        final int cls;

        Block(MemorySegment raw, long size, int cls) {
            this.raw = raw;
            this.size = size;
            this.cls = cls;
            long aligned = (raw.address() + ALIGN - 1) & -ALIGN;
            this.segment = MemorySegment.ofAddress(aligned).reinterpret(size);
        }
    }

    /** An owner of pool blocks; {@link #close()} gives them back. Not thread-safe. */
    static final class PooledArena implements Arena {
        private final NativePool pool;
        private final ArrayList<Block> blocks = new ArrayList<>(2);
        private boolean closed;

        PooledArena(NativePool pool) {
            this.pool = pool;
        }

        @Override
        public MemorySegment allocate(long byteSize, long byteAlignment) {
            if (closed) {
                throw new IllegalStateException("arena closed");
            }
            if (byteSize < 0 || byteAlignment <= 0 || (byteAlignment & (byteAlignment - 1)) != 0) {
                throw new IllegalArgumentException("size " + byteSize + ", alignment " + byteAlignment);
            }
            if (byteAlignment > ALIGN) {
                // Rare (no Arrow buffer asks for more than 64): over-allocate inside one block.
                Block b = pool.take(byteSize + byteAlignment - 1);
                blocks.add(b);
                long start = (b.segment.address() + byteAlignment - 1) & -byteAlignment;
                return b.segment.asSlice(start - b.segment.address(), byteSize);
            }
            Block b = pool.take(byteSize);
            blocks.add(b);
            return b.segment.asSlice(0, byteSize);
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
            for (Block b : blocks) {
                pool.release(b);
            }
            blocks.clear();
        }
    }
}
