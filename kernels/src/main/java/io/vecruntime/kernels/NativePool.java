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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

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

    /**
     * #869 step 3b: where an owner's blocks are counted besides the pool's own
     * totals -- in Spark, the task's execution memory. {@link #charge} is called
     * after a block is handed out and {@link #credit} after it comes back, with
     * the block's class size, on whichever thread allocates or closes.
     */
    public interface Account {
        /** {@code bytes} more held by this account's owners. */
        void charge(long bytes);

        /** {@code bytes} given back. */
        void credit(long bytes);
    }

    private static volatile Supplier<Account> accountSource = () -> null;

    /**
     * The account a new {@link #newArena() arena} charges, looked up on the
     * thread that opens it (null for none). One process-wide source; the
     * default charges nothing.
     */
    public static void setAccountSource(Supplier<Account> source) {
        accountSource = source == null ? () -> null : source;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final ConcurrentLinkedQueue<Block>[] free = new ConcurrentLinkedQueue[CLASSES];

    /**
     * #869 step 4: the smaller classes also have a per-thread cache in front of
     * their shared free list, so the batch-sized buffers 15 task threads take
     * and release every few microseconds do not all meet on one queue head;
     * counters are striped for the same reason. A cached block counts as
     * retained. By default classes up to 1 MiB, 4 blocks each: at most about
     * 8 MiB per thread ({@link #configureThreadCache}).
     */
    static final int MAX_CACHE_DEPTH = 16;

    private static volatile int cachedClasses = 5;

    private static volatile int cacheDepth = 4;

    /**
     * The per-thread cache's shape for every pool: {@code depth} blocks (0 to
     * 16) of each class up to {@code maxBlockBytes} (64 KiB to 64 MiB, rounded
     * down to a class). A thread holds at most about {@code 2 * depth *
     * maxBlockBytes}. Set once, before the pool is busy (the executor plugin
     * does it first thing); blocks already cached past a smaller shape stay
     * until they are taken.
     */
    public static void configureThreadCache(int depth, long maxBlockBytes) {
        if (depth < 0
                || depth > MAX_CACHE_DEPTH
                || maxBlockBytes < MIN_CLASS
                || maxBlockBytes > MAX_CLASS) {
            throw new IllegalArgumentException("thread cache depth " + depth + ", max block " + maxBlockBytes);
        }
        cachedClasses = 63 - Long.numberOfLeadingZeros(maxBlockBytes) - MIN_SHIFT + 1;
        cacheDepth = depth;
    }

    /** Blocks per class in each thread's cache. */
    public static int threadCacheDepth() {
        return cacheDepth;
    }

    /** The largest block a thread caches. */
    public static long threadCacheMaxBlockBytes() {
        return MIN_CLASS << (cachedClasses - 1);
    }

    private static final class ThreadCache {
        final Block[][] blocks = new Block[CLASSES][MAX_CACHE_DEPTH];
        final int[] count = new int[CLASSES];
        // Bytes in this thread's cache, and takes so far (the peak is sampled every PEAK_SAMPLE-th take).
        long bytes;
        int takes;
    }

    /**
     * The in-use peak is refreshed on one take in this many per thread, not on
     * every one: reading a striped counter's total touches every stripe, which
     * the other task threads keep writing, so a read per take would cost what
     * striping saved. {@link #peakInUseBytes()} also folds in the current total
     * when read.
     */
    private static final int PEAK_SAMPLE = 16;

    private final ThreadLocal<ThreadCache> cache = ThreadLocal.withInitial(ThreadCache::new);

    private final LongAdder inUse = new LongAdder();
    private final AtomicLong peakInUse = new AtomicLong();
    private final AtomicLong retained = new AtomicLong();
    private final LongAdder cached = new LongAdder();
    private final LongAdder mallocs = new LongAdder();
    private final LongAdder reuses = new LongAdder();
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
        return new PooledArena(this, accountSource.get());
    }

    /**
     * #869 step 4: one block for an owner outside any task -- an Arrow buffer
     * (the Flight shuffle's, an operator's output vector) that may outlive the
     * task that made it. Not zeroed (Arrow does not zero its buffers) and
     * charged to no account: Arrow's own allocator and its cap count it.
     */
    public Lease lease(long size) {
        return new Lease(this, take(size, false));
    }

    /** A block from {@link #lease}; {@link #close()} gives it back, once. */
    public static final class Lease implements AutoCloseable {
        private final NativePool pool;
        private Block block;

        Lease(NativePool pool, Block block) {
            this.pool = pool;
            this.block = block;
        }

        /** The block's address, 64-byte aligned. */
        public long address() {
            return block.segment.address();
        }

        /** The block's usable size: the request rounded up to its class. */
        public long size() {
            return block.size;
        }

        @Override
        public synchronized void close() {
            if (block != null) {
                pool.release(block);
                block = null;
            }
        }
    }

    /** Bytes handed out and not yet released, rounded to their classes. */
    public long inUseBytes() {
        return inUse.sum();
    }

    /** The highest {@link #inUseBytes()} so far. */
    public long peakInUseBytes() {
        long now = inUse.sum();
        return now > peakInUse.get() ? peakInUse.accumulateAndGet(now, Math::max) : peakInUse.get();
    }

    /**
     * Bytes of released blocks the pool keeps for reuse, per-thread caches
     * included.
     */
    public long retainedBytes() {
        return retained.get() + cached.sum();
    }

    /** Most bytes of released blocks kept for reuse. */
    public long retainLimit() {
        return retainLimit;
    }

    /** Blocks that came from {@code malloc}. */
    public long mallocCount() {
        return mallocs.sum();
    }

    /** Blocks served from a free list. */
    public long reuseCount() {
        return reuses.sum();
    }

    /**
     * Frees every retained block: the shared lists' and the calling thread's
     * cache.
     */
    public void trim() {
        ThreadCache tc = cache.get();
        for (int c = 0; c < CLASSES; c++) {
            while (tc.count[c] > 0) {
                Block b = tc.blocks[c][--tc.count[c]];
                tc.blocks[c][tc.count[c]] = null;
                cached.add(-b.size);
                freeRaw(b.raw);
            }
        }
        tc.bytes = 0;
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
        return take(size, true);
    }

    /**
     * A block of at least {@code size} bytes, zeroed over {@code size} when
     * {@code zero}.
     */
    Block take(long size, boolean zero) {
        int c = size < MIN_CLASS ? -1 : classOf(size);
        Block b = null;
        ThreadCache tc = cache.get();
        if (c >= 0 && c < cachedClasses) {
            if (tc.count[c] > 0) {
                b = tc.blocks[c][--tc.count[c]];
                tc.blocks[c][tc.count[c]] = null;
                tc.bytes -= b.size;
                cached.add(-b.size);
                reuses.increment();
            }
        }
        if (b == null && c >= 0) {
            b = free[c].poll();
            if (b != null) {
                retained.addAndGet(-b.size);
                reuses.increment();
            }
        }
        if (b == null) {
            long blockSize = c < 0 ? Math.max(size, 1) : MIN_CLASS << c;
            b = new Block(mallocRaw(blockSize + ALIGN - 1), blockSize, c);
            mallocs.increment();
        }
        if (zero) {
            b.segment.asSlice(0, size).fill((byte) 0);
        }
        inUse.add(b.size);
        if ((tc.takes++ % PEAK_SAMPLE) == 0) {
            long now = inUse.sum();
            if (now > peakInUse.get()) {
                peakInUse.accumulateAndGet(now, Math::max);
            }
        }
        return b;
    }

    /**
     * Takes {@code b} back: into this thread's cache, onto its free list, or to
     * {@code free}.
     */
    void release(Block b) {
        inUse.add(-b.size);
        if (poison) {
            b.segment.fill(POISON);
        }
        // The retain limit, checked without reading the striped cache total: the shared lists' bytes plus this
        // thread's cache (other threads' caches are bounded by their depth and are not counted here).
        ThreadCache tc = cache.get();
        if (b.cls >= 0 && b.cls < cachedClasses && retained.get() + tc.bytes + b.size <= retainLimit) {
            if (tc.count[b.cls] < cacheDepth) {
                tc.blocks[b.cls][tc.count[b.cls]++] = b;
                tc.bytes += b.size;
                cached.add(b.size);
                return;
            }
        }
        if (b.cls >= 0 && retained.addAndGet(b.size) + tc.bytes <= retainLimit) {
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
        private final Account account;
        private final ArrayList<Block> blocks = new ArrayList<>(2);
        private boolean closed;

        PooledArena(NativePool pool, Account account) {
            this.pool = pool;
            this.account = account;
        }

        private Block takeCounted(long size) {
            Block b = pool.take(size);
            blocks.add(b);
            if (account != null) {
                account.charge(b.size);
            }
            return b;
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
                Block b = takeCounted(byteSize + byteAlignment - 1);
                long start = (b.segment.address() + byteAlignment - 1) & -byteAlignment;
                return b.segment.asSlice(start - b.segment.address(), byteSize);
            }
            Block b = takeCounted(byteSize);
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
            long bytes = 0;
            for (Block b : blocks) {
                bytes += b.size;
                pool.release(b);
            }
            blocks.clear();
            if (account != null && bytes > 0) {
                account.credit(bytes);
            }
        }
    }
}
