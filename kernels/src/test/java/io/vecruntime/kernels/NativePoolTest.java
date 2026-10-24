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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** #869: the native block pool behind owning {@link ColumnBuilder}s. */
class NativePoolTest {

    @Test
    void sizesRoundUpToTheirClass() {
        assertEquals(0, NativePool.classOf(1));
        assertEquals(0, NativePool.classOf(NativePool.MIN_CLASS));
        assertEquals(1, NativePool.classOf(NativePool.MIN_CLASS + 1));
        assertEquals(1, NativePool.classOf(2 * NativePool.MIN_CLASS));
        assertEquals(10, NativePool.classOf(NativePool.MAX_CLASS));
        assertEquals(-1, NativePool.classOf(NativePool.MAX_CLASS + 1));
        assertEquals(-1, NativePool.classOf(0));
    }

    @Test
    void segmentsAreZeroedAlignedAndCountedAndReusedAfterClose() {
        NativePool pool = new NativePool(1L << 30, false);
        long size = 100_000; // class 128 KiB
        Arena a = pool.newArena();
        MemorySegment s = a.allocate(size, 64);
        assertEquals(size, s.byteSize());
        assertEquals(0, s.address() % 64);
        assertAllZero(s);
        s.fill((byte) 7);
        assertEquals(128 * 1024, pool.inUseBytes());
        a.close();
        a.close(); // idempotent
        assertEquals(0, pool.inUseBytes());
        assertEquals(128 * 1024, pool.retainedBytes());
        assertThrows(IllegalStateException.class, () -> a.allocate(8, 8));

        Arena b = pool.newArena();
        MemorySegment t = b.allocate(size, 64);
        assertEquals(1, pool.reuseCount(), "the released block is handed out again");
        assertEquals(1, pool.mallocCount());
        assertAllZero(t); // zeroed again on reuse
        assertEquals(0, pool.retainedBytes());
        b.close();
        assertEquals(128 * 1024, pool.peakInUseBytes());
        pool.trim();
        assertEquals(0, pool.retainedBytes());
    }

    @Test
    void smallLargeAndOverAlignedRequestsBypassOrFit() {
        NativePool pool = new NativePool(1L << 30, false);
        try (Arena a = pool.newArena()) {
            MemorySegment small = a.allocate(100, 8);
            assertEquals(100, small.byteSize());
            assertEquals(100, pool.inUseBytes(), "below the smallest class: exact size, not pooled");
            MemorySegment large = a.allocate(NativePool.MAX_CLASS + 1, 64);
            assertEquals(NativePool.MAX_CLASS + 1, large.byteSize());
            large.set(ValueLayout.JAVA_BYTE, large.byteSize() - 1, (byte) 1);
            MemorySegment page = a.allocate(10_000, 4096);
            assertEquals(0, page.address() % 4096);
            assertEquals(10_000, page.byteSize());
            assertEquals(0, a.allocate(0, 64)
                             .byteSize());
        }
        assertEquals(0, pool.inUseBytes());
        assertEquals(0, pool.retainedBytes() % NativePool.MIN_CLASS, "only pooled classes are retained");
        assertThrows(IllegalArgumentException.class,
                () -> pool.newArena().allocate(8, 3));
        pool.trim();
    }

    @Test
    void releasedBytesAboveTheRetainLimitAreFreed() {
        NativePool pool = new NativePool(NativePool.MIN_CLASS, false);
        Arena a = pool.newArena();
        a.allocate(NativePool.MIN_CLASS, 64);
        a.allocate(NativePool.MIN_CLASS, 64);
        a.close();
        assertEquals(NativePool.MIN_CLASS, pool.retainedBytes(), "one block kept, one freed");
        pool.trim();
    }

    @Test
    void poisonMarksReleasedBlocks() {
        NativePool pool = new NativePool(1L << 30, true);
        Arena a = pool.newArena();
        MemorySegment s = a.allocate(NativePool.MIN_CLASS, 64);
        a.close();
        assertEquals((byte) 0xDB, s.get(ValueLayout.JAVA_BYTE, 0), "a stale view reads the poison");
        pool.trim();
    }

    @Test
    void theThreadCacheShapeIsConfigurableAndBounded() {
        int depth = NativePool.threadCacheDepth();
        long max = NativePool.threadCacheMaxBlockBytes();
        try {
            NativePool.configureThreadCache(8, 4L << 20);
            assertEquals(8, NativePool.threadCacheDepth());
            assertEquals(4L << 20, NativePool.threadCacheMaxBlockBytes());
            NativePool.configureThreadCache(2, 3L << 20); // rounded down to a class
            assertEquals(2L << 20, NativePool.threadCacheMaxBlockBytes());
            NativePool pool = new NativePool(1L << 30, false);
            try (Arena a = pool.newArena()) {
                a.allocate(1L << 20, 8);
                a.allocate(1L << 20, 8);
                a.allocate(1L << 20, 8);
            }
            assertEquals(3L << 20, pool.retainedBytes(), "two in the thread's cache, one on the shared list");
            pool.trim();
            assertEquals(0, pool.retainedBytes());
            assertThrows(IllegalArgumentException.class, () -> NativePool.configureThreadCache(17, 1L << 20));
            assertThrows(IllegalArgumentException.class, () -> NativePool.configureThreadCache(4, 1L << 10));
        } finally {
            NativePool.configureThreadCache(depth, max);
        }
    }

    @Test
    void concurrentOwnersNeverShareABlock() throws Exception {
        NativePool pool = new NativePool(64L << 20, false);
        ExecutorService ex = Executors.newFixedThreadPool(8);
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            final byte tag = (byte) (t + 1);
            fs.add(ex.submit(() -> {
                for (int i = 0; i < 500; i++) {
                    long size = NativePool.MIN_CLASS + ThreadLocalRandom.current().nextLong(1 << 20);
                    try (Arena a = pool.newArena()) {
                        MemorySegment s = a.allocate(size, 64);
                        s.fill(tag);
                        assertEquals(tag, s.get(ValueLayout.JAVA_BYTE, 0));
                        assertEquals(tag, s.get(ValueLayout.JAVA_BYTE, size - 1));
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        ex.shutdown();
        assertEquals(0, pool.inUseBytes());
        assertTrue(pool.reuseCount() > 0);
        assertTrue(pool.retainedBytes() <= 64L << 20);
        pool.trim();
    }

    @Test
    void anArenaChargesItsAccountAndCreditsItOnClose() {
        NativePool pool = new NativePool(1L << 30, false);
        long[] held = {0};
        NativePool.Account account = new NativePool.Account() {
            @Override
            public void charge(long bytes) {
                held[0] += bytes;
            }

            @Override
            public void credit(long bytes) {
                held[0] -= bytes;
            }
        };
        NativePool.setAccountSource(() -> account);
        try {
            Arena a = pool.newArena();
            a.allocate(100_000, 8);
            a.allocate(NativePool.MIN_CLASS, 8);
            // Class sizes: 100,000 rounds to 128 KiB.
            assertEquals((1L << 17) + NativePool.MIN_CLASS, held[0]);
            assertEquals(held[0], pool.inUseBytes());
            a.close();
            assertEquals(0, held[0]);
            a.close();
            assertEquals(0, held[0]);
        } finally {
            NativePool.setAccountSource(null);
        }
        // No source: nothing is charged.
        try (Arena b = pool.newArena()) {
            b.allocate(1000, 8);
        }
        assertEquals(0, held[0]);
    }

    @Test
    void aChargedLeaseChargesTheOpeningThreadsAccountAndAnyThreadCreditsIt() throws Exception {
        NativePool pool = new NativePool(1L << 30, false);
        java.util.concurrent.atomic.AtomicLong held = new java.util.concurrent.atomic.AtomicLong();
        NativePool.Account account = new NativePool.Account() {
            @Override
            public void charge(long bytes) {
                held.addAndGet(bytes);
            }

            @Override
            public void credit(long bytes) {
                held.addAndGet(-bytes);
            }
        };
        NativePool.setAccountSource(() -> account);
        NativePool.Lease charged;
        NativePool.Lease plain;
        try {
            charged = pool.leaseCharged(100_000);
            plain = pool.lease(100_000);
        } finally {
            NativePool.setAccountSource(null);
        }
        assertEquals(1L << 17, held.get(), "only the charged lease, at its class size");
        // Released on another thread (a buffer handed to the next operator): the opening account gets it back.
        ExecutorService other = Executors.newSingleThreadExecutor();
        try {
            other.submit(charged::close).get();
        } finally {
            other.shutdown();
        }
        assertEquals(0L, held.get());
        charged.close(); // once
        assertEquals(0L, held.get());
        plain.close();
        assertEquals(0L, pool.inUseBytes());
    }

    private static void assertAllZero(MemorySegment s) {
        for (long i = 0; i < s.byteSize(); i += 8) {
            long left = Math.min(8, s.byteSize() - i);
            if (left == 8) {
                assertEquals(0L, s.get(ValueLayout.JAVA_LONG_UNALIGNED, i), "zero at " + i);
            } else {
                for (long j = i; j < s.byteSize(); j++) {
                    assertEquals(0, s.get(ValueLayout.JAVA_BYTE, j));
                }
            }
        }
    }
}
