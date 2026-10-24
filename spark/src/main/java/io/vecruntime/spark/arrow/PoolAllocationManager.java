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
package io.vecruntime.spark.arrow;

import java.lang.foreign.Arena;

import io.vecruntime.kernels.NativePool;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;

/**
 * #869 step 4: Arrow's buffers from the {@link NativePool}, so the Flight
 * shuffle's buffers and the operators' output vectors are blocks of the same
 * pool as the kernels' and are reused, not a {@code malloc} and {@code free}
 * each. Arrow still accounts them (its allocators and the executor's cap,
 * {@code spark.vecruntime.arrow.maxBytes}); the pool adds them to its totals.
 *
 * <p>Like Arrow's Unsafe manager, a buffer is not zeroed.
 */
public final class PoolAllocationManager extends AllocationManager {

    /** The zero-length buffer every allocator hands out for an empty request. */
    private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP, null, 0,
            Arena.global()
                    .allocate(64, 64)
                    .address());

    /** The factory for {@code RootAllocator}'s configuration. */
    public static final AllocationManager.Factory FACTORY = new AllocationManager.Factory() {
        @Override
        public AllocationManager create(BufferAllocator accountingAllocator, long size) {
            return new PoolAllocationManager(accountingAllocator, size);
        }

        @Override
        public ArrowBuf empty() {
            return EMPTY;
        }
    };

    /**
     * #924 step 1: an Arrow buffer allocated on a task thread is charged to that
     * task's pool account, and credited when it is released, on any thread;
     * one allocated outside a task (the Flight server's gRPC threads) is
     * charged nowhere, as before. {@code -Dvecruntime.arrow.chargeTasks}; off
     * until a full 10 TB run says it costs nothing.
     */
    static final boolean CHARGE_TASKS = Boolean.getBoolean("vecruntime.arrow.chargeTasks");

    private final NativePool.Lease lease;
    private final long size;

    private PoolAllocationManager(BufferAllocator accountingAllocator, long requestedSize) {
        super(accountingAllocator);
        this.lease = CHARGE_TASKS ? NativePool.shared().leaseCharged(requestedSize) : NativePool.shared().lease(requestedSize);
        this.size = requestedSize;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    protected long memoryAddress() {
        return lease.address();
    }

    @Override
    protected void release0() {
        lease.close();
    }
}
