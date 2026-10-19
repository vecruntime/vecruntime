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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * One Arrow {@link RootAllocator} per JVM; operators create a child allocator
 * per task and close it (after closing every vector they produced) from a
 * task-completion listener.
 */
public final class VectorAllocators {

    private static final RootAllocator ROOT = new RootAllocator(Long.MAX_VALUE);

    private VectorAllocators() {}

    /**
     * #868: the executor's cap on Arrow memory -- the Flight shuffle's server and client buffers, the
     * shuffle readers' batches, the operators' output vectors -- all of which share this root. The
     * Flight server's threads and the buffers that outlive their task cannot be charged to a task's
     * {@code TaskMemoryManager}, so the budget is the executor's: past it an allocation fails with
     * Arrow's {@code OutOfMemoryException} (the task fails and is retried) instead of the container
     * being killed. {@code Long.MAX_VALUE} (the default) for no cap.
     */
    public static void setLimit(long bytes) {
        ROOT.setLimit(bytes <= 0 ? Long.MAX_VALUE : bytes);
    }

    /** Bytes currently allocated under the root, for the executor's metrics. */
    public static long allocatedBytes() {
        return ROOT.getAllocatedMemory();
    }

    /** The most ever allocated under the root at once, for the executor's metrics. */
    public static long peakBytes() {
        return ROOT.getPeakMemoryAllocation();
    }

    /** The cap set by {@link #setLimit}. */
    public static long limit() {
        return ROOT.getLimit();
    }

    public static BufferAllocator root() {
        return ROOT;
    }

    public static BufferAllocator newChild(String name) {
        return ROOT.newChildAllocator(name, 0L, Long.MAX_VALUE);
    }
}
