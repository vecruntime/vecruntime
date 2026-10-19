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

    static {
        // Diagnostics: -Dvecruntime.allocator.logSecs=N logs, every N seconds, the root's live and peak
        // bytes, the JVM's direct buffer pool, and the live bytes per child-allocator name (numeric
        // suffixes folded, so per-task children aggregate by operator).
        long secs = Long.getLong("vecruntime.allocator.logSecs", 0L);
        if (secs > 0) {
            java.util.concurrent.ScheduledExecutorService s = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "vecruntime-allocator-log");
                t.setDaemon(true);
                return t;
            });
            var _ = s.scheduleAtFixedRate(VectorAllocators::logUsage, secs, secs, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static void logUsage() {
        try {
            java.util.Map<String, long[]> byName = new java.util.TreeMap<>();
            for (BufferAllocator c : ROOT.getChildAllocators()) {
                String n = c.getName().replaceAll("[-_]?[0-9]+", "");
                long[] v = byName.computeIfAbsent(n, k -> new long[3]);
                v[0] += c.getAllocatedMemory();
                v[1] = Math.max(v[1], c.getPeakMemoryAllocation());
                v[2]++;
            }
            long direct = 0;
            for (java.lang.management.BufferPoolMXBean b : java.lang.management.ManagementFactory.getPlatformMXBeans(java.lang.management.BufferPoolMXBean.class)) {
                if ("direct".equals(b.getName())) {
                    direct = b.getMemoryUsed();
                }
            }
            StringBuilder sb = new StringBuilder("[alloc] root=")
                    .append(ROOT.getAllocatedMemory() >> 20)
                    .append("M peak=")
                    .append(ROOT.getPeakMemoryAllocation() >> 20)
                    .append("M direct=")
                    .append(direct >> 20)
                    .append("M netty=")
                    .append(nettyDirect("io.netty.util.internal.PlatformDependent") >> 20)
                    .append("M grpcNetty=")
                    .append(nettyDirect("io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent") >> 20)
                    .append('M');
            byName.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]))
                    .limit(10)
                    .forEach(
                            e -> sb.append(' ')
                                   .append(e.getKey())
                                   .append('=')
                                   .append(e.getValue()[0] >> 20)
                                   .append("M/x")
                                   .append(e.getValue()[2])
                                   .append("/maxpeak=")
                                   .append(e.getValue()[1] >> 20)
                                   .append('M'));
            System.err.println(sb);
        } catch (Throwable t) {
            System.err.println("[alloc] sample failed: " + t);
        }
    }

    /**
     * Netty's own count of the direct memory it allocated (pooled arenas
     * included), or -1.
     */
    private static long nettyDirect(String platformDependent) {
        try {
            return (Long) Class.forName(platformDependent)
                    .getMethod("usedDirectMemory")
                    .invoke(null);
        } catch (Throwable t) {
            return -1L << 20;
        }
    }

    private VectorAllocators() {}

    public static BufferAllocator root() {
        return ROOT;
    }

    public static BufferAllocator newChild(String name) {
        return ROOT.newChildAllocator(name, 0L, Long.MAX_VALUE);
    }
}
