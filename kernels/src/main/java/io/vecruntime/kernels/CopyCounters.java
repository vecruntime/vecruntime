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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bytes copied at the boundaries between Arrow buffers, operator arenas and the
 * shuffle (#928 step 1: the copy inventory). Each call site passes a stable
 * name and goes through one of the {@code copy} overloads, which mirror
 * {@link MemorySegment#copy}. Off unless {@code -Dvecruntime.copy.counters} is
 * set: then the copy is the plain {@link MemorySegment#copy} behind a constant
 * branch. On, each site's bytes and calls are summed and a daemon thread prints
 * the running totals as {@code #928:} lines on stderr every
 * {@code vecruntime.copy.countersEverySecs} seconds (default 60), plus once at
 * exit.
 *
 * <p>Copies are intrinsics or native code, so a sampling profiler never shows
 * them as their own frame; these counters are the way to see them.
 */
public final class CopyCounters {

    /** Whether the counters are on (fixed at class load). */
    public static final boolean ON = Boolean.getBoolean("vecruntime.copy.counters");

    private static final Map<String, LongAdder[]> SITES = new ConcurrentHashMap<>();

    static {
        if (ON) {
            long every = Math.max(1, Long.getLong("vecruntime.copy.countersEverySecs", 60L));
            Thread t = new Thread(
                    () -> {
                        while (true) {
                            try {
                                Thread.sleep(every * 1000L);
                            } catch (InterruptedException e) {
                                return;
                            }
                            report();
                        }
                    },
                    "vecruntime-copy-counters");
            t.setDaemon(true);
            t.start();
            Runtime.getRuntime().addShutdownHook(new Thread(CopyCounters::report, "vecruntime-copy-counters-exit"));
        }
    }

    private CopyCounters() {}

    /** Adds {@code bytes} copied at {@code site} (a no-op when off). */
    public static void add(String site, long bytes) {
        if (ON) {
            count(site, bytes);
        }
    }

    /**
     * Counts regardless of {@link #ON} (the counting itself, testable without
     * the property).
     */
    static void count(String site, long bytes) {
        if (bytes > 0) {
            LongAdder[] a = SITES.get(site);
            if (a == null) {
                a = SITES.computeIfAbsent(site,
                        k -> new LongAdder[] {new LongAdder(), new LongAdder()});
            }
            a[0].add(bytes);
            a[1].increment();
        }
    }

    /**
     * {@link MemorySegment#copy(MemorySegment, long, MemorySegment, long,
     * long)}, counted.
     */
    public static void copy(String site, MemorySegment src, long srcOffset,
                            MemorySegment dst, long dstOffset, long bytes) {
        MemorySegment.copy(src, srcOffset, dst, dstOffset, bytes);
        add(site, bytes);
    }

    /**
     * {@link MemorySegment#copy(MemorySegment, ValueLayout, long,
     * MemorySegment, ValueLayout, long, long)}, counted.
     */
    public static void copy(
            String site,
            MemorySegment src,
            ValueLayout srcLayout,
            long srcOffset,
            MemorySegment dst,
            ValueLayout dstLayout,
            long dstOffset,
            long elements) {
        MemorySegment.copy(src, srcLayout, srcOffset, dst, dstLayout, dstOffset,
                elements);
        add(site, elements * srcLayout.byteSize());
    }

    /**
     * {@link MemorySegment#copy(MemorySegment, ValueLayout, long, Object, int,
     * int)}, counted.
     */
    public static void copy(
            String site,
            MemorySegment src,
            ValueLayout srcLayout,
            long srcOffset,
            Object dstArray,
            int dstIndex,
            int elements) {
        MemorySegment.copy(src, srcLayout, srcOffset, dstArray, dstIndex, elements);
        add(site, elements * srcLayout.byteSize());
    }

    /**
     * {@link MemorySegment#copy(Object, int, MemorySegment, ValueLayout, long,
     * int)}, counted.
     */
    public static void copy(
            String site,
            Object srcArray,
            int srcIndex,
            MemorySegment dst,
            ValueLayout dstLayout,
            long dstOffset,
            int elements) {
        MemorySegment.copy(srcArray, srcIndex, dst, dstLayout, dstOffset, elements);
        add(site, elements * dstLayout.byteSize());
    }

    /** Running totals by site: {bytes, calls}. */
    public static Map<String, long[]> snapshot() {
        Map<String, long[]> out = new TreeMap<>();
        SITES.forEach((k, v) -> out.put(k, new long[] {v[0].sum(), v[1].sum()}));
        return out;
    }

    /** Clears every site (tests). */
    static void resetForTests() {
        SITES.clear();
    }

    private static void report() {
        Map<String, long[]> s = snapshot();
        if (s.isEmpty()) {
            return;
        }
        StringBuilder b = new StringBuilder("#928: copies t=").append(System.currentTimeMillis());
        s.forEach(
                (k, v) -> b.append(' ')
                           .append(k)
                           .append('=')
                           .append(v[0])
                           .append('/')
                           .append(v[1]));
        System.err.println(b);
    }
}
