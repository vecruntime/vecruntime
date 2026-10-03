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
package io.vecruntime.benchmarks;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.GatherKernels;
import io.vecruntime.kernels.HashKernels;
import io.vecruntime.kernels.StringCompareKernels;
import io.vecruntime.kernels.VectorBuffers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * #613 research: a 16-byte string view (Umbra, DuckDB {@code string_t}, Velox
 * {@code StringView}, Arrow {@code Utf8View}) against our Arrow
 * offsets-plus-data UTF8 lane, on the operations the issue names: gather (join
 * output), equality with a constant (filter), ordering compare (sort), hashing
 * (join and aggregate keys), and the cost of building views from the offsets
 * layout the Parquet decoder writes.
 *
 * <p>View layout, 16 bytes per row in one native segment: {@code int length},
 * then for a string of at most 12 bytes its bytes inline (zero padded), else a
 * 4-byte prefix and an {@code int} offset into the source's data segment (one
 * buffer, so no buffer index). Baselines are the engine's own kernels where it
 * has one ({@code GatherKernels.gatherUtf8}, {@code
 * StringCompareKernels.compareBytes}, {@code HashKernels.hashBytes}). The value
 * mixes follow TPC-DS: short codes, 16-byte ids with an 8-byte common prefix
 * ({@code AAAAAAAABAAAAAAA}), 20-60-byte names, and URLs with a common scheme
 * prefix.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@OperationsPerInvocation(StringViewBenchmark.N)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class StringViewBenchmark {
    static final int N = 4096;
    static final int SOURCE_ROWS = 65536;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED;
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED;

    /** The value mix. */
    @Param({"codes", "ids", "names", "urls"})
    String mix;

    Arena arena;
    VectorBuffers in; // offsets layout
    MemorySegment views; // 16 bytes per row
    MemorySegment data; // the payload both layouts point into
    int[] idx;
    MemorySegment outOffsets;
    MemorySegment outData;
    MemorySegment outViews;
    byte[] constant;
    MemorySegment constantSeg;
    long constantHead; // the constant's first 8 view bytes: length and prefix
    long constantTail; // its second 8 view bytes when inline
    int[] hashes;

    @Setup(Level.Trial)
    public void setup() {
        arena = Arena.ofShared();
        Random rnd = new Random(613);
        String[] values = new String[SOURCE_ROWS];
        for (int i = 0; i < SOURCE_ROWS; i++) {
            values[i] = value(rnd, i);
        }
        in = ArrowLayout.ofStrings(arena, values);
        data = in.data();
        views = arena.allocate((long) SOURCE_ROWS * 16, 8);
        buildViews(in.offsets(), SOURCE_ROWS, views);
        idx = new int[N];
        for (int i = 0; i < N; i++) {
            idx[i] = rnd.nextInt(SOURCE_ROWS);
        }
        outOffsets = ArrowLayout.allocateOffsets(arena, N);
        outData = ArrowLayout.allocateBytes(arena, GatherKernels.gatherUtf8Bytes(in, idx, 0, N) + 8);
        outViews = arena.allocate((long) N * 16, 8);
        constant = values[rnd.nextInt(SOURCE_ROWS)].getBytes(StandardCharsets.UTF_8);
        constantSeg = arena.allocate(Math.max(1, constant.length));
        MemorySegment.copy(constant, 0, constantSeg, ValueLayout.JAVA_BYTE, 0, constant.length);
        MemorySegment cv = arena.allocate(16, 8);
        writeView(cv, 0, constantSeg, 0, constant.length, 0);
        constantHead = cv.get(LONG, 0);
        constantTail = cv.get(LONG, 8);
        hashes = new int[N];
        verify();
    }

    /**
     * Both layouts must give the same answers, or the timing compares different
     * work.
     */
    private void verify() {
        if (equals_offsets() != equals_views()) {
            throw new IllegalStateException("equals differs: " + equals_offsets() + " vs " + equals_views());
        }
        if (compare_offsets() != compare_views()) {
            throw new IllegalStateException("compare differs: " + compare_offsets() + " vs " + compare_views());
        }
        hash_offsets();
        int[] a = hashes.clone();
        hash_views();
        if (!java.util.Arrays.equals(a, hashes)) {
            throw new IllegalStateException("hashes differ");
        }
    }

    private String value(Random rnd, int i) {
        switch (mix) {
            case "codes":
                {
                    int len = 2 + rnd.nextInt(11); // 2..12: all inline
                    StringBuilder sb = new StringBuilder();
                    for (int k = 0; k < len; k++) {
                        sb.append((char) ('A' + rnd.nextInt(26)));
                    }
                    return sb.toString();
                }
            case "ids":
                { // TPC-DS business keys: AAAAAAAA + 8 base-16-ish letters
                    StringBuilder sb = new StringBuilder("AAAAAAAA");
                    long v = rnd.nextInt(1 << 20);
                    for (int k = 0; k < 8; k++) {
                        sb.append((char) ('A' + (int) (v & 15)));
                        v >>>= 4;
                    }
                    return sb.toString();
                }
            case "names":
                {
                    int len = 20 + rnd.nextInt(41);
                    StringBuilder sb = new StringBuilder();
                    for (int k = 0; k < len; k++) {
                        sb.append(k % 7 == 6 ? ' ' : (char) ('a' + rnd.nextInt(26)));
                    }
                    return sb.toString();
                }
            default:
                { // urls
                    int len = 10 + rnd.nextInt(50);
                    StringBuilder sb = new StringBuilder("http://www.");
                    for (int k = 0; k < len; k++) {
                        sb.append((char) ('a' + rnd.nextInt(26)));
                    }
                    return sb.append(".com").toString();
                }
        }
    }

    /**
     * One view at {@code out + at}: a string of {@code len} bytes at {@code src
     * + start}, offset {@code start}.
     */
    private static void writeView(MemorySegment out, long at, MemorySegment src,
            long start, int len, int offset) {
        out.set(LONG, at, 0L);
        out.set(LONG, at + 8, 0L);
        out.set(INT, at, len);
        if (len <= 12) {
            MemorySegment.copy(src, start, out, at + 4, len);
        } else {
            MemorySegment.copy(src, start, out, at + 4, 4);
            out.set(INT, at + 12, offset);
        }
    }

    /**
     * Views over an offsets column: the conversion a scan would pay (the
     * decoder writes offsets).
     */
    private void buildViews(MemorySegment offsets, int n, MemorySegment out) {
        for (int i = 0; i < n; i++) {
            int s = offsets.get(VectorBuffers.LE_INT, (long) i << 2);
            int e = offsets.get(VectorBuffers.LE_INT, (long) (i + 1) << 2);
            writeView(out, (long) i << 4, data, s,
                    e - s, s);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    // ------------------------------------------------------------ gather (join output)

    @Benchmark
    public int gather_offsets() {
        GatherKernels.gatherUtf8(in, idx, 0, N, outOffsets, outData,
                null);
        return outOffsets.get(VectorBuffers.LE_INT, (long) N << 2);
    }

    @Benchmark
    public long gather_views() {
        // 16 bytes a row; a long string keeps pointing into its source.
        for (int o = 0; o < N; o++) {
            long src = (long) idx[o] << 4;
            long dst = (long) o << 4;
            outViews.set(LONG, dst, views.get(LONG, src));
            outViews.set(LONG, dst + 8, views.get(LONG, src + 8));
        }
        return outViews.get(LONG, 0);
    }

    // ------------------------------------------------------------ equality with a constant (filter)

    @Benchmark
    public int equals_offsets() {
        MemorySegment off = in.offsets();
        int len = constant.length;
        int hits = 0;
        for (int i = 0; i < N; i++) {
            int s = off.get(VectorBuffers.LE_INT, (long) i << 2);
            int e = off.get(VectorBuffers.LE_INT, (long) (i + 1) << 2);
            if (e - s == len && MemorySegment.mismatch(data, s, e, constantSeg, 0, len) < 0) {
                hits++;
            }
        }
        return hits;
    }

    @Benchmark
    public int equals_views() {
        int len = constant.length;
        int hits = 0;
        for (int i = 0; i < N; i++) {
            long at = (long) i << 4;
            if (views.get(LONG, at) != constantHead) {
                continue; // length or prefix differ: most rows end here
            }
            if (len <= 12) {
                if (views.get(LONG, at + 8) == constantTail) {
                    hits++;
                }
            } else {
                int s = views.get(INT, at + 12);
                if (MemorySegment.mismatch(data, s, s + len, constantSeg, 0,
                        len)
                        < 0) {
                    hits++;
                }
            }
        }
        return hits;
    }

    // ------------------------------------------------------------ ordering compare (sort): row i vs idx[i]

    @Benchmark
    public int compare_offsets() {
        MemorySegment off = in.offsets();
        int acc = 0;
        for (int i = 0; i < N; i++) {
            int j = idx[i];
            long xs = off.get(VectorBuffers.LE_INT, (long) i << 2);
            long xe = off.get(VectorBuffers.LE_INT, (long) (i + 1) << 2);
            long ys = off.get(VectorBuffers.LE_INT, (long) j << 2);
            long ye = off.get(VectorBuffers.LE_INT, (long) (j + 1) << 2);
            acc += Integer.signum(StringCompareKernels.compareBytes(data, xs, xe, data, ys, ye));
        }
        return acc;
    }

    @Benchmark
    public int compare_views() {
        int acc = 0;
        for (int i = 0; i < N; i++) {
            long a = (long) i << 4;
            long b = (long) idx[i] << 4;
            // The 4-byte prefix (big-endian order) decides most pairs.
            int pa = Integer.reverseBytes(views.get(INT, a + 4));
            int pb = Integer.reverseBytes(views.get(INT, b + 4));
            int c = Integer.compareUnsigned(pa, pb);
            if (c == 0) {
                int la = views.get(INT, a);
                int lb = views.get(INT, b);
                long sa = la <= 12 ? -1 : views.get(INT, a + 12);
                long sb = lb <= 12 ? -1 : views.get(INT, b + 12);
                MemorySegment xa = sa < 0 ? views : data;
                MemorySegment xb = sb < 0 ? views : data;
                long xs = sa < 0 ? a + 4 : sa;
                long ys = sb < 0 ? b + 4 : sb;
                c = StringCompareKernels.compareBytes(xa, xs, xs + la, xb, ys,
                        ys + lb);
            }
            acc += Integer.signum(c);
        }
        return acc;
    }

    // ------------------------------------------------------------ hashing (join and aggregate keys)

    @Benchmark
    public int hash_offsets() {
        MemorySegment off = in.offsets();
        for (int i = 0; i < N; i++) {
            int s = off.get(VectorBuffers.LE_INT, (long) i << 2);
            int e = off.get(VectorBuffers.LE_INT, (long) (i + 1) << 2);
            hashes[i] = HashKernels.hashBytes(data, s, e - s);
        }
        return hashes[N - 1];
    }

    @Benchmark
    public int hash_views() {
        // The same hash of the bytes (a key must hash alike in either layout): inline bytes from the view.
        for (int i = 0; i < N; i++) {
            long at = (long) i << 4;
            int len = views.get(INT, at);
            hashes[i] = len <= 12 ? HashKernels.hashBytes(views, at + 4, len) : HashKernels.hashBytes(data, views.get(INT, at + 12), len);
        }
        return hashes[N - 1];
    }

    // ------------------------------------------------------------ building views (what the scan would add)

    @Benchmark
    public long build_views() {
        MemorySegment off = in.offsets();
        for (int i = 0; i < N; i++) {
            int s = off.get(VectorBuffers.LE_INT, (long) i << 2);
            int e = off.get(VectorBuffers.LE_INT, (long) (i + 1) << 2);
            writeView(outViews, (long) i << 4, data, s,
                    e - s, s);
        }
        return outViews.get(LONG, 0);
    }
}
