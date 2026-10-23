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
package io.vecruntime.benchmarks;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.NativePool;
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
 * #941 phase 2c: where the grouped aggregate's per-group keys live. At 10 TB
 * q67's live heap is {@code GroupKeyTable}'s per-group key columns (the
 * dictionary id of each UTF8 key, the INT32 keys) and the UTF8 keys'
 * dictionaries ({@code StringDictionary}: value bytes, offsets and its own
 * open-addressing table). This measures, heap arrays against pool blocks of
 * the same layout:
 *
 * <ul>
 *   <li>{@code keysHeap} / {@code keysNative}: the probe's key compare -- for
 *       a batch of rows whose group is known, compare five INT32 key columns
 *       of that group (q67's shape: four UTF8 ids and a year) with the row's;
 *   <li>{@code dictHeap} / {@code dictNative}: a dictionary lookup of a value
 *       already in it -- hash, probe the id table, compare length, fingerprint
 *       and, past eight bytes, the bytes (product names: 16-48 bytes).
 * </ul>
 *
 * {@code size} is the number of groups for the key columns and the number of
 * distinct values for the dictionary; rows pick them uniformly.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class KeyColumnStorageBenchmark {

    static final int N = 4096;
    static final int COLS = 5;
    static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;

    @Param({"1024", "16384", "262144", "1048576", "4194304"})
    int size;

    Arena pool;
    final int[] rowGroup = new int[N];
    final int[][] rowKeys = new int[COLS][N];

    int[][] keys; // per column, per group
    MemorySegment[] nKeys;

    // dictionary, heap
    long[] bits;
    int[] lens;
    int[] ids;
    int[] offsets;
    byte[] data;
    int dmask;
    // dictionary, pool
    MemorySegment nBits;
    MemorySegment nLens;
    MemorySegment nIds;
    MemorySegment nOffsets;
    MemorySegment nData;
    // the probed values, in one row buffer
    byte[] rowBytes;
    int[] rowOff = new int[N + 1];
    long[] rowFp = new long[N];

    static long fingerprint(byte[] src, int from, int len) {
        long h = 0x9E3779B97F4A7C15L ^ len;
        for (int p = from; p < from + len; p++) {
            h = (h ^ (src[p] & 0xFFL)) * 0xBF58476D1CE4E5B9L;
            h ^= h >>> 31;
        }
        return h;
    }

    static int slotHash(long fp, int len) {
        long h = (fp ^ len) * 0x9E3779B97F4A7C15L;
        return (int) (h ^ (h >>> 32));
    }

    @Setup(Level.Trial)
    public void setup() {
        pool = NativePool.shared().newArena();
        Random rnd = new Random(941);
        keys = new int[COLS][size];
        nKeys = new MemorySegment[COLS];
        for (int c = 0; c < COLS; c++) {
            nKeys[c] = pool.allocate(4L * size, 64);
            for (int g = 0; g < size; g++) {
                int v = rnd.nextInt(c == COLS - 1 ? 8 : Math.max(2, size / 4));
                keys[c][g] = v;
                nKeys[c].setAtIndex(INT, g, v);
            }
        }
        for (int i = 0; i < N; i++) {
            int g = rnd.nextInt(size);
            rowGroup[i] = g;
            for (int c = 0; c < COLS; c++) {
                rowKeys[c][i] = keys[c][g];
            }
        }
        // the dictionary: size distinct values of 16-48 bytes
        int cap = Integer.highestOneBit(size) << 2; // load 0.25-0.5, as the dictionary keeps
        dmask = cap - 1;
        ids = new int[cap];
        Arrays.fill(ids, -1);
        bits = new long[size];
        lens = new int[size];
        offsets = new int[size + 1];
        byte[][] values = new byte[size][];
        long total = 0;
        for (int v = 0; v < size; v++) {
            values[v] = ("product-" + v + "-" + "x".repeat(8 + rnd.nextInt(32))).getBytes(StandardCharsets.UTF_8);
            total += values[v].length;
        }
        data = new byte[(int) total];
        int used = 0;
        for (int v = 0; v < size; v++) {
            byte[] b = values[v];
            System.arraycopy(b, 0, data, used, b.length);
            offsets[v] = used;
            used += b.length;
            offsets[v + 1] = used;
            lens[v] = b.length;
            bits[v] = fingerprint(b, 0, b.length);
            int pos = slotHash(bits[v], b.length) & dmask;
            while (ids[pos] >= 0) {
                pos = (pos + 1) & dmask;
            }
            ids[pos] = v;
        }
        nBits = pool.allocate(8L * size, 64);
        nLens = pool.allocate(4L * size, 64);
        nIds = pool.allocate(4L * cap, 64);
        nOffsets = pool.allocate(4L * (size + 1), 64);
        nData = pool.allocate(Math.max(1, total), 64);
        MemorySegment.copy(bits, 0, nBits, LONG, 0, size);
        MemorySegment.copy(lens, 0, nLens, INT, 0, size);
        MemorySegment.copy(ids, 0, nIds, INT, 0, cap);
        MemorySegment.copy(offsets, 0, nOffsets, INT, 0,
                size + 1);
        MemorySegment.copy(data, 0, nData, ValueLayout.JAVA_BYTE, 0, data.length);
        // the rows: copies of existing values (a dictionary hit), in a row buffer of their own
        int rowTotal = 0;
        int[] pick = new int[N];
        for (int i = 0; i < N; i++) {
            pick[i] = rnd.nextInt(size);
            rowTotal += values[pick[i]].length;
        }
        rowBytes = new byte[rowTotal];
        int at = 0;
        for (int i = 0; i < N; i++) {
            byte[] b = values[pick[i]];
            System.arraycopy(b, 0, rowBytes, at, b.length);
            rowOff[i] = at;
            at += b.length;
            rowFp[i] = bits[pick[i]];
        }
        rowOff[N] = at;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.close();
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int keysHeap() {
        int same = 0;
        int[][] k = keys;
        for (int i = 0; i < N; i++) {
            int g = rowGroup[i];
            boolean eq = true;
            for (int c = 0; c < COLS; c++) {
                eq &= k[c][g] == rowKeys[c][i];
            }
            same += eq ? 1 : 0;
        }
        return same;
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int keysNative() {
        int same = 0;
        MemorySegment[] k = nKeys;
        for (int i = 0; i < N; i++) {
            long g = rowGroup[i];
            boolean eq = true;
            for (int c = 0; c < COLS; c++) {
                eq &= k[c].getAtIndex(INT, g) == rowKeys[c][i];
            }
            same += eq ? 1 : 0;
        }
        return same;
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int dictHeap() {
        int found = 0;
        for (int i = 0; i < N; i++) {
            int from = rowOff[i];
            int len = rowOff[i + 1] - from;
            long fp = rowFp[i];
            int pos = slotHash(fp, len) & dmask;
            while (true) {
                int id = ids[pos];
                if (id < 0) {
                    break;
                }
                if (bits[id] == fp && lens[id] == len) {
                    int off = offsets[id];
                    if (Arrays.equals(data, off, off + len, rowBytes, from,
                            from + len)) {
                        found += id;
                        break;
                    }
                }
                pos = (pos + 1) & dmask;
            }
        }
        return found;
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public int dictNative() {
        int found = 0;
        MemorySegment rows = MemorySegment.ofArray(rowBytes);
        for (int i = 0; i < N; i++) {
            int from = rowOff[i];
            int len = rowOff[i + 1] - from;
            long fp = rowFp[i];
            int pos = slotHash(fp, len) & dmask;
            while (true) {
                int id = nIds.getAtIndex(INT, pos);
                if (id < 0) {
                    break;
                }
                if (nBits.getAtIndex(LONG, id) == fp && nLens.getAtIndex(INT, id) == len) {
                    long off = nOffsets.getAtIndex(INT, id);
                    if (MemorySegment.mismatch(nData, off, off + len, rows, from,
                            from + len)
                            < 0) {
                        found += id;
                        break;
                    }
                }
                pos = (pos + 1) & dmask;
            }
        }
        return found;
    }
}
