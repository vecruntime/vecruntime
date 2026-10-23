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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Distinct UTF8 values of any length, stored contiguously so the dictionary
 * reads as a UTF8 VectorBuffers: the group table's per-key dictionary and the
 * shuffle writer's staging dictionary (#377). Each entry is keyed by its length
 * and a 64-bit fingerprint (see PACKED_KEY_BYTES).
 */
public final class StringDictionary {

    /**
     * Values of up to this many bytes are keyed by their packed bytes (so
     * equality is a long compare); longer values by a 64-bit hash plus a byte
     * compare.
     */
    public static final int PACKED_KEY_BYTES = 8;

    private long[] bits = new long[64];
    private int[] lens = new int[64];
    private int[] ids = new int[64];
    private int mask = 63;
    private int size;
    private byte[] data = new byte[256];
    private int used;
    private int[] offsets = new int[33];
    private MemorySegment dataSegment = MemorySegment.ofArray(data);
    private MemorySegment offsetSegment = MemorySegment.ofArray(offsets);

    /**
     * Whether this JVM runs on aarch64, where the pool layout was measured
     * faster (#941 phase 2d).
     */
    static final boolean AARCH64 = "aarch64".equals(System.getProperty("os.arch"));

    /**
     * Value bytes from which a dictionary whose owner {@link #allowNative allows
     * it} moves into pool blocks (#941 phase 2d). {@code
     * KeyColumnStorageBenchmark} on Graviton4, pool against heap: a lookup
     * 1.29-3.28x at every size, the key output's gather 0.79-0.95x to 256K
     * values and 1.01-1.29x from 1M; on Skylake-SP, Sapphire Rapids, Granite
     * Rapids, Genoa and Turin the lookup is 0.39-0.90x and the gather
     * 0.31-0.50x, so there the default is never. 16 MiB -- a G1 humongous
     * array with 32 MiB regions -- is about 500K product-name values, past
     * the gather's crossover. Measured on Graviton4 only among aarch64 CPUs.
     */
    static final long NATIVE_DICT_BYTES = Long.getLong("vecruntime.agg.nativeDictBytes", AARCH64 ? 16L << 20 : Long.MAX_VALUE);

    /**
     * {@link #NATIVE_DICT_BYTES} unless a test moved it ({@link
     * #setNativeBytesForTests}).
     */
    private static long nativeBytesFrom = NATIVE_DICT_BYTES;

    /**
     * Package-private, for the tests: the size past which an allowed dictionary
     * moves; -1 restores the default.
     */
    static void setNativeBytesForTests(long bytes) {
        nativeBytesFrom = bytes < 0 ? NATIVE_DICT_BYTES : bytes;
    }

    private static final ValueLayout.OfLong N_LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt N_INT = ValueLayout.JAVA_INT;

    private boolean nativeAllowed;

    // Pool mode (all null on the heap): per id one 16-byte entry -- fingerprint, length, offset -- so a probe's
    // candidate is one line before the bytes; the id table; the offsets again, contiguous, for view(); the bytes.
    private MemorySegment nEntries;
    private MemorySegment nIds;
    private MemorySegment nOffsets;
    private MemorySegment nData;
    private Arena aEntries;
    private Arena aIds;
    private Arena aOffsets;
    private Arena aData;

    /**
     * Lets the values move into pool blocks past {@link #NATIVE_DICT_BYTES};
     * the owner must {@link #close}.
     */
    public void allowNative() {
        nativeAllowed = true;
    }

    /**
     * Whether the values live in pool blocks: {@link #bytes} is then
     * unavailable, use {@link #dataSegment}.
     */
    public boolean isNative() {
        return nData != null;
    }

    /**
     * The value bytes, wherever they live (heap or pool); valid until the next
     * insert.
     */
    public MemorySegment dataSegment() {
        return dataSegment;
    }

    /** Pool bytes held. */
    public long nativeBytes() {
        if (nData == null) {
            return 0L;
        }
        return nEntries.byteSize()
                + nIds.byteSize()
                + nOffsets.byteSize()
                + nData.byteSize();
    }

    /** Gives the pool blocks back; the dictionary is not used afterwards. */
    public void close() {
        for (Arena a : new Arena[] {aEntries, aIds, aOffsets, aData}) {
            if (a != null) {
                a.close();
            }
        }
        aEntries = aIds = aOffsets = aData = null;
        nEntries = nIds = nOffsets = nData = null;
    }

    private static MemorySegment alloc(long bytes, Arena a) {
        return a.allocate(Math.max(8L, bytes), 64);
    }

    /**
     * The heap arrays into pool blocks of the same capacity; the arrays are
     * dropped.
     */
    private void toNative() {
        int cap = bits.length;
        Arena ae = NativePool.shared().newArena();
        MemorySegment e = alloc(16L * cap, ae);
        for (int id = 0; id < size; id++) {
            e.setAtIndex(N_LONG, 2L * id, bits[id]);
            e.set(N_INT, 16L * id + 8, lens[id]);
            e.set(N_INT, 16L * id + 12, offsets[id]);
        }
        Arena ai = NativePool.shared().newArena();
        MemorySegment t = alloc(4L * ids.length, ai);
        MemorySegment.copy(ids, 0, t, N_INT, 0, ids.length);
        Arena ao = NativePool.shared().newArena();
        MemorySegment o = alloc(4L * offsets.length, ao);
        MemorySegment.copy(offsets, 0, o, N_INT, 0, offsets.length);
        Arena ad = NativePool.shared().newArena();
        MemorySegment d = alloc(data.length, ad);
        MemorySegment.copy(data, 0, d, ValueLayout.JAVA_BYTE, 0, used);
        nEntries = e;
        nIds = t;
        nOffsets = o;
        nData = d;
        aEntries = ae;
        aIds = ai;
        aOffsets = ao;
        aData = ad;
        dataSegment = d;
        offsetSegment = o;
        bits = null;
        lens = null;
        ids = null;
        offsets = null;
        data = null;
    }

    /**
     * {@code seg} regrown to at least {@code bytes}, contents kept; the old
     * block is given back.
     */
    private MemorySegment regrow(MemorySegment seg, Arena[] arena, long bytes) {
        Arena a = NativePool.shared().newArena();
        MemorySegment n = alloc(Math.max(bytes, 2 * seg.byteSize()), a);
        MemorySegment.copy(seg, 0, n, 0, seg.byteSize());
        arena[0].close();
        arena[0] = a;
        return n;
    }

    public int size() {
        return size;
    }

    /**
     * Direct index for single-byte values (TPC-H's flag columns), bypassing the
     * probe.
     */
    private final int[] singleByte = new int[256];

    public StringDictionary() {
        Arrays.fill(ids, -1);
        Arrays.fill(singleByte, -1);
    }

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /**
     * The 64-bit key of a value: its bytes packed little-endian when they fit
     * in a long, otherwise a multiply-xorshift mix over the bytes taken eight
     * at a time (a collision between two long values is caught by the byte
     * compare).
     */
    public static long fingerprint(byte[] src, int from, int len) {
        if (len <= PACKED_KEY_BYTES) {
            long packed = 0L;
            for (int b = len - 1; b >= 0; b--) {
                packed = (packed << 8) | (src[from + b] & 0xFFL);
            }
            return packed;
        }
        long h = 0x9E3779B97F4A7C15L ^ len;
        int end = from + len;
        int p = from;
        for (; p + 8 <= end; p += 8) {
            h = (h ^ (long) LONG_LE.get(src, p)) * 0xBF58476D1CE4E5B9L;
            h ^= h >>> 31;
        }
        if (p < end) {
            long tail = 0L;
            for (int b = end - 1; b >= p; b--) {
                tail = (tail << 8) | (src[b] & 0xFFL);
            }
            h = (h ^ tail) * 0x94D049BB133111EBL;
            h ^= h >>> 29;
        }
        return h;
    }

    /**
     * The id of the value, inserting it when absent if {@code insert}; -1 when
     * absent otherwise.
     */
    public int indexOf(long packed, int len, byte[] src,
                       int from, boolean insert) {
        if (len == 1) {
            int b = (int) packed; // 0..255
            int id = singleByte[b];
            if (id < 0) {
                id = probe(packed, len, src, from, insert);
                if (id >= 0) {
                    singleByte[b] = id;
                }
            }
            return id;
        }
        return probe(packed, len, src, from, insert);
    }

    /**
     * Per-caller byte scratch for {@link #indexOf(MemorySegment, long, int,
     * boolean, Scratch)}.
     */
    public static final class Scratch {
        byte[] bytes = new byte[64];
    }

    /**
     * {@link #indexOf} over a value that lives in a segment (an input
     * dictionary's entry).
     */
    public int indexOf(MemorySegment data, long start, int len,
                       boolean insert, Scratch s) {
        if (s.bytes.length < len) {
            s.bytes = new byte[Math.max(len, s.bytes.length * 2)];
        }
        MemorySegment.copy(data, ValueLayout.JAVA_BYTE, start, s.bytes, 0, len);
        return indexOf(fingerprint(s.bytes, 0, len), len, s.bytes, 0, insert);
    }

    /**
     * Forgets every value: ids start again from 0 (the shuffle writer, once
     * every pending row is flushed).
     */
    public void clear() {
        if (nData != null) {
            nIds.fill((byte) -1);
        } else {
            Arrays.fill(ids, -1);
        }
        Arrays.fill(singleByte, -1);
        size = 0;
        used = 0;
    }

    private int probe(long packed, int len, byte[] src,
                      int from, boolean insert) {
        int pos = HashKernels.finish(HashKernels.mix32(HashKernels.fold(packed), len)) & mask;
        if (nData != null) {
            return probeNative(packed, len, src, from, insert, pos);
        }
        while (true) {
            int id = ids[pos];
            if (id < 0) {
                return insert ? insert(packed, len, src, from, pos) : -1;
            }
            if (bits[id] == packed
                    && lens[id] == len
                    && (len <= PACKED_KEY_BYTES || sameBytes(id, src, from, len))) {
                return id;
            }
            pos = (pos + 1) & mask;
        }
    }

    private int probeNative(long packed, int len, byte[] src,
                            int from, boolean insert, int pos) {
        MemorySegment e = nEntries;
        MemorySegment t = nIds;
        while (true) {
            int id = t.getAtIndex(N_INT, pos);
            if (id < 0) {
                return insert ? insertNative(packed, len, src, from, pos) : -1;
            }
            long at = 16L * id;
            if (e.get(N_LONG, at) == packed
                    && e.get(N_INT, at + 8) == len
                    && (len <= PACKED_KEY_BYTES || MemorySegment.mismatch(nData, e.get(N_INT, at + 12), e.get(N_INT, at + 12) + (long) len,
                            MemorySegment.ofArray(src), from, from + (long) len)
                            < 0)) {
                return id;
            }
            pos = (pos + 1) & mask;
        }
    }

    private int insertNative(long packed, int len, byte[] src,
            int from, int pos) {
        int id = size;
        Arena[] box = new Arena[1];
        if (16L * (id + 1) > nEntries.byteSize()) {
            box[0] = aEntries;
            nEntries = regrow(nEntries, box, 16L * (id + 1));
            aEntries = box[0];
        }
        if (4L * (id + 2) > nOffsets.byteSize()) {
            box[0] = aOffsets;
            nOffsets = regrow(nOffsets, box, 4L * (id + 2));
            aOffsets = box[0];
            offsetSegment = nOffsets;
        }
        if ((long) used + len > nData.byteSize()) {
            box[0] = aData;
            nData = regrow(nData, box, (long) used + len);
            aData = box[0];
            dataSegment = nData;
        }
        long at = 16L * id;
        nEntries.set(N_LONG, at, packed);
        nEntries.set(N_INT, at + 8, len);
        nEntries.set(N_INT, at + 12, used);
        MemorySegment.copy(src, from, nData, ValueLayout.JAVA_BYTE, used, len);
        used += len;
        nOffsets.setAtIndex(N_INT, id + 1, used);
        nIds.setAtIndex(N_INT, pos, id);
        size++;
        if (size * 2L > mask + 1L) {
            rehashNative();
        }
        return id;
    }

    private void rehashNative() {
        long slots = 2L * (mask + 1L);
        Arena a = NativePool.shared().newArena();
        MemorySegment t = alloc(4L * slots, a);
        t.fill((byte) -1);
        int newMask = (int) (slots - 1);
        for (int id = 0; id < size; id++) {
            long at = 16L * id;
            int pos = HashKernels.finish(HashKernels.mix32(HashKernels.fold(nEntries.get(N_LONG, at)), nEntries.get(N_INT, at + 8))) & newMask;
            while (t.getAtIndex(N_INT, pos) >= 0) {
                pos = (pos + 1) & newMask;
            }
            t.setAtIndex(N_INT, pos, id);
        }
        aIds.close();
        aIds = a;
        nIds = t;
        mask = newMask;
    }

    private boolean sameBytes(int id, byte[] src, int from,
            int len) {
        int off = offsets[id];
        return Arrays.equals(data, off, off + len, src, from,
                from + len);
    }

    private int insert(long packed, int len, byte[] src,
                       int from, int pos) {
        int id = size;
        if (id == bits.length) {
            bits = Arrays.copyOf(bits, id * 2);
            lens = Arrays.copyOf(lens, id * 2);
        }
        if (id + 1 >= offsets.length) {
            offsets = Arrays.copyOf(offsets, offsets.length * 2);
            offsetSegment = MemorySegment.ofArray(offsets);
        }
        if (used + len > data.length) {
            data = Arrays.copyOf(data,
                    Math.max(data.length * 2, used + len));
            dataSegment = MemorySegment.ofArray(data);
            if (nativeAllowed && data.length >= nativeBytesFrom) {
                toNative();
                return insertNative(packed, len, src, from, pos);
            }
        }
        bits[id] = packed;
        lens[id] = len;
        System.arraycopy(src, from, data, used, len);
        used += len;
        offsets[id + 1] = used;
        ids[pos] = id;
        size++;
        if (size * 2 > ids.length) {
            rehash();
        }
        return id;
    }

    private void rehash() {
        int[] newIds = new int[ids.length * 2];
        Arrays.fill(newIds, -1);
        int newMask = newIds.length - 1;
        for (int id = 0; id < size; id++) {
            int pos = HashKernels.finish(HashKernels.mix32(HashKernels.fold(bits[id]), lens[id])) & newMask;
            while (newIds[pos] >= 0) {
                pos = (pos + 1) & newMask;
            }
            newIds[pos] = id;
        }
        ids = newIds;
        mask = newMask;
    }

    /**
     * The dictionary as a UTF8 column; valid until the next {@link #indexOf}
     * that inserts.
     */
    public VectorBuffers view() {
        return SegmentVectorBuffers.utf8(size, null, offsetSegment, dataSegment);
    }

    public int offset(int id) {
        return nData == null ? offsets[id] : nOffsets.getAtIndex(N_INT, id);
    }

    public int length(int id) {
        if (nData != null) {
            return nOffsets.getAtIndex(N_INT, id + 1) - nOffsets.getAtIndex(N_INT, id);
        }
        return offsets[id + 1] - offsets[id];
    }

    /**
     * The value bytes on the heap; not for a dictionary in the pool ({@link
     * #isNative}).
     */
    public byte[] bytes() {
        if (nData != null) {
            throw new IllegalStateException("the dictionary is in the pool: read dataSegment()");
        }
        return data;
    }

    /** Bytes of the distinct values held. */
    public long valueBytes() {
        return used;
    }

    public long memoryBytes() {
        if (nData != null) {
            return 4L * singleByte.length; // the rest is in the pool (nativeBytes)
        }
        return 8L * bits.length
                + 4L * lens.length
                + 4L * ids.length
                + data.length
                + 4L * offsets.length
                + 4L * singleByte.length;
    }
}
