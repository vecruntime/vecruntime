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
import java.util.Arrays;
import java.util.BitSet;

/**
 * Open-addressing hash table from grouping-key tuples to dense group ids,
 * accumulated across the batches of one task. Keys are stored column-wise
 * (ints, longs/double bits, UTF-8 bytes) so they can be written back out as
 * columns when the partial aggregate is emitted.
 *
 * <p>UTF8 keys are stored as int ids (#377): each UTF8 key column owns a
 * dictionary of the distinct values seen by this table, a row's string is
 * mapped to its id once per batch (once per entry of the input's dictionary
 * when it has one), and from there the table hashes, compares and stores ints
 * -- five string keys probe like five int keys. The dictionary is what the
 * aggregate emits with the ids, and what the shuffle writer stages, so a string
 * is hashed once in the whole stage.
 */
public final class GroupKeyTable {

    private static final int INITIAL_CAPACITY = 1024;

    /**
     * Largest per-thread scratch array (elements) that {@link
     * #releaseThreadScratch} keeps for the thread's next task: enough for any
     * batch, so batch-sized scratch is still reused, while the arrays a build
     * side or a large aggregate grew to are dropped.
     */
    static final int KEEP_ELEMENTS = 1 << 16;

    /**
     * Ends the calling thread's use of the per-thread scratch ({@code Bound},
     * {@code ProbeKeys}, {@code IdScratch}, #667): clears every reference to a
     * batch, dictionary or table and drops the pooled arrays larger than {@link
     * #KEEP_ELEMENTS}. The pools only grow while in use -- to the largest build
     * side or batch the thread has seen -- and a task thread lives across tasks
     * and queries, so without this a thread kept its largest scratch, and
     * through {@code IdScratch}'s last table that table's own arrays, after its
     * query ended (about 0.8 GB live after TPC-DS SF10 on {@code local[8]}).
     * Called at the end of every task, on the task's thread; safe to call at
     * any time no table is being assigned or probed on this thread.
     */
    public static void releaseThreadScratch() {
        Bound.release();
        ProbeKeys.release();
        IdScratch.release();
    }

    /**
     * For tests: the calling thread's scratch as (elements in its largest
     * pooled array, number of batch / dictionary / table references it holds).
     */
    static long[] threadScratchFootprint() {
        long largest = 0;
        int refs = 0;
        Bound b = Bound.SCRATCH.get();
        largest = Math.max(
                largest,
                Math.max(largest(b.intPool),
                        Math.max(largest(b.longPool), largest(b.wordPool))));
        refs += nonNull(b.keys)
                + nonNull(b.data)
                + nonNull(b.offsets)
                + nonNull(b.validity)
                + nonNull(b.ids)
                + nonNull(b.dictData)
                + nonNull(b.dictOffsets)
                + nonNull(b.ints)
                + nonNull(b.longs)
                + nonNull(b.validWords);
        ProbeKeys p = ProbeKeys.SCRATCH.get();
        largest = Math.max(
                largest,
                Math.max(largest(p.ints),
                        Math.max(largest(p.longs), largest(p.validity))));
        IdScratch s = IdScratch.SCRATCH.get();
        largest = Math.max(largest,
                Math.max(largest(s.ids), largest(s.entries)));
        largest = Math.max(largest, Math.max(s.offs.length, Math.max(s.bytes.length, s.wordScratch.length)));
        refs += nonNull(s.keys) + nonNull(s.lastDict) + nonNull(s.lastTable);
        return new long[] {largest, refs};
    }

    private static long largest(int[][] pool) {
        long m = 0;
        for (int[] a : pool) {
            m = Math.max(m, a == null ? 0 : a.length);
        }
        return m;
    }

    private static long largest(long[][] pool) {
        long m = 0;
        for (long[] a : pool) {
            m = Math.max(m, a == null ? 0 : a.length);
        }
        return m;
    }

    private static int nonNull(Object[] refs) {
        int n = 0;
        for (Object o : refs) {
            if (o != null) {
                n++;
            }
        }
        return n;
    }

    private static void trim(int[][] pool) {
        for (int c = 0; c < pool.length; c++) {
            if (pool[c] != null && pool[c].length > KEEP_ELEMENTS) {
                pool[c] = null;
            }
        }
    }

    private static void trim(long[][] pool) {
        for (int c = 0; c < pool.length; c++) {
            if (pool[c] != null && pool[c].length > KEEP_ELEMENTS) {
                pool[c] = null;
            }
        }
    }

    private final VecType[] types;

    /**
     * The open-addressing slots while the table is below {@link #PACKED_SLOTS}:
     * the group id or -1, the group's row hash in {@link #groupHashes}. Null
     * once the table is packed ({@link #slots}) or in the pool ({@link
     * #slotSeg}).
     */
    private int[] splitSlots;

    /** Row hash per group, for {@link #splitSlots} only; null once packed. */
    private int[] groupHashes;

    /**
     * The open-addressing slots from {@link #PACKED_SLOTS} on, one packed entry
     * each: the group's row hash in the high 32 bits, its id in the low 32,
     * {@link #EMPTY} for an empty slot (#941 phase 2b). A probe compares the
     * hash from the slot it already read instead of loading it from {@link
     * #groupHashes} -- one cache line a probe instead of two, which past the
     * caches is the miss that counts. Null below the threshold and while the
     * entries are in the pool ({@link #slotSeg}).
     */
    private long[] slots;

    /**
     * The same entries in a {@link NativePool} block once a table that {@link
     * #allowNative allows it} reaches {@link #NATIVE_FAST_SLOTS} slots, as the
     * inline pairs do: off the heap, charged to the task. Null otherwise.
     */
    private MemorySegment slotSeg;

    private Arena slotArena;
    private int slotCount; // slots in the table, wherever they live
    private int mask;
    private int size;
    private int groupCap = INITIAL_CAPACITY; // capacity of the per-group key arrays

    /**
     * Integer keys inline in the slot array (#677): {@code fast[2 * pos]} is
     * the packed key of the group in slot {@code pos} -- one INT32 or INT64
     * value, or two INT32 values high and low -- and {@code fast[2 * pos + 1]}
     * its id ({@code -1} empty), mirroring {@link #slots}. A probe then reads
     * one cache line instead of the slot, the group's hash and each key column
     * of the group, which with a million groups were three or four misses per
     * row (q23a's partial aggregates at 1 TB: 43-92 ns per row against 14-18
     * with the table in cache). Only for one or two INT32 keys or one INT64
     * key, and only while no group has a null key: the first null drops it
     * and the table probes the general way from then on.
     */
    private long[] fast;

    /**
     * #941 phase 2: {@link #fast} moved into a {@link NativePool} block once the
     * table reaches {@link #NATIVE_FAST_SLOTS} slots (same layout, key then
     * id): the heap keeps nothing of it, and the pool charges it to the task.
     * {@code GroupTableStorageBenchmark}, against these heap pairs: Graviton 4
     * 1.25-1.48x from 256K groups, Skylake-SP and Sapphire Rapids level
     * (0.94-1.04x from 1M groups) and slower in cache, hence the size floor.
     * Null whenever {@link #fast} is in use or the table is not inline.
     */
    private MemorySegment fastSeg;

    private Arena fastArena;

    /**
     * Whether this table may move its pairs into the pool: only an owner that
     * closes it ({@link #close}) turns it on -- the grouped aggregate does, a
     * join's build table does not.
     */
    private boolean nativeAllowed;

    /**
     * The heap pairs crossed {@link #NATIVE_FAST_SLOTS} during the last assign:
     * move them before the next.
     */
    private boolean pendingNative;

    /** {@link #fast} into a pool block, same slots; the heap array is dropped. */
    private void moveFastToPool() {
        pendingNative = false;
        if (fast == null) {
            return; // a null key group ended the inline path meanwhile
        }
        int slotCount = fast.length / 2;
        allocFastSeg(slotCount);
        MemorySegment.copy(fast, 0, fastSeg, PAIR, 0, fast.length);
        fast = null;
    }

    /**
     * Lets the inline pairs move into the pool past {@link #NATIVE_FAST_SLOTS};
     * the caller must {@link #close}.
     */
    public void allowNative() {
        nativeAllowed = true;
    }

    /**
     * Slots from which an inline table keeps its pairs in the pool: 32 MiB of
     * pairs.
     */
    static final int NATIVE_FAST_SLOTS = Integer.getInteger("vecruntime.agg.nativeFastSlots", 1 << 21);

    /**
     * Slots from which the table packs hash and id into one entry ({@link
     * #slots}). {@code GroupTableStorageBenchmark}, packed against split, 3
     * forks: from 256K groups (512K slots at the benchmark's 0.5 load) 1.08-1.69x
     * on Graviton4, Skylake-SP, Sapphire Rapids, Granite Rapids, Genoa and
     * Turin, 1.43-2.12x from 1M; below it, in cache, the split arrays are
     * faster on every recent x86 (Turin 0.64x at 1K groups, Granite Rapids
     * 0.84-0.88x to 64K) and only Graviton and Skylake gain.
     */
    static final int PACKED_SLOTS = Integer.getInteger("vecruntime.agg.packedSlots", 1 << 19);

    /** An empty slot: no group has id -1. */
    private static final long EMPTY = -1L;

    private static long entryOf(int hash, int gid) {
        return ((long) hash << 32) | (gid & 0xFFFF_FFFFL);
    }

    /** Slot {@code pos} as a packed entry, whatever the layout. */
    private long entry(int pos) {
        if (splitSlots != null) {
            int gid = splitSlots[pos];
            return gid < 0 ? EMPTY : entryOf(groupHashes[gid], gid);
        }
        return slotSeg == null ? slots[pos] : slotSeg.getAtIndex(PAIR, pos);
    }

    private void setEntry(int pos, int hash, int gid) {
        if (splitSlots != null) {
            splitSlots[pos] = gid;
            groupHashes[gid] = hash;
        } else if (slotSeg == null) {
            slots[pos] = entryOf(hash, gid);
        } else {
            slotSeg.setAtIndex(PAIR, pos, entryOf(hash, gid));
        }
    }

    /** Every slot empty, whatever the layout. */
    private void clearSlots() {
        if (splitSlots != null) {
            Arrays.fill(splitSlots, -1);
        } else if (slotSeg == null) {
            Arrays.fill(slots, EMPTY);
        } else {
            slotSeg.fill((byte) -1);
        }
    }

    /** The first empty slot of {@code hash}'s chain. */
    private int emptySlot(int hash) {
        int pos = hash & mask;
        while (entry(pos) != EMPTY) {
            pos = (pos + 1) & mask;
        }
        return pos;
    }

    private static final ValueLayout.OfLong PAIR = ValueLayout.JAVA_LONG;

    /**
     * The groups' keys, one row of {@link #width} ints per group, column {@code
     * c} at {@link #keyOff}{@code [c]} (#941 phase 2c): INT32 and BOOL (0/1) and
     * a UTF8 key's dictionary id take one int, INT64 and FLOAT64 (raw bits) two,
     * DECIMAL128 four (low limb, then high). A probe's key compare then reads
     * one line per group instead of one per column -- {@code
     * KeyColumnStorageBenchmark}, five INT32 keys, against one array per
     * column: 1.43-4.39x on the heap and 1.29-4.08x in a pool block, on
     * Graviton4, Skylake-SP, Sapphire Rapids, Granite Rapids, Genoa and Turin,
     * from 1K to 4M groups. Null while the rows are in the pool ({@link
     * #rowSeg}).
     */
    private int[] rows;

    /**
     * The rows in a {@link NativePool} block once a table that {@link
     * #allowNative allows it} would hold {@link #NATIVE_ROW_BYTES} of them: off
     * the heap, charged to the task. Null otherwise.
     */
    private MemorySegment rowSeg;

    private Arena rowArena;
    private final int width; // ints per group row
    private final int[] keyOff; // per column: its first int in a row

    /**
     * Row bytes from which an allowed table keeps its rows in the pool: an
     * array this size is a humongous object for G1 with 32 MiB regions (half a
     * region), allocated and freed as whole regions and never moved.
     */
    static final long NATIVE_ROW_BYTES = Long.getLong("vecruntime.agg.nativeRowBytes", 16L << 20);

    private static final ValueLayout.OfInt ROW_INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfLong ROW_LONG = ValueLayout.JAVA_LONG_UNALIGNED;

    private long rowIndex(int c, int gid) {
        return (long) gid * width + keyOff[c];
    }

    /** Int {@code k} of column {@code c}'s key of group {@code gid}. */
    private int keyInt(int c, int gid) {
        long i = rowIndex(c, gid);
        return rowSeg == null ? rows[(int) i] : rowSeg.getAtIndex(ROW_INT, i);
    }

    private void setKeyInt(int c, int gid, int v) {
        long i = rowIndex(c, gid);
        if (rowSeg == null) {
            rows[(int) i] = v;
        } else {
            rowSeg.setAtIndex(ROW_INT, i, v);
        }
    }

    /**
     * The long at int {@code at} of column {@code c}'s key of group {@code gid}
     * (0: INT64/low limb, 2: high).
     */
    private long keyLong(int c, int gid, int at) {
        long i = rowIndex(c, gid) + at;
        if (rowSeg == null) {
            int j = (int) i;
            return (rows[j] & 0xFFFF_FFFFL) | ((long) rows[j + 1] << 32);
        }
        return rowSeg.get(ROW_LONG, 4L * i);
    }

    private void setKeyLong(int c, int gid, int at,
                            long v) {
        long i = rowIndex(c, gid) + at;
        if (rowSeg == null) {
            int j = (int) i;
            rows[j] = (int) v;
            rows[j + 1] = (int) (v >>> 32);
        } else {
            rowSeg.set(ROW_LONG, 4L * i, v);
        }
    }

    /**
     * Room for {@code cap} groups' rows, copying the first {@link #size}; moves
     * them to the pool past the size.
     */
    private void growRows(int cap) {
        long ints = (long) cap * width;
        long used = (long) size * width;
        boolean toPool = rowSeg != null
                || (nativeAllowed && GroupedAccumulators.nativeState() && 4L * ints >= NATIVE_ROW_BYTES);
        if (toPool) {
            Arena a = NativePool.shared().newArena();
            MemorySegment seg = a.allocate(4L * ints, 64);
            if (rowSeg != null) {
                MemorySegment.copy(rowSeg, 0, seg, 0, 4L * used);
            } else {
                MemorySegment.copy(rows, 0, seg, ROW_INT, 0,
                        (int) used);
            }
            if (rowArena != null) {
                rowArena.close();
            }
            rows = null;
            rowSeg = seg;
            rowArena = a;
        } else {
            if (ints > Integer.MAX_VALUE - 8) {
                throw new IllegalStateException(cap + " groups of " + width + " key ints do not fit one heap array");
            }
            rows = Arrays.copyOf(rows, (int) ints);
        }
    }

    private final StringDictionary[] dicts; // UTF8 in dictionary mode: the column's distinct values; null once in record mode

    /**
     * UTF8 columns in record mode: the group's value bytes appended to a
     * per-column arena, {@code recOff[c][gid] .. recOff[c][gid + 1]}. A column
     * starts in dictionary mode and switches here once its dictionary passes
     * {@link #dictionaryLimit} entries: a probe into a dictionary that no
     * longer fits the cache is a cache miss per row (q67's {@code
     * i_product_name} at 1 TB: ~300k entries, +36% on the final aggregate),
     * where the bytes compare against one contiguous record costs one.
     */
    private final boolean[] record;

    private final byte[][] recBytes;
    private final MemorySegment[] recSeg;
    private final int[][] recOff;
    private final int[] recUsed;
    private final int dictionaryLimit;
    private final int strCols; // number of UTF8 columns
    private final int[] strCol; // column -> index among the UTF8 columns, or -1
    private final BitSet[] nulls;

    private int[] hashScratch = new int[0]; // row hashes, or combined indices on the memoised path
    private byte[] emitScratch = new byte[0]; // a column's values gathered from the dictionary before one bulk copy out
    private int[] memo = new int[0];

    // Packed multi-key lookup (#692); see assignPacked.
    private boolean packedOff;
    private int[] packedWidth; // per column: bits of its code in the packed key
    private int[] packedShift; // per column: where its code sits
    private LongCodes[] packedValues; // INT32 / INT64 columns: value -> code (1-based; 0 is null)
    private long[] packedIndex; // pairs: packed key, group id (-1 = empty slot)
    private int packedMask;
    private int packedSize;
    private int[][] packedCodes = new int[0][]; // per column: the batch's codes
    private long[] packedKeysScratch = new long[0];
    private long packedRows;
    private long packedMisses;

    /**
     * When every key is dictionary encoded and the dictionaries are small,
     * group ids are memoised per combination of dictionary indices for the
     * batch, so most rows never probe the table.
     */
    private static final long MEMO_MAX_COMBINATIONS = 1 << 16;

    /**
     * The memo is also bounded by the batch (#416): it is reset -- {@code
     * combinations} ints filled -- for every batch, and a batch of a few dozen
     * rows (a block at 1000 shuffle partitions) cannot use more entries than it
     * has rows. Filling four ints per row is cheaper than a probe per row; past
     * that the plain path is.
     */
    private static long memoLimit(int rows) {
        return Math.max(256L, 4L * rows);
    }

    public GroupKeyTable(VecType[] types) {
        this(types, true);
    }

    /**
     * @param dictionaryStrings how UTF8 keys are kept. {@code true}: by id in a
     *     per-column dictionary of the distinct values (#377) -- the table's
     *     key output can then be emitted dictionary-encoded, which is what pays
     *     when the consumer is the shuffle writer (a partial aggregate, a
     *     join's build side). {@code false}: as contiguous byte records per
     *     group, compared byte for byte -- the cheaper probe when the output is
     *     consumed plain (a final aggregate feeding a sort, a window or the
     *     result projection), and the layout that does not pay a cache miss per
     *     row once a key's distinct values outgrow the cache (q67's {@code
     *     i_product_name} at 1 TB: ~300k entries, +36% on the final aggregate
     *     by ids). Either way the table is immutable once {@link #assign} is
     *     done (a probe inserts nothing), so {@link #lookup(VectorBuffers[],
     *     int, int[], MemorySegment, int[])} may run concurrently from several
     *     threads.
     */
    public GroupKeyTable(VecType[] types, boolean dictionaryStrings) {
        this(types, dictionaryStrings ? Integer.MAX_VALUE : 0);
    }

    /**
     * Package-private: the mode switch mid-stream, exercised by the tests.
     * {@code dictionaryLimit} is the number of distinct values a UTF8 key's
     * dictionary may reach before the column converts to records ({@link
     * #toRecord}); callers choose a mode up front through {@link
     * #GroupKeyTable(VecType[], boolean)}.
     */
    GroupKeyTable(VecType[] types, int dictionaryLimit) {
        this.types = types.clone();
        this.dictionaryLimit = dictionaryLimit;
        this.slotCount = INITIAL_CAPACITY * 2;
        if (slotCount < PACKED_SLOTS) {
            this.splitSlots = new int[slotCount];
            this.groupHashes = new int[INITIAL_CAPACITY];
        } else {
            this.slots = new long[slotCount];
        }
        clearSlots();
        this.mask = slotCount - 1;
        if (fastKeys(types)) {
            this.fast = emptyFast(slotCount);
        }
        int k = types.length;
        keyOff = new int[k];
        int w = 0;
        for (int c = 0; c < k; c++) {
            keyOff[c] = w;
            w += switch (types[c]) {
                        case INT64, FLOAT64 -> 2;
                        case DECIMAL128 -> 4;
                        default -> 1; // INT32, BOOL, a UTF8 id (nested types are refused below)
                    };
        }
        width = Math.max(1, w);
        rows = new int[INITIAL_CAPACITY * width];
        dicts = new StringDictionary[k];
        record = new boolean[k];
        recBytes = new byte[k][];
        recSeg = new MemorySegment[k];
        recOff = new int[k][];
        recUsed = new int[k];
        strCol = new int[k];
        nulls = new BitSet[k];
        int s = 0;
        for (int c = 0; c < k; c++) {
            nulls[c] = new BitSet();
            strCol[c] = -1;
            switch (types[c]) {
                case INT32, BOOL, INT64, FLOAT64, DECIMAL128 -> {
                    // in the rows
                }
                case LIST, MAP, STRUCT, FIXED_BINARY ->
                        throw new IllegalStateException("no nested or FIXED_BINARY lane here (#705)");
                case UTF8 -> {
                    strCol[c] = s++;
                    if (dictionaryLimit > 0) {
                        dicts[c] = new StringDictionary();
                    } else {
                        startRecord(c);
                    }
                }
            }
        }
        strCols = s;
        packedOff = !PACKED_ENABLED
                    || !packedKeys(this.types)
                    || fast != null
                    || dictionaryLimit == 0;
        if (!packedOff) {
            packedWidth = new int[k];
            packedShift = new int[k];
            packedValues = new LongCodes[k];
            for (int c = 0; c < k; c++) {
                if (types[c] == VecType.INT32 || types[c] == VecType.INT64) {
                    packedValues[c] = new LongCodes();
                }
            }
        }
    }

    private void startRecord(int c) {
        record[c] = true;
        recBytes[c] = new byte[Math.max(256, INITIAL_CAPACITY * 8)];
        recSeg[c] = MemorySegment.ofArray(recBytes[c]);
        recOff[c] = new int[Math.max(groupCap, INITIAL_CAPACITY) + 1];
        recUsed[c] = 0;
    }

    /**
     * Whether UTF8 key column {@code c} is (still) kept by dictionary id, so
     * {@link #dictionary} and {@link #writeKeyIds} apply.
     */
    public boolean isDictionaryColumn(int c) {
        return types[c] == VecType.UTF8 && !record[c];
    }

    public int size() {
        return size;
    }

    public int numKeys() {
        return types.length;
    }

    public VecType type(int c) {
        return types[c];
    }

    /**
     * Assigns a group id to each of the {@code n} rows described by the key
     * columns, inserting new groups as needed. Returns the number of groups
     * after the batch.
     */
    public int assign(VectorBuffers[] keys, int n, int[] outIds) {
        return assign(keys, n, outIds, null);
    }

    /**
     * As {@link #assign(VectorBuffers[], int, int[])} restricted to the rows
     * set in {@code selection} ({@code null} for all rows): unselected rows get
     * id {@code -1} and never create a group.
     */
    public int assign(VectorBuffers[] keys, int n, int[] outIds,
                      MemorySegment selection) {
        IdScratch ids = toIds(keys, n, true);
        if (ids != null) {
            keys = ids.keys;
        }
        long combinations = dictionaryCombinations();
        if (combinations > 0 && combinations <= MEMO_MAX_COMBINATIONS && combinations <= memoLimit(n)) {
            return assignMemoised(keys, ids, n, outIds, (int) combinations,
                    selection);
        }
        if (!packedOff) {
            int r = assignPacked(keys, ids, n, outIds, selection);
            if (r >= 0) {
                return r;
            }
        }
        if (hashScratch.length < n) {
            hashScratch = new int[Math.max(n, hashScratch.length * 2)];
        }
        int[] hashes = hashScratch;
        HashKernels.init(hashes, n);
        for (VectorBuffers key : keys) {
            HashKernels.mixColumn(key, hashes);
        }
        Bound b = Bound.of(keys, ids);
        if (pendingNative) {
            moveFastToPool();
        }
        if (fastSeg != null && b.noNulls(n, selection)) {
            assignFastNative(b, n, hashes, outIds, selection);
            return size;
        }
        if (fast != null && b.noNulls(n, selection)) {
            assignFast(b, n, hashes, outIds, selection);
            return size;
        }
        if (selection == null) {
            for (int i = 0; i < n; i++) {
                outIds[i] = lookupOrInsert(b, i, HashKernels.finish(hashes[i]));
            }
        } else {
            Arrays.fill(outIds, 0, n, -1);
            for (int w = 0, words = Bitmap.wordsFor(n);
                 w < words;
                 w++) {
                long bits = Bitmap.wordAt(selection, w, n);
                while (bits != 0L) {
                    int i = (w << 6) + Long.numberOfTrailingZeros(bits);
                    bits &= bits - 1;
                    outIds[i] = lookupOrInsert(b, i, HashKernels.finish(hashes[i]));
                }
            }
        }
        return size;
    }

    /**
     * Probes without inserting: {@code outIds[i]} is the id of the group whose
     * key equals row {@code i}, or {@code -1} if none exists (or the row is not
     * selected). This is a hash join's probe side over a table built with
     * {@link #assign}; returns the number of rows that matched.
     */
    public int lookup(VectorBuffers[] keys, int n, int[] outIds,
                      MemorySegment selection) {
        if (hashScratch.length < n) {
            hashScratch = new int[Math.max(n, hashScratch.length * 2)];
        }
        return lookup(keys, n, outIds, selection, hashScratch);
    }

    /**
     * {@link #lookup(VectorBuffers[], int, int[], MemorySegment)} with the
     * caller's row-hash scratch ({@code hashes.length >= n}). On a table
     * constructed without plain-string encoding this reads the table only, so
     * concurrent probes from several threads (a broadcast join's tasks sharing
     * one build table) are safe as long as nobody assigns to it any more.
     */
    public int lookup(VectorBuffers[] keys, int n, int[] outIds,
                      MemorySegment selection, int[] hashes) {
        IdScratch ids = toIds(keys, n, false);
        if (ids != null) {
            keys = ids.keys;
        }
        HashKernels.init(hashes, n);
        for (VectorBuffers key : keys) {
            HashKernels.mixColumn(key, hashes);
        }
        ProbeKeys heap = ProbeKeys.of(keys, n, types);
        Bound b = heap != null ? null : Bound.of(keys, ids);
        int matched = 0;
        if (selection == null) {
            for (int i = 0; i < n; i++) {
                int gid = heap != null ? lookupOnly(heap, i, HashKernels.finish(hashes[i])) : lookupOnly(b, i, HashKernels.finish(hashes[i]));
                outIds[i] = gid;
                if (gid >= 0) {
                    matched++;
                }
            }
        } else {
            Arrays.fill(outIds, 0, n, -1);
            for (int w = 0, words = Bitmap.wordsFor(n);
                 w < words;
                 w++) {
                long bits = Bitmap.wordAt(selection, w, n);
                while (bits != 0L) {
                    int i = (w << 6) + Long.numberOfTrailingZeros(bits);
                    bits &= bits - 1;
                    int gid = heap != null ? lookupOnly(heap, i, HashKernels.finish(hashes[i])) : lookupOnly(b, i, HashKernels.finish(hashes[i]));
                    outIds[i] = gid;
                    if (gid >= 0) {
                        matched++;
                    }
                }
            }
        }
        return matched;
    }

    /*
     * The probe batch's key columns as Java arrays, for the probe of a join
     * (#409). A join probes one row at a time -- hash, slot, compare -- and
     * each compare read the row's key through {@link VectorBuffers#getInt} /
     * {@link VectorBuffers#isNull}: a segment liveness check and a bounds check
     * per read, virtual when the receiver profile mixes heap and native
     * segments, which was 11% of an executor's samples in q88 at SF10
     * (store_sales probing three dimension tables). Plain INT32, INT64 and
     * FLOAT64 keys (a double is its raw bits) are copied out once per batch
     * with one bulk move per column and compared from the arrays; any other key
     * type keeps the segment path. Per thread, since a broadcast table is
     * probed by several tasks at once.
     */
/**
            * The batch's key columns bound once per call (#377): each column's
            * data, offsets and validity segments -- and its dictionary's -- as
            * fields of the concrete segment type. The per-row path (hash slot,
            * compare, insert, append) read every key through {@link
            * VectorBuffers#isNull}, {@link VectorBuffers#getInt} and {@code
            * offsets()}/{@code data()}: interface calls whose receiver profile
            * mixes the adapters' buffers, Arrow-backed buffers and encoded
            * short strings, so they stayed virtual, and each carried a segment
            * liveness and bounds check of its own -- 34% of an executor's
            * samples in q67's rollup at 1 TB were those checks. Per thread, as
            * {@link ProbeKeys}.
            */
    private static final class Bound {
        /** Columns bound by the last {@link #of}; the arrays may be longer. */
        int count;

        VectorBuffers[] keys = new VectorBuffers[0];
        MemorySegment[] data = new MemorySegment[0];
        MemorySegment[] offsets = new MemorySegment[0];
        MemorySegment[] validity = new MemorySegment[0];
        int[][] ids = new int[0][]; // UTF8 columns in dictionary mode: the rows' dictionary ids as a heap array
        boolean[] dictEncoded = new boolean[0]; // UTF8 columns in record mode: the input's own encoding
        MemorySegment[] dictData = new MemorySegment[0];
        MemorySegment[] dictOffsets = new MemorySegment[0];

        /*
         * Heap mirrors (#565): INT32 lanes (and the indices of a dictionary-encoded UTF8 column in record
         * mode) in ints, INT64 / FLOAT64 raw bits in longs, validity as 64-row words; null when the column
         * has none. Reading the segments above per row still paid a liveness and a bounds check each --
         * Bound.getInt was 7.5 % of q67's FFM check samples at 1 TB, Bound.isNull 1.8 %, getLong 1.8 % --
         * because the receiver profile mixes the native batch columns with the heap id columns of
         * toIds, so the accessors stay calls. One bulk copy per column per batch instead; the pools keep
         * the arrays across batches.
         */
        int[][] ints = new int[0][];
        long[][] longs = new long[0][];
        long[][] validWords = new long[0][];
        int[][] intPool = new int[0][];
        long[][] longPool = new long[0][];
        long[][] wordPool = new long[0][];

        private static final ThreadLocal<Bound> SCRATCH = ThreadLocal.withInitial(Bound::new);

        /** See {@link GroupKeyTable#releaseThreadScratch}. */
        static void release() {
            Bound b = SCRATCH.get();
            Arrays.fill(b.keys, null);
            Arrays.fill(b.data, null);
            Arrays.fill(b.offsets, null);
            Arrays.fill(b.validity, null);
            Arrays.fill(b.ids, null);
            Arrays.fill(b.dictData, null);
            Arrays.fill(b.dictOffsets, null);
            Arrays.fill(b.ints, null);
            Arrays.fill(b.longs, null);
            Arrays.fill(b.validWords, null);
            b.count = 0;
            trim(b.intPool);
            trim(b.longPool);
            trim(b.wordPool);
        }

        static Bound of(VectorBuffers[] keys, IdScratch idScratch) {
            Bound b = SCRATCH.get();
            int n = keys.length;
            b.count = n;
            if (b.data.length < n) {
                b.keys = new VectorBuffers[n];
                b.data = new MemorySegment[n];
                b.offsets = new MemorySegment[n];
                b.validity = new MemorySegment[n];
                b.ids = new int[n][];
                b.dictEncoded = new boolean[n];
                b.dictData = new MemorySegment[n];
                b.dictOffsets = new MemorySegment[n];
            }
            if (b.ints.length < n) {
                b.ints = new int[n][];
                b.longs = new long[n][];
                b.validWords = new long[n][];
                b.intPool = Arrays.copyOf(b.intPool, n);
                b.longPool = Arrays.copyOf(b.longPool, n);
                b.wordPool = Arrays.copyOf(b.wordPool, n);
            }
            for (int c = 0; c < n; c++) {
                VectorBuffers k = keys[c];
                b.keys[c] = k;
                b.data[c] = k.data();
                b.offsets[c] = k.offsets();
                b.validity[c] = k.validity();
                b.ids[c] = idScratch == null ? null : idScratch.ids[c];
                boolean dict = k.type() == VecType.UTF8 && k.isDictionaryEncoded();
                b.dictEncoded[c] = dict;
                VectorBuffers d = dict ? k.dictionary() : null;
                b.dictData[c] = d == null ? null : d.data();
                b.dictOffsets[c] = d == null ? null : d.offsets();
                b.mirror(c, k, dict);
            }
            return b;
        }

        /**
         * Whether no row of the first {@code n} of the batch -- of those set in
         * {@code selection}, when there is one -- has a null key. A nullable
         * Parquet column carries a validity bitmap whether or not it holds a
         * null, and a filtered batch can reach the aggregate as its columns
         * plus a selection: the selected rows' bits decide, not the bitmap's
         * presence (#677).
         */
        boolean noNulls(int n, MemorySegment selection) {
            int words = Bitmap.wordsFor(n);
            for (int c = 0; c < count; c++) {
                long[] w = validWords[c];
                if (w == null) {
                    continue;
                }
                for (int i = 0; i < words; i++) {
                    long need = selection == null ? -1L : Bitmap.wordAt(selection, i, n);
                    if (i == words - 1 && (n & 63) != 0) {
                        need &= (1L << (n & 63)) - 1;
                    }
                    if ((w[i] & need) != need) {
                        return false;
                    }
                }
            }
            return true;
        }

        /** Fills column {@code c}'s heap mirrors (see the fields). */
        private void mirror(int c, VectorBuffers k, boolean dict) {
            int rows = k.length();
            VecType t = k.type();
            ints[c] = null;
            longs[c] = null;
            if (t == VecType.INT32 && ids[c] != null) {
                ints[c] = ids[c]; // toIds' column: its data segment is a view of this very array
            } else if (t == VecType.INT32 || dict) {
                int[] a = intPool[c];
                if (a == null || a.length < rows) {
                    a = new int[Math.max(rows, a == null ? 4096 : a.length * 2)];
                    intPool[c] = a;
                }
                MemorySegment.copy(k.data(), VectorBuffers.LE_INT, 0L, a, 0,
                        rows);
                ints[c] = a;
            } else if (t == VecType.INT64 || t == VecType.FLOAT64) {
                long[] a = longPool[c];
                if (a == null || a.length < rows) {
                    a = new long[Math.max(rows, a == null ? 4096 : a.length * 2)];
                    longPool[c] = a;
                }
                MemorySegment.copy(k.data(), VectorBuffers.LE_LONG, 0L, a, 0,
                        rows);
                longs[c] = a;
            }
            MemorySegment v = k.validity();
            if (v == null) {
                validWords[c] = null;
            } else {
                int words = Bitmap.wordsFor(rows);
                long[] w = wordPool[c];
                if (w == null || w.length < words) {
                    w = new long[Math.max(words, w == null ? 64 : w.length * 2)];
                    wordPool[c] = w;
                }
                for (int i = 0; i < words; i++) {
                    w[i] = Bitmap.wordAt(v, i, rows);
                }
                validWords[c] = w;
            }
        }

        int getId(int c, int row) {
            return ids[c][row];
        }

        boolean isNull(int c, int row) {
            long[] v = validWords[c];
            return v != null && ((v[row >>> 6] >>> row) & 1L) == 0L;
        }

        int getInt(int c, int row) {
            return ints[c][row];
        }

        long getLong(int c, int row) {
            return longs[c][row];
        }

        double getDouble(int c, int row) {
            return Double.longBitsToDouble(longs[c][row]);
        }

        boolean getBoolean(int c, int row) {
            return Bitmap.isSet(data[c], row);
        }
    }

    private static final class ProbeKeys {
        int[][] ints = new int[0][];
        long[][] longs = new long[0][];
        long[][] validity = new long[0][];

        private static final ThreadLocal<ProbeKeys> SCRATCH = ThreadLocal.withInitial(ProbeKeys::new);

        /** See {@link GroupKeyTable#releaseThreadScratch}. */
        static void release() {
            ProbeKeys p = SCRATCH.get();
            trim(p.ints);
            trim(p.longs);
            trim(p.validity);
        }

        static ProbeKeys of(VectorBuffers[] keys, int n, VecType[] types) {
            for (int c = 0; c < keys.length; c++) {
                VecType t = types[c];
                if (keys[c].isDictionaryEncoded() || !(t == VecType.INT32 || t == VecType.INT64 || t == VecType.FLOAT64)) {
                    return null;
                }
            }
            ProbeKeys p = SCRATCH.get();
            if (p.ints.length < keys.length) {
                p.ints = Arrays.copyOf(p.ints, keys.length);
                p.longs = Arrays.copyOf(p.longs, keys.length);
                p.validity = Arrays.copyOf(p.validity, keys.length);
            }
            for (int c = 0; c < keys.length; c++) {
                VectorBuffers k = keys[c];
                if (types[c] == VecType.INT32) {
                    int[] a = p.ints[c];
                    if (a == null || a.length < n) {
                        a = new int[Math.max(n, 4096)];
                        p.ints[c] = a;
                    }
                    MemorySegment.copy(k.data(), VectorBuffers.LE_INT, 0L, a, 0,
                            n);
                } else {
                    long[] a = p.longs[c];
                    if (a == null || a.length < n) {
                        a = new long[Math.max(n, 4096)];
                        p.longs[c] = a;
                    }
                    MemorySegment.copy(k.data(), VectorBuffers.LE_LONG, 0L, a, 0,
                            n);
                }
                if (k.hasNulls()) {
                    int words = Bitmap.wordsFor(n);
                    long[] v = p.validity[c];
                    if (v == null || v.length < words) {
                        v = new long[Math.max(words, 64)];
                    }
                    for (int w = 0; w < words; w++) {
                        v[w] = Bitmap.wordAt(k.validity(), w, n);
                    }
                    p.validity[c] = v;
                } else {
                    p.validity[c] = null;
                }
            }
            return p;
        }

        boolean isNull(int c, int row) {
            long[] v = validity[c];
            return v != null && (v[row >>> 6] & (1L << (row & 63))) == 0L;
        }
    }

    private int lookupOnly(ProbeKeys keys, int row, int hash) {
        int pos = hash & mask;
        int[] split = splitSlots;
        if (split != null) {
            int[] hashes = groupHashes;
            while (true) {
                int gid = split[pos];
                if (gid < 0) {
                    return -1;
                }
                if (hashes[gid] == hash && equals(gid, keys, row)) {
                    return gid;
                }
                pos = (pos + 1) & mask;
            }
        }
        while (true) {
            long e = entry(pos);
            if (e == EMPTY) {
                return -1;
            }
            int gid = (int) e;
            if ((int) (e >>> 32) == hash && equals(gid, keys, row)) {
                return gid;
            }
            pos = (pos + 1) & mask;
        }
    }

    private boolean equals(int gid, ProbeKeys keys, int row) {
        for (int c = 0; c < types.length; c++) {
            boolean rowNull = keys.isNull(c, row);
            if (rowNull != nulls[c].get(gid)) {
                return false;
            }
            if (rowNull) {
                continue;
            }
            if (types[c] == VecType.INT32) {
                if (keyInt(c, gid) != keys.ints[c][row]) {
                    return false;
                }
            } else if (keyLong(c, gid, 0) != keys.longs[c][row]) {
                return false;
            }
        }
        return true;
    }

    private int lookupOnly(Bound keys, int row, int hash) {
        int pos = hash & mask;
        int[] split = splitSlots;
        if (split != null) {
            int[] hashes = groupHashes;
            while (true) {
                int gid = split[pos];
                if (gid < 0) {
                    return -1;
                }
                if (hashes[gid] == hash && equals(gid, keys, row)) {
                    return gid;
                }
                pos = (pos + 1) & mask;
            }
        }
        while (true) {
            long e = entry(pos);
            if (e == EMPTY) {
                return -1;
            }
            int gid = (int) e;
            if ((int) (e >>> 32) == hash && equals(gid, keys, row)) {
                return gid;
            }
            pos = (pos + 1) & mask;
        }
    }

    /**
     * Product of (dictionary size + 1) over the UTF8 keys after the batch's ids
     * were assigned, or 0 if a key is not UTF8 (the memoised path needs every
     * key to have a small dictionary).
     */
    private long dictionaryCombinations() {
        if (types.length == 0 || strCols != types.length) {
            return 0;
        }
        long combinations = 1;
        for (int c = 0; c < types.length; c++) {
            if (record[c]) {
                return 0;
            }
            combinations *= dicts[c].size() + 1L;
            if (combinations > MEMO_MAX_COMBINATIONS) {
                return combinations;
            }
        }
        return combinations;
    }

    /**
     * Per-thread scratch of {@link #toIds}: the key columns with every UTF8
     * column replaced by an INT32 column of dictionary ids (a heap array, also
     * kept as such for {@link Bound}), plus the per-column maps from an input
     * dictionary's entries to ids. Per thread because a broadcast join's tasks
     * probe one table concurrently.
     */
    private static final class IdScratch {
        VectorBuffers[] keys = new VectorBuffers[0];
        int[][] ids = new int[0][];
        // Per column: input dictionary entry -> (generation << 32 | id), the id valid while the generation is
        // the column's current one. One array, so a row reads one word per key column: over a join's build
        // side, a dictionary of millions of rows, each read is a likely cache miss (#692).
        long[][] entries = new long[0][];
        int[] gen = new int[0];
        VectorBuffers[] lastDict = new VectorBuffers[0];
        GroupKeyTable[] lastTable = new GroupKeyTable[0]; // per column: the table the entry map is for
        boolean[] lastInsert = new boolean[0]; // per column: whether only inserts used the map (no -1 cached)
        int[] offs = new int[0];
        byte[] bytes = new byte[0];
        long[] wordScratch = new long[0];
        final StringDictionary.Scratch entryBytes = new StringDictionary.Scratch();

        /**
         * The first {@code n} bits of {@code validity} as words, in an array
         * reused by every column of the call (each column reads its words
         * before the next one fills it).
         */
        long[] words(MemorySegment validity, int n) {
            int words = Bitmap.wordsFor(n);
            if (wordScratch.length < words) {
                wordScratch = new long[Math.max(words, wordScratch.length * 2)];
            }
            for (int w = 0; w < words; w++) {
                wordScratch[w] = Bitmap.wordAt(validity, w, n);
            }
            return wordScratch;
        }

        private static final ThreadLocal<IdScratch> SCRATCH = ThreadLocal.withInitial(IdScratch::new);

        /**
         * See {@link GroupKeyTable#releaseThreadScratch}. Forgetting the last
         * dictionary and table also invalidates the entry maps (toIds rebuilds
         * them when lastDict / lastTable differ), so a kept map is never reused
         * against a table it was not made for.
         */
        static void release() {
            IdScratch s = SCRATCH.get();
            Arrays.fill(s.keys, null);
            Arrays.fill(s.lastDict, null);
            Arrays.fill(s.lastTable, null);
            trim(s.ids);
            for (int c = 0; c < s.entries.length; c++) {
                if (s.entries[c] != null && s.entries[c].length > KEEP_ELEMENTS) {
                    s.entries[c] = null;
                    s.gen[c] = 0;
                }
            }
            if (s.offs.length > KEEP_ELEMENTS) {
                s.offs = new int[0];
            }
            if (s.bytes.length > KEEP_ELEMENTS) {
                s.bytes = new byte[0];
            }
            if (s.wordScratch.length > KEEP_ELEMENTS) {
                s.wordScratch = new long[0];
            }
        }

        static IdScratch get(int k) {
            IdScratch s = SCRATCH.get();
            if (s.keys.length != k) {
                s.keys = new VectorBuffers[k]; // exactly k: callers iterate it as the key columns
            }
            if (s.ids.length < k) {
                s.ids = Arrays.copyOf(s.ids, k);
                s.entries = Arrays.copyOf(s.entries, k);
                s.gen = Arrays.copyOf(s.gen, k);
                s.lastDict = Arrays.copyOf(s.lastDict, k);
                s.lastTable = Arrays.copyOf(s.lastTable, k);
                s.lastInsert = Arrays.copyOf(s.lastInsert, k);
            }
            return s;
        }
    }

    /**
     * Maps every UTF8 key column of the batch to ids in the column's
     * dictionary: a plain column row by row (one fingerprint and probe per
     * row), a dictionary-encoded column entry by entry (one probe per distinct
     * entry the batch uses, remembered while the same dictionary keeps
     * arriving) and then a gather. With {@code insert} false (a join probe) an
     * unknown value gets id -1, which no group carries. Returns null when the
     * table has no UTF8 key or the batch is empty.
     */
    @SuppressWarnings("ReferenceEquality") // the dictionary cache is keyed on the buffer object itself
    private IdScratch toIds(VectorBuffers[] keys, int n, boolean insert) {
        if (strCols == 0 || n == 0) {
            return null;
        }
        int k = keys.length;
        IdScratch s = IdScratch.get(k);
        System.arraycopy(keys, 0, s.keys, 0, k);
        if (s.offs.length < n + 1) {
            s.offs = new int[Math.max(n + 1, s.offs.length * 2)];
        }
        for (int c = 0; c < k; c++) {
            if (types[c] != VecType.UTF8) {
                s.ids[c] = null;
                continue;
            }
            if (!record[c] && insert && dicts[c].size() > dictionaryLimit) {
                toRecord(c);
            }
            if (record[c]) {
                s.ids[c] = null; // the UTF8 column itself is hashed and compared, byte for byte
                continue;
            }
            VectorBuffers key = keys[c];
            StringDictionary dict = dicts[c];
            int[] ids = s.ids[c];
            if (ids == null || ids.length < n) {
                ids = new int[Math.max(n, ids == null ? 4096 : ids.length * 2)];
                s.ids[c] = ids;
            }
            MemorySegment validity = key.validity();
            if (key.isDictionaryEncoded()) {
                VectorBuffers d = key.dictionary();
                int m = d.length();
                long[] entries = s.entries[c];
                if (entries == null || entries.length < m) {
                    entries = new long[Math.max(m, entries == null ? 256 : entries.length * 2)];
                    s.entries[c] = entries;
                    s.gen[c] = 0;
                    s.lastDict[c] = null;
                    s.lastTable[c] = null;
                }
                // A new dictionary object invalidates the entry map by bumping the generation: entries are
                // mapped when first used, so a batch costs its rows plus its distinct entries, never the
                // whole dictionary. (Ids only ever grow, so a map of a dictionary that keeps arriving stays right.)
                // The map is per thread, not per table, so it is also invalidated when another table uses
                // it (a join probe and an aggregate over the same batch on one task thread: one table's ids
                // are not the other's), and when a lookup's map (unknown values cached as -1) meets an
                // insert, which must add those values rather than reuse the -1.
                if (s.lastDict[c] != d
                        || s.lastTable[c] != this
                        || (insert && !s.lastInsert[c])) {
                    s.lastDict[c] = d;
                    s.lastTable[c] = this;
                    s.lastInsert[c] = insert;
                    if (++s.gen[c] == 0) {
                        Arrays.fill(entries, 0L);
                        s.gen[c] = 1;
                    }
                }
                if (!insert) {
                    // A lookup that reuses an insert's map still caches its misses as -1 in it (#593), so the
                    // map no longer counts as an insert's: the next insert must rebuild it.
                    s.lastInsert[c] = false;
                }
                int gen = s.gen[c];
                MemorySegment dOff = d.offsets();
                MemorySegment dData = d.data();
                MemorySegment dValidity = d.validity();
                // The rows' dictionary indices land in ids first and are replaced in place by the group
                // dictionary's ids; the validity is read as words. One bulk move each instead of an index
                // and a bit through the segments per row (#565: 2.1 % + 0.9 % of q67's FFM check samples).
                MemorySegment.copy(key.data(), VectorBuffers.LE_INT, 0L, ids, 0,
                        n);
                long[] valid = validity == null ? null : s.words(validity, n);
                for (int i = 0; i < n; i++) {
                    if (valid != null && ((valid[i >>> 6] >>> i) & 1L) == 0L) {
                        ids[i] = 0;
                        continue;
                    }
                    int e = ids[i];
                    long entry = entries[e];
                    if ((int) (entry >>> 32) != gen) {
                        int id;
                        if (dValidity != null && !Bitmap.isSet(dValidity, e)) {
                            id = 0; // a null entry: the row is null through the dictionary; not a key value
                        } else {
                            int start = dOff.get(VectorBuffers.LE_INT, (long) e << 2);
                            int len = dOff.get(VectorBuffers.LE_INT, (long) (e + 1) << 2) - start;
                            id = dict.indexOf(dData, start, len, insert, s.entryBytes);
                        }
                        entry = ((long) gen << 32) | (id & 0xFFFFFFFFL);
                        entries[e] = entry;
                    }
                    ids[i] = (int) entry;
                }
            } else {
                int[] offs = s.offs;
                MemorySegment.copy(key.offsets(), VectorBuffers.LE_INT, 0, offs, 0,
                        n + 1);
                int first = offs[0];
                int total = offs[n] - first;
                if (s.bytes.length < total) {
                    s.bytes = new byte[Math.max(total, s.bytes.length * 2)];
                }
                byte[] bytes = s.bytes;
                MemorySegment.copy(key.data(), ValueLayout.JAVA_BYTE, first, bytes, 0,
                        total);
                long[] valid = validity == null ? null : s.words(validity, n);
                for (int i = 0; i < n; i++) {
                    if (valid != null && ((valid[i >>> 6] >>> i) & 1L) == 0L) {
                        ids[i] = 0;
                        continue;
                    }
                    int start = offs[i] - first;
                    int len = offs[i + 1] - offs[i];
                    ids[i] = dict.indexOf(StringDictionary.fingerprint(bytes, start, len),
                            len, bytes, start, insert);
                }
            }
            s.keys[c] = SegmentVectorBuffers.fixedWidth(VecType.INT32, n, validity, MemorySegment.ofArray(ids));
        }
        return s;
    }

    private int assignMemoised(VectorBuffers[] keys, IdScratch ids, int n,
            int[] outIds, int combinations, MemorySegment selection) {
        Bound b = Bound.of(keys, ids);
        if (memo.length < combinations) {
            memo = new int[Math.max(combinations, memo.length * 2)];
        }
        Arrays.fill(memo, 0, combinations, -1);
        int k = keys.length;
        // Fold the per-column ids (0 = null, id + 1 otherwise) into one combined index per row, column
        // by column, so the hot loop runs over plain int arrays.
        int[] combined = combinedScratch(n);
        Arrays.fill(combined, 0, n, 0);
        for (int c = 0; c < k; c++) {
            int size = dicts[c].size() + 1;
            int[] idx = ids.ids[c];
            MemorySegment validity = keys[c].validity();
            if (validity == null) {
                for (int i = 0; i < n; i++) {
                    combined[i] = combined[i] * size + idx[i] + 1;
                }
            } else {
                for (int i = 0; i < n; i++) {
                    combined[i] = combined[i] * size
                                  + (Bitmap.isSet(validity, i) ? idx[i] + 1 : 0);
                }
            }
        }
        int[] memo = this.memo;
        if (selection == null) {
            for (int i = 0; i < n; i++) {
                int gid = memo[combined[i]];
                if (gid < 0) {
                    gid = lookupOrInsert(b, i, idRowHash(b, i));
                    memo[combined[i]] = gid;
                }
                outIds[i] = gid;
            }
        } else {
            Arrays.fill(outIds, 0, n, -1);
            for (int w = 0, words = Bitmap.wordsFor(n);
                 w < words;
                 w++) {
                long bits = Bitmap.wordAt(selection, w, n);
                while (bits != 0L) {
                    int i = (w << 6) + Long.numberOfTrailingZeros(bits);
                    bits &= bits - 1;
                    int gid = memo[combined[i]];
                    if (gid < 0) {
                        gid = lookupOrInsert(b, i, idRowHash(b, i));
                        memo[combined[i]] = gid;
                    }
                    outIds[i] = gid;
                }
            }
        }
        return size;
    }

    private int[] combinedScratch(int n) {
        if (hashScratch.length < n) {
            hashScratch = new int[Math.max(n, hashScratch.length * 2)];
        }
        return hashScratch;
    }

    /**
     * Same hash {@link HashKernels#mixColumn} produces for a row of INT32 id
     * columns, computed for one row.
     */
    private static int idRowHash(Bound keys, int row) {
        int h = HashKernels.SEED;
        for (int c = 0; c < keys.count; c++) {
            h = HashKernels.mix32(h, keys.isNull(c, row) ? HashKernels.NULL_MARK : keys.getId(c, row));
        }
        return HashKernels.finish(h);
    }

    // ------------------------------------------------------------------ packed multi-key lookup (#692)

    /**
     * Distinct values of a packed path's INT32 / INT64 key column, beyond
     * which it gives up: a column's code must fit its share of 64 bits.
     */
    static final int PACKED_MAX_VALUES = 1 << 16;

    /**
     * Rows after which a packed path that keeps missing gives up: past
     * {@code PACKED_MIN_ROWS} rows, more than one miss in
     * {@code PACKED_MAX_MISS_RATIO} rows means most rows are new groups, which
     * pay the packed probe on top of the general one.
     */
    static final long PACKED_MIN_ROWS = 1 << 16;

    static final int PACKED_MAX_MISS_RATIO = 4;

    /**
     * {@code -Dvecruntime.kernels.packedGroupKeys=false} keeps every table on
     * the general probe (benchmarks compare the two; an escape hatch).
     */
    static final boolean PACKED_ENABLED = !"false".equals(System.getProperty("vecruntime.kernels.packedGroupKeys"));

    /**
     * Whether a table of {@code types} may take the packed path: two or more
     * keys, each a UTF8 (by dictionary id), INT32, INT64 or BOOL.
     */
    private static boolean packedKeys(VecType[] types) {
        if (types.length < 2) {
            return false;
        }
        for (VecType t : types) {
            if (t != VecType.UTF8
                    && t != VecType.INT32
                    && t != VecType.INT64
                    && t != VecType.BOOL) {
                return false;
            }
        }
        return true;
    }

    /**
     * Package-private, for the tests: whether the table still takes the packed
     * path.
     */
    boolean packedLookup() {
        return !packedOff;
    }

    /** Package-private, for the tests: the general probe only from now on. */
    void disablePacked() {
        dropPacked();
    }

    private void dropPacked() {
        packedOff = true;
        packedIndex = null;
        packedValues = null;
        packedCodes = new int[0][];
        packedKeysScratch = new long[0];
    }

    /**
     * {@link #assign} by one packed key per row (#692): each key column is
     * mapped to a small code (a UTF8 column's dictionary id + 1, an integer
     * column's value through a per-column map, a boolean's 1 or 2; 0 is
     * null), the codes are packed into one 128-bit key (two {@code long}s; a
     * code never straddles them), and a table from packed key to group id
     * answers the row in one probe of one cache line.
     * The general probe compares every key column of the group (q24a's partial
     * aggregate: ten keys, its {@code lookupOrInsert} and {@code equals} 10 %
     * of the executor samples at 3 TB). A miss takes the general probe, which
     * finds or inserts the group, and remembers the answer, so the packed
     * table never decides a group on its own and is a cache of the general
     * one.
     *
     * <p>A column's code width grows as its values do; a grown width moves
     * every code, so the packed table is emptied and refills from the general
     * probe. Returns {@code -1}, and the caller takes the general path, when
     * the codes no longer fit 128 bits, an integer column has too many distinct
     * values, a UTF8 column has turned to records, or most rows miss; the
     * table then stays general.
     */
    private int assignPacked(VectorBuffers[] keys, IdScratch ids, int n,
            int[] outIds, MemorySegment selection) {
        int k = types.length;
        if (n == 0) {
            return size;
        }
        for (int c = 0; c < k; c++) {
            if (types[c] == VecType.UTF8
                    && (record[c] || ids == null || ids.ids[c] == null)) {
                dropPacked();
                return -1;
            }
        }
        Bound b = Bound.of(keys, ids);
        if (packedCodes.length < k) {
            packedCodes = new int[k][];
        }
        boolean widened = false;
        for (int c = 0; c < k; c++) {
            int[] codes = packedCodes[c];
            if (codes == null || codes.length < n) {
                codes = new int[Math.max(n, codes == null ? 4096 : codes.length * 2)];
                packedCodes[c] = codes;
            }
            int max = 0;
            switch (types[c]) {
                case UTF8 -> {
                    int[] id = b.ids[c];
                    for (int i = 0; i < n; i++) {
                        int code = b.isNull(c, i) ? 0 : id[i] + 1;
                        codes[i] = code;
                        max = Math.max(max, code);
                    }
                }
                case BOOL -> {
                    for (int i = 0; i < n; i++) {
                        codes[i] = b.isNull(c, i)
                                ? 0
                                : (b.getBoolean(c, i) ? 2 : 1);
                    }
                    max = 2;
                }
                case INT32 -> {
                    LongCodes m = packedValues[c];
                    for (int i = 0; i < n; i++) {
                        int code = b.isNull(c, i) ? 0 : m.codeOf(b.getInt(c, i));
                        codes[i] = code;
                        max = Math.max(max, code);
                    }
                    if (m.size() > PACKED_MAX_VALUES) {
                        dropPacked();
                        return -1;
                    }
                }
                case INT64 -> {
                    LongCodes m = packedValues[c];
                    for (int i = 0; i < n; i++) {
                        int code = b.isNull(c, i) ? 0 : m.codeOf(b.getLong(c, i));
                        codes[i] = code;
                        max = Math.max(max, code);
                    }
                    if (m.size() > PACKED_MAX_VALUES) {
                        dropPacked();
                        return -1;
                    }
                }
                default -> throw new IllegalStateException("no packed key for " + types[c]);
            }
            int need = 64 - Long.numberOfLeadingZeros(max);
            if (need > packedWidth[c]) {
                // One bit of headroom: the width doubles the codes it holds, so it grows log-many times.
                packedWidth[c] = need + 1;
                widened = true;
            }
        }
        if (widened || packedIndex == null) {
            // Columns fill the low word, then the high one; a code never straddles the two.
            int lo = 0;
            int hi = 0;
            for (int c = 0; c < k; c++) {
                int w = packedWidth[c];
                if (lo + w <= 64) {
                    packedShift[c] = lo;
                    lo += w;
                } else if (hi + w <= 64) {
                    packedShift[c] = 64 + hi;
                    hi += w;
                } else {
                    dropPacked();
                    return -1;
                }
            }
            packedIndex = emptyPacked(Math.max(1024, Integer.highestOneBit(Math.max(1, size)) * 4));
            packedMask = packedIndex.length / 3 - 1;
            packedSize = 0;
        }
        if (packedKeysScratch.length < 2 * n) {
            packedKeysScratch = new long[Math.max(2 * n, packedKeysScratch.length * 2)];
        }
        long[] pk = packedKeysScratch; // row i: low word at 2 i, high word at 2 i + 1
        Arrays.fill(pk, 0, 2 * n, 0L);
        for (int c = 0; c < k; c++) {
            int[] codes = packedCodes[c];
            int sh = packedShift[c];
            int word = sh >>> 6;
            int bit = sh & 63;
            for (int i = 0; i < n; i++) {
                pk[2 * i + word] |= (long) codes[i] << bit;
            }
        }
        long misses = 0;
        long rows = 0;
        if (selection == null) {
            for (int i = 0; i < n; i++) {
                long kl = pk[2 * i];
                long kh = pk[2 * i + 1];
                int gid = packedGet(kl, kh);
                if (gid < 0) {
                    gid = lookupOrInsert(b, i, rowHash(b, i));
                    packedPut(kl, kh, gid);
                    misses++;
                }
                outIds[i] = gid;
            }
            rows = n;
        } else {
            Arrays.fill(outIds, 0, n, -1);
            for (int w = 0, words = Bitmap.wordsFor(n);
                 w < words;
                 w++) {
                long bits = Bitmap.wordAt(selection, w, n);
                while (bits != 0L) {
                    int i = (w << 6) + Long.numberOfTrailingZeros(bits);
                    bits &= bits - 1;
                    long kl = pk[2 * i];
                    long kh = pk[2 * i + 1];
                    int gid = packedGet(kl, kh);
                    if (gid < 0) {
                        gid = lookupOrInsert(b, i, rowHash(b, i));
                        packedPut(kl, kh, gid);
                        misses++;
                    }
                    outIds[i] = gid;
                    rows++;
                }
            }
        }
        packedRows += rows;
        packedMisses += misses;
        if (packedRows >= PACKED_MIN_ROWS && packedMisses * PACKED_MAX_MISS_RATIO > packedRows) {
            dropPacked();
        }
        return size;
    }

    /**
     * The hash {@link HashKernels#mixColumn} gives row {@code row} of the
     * packed path's columns (UTF8 by dictionary id): what {@link #groupHash}
     * gives the group the row belongs to.
     */
    private int rowHash(Bound keys, int row) {
        int h = HashKernels.SEED;
        for (int c = 0; c < types.length; c++) {
            int v;
            if (keys.isNull(c, row)) {
                v = HashKernels.NULL_MARK;
            } else {
                v = switch (types[c]) {
                            case INT32 -> keys.getInt(c, row);
                            case INT64 -> HashKernels.fold(keys.getLong(c, row));
                            case BOOL -> keys.getBoolean(c, row) ? 1 : 0;
                            case UTF8 -> keys.getId(c, row);
                            default -> throw new IllegalStateException("no packed key for " + types[c]);
                        };
            }
            h = HashKernels.mix32(h, v);
        }
        return HashKernels.finish(h);
    }

    private static long[] emptyPacked(int slotCount) {
        long[] t = new long[3 * slotCount];
        for (int p = 2; p < t.length; p += 3) {
            t[p] = -1L;
        }
        return t;
    }

    private static int packedSlot(long lo, long hi, int mask) {
        long x = (lo ^ (hi * 0xC2B2AE3D27D4EB4FL)) * 0x9E3779B97F4A7C15L;
        return (int) (x >>> 32) & mask;
    }

    private int packedGet(long lo, long hi) {
        long[] t = packedIndex;
        int pos = packedSlot(lo, hi, packedMask);
        while (true) {
            int p = 3 * pos;
            long gid = t[p + 2];
            if (gid < 0) {
                return -1;
            }
            if (t[p] == lo && t[p + 1] == hi) {
                return (int) gid;
            }
            pos = (pos + 1) & packedMask;
        }
    }

    private void packedPut(long lo, long hi, int gid) {
        if ((packedSize + 1) * 2L > packedMask + 1L) {
            long[] old = packedIndex;
            packedIndex = emptyPacked((packedMask + 1) * 2);
            packedMask = packedIndex.length / 3 - 1;
            packedSize = 0;
            for (int p = 0; p < old.length; p += 3) {
                if (old[p + 2] >= 0) {
                    packedPut(old[p], old[p + 1], (int) old[p + 2]);
                }
            }
        }
        long[] t = packedIndex;
        int pos = packedSlot(lo, hi, packedMask);
        while (t[3 * pos + 2] >= 0) {
            if (t[3 * pos] == lo && t[3 * pos + 1] == hi) {
                return;
            }
            pos = (pos + 1) & packedMask;
        }
        t[3 * pos] = lo;
        t[3 * pos + 1] = hi;
        t[3 * pos + 2] = gid;
        packedSize++;
    }

    /**
     * An integer key column's distinct values, each a code from 1 in order of
     * first sight: open addressing over the values, the codes beside them.
     */
    static final class LongCodes {
        private long[] values = new long[64];
        private int[] codes = new int[64]; // 0 = empty slot
        private int size;

        int size() {
            return size;
        }

        int codeOf(long v) {
            int mask = values.length - 1;
            int pos = (int) ((v * 0x9E3779B97F4A7C15L) >>> 32) & mask;
            while (codes[pos] != 0) {
                if (values[pos] == v) {
                    return codes[pos];
                }
                pos = (pos + 1) & mask;
            }
            values[pos] = v;
            codes[pos] = ++size;
            if (size * 2 > values.length) {
                grow();
            }
            return size;
        }

        private void grow() {
            long[] ov = values;
            int[] oc = codes;
            values = new long[ov.length * 2];
            codes = new int[ov.length * 2];
            int mask = values.length - 1;
            for (int p = 0; p < ov.length; p++) {
                if (oc[p] != 0) {
                    int pos = (int) ((ov[p] * 0x9E3779B97F4A7C15L) >>> 32) & mask;
                    while (codes[pos] != 0) {
                        pos = (pos + 1) & mask;
                    }
                    values[pos] = ov[p];
                    codes[pos] = oc[p];
                }
            }
        }

        long memoryBytes() {
            return 12L * values.length;
        }
    }

    // ------------------------------------------------------------------ inline integer keys (#677)

    /**
     * Package-private, for the tests: whether the table still probes its keys
     * inline.
     */
    boolean inlineKeys() {
        return fast != null || fastSeg != null;
    }

    /** Package-private, for the tests: whether the inline pairs live in the pool. */
    boolean nativeInline() {
        return fastSeg != null;
    }

    /**
     * Bytes the table holds in pool blocks (#941): left out of {@link
     * #memoryBytes}.
     */
    public long nativeBytes() {
        return (fastSeg == null ? 0L : fastSeg.byteSize())
                + (slotSeg == null ? 0L : slotSeg.byteSize())
                + (rowSeg == null ? 0L : rowSeg.byteSize());
    }

    /** Package-private, for the tests: whether the slot entries live in the pool. */
    boolean nativeSlots() {
        return slotSeg != null;
    }

    /**
     * Package-private, for the tests: whether the slots are packed hash/id
     * entries on the heap.
     */
    boolean packedSlots() {
        return slots != null;
    }

    /** Gives the pool blocks back; the table is not used afterwards. */
    public void close() {
        freeFastSeg();
        if (slotArena != null) {
            slotArena.close();
            slotArena = null;
            slotSeg = null;
        }
        if (rowArena != null) {
            rowArena.close();
            rowArena = null;
            rowSeg = null;
        }
    }

    /** Package-private, for the tests: whether the key rows live in the pool. */
    boolean nativeRows() {
        return rowSeg != null;
    }

    private void freeFastSeg() {
        if (fastArena != null) {
            fastArena.close();
            fastArena = null;
            fastSeg = null;
        }
    }

    /** {@code slotCount} empty pairs in a pool block: key 0, id -1. */
    private void allocFastSeg(int slotCount) {
        Arena a = NativePool.shared().newArena();
        MemorySegment s = a.allocate(16L * slotCount, 64);
        for (long p = 0; p < slotCount; p++) {
            s.setAtIndex(PAIR, 2 * p, 0L);
            s.setAtIndex(PAIR, 2 * p + 1, -1L);
        }
        fastArena = a;
        fastSeg = s;
    }

    /**
     * {@link #assignFast} over the pairs in the pool: the same batched probe and
     * one loop per key shape, reading through the segment.
     */
    private void assignFastNative(Bound b, int n, int[] hashes,
            int[] outIds, MemorySegment selection) {
        if (selection != null) {
            Arrays.fill(outIds, 0, n, -1);
            for (int w = 0, words = Bitmap.wordsFor(n);
                 w < words;
                 w++) {
                long bits = Bitmap.wordAt(selection, w, n);
                while (bits != 0L) {
                    int i = (w << 6) + Long.numberOfTrailingZeros(bits);
                    bits &= bits - 1;
                    outIds[i] = fastRow(b, i, HashKernels.finish(hashes[i]));
                }
            }
            return;
        }
        int[] hit = probeHits;
        if (types.length == 2) {
            int[] k0 = b.ints[0];
            int[] k1 = b.ints[1];
            for (int i0 = 0; i0 < n; i0 += PROBE_BATCH) {
                int end = Math.min(n, i0 + PROBE_BATCH);
                MemorySegment f = fastSeg;
                int m = mask;
                for (int i = i0; i < end; i++) {
                    long key = ((long) k0[i] << 32) | (k1[i] & 0xFFFFFFFFL);
                    long p = HashKernels.finish(hashes[i]) & m;
                    hit[i - i0] = f.getAtIndex(PAIR, 2 * p) == key ? (int) f.getAtIndex(PAIR, 2 * p + 1) : -1;
                }
                for (int i = i0; i < end; i++) {
                    int g = hit[i - i0];
                    outIds[i] = g >= 0 ? g : fastLookupOrInsertNative(((long) k0[i] << 32) | (k1[i] & 0xFFFFFFFFL), HashKernels.finish(hashes[i]), k0[i], k1[i],
                            0L);
                }
            }
        } else if (types[0] == VecType.INT32) {
            int[] k0 = b.ints[0];
            for (int i0 = 0; i0 < n; i0 += PROBE_BATCH) {
                int end = Math.min(n, i0 + PROBE_BATCH);
                MemorySegment f = fastSeg;
                int m = mask;
                for (int i = i0; i < end; i++) {
                    long p = HashKernels.finish(hashes[i]) & m;
                    hit[i - i0] = f.getAtIndex(PAIR, 2 * p) == k0[i] ? (int) f.getAtIndex(PAIR, 2 * p + 1) : -1;
                }
                for (int i = i0; i < end; i++) {
                    int g = hit[i - i0];
                    outIds[i] = g >= 0 ? g : fastLookupOrInsertNative(k0[i], HashKernels.finish(hashes[i]), k0[i], 0, 0L);
                }
            }
        } else {
            long[] k0 = b.longs[0];
            for (int i0 = 0; i0 < n; i0 += PROBE_BATCH) {
                int end = Math.min(n, i0 + PROBE_BATCH);
                MemorySegment f = fastSeg;
                int m = mask;
                for (int i = i0; i < end; i++) {
                    long p = HashKernels.finish(hashes[i]) & m;
                    hit[i - i0] = f.getAtIndex(PAIR, 2 * p) == k0[i] ? (int) f.getAtIndex(PAIR, 2 * p + 1) : -1;
                }
                for (int i = i0; i < end; i++) {
                    int g = hit[i - i0];
                    outIds[i] = g >= 0 ? g : fastLookupOrInsertNative(k0[i], HashKernels.finish(hashes[i]), 0, 0, k0[i]);
                }
            }
        }
    }

    /** {@link #fastLookupOrInsert} over the pairs in the pool. */
    private int fastLookupOrInsertNative(long key, int hash, int a,
            int b, long l) {
        MemorySegment f = fastSeg;
        int m = mask;
        long pos = hash & m;
        while (true) {
            int gid = (int) f.getAtIndex(PAIR, 2 * pos + 1);
            if (gid < 0) {
                return fastInsert(key, hash, (int) pos, a, b,
                        l);
            }
            if (f.getAtIndex(PAIR, 2 * pos) == key) {
                return gid;
            }
            pos = (pos + 1) & m;
        }
    }

    /** Pair {@code pos} := ({@code key}, {@code gid}), wherever the pairs live. */
    private void setPair(int pos, long key, int gid) {
        if (fastSeg != null) {
            fastSeg.setAtIndex(PAIR, 2L * pos, key);
            fastSeg.setAtIndex(PAIR, 2L * pos + 1, gid);
        } else {
            fast[2 * pos] = key;
            fast[2 * pos + 1] = gid;
        }
    }

    /**
     * Whether {@code types} can be kept packed in {@link #fast}: one or two
     * INT32, or one INT64.
     */
    private static boolean fastKeys(VecType[] types) {
        if (types.length == 1) {
            return types[0] == VecType.INT32 || types[0] == VecType.INT64;
        }
        return types.length == 2 && types[0] == VecType.INT32 && types[1] == VecType.INT32;
    }

    private static long[] emptyFast(int slotCount) {
        long[] f = new long[2 * slotCount];
        for (int p = 1; p < f.length; p += 2) {
            f[p] = -1L;
        }
        return f;
    }

    /**
     * Group {@code gid}'s packed key, from the stored key columns (no group of
     * a fast table is null).
     */
    private long packedKey(int gid) {
        if (types.length == 2) {
            return ((long) keyInt(0, gid) << 32) | (keyInt(1, gid) & 0xFFFFFFFFL);
        }
        return types[0] == VecType.INT32 ? keyInt(0, gid) : keyLong(0, gid, 0);
    }

    /**
     * {@link #assign} over a fast table and a batch with no null key: the
     * packed key is compared in the slot, so a hit reads nothing indexed by
     * group id. One loop per key shape, so each stays monomorphic.
     */
    private void assignFast(Bound b, int n, int[] hashes,
                            int[] outIds, MemorySegment selection) {
        if (selection != null) {
            // Unselected rows get -1 and never create a group; the selected ones go the same way below.
            Arrays.fill(outIds, 0, n, -1);
            for (int w = 0, words = Bitmap.wordsFor(n);
                 w < words;
                 w++) {
                long bits = Bitmap.wordAt(selection, w, n);
                while (bits != 0L) {
                    int i = (w << 6) + Long.numberOfTrailingZeros(bits);
                    bits &= bits - 1;
                    outIds[i] = fastRow(b, i, HashKernels.finish(hashes[i]));
                }
            }
            return;
        }
        if (slotCount < PROBE_BATCH_SLOTS) {
            // In cache the two passes only add work (JMH, 4K groups: 9 -> 13.5 ns per row); probe row by row.
            assignFastRows(b, n, hashes, outIds);
            return;
        }
        // Batched probing: for PROBE_BATCH rows, first read each row's home slot (independent loads, so their
        // cache misses overlap), then resolve the rows in order. Only a hit in the home slot is taken from the
        // first pass -- a group's id never changes, so it stays right even if a later row of the same batch
        // inserts or the table grows; anything else (empty slot, other key, collision chain) goes the full way,
        // which now finds the home slot's line in cache.
        int[] hit = probeHits;
        if (types.length == 2) {
            int[] k0 = b.ints[0];
            int[] k1 = b.ints[1];
            for (int i0 = 0; i0 < n; i0 += PROBE_BATCH) {
                int end = Math.min(n, i0 + PROBE_BATCH);
                long[] f = fast;
                int m = mask;
                for (int i = i0; i < end; i++) {
                    long key = ((long) k0[i] << 32) | (k1[i] & 0xFFFFFFFFL);
                    int p = HashKernels.finish(hashes[i]) & m;
                    hit[i - i0] = f[2 * p] == key ? (int) f[2 * p + 1] : -1;
                }
                for (int i = i0; i < end; i++) {
                    int g = hit[i - i0];
                    outIds[i] = g >= 0 ? g : fastLookupOrInsert(((long) k0[i] << 32) | (k1[i] & 0xFFFFFFFFL), HashKernels.finish(hashes[i]), k0[i], k1[i],
                            0L);
                }
            }
        } else if (types[0] == VecType.INT32) {
            int[] k0 = b.ints[0];
            for (int i0 = 0; i0 < n; i0 += PROBE_BATCH) {
                int end = Math.min(n, i0 + PROBE_BATCH);
                long[] f = fast;
                int m = mask;
                for (int i = i0; i < end; i++) {
                    int p = HashKernels.finish(hashes[i]) & m;
                    hit[i - i0] = f[2 * p] == k0[i] ? (int) f[2 * p + 1] : -1;
                }
                for (int i = i0; i < end; i++) {
                    int g = hit[i - i0];
                    outIds[i] = g >= 0 ? g : fastLookupOrInsert(k0[i], HashKernels.finish(hashes[i]), k0[i], 0, 0L);
                }
            }
        } else {
            long[] k0 = b.longs[0];
            for (int i0 = 0; i0 < n; i0 += PROBE_BATCH) {
                int end = Math.min(n, i0 + PROBE_BATCH);
                long[] f = fast;
                int m = mask;
                for (int i = i0; i < end; i++) {
                    int p = HashKernels.finish(hashes[i]) & m;
                    hit[i - i0] = f[2 * p] == k0[i] ? (int) f[2 * p + 1] : -1;
                }
                for (int i = i0; i < end; i++) {
                    int g = hit[i - i0];
                    outIds[i] = g >= 0 ? g : fastLookupOrInsert(k0[i], HashKernels.finish(hashes[i]), 0, 0, k0[i]);
                }
            }
        }
    }

    /**
     * Rows whose home slots {@link #assignFast} reads before resolving any of
     * them.
     */
    private static final int PROBE_BATCH = 16;

    /**
     * Slot count from which {@link #assignFast} probes in batches: 64K slots is
     * 1 MiB of {@link #fast}, past which a probe usually misses the cache.
     */
    static final int PROBE_BATCH_SLOTS = 1 << 16;

    private final int[] probeHits = new int[PROBE_BATCH];

    /** {@link #assignFast} for a table still in cache: one probe per row. */
    private void assignFastRows(Bound b, int n, int[] hashes,
            int[] outIds) {
        if (types.length == 2) {
            int[] k0 = b.ints[0];
            int[] k1 = b.ints[1];
            for (int i = 0; i < n; i++) {
                long key = ((long) k0[i] << 32) | (k1[i] & 0xFFFFFFFFL);
                outIds[i] = fastLookupOrInsert(key, HashKernels.finish(hashes[i]), k0[i], k1[i], 0L);
            }
        } else if (types[0] == VecType.INT32) {
            int[] k0 = b.ints[0];
            for (int i = 0; i < n; i++) {
                outIds[i] = fastLookupOrInsert(k0[i], HashKernels.finish(hashes[i]), k0[i], 0, 0L);
            }
        } else {
            long[] k0 = b.longs[0];
            for (int i = 0; i < n; i++) {
                outIds[i] = fastLookupOrInsert(k0[i], HashKernels.finish(hashes[i]), 0, 0, k0[i]);
            }
        }
    }

    /** One row of a selected batch on the inline path. */
    private int fastRow(Bound b, int i, int hash) {
        boolean nat = fastSeg != null;
        if (types.length == 2) {
            int x = b.ints[0][i];
            int y = b.ints[1][i];
            long key = ((long) x << 32) | (y & 0xFFFFFFFFL);
            return nat ? fastLookupOrInsertNative(key, hash, x, y, 0L) : fastLookupOrInsert(key, hash, x, y, 0L);
        }
        if (types[0] == VecType.INT32) {
            int x = b.ints[0][i];
            return nat ? fastLookupOrInsertNative(x, hash, x, 0, 0L) : fastLookupOrInsert(x, hash, x, 0, 0L);
        }
        long l = b.longs[0][i];
        return nat ? fastLookupOrInsertNative(l, hash, 0, 0, l) : fastLookupOrInsert(l, hash, 0, 0, l);
    }

    /**
     * The group of packed {@code key}, inserting it with its column values
     * {@code a}/{@code b} or {@code l}.
     */
    private int fastLookupOrInsert(long key, int hash, int a,
            int b, long l) {
        long[] f = fast;
        int m = mask;
        int pos = hash & m;
        while (true) {
            int gid = (int) f[2 * pos + 1];
            if (gid < 0) {
                return fastInsert(key, hash, pos, a, b, l);
            }
            if (f[2 * pos] == key) {
                return gid;
            }
            pos = (pos + 1) & m;
        }
    }

    private int fastInsert(long key, int hash, int pos,
                           int a, int b, long l) {
        int gid = size;
        ensureGroupCapacity(gid + 1);
        if (types.length == 2) {
            setKeyInt(0, gid, a);
            setKeyInt(1, gid, b);
        } else if (types[0] == VecType.INT32) {
            setKeyInt(0, gid, a);
        } else {
            setKeyLong(0, gid, 0, l);
        }
        setEntry(pos, hash, gid);
        setPair(pos, key, gid);
        size++;
        if (size * 10L > (long) slotCount * 7L) {
            rehash();
        }
        return gid;
    }

    private int lookupOrInsert(Bound keys, int row, int hash) {
        int pos = hash & mask;
        int[] split = splitSlots;
        if (split != null) {
            int[] hashes = groupHashes;
            while (true) {
                int gid = split[pos];
                if (gid < 0) {
                    return insert(keys, row, hash, pos);
                }
                if (hashes[gid] == hash && equals(gid, keys, row)) {
                    return gid;
                }
                pos = (pos + 1) & mask;
            }
        }
        while (true) {
            long e = entry(pos);
            if (e == EMPTY) {
                return insert(keys, row, hash, pos);
            }
            int gid = (int) e;
            if ((int) (e >>> 32) == hash && equals(gid, keys, row)) {
                return gid;
            }
            pos = (pos + 1) & mask;
        }
    }

    private boolean equals(int gid, Bound keys, int row) {
        for (int c = 0; c < types.length; c++) {
            boolean rowNull = keys.isNull(c, row);
            if (rowNull != nulls[c].get(gid)) {
                return false;
            }
            if (rowNull) {
                continue;
            }
            switch (types[c]) {
                case INT32 -> {
                    if (keyInt(c, gid) != keys.getInt(c, row)) {
                        return false;
                    }
                }
                case BOOL -> {
                    if (keyInt(c, gid) != (keys.getBoolean(c, row) ? 1 : 0)) {
                        return false;
                    }
                }
                case INT64 -> {
                    if (keyLong(c, gid, 0) != keys.getLong(c, row)) {
                        return false;
                    }
                }
                case FLOAT64 -> {
                    if (keyLong(c, gid, 0) != Double.doubleToRawLongBits(keys.getDouble(c, row))) {
                        return false;
                    }
                }
                case DECIMAL128 -> {
                    MemorySegment d = keys.data[c];
                    if (keyLong(c, gid, 0) != Decimal128.lo(d, row) || keyLong(c, gid, 2) != Decimal128.hi(d, row)) {
                        return false;
                    }
                }
                case LIST, MAP, STRUCT, FIXED_BINARY ->
                        throw new IllegalStateException("no nested or FIXED_BINARY lane here (#705)");
                case UTF8 -> {
                    if (record[c] ? !recordEquals(c, gid, keys, row) : keyInt(c, gid) != keys.getId(c, row)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static final int SHORT_KEY_BYTES = 16;

    private boolean recordEquals(int c, int gid, Bound keys,
            int row) {
        int[] off = recOff[c];
        int start = off[gid];
        int len = off[gid + 1] - start;
        MemorySegment data;
        long rowStart;
        int rowLen;
        if (keys.dictEncoded[c]) {
            int idx = keys.getInt(c, row);
            MemorySegment o = keys.dictOffsets[c];
            rowStart = o.get(VectorBuffers.LE_INT, (long) idx << 2);
            rowLen = o.get(VectorBuffers.LE_INT, (long) (idx + 1) << 2) - (int) rowStart;
            data = keys.dictData[c];
        } else {
            MemorySegment o = keys.offsets[c];
            rowStart = o.get(VectorBuffers.LE_INT, (long) row << 2);
            rowLen = o.get(VectorBuffers.LE_INT, (long) (row + 1) << 2) - (int) rowStart;
            data = keys.data[c];
        }
        if (rowLen != len) {
            return false;
        }
        if (len <= SHORT_KEY_BYTES) {
            byte[] store = recBytes[c];
            for (int i = 0; i < len; i++) {
                if (store[start + i] != data.get(ValueLayout.JAVA_BYTE, rowStart + i)) {
                    return false;
                }
            }
            return true;
        }
        return MemorySegment.mismatch(recSeg[c], start, start + len, data, rowStart,
                rowStart + len)
                == -1;
    }

    private void appendRecord(int c, int gid, Bound keys,
            int row, boolean isNull) {
        int used = recUsed[c];
        if (!isNull) {
            MemorySegment data;
            long start;
            int len;
            if (keys.dictEncoded[c]) {
                int idx = keys.getInt(c, row);
                MemorySegment o = keys.dictOffsets[c];
                start = o.get(VectorBuffers.LE_INT, (long) idx << 2);
                len = o.get(VectorBuffers.LE_INT, (long) (idx + 1) << 2) - (int) start;
                data = keys.dictData[c];
            } else {
                MemorySegment o = keys.offsets[c];
                start = o.get(VectorBuffers.LE_INT, (long) row << 2);
                len = o.get(VectorBuffers.LE_INT, (long) (row + 1) << 2) - (int) start;
                data = keys.data[c];
            }
            ensureRecordBytes(c, used + len);
            MemorySegment.copy(data, ValueLayout.JAVA_BYTE, start, recBytes[c], used, len);
            used += len;
        }
        recUsed[c] = used;
        recOff[c][gid + 1] = used;
    }

    private void ensureRecordBytes(int c, int needed) {
        if (needed > recBytes[c].length) {
            recBytes[c] = Arrays.copyOf(recBytes[c], Math.max(recBytes[c].length * 2, needed));
            recSeg[c] = MemorySegment.ofArray(recBytes[c]);
        }
    }

    /**
     * Switches UTF8 column {@code c} from dictionary ids to record bytes: every
     * group's value is copied out of the dictionary into the column's arena,
     * the dictionary is dropped, and -- since a record column hashes its bytes
     * where an id column hashed the id -- every group hash is recomputed and
     * the slots rebuilt. Runs between two batches, once per column at most.
     */
    private void toRecord(int c) {
        StringDictionary d = dicts[c];
        BitSet nul = nulls[c];
        startRecord(c);
        if (recOff[c].length < size + 1) {
            recOff[c] = new int[Math.max(size + 1, groupCap + 1)];
        }
        ensureRecordBytes(c, (int) Math.min(Integer.MAX_VALUE - 8, Math.max(256, d.valueBytes() * 2)));
        byte[] store = d.bytes();
        int[] off = recOff[c];
        int used = 0;
        for (int gid = 0; gid < size; gid++) {
            if (!nul.get(gid)) {
                int id = keyInt(c, gid);
                int len = d.length(id);
                ensureRecordBytes(c, used + len);
                System.arraycopy(store, d.offset(id), recBytes[c], used, len);
                used += len;
            }
            off[gid + 1] = used;
        }
        recUsed[c] = used;
        dicts[c] = null;
        clearSlots();
        for (int gid = 0; gid < size; gid++) {
            int hash = groupHash(gid); // the record bytes hash differently from the dictionary ids
            setEntry(emptySlot(hash), hash, gid);
        }
    }

    /**
     * The row hash {@link HashKernels#mixColumn} gives a row equal to group
     * {@code gid}, from the stored keys.
     */
    private int groupHash(int gid) {
        int h = HashKernels.SEED;
        for (int c = 0; c < types.length; c++) {
            int v;
            if (nulls[c].get(gid)) {
                v = HashKernels.NULL_MARK;
            } else {
                v = switch (types[c]) {
                            case INT32, BOOL -> keyInt(c, gid);
                            case INT64, FLOAT64 -> HashKernels.fold(keyLong(c, gid, 0));
                            case DECIMAL128 -> HashKernels.fold(Decimal128.hash(keyLong(c, gid, 2), keyLong(c, gid, 0)));
                            case UTF8 -> record[c] ? HashKernels.hashBytes(recSeg[c], recOff[c][gid], recOff[c][gid + 1] - recOff[c][gid]) : keyInt(c, gid);
                            case LIST, MAP, STRUCT -> throw new IllegalStateException("no nested grouping keys (#705)");
                            case FIXED_BINARY -> throw new IllegalStateException("no FIXED_BINARY grouping keys: the lane does not reach operators");
                        };
            }
            h = HashKernels.mix32(h, v);
        }
        return HashKernels.finish(h);
    }

    private int insert(Bound keys, int row, int hash,
                       int pos) {
        int gid = size;
        ensureGroupCapacity(gid + 1);
        for (int c = 0; c < types.length; c++) {
            boolean isNull = keys.isNull(c, row);
            nulls[c].set(gid, isNull);
            switch (types[c]) {
                case INT32 -> setKeyInt(c, gid, isNull ? 0 : keys.getInt(c, row));
                case BOOL -> setKeyInt(c, gid,
                        isNull ? 0 : (keys.getBoolean(c, row) ? 1 : 0));
                case INT64 -> setKeyLong(c, gid, 0,
                        isNull ? 0L : keys.getLong(c, row));
                case FLOAT64 -> setKeyLong(c, gid, 0,
                        isNull ? 0L : Double.doubleToRawLongBits(keys.getDouble(c, row)));
                case DECIMAL128 -> {
                    setKeyLong(c, gid, 0,
                            isNull ? 0L : Decimal128.lo(keys.data[c], row));
                    setKeyLong(c, gid, 2,
                            isNull ? 0L : Decimal128.hi(keys.data[c], row));
                }
                case LIST, MAP, STRUCT, FIXED_BINARY ->
                        throw new IllegalStateException("no nested or FIXED_BINARY lane here (#705)");
                case UTF8 -> {
                    if (record[c]) {
                        appendRecord(c, gid, keys, row, isNull);
                    } else {
                        setKeyInt(c, gid, isNull ? 0 : keys.getId(c, row));
                    }
                }
            }
        }
        setEntry(pos, hash, gid);
        if (fast != null || fastSeg != null) {
            if (anyNullKey(gid)) {
                fast = null; // a null key cannot be packed: the general probe from now on
                freeFastSeg();
            } else {
                setPair(pos, packedKey(gid), gid);
            }
        }
        size++;
        if (size * 10L > (long) slotCount * 7L) {
            rehash();
        }
        return gid;
    }

    private boolean anyNullKey(int gid) {
        for (int c = 0; c < types.length; c++) {
            if (nulls[c].get(gid)) {
                return true;
            }
        }
        return false;
    }

    private void ensureGroupCapacity(int needed) {
        if (needed <= groupCap) {
            return;
        }
        int cap = Math.max(needed, groupCap * 2);
        groupCap = cap;
        if (groupHashes != null) {
            groupHashes = Arrays.copyOf(groupHashes, cap);
        }
        growRows(cap);
        for (int c = 0; c < types.length; c++) {
            if (types[c] == VecType.UTF8 && record[c]) {
                recOff[c] = Arrays.copyOf(recOff[c], cap + 1);
            }
        }
    }

    /**
     * The heap the table holds right now, as allocated (#367): the slots and
     * hashes at capacity, the key arrays at capacity, the string dictionaries
     * and record arenas as allocated. The next growth step doubles the array it
     * touches and holds both copies for its duration -- the caller adds that
     * headroom.
     */
    public long memoryBytes() {
        long bytes = (slots == null ? 0L : 8L * slots.length)
                + (splitSlots == null ? 0L : 4L * splitSlots.length + 4L * groupHashes.length)
                + (fast == null ? 0L : 8L * fast.length);
        bytes += rows == null ? 0L : 4L * rows.length;
        for (int c = 0; c < types.length; c++) {
            if (types[c] == VecType.UTF8) {
                bytes += record[c] ? 4L * recOff[c].length + recBytes[c].length : dicts[c].memoryBytes();
            }
        }
        if (strCols > 0) {
            bytes += emitScratch.length;
        }
        if (packedIndex != null) {
            bytes += 8L * packedIndex.length + 8L * packedKeysScratch.length;
            for (int[] codes : packedCodes) {
                bytes += codes == null ? 0L : 4L * codes.length;
            }
            for (LongCodes m : packedValues) {
                bytes += m == null ? 0L : m.memoryBytes();
            }
        }
        return bytes;
    }

    /**
     * Slot count from which an inline-key table grows by four instead of two
     * (#677): a partial aggregate over a million new groups per task rehashed
     * ~20 times, each a pass of random writes over both slot arrays (~6% of
     * q23a's {@code frequent_ss_items} stage at 1 TB). Growing by four halves
     * the number of passes once the table is out of cache, for at most twice
     * the slot memory at the step.
     */
    static final int FAST_GROW4_SLOTS = 1 << 17;

    private void rehash() {
        boolean inline = fast != null || fastSeg != null;
        int grow = inline && slotCount >= FAST_GROW4_SLOTS ? 4 : 2;
        int newCount = slotCount * grow;
        int newMask = newCount - 1;
        // #941 phase 2b: the entries go to (or stay in) the pool past NATIVE_FAST_SLOTS. The generic probes read
        // the slot fields on every call, so they can move right here, mid-batch.
        // Below PACKED_SLOTS the split arrays stay (the faster layout in cache); a table never goes back.
        boolean slotsNative = slotSeg != null
                || (nativeAllowed && GroupedAccumulators.nativeState() && newCount >= NATIVE_FAST_SLOTS);
        boolean packed = !slotsNative && (slots != null || newCount >= PACKED_SLOTS);
        int[] newSplit = null;
        long[] newSlots = null;
        Arena newSlotArena = null;
        MemorySegment newSlotSeg = null;
        if (slotsNative) {
            newSlotArena = NativePool.shared().newArena();
            newSlotSeg = newSlotArena.allocate(8L * newCount, 64);
            newSlotSeg.fill((byte) -1); // every entry EMPTY
        } else if (packed) {
            newSlots = new long[newCount];
            Arrays.fill(newSlots, EMPTY);
        } else {
            newSplit = new int[newCount];
            Arrays.fill(newSplit, -1);
        }
        // #941: past NATIVE_FAST_SLOTS the inline pairs go to (or stay in) the pool.
        // A heap table crossing the size moves at the start of the next assign (pendingNative): the loop that
        // triggered this growth is reading the heap pairs. A table already in the pool regrows there.
        boolean toNative = fastSeg != null; // once in the pool, a table stays there (its loops read fastSeg)
        if (fast != null
                && nativeAllowed
                && GroupedAccumulators.nativeState()
                && newCount >= NATIVE_FAST_SLOTS) {
            pendingNative = true;
        }
        Arena oldArena = fastArena;
        long[] newFast = inline && !toNative
                ? emptyFast(newCount)
                : null;
        if (toNative) {
            fastArena = null;
            allocFastSeg(newCount);
        }
        MemorySegment nf = toNative ? fastSeg : null;
        for (int old = 0; old < slotCount; old++) {
            long e = entry(old);
            if (e == EMPTY) {
                continue;
            }
            int hash = (int) (e >>> 32);
            int gid = (int) e;
            int pos = hash & newMask;
            if (newSplit != null) {
                while (newSplit[pos] >= 0) {
                    pos = (pos + 1) & newMask;
                }
                newSplit[pos] = gid; // its hash stays in groupHashes
            } else if (newSlotSeg == null) {
                while (newSlots[pos] != EMPTY) {
                    pos = (pos + 1) & newMask;
                }
                newSlots[pos] = e;
            } else {
                while (newSlotSeg.getAtIndex(PAIR, pos) != EMPTY) {
                    pos = (pos + 1) & newMask;
                }
                newSlotSeg.setAtIndex(PAIR, pos, e);
            }
            if (newFast != null) {
                newFast[2 * pos] = packedKey(gid);
                newFast[2 * pos + 1] = gid;
            } else if (nf != null) {
                nf.setAtIndex(PAIR, 2L * pos, packedKey(gid));
                nf.setAtIndex(PAIR, 2L * pos + 1, gid);
            }
        }
        if (oldArena != null) {
            oldArena.close();
        }
        if (!toNative) {
            fastSeg = null;
            fastArena = null;
        }
        if (slotArena != null) {
            slotArena.close();
        }
        if (newSplit == null) {
            groupHashes = null; // the entries carry the hash from here on
        }
        splitSlots = newSplit;
        slots = newSlots;
        slotSeg = newSlotSeg;
        slotArena = newSlotArena;
        slotCount = newCount;
        fast = newFast;
        mask = newMask;
    }

    // ------------------------------------------------------------------ reading keys back

    public boolean isNull(int c, int gid) {
        return nulls[c].get(gid);
    }

    public int getInt(int c, int gid) {
        return keyInt(c, gid);
    }

    public boolean getBoolean(int c, int gid) {
        return keyInt(c, gid) != 0;
    }

    public long getLong(int c, int gid) {
        return keyLong(c, gid, 0);
    }

    public double getDouble(int c, int gid) {
        return Double.longBitsToDouble(keyLong(c, gid, 0));
    }

    /** The 128-bit key of group {@code gid} of a DECIMAL128 column. */
    public java.math.BigInteger getDecimal128(int c, int gid) {
        return Decimal128.toBigInteger(keyLong(c, gid, 2), keyLong(c, gid, 0));
    }

    public String getString(int c, int gid) {
        if (record[c]) {
            int start = recOff[c][gid];
            return new String(recBytes[c], start, recOff[c][gid + 1] - start, java.nio.charset.StandardCharsets.UTF_8);
        }
        StringDictionary d = dicts[c];
        int id = keyInt(c, gid);
        return new String(d.bytes(), d.offset(id), d.length(id),
                java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * The dictionary id of group {@code gid}'s value in UTF8 column {@code c}
     * (0 for a null); -1 for a column in record mode.
     */
    public int getStringId(int c, int gid) {
        return record[c] ? -1 : keyInt(c, gid);
    }

    /**
     * Number of distinct values UTF8 column {@code c} has seen; 0 for a column
     * in record mode (see {@link #isDictionaryColumn}).
     */
    public int dictionarySize(int c) {
        return record[c] ? 0 : dicts[c].size();
    }

    /**
     * Bytes of the distinct values of UTF8 column {@code c}; the arena's bytes
     * for a column in record mode.
     */
    public long dictionaryBytes(int c) {
        return record[c] ? recUsed[c] : dicts[c].valueBytes();
    }

    /**
     * The distinct values of UTF8 column {@code c} as a plain UTF8 column,
     * indexed by id (#377): the dictionary the ids {@link #writeKeyIds} emits
     * refer to. Valid until the next {@link #assign}; only for a column {@link
     * #isDictionaryColumn} says is in dictionary mode.
     */
    public VectorBuffers dictionary(int c) {
        if (record[c]) {
            throw new IllegalStateException("UTF8 key " + c + " is in record mode: no dictionary");
        }
        return dicts[c].view();
    }

    /**
     * Writes the dictionary ids of groups {@code [from, to)} of UTF8 column
     * {@code c} as an INT32 Arrow-layout column (validity bits for every row,
     * id 0 under a null). Dictionary mode only.
     */
    public void writeKeyIds(int c, int from, int to,
                            MemorySegment validity, MemorySegment ids) {
        if (record[c]) {
            throw new IllegalStateException("UTF8 key " + c + " is in record mode: no ids");
        }
        int count = to - from;
        for (int o = 0; o < count; o++) {
            Bitmap.setTo(validity, o, !nulls[c].get(from + o));
        }
        for (int o = 0; o < count; o++) {
            ids.set(VectorBuffers.LE_INT, (long) o << 2, keyInt(c, from + o));
        }
    }

    /** Total UTF-8 bytes of the keys in {@code [from, to)} of column {@code c}. */
    public long utf8Bytes(int c, int from, int to) {
        if (record[c]) {
            return (long) recOff[c][to] - recOff[c][from];
        }
        StringDictionary d = dicts[c];
        BitSet nul = nulls[c];
        long total = 0;
        for (int gid = from; gid < to; gid++) {
            if (!nul.get(gid)) {
                total += d.length(keyInt(c, gid));
            }
        }
        return total;
    }

    /**
     * Writes the keys of groups {@code [from, to)} of column {@code c} into
     * Arrow-layout output buffers ({@code offsets} only for UTF8). Validity
     * bits are written for every row.
     */
    public void writeKeys(int c, int from, int to,
                          MemorySegment validity, MemorySegment data, MemorySegment offsets) {
        int count = to - from;
        for (int o = 0; o < count; o++) {
            Bitmap.setTo(validity, o, !nulls[c].get(from + o));
        }
        switch (types[c]) {
            case INT32 -> {
                for (int o = 0; o < count; o++) {
                    data.set(VectorBuffers.LE_INT, (long) o << 2, keyInt(c, from + o));
                }
            }
            case BOOL -> {
                for (int o = 0; o < count; o++) {
                    Bitmap.setTo(data, o, keyInt(c, from + o) != 0);
                }
            }
            case INT64, FLOAT64 -> {
                for (int o = 0; o < count; o++) {
                    data.set(VectorBuffers.LE_LONG, (long) o << 3, keyLong(c, from + o, 0));
                }
            }
            case DECIMAL128 -> {
                for (int o = 0; o < count; o++) {
                    Decimal128.set(data, o, keyLong(c, from + o, 2), keyLong(c, from + o, 0));
                }
            }
            case LIST, MAP, STRUCT, FIXED_BINARY ->
                    throw new IllegalStateException("no nested or FIXED_BINARY lane here (#705)");
            case UTF8 -> {
                if (record[c]) {
                    // Groups are appended in id order, so [from, to) is one contiguous range of the arena.
                    int[] off = recOff[c];
                    int base = off[from];
                    for (int o = 0; o <= count; o++) {
                        offsets.set(VectorBuffers.LE_INT, (long) o << 2, off[from + o] - base);
                    }
                    MemorySegment.copy(recBytes[c], base, data, ValueLayout.JAVA_BYTE, 0,
                            off[to] - base);
                    return;
                }
                // Gather the values from the dictionary into a heap buffer (System.arraycopy, no per-value
                // segment checks) and copy them out once: one small MemorySegment.copy per value was 15% of
                // an executor's time in q67 at 1 TB, whose finest level emits nearly every row.
                StringDictionary d = dicts[c];
                BitSet nul = nulls[c];
                int total = (int) utf8Bytes(c, from, to);
                if (emitScratch.length < total) {
                    emitScratch = new byte[Math.max(total, emitScratch.length * 2)];
                }
                byte[] scratch = emitScratch;
                byte[] store = d.bytes();
                int out = 0;
                offsets.set(VectorBuffers.LE_INT, 0L, 0);
                for (int o = 0; o < count; o++) {
                    int gid = from + o;
                    if (!nul.get(gid)) {
                        int id = keyInt(c, gid);
                        int len = d.length(id);
                        System.arraycopy(store, d.offset(id), scratch, out, len);
                        out += len;
                    }
                    offsets.set(VectorBuffers.LE_INT, (long) (o + 1) << 2, out);
                }
                MemorySegment.copy(scratch, 0, data, ValueLayout.JAVA_BYTE, 0, out);
            }
        }
    }
}
