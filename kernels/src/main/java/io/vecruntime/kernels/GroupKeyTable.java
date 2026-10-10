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
                Math.max(largest(s.ids), largest(s.entryIds)));
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
    private int[] slots; // group id or -1
    private int mask;
    private int size;
    private int[] groupHashes = new int[INITIAL_CAPACITY];

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

    private final int[][] intKeys; // INT32 and BOOL (0/1)
    private final long[][] longKeys; // INT64 and FLOAT64 (raw bits); the low limb of DECIMAL128
    private final long[][] hiKeys; // the high limb of DECIMAL128
    private final int[][] strIds; // UTF8 in dictionary mode: the id of the group's value in dicts[c]
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
        this.slots = new int[INITIAL_CAPACITY * 2];
        Arrays.fill(slots, -1);
        this.mask = slots.length - 1;
        if (fastKeys(types)) {
            this.fast = emptyFast(slots.length);
        }
        int k = types.length;
        intKeys = new int[k][];
        longKeys = new long[k][];
        hiKeys = new long[k][];
        strIds = new int[k][];
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
                case INT32, BOOL -> intKeys[c] = new int[INITIAL_CAPACITY];
                case INT64, FLOAT64 -> longKeys[c] = new long[INITIAL_CAPACITY];
                case DECIMAL128 -> {
                    longKeys[c] = new long[INITIAL_CAPACITY];
                    hiKeys[c] = new long[INITIAL_CAPACITY];
                }
                case UTF8 -> {
                    strCol[c] = s++;
                    if (dictionaryLimit > 0) {
                        strIds[c] = new int[INITIAL_CAPACITY];
                        dicts[c] = new StringDictionary();
                    } else {
                        startRecord(c);
                    }
                }
            }
        }
        strCols = s;
    }

    private void startRecord(int c) {
        record[c] = true;
        recBytes[c] = new byte[Math.max(256, INITIAL_CAPACITY * 8)];
        recSeg[c] = MemorySegment.ofArray(recBytes[c]);
        recOff[c] = new int[Math.max(groupHashes.length, INITIAL_CAPACITY) + 1];
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
        if (hashScratch.length < n) {
            hashScratch = new int[Math.max(n, hashScratch.length * 2)];
        }
        int[] hashes = hashScratch;
        HashKernels.init(hashes, n);
        for (VectorBuffers key : keys) {
            HashKernels.mixColumn(key, hashes);
        }
        Bound b = Bound.of(keys, ids);
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
        while (true) {
            int gid = slots[pos];
            if (gid < 0) {
                return -1;
            }
            if (groupHashes[gid] == hash && equals(gid, keys, row)) {
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
                if (intKeys[c][gid] != keys.ints[c][row]) {
                    return false;
                }
            } else if (longKeys[c][gid] != keys.longs[c][row]) {
                return false;
            }
        }
        return true;
    }

    private int lookupOnly(Bound keys, int row, int hash) {
        int pos = hash & mask;
        while (true) {
            int gid = slots[pos];
            if (gid < 0) {
                return -1;
            }
            if (groupHashes[gid] == hash && equals(gid, keys, row)) {
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
        int[][] entryIds = new int[0][]; // per column: input dictionary entry -> id
        int[][] entryGen = new int[0][]; // per column: the generation entryIds[c][e] was computed in
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
            for (int c = 0; c < s.entryIds.length; c++) {
                if (s.entryIds[c] != null && s.entryIds[c].length > KEEP_ELEMENTS) {
                    s.entryIds[c] = null;
                    s.entryGen[c] = null;
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
                s.entryIds = Arrays.copyOf(s.entryIds, k);
                s.entryGen = Arrays.copyOf(s.entryGen, k);
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
                int[] entryIds = s.entryIds[c];
                int[] entryGen = s.entryGen[c];
                if (entryIds == null || entryIds.length < m) {
                    entryIds = new int[Math.max(m, entryIds == null ? 256 : entryIds.length * 2)];
                    entryGen = new int[entryIds.length];
                    s.entryIds[c] = entryIds;
                    s.entryGen[c] = entryGen;
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
                        Arrays.fill(entryGen, 0);
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
                    if (entryGen[e] != gen) {
                        int id;
                        if (dValidity != null && !Bitmap.isSet(dValidity, e)) {
                            id = 0; // a null entry: the row is null through the dictionary; not a key value
                        } else {
                            int start = dOff.get(VectorBuffers.LE_INT, (long) e << 2);
                            int len = dOff.get(VectorBuffers.LE_INT, (long) (e + 1) << 2) - start;
                            id = dict.indexOf(dData, start, len, insert, s.entryBytes);
                        }
                        entryIds[e] = id;
                        entryGen[e] = gen;
                    }
                    ids[i] = entryIds[e];
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

    // ------------------------------------------------------------------ inline integer keys (#677)

    /**
     * Package-private, for the tests: whether the table still probes its keys
     * inline.
     */
    boolean inlineKeys() {
        return fast != null;
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
            return ((long) intKeys[0][gid] << 32) | (intKeys[1][gid] & 0xFFFFFFFFL);
        }
        return types[0] == VecType.INT32 ? intKeys[0][gid] : longKeys[0][gid];
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
        if (slots.length < PROBE_BATCH_SLOTS) {
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
        if (types.length == 2) {
            int x = b.ints[0][i];
            int y = b.ints[1][i];
            return fastLookupOrInsert(((long) x << 32) | (y & 0xFFFFFFFFL),
                    hash, x, y, 0L);
        }
        if (types[0] == VecType.INT32) {
            int x = b.ints[0][i];
            return fastLookupOrInsert(x, hash, x, 0, 0L);
        }
        long l = b.longs[0][i];
        return fastLookupOrInsert(l, hash, 0, 0, l);
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
        groupHashes[gid] = hash;
        if (types.length == 2) {
            intKeys[0][gid] = a;
            intKeys[1][gid] = b;
        } else if (types[0] == VecType.INT32) {
            intKeys[0][gid] = a;
        } else {
            longKeys[0][gid] = l;
        }
        slots[pos] = gid;
        fast[2 * pos] = key;
        fast[2 * pos + 1] = gid;
        size++;
        if (size * 10L > (long) slots.length * 7L) {
            rehash();
        }
        return gid;
    }

    private int lookupOrInsert(Bound keys, int row, int hash) {
        int pos = hash & mask;
        while (true) {
            int gid = slots[pos];
            if (gid < 0) {
                return insert(keys, row, hash, pos);
            }
            if (groupHashes[gid] == hash && equals(gid, keys, row)) {
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
                    if (intKeys[c][gid] != keys.getInt(c, row)) {
                        return false;
                    }
                }
                case BOOL -> {
                    if (intKeys[c][gid] != (keys.getBoolean(c, row) ? 1 : 0)) {
                        return false;
                    }
                }
                case INT64 -> {
                    if (longKeys[c][gid] != keys.getLong(c, row)) {
                        return false;
                    }
                }
                case FLOAT64 -> {
                    if (longKeys[c][gid] != Double.doubleToRawLongBits(keys.getDouble(c, row))) {
                        return false;
                    }
                }
                case DECIMAL128 -> {
                    MemorySegment d = keys.data[c];
                    if (longKeys[c][gid] != Decimal128.lo(d, row) || hiKeys[c][gid] != Decimal128.hi(d, row)) {
                        return false;
                    }
                }
                case UTF8 -> {
                    if (record[c] ? !recordEquals(c, gid, keys, row) : strIds[c][gid] != keys.getId(c, row)) {
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
        int[] ids = strIds[c];
        BitSet nul = nulls[c];
        startRecord(c);
        if (recOff[c].length < size + 1) {
            recOff[c] = new int[Math.max(size + 1, groupHashes.length + 1)];
        }
        ensureRecordBytes(c, (int) Math.min(Integer.MAX_VALUE - 8, Math.max(256, d.valueBytes() * 2)));
        byte[] store = d.bytes();
        int[] off = recOff[c];
        int used = 0;
        for (int gid = 0; gid < size; gid++) {
            if (!nul.get(gid)) {
                int id = ids[gid];
                int len = d.length(id);
                ensureRecordBytes(c, used + len);
                System.arraycopy(store, d.offset(id), recBytes[c], used, len);
                used += len;
            }
            off[gid + 1] = used;
        }
        recUsed[c] = used;
        dicts[c] = null;
        strIds[c] = null;
        for (int gid = 0; gid < size; gid++) {
            groupHashes[gid] = groupHash(gid);
        }
        Arrays.fill(slots, -1);
        for (int gid = 0; gid < size; gid++) {
            int pos = groupHashes[gid] & mask;
            while (slots[pos] >= 0) {
                pos = (pos + 1) & mask;
            }
            slots[pos] = gid;
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
                            case INT32, BOOL -> intKeys[c][gid];
                            case INT64, FLOAT64 -> HashKernels.fold(longKeys[c][gid]);
                            case DECIMAL128 -> HashKernels.fold(Decimal128.hash(hiKeys[c][gid], longKeys[c][gid]));
                            case UTF8 -> record[c] ? HashKernels.hashBytes(recSeg[c], recOff[c][gid], recOff[c][gid + 1] - recOff[c][gid]) : strIds[c][gid];
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
        groupHashes[gid] = hash;
        for (int c = 0; c < types.length; c++) {
            boolean isNull = keys.isNull(c, row);
            nulls[c].set(gid, isNull);
            switch (types[c]) {
                case INT32 -> intKeys[c][gid] = isNull ? 0 : keys.getInt(c, row);
                case BOOL -> intKeys[c][gid] = isNull ? 0 : (keys.getBoolean(c, row) ? 1 : 0);
                case INT64 -> longKeys[c][gid] = isNull ? 0L : keys.getLong(c, row);
                case FLOAT64 -> longKeys[c][gid] = isNull ? 0L : Double.doubleToRawLongBits(keys.getDouble(c, row));
                case DECIMAL128 -> {
                    longKeys[c][gid] = isNull ? 0L : Decimal128.lo(keys.data[c], row);
                    hiKeys[c][gid] = isNull ? 0L : Decimal128.hi(keys.data[c], row);
                }
                case UTF8 -> {
                    if (record[c]) {
                        appendRecord(c, gid, keys, row, isNull);
                    } else {
                        strIds[c][gid] = isNull ? 0 : keys.getId(c, row);
                    }
                }
            }
        }
        slots[pos] = gid;
        if (fast != null) {
            if (anyNullKey(gid)) {
                fast = null; // a null key cannot be packed: the general probe from now on
            } else {
                fast[2 * pos] = packedKey(gid);
                fast[2 * pos + 1] = gid;
            }
        }
        size++;
        if (size * 10L > (long) slots.length * 7L) {
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
        if (needed <= groupHashes.length) {
            return;
        }
        int cap = Math.max(needed, groupHashes.length * 2);
        groupHashes = Arrays.copyOf(groupHashes, cap);
        for (int c = 0; c < types.length; c++) {
            switch (types[c]) {
                case INT32, BOOL -> intKeys[c] = Arrays.copyOf(intKeys[c], cap);
                case INT64, FLOAT64 -> longKeys[c] = Arrays.copyOf(longKeys[c], cap);
                case DECIMAL128 -> {
                    longKeys[c] = Arrays.copyOf(longKeys[c], cap);
                    hiKeys[c] = Arrays.copyOf(hiKeys[c], cap);
                }
                case UTF8 -> {
                    if (record[c]) {
                        recOff[c] = Arrays.copyOf(recOff[c], cap + 1);
                    } else {
                        strIds[c] = Arrays.copyOf(strIds[c], cap);
                    }
                }
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
        long bytes = 4L * slots.length
                + 4L * groupHashes.length
                + (fast == null ? 0L : 8L * fast.length);
        for (int c = 0; c < types.length; c++) {
            switch (types[c]) {
                case INT32, BOOL -> bytes += 4L * intKeys[c].length;
                case INT64, FLOAT64 -> bytes += 8L * longKeys[c].length;
                case DECIMAL128 -> bytes += 16L * longKeys[c].length;
                case UTF8 -> bytes += record[c] ? 4L * recOff[c].length + recBytes[c].length : 4L * strIds[c].length + dicts[c].memoryBytes();
            }
        }
        if (strCols > 0) {
            bytes += emitScratch.length;
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
        int grow = fast != null && slots.length >= FAST_GROW4_SLOTS
                ? 4
                : 2;
        int[] newSlots = new int[slots.length * grow];
        Arrays.fill(newSlots, -1);
        int newMask = newSlots.length - 1;
        long[] newFast = fast == null ? null : emptyFast(newSlots.length);
        for (int gid = 0; gid < size; gid++) {
            int pos = groupHashes[gid] & newMask;
            while (newSlots[pos] >= 0) {
                pos = (pos + 1) & newMask;
            }
            newSlots[pos] = gid;
            if (newFast != null) {
                newFast[2 * pos] = packedKey(gid);
                newFast[2 * pos + 1] = gid;
            }
        }
        slots = newSlots;
        fast = newFast;
        mask = newMask;
    }

    // ------------------------------------------------------------------ reading keys back

    public boolean isNull(int c, int gid) {
        return nulls[c].get(gid);
    }

    public int getInt(int c, int gid) {
        return intKeys[c][gid];
    }

    public boolean getBoolean(int c, int gid) {
        return intKeys[c][gid] != 0;
    }

    public long getLong(int c, int gid) {
        return longKeys[c][gid];
    }

    public double getDouble(int c, int gid) {
        return Double.longBitsToDouble(longKeys[c][gid]);
    }

    /** The 128-bit key of group {@code gid} of a DECIMAL128 column. */
    public java.math.BigInteger getDecimal128(int c, int gid) {
        return Decimal128.toBigInteger(hiKeys[c][gid], longKeys[c][gid]);
    }

    public String getString(int c, int gid) {
        if (record[c]) {
            int start = recOff[c][gid];
            return new String(recBytes[c], start, recOff[c][gid + 1] - start, java.nio.charset.StandardCharsets.UTF_8);
        }
        StringDictionary d = dicts[c];
        int id = strIds[c][gid];
        return new String(d.bytes(), d.offset(id), d.length(id),
                java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * The dictionary id of group {@code gid}'s value in UTF8 column {@code c}
     * (0 for a null); -1 for a column in record mode.
     */
    public int getStringId(int c, int gid) {
        return record[c] ? -1 : strIds[c][gid];
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
        MemorySegment.copy(strIds[c], from, ids, VectorBuffers.LE_INT, 0, count);
    }

    /** Total UTF-8 bytes of the keys in {@code [from, to)} of column {@code c}. */
    public long utf8Bytes(int c, int from, int to) {
        if (record[c]) {
            return (long) recOff[c][to] - recOff[c][from];
        }
        StringDictionary d = dicts[c];
        int[] ids = strIds[c];
        BitSet nul = nulls[c];
        long total = 0;
        for (int gid = from; gid < to; gid++) {
            if (!nul.get(gid)) {
                total += d.length(ids[gid]);
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
            case INT32 -> MemorySegment.copy(intKeys[c], from, data, VectorBuffers.LE_INT, 0, count);
            case BOOL -> {
                for (int o = 0; o < count; o++) {
                    Bitmap.setTo(data, o, intKeys[c][from + o] != 0);
                }
            }
            case INT64, FLOAT64 -> MemorySegment.copy(longKeys[c], from, data, VectorBuffers.LE_LONG, 0, count);
            case DECIMAL128 -> {
                for (int o = 0; o < count; o++) {
                    Decimal128.set(data, o, hiKeys[c][from + o], longKeys[c][from + o]);
                }
            }
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
                int[] ids = strIds[c];
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
                        int id = ids[gid];
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
