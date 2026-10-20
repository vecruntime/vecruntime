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
package io.vecruntime.parquet.read;

import java.lang.foreign.MemorySegment;

import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.column.page.PageReadStore;

/**
 * A list of lists (to any depth) over one leaf (#568; Spark-free since #900).
 * Depth {@code j}'s list exists where an entry's definition level reaches the
 * element level of depth {@code j - 1} (depth 1 always), is null below {@code
 * listDef[j]}, and opens a new element where {@code r <= j} and {@code d >=
 * elemDef[j]}. One pass over the leaf's levels builds every depth's offsets and
 * nulls; the leaf decodes its elements back to back ({@link
 * LeafColumnReader#readElements} at {@code elemDef[depth]}). A batch is a
 * {@link Batch}; the engine's adapter wraps it as nested list columns.
 */
public class NestedListColumnReader implements ColumnReader, RowLevels {

    private final LeafColumnReader leaf;
    private final int depth;
    private final int[] listDef; // 1-based: listDef[j], elemDef[j] for j in 1..depth
    private final int[] elemDef;
    private int[] rowLevels = new int[0]; // each row's first-entry level, last batch

    /**
     * A list of lists, {@code listDef.length - 1} deep, over {@code leaf};
     * index 0 of both arrays unused.
     */
    public NestedListColumnReader(LeafColumnReader leaf, int[] listDef, int[] elemDef) {
        this.leaf = leaf;
        this.depth = listDef.length - 1;
        this.listDef = listDef;
        this.elemDef = elemDef;
        leaf.setDictionaryIds(false);
    }

    /**
     * A batch: each depth's lists (rows at depth 1), their nulls and offsets,
     * and the leaf's elements.
     */
    public static final class Batch {
        public final int[] lists; // lists[j]: depth-j lists
        public final boolean[][] nulls; // nulls[j]: null depth-j lists (null: none)
        public final int[][] offsets; // offsets[j]: lists[j] + 1 offsets into depth j's elements
        public final FieldVector elements;

        Batch(int[] lists, boolean[][] nulls, int[][] offsets,
              FieldVector elements) {
            this.lists = lists;
            this.nulls = nulls;
            this.offsets = offsets;
            this.elements = elements;
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        leaf.startRowGroup(store, rows);
    }

    @Override
    public Object readBatch(int n) {
        int entries = leaf.entriesForRows(n);
        int[] r = leaf.repLevels();
        int[] d = leaf.defLevels();
        int off = leaf.levelOffset();
        int[] lists = new int[depth + 1];
        int[] elems = new int[depth + 1];
        boolean[][] nulls = new boolean[depth + 1][];
        int[][] offsets = new int[depth + 1][];
        // Each depth's list count is bounded by the entries (and depth 1 by the rows).
        offsets[1] = new int[n + 1];
        if (rowLevels.length < n) {
            rowLevels = new int[n];
        }
        for (int j = 2; j <= depth; j++) {
            offsets[j] = new int[entries + 1];
        }
        for (int e = 0; e < entries; e++) {
            int rep = r[off + e];
            int def = d[off + e];
            for (int j = 1; j <= depth; j++) {
                boolean exists = j == 1 || def >= elemDef[j - 1];
                if (rep <= j - 1 && exists) {
                    int at = lists[j];
                    if (j == 1) {
                        rowLevels[at] = def;
                    }
                    offsets[j][at] = elems[j];
                    if (def < listDef[j]) {
                        if (nulls[j] == null) {
                            nulls[j] = new boolean[j == 1 ? n : entries];
                        }
                        nulls[j][at] = true;
                    }
                    lists[j]++;
                }
                if (rep <= j && def >= elemDef[j]) {
                    elems[j]++;
                }
            }
        }
        if (lists[1] != n) {
            throw new IllegalStateException("a list column ended at row " + lists[1] + " of " + n);
        }
        for (int j = 1; j <= depth; j++) {
            offsets[j][lists[j]] = elems[j];
        }
        FieldVector v = leaf.readElements(n, entries, elemDef[depth]);
        return new Batch(lists, nulls, offsets, v);
    }

    @Override
    public Object readBatchSelected(int n, MemorySegment selection) {
        return readBatch(n);
    }

    @Override
    public boolean decodesSelectedFaster() {
        return false;
    }

    @Override
    public void skipRows(int n) {
        leaf.skipRows(n);
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            leaf.release(((Batch) batch).elements);
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        leaf.setPoolLimit(limit);
    }

    @Override
    public void setReuseDecompress(boolean reuse) {
        leaf.setReuseDecompress(reuse);
    }

    @Override
    public void setRebase(io.vecruntime.parquet.read.CalendarRebase rebase) {
        leaf.setRebase(rebase);
    }

    @Override
    public void setDictionaryIds(boolean enabled) {
        leaf.setDictionaryIds(enabled); // #568: innermost strings as dictionary ids (#612)
    }

    @Override
    public int rowsRemaining() {
        return leaf.rowsRemaining();
    }

    @Override
    public int[] rowLevels() {
        return rowLevels;
    }

    /** Always kept: one store per row while the offsets are built. */
    @Override
    public void recordRowLevels() {}

    @Override
    public void close() {
        leaf.close();
    }
}
