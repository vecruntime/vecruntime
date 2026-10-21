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
 * A list or a map column whose element (or key and value) is a leaf, or a
 * struct of leaves (#568; Spark-free since #900). The rows come from the first
 * leaf's repetition and definition levels: an entry with repetition level 0
 * starts a row, a definition level below the list's own marks a null row, and
 * one at or above the element's level is an element. Each leaf then reads its
 * elements back to back ({@link LeafColumnReader#readElements}). A batch is a
 * {@link Batch} of the row offsets, the null rows and each leaf's elements; the
 * engine's adapter wraps it as its list or map column.
 */
public class ListColumnReader implements ColumnReader, RowLevels {

    private final boolean map; // a map (key, value) rather than a list
    private final LeafColumnReader first; // the element, or the key
    private final LeafColumnReader second; // the value, or null
    private final int listDef; // the list is present at this level (0: never null)
    private final int elementDef; // the entry is an element at this level
    // An array of structs of leaves: one reader per field, in the struct's order (fields[0] is `first`), and the
    // level at which an element's struct is present (elementDef when the repeated group is the struct itself).
    // A map of structs (#568): fields[0] is the key, fields[1..] the value struct's fields, and structDef the level
    // at which an entry's value struct is present.
    private final LeafColumnReader[] fields;
    private final int structDef;
    private int[] rowLevels = new int[0]; // each row's first-entry level, last batch

    /**
     * A list of leaves ({@code second} null) or a map of a leaf key to a leaf
     * value.
     */
    public ListColumnReader(boolean map, LeafColumnReader first, LeafColumnReader second,
                            int listDef, int elementDef) {
        this.map = map;
        this.first = first;
        this.second = second;
        this.listDef = listDef;
        this.elementDef = elementDef;
        this.fields = null;
        this.structDef = elementDef;
        first.setDictionaryIds(false);
        if (second != null) {
            second.setDictionaryIds(false);
        }
    }

    /**
     * A list of structs of leaves, or ({@code map}) a map of a leaf key to a
     * struct of leaves.
     */
    public ListColumnReader(boolean map, LeafColumnReader[] fields, int listDef,
                            int elementDef, int structDef) {
        this.map = map;
        this.first = fields[0];
        this.second = null;
        this.listDef = listDef;
        this.elementDef = elementDef;
        this.fields = fields;
        this.structDef = structDef;
        for (LeafColumnReader f : fields) {
            f.setDictionaryIds(false);
        }
    }

    /**
     * A list batch: its rows, null rows ({@code null}: none), offsets and each
     * leaf's elements.
     */
    public static final class Batch {
        public final int rows;
        public final boolean[] nulls;
        public final int[] offsets;
        public final FieldVector a;
        public final FieldVector b;
        public final FieldVector[] fs; // an array of structs: each field's elements
        public final boolean[] elementNulls; // and its null structs (null: none)
        public final int elements;

        Batch(int rows, boolean[] nulls, int[] offsets,
              FieldVector a, FieldVector b) {
            this.rows = rows;
            this.nulls = nulls;
            this.offsets = offsets;
            this.a = a;
            this.b = b;
            this.fs = null;
            this.elementNulls = null;
            this.elements = 0;
        }

        Batch(int rows, boolean[] nulls, int[] offsets,
              FieldVector[] fs, boolean[] elementNulls, int elements) {
            this.rows = rows;
            this.nulls = nulls;
            this.offsets = offsets;
            this.a = null;
            this.b = null;
            this.fs = fs;
            this.elementNulls = elementNulls;
            this.elements = elements;
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        first.startRowGroup(store, rows);
        if (second != null) {
            second.startRowGroup(store, rows);
        }
        for (LeafColumnReader f : others()) {
            f.startRowGroup(store, rows);
        }
    }

    @Override
    public Object readBatch(int n) {
        int entries = first.entriesForRows(n);
        int[] r = first.repLevels();
        int[] d = first.defLevels();
        int off = first.levelOffset();
        int[] offsets = new int[n + 1];
        if (rowLevels.length < n) {
            rowLevels = new int[n];
        }
        boolean[] nulls = null;
        int row = -1;
        int count = 0;
        boolean[] elementNulls = null;
        for (int k = 0; k < entries; k++) {
            int def = d[off + k];
            if (r[off + k] == 0) {
                row++;
                offsets[row] = count;
                rowLevels[row] = def;
                if (def < listDef) {
                    if (nulls == null) {
                        nulls = new boolean[n];
                    }
                    nulls[row] = true;
                }
            }
            if (def >= elementDef) {
                if (fields != null && !map && def < structDef) {
                    if (elementNulls == null) {
                        elementNulls = new boolean[entries];
                    }
                    elementNulls[count] = true;
                }
                count++;
            }
        }
        if (row != n - 1) {
            throw new IllegalStateException("a list column ended at row " + (row + 1) + " of " + n);
        }
        offsets[n] = count;
        if (fields != null) {
            FieldVector[] fs = new FieldVector[fields.length];
            try {
                fs[0] = first.readElements(n, entries, elementDef);
                for (int i = 1; i < fields.length; i++) {
                    int e = fields[i].entriesForRows(n);
                    if (i == 1 && map) {
                        // A map's value struct is null where its first field's level is below the struct's; the
                        // key is required, so its own levels cannot tell.
                        elementNulls = valueNulls(fields[1], e, count);
                    }
                    fs[i] = fields[i].readElements(n, e, elementDef);
                }
            } catch (RuntimeException | Error e) {
                for (int i = 0; i < fs.length; i++) {
                    fields[i].release(fs[i]);
                }
                throw e;
            }
            return new Batch(n, nulls, offsets, fs, elementNulls, count);
        }
        FieldVector a = first.readElements(n, entries, elementDef);
        FieldVector b = null;
        if (second != null) {
            try {
                b = second.readElements(n, second.entriesForRows(n), elementDef);
            } catch (RuntimeException | Error e) {
                first.release(a);
                throw e;
            }
        }
        return new Batch(n, nulls, offsets, a, b);
    }

    /**
     * The null value structs among a map's next {@code entries} entries ({@code
     * null}: none).
     */
    private boolean[] valueNulls(LeafColumnReader valueField, int entries, int elements) {
        int[] d = valueField.defLevels();
        int off = valueField.levelOffset();
        boolean[] out = null;
        int at = 0;
        for (int k = 0; k < entries; k++) {
            int def = d[off + k];
            if (def >= elementDef) {
                if (def < structDef) {
                    if (out == null) {
                        out = new boolean[elements];
                    }
                    out[at] = true;
                }
                at++;
            }
        }
        return out;
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
        first.skipRows(n);
        if (second != null) {
            second.skipRows(n);
        }
        for (LeafColumnReader f : others()) {
            f.skipRows(n);
        }
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            Batch b = (Batch) batch;
            if (fields != null) {
                for (int i = 0; i < fields.length; i++) {
                    fields[i].release(b.fs[i]);
                }
                return;
            }
            first.release(b.a);
            if (second != null) {
                second.release(b.b);
            }
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        first.setPoolLimit(limit);
        if (second != null) {
            second.setPoolLimit(limit);
        }
        for (LeafColumnReader f : others()) {
            f.setPoolLimit(limit);
        }
    }

    @Override
    public void setReuseDecompress(boolean reuse) {
        first.setReuseDecompress(reuse);
        if (second != null) {
            second.setReuseDecompress(reuse);
        }
        for (LeafColumnReader f : others()) {
            f.setReuseDecompress(reuse);
        }
    }

    @Override
    public void setRebase(io.vecruntime.parquet.read.CalendarRebase rebase) {
        first.setRebase(rebase);
        if (second != null) {
            second.setRebase(rebase);
        }
        for (LeafColumnReader f : others()) {
            f.setRebase(rebase);
        }
    }

    /**
     * #568: a list's or map's strings are dictionary ids too (#612), where the
     * leaf allows it: keys, values, elements and the fields of a list of
     * structs. Spark's getters and the adapters both read a dictionary vector.
     */
    @Override
    public void setDictionaryIds(boolean enabled) {
        first.setDictionaryIds(enabled);
        if (second != null) {
            second.setDictionaryIds(enabled);
        }
        for (LeafColumnReader f : others()) {
            f.setDictionaryIds(enabled);
        }
    }

    @Override
    public int rowsRemaining() {
        return first.rowsRemaining();
    }

    @Override
    public void close() {
        first.close();
        if (second != null) {
            second.close();
        }
        for (LeafColumnReader f : others()) {
            f.close();
        }
    }

    /** An array of structs' readers after the first; none otherwise. */
    private LeafColumnReader[] others() {
        return fields == null ? new LeafColumnReader[0] : java.util.Arrays.copyOfRange(fields, 1, fields.length);
    }

    @Override
    public int[] rowLevels() {
        return rowLevels;
    }

    /** Always kept: one store per row while the offsets are built. */
    @Override
    public void recordRowLevels() {}
}
