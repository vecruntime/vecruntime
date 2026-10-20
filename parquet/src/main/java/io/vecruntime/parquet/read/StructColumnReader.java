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

import org.apache.parquet.column.page.PageReadStore;

/**
 * A struct column (#568, work item 8; Spark-free since #900): one reader per
 * field. A struct has no column chunk of its own; Parquet records its nulls in
 * its leaves' definition levels. The struct is present on a row where a leaf's
 * level reaches the struct's own definition level (the optional groups from the
 * root down to the struct), so its validity comes from its first field's levels
 * -- every leaf under it carries the same information about it. That is how
 * Spark's vectorized reader computes a struct's nulls too.
 *
 * <p>Each field decodes its rows straight into its own batch, as a flat column
 * does; a batch is a {@link Batch} of the fields' batches and the struct's
 * nulls, which the engine's adapter wraps as its struct column. Which fields
 * there are, and which of them only supplies the levels, is the adapter's
 * decision.
 */
public class StructColumnReader implements ColumnReader, RowLevels {

    private final ColumnReader[] fields;
    // The field whose per-row levels give this struct's validity (its first: a leaf, a struct, a list or a map --
    // each carries the same information about the struct), and the level at which the struct is present;
    // null / 0 when no group from the root down to the struct is optional (never null).
    private final RowLevels levelSource;
    private final int defLevel;
    // The first field the file holds: a struct's levels come from it (fields the file lacks are null columns).
    private final RowLevels levelField;

    /**
     * A struct over {@code fields}; field {@code levelIndex} (a {@link
     * RowLevels}) gives the struct's nulls, the rows where its level is below
     * {@code defLevel} (0: the struct is never null).
     */
    public StructColumnReader(ColumnReader[] fields, int levelIndex, int defLevel) {
        this.fields = fields;
        this.levelField = (RowLevels) fields[levelIndex];
        this.levelSource = defLevel > 0 ? levelField : null;
        this.defLevel = defLevel;
        if (this.levelSource != null) {
            this.levelSource.recordRowLevels();
        }
    }

    @Override
    public int[] rowLevels() {
        return levelField.rowLevels();
    }

    @Override
    public void recordRowLevels() {
        levelField.recordRowLevels();
    }

    /**
     * A struct batch: its rows, its null rows ({@code null}: none) and each
     * field's batch.
     */
    public static final class Batch {
        private final int rows;
        private final boolean[] nulls;
        private final Object[] fields;

        Batch(int rows, boolean[] nulls, Object[] fields) {
            this.rows = rows;
            this.nulls = nulls;
            this.fields = fields;
        }

        public int rows() {
            return rows;
        }

        /** The struct's null rows, or {@code null} when none is. */
        public boolean[] nulls() {
            return nulls;
        }

        /** Field {@code i}'s batch. */
        public Object field(int i) {
            return fields[i];
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        for (ColumnReader f : fields) {
            f.startRowGroup(store, rows);
        }
    }

    @Override
    public Object readBatch(int n) {
        return read(n, null);
    }

    /**
     * {@code n} rows of every field; {@code selection} null: all of them, else
     * the set rows (others unread).
     */
    private Object read(int n, MemorySegment selection) {
        Object[] kids = new Object[fields.length];
        try {
            for (int i = 0; i < fields.length; i++) {
                kids[i] = selection == null ? fields[i].readBatch(n) : fields[i].readBatchSelected(n, selection);
            }
        } catch (RuntimeException | Error e) {
            releaseFields(kids);
            throw e;
        }
        // Read after every field: the level source may be under a nested field, and its levels stay valid until
        // its next batch. Under a selection the unselected rows' levels are stale: those rows are excluded.
        boolean[] nulls = null;
        if (levelSource != null) {
            int[] levels = levelSource.rowLevels();
            int d = defLevel;
            for (int r = 0; r < n; r++) {
                if (levels[r] < d) {
                    if (nulls == null) {
                        nulls = new boolean[n];
                    }
                    nulls[r] = true;
                }
            }
        }
        return new Batch(n, nulls, kids);
    }

    /**
     * #568: each field decodes only the rows set in {@code selection} (a flat
     * string leaf skips the others in its pages; a nested field decodes whole);
     * the batch's selection excludes the other rows.
     */
    @Override
    public Object readBatchSelected(int n, MemorySegment selection) {
        return read(n, selection);
    }

    /**
     * Whether some field decodes its selected rows faster than the whole batch
     * (a string leaf).
     */
    @Override
    public boolean decodesSelectedFaster() {
        for (ColumnReader f : fields) {
            if (f.decodesSelectedFaster()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void skipRows(int n) {
        for (ColumnReader f : fields) {
            f.skipRows(n);
        }
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            releaseFields(((Batch) batch).fields);
        }
    }

    private void releaseFields(Object[] kids) {
        for (int i = 0; i < kids.length; i++) {
            if (kids[i] != null) {
                fields[i].release(kids[i]);
            }
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        for (ColumnReader f : fields) {
            f.setPoolLimit(limit);
        }
    }

    @Override
    public void setReuseDecompress(boolean reuse) {
        for (ColumnReader f : fields) {
            f.setReuseDecompress(reuse);
        }
    }

    @Override
    public void setRebase(CalendarRebase rebase) {
        for (ColumnReader f : fields) {
            f.setRebase(rebase);
        }
    }

    /**
     * #568: a struct's flat string fields are emitted as dictionary ids too
     * (#612), the way a top-level string column is, where the leaf allows it. A
     * list or map field keeps its own choice (off).
     */
    @Override
    public void setDictionaryIds(boolean enabled) {
        for (ColumnReader f : fields) {
            f.setDictionaryIds(enabled);
        }
    }

    @Override
    public int rowsRemaining() {
        return ((ColumnReader) levelField).rowsRemaining();
    }

    @Override
    public void close() {
        for (ColumnReader f : fields) {
            f.close();
        }
    }
}
