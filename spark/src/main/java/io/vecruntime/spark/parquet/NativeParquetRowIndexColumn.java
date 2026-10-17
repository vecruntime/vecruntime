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
package io.vecruntime.spark.parquet;

import java.lang.foreign.MemorySegment;

import io.vecruntime.spark.arrow.ArrowOutput;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * The row index of each row in its file (`_metadata.row_index`, #568 work item
 * 9): the row group's first row index, which the footer records, plus the row's
 * position in the group. The scan reads whole row groups (column-index page
 * filtering is off), so positions within a group are contiguous; a skip moves
 * the position as it moves every other column.
 */
public final class NativeParquetRowIndexColumn implements NativeParquetColumn {

    private final String name;
    private final BufferAllocator allocator;
    private long base;
    private int position;
    private int rows;

    public NativeParquetRowIndexColumn(String name, BufferAllocator allocator) {
        this.name = name;
        this.allocator = allocator;
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        this.base = store.getRowIndexOffset().orElseThrow(() -> new IllegalStateException("the row group carries no row index offset"));
        this.position = 0;
        this.rows = rows;
        // #568: a row group filtered by the column index emits only its selected rows; their indexes in the group.
        this.selected = null;
        java.util.Optional<java.util.PrimitiveIterator.OfLong> indexes = store.getRowIndexes();
        if (indexes.isPresent()) {
            long[] s = new long[rows];
            java.util.PrimitiveIterator.OfLong it = indexes.get();
            for (int i = 0;
                 i < rows && it.hasNext();
                 i++) {
                s[i] = it.nextLong();
            }
            this.selected = s;
        }
    }

    private long[] selected;

    @Override
    public Object readBatch(int n) {
        BigIntVector v = new BigIntVector(name, allocator);
        try {
            v.allocateNew(n);
            long first = base + position;
            for (int i = 0; i < n; i++) {
                v.set(i,
                        selected != null ? base + selected[position + i] : first + i);
            }
            v.setValueCount(n);
        } catch (RuntimeException | Error e) {
            v.close();
            throw e;
        }
        position += n;
        return v;
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
        position += n;
    }

    @Override
    public ColumnVector wrap(Object batch) {
        return ArrowOutput.wrap((BigIntVector) batch, DataTypes.LongType);
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            ((BigIntVector) batch).close();
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        // Nothing pooled: a batch is one fill of a fresh vector.
    }

    @Override
    public void setReuseDecompress(boolean reuse) {
        // Row indexes are not read from pages.
    }

    @Override
    public void setRebase(ParquetRebase rebase) {
        // Row indexes are not dates or timestamps.
    }

    @Override
    public void setDictionaryIds(boolean enabled) {
        // Not dictionary encoded.
    }

    @Override
    public int rowsRemaining() {
        return rows - position;
    }

    @Override
    public void close() {
        // Nothing held between batches.
    }
}
