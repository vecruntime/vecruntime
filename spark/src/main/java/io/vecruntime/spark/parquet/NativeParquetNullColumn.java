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
import org.apache.parquet.column.page.PageReadStore;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A requested column the file does not hold (#568 schema evolution): a column
 * or struct field added since the file was written. Every row is null, as
 * Spark's reader makes it ({@code ParquetColumnVector}: {@code setMissing}).
 * A flat column is an all-null Arrow vector; a nested one is Spark's own
 * all-null vector, which the operators read as such
 * ({@code SparkColumnVectorBuffers}: {@code isAllNull}).
 */
public final class NativeParquetNullColumn implements NativeParquetColumn {

    private final String name;
    private final DataType type;
    private final BufferAllocator allocator;
    private final boolean nested;
    private int position;
    private int rows;

    public NativeParquetNullColumn(String name, DataType type, BufferAllocator allocator) {
        this.name = name;
        this.type = type;
        this.allocator = allocator;
        this.nested = type instanceof StructType || type instanceof ArrayType || type instanceof MapType;
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        this.position = 0;
        this.rows = rows;
    }

    @Override
    public Object readBatch(int n) {
        position += n;
        if (nested) {
            OnHeapColumnVector v = new OnHeapColumnVector(n, type);
            v.setMissing();
            return v;
        }
        return ArrowOutput.nulls(name, type, n, allocator);
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
        return (ColumnVector) batch;
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            ((ColumnVector) batch).close();
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        // Nothing pooled: a batch is one fresh all-null vector.
    }

    @Override
    public void setDictionaryIds(boolean enabled) {
        // No values.
    }

    @Override
    public void setRebase(ParquetRebase rebase) {
        // No values to rebase.
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
