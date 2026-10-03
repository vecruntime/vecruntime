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
package io.vecruntime.spark.arrow;

import java.lang.foreign.Arena;

import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.spark.adapter.ColumnVectorAdapters;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A join's probe-side output column, not yet gathered (#603): rows {@code
 * idx[i]} of {@code base}, gathered on first read into an Arrow vector and kept
 * until the batch closes. Velox wraps the probe columns of a hash join's output
 * the same way (a dictionary over the matched row ids) instead of copying them.
 *
 * <p>When the next join of a chain probes this column, it composes the row ids
 * ({@link #compose}) instead of gathering: a column carried through a chain of
 * joins is gathered once, by whichever operator reads it, or never. The base
 * column belongs to the batch the producing join read; it stays valid because a
 * join closes its last output before it reads its next input (Spark's columnar
 * contract: a batch is valid until the next {@code next()}), so a view never
 * outlives the input it points into. An id of {@code -1} is a null row (an
 * outer join's padding).
 */
public final class DeferredGatherColumnVector extends ColumnVector {

    /**
     * Where the rows come from: gathers rows {@code idx} (-1 a null row) into a
     * new column. A probe column's source is its input batch; a build column's
     * is the join's build table, whose own gathers (heap mirrors, #565) it
     * keeps using.
     */
    @FunctionalInterface
    public interface Source {
        ColumnVector gather(String name, DataType dt, int[] idx,
                            BufferAllocator allocator);
    }

    private final String name;
    private final Source source;
    private final int[] idx;
    private final BufferAllocator allocator;
    private ColumnVector gathered;

    private DeferredGatherColumnVector(String name, DataType dt, Source source,
            int[] idx, BufferAllocator allocator) {
        super(dt);
        this.name = name;
        this.source = source;
        this.idx = idx;
        this.allocator = allocator;
    }

    /**
     * A column of a batch of {@code numRows} rows as a source, adapted to
     * buffers when gathered.
     */
    private static Source ofColumn(ColumnVector column, int numRows) {
        return (name, dt, ids, allocator) -> {
            try (Arena arena = Arena.ofConfined()) {
                VectorBuffers in = ColumnVectorAdapters.adapt(column, numRows, arena);
                return ArrowOutput.gather(name, dt, in, ids, 0, ids.length,
                        allocator);
            }
        };
    }

    /**
     * Rows {@code ids[from..to)} of {@code source} (#603 step 2: a build column
     * through the build row ids). The source must outlive every batch the view
     * is in; a join's build table does.
     */
    public static DeferredGatherColumnVector over(
            String name,
            DataType dt,
            Source source,
            int[] ids,
            int from,
            int to,
            BufferAllocator allocator) {
        int[] rows = new int[to - from];
        System.arraycopy(ids, from, rows, 0, to - from);
        return new DeferredGatherColumnVector(name, dt, source, rows, allocator);
    }

    /**
     * Rows {@code probe[from..to)} of {@code column} (a batch of {@code
     * numRows} rows). Over another deferred column the ids compose, so no
     * gather happens here at all.
     */
    public static DeferredGatherColumnVector of(
            String name,
            DataType dt,
            ColumnVector column,
            int numRows,
            int[] probe,
            int from,
            int to,
            BufferAllocator allocator) {
        if (column instanceof BorrowedColumnVector b && b.inner() instanceof DeferredGatherColumnVector) {
            column = b.inner(); // a projection forwarded the view: compose through it
        }
        if (column instanceof DeferredGatherColumnVector d && d.gathered == null) {
            return d.compose(name, probe, from, to, allocator);
        }
        return over(name, dt, ofColumn(column, numRows), probe, from,
                to, allocator);
    }

    private DeferredGatherColumnVector compose(String outName, int[] probe, int from,
            int to, BufferAllocator outAllocator) {
        int n = to - from;
        int[] rows = new int[n];
        for (int i = 0; i < n; i++) {
            int p = probe[from + i];
            rows[i] = p < 0 ? -1 : idx[p];
        }
        return new DeferredGatherColumnVector(outName, dataType(), source, rows, outAllocator);
    }

    /** True once the column was read and gathered. */
    public boolean isGathered() {
        return gathered != null;
    }

    /** The gathered column, gathering it on first use. */
    public ColumnVector gathered() {
        if (gathered == null) {
            gathered = source.gather(name, dataType(), idx, allocator);
            GATHERED.increment();
        }
        return gathered;
    }

    /**
     * Deferred columns created, and how many of them were gathered (the rest
     * never were). Read by tests.
     */
    public static final java.util.concurrent.atomic.LongAdder CREATED = new java.util.concurrent.atomic.LongAdder();

    public static final java.util.concurrent.atomic.LongAdder GATHERED = new java.util.concurrent.atomic.LongAdder();

    {
        CREATED.increment();
    }

    @Override
    public void close() {
        if (gathered != null) {
            gathered.close();
            gathered = null;
        }
    }

    @Override
    public boolean hasNull() {
        return gathered().hasNull();
    }

    @Override
    public int numNulls() {
        return gathered().numNulls();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return gathered().isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return gathered().getBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return gathered().getByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return gathered().getShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return gathered().getInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return gathered().getLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return gathered().getFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return gathered().getDouble(rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        return gathered().getArray(rowId);
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        return gathered().getMap(ordinal);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        return gathered().getDecimal(rowId, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        return gathered().getUTF8String(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        return gathered().getBinary(rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        return gathered().getChild(ordinal);
    }
}
