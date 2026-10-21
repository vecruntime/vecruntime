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

import io.vecruntime.kernels.VecType;
import io.vecruntime.parquet.read.LeafColumnReader;
import io.vecruntime.parquet.read.LeafReadSpec;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A Parquet leaf read for Spark (#900 phase 2): the Spark-free {@link
 * LeafColumnReader} -- every page, decode and batch path -- plus the two things
 * only Spark knows: the {@link LeafReadSpec} its {@code DataType} asks for
 * ({@link NativeParquetSupport#leafSpec}) and the Spark column a batch is
 * wrapped as. The decode loops are the base class's own; this subclass adds no
 * call on their path.
 */
public final class NativeParquetColumnReader extends LeafColumnReader implements NativeParquetColumn {

    private final DataType sparkType;

    public NativeParquetColumnReader(ColumnDescriptor column, VecType type, DataType sparkType,
            String name, int batchRows, BufferAllocator allocator) {
        this(column, NativeParquetSupport.leafSpec(column, type, sparkType),
                sparkType, name, batchRows, allocator);
    }

    public NativeParquetColumnReader(ColumnDescriptor column, LeafReadSpec spec, DataType sparkType,
            String name, int batchRows, BufferAllocator allocator) {
        super(column, spec, name, batchRows, allocator);
        this.sparkType = sparkType;
    }

    @Override
    public ColumnVector wrap(Object batch) {
        return wrap((FieldVector) batch);
    }

    /**
     * The batch vector as Spark's column: a dictionary view for an id batch
     * (#612), else the plain Arrow wrapper for the Spark type.
     */
    public ColumnVector wrap(FieldVector v) {
        org.apache.arrow.vector.VarCharVector dict = dictionaryOf(v);
        if (dict != null) {
            return io.vecruntime.spark.arrow.VectorDictionaryColumnVector.borrowed((org.apache.arrow.vector.IntVector) v, dict);
        }
        return io.vecruntime.spark.arrow.ArrowOutput.wrap(v, sparkType);
    }
}
