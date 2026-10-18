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

import org.apache.parquet.column.page.PageReadStore;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.RowToColumnConverter;
import org.apache.spark.sql.execution.datasources.parquet.FieldToExtract;
import org.apache.spark.sql.execution.datasources.parquet.SparkShreddingUtils$;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.types.variant.VariantSchema;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * #812: a top-level variant column read from its file group as decoded, then
 * assembled per row by Spark's own {@code SparkShreddingUtils}, as Spark's
 * vectorized reader assembles it ({@code assembleVariantBatch} /
 * {@code assembleVariantStructBatch}):
 * <ul>
 *   <li>a shredded variant (a {@code typed_value} beside the group's
 *       {@code metadata} and {@code value}): each row's variant rebuilt from the
 *       typed columns and the residual binary, emitted as the struct of its value
 *       and metadata binaries that {@code ColumnVector.getVariant} reads;</li>
 *   <li>a variant whose extraction Spark pushed into the scan
 *       ({@code PushVariantIntoScan}): each row's struct of the extracted paths,
 *       shredded or not, with Spark's casts and error behaviour.</li>
 * </ul>
 * The inner reader decodes the group as the plain struct of its fields (the
 * struct, list and tree readers), every leaf natively. The assembly allocates
 * per row, as Spark's does.
 */
public final class NativeParquetVariantColumn implements NativeParquetColumn {

    private final NativeParquetColumn inner;
    private final VariantSchema schema;
    private final DataType outType;
    private final FieldToExtract[] fields; // null: the variant itself

    /**
     * {@code inner} decodes the file group as {@code groupType}; the column
     * emits {@code outType}: the struct of the variant's binaries ({@code
     * fields} null) or the pushed struct of extracted paths.
     */
    public NativeParquetVariantColumn(NativeParquetColumn inner, StructType groupType, DataType outType,
            boolean extract) {
        this.inner = inner;
        this.schema = SparkShreddingUtils$.MODULE$.buildVariantSchema(groupType);
        this.outType = outType;
        this.fields = extract ? SparkShreddingUtils$.MODULE$.getFieldsToExtract(outType, schema) : null;
    }

    private static final class Batch {
        final Object inner;
        final OnHeapColumnVector out;

        Batch(Object inner, OnHeapColumnVector out) {
            this.inner = inner;
            this.out = out;
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        inner.startRowGroup(store, rows);
    }

    @Override
    public Object readBatch(int n) {
        Object b = inner.readBatch(n);
        OnHeapColumnVector out = new OnHeapColumnVector(n, outType);
        try {
            ColumnVector in = inner.wrap(b);
            if (fields == null) {
                WritableColumnVector value = out.getChild(0);
                WritableColumnVector metadata = out.getChild(1);
                for (int i = 0; i < n; i++) {
                    if (in.isNullAt(i)) {
                        out.putNull(i);
                        value.putNull(i);
                        metadata.putNull(i);
                        continue;
                    }
                    VariantVal v = SparkShreddingUtils$.MODULE$.assembleVariant(in.getStruct(i), schema);
                    value.putByteArray(i, v.getValue());
                    metadata.putByteArray(i, v.getMetadata());
                }
            } else {
                StructType one = new StructType(new StructField[] {new StructField("", outType, true, Metadata.empty())});
                RowToColumnConverter converter = new RowToColumnConverter(one);
                WritableColumnVector[] vectors = new WritableColumnVector[] {out};
                GenericInternalRow row = new GenericInternalRow(1);
                for (int i = 0; i < n; i++) {
                    row.update(0, in.isNullAt(i) ? null : SparkShreddingUtils$.MODULE$.assembleVariantStruct(in.getStruct(i), schema, fields));
                    converter.convert(row, vectors);
                }
            }
        } catch (RuntimeException | Error e) {
            out.close();
            inner.release(b);
            throw e;
        }
        return new Batch(b, out);
    }

    /**
     * The whole batch: the assembly reads every row, so a selection's stale
     * rows must hold decoded values (the batch's selection excludes them).
     */
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
        inner.skipRows(n);
    }

    @Override
    public ColumnVector wrap(Object batch) {
        return ((Batch) batch).out;
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            Batch b = (Batch) batch;
            b.out.close();
            inner.release(b.inner);
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        inner.setPoolLimit(limit);
    }

    @Override
    public void setReuseDecompress(boolean reuse) {
        inner.setReuseDecompress(reuse);
    }

    @Override
    public void setDictionaryIds(boolean enabled) {
        // The assembly reads the binaries through Spark's getters: keep them plain.
        inner.setDictionaryIds(false);
    }

    @Override
    public void setRebase(ParquetRebase rebase) {
        inner.setRebase(rebase);
    }

    @Override
    public int rowsRemaining() {
        return inner.rowsRemaining();
    }

    @Override
    public void close() {
        inner.close();
    }
}
