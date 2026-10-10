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
package io.vecruntime.spark.arrow;

import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

/**
 * Spark's {@link ArrowColumnVector} hides the wrapped {@link ValueVector}. Our
 * operators emit this subclass so downstream spark-vector operators can get
 * back to the Arrow buffers without a copy.
 *
 * <p>A column may be <em>borrowed</em>: a projection that forwards an input
 * column unchanged wraps the child's vector without taking ownership, so
 * closing the output batch leaves the child's memory alone (the child releases
 * it when it produces its next batch).
 *
 * <p>A column may be <em>reusable</em>: a producer that refills the same vector
 * for every batch (the range leaf) owns it and releases it at task end, and
 * Spark's {@code ColumnarToRowExec} must not free it after reading a batch --
 * it calls {@link #closeIfFreeable()} on every batch it has consumed, which
 * Spark's own reusable {@code WritableColumnVector}s answer with a no-op, and
 * so does a reusable column here. {@link #close()} still frees it.
 */
public final class VectorArrowColumnVector extends ArrowColumnVector {

    private final ValueVector valueVector;
    private final boolean owns;
    private final boolean reusable;

    public VectorArrowColumnVector(ValueVector vector) {
        this(vector, true, false);
    }

    public VectorArrowColumnVector(ValueVector vector, boolean owns) {
        this(vector, owns, false);
    }

    private VectorArrowColumnVector(ValueVector vector, boolean owns, boolean reusable) {
        super(vector);
        this.valueVector = vector;
        this.owns = owns;
        this.reusable = reusable;
    }

    /**
     * An owned column over a vector its producer refills for every batch:
     * survives Spark's per-batch {@link #closeIfFreeable()}, freed by {@link
     * #close()}.
     */
    public static VectorArrowColumnVector reusable(ValueVector vector) {
        return new VectorArrowColumnVector(vector, true, true);
    }

    @Override
    public ValueVector getValueVector() {
        return valueVector;
    }

    public boolean ownsMemory() {
        return owns;
    }

    /** Same vector, not owned. */
    public VectorArrowColumnVector borrow() {
        return new VectorArrowColumnVector(valueVector, false, false);
    }

    @Override
    public void closeIfFreeable() {
        if (!reusable) {
            close();
        }
    }

    @Override
    public void close() {
        if (owns) {
            super.close();
        }
    }
}
