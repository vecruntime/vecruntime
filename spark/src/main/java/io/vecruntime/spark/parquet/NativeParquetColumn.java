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

import io.vecruntime.parquet.read.ColumnReader;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * One top-level column of the native Parquet scan, for Spark: a Spark-free
 * {@link ColumnReader} (#900) -- a leaf column chunk ({@link
 * NativeParquetColumnReader}) or a nested reader over several ({@link
 * NativeParquetStructReader}, #568) -- that can also wrap its batch handle as
 * Spark's column ({@link #wrap}). The scan hands the batch back ({@link
 * #release}) when it is done.
 */
public interface NativeParquetColumn extends ColumnReader {

    /** The batch as Spark's column. */
    ColumnVector wrap(Object batch);

    /**
     * This file's calendar rebase from Spark's settings and footer keys (#568
     * item 6).
     */
    default void setRebase(ParquetRebase rebase) {
        setRebase((io.vecruntime.parquet.read.CalendarRebase) rebase);
    }
}
