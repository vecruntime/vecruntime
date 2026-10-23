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
package org.apache.spark.sql.execution.vector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.StructType

/**
 * #901 item 6: a bridge to Spark's `private[sql]` `RowToColumnConverter`, so the native Iceberg scan
 * ([[org.apache.spark.sql.vecruntime.VectorIcebergScanExec]]) can turn the `InternalRow`s of a task group it
 * reads with Iceberg's own ROW reader into columnar vectors -- all Spark types, nested included -- without
 * re-implementing the conversion. One instance per schema, reused across batches; `convert` writes one row into
 * the given writable vectors (as `RowToColumnarExec` does).
 */
final class RowToColumnarAccess(schema: StructType) {
  private val converter = new RowToColumnConverter(schema)

  def convert(row: InternalRow, vectors: Array[WritableColumnVector]): Unit = converter.convert(row, vectors)
}
