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
package org.apache.spark.sql.vecruntime

import io.vecruntime.spark.arrow.BuildPayloadColumn
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * The output column of an object aggregate whose value has no kernel lane (#57): `collect_list` /
 * `collect_set` emit an `array<T>` (`GenericArrayData` per group), `bloom_filter_agg` a `binary` (the
 * serialized filter, or `null` when no bit was set). Held in Spark's own on-heap column vector -- the
 * same store the broadcast join's payload columns use ([[io.vecruntime.spark.arrow.BuildPayloadColumn]])
 * -- so Spark's `ColumnarToRowExec` and any Arrow consumer read the declared type unchanged. Each
 * boxed value (Spark's internal representation, or `null`) is appended through a one-cell
 * `InternalRow`, which reuses the store's nested-append logic for arrays, decimals and binary.
 */
object ObjectAggColumns {

  /** A column of `count` values, `get(o)` the boxed value of row `o` (Spark's representation, or null). */
  def column(dt: DataType, count: Int, get: Int => Any): ColumnVector = {
    val store = new BuildPayloadColumn(dt, math.max(count, 1))
    val row = new GenericInternalRow(1)
    var o = 0
    while (o < count) {
      row.update(0, get(o))
      store.add(row, 0)
      o += 1
    }
    store.plain()
  }
}
