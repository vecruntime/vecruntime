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
package org.apache.spark.sql.vecruntime.bench

import io.vecruntime.spark.expr.ColumnRef
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructType}
import org.apache.spark.sql.vecruntime.{BuildTable, JoinSpec, VectorHashJoinIterator, VectorMetrics, VectorRowStages}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The broadcast hash join's probe and output, driven from JMH (#547): a build side of `buildRows` rows
 * with an INT32 key and an INT32 payload, plus -- with `arrayPayload` -- an `array<int>` payload held
 * in the row store, joined against `batches` streamed batches of `rowsPerBatch` keys. The table is
 * built once from rows (the broadcast relation's path) as a shared table, so closing an iterator does
 * not free it; `run()` joins every streamed batch and reads every output value, returning a checksum.
 */
final class JoinPayloadBench(
    buildRows: Int,
    rowsPerBatch: Int,
    batches: Int,
    arrayPayload: Boolean,
    outer: Boolean,
    seed: Long
) {

  private val payloadType: DataType = ArrayType(IntegerType, containsNull = false)
  private val buildTypes: Array[DataType] =
    if (arrayPayload) Array(IntegerType, IntegerType, payloadType) else Array(IntegerType, IntegerType)

  private val spec: JoinSpec = {
    val joined: Array[(String, DataType)] =
      Array[(String, DataType)]("k" -> IntegerType, "bk" -> IntegerType, "v" -> IntegerType) ++
        (if (arrayPayload) Array[(String, DataType)]("arr" -> payloadType) else Array.empty[(String, DataType)])
    JoinSpec(
      joinType = if (outer) LeftOuter else Inner,
      buildIsLeft = false,
      buildKeys = Array(ColumnRef(0, IntegerType)),
      streamedKeys = Array(ColumnRef(0, IntegerType)),
      condition = None,
      outputAttrs = joined,
      joinedAttrs = joined,
      buildTypes = buildTypes,
      streamedWidth = 1,
      // The hash probe, so the output path is measured over the same probe either way.
      denseKeys = false
    )
  }

  private val rnd = new java.util.Random(seed)

  /**
   * The broadcast relation the shared table is keyed on. Held for the benchmark's life, as the
   * operator's closure holds Spark's relation for a task's: once it is collected, the table's
   * cleaner frees the table.
   */
  private val relation: Array[InternalRow] = (0 until buildRows).map { i =>
    val k = i * 7 + 1 // spread keys; half of the probe keys match
    if (arrayPayload) InternalRow(k, i, new GenericArrayData(Array[Any](i, i + 1, i + 2, i + 3)))
    else InternalRow(k, i)
  }.toArray

  private val table: BuildTable = BuildTable.sharedFromRows(relation, spec)

  private val probes: Array[ColumnarBatch] = Array.fill(batches) {
    val rows: Array[InternalRow] = Array.fill(rowsPerBatch)(InternalRow(rnd.nextInt(buildRows * 14) + 1))
    VectorRowStages.toBatch(new StructType().add("k", IntegerType), rows)
  }

  private def metrics =
    new VectorMetrics(new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"))

  /** Joins every streamed batch; the checksum reads each output value, the array's elements included. */
  def run(): Long = {
    val it = new VectorHashJoinIterator(probes.iterator, table, spec, metrics, closeOnTaskEnd = false)
    var sum = 0L
    try {
      while (it.hasNext) {
        val b = it.next()
        val n = b.numRows()
        val v = b.column(2)
        var r = 0
        while (r < n) {
          if (!v.isNullAt(r)) sum += v.getInt(r)
          r += 1
        }
        if (arrayPayload) {
          val a = b.column(3)
          r = 0
          while (r < n) {
            if (!a.isNullAt(r)) {
              val arr = a.getArray(r)
              var e = 0
              while (e < arr.numElements()) { sum += arr.getInt(e); e += 1 }
            }
            r += 1
          }
        }
        sum += n
      }
    } finally it.close()
    sum
  }

  def close(): Unit = {
    probes.foreach(_.close())
    table.release()
  }
}
