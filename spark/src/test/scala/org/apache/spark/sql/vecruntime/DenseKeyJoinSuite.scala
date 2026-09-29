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

import io.vecruntime.spark.expr.ColumnRef
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.funsuite.AnyFunSuite

/**
 * The dense-key probe of a hash join (#546): a single INT32/INT64 key whose build values span a small
 * range is indexed by value. Built on the same build side, the dense and the hash probe must return
 * the same rows for every join type; the dense form must be chosen only when the range rule allows.
 */
class DenseKeyJoinSuite extends AnyFunSuite {

  private def metrics =
    new VectorMetrics(new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"))

  private def spec(joinType: org.apache.spark.sql.catalyst.plans.JoinType, wide: Boolean): JoinSpec = {
    val kt: DataType = if (wide) LongType else IntegerType
    val out: Array[(String, DataType)] =
      if (joinType == LeftSemi || joinType == LeftAnti) Array("k" -> kt)
      else Array("k" -> kt, "bk" -> kt, "v" -> IntegerType)
    JoinSpec(
      joinType = joinType,
      buildIsLeft = false,
      buildKeys = Array(ColumnRef(0, kt)),
      streamedKeys = Array(ColumnRef(0, kt)),
      condition = None,
      outputAttrs = out,
      joinedAttrs = Array("k" -> kt, "bk" -> kt, "v" -> IntegerType),
      buildTypes = Array(kt, IntegerType),
      streamedWidth = 1
    )
  }

  private def batches(schema: StructType, rows: Seq[InternalRow], batchRows: Int): Iterator[ColumnarBatch] =
    rows.grouped(batchRows).map(g => VectorRowStages.toBatch(schema, g.toArray))

  /** Build keys `base + (i * 3) % span` (some null), each with a payload; nulls every 11th row. */
  private def buildRows(wide: Boolean, base: Long, span: Int, n: Int): Seq[InternalRow] =
    (0 until n).map { i =>
      val k: Any = if (i % 11 == 0) null else if (wide) base + (i * 3L) % span else (base + (i * 3L) % span).toInt
      InternalRow(k, i % 7)
    }

  /** Probe keys around the build range, beyond both ends, and null every 13th row. */
  private def probeRows(wide: Boolean, base: Long, span: Int, n: Int): Seq[InternalRow] =
    (0 until n).map { i =>
      val v = base - 20 + (i * 7L) % (span + 40)
      InternalRow(if (i % 13 == 0) null else if (wide) v else v.toInt)
    }

  /** Every output row as a string, sorted: the two probes must agree row for row. */
  private def run(s: JoinSpec, build: Seq[InternalRow], probe: Seq[InternalRow], wide: Boolean): Seq[String] = {
    val kt: DataType = if (wide) LongType else IntegerType
    val table = BuildTable.fromBatches(batches(new StructType().add("bk", kt).add("v", IntegerType), build, 1000), s)
    if (s.denseKeys) assert(table.dense != null, "a small-range key must be indexed densely")
    else assert(table.dense == null)
    val it = new VectorHashJoinIterator(
      batches(new StructType().add("k", kt), probe, 700),
      table,
      s,
      metrics,
      closeOnTaskEnd = false
    )
    try {
      val out = Seq.newBuilder[String]
      while (it.hasNext) {
        val b = it.next()
        var r = 0
        while (r < b.numRows()) {
          out += (0 until b.numCols()).map { c =>
            val col = b.column(c)
            if (col.isNullAt(r)) "null"
            else col.dataType() match {
              case LongType => col.getLong(r).toString
              case _ => col.getInt(r).toString
            }
          }.mkString(",")
          r += 1
        }
      }
      out.result().sorted
    } finally it.close() // closes the table too
  }

  for (wide <- Seq(false, true); jt <- Seq(Inner, LeftOuter, LeftSemi, LeftAnti)) {
    test(s"dense and hash probes agree: ${if (wide) "INT64" else "INT32"} key, $jt") {
      val base = if (wide) (1L << 40) - 500 else -500L
      val build = buildRows(wide, base, 1200, 3000)
      val probe = probeRows(wide, base, 1200, 9000)
      val s = spec(jt, wide)
      val dense = run(s, build, probe, wide)
      val hashed = run(s.copy(denseKeys = false), build, probe, wide)
      assert(dense.nonEmpty)
      assert(dense === hashed)
    }
  }

  test("a key range past 10x the distinct keys keeps the hash table") {
    val rows = (0 until 100).map(i => InternalRow(i.toLong * 1000, i)) // 100 keys over a range of 99,001
    val table = BuildTable.fromBatches(
      batches(new StructType().add("bk", LongType).add("v", IntegerType), rows, 64),
      spec(Inner, wide = true)
    )
    try assert(table.dense == null)
    finally table.close()
  }
}
