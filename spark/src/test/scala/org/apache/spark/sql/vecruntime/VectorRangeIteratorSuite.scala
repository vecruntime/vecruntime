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
package org.apache.spark.sql.vecruntime

import io.vecruntime.spark.arrow.{VectorAllocators, VectorArrowColumnVector}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.scalatest.funsuite.AnyFunSuite

/**
 * The range iterator without a Spark job: one reused native vector per partition, refilled per batch,
 * read through Spark's own `ArrowColumnVector` accessor -- and surviving the `closeIfFreeable()` Spark's
 * `ColumnarToRowExec` calls on every batch it has consumed, which is what makes reuse possible at all.
 */
class VectorRangeIteratorSuite extends AnyFunSuite {

  private def metrics(): VectorMetrics =
    new VectorMetrics(new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"))

  test("batches are one reused vector, refilled and re-counted, read through Spark's accessor") {
    val m = metrics()
    val before = VectorAllocators.root().getAllocatedMemory
    val it = new VectorRangeIterator(1000L, -3L, 25L, 10, m)
    var expected = 1000L
    var batches = 0
    var lastColumn: AnyRef = null
    while (it.hasNext) {
      val batch = it.next()
      batches += 1
      assert(batch.numRows() === (if (batches < 3) 10 else 5))
      val col = batch.column(0)
      if (lastColumn != null) assert(col eq lastColumn, "the column object is reused across batches")
      lastColumn = col
      assert(!col.hasNull && col.numNulls() === 0)
      for (i <- 0 until batch.numRows()) {
        assert(!col.isNullAt(i))
        assert(col.getLong(i) === expected, s"row $i of batch $batches")
        expected -= 3
      }
      // What Spark's ColumnarToRowExec does after reading a batch: a reusable column ignores it.
      batch.closeIfFreeable()
    }
    assert(batches === 3)
    assert(m.numOutputBatches.value === 3 && m.numOutputRows.value === 25)
    assert(VectorAllocators.root().getAllocatedMemory > before, "the vector is still allocated")
    it.close()
    assert(VectorAllocators.root().getAllocatedMemory === before, "close() frees the vector and the allocator")
    it.close() // idempotent
    assert(!it.hasNext)
  }

  test("a partition shorter than a batch allocates only its rows; an exhausted iterator refuses next()") {
    val it = new VectorRangeIterator(7L, 1L, 3L, 10000, metrics())
    val batch = it.next()
    assert(batch.numRows() === 3)
    assert((0 until 3).map(batch.column(0).getLong) === Seq(7L, 8L, 9L))
    assert(!it.hasNext)
    intercept[NoSuchElementException](it.next())
    it.close()
  }

  test("a reusable column ignores closeIfFreeable and frees on close; a plain one frees on either") {
    val allocator = VectorAllocators.newChild("reusable-test")
    val plain = new org.apache.arrow.vector.BigIntVector("p", allocator)
    plain.allocateNew(4)
    plain.setValueCount(4)
    val reused = new org.apache.arrow.vector.BigIntVector("r", allocator)
    reused.allocateNew(4)
    reused.setValueCount(4)
    val allocated = allocator.getAllocatedMemory
    new VectorArrowColumnVector(plain).closeIfFreeable()
    assert(allocator.getAllocatedMemory < allocated, "a plain owned column is freed by closeIfFreeable")
    val column = VectorArrowColumnVector.reusable(reused)
    val afterPlain = allocator.getAllocatedMemory
    column.closeIfFreeable()
    assert(allocator.getAllocatedMemory === afterPlain, "a reusable column survives closeIfFreeable")
    column.close()
    assert(allocator.getAllocatedMemory === 0L)
    allocator.close()
  }
}
