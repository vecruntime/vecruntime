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

import io.vecruntime.kernels.{Bitmap, SequenceKernels}
import io.vecruntime.spark.arrow.{ArrowOutput, ArrowVectorBuffers, VectorAllocators, VectorArrowColumnVector}
import org.apache.arrow.vector.BigIntVector
import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.plans.physical.{
  Partitioning,
  RangePartitioning,
  SinglePartition,
  UnknownPartitioning
}
import org.apache.spark.sql.execution.{LeafExecNode, RangeExec, SparkPlan}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Columnar replacement for Spark's `RangeExec` -- the leaf behind `spark.range(...)` and the
 * `range()` table-valued function. It writes the `id` column straight into Arrow-layout INT64
 * batches, so the chain above `range()` is ours from the leaf. Over Spark's row `RangeExec` nothing of
 * ours converted until the first exchange -- every operator refuses a row child, so the filter, projection
 * and partial aggregate over `range()` stayed Spark's and only a merging aggregate or a join above the
 * shuffle was ours, behind a `RowToColumnarExec`.
 *
 * Spark's semantics are kept exactly, because they are visible: the same rows in the same partitions
 * (`spark_partition_id()`, `monotonically_increasing_id()` and the partition-dependent shapes below
 * depend on the split), the same `outputOrdering` (a sort on `id` above a range is elided by the
 * planner) and the same `outputPartitioning` (`RangePartitioning(id, numSlices)`, or
 * `SinglePartition` for one slice, which `EnsureRequirements` already relied on to omit exchanges
 * before this operator replaced Spark's). Partition `i` holds the elements `[i * n / slices, (i + 1) *
 * n / slices)` of the `n = numElements` values `start + k * step`, with the partition bounds clamped
 * to `Long` exactly as Spark's `RangeExec` clamps them (`getSafeMargin`) and the row count derived from
 * the clamped bounds as its generated `initRange` derives it; no value can leave `[start, end)`, so the
 * kernel's wrapping arithmetic never wraps. A range past `Long.MaxValue` rows (`range(Long.MinValue,
 * Long.MaxValue)`, which neither engine could ever finish) is capped at `Long.MaxValue` rows per
 * partition.
 *
 * Memory: one native `BigIntVector` of `batchRows` values per task, allocated from the task's Arrow
 * allocator, filled by `SequenceKernels.range` and re-emitted for every batch -- the columnar batch
 * contract lets the producer reuse a batch once the consumer asks for the next one, and Spark's own
 * `RowToColumnarExec` reuses its vectors the same way. The column is a *reusable*
 * `VectorArrowColumnVector`: Spark 4.1's `ColumnarToRowExec` calls `closeIfFreeable()` on every batch
 * it has consumed (a no-op for Spark's reusable `WritableColumnVector`s, a release for any other
 * column), and a reusable column answers it the way Spark's do. Nothing is allocated per row or per
 * batch; the vector and the allocator are released from the task-completion listener. Batches hold
 * `spark.sql.inMemoryColumnarStorage.batchSize` rows (`conf.columnBatchSize`), the size the
 * `RowToColumnarExec` this operator replaces would have produced.
 *
 * Metrics: `numOutputRows` and `numOutputBatches`, `time` for the fill, and the task's
 * `inputMetrics.recordsRead` incremented per batch as Spark's range increments it per row.
 */
case class VectorRangeExec(range: Range, numSlices: Int) extends LeafExecNode with VectorPlan {

  val start: Long = range.start
  val end: Long = range.end
  val step: Long = range.step
  val numElements: BigInt = range.numElements
  val isEmptyRange: Boolean = start == end || (start < end ^ 0 < step)

  override val output: Seq[Attribute] = range.output

  override def outputOrdering: Seq[SortOrder] = range.outputOrdering

  /** Exactly what Spark's `RangeExec` reports; the plan above was built on it. */
  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) SinglePartition else RangePartitioning(outputOrdering, numSlices)
    } else {
      UnknownPartitioning(0)
    }
  }

  override def doCanonicalize(): SparkPlan =
    VectorRangeExec(range.canonicalized.asInstanceOf[Range], numSlices)

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (isEmptyRange) {
      sparkContext.emptyRDD[ColumnarBatch]
    } else {
      val (rangeStart, rangeStep, elements, slices) = (start, step, numElements, numSlices)
      val batchRows = conf.columnBatchSize
      val m = vectorMetrics
      sparkContext.parallelize(0 until slices, slices).mapPartitionsWithIndexInternal { (i, _) =>
        val (partitionStart, rows) = VectorRangeExec.partition(i, slices, elements, rangeStart, rangeStep)
        if (rows == 0L) Iterator.empty
        else new VectorRangeIterator(partitionStart, rangeStep, rows, batchRows, m)
      }
    }
  }

  override def simpleString(maxFields: Int): String =
    s"VectorRange ($start, $end, step=$step, splits=$numSlices)"
}

object VectorRangeExec {

  /**
   * The first value and the row count of partition `i`, as Spark's `RangeExec` computes them: the
   * partition covers elements `[i * n / slices, (i + 1) * n / slices)`, both bounds clamped to `Long`
   * (`getSafeMargin`), and the count is the values from the clamped start that stay short of the
   * clamped end in the step's direction (the generated `initRange`: `ceil((end - start) / step)`, 0
   * when the difference points the other way).
   */
  def partition(i: Int, slices: Int, numElements: BigInt, start: Long, step: Long): (Long, Long) = {
    def safe(bi: BigInt): Long =
      if (bi.isValidLong) bi.toLong else if (bi > 0) Long.MaxValue else Long.MinValue
    val partitionStart = safe((BigInt(i) * numElements) / slices * step + start)
    val partitionEnd = safe((BigInt(i + 1) * numElements) / slices * step + start)
    val startToEnd = BigInt(partitionEnd) - BigInt(partitionStart)
    // BigInt division truncates toward zero, and the remainder carries the dividend's sign -- the
    // same BigInteger operations Spark's generated code performs.
    var rows = startToEnd / step
    if (rows < 0) rows = 0 else if (startToEnd % step != 0) rows += 1
    (partitionStart, if (rows.isValidLong) rows.toLong else Long.MaxValue)
  }
}

/**
 * The batches of one partition of a range: `rows` values from `start` by `step`, `batchRows` per
 * batch, written into one reused native INT64 vector (see [[VectorRangeExec]]).
 */
private[vecruntime] final class VectorRangeIterator(
    start: Long,
    step: Long,
    rows: Long,
    batchRows: Int,
    metrics: VectorMetrics
) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  private val allocator = VectorAllocators.newChild("VectorRangeExec")
  private val capacity: Int = if (rows < batchRows) rows.toInt else batchRows
  private val buffers: ArrowVectorBuffers = ArrowOutput.allocateFixed("id", LongType, capacity, allocator)
  private val vector = buffers.vector().asInstanceOf[BigIntVector]
  // Every row is valid, for every batch: the validity bits are written once.
  Bitmap.fill(buffers.validity(), capacity, true)
  // Reusable: Spark's ColumnarToRowExec calls closeIfFreeable() on every batch it has read, which
  // would free the vector before its next refill; this column ignores that call and is freed by close().
  private val batch = new ColumnarBatch(Array[ColumnVector](VectorArrowColumnVector.reusable(vector)), 0)

  private val taskContext = TaskContext.get()
  private val inputMetrics: InputMetrics =
    if (taskContext != null) taskContext.taskMetrics().inputMetrics else null

  private var cursor: Long = start
  private var remaining: Long = rows
  private var closed = false

  if (taskContext != null) taskContext.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean = !closed && remaining > 0L

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    if (taskContext != null) taskContext.killTaskIfInterrupted()
    val n = if (remaining < capacity) remaining.toInt else capacity
    metrics.timed {
      cursor = SequenceKernels.range(buffers.data(), n, cursor, step)
      vector.setValueCount(n)
      batch.setNumRows(n)
    }
    remaining -= n
    metrics.numOutputBatches += 1
    metrics.numOutputRows += n
    if (inputMetrics != null) inputMetrics.incRecordsRead(n)
    batch
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      batch.close()
      allocator.close()
    }
  }
}

object VectorRangePlanner {

  /** The shapes kept on Spark's operator: a streaming range and a slice count Spark itself rejects at execution. */
  def plan(r: RangeExec): Either[String, VectorRangeExec] =
    if (r.range.isStreaming) Left("streaming range")
    else if (r.numSlices < 1) Left(s"range with ${r.numSlices} slices")
    else Right(VectorRangeExec(r.range, r.numSlices))
}
