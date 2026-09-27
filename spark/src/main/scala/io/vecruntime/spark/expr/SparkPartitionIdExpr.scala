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
package io.vecruntime.spark.expr

import io.vecruntime.kernels.{ArrowLayout, SegmentVectorBuffers, SequenceKernels, VecType, VectorBuffers}
import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * `spark_partition_id()`: the task's partition index as a non-null INT32 column, read from the task
 * context like Spark's `SparkPartitionID` (its `initializeInternal` reads the same partition index).
 * The value is the same for every row of the task, so it is read once per task and the column is one
 * Vector API broadcast fill per batch; nothing is allocated per row.
 */
final case class SparkPartitionIdExpr() extends VectorExpr {
  override def dataType: DataType = IntegerType
  override def children: Seq[VectorExpr] = Nil

  // Not transient on purpose, as in MonotonicIdExpr: -1 travels to every task and marks "not yet read".
  private var partition: Int = -1

  override def eval(ctx: EvalContext): VectorBuffers = {
    if (partition < 0) partition = Option(TaskContext.get()).map(_.partitionId()).getOrElse(0)
    val n = ctx.numRows
    val data = ArrowLayout.allocateData(ctx.arena, VecType.INT32, n)
    SequenceKernels.fillInt(data, n, partition)
    SegmentVectorBuffers.fixedWidth(VecType.INT32, n, null, data)
  }
}
