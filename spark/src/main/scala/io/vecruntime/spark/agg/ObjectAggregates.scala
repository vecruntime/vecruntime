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
package io.vecruntime.spark.agg

import io.vecruntime.kernels.{Bitmap, GroupAssignment}
import io.vecruntime.spark.expr.EvalContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * `ObjectHashAggregateExec` functions whose aggregation buffer is an object rather than a mutable
 * `UnsafeRow` -- `bloom_filter_agg`, `collect_list`, `collect_set` (#57). Spark carries their state
 * between the Partial and Final stages as a single `BinaryType` column (`serialize` / `deserialize`),
 * exactly as its own `ObjectHashAggregateExec` does, so a partial buffer we emit is byte-identical to
 * Spark's and a Spark Final (or the runtime bloom-filter probe) reads it unchanged, and a merging
 * stage reads Spark's partial buffers unchanged.
 *
 * We do not re-implement the functions: we drive Spark's own `TypedImperativeAggregate` object per
 * group. The update modes evaluate the function's child through `AggFunction.update(buffer, row)`
 * over an `InternalRow` view of the batch (`ColumnarBatch.getRow`, physical rows, the selection
 * skipping unselected ones); the merge modes `deserialize` the input binary column and
 * `merge(buffer, other)`. A buffer-emitting stage emits `serialize(buffer)` in its one binary slot;
 * a result stage emits `eval(buffer)` (a serialized filter or null for bloom, an `ArrayData` for the
 * collects). This matches Spark's cost profile -- these aggregates are row-object based in Spark too
 * -- and its result to the byte.
 *
 * `function` is the Catalyst function with its child(ren) already bound to the operator's input
 * attributes by ordinal, so `child.eval(row)` reads the right columns; it is `Serializable` and
 * travels in the operator. `bufferOrdinal` is the input column holding the partial buffer in a merge
 * stage, `-1` otherwise. `finalResult` selects `eval` over `serialize` for the emitted value.
 */
final case class SparkObjectAgg(
    function: TypedImperativeAggregate[AnyRef],
    merge: Boolean,
    bufferOrdinal: Int,
    finalResult: Boolean
) extends VectorAggFunction {

  // A TypedImperativeAggregate exchanges its state as one BinaryType buffer column.
  override def bufferTypes: Seq[DataType] = Seq(BinaryType)

  /**
   * A result stage emits the function's own result type (collect's `array<T>`, bloom's `binary`) in
   * the one buffer slot; a buffer stage emits the serialized state (`binary`).
   */
  override def emittedTypes(declared: Seq[DataType]): Seq[DataType] =
    if (finalResult) Seq(function.dataType) else declared

  /** A buffer-emitting stage emits the serialized state; a result stage the function's own eval. */
  private def emit(buffer: AnyRef): Any =
    if (finalResult) function.eval(buffer) else function.serialize(buffer)

  /** Merges one input buffer column value at row `i` (a serialized partial state) into `buffer`. */
  private def mergeRow(buffer: AnyRef, ctx: EvalContext, i: Int): AnyRef = {
    val col = ctx.column(bufferOrdinal)
    if (col.isNullAt(i)) buffer // Spark emits no partial buffer for a group with no non-null input
    else function.merge(buffer, function.deserialize(SparkObjectAgg.bytesOf(col, i)))
  }

  override def newState(): AggState = new AggState {
    private var buffer: AnyRef = function.createAggregationBuffer()
    override def update(ctx: EvalContext): Unit = {
      val n = ctx.numRows
      val sel = ctx.selection
      var i = 0
      while (i < n) {
        if (sel == null || Bitmap.isSet(sel, i)) {
          buffer = if (merge) mergeRow(buffer, ctx, i) else function.update(buffer, ctx.batch.getRow(i))
        }
        i += 1
      }
    }
    override def bufferValues: Array[Any] = Array(emit(buffer))
  }

  override def newGroupedState(): GroupedAggState = new GroupedAggState {
    private var buffers = new Array[AnyRef](0)
    // A running heap estimate per group (#57), so the operator's budget sees the object buffers and
    // spills instead of pinning memory: a bloom filter's bit array is a fixed size known once it
    // exists; a collect buffer grows by an estimated element width per appended row.
    private var bytes = new Array[Long](0)
    private def ensure(g: Int): Unit = if (g >= buffers.length) {
      val next = math.max(g + 1, buffers.length * 2)
      val grown = java.util.Arrays.copyOf(buffers, next)
      var k = buffers.length
      while (k < next) { grown(k) = function.createAggregationBuffer(); k += 1 }
      buffers = grown
      bytes = java.util.Arrays.copyOf(bytes, next)
    }
    override def update(ctx: EvalContext, groups: GroupAssignment): Unit = {
      val ids = groups.ids()
      val n = ctx.numRows
      val sel = ctx.selection
      var i = 0
      while (i < n) {
        if (sel == null || Bitmap.isSet(sel, i)) {
          val g = ids(i)
          if (g >= 0) {
            ensure(g)
            if (merge) buffers(g) = mergeRow(buffers(g), ctx, i)
            else buffers(g) = function.update(buffers(g), ctx.batch.getRow(i))
            bytes(g) = SparkObjectAgg.estimateBytes(function, buffers(g), bytes(g), grew = !merge)
          }
        }
        i += 1
      }
    }
    override def bufferValue(g: Int, slot: Int): Any = { ensure(g); emit(buffers(g)) }
    // Spilling always emits the mergeable serialized buffer, even in a result stage (finalResult).
    override def spillValue(g: Int, slot: Int): Any = { ensure(g); function.serialize(buffers(g)) }
    override def groupBytes(g: Int): Long = if (g < bytes.length) bytes(g) else 0L
  }
}

object SparkObjectAgg {

  /**
   * The serialized buffer bytes at row `i`, whether the column is a real `BinaryType` (the child's
   * partial buffer, from the exchange) or a `StringType` carrier (a spilled buffer, #57): a spilled
   * object-agg buffer travels as UTF8-carried bytes to reuse the existing UTF8 spill path, and Spark's
   * varchar accessor gives them back unchanged. `getBinary` works for the binary column; the varchar
   * accessor does not implement it, so we fall back to the UTF8 bytes there.
   */
  def bytesOf(col: org.apache.spark.sql.vectorized.ColumnVector, i: Int): Array[Byte] =
    try col.getBinary(i)
    catch { case _: UnsupportedOperationException => col.getUTF8String(i).getBytes }

  /** Whether `f` is one of the object aggregates this path carries. */
  def carries(f: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction): Boolean = f match {
    case _: org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate => true
    case _: org.apache.spark.sql.catalyst.expressions.aggregate.CollectList => true
    case _: org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet => true
    case _ => false
  }

  /**
   * A cheap per-group heap estimate for the memory budget (#57), without serializing every group on
   * every budget check. A bloom filter's footprint is its bit array, a fixed size read from
   * `bitSize()`; a collect buffer grows by an estimated element width per appended row (`grew`),
   * added to the previous estimate. Deliberately an upper-ish bound: over-counting spills a little
   * early, which is safe; under-counting an object buffer is what OOMs an executor.
   */
  def estimateBytes(
      function: TypedImperativeAggregate[AnyRef],
      buffer: AnyRef,
      previous: Long,
      grew: Boolean
  ): Long = buffer match {
    case bf: org.apache.spark.util.sketch.BloomFilter =>
      // The serialized size BloomFilterAggregate.serialize would write: bitSize/8 + 8, plus object overhead.
      (bf.bitSize() / 8L) + 32L
    case _ =>
      // A collect_list/collect_set buffer: previous estimate plus one element on an append (merge
      // recomputes from the merged buffer's own updates on the other side, so only update grows here).
      val base = if (previous == 0L) 48L else previous // ArrayBuffer/HashSet object overhead
      if (grew) base + collectElementWidth(function) else base
  }

  /** A rough per-element width for a collect buffer, by the collected element type. */
  private def collectElementWidth(function: TypedImperativeAggregate[AnyRef]): Long = {
    val dt = (function: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction) match {
      case c: org.apache.spark.sql.catalyst.expressions.aggregate.CollectList => c.child.dataType
      case c: org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet => c.child.dataType
      case _ => org.apache.spark.sql.types.LongType
    }
    dt match {
      case _: org.apache.spark.sql.types.StringType => 48L // a UTF8String plus a boxed reference
      case _: org.apache.spark.sql.types.BinaryType => 48L
      case _: org.apache.spark.sql.types.DecimalType => 40L
      case _ => 24L // a boxed primitive plus a slot
    }
  }

  /**
   * Compiles the update side (`Partial` / `Complete`): the function bound to `input` by ordinal so
   * its child reads the batch rows.
   */
  def compileUpdate(
      f: TypedImperativeAggregate[AnyRef],
      input: Seq[Attribute],
      finalResult: Boolean
  ): Either[String, VectorAggFunction] = {
    val bound = BindReferences.bindReference(f, input).asInstanceOf[TypedImperativeAggregate[AnyRef]]
    Right(SparkObjectAgg(bound, merge = false, bufferOrdinal = -1, finalResult = finalResult))
  }

  /**
   * Compiles the merge side (`PartialMerge` / `Final`): the input buffer is the function's single
   * `inputAggBufferAttribute`, found by exprId or by Spark's position when a rewrite gave the Final a
   * fresh instance.
   */
  def compileMerge(
      f: TypedImperativeAggregate[AnyRef],
      input: Seq[Attribute],
      finalResult: Boolean,
      bufferOffset: Int
  ): Either[String, VectorAggFunction] = {
    val bufferAttr = f.inputAggBufferAttributes.head
    val byId = input.indexWhere(_.exprId == bufferAttr.exprId)
    val ordinal =
      if (byId >= 0) byId
      else if (bufferOffset >= 0 && bufferOffset < input.length && input(bufferOffset).dataType == BinaryType)
        bufferOffset
      else -1
    if (ordinal < 0) Left(s"buffer ${bufferAttr.name} not found in the input")
    else Right(SparkObjectAgg(f, merge = true, bufferOrdinal = ordinal, finalResult = finalResult))
  }
}
