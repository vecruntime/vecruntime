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

import io.vecruntime.spark.adapter.TypeMapping
import io.vecruntime.spark.arrow.{ArrowOutput, BorrowedColumnVector, SelectedColumnarBatch, VectorAllocators}
import io.vecruntime.spark.expr.{ColumnRef, ExpressionCompiler, LiteralExpr, VectorExpr}
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Columnar replacement for ExpandExec (`ROLLUP`, `CUBE`, `GROUPING SETS`, and the distinct-aggregate
 * rewrite). Every projection of every input batch becomes one output batch with no data copy: a
 * retained column is borrowed from the input, a nulled key is an all-invalid constant column and the
 * grouping id (or any other literal) a constant column. Projection slots therefore have to be column
 * references or literals, which is what Spark's grouping-set analysis emits after it has aliased the
 * key expressions into the project below.
 *
 * Ownership, made explicit because this operator emits several batches per input: the input batch is
 * held while its projections are emitted and the child is only asked for the next one after the last
 * projection has been handed out and released by the consumer -- so borrowed columns stay valid
 * exactly as long as Spark's producer contract keeps them. Constant columns belong to the output
 * batch and are released when the consumer asks for the next batch. A pass-through operator for the
 * selection rule: a filter below compacts rather than forwarding a selection.
 */
case class VectorExpandExec(projections: Seq[Seq[Expression]], output: Seq[Attribute], child: SparkPlan)
    extends VectorExec
    with VectorPassThrough {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  /** Compiled on the driver; the rule already checked every slot. A carried nested column's slot is null here. */
  @transient private lazy val compiled: Array[Array[VectorExpr]] = projections.map { p =>
    p.zipWithIndex.map {
      case (_, c) if !TypeMapping.hasLane(output(c).dataType) => null
      // Literals go straight to LiteralExpr: the compiler refuses NULL and boolean literals as
      // operands, but here they are whole constant columns.
      case (Literal(v, dt), _) => LiteralExpr(v, dt)
      case (e, _) =>
        ExpressionCompiler.compileMovedColumn(e, child.output) match {
          case Right(c) => c
          case Left(reason) => throw new IllegalStateException(s"cannot vectorize expand slot ${e.sql}: $reason")
        }
    }.toArray
  }.toArray

  /**
   * Per projection and column, a carried nested column's source: the child ordinal it borrows, -1 for NULL,
   * -2 for a lane column (compiled above).
   */
  @transient private lazy val nested: Array[Array[Int]] = projections.map { p =>
    p.zipWithIndex.map {
      case (_, c) if TypeMapping.hasLane(output(c).dataType) => -2
      case (a: Attribute, _) => child.output.indexWhere(_.exprId == a.exprId)
      case _ => -1
    }.toArray
  }.toArray

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val slots = compiled
    val carried = nested
    val outputAttrs = output.map(a => (a.name, a.dataType)).toArray
    val m = vectorMetrics
    child.executeColumnar().mapPartitionsInternal(iter =>
      new VectorExpandIterator(iter, slots, outputAttrs, m, carried)
    )
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  override def simpleString(maxFields: Int): String =
    s"VectorExpand(projections=${projections.length}, output=${output.map(_.name).mkString(", ")})"
}

/** One input batch in, `projections.length` output batches out, then the next input. */
private[vecruntime] class VectorExpandIterator(
    input: Iterator[ColumnarBatch],
    projections: Array[Array[VectorExpr]],
    outputAttrs: Array[(String, DataType)],
    metrics: VectorMetrics,
    carried: Array[Array[Int]]
) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  /**
   * A carried nested column of projection `p`, column `c`, or null for a lane column: the child's column
   * borrowed (through `rows`, the selected rows, when given) or an all-null column.
   */
  private def carriedColumn(p: Int, c: Int, batch: ColumnarBatch, n: Int, rows: Array[Int]): ColumnVector = {
    val source = carried(p)(c)
    if (source == -2) null
    else if (source == -1) {
      val v = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector(n, outputAttrs(c)._2)
      v.putNulls(0, n)
      v
    } else if (rows == null) BorrowedColumnVector.of(batch.column(source))
    else io.vecruntime.spark.arrow.RemappedColumnVector.of(batch.column(source), rows)
  }

  private val allocator: BufferAllocator = VectorAllocators.newChild("VectorExpandExec")
  private var current: ColumnarBatch = _ // the input batch whose projections are being emitted
  private var currentWrapped = false // `current` is a normalization wrapper we own (selection only)
  private var nextProjection = 0
  private var emitted: ColumnarBatch = _ // our last output, released when the next is requested
  private var closed = false

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def hasNext: Boolean = {
    while ((current == null || nextProjection >= projections.length) && input.hasNext) {
      // Asking the child for its next batch is what releases the previous one; every projection of
      // it has been emitted and the consumer has moved past the last of them by now.
      releaseCurrent()
      val raw = input.next()
      val batch = InputBatches.normalize(raw)
      if (batch.numRows() > 0) {
        current = batch
        currentWrapped = batch ne raw
        nextProjection = 0
        metrics.numInputBatches += 1
      } else if (batch ne raw) {
        batch.close()
      }
    }
    current != null && nextProjection < projections.length
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    releaseEmitted()
    val p = nextProjection
    val slots = projections(nextProjection)
    nextProjection += 1
    val out = metrics.timed {
      current match {
        case s: SelectedColumnarBatch =>
          // A row-id-mapped foreign batch (Iceberg merge-on-read) arrives as physical rows plus a
          // selection; our own children compact, so this is the only selected shape seen here.
          EvalContexts.withBatch(s) { ctx =>
            val n = ctx.selectedCount
            lazy val rows = VectorWriteTask2.selectedRows(s.selection(), s.numRows(), n)
            val columns = new Array[ColumnVector](slots.length)
            var c = 0
            while (c < columns.length) {
              val (name, dt) = outputAttrs(c)
              columns(c) = slots(c) match {
                case null => carriedColumn(p, c, s, n, rows)
                case ColumnRef(ordinal, _) =>
                  ArrowOutput.compact(name, dt, ctx.input(ordinal), ctx.selection, n, allocator)
                case lit: LiteralExpr if lit.value == null => ArrowOutput.nulls(name, dt, n, allocator)
                case lit: LiteralExpr => ArrowOutput.constant(name, dt, lit.value, n, allocator)
                case e => ArrowOutput.compact(name, dt, e.eval(ctx), ctx.selection, n, allocator)
              }
              c += 1
            }
            new ColumnarBatch(columns, n)
          }
        case batch if slots.exists(s => s != null && !s.isInstanceOf[ColumnRef] && !s.isInstanceOf[LiteralExpr]) =>
          // A computed slot (Spark's distinct rewrite puts a FILTER condition such as `l IS NOT NULL`
          // in the expanded row): evaluated over the batch and copied out; columns are still borrowed.
          EvalContexts.withBatch(batch) { ctx =>
            val n = batch.numRows()
            val columns = new Array[ColumnVector](slots.length)
            var c = 0
            while (c < columns.length) {
              val (name, dt) = outputAttrs(c)
              columns(c) = slots(c) match {
                case null => carriedColumn(p, c, batch, n, null)
                case ColumnRef(ordinal, _) => BorrowedColumnVector.of(batch.column(ordinal))
                case lit: LiteralExpr if lit.value == null => ArrowOutput.nulls(name, dt, n, allocator)
                case lit: LiteralExpr => ArrowOutput.constant(name, dt, lit.value, n, allocator)
                case e => ArrowOutput.copy(name, dt, e.eval(ctx), allocator)
              }
              c += 1
            }
            new ColumnarBatch(columns, n)
          }
        case batch =>
          val n = batch.numRows()
          val columns = new Array[ColumnVector](slots.length)
          var c = 0
          while (c < columns.length) {
            val (name, dt) = outputAttrs(c)
            columns(c) = slots(c) match {
              case null => carriedColumn(p, c, batch, n, null)
              case ColumnRef(ordinal, _) => BorrowedColumnVector.of(batch.column(ordinal))
              case lit: LiteralExpr if lit.value == null => ArrowOutput.nulls(name, dt, n, allocator)
              case lit: LiteralExpr => ArrowOutput.constant(name, dt, lit.value, n, allocator)
              case other => throw new IllegalStateException(s"expand slot is not a column or literal: $other")
            }
            c += 1
          }
          new ColumnarBatch(columns, n)
      }
    }
    metrics.numOutputBatches += 1
    metrics.numOutputRows += out.numRows()
    emitted = out
    out
  }

  private def releaseEmitted(): Unit = {
    if (emitted != null) { emitted.close(); emitted = null } // borrowed columns ignore close
  }

  /** Drops our reference to the input; a normalization wrapper (selection bitmap only) is ours to close. */
  private def releaseCurrent(): Unit = {
    if (current != null) {
      if (currentWrapped) current.close()
      current = null
      currentWrapped = false
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      releaseEmitted()
      releaseCurrent()
      allocator.close()
    }
  }
}

/** Planning-time checks for the expand, shared by the rule. */
object VectorExpandPlanner {

  /**
   * Every slot a literal (incl. `NULL`) of a supported type, or an expression that compiles. A struct, array
   * or map column (an UPDATE's `_partition`, #901) is carried through: each of its slots a reference to a
   * child column, borrowed, or a `NULL`.
   */
  def plan(e: ExpandExec): Either[String, VectorExpandExec] = {
    val nestedColumns = e.output.indices.filter(i => !TypeMapping.hasLane(e.output(i).dataType)).toSet
    val outputFailures = nestedColumns.toSeq.flatMap { i =>
      val a = e.output(i)
      if (!TypeMapping.carriesNested(a.dataType))
        Some(s"unsupported output type ${a.dataType.simpleString} for ${a.name}")
      else
        e.projections.zipWithIndex.collectFirst {
          case (p, j) if !passedThrough(p(i), e.child.output) =>
            s"projection $j: nested ${a.name} is ${p(i).sql}, not a column or NULL"
        }
    }
    val slotFailures = e.projections.zipWithIndex.flatMap { case (p, i) =>
      p.zipWithIndex.filterNot { case (_, c) => nestedColumns.contains(c) }.map(_._1).flatMap {
        case Literal(_, dt) if !TypeMapping.hasLane(dt) =>
          Some(s"projection $i: literal of unsupported type ${dt.simpleString}")
        case _: Literal => None
        case expr =>
          ExpressionCompiler.compileMovedColumn(expr, e.child.output) match {
            case Right(_) => None
            case Left(reason) => Some(s"projection $i: ${expr.sql}: $reason")
          }
      }
    }
    if (e.projections.isEmpty) Left("expand without projections")
    else if (outputFailures.nonEmpty) Left(outputFailures.mkString("; "))
    else if (slotFailures.nonEmpty) Left(slotFailures.distinct.mkString("; "))
    else Right(VectorExpandExec(e.projections, e.output, e.child))
  }

  /** A nested slot carried through: a reference to a child column, or a NULL. */
  private def passedThrough(slot: Expression, input: Seq[Attribute]): Boolean = slot match {
    case Literal(null, _) => true
    case a: Attribute => input.exists(_.exprId == a.exprId)
    case _ => false
  }
}
