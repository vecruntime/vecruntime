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
package io.vecruntime.spark.expr

import io.vecruntime.kernels.{ArrowLayout, Bitmap, NestedVectorBuffers, SegmentVectorBuffers, VecType, VectorBuffers}
import org.apache.spark.sql.types.DataType

/**
 * Expressions whose result is itself an array (#705, nested results): a [[NestedVectorBuffers]] LIST
 * lane that [[org.apache.spark.sql.vecruntime.VectorProjectExec]] hands to Spark as a
 * `VectorNestedColumnVector`. Each keeps its input's offsets where the elements stay where they are
 * (`map_keys`, `map_values`, `transform`) and builds new ones where they do not (`filter`).
 */

/** `map_keys(m)` / `map_values(m)`: the map lane's keys or values child under the map's own offsets. */
final case class MapPartExpr(inputOrdinal: Int, path: Seq[Int], mapType: DataType, values: Boolean, dataType: DataType)
    extends VectorExpr {
  private val steps = path.toArray
  override def children: Seq[VectorExpr] = Nil

  override def eval(ctx: EvalContext): VectorBuffers = {
    val (lane, nulls) = NestedInput.read(ctx, inputOrdinal, steps, mapType)
    NestedVectorBuffers.list(
      ctx.numRows,
      NestedInput.validity(ctx, nulls),
      lane.offsets(),
      lane.children().get(if (values) 1 else 0)
    )
  }
}

/**
 * `transform(arr, x -> f)` / `transform(arr, (x, i) -> f)`: the lambda's answer per element, under
 * the array's offsets. A null array is null.
 */
final case class TransformExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    arrayType: DataType,
    body: VectorExpr,
    outer: Seq[Int],
    withIndex: Boolean,
    dataType: DataType
) extends VectorExpr {
  private val steps = path.toArray
  private val outers = outer.toArray
  override def children: Seq[VectorExpr] = Seq(body)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val ev = LambdaElements.evaluate(ctx, inputOrdinal, steps, arrayType, body, outers, withIndex)
    val m = ev.lane.children().get(0).length()
    val child = if (ev.answers != null) ev.answers else NestedResults.nulls(body.vecType, m, ctx)
    NestedVectorBuffers.list(ctx.numRows, NestedInput.validity(ctx, ev.nulls), ev.lane.offsets(), child)
  }
}

/**
 * `filter(arr, x -> p)` / `filter(arr, (x, i) -> p)`: the elements whose answer is true, in order, under
 * new offsets. A null array is null; a null answer drops the element, as Spark does.
 */
final case class ArrayFilterExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    arrayType: DataType,
    body: VectorExpr,
    outer: Seq[Int],
    withIndex: Boolean,
    dataType: DataType
) extends VectorExpr {
  private val steps = path.toArray
  private val outers = outer.toArray
  override def children: Seq[VectorExpr] = Seq(body)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val ev = LambdaElements.evaluate(ctx, inputOrdinal, steps, arrayType, body, outers, withIndex)
    val elements = ev.lane.children().get(0)
    val offsets = ArrowLayout.allocateOffsets(ctx.arena, n)
    val kept = new Array[Int](math.max(elements.length(), 1))
    var k = 0
    var r = 0
    while (r < n) {
      if (ev.live(r) && ev.answers != null) {
        var j = ev.lane.start(r)
        val end = ev.lane.end(r)
        while (j < end) {
          if (!ev.answers.isNull(j) && ev.answers.getBoolean(j)) { kept(k) = j; k += 1 }
          j += 1
        }
      }
      offsets.set(VectorBuffers.LE_INT, (r + 1).toLong << 2, k)
      r += 1
    }
    val child = ElementAccessExpr.gather(elements, kept, k, ctx.arena)
    NestedVectorBuffers.list(n, NestedInput.validity(ctx, ev.nulls), offsets, child)
  }
}

private[expr] object NestedResults {

  /** A literal as a constant lane of `n` rows (an `aggregate`'s zero, #705). */
  def constant(lit: LiteralExpr, n: Int, ctx: EvalContext): VectorBuffers = lit.vecType match {
    case VecType.INT32 =>
      val d = ArrowLayout.allocateData(ctx.arena, VecType.INT32, math.max(n, 1))
      val v = lit.number.intValue(); var i = 0
      while (i < n) { d.setAtIndex(VectorBuffers.LE_INT, i.toLong, v); i += 1 }
      SegmentVectorBuffers.fixedWidth(VecType.INT32, n, null, d)
    case VecType.INT64 =>
      val d = ArrowLayout.allocateData(ctx.arena, VecType.INT64, math.max(n, 1))
      val v = lit.number.longValue(); var i = 0
      while (i < n) { d.setAtIndex(VectorBuffers.LE_LONG, i.toLong, v); i += 1 }
      SegmentVectorBuffers.fixedWidth(VecType.INT64, n, null, d)
    case VecType.FLOAT64 =>
      val d = ArrowLayout.allocateData(ctx.arena, VecType.FLOAT64, math.max(n, 1))
      val v = lit.number.doubleValue(); var i = 0
      while (i < n) { d.setAtIndex(VectorBuffers.LE_DOUBLE, i.toLong, v); i += 1 }
      SegmentVectorBuffers.fixedWidth(VecType.FLOAT64, n, null, d)
    case VecType.UTF8 =>
      val b = lit.utf8Bytes
      val offsets = ArrowLayout.allocateOffsets(ctx.arena, n)
      val data = ArrowLayout.allocateBytes(ctx.arena, math.max(b.length.toLong * n, 1L))
      var i = 0
      while (i < n) {
        java.lang.foreign.MemorySegment.copy(
          b,
          0,
          data,
          java.lang.foreign.ValueLayout.JAVA_BYTE,
          i.toLong * b.length,
          b.length
        )
        offsets.set(VectorBuffers.LE_INT, (i + 1).toLong << 2, (i + 1) * b.length)
        i += 1
      }
      SegmentVectorBuffers.utf8(n, null, offsets, data)
    case other => throw new IllegalStateException(s"no constant $other lane")
  }

  /** An all-null lane of `m` rows (the answers of a lambda no live element ran). */
  def nulls(t: VecType, m: Int, ctx: EvalContext): VectorBuffers = {
    val validity = ArrowLayout.allocateBitmap(ctx.arena, math.max(m, 1))
    t match {
      case VecType.UTF8 =>
        SegmentVectorBuffers.utf8(
          m,
          validity,
          ArrowLayout.allocateOffsets(ctx.arena, m),
          ArrowLayout.allocateBytes(ctx.arena, 1L)
        )
      case VecType.BOOL =>
        SegmentVectorBuffers.fixedWidth(t, m, validity, ArrowLayout.allocateBitmap(ctx.arena, math.max(m, 1)))
      case _ => SegmentVectorBuffers.fixedWidth(t, m, validity, ArrowLayout.allocateData(ctx.arena, t, math.max(m, 1)))
    }
  }
}

/**
 * `slice(arr, start, length)` (#705): the elements from the one-based `start` (negative: from the end)
 * for `length` of them, under new offsets, the elements gathered once for the batch whatever their
 * lane (nested ones too). A start past either end is an empty array; a null array, start or length is
 * null. `start = 0` and a negative `length` raise Spark's errors, for active rows, whatever ANSI says.
 */
final case class SliceExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    arrayType: DataType,
    start: VectorExpr,
    length: VectorExpr,
    dataType: DataType
) extends VectorExpr {
  private val steps = path.toArray
  override def children: Seq[VectorExpr] = Seq(start, length)

  private def operand(e: VectorExpr, ctx: EvalContext): (VectorBuffers, Boolean) = e match {
    case lit: LiteralExpr => (ElementAccessExpr.literalLane(lit, ctx.arena), true)
    case other => (other.eval(ctx), false)
  }

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val (lane, nulls) = NestedInput.read(ctx, inputOrdinal, steps, arrayType)
    val (s, sLit) = operand(start, ctx)
    val (l, lLit) = operand(length, ctx)
    val active = ctx.active
    val validity = ctx.bitmap()
    val offsets = ArrowLayout.allocateOffsets(ctx.arena, n)
    val elements = lane.children().get(0)
    val kept = new Array[Int](math.max(elements.length(), 1))
    var k = 0
    var r = 0
    while (r < n) {
      val sr = if (sLit) 0 else r
      val lr = if (lLit) 0 else r
      if (!nulls(r) && !s.isNull(sr) && !l.isNull(lr)) {
        val st = s.getInt(sr)
        val len = l.getInt(lr)
        val live = active == null || Bitmap.isSet(active, r)
        if (live && st == 0) throw org.apache.spark.sql.vecruntime.VectorErrors.sliceStartZero()
        if (live && len < 0) throw org.apache.spark.sql.vecruntime.VectorErrors.sliceNegativeLength(len)
        Bitmap.set(validity, r)
        val size = lane.size(r)
        val from = if (st > 0) st - 1 else size + st
        if (live && from >= 0 && from < size && len > 0) {
          val to = math.min(size.toLong, from.toLong + len).toInt
          var j = lane.start(r) + from
          val end = lane.start(r) + to
          while (j < end) { kept(k) = j; k += 1; j += 1 }
        }
      }
      offsets.set(VectorBuffers.LE_INT, (r + 1).toLong << 2, k)
      r += 1
    }
    NestedVectorBuffers.list(n, validity, offsets, ElementAccessExpr.gather(elements, kept, k, ctx.arena))
  }
}
