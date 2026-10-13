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

import java.lang.foreign.MemorySegment

import io.vecruntime.kernels.{
  ArrowLayout,
  Bitmap,
  BitmapKernels,
  NestedVectorBuffers,
  SegmentVectorBuffers,
  VecType,
  VectorBuffers
}
import io.vecruntime.spark.arrow.ArrowOutput
import org.apache.spark.sql.types.DataType

/**
 * A lambda run once per batch over element rows (#705): `lanes` are its variables, one row per
 * element; `rowOf` maps each element to its row (-1 when no live row owns it), and the outer lane
 * columns the body reads are gathered through it. Only the elements in `selection` are evaluated.
 */
private[expr] object ElementLambda {
  def run(
      ctx: EvalContext,
      lanes: Array[VectorBuffers],
      rowOf: Array[Int],
      m: Int,
      selection: MemorySegment,
      selected: Int,
      body: VectorExpr,
      outers: Array[Int]
  ): VectorBuffers = {
    val gathered = outers.map(o => ElementAccessExpr.gather(NestedInput.outerLane(ctx, o), rowOf, m, ctx.arena))
    val k = lanes.length
    val inner = new EvalContext(ctx.arena, m, (o: Int) => if (o < k) lanes(o) else gathered(o - k), selection, selected)
    body.eval(inner)
  }
}

/**
 * `zip_with(a, b, (x, y) -> f)` (#705): one result per position of the longer array, the shorter
 * array's missing positions read as null, under new offsets. A null array on either side is null.
 */
final case class ZipWithExpr(
    left: (Int, Seq[Int]),
    leftType: DataType,
    right: (Int, Seq[Int]),
    rightType: DataType,
    body: VectorExpr,
    outer: Seq[Int],
    dataType: DataType
) extends VectorExpr {
  private val lSteps = left._2.toArray
  private val rSteps = right._2.toArray
  private val outers = outer.toArray
  override def children: Seq[VectorExpr] = Seq(body)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val (a, aNulls) = NestedInput.read(ctx, left._1, lSteps, leftType)
    val (b, bNulls) = NestedInput.read(ctx, right._1, rSteps, rightType)
    val active = ctx.active
    val nulls = Array.tabulate(n)(r => aNulls(r) || bNulls(r))
    val offsets = ArrowLayout.allocateOffsets(ctx.arena, n)
    var total = 0
    var r = 0
    while (r < n) {
      if (!nulls(r)) total += math.max(a.size(r), b.size(r))
      offsets.set(VectorBuffers.LE_INT, (r + 1).toLong << 2, total)
      r += 1
    }
    val ia = new Array[Int](math.max(total, 1))
    val ib = new Array[Int](math.max(total, 1))
    val rowOf = new Array[Int](math.max(total, 1))
    val selection = ArrowLayout.allocateBitmap(ctx.arena, math.max(total, 1))
    var selected = 0
    var e = 0
    r = 0
    while (r < n) {
      if (!nulls(r)) {
        val live = active == null || Bitmap.isSet(active, r)
        val len = math.max(a.size(r), b.size(r))
        var j = 0
        while (j < len) {
          ia(e) = if (j < a.size(r)) a.start(r) + j else -1
          ib(e) = if (j < b.size(r)) b.start(r) + j else -1
          rowOf(e) = if (live) r else -1
          if (live) { Bitmap.set(selection, e); selected += 1 }
          e += 1; j += 1
        }
      }
      r += 1
    }
    val x = ElementAccessExpr.gather(a.children().get(0), ia, total, ctx.arena)
    val y = ElementAccessExpr.gather(b.children().get(0), ib, total, ctx.arena)
    val answers =
      if (selected == 0) NestedResults.nullsOf(body.dataType, total, ctx)
      else ElementLambda.run(ctx, Array(x, y), rowOf, total, selection, selected, body, outers)
    NestedVectorBuffers.list(n, NestedInput.validity(ctx, nulls), offsets, answers)
  }
}

/**
 * `map_filter(m, (k, v) -> p)` and `transform_values(m, (k, v) -> f)` (#705): the lambda over each
 * entry's key and value; map_filter keeps the entries whose answer is true (a null answer drops the
 * entry, as Spark does) under new offsets, transform_values keeps every entry with the answer as its
 * value. A null map is null.
 */
final case class MapLambdaExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    mapType: DataType,
    body: VectorExpr,
    outer: Seq[Int],
    filter: Boolean,
    dataType: DataType
) extends VectorExpr {
  private val steps = path.toArray
  private val outers = outer.toArray
  override def children: Seq[VectorExpr] = Seq(body)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val (lane, nulls) = NestedInput.read(ctx, inputOrdinal, steps, mapType)
    val keys = lane.children().get(0)
    val values = lane.children().get(1)
    val m = keys.length()
    val active = ctx.active
    val rowOf = new Array[Int](math.max(m, 1))
    java.util.Arrays.fill(rowOf, -1)
    val live = new Array[Boolean](n)
    val selection = ArrowLayout.allocateBitmap(ctx.arena, math.max(m, 1))
    var selected = 0
    var r = 0
    while (r < n) {
      live(r) = !nulls(r) && (active == null || Bitmap.isSet(active, r))
      if (live(r)) {
        var j = lane.start(r)
        while (j < lane.end(r)) { rowOf(j) = r; Bitmap.set(selection, j); selected += 1; j += 1 }
      }
      r += 1
    }
    val answers =
      if (selected == 0) null
      else ElementLambda.run(ctx, Array(keys, values), rowOf, m, selection, selected, body, outers)
    val validity = NestedInput.validity(ctx, nulls)
    if (!filter) {
      val v = if (answers != null) answers else NestedResults.nullsOf(body.dataType, m, ctx)
      NestedVectorBuffers.map(n, validity, lane.offsets(), keys, v)
    } else {
      val offsets = ArrowLayout.allocateOffsets(ctx.arena, n)
      val kept = new Array[Int](math.max(m, 1))
      var k = 0
      r = 0
      while (r < n) {
        if (live(r) && answers != null) {
          var j = lane.start(r)
          while (j < lane.end(r)) {
            if (!answers.isNull(j) && answers.getBoolean(j)) { kept(k) = j; k += 1 }
            j += 1
          }
        }
        offsets.set(VectorBuffers.LE_INT, (r + 1).toLong << 2, k)
        r += 1
      }
      NestedVectorBuffers.map(
        n,
        validity,
        offsets,
        ElementAccessExpr.gather(keys, kept, k, ctx.arena),
        ElementAccessExpr.gather(values, kept, k, ctx.arena)
      )
    }
  }
}

/**
 * `aggregate(arr, zero, (acc, x) -> merge, acc -> finish)` (#705): the fold is sequential within a
 * row but independent across rows, so it runs position by position across the batch -- at position
 * `j` every live row with more than `j` elements merges its `j`-th element into its accumulator, the
 * others keep theirs (the merge is compiled as `if(live, merge, acc)`, so it is evaluated, and can
 * raise, only for rows Spark folds). Then `finish` over the accumulators; a null array is null, an
 * empty one is `finish(zero)`.
 *
 * Inputs of `merge`: 0 the accumulator, 1 the element, 2 the live flag, then the outer columns (row
 * aligned, read from the batch as they are). Inputs of `finish`: 0 the accumulator, then its outer
 * columns.
 */
final case class ArrayAggregateExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    arrayType: DataType,
    zero: VectorExpr,
    merge: VectorExpr,
    mergeOuter: Seq[Int],
    finish: Option[(VectorExpr, Seq[Int])],
    dataType: DataType
) extends VectorExpr {
  private val steps = path.toArray
  private val mergeOuters = mergeOuter.toArray
  override def children: Seq[VectorExpr] = Seq(zero, merge) ++ finish.map(_._1).toSeq

  private def context(ctx: EvalContext, lanes: Array[VectorBuffers], outers: Array[Int]): EvalContext = {
    val k = lanes.length
    val c = new EvalContext(
      ctx.arena,
      ctx.numRows,
      (o: Int) => if (o < k) lanes(o) else ctx.input(outers(o - k)),
      ctx.selection,
      ctx.selectedCount
    )
    c.active = ctx.active
    c
  }

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val (lane, nulls) = NestedInput.read(ctx, inputOrdinal, steps, arrayType)
    val elements = lane.children().get(0)
    val active = ctx.active
    var acc: VectorBuffers = zero match {
      case lit: LiteralExpr => NestedResults.constant(lit, n, ctx)
      case z => z.eval(ctx)
    }
    var maxLen = 0
    var r = 0
    while (r < n) {
      if (!nulls(r) && (active == null || Bitmap.isSet(active, r))) maxLen = math.max(maxLen, lane.size(r))
      r += 1
    }
    val idx = new Array[Int](n)
    var j = 0
    while (j < maxLen) {
      val liveBits = ArrowLayout.allocateBitmap(ctx.arena, n)
      r = 0
      while (r < n) {
        val live = !nulls(r) && (active == null || Bitmap.isSet(active, r)) && j < lane.size(r)
        idx(r) = if (live) lane.start(r) + j else -1
        if (live) Bitmap.set(liveBits, r)
        r += 1
      }
      val x = ElementAccessExpr.gather(elements, idx, n, ctx.arena)
      val liveLane = SegmentVectorBuffers.fixedWidth(VecType.BOOL, n, null, liveBits)
      acc = merge.eval(context(ctx, Array(acc, x, liveLane), mergeOuters))
      j += 1
    }
    val out = finish match {
      case Some((f, outers)) => f.eval(context(ctx, Array(acc), outers.toArray))
      case None => acc
    }
    NestedInput.validity(ctx, nulls) match {
      case null => out
      case v =>
        val combined = if (out.validity() == null) v
        else { BitmapKernels.and(v, out.validity(), v, n); v }
        StructFieldExpr.withValidity(out, combined)
    }
  }
}
