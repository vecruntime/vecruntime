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
import io.vecruntime.spark.adapter.NestedColumns
import io.vecruntime.spark.arrow.ArrowOutput
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * A column (or a struct field of one) read as a nested lane, its ancestors' nulls folded in: the
 * shared first step of every expression over a nested value (#705).
 */
private[expr] object NestedInput {

  /** The lane of `containerType` at `path` under input `ordinal`, and per row whether it is null. */
  def read(
      ctx: EvalContext,
      ordinal: Int,
      steps: Array[Int],
      containerType: DataType
  ): (NestedVectorBuffers, Array[Boolean]) = {
    val n = ctx.numRows
    if (ctx.column(ordinal) == null) {
      // A lambda body's element context (#705, nested lambdas): the input is already a lane.
      val (leaf, nulls) = walkLane(ctx, ordinal, steps)
      return (leaf.asInstanceOf[NestedVectorBuffers], nulls)
    }
    var cv: ColumnVector = ctx.column(ordinal)
    val ancestors = new scala.collection.mutable.ArrayBuffer[ColumnVector]
    var s = 0
    while (s < steps.length) { if (cv.hasNull) ancestors += cv; cv = cv.getChild(steps(s)); s += 1 }
    val parents = ancestors.toArray
    val lane = NestedColumns.adapt(cv, containerType, n, ctx.arena).asInstanceOf[NestedVectorBuffers]
    val nulls = new Array[Boolean](n)
    var r = 0
    while (r < n) {
      var isNull = lane.isNull(r)
      var p = 0
      while (!isNull && p < parents.length) { isNull = parents(p).isNullAt(r); p += 1 }
      nulls(r) = isNull
      r += 1
    }
    (lane, nulls)
  }

  /**
   * Input `ordinal` taken as a lane (no batch column behind it -- a lambda body's element context) and
   * walked down the struct fields `steps`: the field's lane, and per row whether it or any ancestor
   * struct is null.
   */
  def walkLane(ctx: EvalContext, ordinal: Int, steps: Array[Int]): (VectorBuffers, Array[Boolean]) = {
    val n = ctx.numRows
    var v = ctx.input(ordinal)
    val nulls = new Array[Boolean](n)
    var s = 0
    while (s < steps.length) {
      var r = 0
      while (r < n) { if (v.isNull(r)) nulls(r) = true; r += 1 }
      v = v.asInstanceOf[NestedVectorBuffers].children().get(steps(s))
      s += 1
    }
    var r = 0
    while (r < n) { if (v.isNull(r)) nulls(r) = true; r += 1 }
    (v, nulls)
  }

  /** A validity bitmap of the rows that are not null (null when none is). */
  def validity(ctx: EvalContext, nulls: Array[Boolean]): java.lang.foreign.MemorySegment =
    if (!nulls.contains(true)) null
    else {
      val v = ctx.bitmap()
      var r = 0
      while (r < nulls.length) { if (!nulls(r)) Bitmap.set(v, r); r += 1 }
      v
    }
}

/**
 * A lambda over the elements of an array lane (#705), evaluated once per batch: input 0 is the
 * element, input 1 the element's index in its array when the lambda takes one, then the outer lane
 * columns the body reads, gathered so each element sees its row's value. Only the elements of live
 * rows (non-null and active) are evaluated, so the body raises what Spark would.
 */
private[expr] object LambdaElements {

  final class Evaluated(
      val lane: NestedVectorBuffers,
      val nulls: Array[Boolean],
      val live: Array[Boolean],
      /** The body's answer per element (`lane.children(0)`'s rows); null when no element is live. */
      val answers: VectorBuffers
  )

  def evaluate(
      ctx: EvalContext,
      ordinal: Int,
      steps: Array[Int],
      containerType: DataType,
      body: VectorExpr,
      outers: Array[Int],
      withIndex: Boolean
  ): Evaluated = {
    val n = ctx.numRows
    val (lane, nulls) = NestedInput.read(ctx, ordinal, steps, containerType)
    val elements = lane.children().get(0)
    val m = elements.length()
    val active = ctx.active
    val rowOf = new Array[Int](m)
    java.util.Arrays.fill(rowOf, -1)
    val index = if (withIndex) ArrowLayout.allocateData(ctx.arena, VecType.INT32, math.max(m, 1)) else null
    val live = new Array[Boolean](n)
    val selection = ArrowLayout.allocateBitmap(ctx.arena, math.max(m, 1))
    var selected = 0
    var r = 0
    while (r < n) {
      live(r) = !nulls(r) && (active == null || Bitmap.isSet(active, r))
      if (live(r)) {
        val start = lane.start(r)
        var j = start
        val end = lane.end(r)
        while (j < end) {
          rowOf(j) = r
          if (index != null) index.set(VectorBuffers.LE_INT, j.toLong << 2, j - start)
          Bitmap.set(selection, j); selected += 1; j += 1
        }
      }
      r += 1
    }
    val answers = if (selected == 0) null
    else {
      val gathered = outers.map { o =>
        val in = ctx.input(o)
        val plain = if (in.isDictionaryEncoded()) ArrowOutput.decodeDictionary(in, ctx.arena) else in
        ElementAccessExpr.gather(plain, rowOf, m, ctx.arena)
      }
      val indexLane = if (index == null) null else SegmentVectorBuffers.fixedWidth(VecType.INT32, m, null, index)
      val first = if (withIndex) 2 else 1
      val inner = new EvalContext(
        ctx.arena,
        m,
        (o: Int) => if (o == 0) elements else if (o == 1 && withIndex) indexLane else gathered(o - first),
        selection,
        selected
      )
      body.eval(inner)
    }
    new Evaluated(lane, nulls, live, answers)
  }
}
