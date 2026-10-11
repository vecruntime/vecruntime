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
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * `exists(arr, x -> p)` / `forall(arr, x -> p)` (#705, nested step 5): the lambda is compiled once as
 * an ordinary vector expression over the element lane -- input 0 is the lambda variable, inputs 1..
 * are the outer columns it reads, gathered so each element sees its row's value -- and evaluated
 * once per batch over every element of the active rows. Each row then folds its elements' answers
 * with Spark's three-valued logic:
 *
 *  - exists: true when any element is true; otherwise null when any is null (unless
 *    `spark.sql.legacy.followThreeValuedLogicInArrayExists` is off: then false); else false. An empty
 *    array is false.
 *  - forall: false when any element is false; otherwise null when any is null; else true. An empty
 *    array is true.
 *
 * A null array is null. The lambda runs only over the elements of active rows, so an error it raises
 * (ANSI arithmetic) is one Spark would raise too.
 */
final case class ArrayPredicateExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    containerType: DataType,
    forAll: Boolean,
    body: VectorExpr,
    outer: Seq[Int],
    followThreeValuedLogic: Boolean
) extends VectorExpr {
  private val steps = path.toArray
  private val outers = outer.toArray

  override def dataType: DataType = BooleanType
  override def children: Seq[VectorExpr] = Seq(body)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    var cv: ColumnVector = ctx.column(inputOrdinal)
    val ancestors = new scala.collection.mutable.ArrayBuffer[ColumnVector]
    var s = 0
    while (s < steps.length) { if (cv.hasNull) ancestors += cv; cv = cv.getChild(steps(s)); s += 1 }
    val parents = ancestors.toArray
    val lane = NestedColumns.adapt(cv, containerType, n, ctx.arena).asInstanceOf[NestedVectorBuffers]
    val elements = lane.children().get(0)
    val m = elements.length()
    val active = ctx.active
    // Each element's row (-1 for an element no live row owns), and the elements to evaluate.
    val rowOf = new Array[Int](m)
    java.util.Arrays.fill(rowOf, -1)
    val live = new Array[Boolean](n)
    val selection = ArrowLayout.allocateBitmap(ctx.arena, math.max(m, 1))
    var selected = 0
    var r = 0
    while (r < n) {
      var isNull = lane.isNull(r)
      var p = 0
      while (!isNull && p < parents.length) { isNull = parents(p).isNullAt(r); p += 1 }
      live(r) = !isNull && (active == null || Bitmap.isSet(active, r))
      if (live(r)) {
        var j = lane.start(r)
        val end = lane.end(r)
        while (j < end) { rowOf(j) = r; Bitmap.set(selection, j); selected += 1; j += 1 }
      }
      r += 1
    }
    val answers: VectorBuffers = if (selected == 0) null
    else {
      val gathered = outers.map { o =>
        val in = ctx.input(o)
        val plain = if (in.isDictionaryEncoded()) ArrowOutput.decodeDictionary(in, ctx.arena) else in
        ElementAccessExpr.gather(plain, rowOf, m, ctx.arena)
      }
      val inner = new EvalContext(
        ctx.arena,
        m,
        (o: Int) => if (o == 0) elements else gathered(o - 1),
        selection,
        selected
      )
      body.eval(inner)
    }
    val data = ArrowLayout.allocateBitmap(ctx.arena, n)
    val validity = ctx.bitmap()
    r = 0
    while (r < n) {
      var isNull = lane.isNull(r)
      var p = 0
      while (!isNull && p < parents.length) { isNull = parents(p).isNullAt(r); p += 1 }
      if (!isNull) {
        // exists looks for a true, forall for a false: the decisive answer.
        val decisive = !forAll
        var hit = false
        var sawNull = false
        if (live(r)) {
          var j = lane.start(r)
          val end = lane.end(r)
          while (j < end && !hit) {
            if (answers.isNull(j)) sawNull = true else if (answers.getBoolean(j) == decisive) hit = true
            j += 1
          }
        }
        val value = if (hit) decisive else !decisive
        if (hit || !sawNull || (!forAll && !followThreeValuedLogic)) {
          if (value) Bitmap.set(data, r)
          Bitmap.set(validity, r)
        }
      }
      r += 1
    }
    SegmentVectorBuffers.fixedWidth(VecType.BOOL, n, validity, data)
  }
}
