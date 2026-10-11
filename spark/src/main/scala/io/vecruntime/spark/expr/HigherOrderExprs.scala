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

import io.vecruntime.kernels.{ArrowLayout, Bitmap, SegmentVectorBuffers, VecType, VectorBuffers}
import org.apache.spark.sql.types.{BooleanType, DataType}

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
    val ev = LambdaElements.evaluate(ctx, inputOrdinal, steps, containerType, body, outers, withIndex = false)
    val lane = ev.lane
    val live = ev.live
    val answers = ev.answers
    val data = ArrowLayout.allocateBitmap(ctx.arena, n)
    val validity = ctx.bitmap()
    var r = 0
    while (r < n) {
      if (!ev.nulls(r)) {
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
