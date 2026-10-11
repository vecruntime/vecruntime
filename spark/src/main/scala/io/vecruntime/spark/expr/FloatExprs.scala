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

import io.vecruntime.kernels.{ArrowLayout, SegmentVectorBuffers, VecType, VectorBuffers}
import org.apache.spark.sql.types.{DataType, FloatType}

/**
 * A FLOAT64 lane rounded to single precision (#712): every value `v` becomes `(double) (float) v`, so a
 * FLOAT result -- a cast into float, float arithmetic -- holds exactly the float Spark computes, widened.
 * Round-to-nearest-even, overflow to infinity and NaN preserved, as Java's narrowing conversion.
 */
final case class RoundFloatExpr(child: VectorExpr) extends VectorExpr {
  override def dataType: DataType = FloatType
  override def children: Seq[VectorExpr] = Seq(child)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val in = child.eval(ctx)
    val n = ctx.numRows
    val data = ArrowLayout.allocateData(ctx.arena, VecType.FLOAT64, n)
    val src = in.data()
    var i = 0
    while (i < n) {
      val d = src.getAtIndex(VectorBuffers.LE_DOUBLE, i.toLong)
      data.setAtIndex(VectorBuffers.LE_DOUBLE, i.toLong, d.toFloat.toDouble)
      i += 1
    }
    SegmentVectorBuffers.fixedWidth(VecType.FLOAT64, n, in.validity(), data)
  }
}
