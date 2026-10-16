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

import io.vecruntime.kernels.{NestedSortKey, VectorBuffers}
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * An array or struct sort key (#705) as its order-preserving bytes ([[NestedSortKey]]) on the UTF8 lane: the sort
 * kernels order it as they order a string, unsigned byte by byte, which is Spark's order of the value.
 */
final case class NestedSortKeyExpr(child: VectorExpr, keyType: DataType) extends VectorExpr {
  override def dataType: DataType = BinaryType
  override def children: Seq[VectorExpr] = Seq(child)

  override def eval(ctx: EvalContext): VectorBuffers = NestedSortKey.encode(ctx.arena, child.eval(ctx), ctx.numRows)
}

object NestedSortKeyExpr {

  /** Whether a sort key of type `dt` rides the encoding: an array or struct with lanes, no map inside (not orderable). */
  def supports(dt: DataType): Boolean = NestedKeyBytesExpr.supports(dt)
}
