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

import io.vecruntime.kernels.{NestedVectorBuffers, VectorBuffers}
import org.apache.spark.sql.types.DataType

import scala.jdk.CollectionConverters._

/**
 * `named_struct(...)` (#705): a struct lane over its fields' lanes, each evaluated over the batch.
 * With `nullWhen`, the struct is null where that lane is null -- Spark's `if (isnull(s)) null else
 * named_struct(... s.f ...)`, the shape NormalizeFloatingNumbers rebuilds a struct key with -- and
 * not null anywhere else, as the constructor is.
 */
final case class NamedStructExpr(fields: Seq[VectorExpr], nullWhen: Option[VectorExpr], dataType: DataType)
    extends VectorExpr {
  override def children: Seq[VectorExpr] = fields ++ nullWhen.toSeq

  override def eval(ctx: EvalContext): VectorBuffers = {
    // A field may be a literal (a folded `named_struct('a', 1)`, #901 R2): materialised to a constant
    // lane, not evaluated as a column.
    val lanes = fields.map(CaseWhenExpr.materialise(_, ctx))
    val validity = nullWhen.map(_.eval(ctx).validity()).orNull
    NestedVectorBuffers.struct(ctx.numRows, validity, lanes.asJava)
  }
}
