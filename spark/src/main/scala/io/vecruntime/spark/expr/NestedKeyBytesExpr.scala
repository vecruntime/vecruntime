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

import io.vecruntime.kernels.{NestedKeyCodec, VecType, VectorBuffers}
import io.vecruntime.spark.adapter.TypeMapping
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, MapType, StructType}

import scala.jdk.CollectionConverters._

/**
 * A nested grouping key (#705) as its canonical bytes ([[NestedKeyCodec]]) on the UTF8 lane, which
 * the group table hashes and compares like any string key. `keyType` is the key's own Spark type, the
 * one the aggregate decodes the stored bytes back to for its output.
 */
final case class NestedKeyBytesExpr(child: VectorExpr, keyType: DataType) extends VectorExpr {
  override def dataType: DataType = BinaryType
  override def children: Seq[VectorExpr] = Seq(child)

  override def eval(ctx: EvalContext): VectorBuffers = NestedKeyCodec.encode(ctx.arena, child.eval(ctx), ctx.numRows)
}

object NestedKeyBytesExpr {

  /** Whether a grouping key of type `dt` rides the codec: an array or struct with lanes and no map inside. */
  def supports(dt: DataType): Boolean = (dt.isInstanceOf[ArrayType] || dt.isInstanceOf[StructType]) &&
    !hasMap(dt) && TypeMapping.nestedTypeOf(dt) != null &&
    !TypeMapping.isCollated(dt) // a collated leaf's bytes are not its equality (#705)

  private def hasMap(dt: DataType): Boolean = dt match {
    case _: MapType => true
    case a: ArrayType => hasMap(a.elementType)
    case s: StructType => s.fields.exists(f => hasMap(f.dataType))
    case _ => false
  }

  /** The lane tree of `dt`, what [[NestedKeyCodec.decode]] rebuilds. */
  def shape(dt: DataType): NestedKeyCodec.Shape = dt match {
    case a: ArrayType => new NestedKeyCodec.Shape(VecType.LIST, java.util.List.of(shape(a.elementType)))
    case m: MapType => new NestedKeyCodec.Shape(VecType.MAP, java.util.List.of(shape(m.keyType), shape(m.valueType)))
    case s: StructType => new NestedKeyCodec.Shape(VecType.STRUCT, s.fields.map(f => shape(f.dataType)).toList.asJava)
    case leaf => NestedKeyCodec.Shape.leaf(TypeMapping.vecTypeOf(leaf))
  }
}
