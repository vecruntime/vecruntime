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

import io.vecruntime.kernels.{ArrowLayout, VectorBuffers}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A collated string's collation key (#705), Spark's `CollationKey`: the bytes
 * `CollationFactory.getCollationKeyBytes` gives each value, on the UTF8 lane as BINARY. Two values are equal
 * under the collation exactly when their keys are equal bytes, and the keys compare, unsigned, in the
 * collation's order, so a grouping, join, sort or window key over a collated string is its key: the kernels
 * order and compare it as any binary key, and our shuffle hashes it as Spark's `CollationAwareMurmur3Hash`
 * hashes the string (Murmur3 over the same bytes). A null string is a null key.
 */
final case class CollationKeyExpr(child: VectorExpr, collationId: Int) extends VectorExpr {
  override def dataType: DataType = BinaryType
  override def children: Seq[VectorExpr] = Seq(child)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val in = child.eval(ctx)
    val n = ctx.numRows
    val offsets = new Array[Int](n + 1)
    val nulls = new Array[Boolean](n)
    var out = new Array[Byte](math.max(16, n * 8))
    var size = 0
    var i = 0
    while (i < n) {
      if (in.isNull(i)) nulls(i) = true
      else {
        val key = CollationFactory.getCollationKeyBytes(UTF8String.fromBytes(in.getUtf8Bytes(i)), collationId)
        if (size + key.length > out.length) out = java.util.Arrays.copyOf(out, math.max(out.length * 2, size + key.length))
        System.arraycopy(key, 0, out, size, key.length)
        size += key.length
      }
      i += 1
      offsets(i) = size
    }
    ArrowLayout.ofUtf8(ctx.arena, out, offsets, n, nulls)
  }
}

object CollationKeyExpr {

  /** The collation id of a collated string type, or -1 for any other type (a plain string included). */
  def collationOf(dt: DataType): Int = dt match {
    case st: StringType if io.vecruntime.spark.adapter.TypeMapping.isCollated(st) => st.collationId
    case _ => -1
  }
}
