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

import io.vecruntime.kernels.{ArrowLayout, Bitmap, SegmentVectorBuffers, VectorBuffers}

import org.apache.spark.sql.types.DataType

/**
 * Spark's `Empty2Null` (#842), which `V1Writes` puts on every string partition column so an empty string writes
 * the default partition: an empty string becomes null, everything else passes through. The child's buffers are
 * reused -- only the validity bitmap is new, and only when the batch holds an empty string. A dictionary column
 * tests each entry once and keeps its ids and dictionary.
 */
final case class Empty2NullExpr(child: VectorExpr, dataType: DataType) extends VectorExpr {
  override def children: Seq[VectorExpr] = Seq(child)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val in = child.eval(ctx)
    val n = ctx.numRows
    val dict = in.dictionary()
    // Which dictionary entries are empty: one test per entry, not per row.
    val emptyEntry: Array[Boolean] =
      if (dict == null) null
      else {
        val e = new Array[Boolean](dict.length())
        var k = 0
        while (k < e.length) {
          e(k) = !dict.isNull(k) && Empty2NullExpr.byteLength(dict, k) == 0
          k += 1
        }
        e
      }
    if (emptyEntry != null && !emptyEntry.contains(true)) return in
    var validity: java.lang.foreign.MemorySegment = null
    var i = 0
    while (i < n) {
      if (!in.isNull(i)) {
        val empty = if (dict != null) emptyEntry(in.getInt(i)) else Empty2NullExpr.byteLength(in, i) == 0
        if (empty) {
          if (validity == null) {
            validity = ArrowLayout.allocateBitmap(ctx.arena, n)
            if (in.hasNulls) java.lang.foreign.MemorySegment.copy(in.validity(), 0, validity, 0, Bitmap.bytesFor(n))
            else Bitmap.fillRange(validity, 0, n, true)
          }
          Bitmap.clear(validity, i)
        }
      }
      i += 1
    }
    if (validity == null) in
    else new SegmentVectorBuffers(in.`type`(), n, validity, in.data(), in.offsets(), dict)
  }
}

object Empty2NullExpr {

  /** The byte length of string `i` of an offsets-laid UTF8 column. */
  private[expr] def byteLength(v: VectorBuffers, i: Int): Int = {
    val off = v.offsets()
    off.get(VectorBuffers.LE_INT, (i + 1).toLong << 2) - off.get(VectorBuffers.LE_INT, i.toLong << 2)
  }
}
