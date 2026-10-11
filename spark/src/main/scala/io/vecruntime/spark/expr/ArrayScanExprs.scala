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
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, LongType, MapType}
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * A scan of each row's elements (#705, nested step 4), over an array column -- or a map's keys
 * (`map_keys(m)`, which `map_contains_key` becomes) -- read as a nested lane:
 *
 *  - `Contains`: `array_contains(arr, v)`. True when an element equals `v`; otherwise null when the
 *    array holds a null element, false when it does not. Null for a null array or a null `v`.
 *  - `Position`: `array_position(arr, v)`. The one-based position of the first element equal to `v`,
 *    0 when there is none (null elements never match); null for a null array or `v`.
 *  - `Min` / `Max`: `array_min(arr)` / `array_max(arr)`. The least / greatest non-null element (strings
 *    by unsigned bytes, booleans false < true), null when there is none.
 *
 * Elements compare on INT32, INT64, BOOL and UTF8 lanes (Spark's equality on those is the lane's own;
 * doubles, whose NaN and signed zeros Spark's ordering folds, stay with Spark).
 */
final case class ArrayScanExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    containerType: DataType,
    kind: ArrayScanExpr.Kind,
    arg: VectorExpr
) extends VectorExpr {
  private val steps = path.toArray

  private val elementType: DataType = containerType match {
    case a: ArrayType => a.elementType
    case m: MapType => m.keyType // the keys of a map: `map_keys(m)`
    case other => throw new IllegalArgumentException(s"an element scan over ${other.simpleString}")
  }

  override val dataType: DataType = kind match {
    case ArrayScanExpr.Contains => BooleanType
    case ArrayScanExpr.Position => LongType
    case _ => elementType
  }

  override def children: Seq[VectorExpr] = if (arg == null) Nil else Seq(arg)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    var cv: ColumnVector = ctx.column(inputOrdinal)
    val ancestors = new scala.collection.mutable.ArrayBuffer[ColumnVector]
    var s = 0
    while (s < steps.length) { if (cv.hasNull) ancestors += cv; cv = cv.getChild(steps(s)); s += 1 }
    val parents = ancestors.toArray
    val lane = NestedColumns.adapt(cv, containerType, n, ctx.arena).asInstanceOf[NestedVectorBuffers]
    val elements = lane.children().get(0)
    def containerNull(r: Int): Boolean =
      lane.isNull(r) || {
        var p = 0; var hit = false; while (p < parents.length && !hit) { hit = parents(p).isNullAt(r); p += 1 }; hit
      }

    kind match {
      case ArrayScanExpr.Min | ArrayScanExpr.Max =>
        val idx = new Array[Int](n)
        val sign = if (kind == ArrayScanExpr.Min) -1 else 1
        var r = 0
        while (r < n) {
          var best = -1
          if (!containerNull(r)) {
            var j = lane.start(r)
            val end = lane.end(r)
            while (j < end) {
              if (!elements.isNull(j) && (best < 0 || sign * ArrayScanExpr.compare(elements, j, best) > 0)) best = j
              j += 1
            }
          }
          idx(r) = best
          r += 1
        }
        ElementAccessExpr.gather(elements, idx, n, ctx.arena)

      case _ =>
        val literal = arg.isInstanceOf[LiteralExpr]
        val raw =
          if (literal) ElementAccessExpr.literalLane(arg.asInstanceOf[LiteralExpr], ctx.arena) else arg.eval(ctx)
        val v = if (raw.isDictionaryEncoded()) ArrowOutput.decodeDictionary(raw, ctx.arena) else raw
        val validity = ctx.bitmap()
        val position = kind == ArrayScanExpr.Position
        val data =
          if (position) ArrowLayout.allocateData(ctx.arena, VecType.INT64, n)
          else ArrowLayout.allocateBitmap(ctx.arena, n)
        var r = 0
        while (r < n) {
          val vr = if (literal) 0 else r
          if (!containerNull(r) && !v.isNull(vr)) {
            val start = lane.start(r)
            val end = lane.end(r)
            var j = start
            var found = -1
            var sawNull = false
            while (j < end && found < 0) {
              if (elements.isNull(j)) sawNull = true
              else if (ElementAccessExpr.sameKey(elements, j, v, vr)) found = j
              j += 1
            }
            if (position) {
              data.set(VectorBuffers.LE_LONG, r.toLong << 3, if (found < 0) 0L else (found - start + 1).toLong)
              Bitmap.set(validity, r)
            } else if (found >= 0) { Bitmap.set(data, r); Bitmap.set(validity, r) }
            else if (!sawNull) Bitmap.set(validity, r) // false; a null element leaves the answer null
          }
          r += 1
        }
        SegmentVectorBuffers.fixedWidth(if (position) VecType.INT64 else VecType.BOOL, n, validity, data)
    }
  }
}

object ArrayScanExpr {
  sealed trait Kind extends Serializable
  case object Contains extends Kind
  case object Position extends Kind
  case object Min extends Kind
  case object Max extends Kind

  /** Elements `i` and `j` of one lane in Spark's order (non-null both). */
  private[expr] def compare(v: VectorBuffers, i: Int, j: Int): Int = v.`type`() match {
    case VecType.INT32 => Integer.compare(v.getInt(i), v.getInt(j))
    case VecType.INT64 => java.lang.Long.compare(v.getLong(i), v.getLong(j))
    case VecType.BOOL => java.lang.Boolean.compare(v.getBoolean(i), v.getBoolean(j))
    case VecType.UTF8 =>
      val o = v.offsets()
      val is = o.get(VectorBuffers.LE_INT, i.toLong << 2)
      val ie = o.get(VectorBuffers.LE_INT, (i + 1).toLong << 2)
      val js = o.get(VectorBuffers.LE_INT, j.toLong << 2)
      val je = o.get(VectorBuffers.LE_INT, (j + 1).toLong << 2)
      val d = v.data()
      val m = java.lang.foreign.MemorySegment.mismatch(d, is.toLong, ie.toLong, d, js.toLong, je.toLong)
      if (m < 0) 0
      else if (m >= ie - is) -1 // i is a prefix of j
      else if (m >= je - js) 1
      else Integer.compare(
        d.get(java.lang.foreign.ValueLayout.JAVA_BYTE, is + m) & 0xff,
        d.get(java.lang.foreign.ValueLayout.JAVA_BYTE, js + m) & 0xff
      )
    case other => throw new IllegalStateException(s"no element order on a $other lane")
  }
}
