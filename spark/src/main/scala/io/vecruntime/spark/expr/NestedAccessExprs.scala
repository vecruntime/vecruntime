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

import java.lang.foreign.{Arena, MemorySegment}

import io.vecruntime.kernels.{
  ArrowLayout,
  Bitmap,
  GatherKernels,
  NestedVectorBuffers,
  SegmentVectorBuffers,
  VecType,
  VectorBuffers
}
import io.vecruntime.spark.adapter.NestedColumns
import io.vecruntime.spark.arrow.ArrowOutput
import org.apache.spark.QueryContext
import org.apache.spark.sql.types.{ArrayType, DataType, MapType}
import org.apache.spark.sql.vecruntime.VectorErrors
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * One element of an array or one value of a map per row (#705, nested step 3), over a column (or a
 * struct field of one) read as a nested lane: the row's element position is resolved against the
 * lane's offsets, then the element child is gathered at those positions -- one gather for the batch,
 * whatever the element lane.
 *
 *  - `ZeroBased`: `arr[i]` (`GetArrayItem`). Out of range is null, or Spark's INVALID_ARRAY_INDEX
 *    when `failOnError` (ANSI).
 *  - `OneBased`: `element_at(arr, i)`. Negative indexes count from the end. 0 is always
 *    INVALID_INDEX_OF_ZERO. Out of range is null, or INVALID_ARRAY_INDEX_IN_ELEMENT_AT under ANSI.
 *  - `MapKey`: `map[k]` / `element_at(map, k)`. The value of the first entry whose key equals `k`,
 *    null when there is none (Spark returns null for a missing key under ANSI too).
 *
 * A null container, a null index or key, and a null element are null. Errors are raised only for
 * rows that are active (`EvalContext.active`), as Spark raises them only for rows it evaluates.
 */
final case class ElementAccessExpr(
    inputOrdinal: Int,
    path: Seq[Int],
    containerType: DataType,
    mode: ElementAccessExpr.Mode,
    arg: VectorExpr,
    failOnError: Boolean,
    queryContext: QueryContext
) extends VectorExpr {
  private val steps = path.toArray

  override val dataType: DataType = containerType match {
    case a: ArrayType => a.elementType
    case m: MapType => m.valueType
    case other => throw new IllegalArgumentException(s"element access over ${other.simpleString}")
  }

  override def children: Seq[VectorExpr] = Seq(arg)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    var cv: ColumnVector = ctx.column(inputOrdinal)
    val ancestors = new scala.collection.mutable.ArrayBuffer[ColumnVector]
    var s = 0
    while (s < steps.length) { if (cv.hasNull) ancestors += cv; cv = cv.getChild(steps(s)); s += 1 }
    val parents = ancestors.toArray
    val lane = NestedColumns.adapt(cv, containerType, n, ctx.arena).asInstanceOf[NestedVectorBuffers]
    // A literal index or key is one row read for every row.
    val literal = arg.isInstanceOf[LiteralExpr]
    val a = if (literal) ElementAccessExpr.literalLane(arg.asInstanceOf[LiteralExpr], ctx.arena) else arg.eval(ctx)
    val idx = new Array[Int](n)
    val active = ctx.active
    mode match {
      case ElementAccessExpr.MapKey => resolveKeys(lane, a, literal, parents, idx, n, ctx.arena)
      case _ =>
        val wide = a.`type`() == VecType.INT64
        var r = 0
        while (r < n) {
          val ar = if (literal) 0 else r
          idx(r) =
            if (a.isNull(ar) || lane.isNull(r) || isNullIn(parents, r)) -1
            else {
              // Spark reads the index as an Int (`intValue`), so a long ordinal truncates as there.
              val i = if (wide) a.getLong(ar).toInt else a.getInt(ar)
              val start = lane.start(r)
              val size = lane.size(r)
              val live = active == null || Bitmap.isSet(active, r)
              if (mode == ElementAccessExpr.ZeroBased) {
                if (i >= 0 && i < size) start + i
                else if (failOnError && live) throw VectorErrors.invalidArrayIndex(i, size, queryContext)
                else -1
              } else if (size < math.abs(i)) {
                if (failOnError && live) throw VectorErrors.invalidElementAtIndex(i, size, queryContext)
                else -1
              } else if (i == 0) {
                if (live) throw VectorErrors.invalidIndexOfZero(queryContext) else -1
              } else if (i > 0) start + i - 1
              else start + size + i
            }
          r += 1
        }
    }
    val child = lane.children().get(if (mode == ElementAccessExpr.MapKey) 1 else 0)
    ElementAccessExpr.gather(child, idx, n, ctx.arena)
  }

  private def isNullIn(parents: Array[ColumnVector], r: Int): Boolean = {
    var p = 0
    while (p < parents.length) { if (parents(p).isNullAt(r)) return true; p += 1 }
    false
  }

  /** The position of each row's first entry whose key equals the row's key, or -1. */
  private def resolveKeys(
      lane: NestedVectorBuffers,
      key: VectorBuffers,
      literal: Boolean,
      parents: Array[ColumnVector],
      idx: Array[Int],
      n: Int,
      arena: Arena
  ): Unit = {
    val keys = lane.children().get(0)
    val k = if (key.isDictionaryEncoded()) ArrowOutput.decodeDictionary(key, arena) else key
    var r = 0
    while (r < n) {
      idx(r) = -1
      val kr = if (literal) 0 else r
      if (!k.isNull(kr) && !lane.isNull(r) && !isNullIn(parents, r)) {
        var j = lane.start(r)
        val end = lane.end(r)
        while (j < end && idx(r) < 0) { if (ElementAccessExpr.sameKey(keys, j, k, kr)) idx(r) = j; j += 1 }
      }
      r += 1
    }
  }
}

object ElementAccessExpr {
  sealed trait Mode extends Serializable
  case object ZeroBased extends Mode
  case object OneBased extends Mode
  case object MapKey extends Mode

  /** Key lanes a map lookup compares (map keys are never null). */
  def keyLane(t: VecType): Boolean =
    t == VecType.INT32 || t == VecType.INT64 || t == VecType.UTF8 || t == VecType.BOOL

  private[expr] def sameKey(keys: VectorBuffers, j: Int, key: VectorBuffers, r: Int): Boolean = keys.`type`() match {
    case VecType.INT32 => keys.getInt(j) == key.getInt(r)
    case VecType.INT64 => keys.getLong(j) == key.getLong(r)
    case VecType.BOOL => keys.getBoolean(j) == key.getBoolean(r)
    case VecType.UTF8 =>
      val ks = keys.offsets().get(VectorBuffers.LE_INT, j.toLong << 2)
      val ke = keys.offsets().get(VectorBuffers.LE_INT, (j + 1).toLong << 2)
      val vs = key.offsets().get(VectorBuffers.LE_INT, r.toLong << 2)
      val ve = key.offsets().get(VectorBuffers.LE_INT, (r + 1).toLong << 2)
      ke - ks == ve - vs &&
      MemorySegment.mismatch(keys.data(), ks.toLong, ke.toLong, key.data(), vs.toLong, ve.toLong) < 0
    case other => throw new IllegalStateException(s"no map lookup on a $other key")
  }

  /** A literal index or key as a one-row lane. */
  def literalLane(lit: LiteralExpr, arena: Arena): VectorBuffers = lit.vecType match {
    case VecType.INT32 => ArrowLayout.ofInts(arena, Array(lit.number.intValue()), null)
    case VecType.INT64 => ArrowLayout.ofLongs(arena, Array(lit.number.longValue()), null)
    case VecType.UTF8 =>
      ArrowLayout.ofStrings(arena, Array(new String(lit.utf8Bytes, java.nio.charset.StandardCharsets.UTF_8)))
    case other => throw new IllegalStateException(s"no literal $other index or key")
  }

  /** Rows `idx` of `child` (-1 = null) as a lane of `n` rows. */
  def gather(child: VectorBuffers, idx: Array[Int], n: Int, arena: Arena): VectorBuffers = {
    val validity = ArrowLayout.allocateBitmap(arena, n)
    child.`type`() match {
      case VecType.UTF8 =>
        val offsets = ArrowLayout.allocateOffsets(arena, n)
        val data = ArrowLayout.allocateBytes(arena, math.max(GatherKernels.gatherUtf8Bytes(child, idx, 0, n), 1L))
        GatherKernels.gatherUtf8(child, idx, 0, n, offsets, data, validity)
        SegmentVectorBuffers.utf8(n, validity, offsets, data)
      case t =>
        val data =
          if (t == VecType.BOOL) ArrowLayout.allocateBitmap(arena, n) else ArrowLayout.allocateData(arena, t, n)
        if (child.length() == 0) {
          // No element anywhere: every row is null (and no position to read).
          Bitmap.fillRange(validity, 0, n, false)
          SegmentVectorBuffers.fixedWidth(t, n, validity, data)
        } else {
          GatherKernels.gatherFixed(child, idx, 0, n, data, validity)
          SegmentVectorBuffers.fixedWidth(t, n, validity, data)
        }
    }
  }
}
