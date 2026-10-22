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

import java.lang.foreign.MemorySegment

import io.vecruntime.kernels.{
  ArrowLayout,
  Bitmap,
  NestedGatherKernels,
  NestedVectorBuffers,
  SegmentVectorBuffers,
  VecType,
  VectorBuffers
}
import io.vecruntime.spark.arrow.ArrowOutput
import org.apache.spark.sql.types.DataType

/**
 * `if(p, null, x)` / `if(p, x, null)` over a nested `x` (#842): `x`'s lane with its top-level validity
 * narrowed -- null where the branch picks the null (a null predicate takes the else branch, as Spark's `If`
 * does). The children are `x`'s own; Arrow leaves the values under a null slot unread.
 */
final case class NestedNullWhenExpr(predicate: VectorExpr, child: VectorExpr, nullWhenTrue: Boolean, dataType: DataType)
    extends VectorExpr {
  override def children: Seq[VectorExpr] = Seq(predicate, child)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val c = child.eval(ctx).asInstanceOf[NestedVectorBuffers]
    val p = CaseWhenExpr.materialise(predicate, ctx)
    val pv = p.validity()
    val pd = p.data()
    val cv = c.validity()
    val out = ArrowLayout.allocateBitmap(ctx.arena, n)
    var i = 0
    while (i < n) {
      val taken = (pv == null || Bitmap.isSet(pv, i)) && Bitmap.isSet(pd, i)
      val keep = if (nullWhenTrue) !taken else taken
      if (keep && (cv == null || Bitmap.isSet(cv, i))) Bitmap.set(out, i)
      i += 1
    }
    new NestedVectorBuffers(c.`type`(), c.length(), out, c.offsets(), c.children())
  }
}

/**
 * `array(e1, ..., ek)` over scalar lanes (#842): a list lane of `k` elements per row, the elements the
 * lanes interleaved row by row. Never null itself; a null element stays a null element.
 */
final case class CreateArrayExpr(elements: Seq[VectorExpr], dataType: DataType) extends VectorExpr {
  override def children: Seq[VectorExpr] = elements

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val k = elements.length
    NestedVectorBuffers.list(
      n,
      null,
      NestedConstructors.offsets(ctx, n, k),
      NestedConstructors.elements(
        elements.map(NestedConstructors.lane(_, ctx)),
        n,
        ctx
      )
    )
  }
}

/**
 * `map(k1, v1, ..., kk, vk)` (#842, #901 R2): `k` entries per row, keys and values interleaved row by
 * row; the keys and the values may themselves be nested (a map of struct values, Spark allows a nested
 * value type). The compiler takes it only when the keys cannot repeat within a row (one entry, or
 * distinct literal keys), so Spark's duplicate-key handling never applies; a null key fails as Spark's.
 */
final case class CreateMapExpr(keys: Seq[VectorExpr], values: Seq[VectorExpr], dataType: DataType)
    extends VectorExpr {
  override def children: Seq[VectorExpr] = keys ++ values

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val offsets = NestedConstructors.offsets(ctx, n, keys.length)
    val keyLanes = keys.map(NestedConstructors.lane(_, ctx))
    // Spark builds a row's map only when the operator keeps that row, so a null key raises only for an
    // ACTIVE row (a selection or a merge-clause mask narrows which rows the batch-wide eval emits).
    val active = ctx.active
    keyLanes.foreach { k =>
      val v = k.validity()
      if (v != null) {
        var i = 0
        while (i < n) {
          if ((active == null || Bitmap.isSet(active, i)) && !Bitmap.isSet(v, i))
            throw org.apache.spark.sql.vecruntime.VectorErrors.nullMapKey()
          i += 1
        }
      }
    }
    NestedVectorBuffers.map(
      n,
      null,
      offsets,
      NestedConstructors.elements(keyLanes, n, ctx),
      NestedConstructors.elements(values.map(NestedConstructors.lane(_, ctx)), n, ctx)
    )
  }
}

private[expr] object NestedConstructors {

  /** The lane of `e` over the batch: a literal materialised, a dictionary decoded. */
  def lane(e: VectorExpr, ctx: EvalContext): VectorBuffers = {
    val v = CaseWhenExpr.materialise(e, ctx)
    if (v.isDictionaryEncoded()) ArrowOutput.decodeDictionary(v, ctx.arena) else v
  }

  /** `k` elements per row: offsets `0, k, 2k, ...`. */
  def offsets(ctx: EvalContext, n: Int, k: Int): MemorySegment = {
    val off = ArrowLayout.allocateOffsets(ctx.arena, n)
    var i = 0
    while (i <= n) {
      off.setAtIndex(VectorBuffers.LE_INT, i.toLong, i * k)
      i += 1
    }
    off
  }

  /**
   * The child of `array(...)` / `map(...)`: element `i * k + j` is row `i` of lane `j`. A flat lane
   * (fixed width, BOOL, UTF8) is interleaved directly; a nested lane (a struct, array or map element or
   * value, #901 R2) is built by concatenating the `k` lanes and gathering them into interleaved order,
   * which reuses the tested nested concat and gather and composes to any depth.
   */
  def elements(lanes: Seq[VectorBuffers], n: Int, ctx: EvalContext): VectorBuffers =
    if (lanes.head.`type`().isNested) interleaveNested(lanes, n, ctx) else interleave(lanes, n, ctx)

  /**
   * Nested elements interleaved: the `k` lanes concatenated into one lane of `n * k` rows (lane `j`
   * occupying rows `[j * n, (j + 1) * n)`), then gathered by the permutation that puts row `i` of lane
   * `j` at position `i * k + j`.
   */
  private def interleaveNested(lanes: Seq[VectorBuffers], n: Int, ctx: EvalContext): VectorBuffers = {
    val k = lanes.length
    val m = n * k
    val builder = new io.vecruntime.kernels.ColumnBuilder(ctx.arena, lanes.head.`type`(), math.max(m, 1))
    lanes.foreach(builder.append)
    val combined = builder.view()
    val idx = new Array[Int](math.max(m, 1))
    var j = 0
    while (j < k) {
      var i = 0
      while (i < n) { idx(i * k + j) = j * n + i; i += 1 }
      j += 1
    }
    NestedGatherKernels.gather(combined, idx, m, ctx.arena)
  }

  /** Element `i * k + j` is row `i` of lane `j`; the lanes share one type (fixed width, BOOL or UTF8). */
  def interleave(lanes: Seq[VectorBuffers], n: Int, ctx: EvalContext): VectorBuffers = {
    val k = lanes.length
    val m = n * k
    val vt = lanes.head.`type`()
    val validity =
      if (lanes.forall(_.validity() == null)) null
      else {
        val v = ArrowLayout.allocateBitmap(ctx.arena, m)
        var j = 0
        while (j < k) {
          val src = lanes(j).validity()
          var i = 0
          while (i < n) {
            if (src == null || Bitmap.isSet(src, i)) Bitmap.set(v, i * k + j)
            i += 1
          }
          j += 1
        }
        v
      }
    vt match {
      case VecType.BOOL =>
        val d = ArrowLayout.allocateBitmap(ctx.arena, m)
        var j = 0
        while (j < k) {
          val src = lanes(j).data()
          var i = 0
          while (i < n) {
            if (Bitmap.isSet(src, i)) Bitmap.set(d, i * k + j)
            i += 1
          }
          j += 1
        }
        SegmentVectorBuffers.fixedWidth(VecType.BOOL, m, validity, d)
      case VecType.UTF8 =>
        var total = 0L
        lanes.foreach { l =>
          total += l.offsets().getAtIndex(
            VectorBuffers.LE_INT,
            n.toLong
          ) - l.offsets().getAtIndex(VectorBuffers.LE_INT, 0L)
        }
        val bytes = ArrowLayout.allocateBytes(ctx.arena, total)
        val off = ArrowLayout.allocateOffsets(ctx.arena, m)
        var pos = 0L
        var i = 0
        while (i < n) {
          var j = 0
          while (j < k) {
            val l = lanes(j)
            val s = l.offsets().getAtIndex(VectorBuffers.LE_INT, i.toLong)
            val e = l.offsets().getAtIndex(VectorBuffers.LE_INT, i.toLong + 1)
            off.setAtIndex(VectorBuffers.LE_INT, (i * k + j).toLong, pos.toInt)
            MemorySegment.copy(l.data(), s.toLong, bytes, pos, (e - s).toLong)
            pos += e - s
            j += 1
          }
          i += 1
        }
        off.setAtIndex(VectorBuffers.LE_INT, m.toLong, pos.toInt)
        SegmentVectorBuffers.utf8(m, validity, off, bytes)
      case fixed =>
        val w = fixed.byteWidth().toLong
        val d = ArrowLayout.allocateData(ctx.arena, fixed, m)
        var j = 0
        while (j < k) {
          val src = lanes(j).data()
          var i = 0
          while (i < n) {
            MemorySegment.copy(src, i * w, d, (i.toLong * k + j) * w, w)
            i += 1
          }
          j += 1
        }
        SegmentVectorBuffers.fixedWidth(fixed, m, validity, d)
    }
  }
}
