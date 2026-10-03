/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import io.vecruntime.kernels.{ArrowLayout, GatherKernels, SegmentVectorBuffers, VecType, VectorBuffers}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BooleanType, DataType, StringType}

/**
 * Velox's `evalWithMemo` over dictionary-encoded strings: an expression whose only input is one string
 * column is evaluated once per entry of that column's dictionary instead of once per row, and the rows
 * take their entry's result through their ids. The result is kept for as long as the dictionary is the
 * same one -- the native scan shares a row group's dictionary across its batches (#612) -- so a row group
 * pays for one evaluation over its distinct values, whatever its row count.
 *
 * A string result goes out dictionary encoded (the rows' ids over the results); any other lane is
 * gathered. Only for a dictionary with fewer entries than the batch has rows (else per row is no worse).
 *
 * Which expressions: deterministic trees over one string attribute and literals, built only of functions
 * that give a null for a null input and never raise on any value (an entry no row in the batch uses is
 * evaluated too). An expression that does not qualify, or a result with a null for a non-null entry, runs
 * per row as before.
 */
final case class DictionaryMemoExpr(child: VectorExpr, ordinal: Int) extends VectorExpr {

  override def dataType: DataType = child.dataType
  override def children: Seq[VectorExpr] = child :: Nil

  /** The last dictionary seen (its data and offsets segments) and the results over it, on the heap. */
  @transient @volatile private var memo: DictionaryMemoExpr.Memo = _

  /**
   * The dictionary of the previous batch, when it was seen once and not memoized: a memo is built on the
   * second batch over the same dictionary, so a per-batch dictionary (an exchange's, an aggregate's
   * output) never pays for one.
   */
  @transient @volatile private var seen: DictionaryMemoExpr.Memo = _

  override def eval(ctx: EvalContext): VectorBuffers = {
    val in = ctx.input(ordinal)
    if (!in.isDictionaryEncoded) return child.eval(ctx)
    val dict = in.dictionary()
    if (dict.length() >= ctx.numRows) return child.eval(ctx)
    var m = memo
    if (m == null || !m.isFor(dict)) {
      val s = seen
      if (s == null || !s.isFor(dict)) {
        seen = DictionaryMemoExpr.Memo.marker(dict)
        return child.eval(ctx)
      }
      m = DictionaryMemoExpr.Memo.of(child, ordinal, dict)
      memo = m
      DictionaryMemoExpr.BUILDS.increment()
    } else DictionaryMemoExpr.HITS.increment()
    if (m.results == null) return child.eval(ctx) // a null result for a non-null entry: per row
    DictionaryMemoExpr.gatherResults(in, m.results, ctx)
  }
}

object DictionaryMemoExpr {

  /** Memos built (a new dictionary) and batches served from one already built. Read by tests. */
  val BUILDS = new java.util.concurrent.atomic.LongAdder
  val HITS = new java.util.concurrent.atomic.LongAdder

  /** Results of the child over one dictionary, copied to the heap (they outlive any batch's arena). */
  final class Memo(val data: MemorySegment, val offsets: MemorySegment, val length: Int, val results: VectorBuffers) {
    def isFor(dict: VectorBuffers): Boolean =
      dict.length() == length && sameSegment(dict.data(), data) && sameSegment(dict.offsets(), offsets)
  }

  private def sameSegment(a: MemorySegment, b: MemorySegment): Boolean =
    (a eq b) || (a != null && b != null && a.isNative && b.isNative && a.address() == b.address() &&
      a.byteSize() == b.byteSize())

  object Memo {

    /** The identity of `dict`, with no results. */
    def marker(dict: VectorBuffers): Memo = new Memo(dict.data(), dict.offsets(), dict.length(), null)

    def of(child: VectorExpr, ordinal: Int, dict: VectorBuffers): Memo = {
      val n = dict.length()
      val arena = java.lang.foreign.Arena.ofConfined()
      try {
        val ctx = new EvalContext(
          arena,
          n,
          c => if (c == ordinal) dict else throw new IllegalStateException(s"memoized expression reads column $c")
        )
        val r = child.eval(ctx)
        val results = if (hasNull(r, n)) null else toHeap(r, n)
        new Memo(dict.data(), dict.offsets(), n, results)
      } finally arena.close()
    }
  }

  private def hasNull(r: VectorBuffers, n: Int): Boolean = {
    if (!r.hasNulls()) return false
    var i = 0
    while (i < n) { if (r.isNull(i)) return true; i += 1 }
    false
  }

  /** `r` (n rows, no nulls) on the heap, in plain form. */
  private def toHeap(r: VectorBuffers, n: Int): VectorBuffers = r.`type`() match {
    case VecType.UTF8 if !r.isDictionaryEncoded =>
      // Two bulk copies: the offsets rebased to 0, the bytes they span.
      val offsets = new Array[Int](n + 1)
      MemorySegment.copy(r.offsets(), VectorBuffers.LE_INT, 0L, offsets, 0, n + 1)
      val base = offsets(0)
      val bytes = new Array[Byte](offsets(n) - base)
      MemorySegment.copy(r.data(), java.lang.foreign.ValueLayout.JAVA_BYTE, base.toLong, bytes, 0, bytes.length)
      if (base != 0) { var i = 0; while (i <= n) { offsets(i) -= base; i += 1 } }
      SegmentVectorBuffers.utf8(n, null, MemorySegment.ofArray(offsets), MemorySegment.ofArray(bytes))
    case VecType.UTF8 =>
      val offsets = new Array[Int](n + 1)
      val bytes = new java.io.ByteArrayOutputStream()
      var i = 0
      while (i < n) {
        offsets(i) = bytes.size()
        bytes.writeBytes(r.getUtf8Bytes(i))
        i += 1
      }
      offsets(n) = bytes.size()
      SegmentVectorBuffers.utf8(n, null, MemorySegment.ofArray(offsets), MemorySegment.ofArray(bytes.toByteArray))
    case VecType.BOOL =>
      val words = new Array[Long]((n + 63) >>> 6)
      var i = 0
      while (i < n) { if (r.getBoolean(i)) words(i >>> 6) |= 1L << (i & 63); i += 1 }
      SegmentVectorBuffers.fixedWidth(VecType.BOOL, n, null, MemorySegment.ofArray(words))
    case t @ (VecType.INT32 | VecType.INT64 | VecType.FLOAT64) =>
      val w = t.byteWidth()
      val data = MemorySegment.ofArray(new Array[Long](math.max(1, (n * w + 7) / 8)))
      MemorySegment.copy(r.data(), 0L, data, 0L, n.toLong * w)
      SegmentVectorBuffers.fixedWidth(t, n, null, data)
    case other => throw new IllegalStateException(s"no memo for a $other result")
  }

  /** The batch's rows through their ids: dictionary-encoded strings, or the gathered lane. */
  def gatherResults(in: VectorBuffers, results: VectorBuffers, ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val validity = in.validity()
    if (results.`type`() == VecType.UTF8) return SegmentVectorBuffers.dictionaryUtf8(n, validity, in.data(), results)
    val ids = new Array[Int](n)
    MemorySegment.copy(in.data(), VectorBuffers.LE_INT, 0L, ids, 0, n)
    if (validity != null) {
      var i = 0
      while (i < n) { if (!io.vecruntime.kernels.Bitmap.isSet(validity, i)) ids(i) = -1; i += 1 }
    }
    val out =
      if (results.`type`() == VecType.BOOL) ArrowLayout.allocateBitmap(ctx.arena, n)
      else ArrowLayout.allocateData(ctx.arena, results.`type`(), n)
    GatherKernels.gatherFixed(results.`type`(), results.data(), ids, 0, n, out)
    SegmentVectorBuffers.fixedWidth(results.`type`(), n, validity, out)
  }

  /** The functions a memoized tree may contain: a null in gives a null out, and no value raises. */
  private def safe(e: Expression): Boolean = e match {
    case _: AttributeReference | _: Literal => true
    case _: Substring | _: Upper | _: Lower | _: StringTrim | _: StringTrimLeft | _: StringTrimRight => true
    case _: Like | _: StartsWith | _: EndsWith | _: Contains | _: StringReplace | _: Length | _: OctetLength => true
    case _: EqualTo | _: Not | _: In | _: InSet | _: Concat => true
    case _ => false
  }

  /**
   * Wraps `compiled` (the compilation of `e`) when `e` qualifies: deterministic, not a bare column or a
   * literal, one string attribute (bound at `ordinal` in `input`), and only [[safe]] functions.
   */
  def wrap(e: Expression, compiled: VectorExpr, input: Seq[Attribute]): VectorExpr = {
    if (!enabled || compiled.isInstanceOf[ColumnRef] || compiled.isInstanceOf[LiteralExpr]) return compiled
    if (e.isInstanceOf[Attribute] || !e.deterministic || e.references.size != 1) return compiled
    val a = e.references.head
    if (a.dataType != StringType || !e.find(x => !safe(x)).isEmpty) return compiled
    val lane = compiled.vecType
    if (
      !(compiled.dataType == StringType || compiled.dataType == BooleanType || lane == VecType.INT32 ||
        lane == VecType.INT64 || lane == VecType.FLOAT64)
    )
      return compiled
    val ordinal = input.indexWhere(_.exprId == a.exprId)
    if (ordinal < 0) compiled else DictionaryMemoExpr(compiled, ordinal)
  }

  /** `spark.vecruntime.expr.dictionaryMemo`, read when an expression is compiled (on the driver). */
  private def enabled: Boolean =
    io.vecruntime.spark.VectorConf.exprDictionaryMemo(org.apache.spark.sql.internal.SQLConf.get)
}
