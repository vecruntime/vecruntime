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
package io.vecruntime.spark.parquet

import java.lang.foreign.{Arena, MemorySegment, ValueLayout}
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

import io.vecruntime.kernels.{VecType, VectorBuffers}
import io.vecruntime.kernels.parquet.ParquetPageDecoder
import io.vecruntime.kernels.parquet.ParquetPageDecoder.{Encoding, Page}
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.values.plain.PlainValuesWriter
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.io.api.Binary
import org.scalatest.funsuite.AnyFunSuite

/**
 * Cross-checks [[ParquetPageDecoder]] against pages produced by parquet-java's OWN value writers
 * ([[PlainValuesWriter]], [[RunLengthBitPackingHybridEncoder]]). The kernels module has no
 * parquet-java dependency, so this lives in spark/, whose test classpath carries parquet-column
 * transitively via Spark. It complements the kernels scalar-oracle tests: those pin the decoder
 * against a from-scratch encoder, this pins it against the reference writer whose bytes the real
 * scan node will feed it.
 */
class ParquetPageDecoderCrossCheckSuite extends AnyFunSuite {

  private val LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN)

  private def segOf(bytes: Array[Byte], arena: Arena): MemorySegment = {
    val seg = arena.allocate(bytes.length.toLong + 8L)
    MemorySegment.copy(MemorySegment.ofArray(bytes), 0, seg, 0, bytes.length)
    seg
  }

  /** parquet-java's RLE/bit-packed encoding of `levels` at `bitWidth` (definition levels, dict ids). */
  private def rleEncode(levels: Array[Int], bitWidth: Int): Array[Byte] = {
    val enc = new RunLengthBitPackingHybridEncoder(bitWidth, 64, 1024, HeapByteBufferAllocator.getInstance())
    levels.foreach(enc.writeInt)
    enc.toBytes.toByteArray
  }

  /** A v1 page: int32 def-level byte-length prefix + RLE def levels + value bytes. */
  private def v1(valueBytes: Array[Byte], present: Array[Boolean], maxDef: Int, enc: Encoding, arena: Arena): Page = {
    val out = new java.io.ByteArrayOutputStream()
    if (maxDef > 0) {
      val bw = ParquetPageDecoder.bitWidth(maxDef)
      val levels = present.map(p => if (p) maxDef else 0)
      val lvl = rleEncode(levels, bw)
      val len = lvl.length
      for (b <- 0 until 4) out.write((len >>> (8 * b)) & 0xff)
      out.write(lvl)
    }
    out.write(valueBytes)
    val all = out.toByteArray
    Page.v1(segOf(all, arena), 0, all.length.toLong, present.length, maxDef, enc)
  }

  /** A v2 page: separate RLE def-level slice + value slice. */
  private def v2(valueBytes: Array[Byte], present: Array[Boolean], maxDef: Int, enc: Encoding, arena: Arena): Page = {
    val lvl = if (maxDef > 0) rleEncode(present.map(p => if (p) maxDef else 0), ParquetPageDecoder.bitWidth(maxDef))
    else Array.emptyByteArray
    val seg = arena.allocate(lvl.length.toLong + valueBytes.length.toLong + 8L)
    MemorySegment.copy(MemorySegment.ofArray(lvl), 0, seg, 0, lvl.length)
    MemorySegment.copy(MemorySegment.ofArray(valueBytes), 0, seg, lvl.length.toLong, valueBytes.length)
    Page.v2(seg, 0, lvl.length, lvl.length.toLong, valueBytes.length.toLong, present.length, maxDef, enc)
  }

  private def plainInts(values: Array[Int], present: Array[Boolean]): Array[Byte] = {
    val w = new PlainValuesWriter(64, 1024, HeapByteBufferAllocator.getInstance())
    values.indices.foreach(i => if (present(i)) w.writeInteger(values(i)))
    w.getBytes.toByteArray
  }

  private def plainLongs(values: Array[Long], present: Array[Boolean]): Array[Byte] = {
    val w = new PlainValuesWriter(64, 1024, HeapByteBufferAllocator.getInstance())
    values.indices.foreach(i => if (present(i)) w.writeLong(values(i)))
    w.getBytes.toByteArray
  }

  private def plainDoubles(values: Array[Double], present: Array[Boolean]): Array[Byte] = {
    val w = new PlainValuesWriter(64, 1024, HeapByteBufferAllocator.getInstance())
    values.indices.foreach(i => if (present(i)) w.writeDouble(values(i)))
    w.getBytes.toByteArray
  }

  private def plainBinaries(values: Array[Array[Byte]], present: Array[Boolean]): Array[Byte] = {
    val w = new PlainValuesWriter(64, 1024, HeapByteBufferAllocator.getInstance())
    values.indices.foreach(i => if (present(i)) w.writeBytes(Binary.fromConstantByteArray(values(i))))
    w.getBytes.toByteArray
  }

  /** RLE_DICTIONARY value bytes: 1-byte id bit width + parquet-java RLE id stream (present ids only). */
  private def dictIds(ids: Array[Int], present: Array[Boolean], idBitWidth: Int): Array[Byte] = {
    val live = ids.indices.filter(present).map(ids).toArray
    val stream = rleEncode(live, idBitWidth)
    val out = new java.io.ByteArrayOutputStream()
    out.write(idBitWidth)
    out.write(stream)
    out.toByteArray
  }

  private def idBitWidth(maxId: Int): Int =
    if (maxId == 0) 1 else 32 - Integer.numberOfLeadingZeros(maxId)

  private def fixedDict(values: Array[Long], tpe: VecType, arena: Arena): VectorBuffers = {
    val present = Array.fill(values.length)(true)
    val bytes = tpe match {
      case VecType.INT32 => plainInts(values.map(_.toInt), present)
      case VecType.INT64 => plainLongs(values, present)
      case VecType.FLOAT64 => plainDoubles(values.map(java.lang.Double.longBitsToDouble), present)
      case other => throw new IllegalArgumentException(other.toString)
    }
    ParquetPageDecoder.decodeDictionary(segOf(bytes, arena), 0, bytes.length.toLong, values.length, tpe, arena)
  }

  private def utf8Dict(values: Array[Array[Byte]], arena: Arena): VectorBuffers = {
    val bytes = plainBinaries(values, Array.fill(values.length)(true))
    ParquetPageDecoder.decodeDictionary(segOf(bytes, arena), 0, bytes.length.toLong, values.length, VecType.UTF8, arena)
  }

  // ------------------------------------------------------------------- tests

  test("PLAIN INT32 with nulls, v1 and v2, matches parquet-java writer bytes") {
    val v = Array(5, -7, 11, 0, 123456, -1, 0, 99)
    val p = Array(true, true, false, true, true, false, false, true)
    for (
      page <- Seq(
        v1(plainInts(v, p), p, 1, Encoding.PLAIN, _),
        v2(plainInts(v, p), p, 1, Encoding.PLAIN, _)
      )
    ) {
      val arena = Arena.ofConfined()
      try {
        val vb = ParquetPageDecoder.decode(page(arena), VecType.INT32, null, arena)
        assert(vb.length() == v.length)
        for (i <- v.indices) {
          assert(vb.isNull(i) == !p(i))
          if (p(i)) assert(vb.getInt(i) == v(i))
        }
      } finally arena.close()
    }
  }

  test("PLAIN INT64 no nulls matches") {
    val v = Array(1L, Long.MaxValue, Long.MinValue, -123456789012345L, 0L)
    val p = Array.fill(v.length)(true)
    val arena = Arena.ofConfined()
    try {
      val vb = ParquetPageDecoder.decode(v1(plainLongs(v, p), p, 0, Encoding.PLAIN, arena), VecType.INT64, null, arena)
      assert(!vb.hasNulls)
      for (i <- v.indices) assert(vb.getLong(i) == v(i))
    } finally arena.close()
  }

  test("PLAIN DOUBLE incl. NaN and -0.0 matches") {
    val v = Array(1.5, -0.0, math.Pi, 1e-300, Double.NaN, Double.PositiveInfinity)
    val p = Array.fill(v.length)(true)
    val arena = Arena.ofConfined()
    try {
      val vb =
        ParquetPageDecoder.decode(v2(plainDoubles(v, p), p, 0, Encoding.PLAIN, arena), VecType.FLOAT64, null, arena)
      for (i <- v.indices) assert(java.lang.Double.doubleToRawLongBits(vb.getDouble(i))
        == java.lang.Double.doubleToRawLongBits(v(i)))
    } finally arena.close()
  }

  test("PLAIN BINARY/UTF8 with nulls and empties matches") {
    val v = Array(
      "héllo".getBytes(StandardCharsets.UTF_8),
      Array.emptyByteArray,
      Array.emptyByteArray,
      "a longer value".getBytes(StandardCharsets.UTF_8),
      "z".getBytes(StandardCharsets.UTF_8)
    )
    val p = Array(true, true, false, true, true)
    for (
      page <- Seq(v1(plainBinaries(v, p), p, 1, Encoding.PLAIN, _), v2(plainBinaries(v, p), p, 1, Encoding.PLAIN, _))
    ) {
      val arena = Arena.ofConfined()
      try {
        val vb = ParquetPageDecoder.decode(page(arena), VecType.UTF8, null, arena)
        for (i <- v.indices) {
          assert(vb.isNull(i) == !p(i))
          if (p(i)) assert(vb.getUtf8Bytes(i).sameElements(v(i)))
        }
      } finally arena.close()
    }
  }

  test("RLE_DICTIONARY INT64 with nulls matches (ids via parquet-java RLE encoder)") {
    val dict = Array(1000L, 2000L, 3000L, 4000L, 5000L)
    val ids = Array(0, 4, 2, 1, 3, 0, 0, 4, 2, 2)
    val p = Array(true, false, true, true, false, true, true, true, false, true)
    val arena = Arena.ofConfined()
    try {
      val dictVb = fixedDict(dict, VecType.INT64, arena)
      val bytes = dictIds(ids, p, idBitWidth(dict.length - 1))
      for (
        page <- Seq(v1(bytes, p, 1, Encoding.RLE_DICTIONARY, arena), v2(bytes, p, 1, Encoding.RLE_DICTIONARY, arena))
      ) {
        val vb = ParquetPageDecoder.decode(page, VecType.INT64, dictVb, arena)
        for (i <- ids.indices) {
          assert(vb.isNull(i) == !p(i))
          if (p(i)) assert(vb.getLong(i) == dict(ids(i)))
        }
      }
    } finally arena.close()
  }

  test("RLE_DICTIONARY UTF8 then PLAIN over the same column (dict->PLAIN fallback mid-column)") {
    val dict = Array("apple", "banana", "cherry", "date").map(_.getBytes(StandardCharsets.UTF_8))
    val ids = Array(3, 0, 1, 2, 0, 3)
    val p1 = Array(true, true, false, true, true, true)
    val plainVals = Array("elderberry", "", "fig", "grape").map(_.getBytes(StandardCharsets.UTF_8))
    val p2 = Array(true, true, false, true)
    val arena = Arena.ofConfined()
    try {
      val dictVb = utf8Dict(dict, arena)
      val dictPage = v1(dictIds(ids, p1, idBitWidth(dict.length - 1)), p1, 1, Encoding.RLE_DICTIONARY, arena)
      val vb1 = ParquetPageDecoder.decode(dictPage, VecType.UTF8, dictVb, arena)
      for (i <- ids.indices) {
        assert(vb1.isNull(i) == !p1(i))
        if (p1(i)) assert(vb1.getUtf8Bytes(i).sameElements(dict(ids(i))))
      }
      val plainPage = v1(plainBinaries(plainVals, p2), p2, 1, Encoding.PLAIN, arena)
      val vb2 = ParquetPageDecoder.decode(plainPage, VecType.UTF8, null, arena)
      for (i <- plainVals.indices) {
        assert(vb2.isNull(i) == !p2(i))
        if (p2(i)) assert(vb2.getUtf8Bytes(i).sameElements(plainVals(i)))
      }
    } finally arena.close()
  }

  test("all-null page decodes to a fully-null column") {
    val v = Array.fill(20)(0)
    val p = Array.fill(20)(false)
    val arena = Arena.ofConfined()
    try {
      val vb = ParquetPageDecoder.decode(v1(plainInts(v, p), p, 1, Encoding.PLAIN, arena), VecType.INT32, null, arena)
      assert(vb.length() == 20)
      assert(vb.nullCount() == 20)
    } finally arena.close()
  }
}
