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
package io.vecruntime.spark.parquet

import java.nio.charset.StandardCharsets

import io.vecruntime.kernels.VecType
import io.vecruntime.kernels.parquet.ParquetPageDecoder
import io.vecruntime.spark.arrow.VectorAllocators
import org.apache.parquet.bytes.{BytesInput, HeapByteBufferAllocator}
import org.apache.parquet.column.{ColumnDescriptor, Encoding}
import org.apache.parquet.column.page.{DataPage, DataPageV1, DictionaryPage, PageReader}
import org.apache.parquet.column.values.plain.PlainValuesWriter
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.types.DataTypes
import org.scalatest.funsuite.AnyFunSuite

/**
 * Reader-level tests for [[NativeParquetColumnReader]]'s vector reuse across row groups: the reader owns
 * one Arrow FieldVector per column for the life of the file and recycles it (grow-only), the way Spark
 * recycles one ColumnarBatch. Drives the reader with a fake [[PageReader]] feeding pages built by
 * parquet-java's own writers (the bytes the real scan feeds it), across row groups of different sizes.
 *
 * Covered: reuse across bigger -> smaller -> bigger row groups (same vector instance, no allocateNew);
 * a with-nulls row group followed by a no-nulls one (stale validity tail must not leak); a UTF8 column
 * whose data buffer grows; and the validity tail beyond valueCount masked / not read.
 */
class NativeParquetColumnReaderSuite extends AnyFunSuite {

  private def rleEncode(levels: Array[Int], bitWidth: Int): Array[Byte] = {
    val enc = new RunLengthBitPackingHybridEncoder(bitWidth, 64, 1024, HeapByteBufferAllocator.getInstance())
    levels.foreach(enc.writeInt)
    enc.toBytes.toByteArray
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

  private def plainBinaries(values: Array[Array[Byte]], present: Array[Boolean]): Array[Byte] = {
    val w = new PlainValuesWriter(64, 1024, HeapByteBufferAllocator.getInstance())
    values.indices.foreach(i => if (present(i)) w.writeBytes(Binary.fromConstantByteArray(values(i))))
    w.getBytes.toByteArray
  }

  /** A v1 page body: int32 def-level byte-length prefix + RLE def levels + PLAIN value bytes. */
  private def v1Body(valueBytes: Array[Byte], present: Array[Boolean], maxDef: Int): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    if (maxDef > 0) {
      val bw = ParquetPageDecoder.bitWidth(maxDef)
      val lvl = rleEncode(present.map(p => if (p) maxDef else 0), bw)
      val len = lvl.length
      for (b <- 0 until 4) out.write((len >>> (8 * b)) & 0xff)
      out.write(lvl)
    }
    out.write(valueBytes)
    out.toByteArray
  }

  private def dataPageV1(body: Array[Byte], valueCount: Int): DataPageV1 =
    new DataPageV1(BytesInput.from(body), valueCount, body.length, null, Encoding.RLE, Encoding.RLE, Encoding.PLAIN)

  /** A PageReader that hands out one data page then nulls; no dictionary. */
  private def singlePage(page: DataPage, valueCount: Int): PageReader = new PageReader {
    private var served = false
    override def readDictionaryPage(): DictionaryPage = null
    override def getTotalValueCount: Long = valueCount.toLong
    override def readPage(): DataPage = if (served) null else { served = true; page }
  }

  private def descriptor(name: String, tpe: PrimitiveTypeName, maxDef: Int): ColumnDescriptor = {
    val prim: PrimitiveType =
      if (maxDef > 0) Types.optional(tpe).named(name) else Types.required(tpe).named(name)
    new ColumnDescriptor(Array(name), prim, 0, maxDef)
  }

  private def readIntRowGroup(reader: NativeParquetColumnReader, values: Array[Int], present: Array[Boolean]) = {
    // The column is optional (maxDef 1): Parquet writes def levels for every row even when all are present.
    val body = v1Body(plainInts(values, present), present, 1)
    reader.readRowGroup(singlePage(dataPageV1(body, values.length), values.length), values.length)
  }

  // ------------------------------------------------------------------- tests

  test("INT32 vector reused across bigger -> smaller -> bigger row groups") {
    val allocator = VectorAllocators.newChild("reader-reuse-i32")
    val reader = new NativeParquetColumnReader(
      descriptor("i", PrimitiveTypeName.INT32, 1),
      VecType.INT32,
      DataTypes.IntegerType,
      "i",
      allocator
    )
    try {
      def rg(n: Int, base: Int): (org.apache.arrow.vector.FieldVector, Array[Int], Array[Boolean]) = {
        val v = Array.tabulate(n)(k => base + k * 7 - 3)
        val p = Array.tabulate(n)(k => k % 5 != 0) // ~20% null
        (readIntRowGroup(reader, v, p), v, p)
      }
      val (v1, vals1, pres1) = rg(3000, 100)
      assertIntColumn(v1, vals1, pres1)
      val (v2, vals2, pres2) = rg(500, 9000) // smaller: same buffers, no realloc
      assert(v2 eq v1, "smaller row group must reuse the same vector instance")
      assertIntColumn(v2, vals2, pres2)
      val (v3, vals3, pres3) = rg(4000, 50000) // bigger: may realloc, still correct
      assertIntColumn(v3, vals3, pres3)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("with-nulls row group then no-nulls: stale validity tail does not leak") {
    val allocator = VectorAllocators.newChild("reader-reuse-nulls")
    val reader = new NativeParquetColumnReader(
      descriptor("i", PrimitiveTypeName.INT32, 1),
      VecType.INT32,
      DataTypes.IntegerType,
      "i",
      allocator
    )
    try {
      val vNull = Array.tabulate(2000)(k => k)
      val pNull = Array.tabulate(2000)(k => k % 3 != 0)
      assertIntColumn(readIntRowGroup(reader, vNull, pNull), vNull, pNull)
      // A no-null, SMALLER row group: reuses the vector; every row must read as valid despite the prior nulls.
      val vFull = Array.tabulate(700)(k => k * 2)
      val pFull = Array.fill(700)(true)
      val out = readIntRowGroup(reader, vFull, pFull)
      assert(out.getValueCount == 700)
      assert(out.getNullCount == 0, "no-null row group must report zero nulls after a with-null one")
      assertIntColumn(out, vFull, pFull)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("validity tail beyond valueCount is masked (a smaller all-present group after a larger nulls group)") {
    val allocator = VectorAllocators.newChild("reader-tail")
    val reader = new NativeParquetColumnReader(
      descriptor("i", PrimitiveTypeName.INT32, 1),
      VecType.INT32,
      DataTypes.IntegerType,
      "i",
      allocator
    )
    try {
      // First a large group with nulls near the end so the tail words carry cleared bits.
      val big = Array.tabulate(4096)(k => k)
      val bigP = Array.tabulate(4096)(k => k < 4000) // last 96 null
      assertIntColumn(readIntRowGroup(reader, big, bigP), big, bigP)
      // Then a small all-present group whose row count is not a multiple of 64 (130): its own validity
      // words are all-ones over [0,130); Arrow reads only valueCount rows, so nullCount must be 0.
      val small = Array.tabulate(130)(k => k + 1)
      val smallP = Array.fill(130)(true)
      val out = readIntRowGroup(reader, small, smallP)
      assert(out.getValueCount == 130)
      assert(out.getNullCount == 0)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("INT64 reuse across row groups") {
    val allocator = VectorAllocators.newChild("reader-reuse-i64")
    val reader = new NativeParquetColumnReader(
      descriptor("l", PrimitiveTypeName.INT64, 1),
      VecType.INT64,
      DataTypes.LongType,
      "l",
      allocator
    )
    try {
      def rg(n: Int, base: Long) = {
        val v = Array.tabulate(n)(k => base + k.toLong * 1000003L)
        val p = Array.tabulate(n)(k => k % 4 != 0)
        val body = v1Body(plainLongs(v, p), p, 1)
        val out = reader.readRowGroup(singlePage(dataPageV1(body, n), n), n)
        for (i <- 0 until n) {
          assert(out.isNull(i) == !p(i))
          if (p(i)) assert(out.asInstanceOf[org.apache.arrow.vector.BigIntVector].get(i) == v(i))
        }
        out
      }
      val a = rg(2500, 10L)
      val b = rg(400, 999L)
      assert(b eq a, "smaller INT64 row group reuses the vector")
      rg(3000, 5L)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("UTF8 reuse; the data buffer grows for a larger row group") {
    val allocator = VectorAllocators.newChild("reader-reuse-utf8")
    val reader = new NativeParquetColumnReader(
      descriptor("s", PrimitiveTypeName.BINARY, 1),
      VecType.UTF8,
      DataTypes.StringType,
      "s",
      allocator
    )
    try {
      def rg(strings: Array[String]): org.apache.arrow.vector.VarCharVector = {
        val p = strings.map(_ != null)
        val bytes = strings.map(s => if (s == null) Array.emptyByteArray else s.getBytes(StandardCharsets.UTF_8))
        val body = v1Body(plainBinaries(bytes, p), p, 1)
        val out = reader.readRowGroup(singlePage(dataPageV1(body, strings.length), strings.length), strings.length)
          .asInstanceOf[org.apache.arrow.vector.VarCharVector]
        for (i <- strings.indices) {
          assert(out.isNull(i) == (strings(i) == null))
          if (strings(i) != null) assert(new String(out.get(i), StandardCharsets.UTF_8) == strings(i))
        }
        out
      }
      // Small group of short strings, then a group whose total bytes are much larger (grows the data buffer).
      rg(Array("a", "bb", null, "ccc", "dddd"))
      val big = Array.tabulate(3000)(k => if (k % 7 == 0) null else ("value-" + k) * (1 + k % 4))
      rg(big)
      rg(Array("x", null, "y")) // smaller again: reuse
    } finally {
      reader.close()
      allocator.close()
    }
  }

  private def assertIntColumn(
      v: org.apache.arrow.vector.FieldVector,
      values: Array[Int],
      present: Array[Boolean]
  ): Unit = {
    assert(v.getValueCount == values.length)
    val iv = v.asInstanceOf[org.apache.arrow.vector.IntVector]
    for (i <- values.indices) {
      assert(iv.isNull(i) == !present(i), s"row $i null flag")
      if (present(i)) assert(iv.get(i) == values(i), s"row $i value")
    }
  }
}
