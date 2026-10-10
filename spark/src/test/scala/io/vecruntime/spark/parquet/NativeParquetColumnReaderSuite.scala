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

import java.nio.charset.StandardCharsets

import scala.collection.mutable.ArrayBuffer

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
 * Reader-level tests for [[NativeParquetColumnReader]]'s STREAMING decode: {@code startRowGroup} then a
 * sequence of {@code readBatch(n)} calls that each decode the next n rows straight into a batch-owned Arrow
 * vector, spanning pages, resuming mid-run, with the null count from the definition levels; released
 * vectors are pooled and reused. Drives the reader with a fake [[PageReader]] feeding pages built by
 * parquet-java's own writers (the bytes the real scan feeds it), across many pages per row group.
 *
 * Covered: a row group split into several pages consumed by batches whose size does not divide the page
 * size (page-spanning batches); a with-nulls batch followed by a no-nulls batch (no stale validity, no
 * getNullCount); UTF8 batches; and pool reuse (a released vector instance is handed back on the next batch).
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

  /** A PageReader that hands out a queue of data pages then nulls; no dictionary. */
  private def pagesOf(pages: Seq[DataPage], totalValues: Int): PageReader = new PageReader {
    private val it = pages.iterator
    override def readDictionaryPage(): DictionaryPage = null
    override def getTotalValueCount: Long = totalValues.toLong
    override def readPage(): DataPage = if (it.hasNext) it.next() else null
  }

  private def descriptor(name: String, tpe: PrimitiveTypeName, maxDef: Int): ColumnDescriptor = {
    val prim: PrimitiveType =
      if (maxDef > 0) Types.optional(tpe).named(name) else Types.required(tpe).named(name)
    new ColumnDescriptor(Array(name), prim, 0, maxDef)
  }

  /** Split [0,n) into `pageCount` contiguous int32 pages (parquet-java bodies), and drive readBatch. */
  private def intPages(values: Array[Int], present: Array[Boolean], pageCount: Int): Seq[DataPage] = {
    val n = values.length
    val bounds = (0 to pageCount).map(p => (n.toLong * p / pageCount).toInt)
    (0 until pageCount).map { p =>
      val s = bounds(p)
      val e = bounds(p + 1)
      val body = v1Body(plainInts(values.slice(s, e), present.slice(s, e)), present.slice(s, e), 1)
      dataPageV1(body, e - s)
    }
  }

  /** Read the whole row group in batches of `batchRows`, collecting (releasing) each batch vector. */
  private def readAllInt(
      reader: NativeParquetColumnReader,
      pages: Seq[DataPage],
      rows: Int,
      batchRows: Int
  ): (Array[Int], Array[Boolean], ArrayBuffer[org.apache.arrow.vector.FieldVector]) = {
    reader.startRowGroup(pagesOf(pages, rows), rows)
    val gotValues = new Array[Int](rows)
    val gotPresent = new Array[Boolean](rows)
    val seen = ArrayBuffer.empty[org.apache.arrow.vector.FieldVector]
    var done = 0
    while (done < rows) {
      val n = math.min(batchRows, rows - done)
      val fv = reader.readBatch(n)
      seen += fv
      val iv = fv.asInstanceOf[org.apache.arrow.vector.IntVector]
      assert(fv.getValueCount == n)
      for (i <- 0 until n) {
        gotPresent(done + i) = !iv.isNull(i)
        if (!iv.isNull(i)) gotValues(done + i) = iv.get(i)
      }
      reader.release(fv)
      done += n
    }
    (gotValues, gotPresent, seen)
  }

  // ------------------------------------------------------------------- tests

  test("INT32 batches span pages when the batch size does not divide the page size") {
    val allocator = VectorAllocators.newChild("reader-stream-i32")
    val reader = new NativeParquetColumnReader(
      descriptor("i", PrimitiveTypeName.INT32, 1),
      VecType.INT32,
      DataTypes.IntegerType,
      "i",
      384,
      allocator
    )
    try {
      val rows = 5000
      val values = Array.tabulate(rows)(k => k * 7 - 3)
      val present = Array.tabulate(rows)(k => k % 5 != 0)
      val (gotV, gotP, _) = readAllInt(reader, intPages(values, present, 5), rows, 384)
      for (i <- 0 until rows) {
        assert(gotP(i) == present(i), s"row $i null flag")
        if (present(i)) assert(gotV(i) == values(i), s"row $i value")
      }
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("released batch vectors are pooled and reused") {
    val allocator = VectorAllocators.newChild("reader-stream-pool")
    val reader = new NativeParquetColumnReader(
      descriptor("i", PrimitiveTypeName.INT32, 1),
      VecType.INT32,
      DataTypes.IntegerType,
      "i",
      512,
      allocator
    )
    try {
      val rows = 4096
      val values = Array.tabulate(rows)(k => k)
      val present = Array.fill(rows)(true)
      val (_, _, seen) = readAllInt(reader, intPages(values, present, 4), rows, 512)
      // With release-after-each-batch and equal-sized batches, the pool hands the same instance back.
      assert(seen.size > 1)
      assert(seen.toSet.size < seen.size, "a released vector instance must be reused from the pool")
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("with-nulls batch then no-nulls batch: no stale validity, null count from def levels") {
    val allocator = VectorAllocators.newChild("reader-stream-nulls")
    val reader = new NativeParquetColumnReader(
      descriptor("i", PrimitiveTypeName.INT32, 1),
      VecType.INT32,
      DataTypes.IntegerType,
      "i",
      2000,
      allocator
    )
    try {
      // One row group: first 2000 rows have nulls, next 700 have none. Batches of 2000 then 700.
      val rows = 2700
      val values = Array.tabulate(rows)(k => k)
      val present = Array.tabulate(rows)(k => if (k < 2000) k % 3 != 0 else true)
      reader.startRowGroup(pagesOf(intPages(values, present, 3), rows), rows)
      val b1 = reader.readBatch(2000)
      assert(b1.getValueCount == 2000)
      assert(b1.getNullCount > 0)
      reader.release(b1)
      val b2 = reader.readBatch(700)
      assert(b2.getValueCount == 700)
      assert(b2.getNullCount == 0, "no-null batch must report zero nulls after a with-null one")
      val iv = b2.asInstanceOf[org.apache.arrow.vector.IntVector]
      for (i <- 0 until 700) {
        assert(!iv.isNull(i))
        assert(iv.get(i) == values(2000 + i))
      }
      reader.release(b2)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("INT64 streaming across pages") {
    val allocator = VectorAllocators.newChild("reader-stream-i64")
    val reader = new NativeParquetColumnReader(
      descriptor("l", PrimitiveTypeName.INT64, 1),
      VecType.INT64,
      DataTypes.LongType,
      "l",
      333,
      allocator
    )
    try {
      val rows = 2500
      val v = Array.tabulate(rows)(k => 10L + k.toLong * 1000003L)
      val p = Array.tabulate(rows)(k => k % 4 != 0)
      val bounds = (0 to 4).map(x => (rows.toLong * x / 4).toInt)
      val pages = (0 until 4).map { x =>
        val s = bounds(x)
        val e = bounds(x + 1)
        dataPageV1(v1Body(plainLongs(v.slice(s, e), p.slice(s, e)), p.slice(s, e), 1), e - s)
      }
      reader.startRowGroup(pagesOf(pages, rows), rows)
      var done = 0
      while (done < rows) {
        val n = math.min(333, rows - done)
        val fv = reader.readBatch(n).asInstanceOf[org.apache.arrow.vector.BigIntVector]
        for (i <- 0 until n) {
          assert(fv.isNull(i) == !p(done + i))
          if (p(done + i)) assert(fv.get(i) == v(done + i))
        }
        reader.release(fv)
        done += n
      }
    } finally {
      reader.close()
      allocator.close()
    }
  }

  test("UTF8 streaming across pages") {
    val allocator = VectorAllocators.newChild("reader-stream-utf8")
    val reader = new NativeParquetColumnReader(
      descriptor("s", PrimitiveTypeName.BINARY, 1),
      VecType.UTF8,
      DataTypes.StringType,
      "s",
      200,
      allocator
    )
    try {
      val rows = 1500
      val strings = Array.tabulate(rows)(k => if (k % 7 == 0) null else ("value-" + k) * (1 + k % 3))
      val p = strings.map(_ != null)
      val bytes = strings.map(s => if (s == null) Array.emptyByteArray else s.getBytes(StandardCharsets.UTF_8))
      val bounds = (0 to 5).map(x => (rows.toLong * x / 5).toInt)
      val pages = (0 until 5).map { x =>
        val s = bounds(x)
        val e = bounds(x + 1)
        dataPageV1(v1Body(plainBinaries(bytes.slice(s, e), p.slice(s, e)), p.slice(s, e), 1), e - s)
      }
      reader.startRowGroup(pagesOf(pages, rows), rows)
      var done = 0
      while (done < rows) {
        val n = math.min(200, rows - done)
        val fv = reader.readBatch(n).asInstanceOf[org.apache.arrow.vector.VarCharVector]
        for (i <- 0 until n) {
          assert(fv.isNull(i) == (strings(done + i) == null))
          if (strings(done + i) != null) assert(new String(fv.get(i), StandardCharsets.UTF_8) == strings(done + i))
        }
        reader.release(fv)
        done += n
      }
    } finally {
      reader.close()
      allocator.close()
    }
  }
}
