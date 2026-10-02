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

import java.lang.foreign.Arena
import java.nio.charset.StandardCharsets
import java.util.function.IntFunction

import scala.util.Random

import io.vecruntime.kernels.{ArrowLayout, SegmentVectorBuffers, VecType, VectorBuffers}
import io.vecruntime.kernels.parquet.{ColumnChunkDecoder, DeltaBinaryPackedReader, GroupUnpacker, ParquetPageDecoder}
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.values.bitpacking.Packer
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter
import org.apache.parquet.column.values.delta.{
  DeltaBinaryPackingValuesWriterForInteger,
  DeltaBinaryPackingValuesWriterForLong
}
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.io.api.Binary
import org.scalatest.funsuite.AnyFunSuite

/**
 * Cross-checks the v2 value decoders (#559) against streams written by parquet-java's own writers:
 * [[DeltaBinaryPackedReader]] against `DELTA_BINARY_PACKED` ([[DeltaBinaryPackingValuesWriterForInteger]] /
 * `ForLong`), with the `BytePacker` unpacker the scan node injects and with the built-in scalar one, and
 * [[ColumnChunkDecoder]] against `DELTA_LENGTH_BYTE_ARRAY` and `BYTE_STREAM_SPLIT` pages. The kernels tests pin the
 * reader against a from-scratch encoder of the spec; this pins it against the writer whose bytes Spark's
 * `parquet.writer.version=v2` files carry.
 */
class DeltaBinaryPackedCrossCheckSuite extends AnyFunSuite {

  @SuppressWarnings(Array("deprecation"))
  private val bytePackers: IntFunction[GroupUnpacker] = new IntFunction[GroupUnpacker] {
    private val cache = new Array[GroupUnpacker](33)
    override def apply(w: Int): GroupUnpacker = {
      if (cache(w) == null) {
        val p = Packer.LITTLE_ENDIAN.newBytePacker(w)
        cache(w) = (src: Array[Byte], sp: Int, dst: Array[Int], dp: Int) => p.unpack8Values(src, sp, dst, dp)
      }
      cache(w)
    }
  }

  private val layouts = Seq((128, 4), (256, 8), (64, 2))
  private val lengths = Seq(0, 1, 2, 31, 32, 33, 127, 128, 129, 1000, 5003)

  private def intStream(v: Array[Int], block: Int, minis: Int): Array[Byte] = {
    val w =
      new DeltaBinaryPackingValuesWriterForInteger(block, minis, 64, 1 << 20, HeapByteBufferAllocator.getInstance())
    v.foreach(w.writeInteger)
    w.getBytes.toByteArray
  }

  private def longStream(v: Array[Long], block: Int, minis: Int): Array[Byte] = {
    val w = new DeltaBinaryPackingValuesWriterForLong(block, minis, 64, 1 << 20, HeapByteBufferAllocator.getInstance())
    v.foreach(w.writeLong)
    w.getBytes.toByteArray
  }

  private def intShapes(rnd: Random, n: Int): Seq[(String, Array[Int])] = Seq(
    "sorted" -> Array.tabulate(n)(i => 1000 + 3 * i),
    "small" -> Array.fill(n)(rnd.nextInt(1000)),
    "full-range" -> Array.fill(n)(rnd.nextInt()),
    "extremes" -> Array.tabulate(n)(i => if (i % 2 == 0) Int.MinValue else Int.MaxValue),
    "dates" -> Array.tabulate(n)(i => 10957 + (i % 3000)) // DATE days, with a reset every 3000
  )

  private def longShapes(rnd: Random, n: Int): Seq[(String, Array[Long])] = Seq(
    "timestamps" -> Array.tabulate(n)(i => 1700000000000000L + 1000L * i),
    "44-bit" -> Array.fill(n)(rnd.nextLong() >> 20),
    "full-range" -> Array.fill(n)(rnd.nextLong()),
    "extremes" -> Array.tabulate(n)(i => if (i % 2 == 0) Long.MinValue else Long.MaxValue)
  )

  test("INT32 streams from parquet-java's writer decode identically, BytePacker and scalar unpack") {
    val rnd = new Random(5597)
    for ((block, minis) <- layouts; n <- lengths; (shape, v) <- intShapes(rnd, n); fast <- Seq(true, false)) {
      val bytes = intStream(v, block, minis)
      val r = new DeltaBinaryPackedReader(bytes, 0, bytes.length, false, if (fast) bytePackers else null)
      val got = new Array[Int](n)
      // Read in uneven chunks so a read stops inside a miniblock.
      var done = 0
      while (done < n) {
        val take = math.min(n - done, 1 + rnd.nextInt(97))
        r.readInts(got, done, take)
        done += take
      }
      assert(got.sameElements(v), s"layout $block/$minis n=$n shape=$shape fast=$fast")
    }
  }

  test("INT64 streams from parquet-java's writer decode identically, including widths above 32") {
    val rnd = new Random(5598)
    for ((block, minis) <- layouts; n <- lengths; (shape, v) <- longShapes(rnd, n); fast <- Seq(true, false)) {
      val bytes = longStream(v, block, minis)
      val r = new DeltaBinaryPackedReader(bytes, 0, bytes.length, true, if (fast) bytePackers else null)
      val got = new Array[Long](n)
      var done = 0
      while (done < n) {
        val take = math.min(n - done, 1 + rnd.nextInt(97))
        r.readLongs(got, done, take)
        done += take
      }
      assert(got.sameElements(v), s"layout $block/$minis n=$n shape=$shape fast=$fast")
    }
  }

  // ------------------------------------------------------------------- DELTA_LENGTH_BYTE_ARRAY, BYTE_STREAM_SPLIT

  private def decodeChunk(
      page: Array[Byte],
      n: Int,
      physical: VecType,
      lane: VecType,
      enc: ParquetPageDecoder.Encoding,
      batch: Int,
      arena: Arena
  ): VectorBuffers = {
    val d = new ColumnChunkDecoder(physical, lane, 0, batch, bytePackers)
    d.startChunk(n)
    val data = ArrowLayout.allocateData(arena, lane, math.max(n, 1))
    var done = 0
    while (done < n) {
      val want = math.min(batch, n - done)
      d.startBatch(want)
      var filled = 0
      while (filled < want) {
        if (d.needsPage()) d.feedPage(ColumnChunkDecoder.Page.v1(page, page.length, n, enc))
        filled += d.readBatchDirectA(want - filled, done + filled, data, null)
      }
      done += want
    }
    SegmentVectorBuffers.fixedWidth(lane, n, null, data)
  }

  test("DELTA_LENGTH_BYTE_ARRAY pages from parquet-java's writer decode identically") {
    val rnd = new Random(55912)
    for (n <- lengths; batch <- Seq(64, 1000)) {
      val v = Array.tabulate(n) { i =>
        (i % 5 match {
          case 0 => ""
          case 1 => s"héllo $i"
          case 2 => "y" * rnd.nextInt(700)
          case _ => s"v${rnd.nextInt()}"
        }).getBytes(StandardCharsets.UTF_8)
      }
      val w = new DeltaLengthByteArrayValuesWriter(64, 1 << 20, HeapByteBufferAllocator.getInstance())
      v.foreach(b => w.writeBytes(Binary.fromConstantByteArray(b)))
      val page = w.getBytes.toByteArray
      val d = new ColumnChunkDecoder(VecType.UTF8, 0, batch, bytePackers)
      d.startChunk(n)
      var row = 0
      while (row < n) {
        val want = math.min(batch, n - row)
        d.startBatch(want)
        var filled = 0
        while (filled < want) {
          if (d.needsPage()) {
            d.feedPage(ColumnChunkDecoder.Page.v1(
              page,
              page.length,
              n,
              ParquetPageDecoder.Encoding.DELTA_LENGTH_BYTE_ARRAY
            ))
          }
          filled += d.readBatch(want - filled, filled)
        }
        val arena = Arena.ofConfined()
        try {
          val offsets = ArrowLayout.allocateOffsets(arena, want)
          val bytes = ArrowLayout.allocateBytes(arena, math.max(d.utf8Bytes(), 1L))
          d.flushUtf8(want, offsets, bytes, null)
          val col = SegmentVectorBuffers.utf8(want, null, offsets, bytes)
          for (k <- 0 until want) {
            assert(col.getUtf8Bytes(k).sameElements(v(row + k)), s"n=$n batch=$batch row ${row + k}")
          }
        } finally arena.close()
        row += want
      }
    }
  }

  test("BYTE_STREAM_SPLIT pages from parquet-java's writers decode identically (INT32, INT64, DOUBLE)") {
    val rnd = new Random(55913)
    val alloc = HeapByteBufferAllocator.getInstance()
    for (n <- lengths; batch <- Seq(64, 1000)) {
      val arena = Arena.ofConfined()
      try {
        val ints = Array.tabulate(n)(i => if (i % 7 == 0) Int.MinValue else rnd.nextInt())
        val wi = new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(64, 1 << 20, alloc)
        ints.foreach(wi.writeInteger)
        val pi = wi.getBytes.toByteArray
        val gotI =
          decodeChunk(pi, n, VecType.INT32, VecType.INT32, ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT, batch, arena)
        for (i <- 0 until n) assert(gotI.getInt(i) == ints(i), s"INT32 n=$n row $i")
        val widened =
          decodeChunk(pi, n, VecType.INT32, VecType.INT64, ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT, batch, arena)
        for (i <- 0 until n) assert(widened.getLong(i) == ints(i).toLong, s"INT32->INT64 n=$n row $i")

        val longs = Array.tabulate(n)(i => if (i % 7 == 0) Long.MaxValue else rnd.nextLong())
        val wl = new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(64, 1 << 20, alloc)
        longs.foreach(wl.writeLong)
        val gotL = decodeChunk(
          wl.getBytes.toByteArray,
          n,
          VecType.INT64,
          VecType.INT64,
          ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT,
          batch,
          arena
        )
        for (i <- 0 until n) assert(gotL.getLong(i) == longs(i), s"INT64 n=$n row $i")

        val specials = Array(Double.NaN, -0.0, Double.PositiveInfinity, Double.MinPositiveValue)
        val doubles = Array.tabulate(n)(i => if (i % 9 == 0) specials(i % specials.length) else rnd.nextGaussian())
        val wd = new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(64, 1 << 20, alloc)
        doubles.foreach(wd.writeDouble)
        val gotD = decodeChunk(
          wd.getBytes.toByteArray,
          n,
          VecType.FLOAT64,
          VecType.FLOAT64,
          ParquetPageDecoder.Encoding.BYTE_STREAM_SPLIT,
          batch,
          arena
        )
        for (i <- 0 until n) {
          assert(
            java.lang.Double.doubleToRawLongBits(gotD.getDouble(i)) == java.lang.Double.doubleToRawLongBits(doubles(i)),
            s"DOUBLE n=$n row $i"
          )
        }
      } finally arena.close()
    }
  }
}
