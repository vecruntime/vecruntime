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

import java.util.function.IntFunction

import scala.util.Random

import io.vecruntime.kernels.parquet.{DeltaBinaryPackedReader, GroupUnpacker}
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.values.bitpacking.Packer
import org.apache.parquet.column.values.delta.{
  DeltaBinaryPackingValuesWriterForInteger,
  DeltaBinaryPackingValuesWriterForLong
}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Cross-checks [[DeltaBinaryPackedReader]] (#559 slice 2) against `DELTA_BINARY_PACKED` streams written by
 * parquet-java's own writers ([[DeltaBinaryPackingValuesWriterForInteger]] / `ForLong`), with the
 * `BytePacker` unpacker the scan node injects and with the built-in scalar one. The kernels tests pin the
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
}
