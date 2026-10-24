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

import java.io.EOFException
import java.nio.ByteBuffer
import java.util.Random

import scala.jdk.CollectionConverters._

import org.apache.parquet.io.SeekableInputStream
import org.scalatest.funsuite.AnyFunSuite

/**
 * #834 unit tests, no Spark: the range planner coalesces gaps, splits long runs and keeps page-level ranges
 * (column index on) distinct; and the buffer-backed stream serves every byte of a whole object and of a set
 * of planned ranges exactly as the file holds them.
 */
@io.vecruntime.spark.test.tags.Scan
class ParquetReadRangePlannerSuite extends AnyFunSuite {

  private def r(off: Long, len: Long) = new ParquetReadRangePlanner.Range(off, len)
  private def plan(gap: Long, target: Long, tol: Double, in: ParquetReadRangePlanner.Range*) =
    new ParquetReadRangePlanner(gap, target, tol).plan(in.toList.asJava).asScala.toList

  test("adjacent and near ranges merge; a wide gap splits them") {
    // gap 0: only exactly-consecutive ranges merge, as parquet-java does on its own.
    assert(plan(0, 0, 1.0, r(0, 100), r(100, 50), r(200, 10)) == List(r(0, 150), r(200, 10)))
    // gap 64: the 50-byte hole between 150 and 200 is swallowed; the 1000-byte hole is not.
    assert(plan(64, 0, 1.0, r(0, 100), r(100, 50), r(200, 10), r(1210, 10)) == List(r(0, 210), r(1210, 10)))
  }

  test("ranges are sorted before coalescing; overlaps keep the farther end") {
    assert(plan(0, 0, 1.0, r(200, 10), r(0, 100), r(100, 50)) == List(r(0, 150), r(200, 10)))
    // An overlap (a shared region) does not shrink the run: end is the max.
    assert(plan(0, 0, 1.0, r(0, 100), r(50, 100)) == List(r(0, 150)))
  }

  test("a long coalesced run is split into target-sized parts, the short tail folded in") {
    // target 1000, tol 1.4: split above 1400; parts of 1000; a remainder <= target + minTail (1400) is the
    // last part, and a tail < minTail (400) folds onto the previous part instead of standing alone.
    // 2600 -> 1000 + 1000 + 600 (the 600 remainder is <= 1400, so it is the last part).
    assert(plan(0, 1000, 1.4, r(0, 2600)) == List(r(0, 1000), r(1000, 1000), r(2000, 600)))
    // 3600 -> 1000 + 1000 + 1000 + 600.
    assert(plan(0, 1000, 1.4, r(0, 3600)) == List(r(0, 1000), r(1000, 1000), r(2000, 1000), r(3000, 600)))
    // 1300 <= 1400 tolerance: left as one part.
    assert(plan(0, 1000, 1.4, r(0, 1300)) == List(r(0, 1300)))
    // 1350 -> a 350 tail (< minTail 400) folds onto the first part: one part of 1350 (<= 1400, whole anyway).
    assert(plan(0, 1000, 1.4, r(0, 1350)) == List(r(0, 1350)))
    // 1500 > 1400: splits, remainder 500 is the last part (500 <= 1400).
    assert(plan(0, 1000, 1.4, r(0, 1500)) == List(r(0, 1000), r(1000, 500)))
  }

  test("coalesce then split compose: merged run over small gaps is still split") {
    val out = plan(100, 1000, 1.4, r(0, 900), r(950, 900), r(1900, 900))
    // merged into [0, 2800) (two 50-byte gaps swallowed), then split 1000 + 1000 + 800.
    assert(out == List(r(0, 1000), r(1000, 1000), r(2000, 800)))
    assert(ParquetReadRangePlanner.totalBytes(out.asJava) == 2800)
  }

  test("page-level ranges (column index on) stay as separate requests when gaps are large") {
    // Selected pages scattered across a column chunk: with a small gap tolerance they are not all merged.
    val pages = List(r(0, 64), r(4096, 64), r(8192, 64), r(8256, 64))
    // gap 128: only the last two (adjacent) merge; the first two are 4 KiB apart.
    assert(plan(128, 0, 1.0, pages: _*) == List(r(0, 64), r(4096, 64), r(8192, 128)))
    // A tolerance above the page stride merges the scattered pages into one GET (fewer, larger requests).
    assert(plan(8192, 0, 1.0, pages: _*) == List(r(0, 8320)))
  }

  test("empty and zero-length ranges are dropped; empty input is empty") {
    assert(plan(0, 0, 1.0) == Nil)
    assert(plan(0, 0, 1.0, r(10, 0), r(20, 0)) == Nil)
    assert(plan(0, 0, 1.0, r(10, 0), r(20, 5)) == List(r(20, 5)))
  }

  test("every input byte lands in exactly one output range (the covering invariant), randomized") {
    val rnd = new Random(42)
    for (_ <- 0 until 300) {
      val gap = rnd.nextInt(4096).toLong
      val target = (rnd.nextInt(16) * 1000).toLong
      val n = rnd.nextInt(20)
      var off = rnd.nextInt(1000).toLong
      val in = (0 until n).map { _ =>
        val len = 1L + rnd.nextInt(5000)
        val range = r(off, len)
        off += len + rnd.nextInt(6000) // a gap after each
        range
      }.toList
      val out = plan(gap, target, 1.4, in: _*)
      // output sorted, non-overlapping
      out.sliding(2).foreach {
        case List(a, b) => assert(a.end() <= b.offset, s"overlap $a $b in $out")
        case _ =>
      }
      // every input byte covered by some output range
      in.foreach { rng =>
        var p = rng.offset
        while (p < rng.end()) {
          assert(out.exists(o => o.offset <= p && p < o.end()), s"byte $p of $rng uncovered by $out")
          p += 1
        }
      }
    }
  }

  // --------------------------------------------------------------- the buffer-backed stream over real bytes

  /** A seekable stream over an in-memory file that counts the ranged reads issued against it (seek+read runs). */
  private final class CountingStream(data: Array[Byte]) extends SeekableInputStream {
    var pos = 0L
    var reads = 0 // a read after a seek (or at a non-contiguous position) is one ranged request
    private var lastEnd = -1L
    override def getPos: Long = pos
    override def seek(p: Long): Unit = pos = p
    private def account(n: Int): Unit = {
      if (pos != lastEnd) reads += 1
      lastEnd = pos + n
    }
    override def read(): Int = {
      if (pos >= data.length) return -1
      account(1)
      val b = data(pos.toInt) & 0xff
      pos += 1
      b
    }
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (pos >= data.length) return -1
      val n = math.min(len, data.length - pos.toInt)
      account(n)
      System.arraycopy(data, pos.toInt, b, off, n)
      pos += n
      n
    }
    override def readFully(b: Array[Byte]): Unit = readFully(b, 0, b.length)
    override def readFully(b: Array[Byte], off: Int, len: Int): Unit = {
      account(len)
      System.arraycopy(data, pos.toInt, b, off, len)
      pos += len
    }
    override def read(buf: ByteBuffer): Int = {
      val n = math.min(buf.remaining(), data.length - pos.toInt)
      account(n)
      buf.put(data, pos.toInt, n)
      pos += n
      n
    }
    override def readFully(buf: ByteBuffer): Unit = {
      val n = buf.remaining()
      account(n)
      buf.put(data, pos.toInt, n)
      pos += n
    }
    override def close(): Unit = {}
  }

  private def bytes(n: Int): Array[Byte] = {
    val a = new Array[Byte](n)
    new Random(7).nextBytes(a)
    a
  }

  private def readAt(s: SeekableInputStream, off: Long, len: Int): Array[Byte] = {
    val b = new Array[Byte](len)
    s.seek(off)
    s.readFully(b)
    b
  }

  test("whole-object stream serves every byte from one GET") {
    val data = bytes(10000)
    val src = new CountingStream(data)
    val s = BufferBackedSeekableInputStream.wholeObject(src, data.length.toLong)
    assert(src.reads == 1, "the whole object is one ranged read")
    assert(s.bufferedBytes() == data.length)
    // random reads, all served from memory (the source is not touched again)
    val rnd = new Random(1)
    for (_ <- 0 until 200) {
      val off = rnd.nextInt(data.length)
      val len = math.min(1 + rnd.nextInt(500), data.length - off)
      assert(readAt(s, off.toLong, len).toSeq == data.slice(off, off + len).toSeq)
    }
    assert(src.reads == 1, "no further GETs after the whole-object load")
    // reading to the end then one more byte returns EOF
    s.seek(data.length.toLong)
    assert(s.read() == -1)
  }

  test("planned-ranges stream serves bytes inside the ranges from the buffers, misses fall through") {
    val data = bytes(20000)
    val src = new CountingStream(data)
    // Two coalesced ranges (as the planner would produce): [1000,3000) and [8000,12000).
    val ranges = List(r(1000, 2000), r(8000, 4000)).asJava
    val delegate = new CountingStream(data)
    val s = BufferBackedSeekableInputStream.plannedRanges(src, ranges, data.length.toLong, delegate)
    assert(src.reads == 2, "two coalesced ranges are two ranged reads")
    assert(s.bufferedBytes() == 6000)
    // inside the ranges: served from memory, no delegate read
    assert(readAt(s, 1000L, 2000).toSeq == data.slice(1000, 3000).toSeq)
    assert(readAt(s, 8500L, 1000).toSeq == data.slice(8500, 9500).toSeq)
    assert(delegate.reads == 0, "reads inside the planned ranges never touch the delegate")
    // a miss (outside every range) falls through to the delegate, correctly
    assert(readAt(s, 15000L, 100).toSeq == data.slice(15000, 15100).toSeq)
    assert(delegate.reads == 1, "a miss is served by the delegate")
  }

  test("planned-ranges rejects overlapping or unsorted ranges") {
    val data = bytes(1000)
    intercept[java.io.IOException] {
      BufferBackedSeekableInputStream
        .plannedRanges(new CountingStream(data), List(r(100, 200), r(150, 50)).asJava, 1000L, null)
    }
  }

  test("a read outside any range with no delegate is an EOFException, not silent wrong bytes") {
    val data = bytes(5000)
    val s = BufferBackedSeekableInputStream
      .plannedRanges(new CountingStream(data), List(r(0, 1000)).asJava, 5000L, null)
    intercept[EOFException](readAt(s, 2000L, 10))
  }
}
