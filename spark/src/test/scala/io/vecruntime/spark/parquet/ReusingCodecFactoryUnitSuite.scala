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

import java.nio.ByteBuffer
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.hadoop.CodecFactory
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.funsuite.AnyFunSuite

/**
 * #735 unit tests, no Spark: the reused-buffer decompressor ({@link ReusingCodecFactory}) round-trips zstd and
 * snappy against parquet-java's default {@link CodecFactory} and grows its reused buffer; the pooled allocator
 * ({@link PooledHeapAllocator}) is bounded and reuses buffers.
 */
class ReusingCodecFactoryUnitSuite extends AnyFunSuite {

  private val conf = new Configuration(false)

  /** Compress `data` with parquet-java's own compressor for `codec`, so the decompressor reads real frames. */
  private def compress(codec: CompressionCodecName, data: Array[Byte]): Array[Byte] = {
    val factory = new CodecFactory(conf, 1 << 16)
    try factory.getCompressor(codec).compress(BytesInput.from(data)).toByteArray
    finally factory.release()
  }

  private def payload(seed: Long, len: Int): Array[Byte] = {
    val b = new Array[Byte](len)
    // Semi-compressible: a run of a few byte values, so zstd/snappy actually shrink it.
    val r = new Random(seed)
    var i = 0
    while (i < len) {
      b(i) = (r.nextInt(8)).toByte
      i += 1
    }
    b
  }

  for (codec <- Seq(CompressionCodecName.ZSTD, CompressionCodecName.SNAPPY)) {
    test(s"$codec: round trip matches the uncompressed bytes, buffer reused and grown across pages") {
      val factory = new ReusingCodecFactory(conf, 1 << 16)
      try {
        val dec = factory.getDecompressor(codec)
        // Pages of growing size: the first (treated as the chunk's dictionary) gets a fresh array; the rest
        // reuse one growable buffer, which must grow as the pages grow and still return the right bytes.
        val sizes = Seq(37, 500, 64 * 1024, 1024, 200 * 1024, 8)
        var prevReused: Array[Byte] = null
        sizes.zipWithIndex.foreach { case (len, idx) =>
          val original = payload(idx, len)
          val compressed = compress(codec, original)
          val out = dec.decompress(BytesInput.from(compressed), original.length)
          val got = out.toByteArray
          assert(got.length == original.length, s"$codec page $idx length")
          assert(java.util.Arrays.equals(got, original), s"$codec page $idx bytes")
        }
      } finally factory.release()
    }

    test(s"$codec: matches parquet-java's default decompressor byte for byte") {
      val original = payload(99, 50 * 1024)
      val compressed = compress(codec, original)
      val reusing = new ReusingCodecFactory(conf, 1 << 16)
      val default = new CodecFactory(conf, 1 << 16)
      try {
        val d = reusing.getDecompressor(codec)
        d.decompress(BytesInput.from(compressed), original.length) // first call = fresh (dictionary) array
        val ours = d.decompress(BytesInput.from(compressed), original.length).toByteArray
        val theirs = default.getDecompressor(codec).decompress(BytesInput.from(compressed), original.length).toByteArray
        assert(java.util.Arrays.equals(ours, original))
        assert(java.util.Arrays.equals(ours, theirs))
      } finally {
        reusing.release()
        default.release()
      }
    }
  }

  test("the reused data-page buffer is the SAME array across pages (so #786 reads it in place), dictionary apart") {
    val factory = new ReusingCodecFactory(conf, 1 << 16)
    try {
      val dec = factory.getDecompressor(CompressionCodecName.ZSTD)
      // The first decompress is treated as the chunk's dictionary and gets a fresh array; later data pages
      // share one reused buffer. #786 reads that buffer in place, so the identity below is the point of #735.
      val dictOut = dec.decompress(BytesInput.from(compress(CompressionCodecName.ZSTD, payload(1, 100))), 100)
      val a1 = backing(dec.decompress(BytesInput.from(compress(CompressionCodecName.ZSTD, payload(2, 100))), 100))
      val a2 = backing(dec.decompress(BytesInput.from(compress(CompressionCodecName.ZSTD, payload(3, 100))), 100))
      assert(a1 != null && (a1 eq a2), "two data pages share the reused backing array")
      assert(backing(dictOut) == null || (backing(dictOut) ne a1), "the dictionary page is a separate array")
    } finally factory.release()
  }

  /** The backing array of a BytesInput that is a view over one heap array, or null when it is not. */
  private def backing(in: BytesInput): Array[Byte] = {
    val b = in.toByteBuffer()
    if (b.hasArray) b.array() else null
  }

  test("PooledHeapAllocator is heap, bounded, and reuses a released buffer") {
    val pool = new PooledHeapAllocator(2, 1L << 20)
    assert(!pool.isDirect)
    val a = pool.allocate(1000)
    assert(!a.isDirect && a.capacity() >= 1000)
    pool.release(a)
    val b = pool.allocate(500)
    assert(b eq a, "a released buffer that fits is handed back")
    // Bound by buffer count: release three, only two are kept.
    val x = ByteBuffer.allocate(100)
    val y = ByteBuffer.allocate(100)
    val z = ByteBuffer.allocate(100)
    pool.release(b)
    pool.release(x)
    pool.release(y) // pool now full (2); z dropped
    pool.release(z)
    val got = Seq(pool.allocate(100), pool.allocate(100), pool.allocate(100))
    val reused = got.count(g => (g eq b) || (g eq x) || (g eq y) || (g eq z))
    assert(reused == 2, s"exactly two pooled buffers reused, got $reused")
  }

  test("PooledHeapAllocator honours the total byte budget") {
    val pool = new PooledHeapAllocator(10, 1500L) // room for one 1000-byte buffer, not two
    val a = pool.allocate(1000)
    val b = pool.allocate(1000)
    pool.release(a)
    pool.release(b) // over the byte budget, dropped
    val c = pool.allocate(1000)
    val d = pool.allocate(1000)
    assert((c eq a) || (c eq b), "the first pooled buffer is reused")
    assert(!(d eq a) && !(d eq b), "the second was not pooled (byte budget)")
  }
}
