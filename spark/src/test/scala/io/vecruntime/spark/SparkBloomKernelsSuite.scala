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
package io.vecruntime.spark

import java.io.ByteArrayOutputStream
import java.util.SplittableRandom

import io.vecruntime.kernels.SparkBloomKernels
import io.vecruntime.spark.expr.BloomFilterCache
import org.apache.spark.util.sketch.BloomFilter
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * SparkBloomKernels (#692): Spark's version-2 runtime bloom filter probed a batch at a time answers exactly what
 * Spark's own `mightContainLong` answers, for every key, hash count, size and seed.
 */
class SparkBloomKernelsSuite extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = BloomFilterCache.clearForTest()
  override def afterEach(): Unit = BloomFilterCache.clearForTest()

  private def bytesOf(f: BloomFilter): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    f.writeTo(out)
    out.toByteArray
  }

  /** Spark's filter over `keys` random longs, and probes of which about `hitPercent` are keys. */
  private def case_(numItems: Int, numBits: Long, seed: Int, hitPercent: Int, probes: Int, rnd: SplittableRandom) = {
    val f = BloomFilter.create(numItems.toLong, numBits, seed)
    val keys = Array.fill(numItems)(rnd.nextLong())
    keys.foreach(f.putLong)
    val items = Array.tabulate(probes) { _ =>
      if (rnd.nextInt(100) < hitPercent) keys(rnd.nextInt(keys.length)) else rnd.nextLong()
    }
    (f, items)
  }

  private def batch(p: SparkBloomKernels.Filter, items: Array[Long]): Array[Boolean] = {
    val n = items.length
    val out = new Array[Long]((n + 63) >>> 6 max 1)
    // Scratch dirty from an earlier batch, as a reused thread-local's is.
    java.util.Arrays.fill(out, -1L)
    SparkBloomKernels.probe(
      p,
      items,
      n,
      out,
      Array.fill(n max 1)(-7L),
      Array.fill(n max 1)(-3),
      Array.fill(n max 1)(-5)
    )
    Array.tabulate(n)(i => ((out(i >>> 6) >>> i) & 1L) != 0)
  }

  test("the batch probe answers as Spark's mightContainLong, bit for bit") {
    val rnd = new SplittableRandom(692)
    for {
      (numItems, numBits) <- Seq((10, 64L), (1000, 1L << 13), (5000, 40000L), (100000, 1L << 20), (2000, 1L << 22))
      seed <- Seq(0, 42, -1)
      hit <- Seq(0, 3, 50, 100)
      probes <- Seq(0, 1, 63, 64, 65, 4096)
    } {
      val (f, items) = case_(numItems, numBits, seed, hit, probes, rnd)
      val p = SparkBloomKernels.parse(bytesOf(f))
      assert(p != null)
      val expected = items.map(f.mightContainLong)
      assert(batch(p, items).toSeq === expected.toSeq, s"items=$numItems bits=$numBits seed=$seed hit=$hit n=$probes")
      assert(items.map(SparkBloomKernels.mightContainLong(p, _)).toSeq === expected.toSeq)
      // Every inserted key is found: no false negatives.
      if (hit == 100) assert(expected.forall(identity))
    }
  }

  test("extreme hash values and every hash count from 1 to 20 agree with Spark") {
    val items = Array(0L, -1L, 1L, Long.MinValue, Long.MaxValue, Int.MinValue.toLong, Int.MaxValue.toLong, 42L)
    (1 to 20).foreach { k =>
      // numItems chosen so Spark picks k hash functions: k = round(bits / items * ln 2).
      val bits = 1L << 12
      val n = math.max(1L, math.round(bits * math.log(2) / k))
      val f = BloomFilter.create(n, bits)
      (0L until n).foreach(i => f.putLong(i * 7919L - 3L))
      items.foreach(f.putLong)
      val probe = items ++ (0 until 200).map(i => i * 104729L + 11L)
      val p = SparkBloomKernels.parse(bytesOf(f))
      assert(batch(p, probe).toSeq === probe.map(f.mightContainLong).toSeq, s"k=$k (spark ${p.numHashFunctions})")
    }
  }

  test("a version-1 filter is not parsed and stays with Spark's own class") {
    val v1 = BloomFilter.create(BloomFilter.Version.V1, 100L, 1L << 10, 0)
    (0 until 100).foreach(i => v1.putLong(i.toLong))
    val bytes = bytesOf(v1)
    assert(SparkBloomKernels.parse(bytes) == null)
    assert(BloomFilterCache.getSpark(bytes) == null)
    assert((0 until 100).forall(i => BloomFilterCache.get(bytes).mightContainLong(i.toLong)))
    assert(SparkBloomKernels.parse(new Array[Byte](3)) == null)
  }

  test("the parsed filter is cached once per executor like Spark's") {
    val f = BloomFilter.create(1000L, 1L << 14)
    (0 until 1000).foreach(i => f.putLong(i.toLong))
    val bytes = bytesOf(f)
    val a = BloomFilterCache.getSpark(bytes.clone())
    val b = BloomFilterCache.getSpark(bytes.clone())
    assert(a eq b)
    assert(BloomFilterCache.buildCount == 1)
  }
}
