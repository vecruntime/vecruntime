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
package io.vecruntime.spark

import java.io.ByteArrayOutputStream
import java.util.concurrent.{Callable, Executors, TimeUnit}

import io.vecruntime.spark.expr.BloomFilterCache
import org.apache.spark.util.sketch.BloomFilter
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class BloomFilterCacheSuite extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = BloomFilterCache.clearForTest()
  override def afterEach(): Unit = BloomFilterCache.clearForTest()

  private def bytesOf(keys: Range, numBits: Long = 1L << 16): Array[Byte] = {
    val f = BloomFilter.create(keys.size.toLong, numBits)
    keys.foreach(k => f.putLong(k.toLong))
    val out = new ByteArrayOutputStream()
    f.writeTo(out)
    out.toByteArray
  }

  test("the same bytes, in separate copies as each task gets them, are deserialised once") {
    val a = bytesOf(0 until 1000)
    val f1 = BloomFilterCache.get(a.clone())
    val f2 = BloomFilterCache.get(a.clone())
    assert(f1 eq f2)
    assert(BloomFilterCache.buildCount == 1)
    assert((0 until 1000).forall(k => f1.mightContainLong(k.toLong)))
  }

  test("different contents are different entries, never a stale filter") {
    val f1 = BloomFilterCache.get(bytesOf(0 until 1000))
    val f2 = BloomFilterCache.get(bytesOf(5000 until 6000))
    assert(!(f1 eq f2))
    assert(BloomFilterCache.buildCount == 2)
    assert((5000 until 6000).forall(k => f2.mightContainLong(k.toLong)))
  }

  test("concurrent tasks for one filter share one deserialisation") {
    val a = bytesOf(0 until 1000)
    val pool = Executors.newFixedThreadPool(8)
    try {
      val got =
        (1 to 32).map(_ => pool.submit(new Callable[BloomFilter] { def call() = BloomFilterCache.get(a.clone()) }))
      val filters = got.map(_.get(30, TimeUnit.SECONDS))
      assert(filters.forall(_ eq filters.head))
      assert(BloomFilterCache.buildCount == 1)
    } finally pool.shutdownNow()
  }

  test("bounded by bytes: the least recently used filter goes first; one over the budget is not kept") {
    val a = bytesOf(0 until 100)
    val b = bytesOf(100 until 200)
    val c = bytesOf(200 until 300)
    BloomFilterCache.clearForTest(newBudget = a.length.toLong * 2)
    BloomFilterCache.get(a)
    BloomFilterCache.get(b)
    BloomFilterCache.get(a) // a is now the most recently used
    BloomFilterCache.get(c) // evicts b
    assert(BloomFilterCache.cachedBytes <= a.length.toLong * 2)
    val before = BloomFilterCache.buildCount
    BloomFilterCache.get(a) // still cached
    assert(BloomFilterCache.buildCount == before)
    BloomFilterCache.get(b) // was evicted: built again
    assert(BloomFilterCache.buildCount == before + 1)

    val big = bytesOf(0 until 100, numBits = 1L << 20)
    BloomFilterCache.clearForTest(newBudget = big.length.toLong - 1)
    val g = BloomFilterCache.get(big)
    assert((0 until 100).forall(k => g.mightContainLong(k.toLong)))
    assert(BloomFilterCache.buildCount == 0 && BloomFilterCache.cachedBytes == 0)
  }
}
