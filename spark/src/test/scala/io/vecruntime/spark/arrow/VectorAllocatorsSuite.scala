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
package io.vecruntime.spark.arrow

import io.vecruntime.spark.VectorConf
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

/** #868: the executor's cap on Arrow memory, and the numbers its metrics report. */
class VectorAllocatorsSuite extends AnyFunSuite {

  test("#868: past the executor's Arrow cap an allocation fails instead of growing the process") {
    VectorAllocators.setLimit(4L << 20)
    try {
      val child = VectorAllocators.newChild("VectorAllocatorsSuite")
      try {
        val held = child.buffer(2L << 20)
        try {
          assert(VectorAllocators.allocatedBytes() >= (2L << 20))
          assert(VectorAllocators.peakBytes() >= (2L << 20))
          intercept[OutOfMemoryException](child.buffer(4L << 20).close())
        } finally held.close()
      } finally child.close()
    } finally VectorAllocators.setLimit(0L)
    assert(VectorAllocators.limit() === Long.MaxValue, "0 removes the cap")
  }

  test("#868: spark.vecruntime.arrow.maxBytes reads size strings, 0 or unset for no cap") {
    assert(VectorConf.arrowMaxBytes(new SparkConf(false)) === 0L)
    assert(VectorConf.arrowMaxBytes(new SparkConf(false).set(VectorConf.ArrowMaxBytes, "2g")) === (2L << 30))
  }

  test("#869: Arrow buffers from the native pool: usable, counted by Arrow and the pool, reused once released") {
    val pool = io.vecruntime.kernels.NativePool.shared()
    val root = org.apache.arrow.memory.PooledRoot.create()
    try {
      val before = pool.inUseBytes()
      val vec = new org.apache.arrow.vector.BigIntVector("v", root)
      try {
        vec.allocateNew(100000)
        (0 until 100000).foreach(i => vec.set(i, i.toLong * 3))
        vec.setValueCount(100000)
        assert(vec.get(99999) === 299997L)
        assert(root.getAllocatedMemory >= 800000L, "Arrow counts the buffer")
        assert(pool.inUseBytes() - before >= 800000L, "and the pool holds it")
      } finally vec.close()
      assert(root.getAllocatedMemory === 0L)
      assert(pool.inUseBytes() === before, "released back to the pool")
      val reusedBefore = pool.reuseCount()
      val again = root.buffer(800000L)
      try assert(pool.reuseCount() > reusedBefore, "the released block is reused")
      finally again.close()
      assert(root.buffer(0L).capacity() === 0L, "an empty request is the shared empty buffer")
    } finally root.close()
  }

  test("#869: Arrow on the native pool by default; the pool's per-thread cache shape from the conf") {
    assert(VectorConf.arrowNativePoolEnabled(new SparkConf(false)))
    assert(!VectorConf.arrowNativePoolEnabled(new SparkConf(false).set(VectorConf.ArrowNativePoolEnabled, "false")))
    assert(VectorConf.nativePoolThreadCacheDepth(new SparkConf(false)) === 0)
    assert(VectorConf.nativePoolThreadCacheMaxBlockBytes(new SparkConf(false)) === (1L << 20))
    val big = new SparkConf(false)
      .set(VectorConf.NativePoolThreadCacheDepth, "8")
      .set(VectorConf.NativePoolThreadCacheMaxBlockBytes, "4m")
    assert(VectorConf.nativePoolThreadCacheDepth(big) === 8)
    assert(VectorConf.nativePoolThreadCacheMaxBlockBytes(big) === (4L << 20))
  }
}
