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

import io.vecruntime.spark.test.VectorQuerySuite

/**
 * #925: the executor memory gauges as per-query values -- a snapshot has every gauge, a reset restarts the peaks,
 * the probe job returns one snapshot per executor, and the summary takes each value's max and sum and the growth of
 * the cumulative counters.
 */
class MemoryProbeSuite extends VectorQuerySuite {

  test("#925: a snapshot names every gauge, and a reset restarts the native pool's peak") {
    val s = MemoryProbe.snapshot()
    Seq(
      "nativePoolInUse",
      "nativePoolPeak",
      "nativePoolRetained",
      "nativePoolOverdraft",
      "nativePoolLeaked",
      "pinned",
      "pinnedPeak",
      "arrow",
      "arrowPeakSinceStart",
      "prefetchPeak",
      "heapOldPeak",
      "heapOldAfterGc",
      "rss",
      "rssPeak"
    ).foreach(k => assert(s.contains(k), s"missing $k"))
    val pool = io.vecruntime.kernels.NativePool.shared()
    val arena = pool.newArena()
    arena.allocate(1L << 20, 64)
    arena.close()
    assert(pool.peakInUseBytes() >= (1L << 20))
    MemoryProbe.resetPeaks()
    assert(pool.peakInUseBytes() === pool.inUseBytes(), "the peak restarts from what is in use")
  }

  test("#925: the probe job returns a snapshot per executor, and the summary takes max, sum and growth") {
    val snaps = MemoryProbe.collect(spark.sparkContext, reset = true)
    assert(snaps.nonEmpty)
    assert(snaps.values.forall(_.contains("rssPeak")))
    val before = Map(
      "a" -> Map("nativePoolOverdraft" -> 10L, "rssPeak" -> 5L),
      "b" -> Map("nativePoolOverdraft" -> 0L, "rssPeak" -> 7L)
    )
    val after = Map(
      "a" -> Map("nativePoolOverdraft" -> 14L, "rssPeak" -> 6L),
      "b" -> Map("nativePoolOverdraft" -> 1L, "rssPeak" -> 9L)
    )
    val m = MemoryProbe.summarize(before, after)
    assert(m("rssPeakMax") === 9L)
    assert(m("rssPeakSum") === 15L)
    assert(m("nativePoolOverdraftMax") === 4L, "a cumulative counter reports its growth over the run")
    assert(m("nativePoolOverdraftSum") === 5L)
    assert(m("executors") === 2L)
    assert(MemoryProbe.summarize(before, Map.empty).isEmpty)
  }
}
