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
package org.apache.spark.sql.vecruntime

import org.apache.spark.SparkConf
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, UnifiedMemoryManager}
import org.scalatest.funsuite.AnyFunSuite

/**
 * #915: the scan's read-ahead takes execution memory only when it is free. Driven over a small on-heap pool: a grant
 * the pool can make is held and given back; one it can only make by spilling another consumer of the task is refused,
 * and that consumer is never asked to spill; in the operators' mode the task's share of the pool bounds it too.
 */
@io.vecruntime.spark.test.tags.Scan
class ReadAheadMemorySuite extends AnyFunSuite {

  private val Pool = 64L << 20

  /** A consumer of the task holding memory it would give up if asked, counting how often it is asked. */
  private final class Holder(tmm: TaskMemoryManager)
      extends MemoryConsumer(tmm, tmm.pageSizeBytes(), MemoryMode.ON_HEAP) {
    var spillCalls = 0
    def take(bytes: Long): Long = acquireMemory(bytes)
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      spillCalls += 1
      val give = math.min(size, getUsed)
      freeMemory(give)
      give
    }
  }

  private def withTask[T](cores: Int, operatorsMode: MemoryMode = MemoryMode.ON_HEAP)(
      body: (UnifiedMemoryManager, TaskMemoryManager, ReadAheadMemory) => T
  ): T = {
    val mm = new UnifiedMemoryManager(new SparkConf(false), Pool, 0L, cores)
    val tmm = new TaskMemoryManager(mm, 0L)
    val ra = new ReadAheadMemory(tmm, mm, MemoryMode.ON_HEAP, operatorsMode, cores)
    try body(mm, tmm, ra)
    finally {
      ra.releaseTo(0L)
      tmm.cleanUpAllAllocatedMemory()
    }
  }

  test("#915: a grant the pool can make is held, and given back") {
    withTask(cores = 1) { (mm, _, ra) =>
      assert(ra.tryHold(8L << 20))
      assert(ra.tryHoldTo(24L << 20))
      assert(ra.held === (24L << 20))
      assert(mm.onHeapExecutionMemoryUsed === (24L << 20))
      ra.release(8L << 20)
      assert(ra.held === (16L << 20))
      ra.releaseTo(0L)
      assert(mm.onHeapExecutionMemoryUsed === 0L)
    }
  }

  test("#915: read-ahead never makes another consumer of the task spill; it is refused instead") {
    withTask(cores = 1) { (_, tmm, ra) =>
      val sort = new Holder(tmm)
      assert(sort.take(Pool - (4L << 20)) === Pool - (4L << 20))
      val before = ReadAheadMemory.refusals
      assert(!ra.tryHold(8L << 20), "only 4 MiB free: refused")
      assert(sort.spillCalls === 0, "the other consumer was not asked to spill")
      assert(sort.getUsed === Pool - (4L << 20))
      assert(ra.held === 0L)
      assert(ReadAheadMemory.refusals === before + 1)
      assert(ra.tryHold(2L << 20), "what is free is still granted")
      sort.spill(Long.MaxValue, null)
      assert(ra.tryHold(8L << 20), "granted once the pool has room again")
    }
  }

  test("#915: in the operators' mode the task's share of the pool bounds read-ahead") {
    withTask(cores = 4) { (_, _, ra) =>
      // A quarter of the pool is the task's share; the pool itself has far more free.
      assert(ra.tryHoldTo(Pool / 4))
      assert(!ra.tryHold(1L << 20), "past the task's share")
    }
    withTask(cores = 4, operatorsMode = MemoryMode.OFF_HEAP) { (_, _, ra) =>
      // Not the operators' mode (the compressed pages of an off-heap run): free memory alone decides.
      assert(ra.tryHoldTo(Pool / 2))
    }
  }
}
