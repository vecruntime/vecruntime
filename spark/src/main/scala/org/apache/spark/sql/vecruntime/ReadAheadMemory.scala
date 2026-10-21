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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryManager, MemoryMode, TaskMemoryManager}

/**
 * #915: the scan's read-ahead held in the task's Spark execution memory, granted only from memory that is free.
 *
 * Read-ahead is optional work: it must take what nobody else is using and give way when the operators above the scan
 * need the room, never make them spill for it. A plain `acquireMemory` would: when the pool is short, the task memory
 * manager spills the task's other consumers (a sort's runs, a join's build) to satisfy the request. So a grant is made
 * only when the pool has the bytes free now and, in the operators' memory mode, the task stays within its share of the
 * pool (`pool / spark.executor.cores`); otherwise it is refused and the read-ahead narrows its window. That is what
 * makes the window follow the stage: a wide scan stage with little else allocating gets a deep window, a narrow stage
 * whose join or aggregate holds the pool gets a shallow one. The read-ahead's fixed byte caps stay as the upper clamp.
 *
 * The holder never spills (its bytes are already in flight). Calls may come from the task thread and from a decode-ahead
 * producer: they are serialised.
 */
private[vecruntime] final class ReadAheadMemory(
    tmm: TaskMemoryManager,
    mm: MemoryManager,
    val mode: MemoryMode,
    operatorsMode: MemoryMode,
    cores: Int
) {

  private val consumer = new MemoryConsumer(tmm, tmm.pageSizeBytes(), mode) {
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }
  private var heldBytes = 0L

  /** Bytes held in the memory manager. */
  def held: Long = synchronized(heldBytes)

  /**
   * The bytes the pool can grant this task now without any consumer spilling: the pool's free bytes (storage that
   * could be evicted is not counted as free), and in the operators' mode also what is left of the task's share, so
   * the task memory manager grants from free memory (past the task's fair share is where it spills the task's other
   * consumers).
   */
  def grantable: Long = {
    // maxXStorageMemory is the pool less what execution uses; less what storage uses, it is what is free.
    val (free, used) = mode match {
      case MemoryMode.OFF_HEAP =>
        (
          mm.maxOffHeapStorageMemory - mm.offHeapStorageMemoryUsed,
          mm.offHeapExecutionMemoryUsed + mm.offHeapStorageMemoryUsed
        )
      case _ =>
        (
          mm.maxOnHeapStorageMemory - mm.onHeapStorageMemoryUsed,
          mm.onHeapExecutionMemoryUsed + mm.onHeapStorageMemoryUsed
        )
    }
    if (mode != operatorsMode) return math.max(0L, free)
    val share = (free + used) / math.max(1, cores)
    math.max(0L, math.min(free, share - tmm.getMemoryConsumptionForThisTask))
  }

  /**
   * Raises the holding to `target` if the pool can grant the difference without making anyone spill; false, holding
   * what it held, when it cannot (counted in [[ReadAheadMemory.refusals]]).
   */
  def tryHoldTo(target: Long): Boolean = synchronized {
    val more = target - heldBytes
    if (more <= 0) return true
    if (more > grantable) { ReadAheadMemory.refused.incrementAndGet(); return false }
    val got = consumer.acquireMemory(more)
    if (got < more) {
      // The pool moved between the check and the grant: the partial grant goes back, the item is not read ahead.
      consumer.freeMemory(got)
      ReadAheadMemory.refused.incrementAndGet()
      false
    } else {
      heldBytes += got
      ReadAheadMemory.noteHeld(got)
      true
    }
  }

  /** Holds `bytes` more, if granted (see [[tryHoldTo]]). */
  def tryHold(bytes: Long): Boolean = synchronized(tryHoldTo(heldBytes + bytes))

  /** Lowers the holding to `target` (0: everything back). */
  def releaseTo(target: Long): Unit = synchronized {
    val give = heldBytes - math.max(0L, target)
    if (give > 0) {
      consumer.freeMemory(give)
      heldBytes -= give
      ReadAheadMemory.inFlight.addAndGet(-give)
    }
  }

  /** Gives back `bytes` of the holding. */
  def release(bytes: Long): Unit = synchronized(releaseTo(heldBytes - bytes))
}

private[vecruntime] object ReadAheadMemory {

  private val refused = new AtomicLong()
  private val inFlight = new AtomicLong()
  private val peak = new AtomicLong()

  /** Read-ahead grants refused in this JVM so far: how often a window narrowed for memory. */
  def refusals: Long = refused.get()

  /** Read-ahead bytes held in execution memory in this JVM now, and the most at once. */
  def heldBytes: Long = inFlight.get()
  def peakBytes: Long = peak.get()
  def resetPeak(): Unit = peak.set(inFlight.get())

  private def noteHeld(bytes: Long): Unit = peak.accumulateAndGet(inFlight.addAndGet(bytes), math.max)

  /**
   * The holder for the task on `context` in `mode`, given back when the task completes; null without a task, a memory
   * manager or a SparkEnv (a reader driven outside a task, in a test): its read-ahead is then bounded by the byte caps
   * alone, as before #915.
   */
  def apply(context: TaskContext, mode: MemoryMode): ReadAheadMemory = {
    val env = SparkEnv.get
    if (context == null || context.taskMemoryManager() == null || env == null) return null
    val m = new ReadAheadMemory(
      context.taskMemoryManager(),
      env.memoryManager,
      mode,
      VectorSortIterator.memoryMode(),
      env.conf.getInt("spark.executor.cores", 1)
    )
    context.addTaskCompletionListener[Unit](_ => m.releaseTo(0L))
    m
  }
}
