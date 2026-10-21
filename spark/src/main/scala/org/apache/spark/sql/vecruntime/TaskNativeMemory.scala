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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import io.vecruntime.kernels.NativePool
import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/**
 * #869 step 3b: every block a task takes from the [[NativePool]] counted in that task's Spark execution memory,
 * in one place, so an allocation no operator reserved for is still in the budget.
 *
 * A pooled arena opened on a task thread charges the task's account (the account is looked up when the arena
 * opens, so a close on another thread credits the right task). The account holds the bytes as a
 * [[MemoryConsumer]] in the sort's memory mode -- OFF_HEAP when `spark.memory.offHeap.enabled` -- acquiring in
 * [[TaskNativeMemory.Chunk]] steps as the task's blocks grow and giving back what it holds past a chunk of slack as
 * they shrink. An allocation is never refused here: a block is already handed out when it is charged. What the
 * memory manager does not grant is recorded as overdraft (an executor metric), and the operators' own reservations,
 * made before they allocate, stay the place where a refusal turns into a spill.
 *
 * Off by default (`spark.vecruntime.memory.poolAccounting.enabled`): until the operators' own estimates move onto
 * it, turning it on counts their bytes twice.
 */
object TaskNativeMemory {

  /** The acquire and release step, so that each 64 KiB block is not a call into the memory manager. */
  val Chunk: Long = 4L << 20

  private val accounts = new ConcurrentHashMap[java.lang.Long, TaskAccount]()
  private val overdraft = new AtomicLong()
  private val leaked = new AtomicLong()

  /** Each task's largest shortfall (bytes charged past what the memory manager granted), summed so far. */
  def overdraftBytes: Long = overdraft.get()

  /** Bytes still charged when their task completed (an arena not closed), summed so far. */
  def leakedBytes: Long = leaked.get()

  /** Points the pool at the task accounts; once per executor, when the feature is on. */
  def install(): Unit = NativePool.setAccountSource(() => current())

  /** Stops charging; arenas already open keep their account. */
  def uninstall(): Unit = NativePool.setAccountSource(null)

  /** The account of the task running on this thread, opened on first use; null off a task thread. */
  def current(): NativePool.Account = {
    val tc = TaskContext.get()
    if (tc == null || tc.taskMemoryManager() == null) return null
    val id = java.lang.Long.valueOf(tc.taskAttemptId())
    val existing = accounts.get(id)
    if (existing != null) return existing
    val created = new TaskAccount(tc.taskMemoryManager(), VectorSortIterator.memoryMode())
    val prior = accounts.putIfAbsent(id, created)
    if (prior != null) return prior
    tc.addTaskCompletionListener[Unit] { _ =>
      accounts.remove(id)
      created.complete()
    }
    created
  }

  /** The account of one task attempt. Thread-safe: an arena may close on another thread than its task's. */
  private[vecruntime] final class TaskAccount(tmm: TaskMemoryManager, mode: MemoryMode) extends NativePool.Account {
    private val consumer = new MemoryConsumer(tmm, tmm.pageSizeBytes(), mode) {
      override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
    }
    private var charged = 0L
    private var acquired = 0L
    private var shortfall = 0L

    /** Bytes the task's open arenas hold. */
    def chargedBytes: Long = synchronized(charged)

    /** Bytes held in the memory manager for them. */
    def acquiredBytes: Long = synchronized(acquired)

    override def charge(bytes: Long): Unit = synchronized {
      charged += bytes
      if (charged > acquired) {
        val want = math.max(charged - acquired, Chunk)
        val got = consumer.acquireMemory(want)
        acquired += got
        if (charged > acquired) shortfall = math.max(shortfall, charged - acquired)
      }
    }

    override def credit(bytes: Long): Unit = synchronized {
      charged -= bytes
      val keep = math.max(charged, 0L) + Chunk
      if (acquired > keep + Chunk) {
        consumer.freeMemory(acquired - keep)
        acquired = keep
      }
    }

    /** The task ended: everything back to the memory manager; what is still charged is a leak. */
    private[vecruntime] def complete(): Unit = synchronized {
      if (charged > 0) leaked.addAndGet(charged)
      if (shortfall > 0) overdraft.addAndGet(shortfall)
      if (acquired > 0) consumer.freeMemory(acquired)
      acquired = 0L
    }
  }
}
