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

  @volatile private var installed = false

  /** Whether the pool charges task accounts on this executor. */
  def isInstalled: Boolean = installed

  /** Points the pool at the task accounts; once per executor, when the feature is on. */
  def install(): Unit = { NativePool.setAccountSource(() => current()); installed = true }

  /** Stops charging; arenas already open keep their account. */
  def uninstall(): Unit = { NativePool.setAccountSource(null); installed = false }

  /**
   * An operator's reservation of native memory, made before it allocates: `hold` returns false when the memory
   * manager grants less, which is where an operator seals and spills. The `spill` function given at creation is
   * what the operator gives up when another consumer of the task needs memory (0 when it cannot); it is never
   * called from the operator's own `hold`.
   */
  trait Reservation {
    def held: Long
    def hold(bytes: Long): Boolean
    def releaseTo(bytes: Long): Unit
  }

  /**
   * The reservation for an operator of the task on this thread, null off a task thread. With the pool's accounting
   * on it is part of the task's account, so the bytes it reserves and the blocks it then takes from the pool are the
   * same bytes, counted once. Otherwise it is a consumer of its own in `mode`, as the operators' were before.
   */
  def reservation(mode: MemoryMode, spill: Long => Long): Reservation = {
    val tc = TaskContext.get()
    if (tc == null || tc.taskMemoryManager() == null) return null
    if (installed) current().asInstanceOf[TaskAccount].newReservation(spill)
    else new ConsumerReservation(tc.taskMemoryManager(), mode, spill)
  }

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

  /** A reservation as a consumer of its own: the operators' accounting before #869. */
  private final class ConsumerReservation(tmm: TaskMemoryManager, mode: MemoryMode, onSpill: Long => Long)
      extends Reservation {
    private val consumer = new MemoryConsumer(tmm, tmm.pageSizeBytes(), mode) {
      override def spill(size: Long, trigger: MemoryConsumer): Long = if (trigger eq this) 0L else onSpill(size)
    }
    private var reserved = 0L
    def held: Long = reserved
    def hold(bytes: Long): Boolean = {
      if (bytes <= reserved) return true
      reserved += consumer.acquireMemory(bytes - reserved)
      reserved >= bytes
    }
    def releaseTo(bytes: Long): Unit = if (reserved > bytes) {
      val give = reserved - math.max(bytes, 0L)
      consumer.freeMemory(give)
      reserved -= give
    }
  }

  /**
   * The account of one task attempt: one consumer holding `max(reserved, charged)` -- the operators' reservations
   * and the pool blocks the task holds, mostly the same bytes -- plus up to two chunks of slack.
   *
   * When the memory manager asks it to spill, its reservations are asked in turn, except the one whose `hold` is
   * acquiring (an operator seals and spills itself on a refusal), and none while a pool block is being charged (that
   * may be from inside an operator). Calls come from the task's thread; `charge` and `credit` may also come from
   * another (an arena closed elsewhere) and are serialised with them.
   */
  private[vecruntime] final class TaskAccount(tmm: TaskMemoryManager, mode: MemoryMode) extends NativePool.Account {
    private val consumer = new MemoryConsumer(tmm, tmm.pageSizeBytes(), mode) {
      override def spill(size: Long, trigger: MemoryConsumer): Long = spillFor(size)
    }
    private val reservations = new java.util.ArrayList[AccountReservation]()
    private val NoSpill = new Object
    private var reserved = 0L
    private var charged = 0L
    private var acquired = 0L
    private var shortfall = 0L
    // The reservation whose hold is acquiring (never asked to spill), or NoSpill while a block is charged.
    private var asking: AnyRef = null

    /** Bytes the task's open arenas hold. */
    def chargedBytes: Long = synchronized(charged)

    /** Bytes the operators reserved. */
    def reservedBytes: Long = synchronized(reserved)

    /** Bytes held in the memory manager for both. */
    def acquiredBytes: Long = synchronized(acquired)

    private[TaskNativeMemory] def newReservation(spill: Long => Long): Reservation = synchronized {
      val r = new AccountReservation(spill)
      reservations.add(r)
      r
    }

    private def spillFor(size: Long): Long = {
      if (asking eq NoSpill) return 0L
      var freed = 0L
      val it = new java.util.ArrayList[AccountReservation](reservations).iterator()
      while (freed < size && it.hasNext) {
        val r = it.next()
        if (!(r eq asking)) freed += r.onSpill(size - freed)
      }
      freed
    }

    /** Brings `acquired` up to `target` as far as the memory manager grants, at least `step` at a time. */
    private def acquireTo(target: Long, who: AnyRef, step: Long): Unit = if (target > acquired) {
      val prior = asking
      asking = who
      try acquired += consumer.acquireMemory(math.max(target - acquired, step))
      finally asking = prior
    }

    /** Gives back what is held past the larger of the two counts, keeping one chunk of slack. */
    private def trim(): Unit = {
      val keep = math.max(math.max(reserved, charged), 0L) + Chunk
      if (acquired > keep + Chunk) {
        consumer.freeMemory(acquired - keep)
        acquired = keep
      }
    }

    override def charge(bytes: Long): Unit = synchronized {
      charged += bytes
      acquireTo(charged, NoSpill, Chunk)
      if (charged > acquired) shortfall = math.max(shortfall, charged - acquired)
    }

    override def credit(bytes: Long): Unit = synchronized {
      charged -= bytes
      trim()
    }

    /** The task ended: everything back to the memory manager; what is still charged is a leak. */
    private[vecruntime] def complete(): Unit = synchronized {
      if (charged > 0) leaked.addAndGet(charged)
      if (shortfall > 0) overdraft.addAndGet(shortfall)
      if (acquired > 0) consumer.freeMemory(acquired)
      acquired = 0L
      reserved = 0L
      reservations.clear()
    }

    private final class AccountReservation(val onSpill: Long => Long) extends Reservation {
      private var mine = 0L
      def held: Long = TaskAccount.this.synchronized(mine)

      /** Raises this reservation to `bytes`; false, keeping what was granted, when the manager grants less. */
      def hold(bytes: Long): Boolean = TaskAccount.this.synchronized {
        if (bytes <= mine) return true
        val others = reserved - mine
        acquireTo(math.max(others + bytes, charged), this, 0L)
        val room = acquired - others
        val granted = math.max(mine, math.min(bytes, room))
        reserved += granted - mine
        mine = granted
        mine >= bytes
      }

      def releaseTo(bytes: Long): Unit = TaskAccount.this.synchronized {
        if (mine > bytes) {
          val to = math.max(bytes, 0L)
          reserved -= mine - to
          mine = to
          trim()
        }
      }
    }
  }
}
