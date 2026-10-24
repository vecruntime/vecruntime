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
 * On by default (`spark.vecruntime.memory.poolAccounting.enabled`). The sort and the grace hash join reserve through
 * [[TaskNativeMemory.reservation]], so the bytes they reserve before allocating and the blocks they then take are one
 * budget; reservations for bytes that are not pool blocks (Arrow vectors, heap mirrors) use
 * [[TaskNativeMemory.ownReservation]].
 */
object TaskNativeMemory extends org.apache.spark.internal.Logging {

  /** The acquire and release step, so that each 64 KiB block is not a call into the memory manager. */
  val Chunk: Long = 4L << 20

  private val accounts = new ConcurrentHashMap[java.lang.Long, TaskAccount]()
  private val overdraft = new AtomicLong()
  private val leaked = new AtomicLong()

  /** Each task's largest shortfall (bytes charged past what the memory manager granted), summed so far. */
  def overdraftBytes: Long = overdraft.get()

  /** Bytes still charged when their task completed (an arena not closed), summed so far. */
  def leakedBytes: Long = leaked.get()

  /** #924: memory no task owns, charged to the executor's account now. */
  def executorBytes: Long = ExecutorAccount.chargedBytes

  /** #924: the executor account's largest shortfall: bytes past what storage memory granted. */
  def executorShortfallBytes: Long = ExecutorAccount.shortfallBytes

  /**
   * #940 debug mode: with `-Dvecruntime.pool.leakTrace=N` (N >= 1) one task attempt in N records where each of its
   * arenas was opened; at its completion the arenas still charged are logged, grouped by opening site, and an arena
   * closed after its task completed logs the thread and site that closed it. Off (0) by default: an untraced
   * account records nothing.
   */
  @volatile private[vecruntime] var leakTraceEvery: Int = Integer.getInteger("vecruntime.pool.leakTrace", 0)

  /** Frames of a site in the report: past the pool's own frames, enough to name the operator. */
  private val SiteFrames = 8

  private val tracedLeaks = new AtomicLong()

  /** Arenas a traced task left charged at its completion, so far (tests). */
  private[vecruntime] def tracedLeakedArenas: Long = tracedLeaks.get()

  private val lateCloses = new ConcurrentHashMap[String, AtomicLong]()

  /** Closing sites of arenas closed after their task completed, with their counts (tests, and the log). */
  private[vecruntime] def lateCloseSites: java.util.Map[String, AtomicLong] = lateCloses

  /** The frames of `t` from the first one outside the pool and this object, as one line per frame. */
  private[vecruntime] def site(t: Throwable): String =
    t.getStackTrace.iterator
      .dropWhile { f =>
        val c = f.getClassName
        c.startsWith("io.vecruntime.kernels.NativePool") || c.startsWith(
          "org.apache.spark.sql.vecruntime.TaskNativeMemory"
        ) ||
        c.startsWith("java.") || c.startsWith("jdk.")
      }
      .take(SiteFrames)
      .map(f => s"${f.getClassName}.${f.getMethodName}:${f.getLineNumber}")
      .mkString(" < ")

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
    val account = if (installed) accountOf(tc, mode) else null
    // An account in another mode than the operator's (an operator told its mode at plan time, say) cannot hold its
    // bytes: such an operator keeps a consumer of its own.
    if (account != null && account.mode == mode) account.newReservation(spill)
    else new ConsumerReservation(tc.taskMemoryManager(), mode, spill)
  }

  /** A reservation of its own for bytes that are not pool blocks, whatever the accounting; null off a task thread. */
  def ownReservation(mode: MemoryMode, spill: Long => Long): Reservation = {
    val tc = TaskContext.get()
    if (tc == null || tc.taskMemoryManager() == null) null
    else new ConsumerReservation(tc.taskMemoryManager(), mode, spill)
  }

  /** The account of the task running on this thread, opened on first use; null off a task thread. */
  def current(): NativePool.Account = {
    if (unaccountedDepth.get()(0) > 0) return executorOrNull
    val tc = TaskContext.get()
    if (tc == null || tc.taskMemoryManager() == null) executorOrNull
    else accountOf(tc, VectorSortIterator.memoryMode())
  }

  /**
   * #924 step 2: whether memory no task owns -- pool blocks and Arrow buffers opened off any task thread (the
   * Flight server's), and what [[unaccounted]] builds (a broadcast's shared table) -- is charged to the
   * [[ExecutorAccount]]. On by default; `-Dvecruntime.pool.executorAccount=false` leaves it uncharged, as before.
   */
  @volatile private[vecruntime] var executorAccountEnabled: Boolean =
    java.lang.Boolean.parseBoolean(System.getProperty("vecruntime.pool.executorAccount", "true"))

  private def executorOrNull: NativePool.Account = if (executorAccountEnabled) ExecutorAccount else null

  /**
   * #924 step 2: the executor's own account, for memory that outlives every task by design. Spark grants execution
   * memory only to a task attempt, so these bytes are held as off-heap (or on-heap, in the sort's mode) STORAGE
   * memory under a block id of their own -- the pool Spark already uses for memory that outlives tasks (cached and
   * broadcast blocks): it shrinks what execution can borrow exactly as a cached block does. Acquired in
   * [[Chunk]] steps as the bytes grow and given back past a chunk of slack as they shrink; what storage cannot grant
   * is recorded as overdraft, never refused (a block is already handed out when it is charged).
   */
  private[vecruntime] object ExecutorAccount extends NativePool.Account {
    private val blockId = org.apache.spark.storage.TestBlockId("vecruntime-executor-native-pool")
    private var charged = 0L
    private var acquired = 0L
    private var shortfall = 0L

    /** Bytes charged to the executor now. */
    def chargedBytes: Long = synchronized(charged)

    /** Bytes held in Spark's storage memory for them. */
    def acquiredBytes: Long = synchronized(acquired)

    /** The largest shortfall so far: bytes charged past what storage granted. */
    def shortfallBytes: Long = synchronized(shortfall)

    private def memoryManager: org.apache.spark.memory.MemoryManager = {
      val o = memoryManagerForTests
      if (o != null) return o
      val env = org.apache.spark.SparkEnv.get
      if (env == null) null else env.memoryManager
    }

    /** The memory manager to hold storage in instead of SparkEnv's (tests). */
    @volatile private[vecruntime] var memoryManagerForTests: org.apache.spark.memory.MemoryManager = null

    override def charge(bytes: Long): Unit = synchronized {
      charged += bytes
      if (charged > acquired) {
        val mm = memoryManager
        val step = math.max(charged - acquired, Chunk)
        if (mm != null && mm.acquireStorageMemory(blockId, step, VectorSortIterator.memoryMode())) acquired += step
        if (charged > acquired) shortfall = math.max(shortfall, charged - acquired)
      }
    }

    override def credit(bytes: Long): Unit = synchronized {
      charged -= bytes
      val keep = math.max(charged, 0L) + Chunk
      if (acquired > keep + Chunk) {
        val mm = memoryManager
        if (mm != null) mm.releaseStorageMemory(acquired - keep, VectorSortIterator.memoryMode())
        acquired = keep
      }
    }
  }

  /**
   * #940: opens the task's account before any operator registers a completion listener. Spark runs those
   * listeners in reverse order of registration, so an account opened lazily, on the first pooled block, was
   * completed BEFORE the operators that close their memory in their own listener (the aggregate's
   * accumulators): its memory counted as leaked although the same completion pass freed it a moment later.
   * Opened here, from the executor plugin's `onTaskStart`, the account's listener runs last.
   */
  def onTaskStart(): Unit = if (installed) current()

  private val unaccountedDepth = ThreadLocal.withInitial[Array[Int]](() => Array(0))

  /**
   * Runs `f` with no task account: what it allocates from the pool is the executor's, not this task's
   * (#940). For memory that outlives the task by design -- a broadcast's build table, shared by every task
   * of the executor and freed by its relation's cleaner -- charging the task that happened to build it
   * left those bytes charged at that task's completion (counted as leaked), and credited a finished task
   * when the cleaner closed them.
   */
  def unaccounted[T](f: => T): T = {
    val d = unaccountedDepth.get()
    d(0) += 1
    try f
    finally d(0) -= 1
  }

  /**
   * The task's account, opened in `mode` on first use. Keyed by attempt id and checked against the task's memory
   * manager: an entry left by another context with the same id (tests reuse ids) is replaced, not shared.
   */
  private def accountOf(tc: TaskContext, mode: MemoryMode): TaskAccount = {
    val id = java.lang.Long.valueOf(tc.taskAttemptId())
    val tmm = tc.taskMemoryManager()
    val existing = accounts.get(id)
    if (existing != null && (existing.tmm eq tmm)) return existing
    val every = leakTraceEvery
    val created = new TaskAccount(tmm, mode, traceArenas = every > 0 && tc.taskAttemptId() % every == 0)
    accounts.put(id, created)
    tc.addTaskCompletionListener[Unit] { _ =>
      accounts.remove(id, created)
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
  private[vecruntime] final class TaskAccount(
      val tmm: TaskMemoryManager,
      val mode: MemoryMode,
      traceArenas: Boolean = false
  ) extends NativePool.Account {
    // #940: the open arenas and where each was opened, in a traced task only.
    private val openArenas =
      if (traceArenas) new java.util.IdentityHashMap[java.lang.foreign.Arena, Throwable]() else null
    @volatile private var completed = false

    override def traced(): Boolean = traceArenas

    override def opened(arena: java.lang.foreign.Arena, where: Throwable): Unit =
      if (openArenas != null) synchronized(openArenas.put(arena, where))

    override def closed(arena: java.lang.foreign.Arena): Unit = if (openArenas != null) {
      val where = synchronized(openArenas.remove(arena))
      if (completed && where != null) {
        val key = s"[${Thread.currentThread().getName}] closed at ${site(new Throwable)}; opened at ${site(where)}"
        val n = lateCloses.computeIfAbsent(key, _ => new AtomicLong()).incrementAndGet()
        if (n == 1) logWarning(s"#940: a pool arena closed after its task completed: $key")
      }
    }
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
      if (charged > 0 && openArenas != null) reportOpen()
      completed = true
      if (charged > 0) leaked.addAndGet(charged)
      if (shortfall > 0) overdraft.addAndGet(shortfall)
      if (acquired > 0) consumer.freeMemory(acquired)
      acquired = 0L
      reserved = 0L
      reservations.clear()
    }

    /** #940: the arenas still charged, by opening site, the largest first. */
    private def reportOpen(): Unit = {
      val bySite = new java.util.HashMap[String, Array[Long]]() // site -> (arenas, bytes)
      val it = openArenas.entrySet().iterator()
      while (it.hasNext) {
        val e = it.next()
        val bytes = NativePool.heldBytes(e.getKey)
        if (bytes > 0) {
          val agg = bySite.computeIfAbsent(site(e.getValue), _ => new Array[Long](2))
          agg(0) += 1; agg(1) += bytes
          tracedLeaks.incrementAndGet()
        }
      }
      if (!bySite.isEmpty) {
        val lines = scala.jdk.CollectionConverters.MapHasAsScala(bySite).asScala.toSeq.sortBy(-_._2(1)).take(10).map {
          case (where, Array(n, b)) => s"  $n arenas, $b bytes: $where"
          case (where, _) => s"  ?: $where"
        }
        logWarning(
          s"#940: task's pool account completed with $charged bytes charged; open arenas by site:\n${lines.mkString("\n")}"
        )
      }
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
