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

import scala.collection.mutable.ArrayBuffer

import io.vecruntime.spark.expr.ColumnRef
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import org.scalatest.funsuite.AnyFunSuite

/**
 * #867: the sort's memory comes from Spark's execution memory. Driven directly under a task context whose
 * `TaskMemoryManager` sits over a small pool: a pool too small for the partition makes the sort spill (with no
 * `spillBytes` cap), a pool large enough keeps it in memory, another consumer of the task can make it spill its
 * sealed runs, and everything it held is returned at the end. The rows come out sorted and complete every time.
 */
@io.vecruntime.spark.test.tags.SortWindow
class SortMemoryBudgetSuite extends AnyFunSuite {

  private val Rows = 8 * 4096

  private def metrics =
    new VectorMetrics(new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"))

  /** `(k: long, v: long)` with k descending, so that sorting ascending reorders every row. */
  private def input: Iterator[ColumnarBatch] = {
    val schema = new StructType().add("k", LongType).add("v", LongType)
    Iterator.tabulate(8) { b =>
      VectorRowStages.toBatch(
        schema,
        Array.tabulate(4096) { i =>
          val n = b.toLong * 4096 + i
          InternalRow(Rows - n, n * 3)
        }
      )
    }
  }

  private def sorter(in: Iterator[ColumnarBatch]): VectorSortIterator =
    new VectorSortIterator(
      in,
      Array(ColumnRef(0, LongType)),
      Array(true),
      Array(true),
      Array("k" -> LongType, "v" -> LongType),
      metrics,
      runRows = 1 << 20
    )

  /** Drains the sorter: every key in order, the rows counted. */
  private def check(it: VectorSortIterator): Unit = {
    var rows = 0
    var last = Long.MinValue
    while (it.hasNext) {
      val batch = it.next()
      var i = 0
      while (i < batch.numRows()) {
        val k = batch.column(0).getLong(i)
        assert(k >= last, s"out of order at row $rows")
        assert(batch.column(1).getLong(i) === (Rows - k) * 3)
        last = k; i += 1; rows += 1
      }
    }
    assert(rows === Rows)
  }

  private def withPool[T](poolBytes: Long)(body: (TaskMemoryManager, PoolTaskContext) => T): T = {
    val conf = new SparkConf(false)
    val mm = new UnifiedMemoryManager(conf, poolBytes, 0L, 1)
    val tmm = new TaskMemoryManager(mm, 0L)
    val ctx = new PoolTaskContext(tmm)
    TaskContext.setTaskContext(ctx)
    try {
      val out = body(tmm, ctx)
      ctx.complete()
      assert(mm.executionMemoryUsed === 0L, "the sort returned everything it held")
      out
    } finally TaskContext.unset()
  }

  test("#867: a pool too small for the partition makes the sort spill, with no spillBytes cap") {
    withPool(256L * 1024) { (_, _) =>
      val it = sorter(input)
      check(it)
      assert(it.spilledRuns > 0, "the pool refused, so runs went to disk")
      it.close()
    }
  }

  test("#867: a pool large enough keeps the partition in memory") {
    withPool(1L << 30) { (_, _) =>
      val it = sorter(input)
      check(it)
      assert(it.spilledRuns === 0)
      it.close()
    }
  }

  test("#874: the sort's peak native memory is measured, and covers what it reserved from the pool") {
    withPool(1L << 30) { (tmm, _) =>
      val m = new VectorMetrics(
        new SQLMetric("sum"),
        new SQLMetric("sum"),
        new SQLMetric("sum"),
        new SQLMetric("sum"),
        new SQLMetric("size")
      )
      val it = new VectorSortIterator(
        input,
        Array(ColumnRef(0, LongType)),
        Array(true),
        Array(true),
        Array("k" -> LongType, "v" -> LongType),
        m,
        runRows = 1 << 20
      )
      var reservedPeak = 0L
      while (it.hasNext) { it.next(); reservedPeak = math.max(reservedPeak, tmm.getMemoryConsumptionForThisTask()) }
      it.close()
      // Two long lanes of every row were resident at once; the reservation is checkable against the measure.
      assert(m.peakNativeBytes.value >= 16L * Rows, s"peak ${m.peakNativeBytes.value}")
      assert(reservedPeak > 0 && reservedPeak <= m.peakNativeBytes.value * 2, s"reserved $reservedPeak")
      m.recordPeakNative(1L)
      assert(m.peakNativeBytes.value >= 16L * Rows, "a high-water mark: a smaller value does not lower it")
    }
  }

  test("#867: another consumer of the task can make the sort spill its sealed runs") {
    // A pool the sort's runs nearly fill; after its first batches another consumer asks for most of it, and the
    // sort, still draining, gives back sealed runs by spilling them.
    withPool(4L << 20) { (tmm, _) =>
      var it: VectorSortIterator = null
      val other = new MemoryConsumer(tmm, tmm.pageSizeBytes(), MemoryMode.ON_HEAP) {
        override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
      }
      var granted = 0L
      var seen = 0
      val probing = input.map { b =>
        seen += 1
        if (seen == 6) granted = other.acquireMemory(4L << 20) // the whole pool: only the sort's spill frees it
        b
      }
      it = new VectorSortIterator(
        probing,
        Array(ColumnRef(0, LongType)),
        Array(true),
        Array(true),
        Array("k" -> LongType, "v" -> LongType),
        metrics,
        runRows = 4096 // a sealed run per batch, so there is something to give back
      )
      check(it)
      assert(granted > 0, "the other consumer was granted memory the sort released")
      assert(it.spilledRuns > 0)
      other.freeMemory(granted)
      it.close()
    }
  }

  test("#868: with off-heap runs, a heap too small for the merge's heap side makes the sort spill") {
    // Off-heap pool large enough for every run; on-heap pool of 128 KiB, a third of the permutations and normalised
    // keys of 32768 rows (4 + 9 bytes per row and key). The heap side's refusal must spill, not OOM the heap.
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
    val mm = new UnifiedMemoryManager(conf, 128L * 1024, 0L, 1)
    val tmm = new TaskMemoryManager(mm, 0L)
    val ctx = new PoolTaskContext(tmm)
    TaskContext.setTaskContext(ctx)
    try {
      val it = new VectorSortIterator(
        input,
        Array(ColumnRef(0, LongType)),
        Array(true),
        Array(true),
        Array("k" -> LongType, "v" -> LongType),
        metrics,
        runRows = 4096,
        memoryMode = MemoryMode.OFF_HEAP // #879: passed in, as the plan does
      )
      check(it)
      assert(it.spilledRuns > 0, "the on-heap pool refused the heap side, so runs went to disk")
      it.close()
      ctx.complete()
      assert(mm.executionMemoryUsed === 0L)
    } finally TaskContext.unset()
  }

  test("#879: the memory mode is the conf's, and ON_HEAP without a SparkEnv") {
    assert(VectorSortIterator.memoryMode() === MemoryMode.ON_HEAP)
    assert(VectorSortIterator.memoryMode(new SparkConf(false)) === MemoryMode.ON_HEAP)
    assert(
      VectorSortIterator.memoryMode(new SparkConf(false).set("spark.memory.offHeap.enabled", "true")) ===
        MemoryMode.OFF_HEAP
    )
  }

  test("#878: a sort with a nested column stays one in-memory run, and its bytes are held in the pool") {
    withPool(1L << 30) { (tmm, _) =>
      val schema = new StructType().add("k", LongType).add("a", org.apache.spark.sql.types.ArrayType(LongType))
      val in = Iterator.tabulate(4) { b =>
        VectorRowStages.toBatch(
          schema,
          Array.tabulate(4096) { i =>
            val n = b.toLong * 4096 + i
            InternalRow(16384L - n, new org.apache.spark.sql.catalyst.util.GenericArrayData(Array[Any](n, n + 1)))
          }
        )
      }
      val it = new VectorSortIterator(
        in,
        Array(ColumnRef(0, LongType)),
        Array(true),
        Array(true),
        Array("k" -> LongType, "a" -> org.apache.spark.sql.types.ArrayType(LongType)),
        metrics,
        runRows = 4096,
        spillBytes = 1L // a cap the nested sort cannot honour: it stays in memory
      )
      var rows = 0
      var last = Long.MinValue
      var peak = 0L
      while (it.hasNext) {
        val batch = it.next()
        peak = math.max(peak, tmm.getMemoryConsumptionForThisTask())
        var i = 0
        while (i < batch.numRows()) {
          val k = batch.column(0).getLong(i)
          assert(k >= last)
          assert(batch.column(1).getArray(i).getLong(0) === 16384L - k)
          last = k; i += 1; rows += 1
        }
      }
      assert(rows === 16384)
      assert(it.spilledRuns === 0, "nested runs are never spilled")
      assert(peak > 0, "but they are held in the pool")
      it.close()
    }
  }

  test("#876: a build table counts its heap mirrors as it makes them") {
    val schema = new StructType().add("k", LongType)
    val batches = Iterator(VectorRowStages.toBatch(schema, Array.tabulate(1000)(i => InternalRow(i.toLong))))
    val table = BuildTable.fromBatches(batches, joinSpec)
    try {
      assert(table.mirrorHeapBytes === 0L)
      assert(table.mirror(0) != null)
      assert(table.mirrorHeapBytes === 8L * 1000, "a long per row, no nulls")
      table.mirror(0)
      assert(table.mirrorHeapBytes === 8L * 1000, "made once")
    } finally table.close()
  }

  // #868: the shuffled hash join's build side on the same pool.
  private val joinSpec = JoinSpec(
    joinType = org.apache.spark.sql.catalyst.plans.Inner,
    buildIsLeft = false,
    buildKeys = Array(ColumnRef(0, LongType)),
    streamedKeys = Array(ColumnRef(0, LongType)),
    condition = None,
    outputAttrs = Array("k" -> LongType, "bk" -> LongType),
    joinedAttrs = Array("k" -> LongType, "bk" -> LongType),
    buildTypes = Array(LongType),
    streamedWidth = 1
  )

  /** Build keys 0 until 32768, streamed even keys 0 until 65536: (matches, sum of the streamed keys). */
  private def join(budgetBytes: Long): (Long, Long) = {
    val schema = new StructType().add("k", LongType)
    def batches(n: Int)(v: Long => Long) =
      Iterator.tabulate(8)(b =>
        VectorRowStages.toBatch(schema, Array.tabulate(n)(i => InternalRow(v(b.toLong * n + i))))
      )
    val it = GraceHashJoin.iterator(batches(4096)(identity), batches(4096)(_ * 2), joinSpec, metrics, budgetBytes, 32)
    var rows = 0L
    var sum = 0L
    while (it.hasNext) {
      val b = it.next()
      var i = 0
      while (i < b.numRows()) { sum += b.column(0).getLong(i); i += 1 }
      rows += b.numRows()
    }
    (rows, sum)
  }

  private val joinExpected = (16384L, (0L until 32768L by 2).sum)

  test("#868: a bucketed join's per-bucket tables are held in the pool while probed, and returned") {
    withPool(1L << 30) { (tmm, _) =>
      val schema = new StructType().add("k", LongType)
      def batches(n: Int)(v: Long => Long) =
        Iterator.tabulate(8)(b =>
          VectorRowStages.toBatch(schema, Array.tabulate(n)(i => InternalRow(v(b.toLong * n + i))))
        )
      // A budget of 1 byte buckets at once; the pool is large, so each bucket's table is granted.
      val it = GraceHashJoin.iterator(batches(4096)(identity), batches(4096)(_ * 2), joinSpec, metrics, 1L, 32)
      var rows = 0L
      var peak = 0L
      while (it.hasNext) {
        rows += it.next().numRows()
        peak = math.max(peak, tmm.getMemoryConsumptionForThisTask())
      }
      assert(rows === joinExpected._1)
      assert(peak > 0, "the bucket being probed was held")
    }
  }

  test("#869: a pooled arena's blocks are held in the task's execution memory and given back") {
    withPool(1L << 30) { (tmm, _) =>
      TaskNativeMemory.install()
      try {
        val a = io.vecruntime.kernels.NativePool.shared().newArena()
        a.allocate(3L << 20, 8)
        a.allocate(1L << 20, 8)
        assert(tmm.getMemoryConsumptionForThisTask() >= (5L << 20), "4 MiB class + 1 MiB class, charged")
        a.close()
        assert(tmm.getMemoryConsumptionForThisTask() <= 2 * TaskNativeMemory.Chunk, "given back to one chunk of slack")
      } finally TaskNativeMemory.uninstall()
    }
  }

  test("#869: a pooled allocation the memory manager cannot cover is counted as overdraft, not refused") {
    val before = TaskNativeMemory.overdraftBytes
    withPool(1L << 20) { (_, _) =>
      TaskNativeMemory.install()
      try {
        val a = io.vecruntime.kernels.NativePool.shared().newArena()
        a.allocate(8L << 20, 8) // larger than the whole pool: handed out anyway
        a.close()
      } finally TaskNativeMemory.uninstall()
    }
    assert(TaskNativeMemory.overdraftBytes - before >= (7L << 20))
  }

  test("#869: an arena still open when its task completes is counted as leaked, and the memory is returned") {
    val before = TaskNativeMemory.leakedBytes
    var open: java.lang.foreign.Arena = null
    withPool(1L << 30) { (_, _) =>
      TaskNativeMemory.install()
      try {
        open = io.vecruntime.kernels.NativePool.shared().newArena()
        open.allocate(1L << 20, 8)
      } finally TaskNativeMemory.uninstall()
    }
    assert(TaskNativeMemory.leakedBytes - before === (1L << 20))
    open.close()
  }

  test("#869: no task, no account: an arena opened off a task thread charges nothing") {
    TaskNativeMemory.install()
    try assert(TaskNativeMemory.current() === null)
    finally TaskNativeMemory.uninstall()
  }

  test("#875: the pool memory unspillable consumers hold is counted, current and peak, and returned") {
    withPool(1L << 30) { (_, _) =>
      val before = PinnedMemory.bytes
      val m = new BuildMemory(MemoryMode.ON_HEAP)
      assert(m.hold(1L << 20))
      assert(PinnedMemory.bytes - before === (1L << 20))
      assert(PinnedMemory.peakBytes >= PinnedMemory.bytes)
      m.releaseTo(1L << 18)
      assert(PinnedMemory.bytes - before === (1L << 18))
      m.releaseTo(0L)
      assert(PinnedMemory.bytes === before)
    }
  }

  test("#875: a join build is admitted within its spill budget and the task's share of the off-heap pool") {
    val sql = new org.apache.spark.sql.internal.SQLConf
    val offHeap = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
      .set("spark.executor.cores", "8")
    assert(io.vecruntime.spark.VectorConf.joinBuildAdmissionBytes(sql, new SparkConf(false)) === (256L << 20))
    assert(io.vecruntime.spark.VectorConf.joinBuildAdmissionBytes(sql, offHeap) === (128L << 20), "the share")
    sql.setConfString("spark.vecruntime.join.spillBytes", "64m")
    assert(io.vecruntime.spark.VectorConf.joinBuildAdmissionBytes(sql, offHeap) === (64L << 20), "the budget")
  }

  test("#886: an aggregate's reservation survives a reset, and a refusal under the floor is not acted on") {
    withPool(1L << 20) { (_, _) =>
      val m = new AggregateMemory(minRefusalBytes = 4L << 20)
      assert(!m.overBudget(512L << 10, Long.MaxValue), "granted")
      assert(m.held === (512L << 10))
      assert(!m.overBudget(2L << 20, Long.MaxValue), "refused, but a 2 MiB table is under the floor: it goes on")
      assert(m.overBudget(8L << 20, Long.MaxValue), "past the floor a refusal emits or spills the table")
      val held = m.held
      // A reset keeps the reservation: the next table regrows into it without asking the pool.
      assert(!m.overBudget(held, Long.MaxValue))
      assert(m.held === held)
      assert(m.overBudget(2L << 20, 1L << 20), "the fixed threshold stays a hard cap")
      m.release()
      assert(m.held === 0L)
    }
  }

  test("#868: a pool too small for the build side makes the hash join bucket, within a large budget") {
    withPool(64L * 1024) { (_, _) =>
      val before = GraceHashJoin.bucketedJoins.get()
      assert(join(budgetBytes = 1L << 30) === joinExpected)
      assert(GraceHashJoin.bucketedJoins.get() > before, "the pool refused, so the join went to buckets")
    }
  }

  test("#868: a pool large enough keeps the build side in memory") {
    withPool(1L << 30) { (_, _) =>
      val before = GraceHashJoin.bucketedJoins.get()
      assert(join(budgetBytes = 1L << 30) === joinExpected)
      assert(GraceHashJoin.bucketedJoins.get() === before)
    }
  }

  test("#877: with off-heap memory the pool is the ceiling and the task's share only the spill trigger") {
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "24g")
      .set("spark.executor.cores", "15")
    val sql = new org.apache.spark.sql.internal.SQLConf
    assert(io.vecruntime.spark.VectorConf.sortSpillBytes(sql, conf) === Long.MaxValue, "no cap: the grant decides")
    assert(io.vecruntime.spark.VectorConf.sortFairShareBytes(sql, conf) === (24L << 30) / 15)
    assert(io.vecruntime.spark.VectorConf.sortSpillBytes(sql, new SparkConf(false)) === (1L << 30))
    assert(io.vecruntime.spark.VectorConf.sortFairShareBytes(sql, new SparkConf(false)) === 0L)
    sql.setConfString("spark.vecruntime.sort.spillBytes", "256m")
    assert(io.vecruntime.spark.VectorConf.sortSpillBytes(sql, conf) === (256L << 20), "an explicit setting wins")
    assert(io.vecruntime.spark.VectorConf.sortFairShareBytes(sql, conf) === 0L, "and is a hard cap, not a trigger")
  }

  test("#877: without off-heap memory the default cap is bounded by the task's share of the overhead") {
    val sql = new org.apache.spark.sql.internal.SQLConf
    def cap(c: SparkConf) = io.vecruntime.spark.VectorConf.sortSpillBytes(sql, c)
    val base = new SparkConf(false).set("spark.executor.cores", "13").set("spark.executor.memoryOverhead", "20g")
    // No MaxDirectMemorySize: half the overhead is set aside for direct memory, half of the rest per task.
    assert(cap(base) === (10L << 30) / 26)
    val wide = base.clone.set("spark.executor.memoryOverhead", "40g")
    assert(
      cap(wide.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxDirectMemorySize=4g")) === (1L << 30),
      "36g left over 26 is past the 1 GiB default, which stays the ceiling"
    )
    assert(cap(base.clone.set("spark.executor.extraJavaOptions", "-XX:MaxDirectMemorySize=19g")) === (64L << 20))
    assert(cap(base.clone.set("spark.executor.memoryOverhead", "4096")) === (2L << 30) / 26, "a bare number is MiB")
    assert(cap(new SparkConf(false)) === (1L << 30), "no overhead set: the 1 GiB default")
    sql.setConfString("spark.vecruntime.sort.spillBytes", "2g")
    assert(cap(base) === (2L << 30), "an explicit setting wins")
  }

  test("#877: past its fair share the sort keeps runs resident while the pool has room, spills when it has none") {
    // Fair share 64 KiB: the partition's runs (~256 KiB of longs) pass it. With 4 MiB free in the pool the trigger is
    // share + 2 MiB and nothing spills; with nothing free the trigger is the share and runs go to disk.
    def run(free: Long): Int = withPool(1L << 30) { (_, _) =>
      val it = new VectorSortIterator(
        input,
        Array(ColumnRef(0, LongType)),
        Array(true),
        Array(true),
        Array("k" -> LongType, "v" -> LongType),
        metrics,
        runRows = 4096,
        fairShareBytes = 64L * 1024,
        poolFree = () => free
      )
      check(it)
      val spilled = it.spilledRuns
      it.close()
      spilled
    }
    assert(run(free = 4L << 20) === 0, "a pool with room keeps the runs resident past the share")
    assert(run(free = 0L) > 0, "a full pool brings the trigger back to the share")
  }

  /** A task context over `tmm` that records and runs its completion listeners. */
  private final class PoolTaskContext(tmm: TaskMemoryManager)
      extends org.apache.spark.sql.vecruntime.shims.TestTaskContextBase {
    private val completion = new ArrayBuffer[TaskCompletionListener]
    def complete(): Unit = completion.reverseIterator.foreach(_.onTaskCompletion(this))

    override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
      completion += listener; this
    }
    override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = this
    override def isCompleted(): Boolean = false
    override def isFailed(): Boolean = false
    override def isInterrupted(): Boolean = false
    override def stageId(): Int = 0
    override def stageAttemptNumber(): Int = 0
    override def partitionId(): Int = 0
    override def numPartitions(): Int = 1
    override def attemptNumber(): Int = 0
    override def taskAttemptId(): Long = 0L
    override def getLocalProperty(key: String): String = null
    override def cpus(): Int = 1
    override def resources(): Map[String, org.apache.spark.resource.ResourceInformation] = Map.empty
    override def resourcesJMap(): java.util.Map[String, org.apache.spark.resource.ResourceInformation] =
      java.util.Collections.emptyMap()
    override def taskMetrics(): org.apache.spark.executor.TaskMetrics = null
    override def getMetricsSources(sourceName: String): Seq[org.apache.spark.metrics.source.Source] = Nil
    override private[spark] def killTaskIfInterrupted(): Unit = ()
    override private[spark] def getKillReason(): Option[String] = None
    override private[spark] def taskMemoryManager(): TaskMemoryManager = tmm
    override private[spark] def registerAccumulator(a: org.apache.spark.util.AccumulatorV2[_, _]): Unit = ()
    override private[spark] def setFetchFailed(e: org.apache.spark.shuffle.FetchFailedException): Unit = ()
    override private[spark] def markInterrupted(reason: String): Unit = ()
    override private[spark] def markTaskFailed(error: Throwable): Unit = ()
    override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = ()
    override private[spark] def fetchFailed: Option[org.apache.spark.shuffle.FetchFailedException] = None
    override private[spark] def getLocalProperties: java.util.Properties = new java.util.Properties
    override private[spark] def interruptible(): Boolean = false
    override private[spark] def pendingInterrupt(threadToInterrupt: Option[Thread], reason: String): Unit = ()
    override private[spark] def createResourceUninterruptibly[T <: java.io.Closeable](resourceBuilder: => T): T =
      resourceBuilder
  }
}
