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
    VectorSortIterator.modeOverride = MemoryMode.OFF_HEAP
    TaskContext.setTaskContext(ctx)
    try {
      val it = new VectorSortIterator(
        input,
        Array(ColumnRef(0, LongType)),
        Array(true),
        Array(true),
        Array("k" -> LongType, "v" -> LongType),
        metrics,
        runRows = 4096
      )
      check(it)
      assert(it.spilledRuns > 0, "the on-heap pool refused the heap side, so runs went to disk")
      it.close()
      ctx.complete()
      assert(mm.executionMemoryUsed === 0L)
    } finally {
      VectorSortIterator.modeOverride = null
      TaskContext.unset()
    }
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

  test("#868: with off-heap memory the sort's default cap is the task's share of the pool") {
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "24g")
      .set("spark.executor.cores", "15")
    val sql = new org.apache.spark.sql.internal.SQLConf
    assert(io.vecruntime.spark.VectorConf.sortSpillBytes(sql, conf) === (24L << 30) / 15)
    assert(io.vecruntime.spark.VectorConf.sortSpillBytes(sql, new SparkConf(false)) === (1L << 30))
    sql.setConfString("spark.vecruntime.sort.spillBytes", "256m")
    assert(io.vecruntime.spark.VectorConf.sortSpillBytes(sql, conf) === (256L << 20), "an explicit setting wins")
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
