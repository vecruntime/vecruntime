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

import java.lang.management.ManagementFactory

import scala.collection.mutable.ArrayBuffer

import io.vecruntime.spark.expr.ColumnRef
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import org.scalatest.funsuite.AnyFunSuite

/**
 * The grace hash join (#416) driven directly, under a task context that only records what is
 * registered on it: the memory the bucketing path holds per task must not grow with the buckets, nor
 * with the rows it held before it started bucketing.
 */
@io.vecruntime.spark.test.tags.Join
class GraceHashJoinSuite extends AnyFunSuite {

  /** Streamed `(k: long)` on the left, build `(bk: long, v: int)` on the right, joined on `k = bk`. */
  private val spec = JoinSpec(
    joinType = Inner,
    buildIsLeft = false,
    buildKeys = Array(ColumnRef(0, LongType)),
    streamedKeys = Array(ColumnRef(0, LongType)),
    condition = None,
    outputAttrs = Array("k" -> LongType, "bk" -> LongType, "v" -> IntegerType),
    joinedAttrs = Array("k" -> LongType, "bk" -> LongType, "v" -> IntegerType),
    buildTypes = Array(LongType, IntegerType),
    streamedWidth = 1
  )

  private def metrics =
    new VectorMetrics(new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"))

  private def batches(
      schema: StructType,
      batchRows: Int,
      numBatches: Int
  )(row: Long => InternalRow): Iterator[ColumnarBatch] =
    Iterator.tabulate(numBatches) { b =>
      VectorRowStages.toBatch(schema, Array.tabulate(batchRows)(i => row(b.toLong * batchRows + i)))
    }

  /** Rows and the sum of the first column, read with `key` (`getLong` by default). */
  private def rowsOf(
      it: Iterator[ColumnarBatch],
      key: (org.apache.spark.sql.vectorized.ColumnVector, Int) => Long = _.getLong(_)
  ): (Long, Long) = {
    var rows = 0L
    var keySum = 0L
    while (it.hasNext) {
      val batch = it.next()
      rows += batch.numRows()
      var i = 0
      val c = batch.column(0)
      while (i < batch.numRows()) { keySum += key(c, i); i += 1 }
    }
    (rows, keySum)
  }

  private def join(budgetBytes: Long, buckets: Int): ((Long, Long), RecordingTaskContext) = {
    val ctx = new RecordingTaskContext
    TaskContext.setTaskContext(ctx)
    try {
      val build = batches(new StructType().add("bk", LongType).add("v", IntegerType), 4096, 8) { i =>
        InternalRow(i, (i % 7).toInt)
      }
      val streamed = batches(new StructType().add("k", LongType), 4096, 8) { i => InternalRow(i * 2) }
      val result = rowsOf(GraceHashJoin.iterator(build, streamed, spec, metrics, budgetBytes, buckets))
      ctx.complete()
      (result, ctx)
    } finally TaskContext.unset()
  }

  test("#416: a bucketed join registers one task-completion listener, not one per bucket") {
    // Keys 0 until 32768 on the build side, even keys 0 until 65536 streamed: 16384 matches.
    val expected = (16384L, (0L until 32768L by 2).sum)
    val (inMemory, ctxMem) = join(budgetBytes = Long.MaxValue, buckets = 32)
    assert(inMemory === expected)
    val (bucketed, ctxBuckets) = join(budgetBytes = 1, buckets = 32)
    assert(bucketed === expected)
    // Every registration keeps its closure -- the iterator, its build table and heap arrays -- reachable
    // until the task ends, so the bucketing path must register exactly what the in-memory path does.
    assert(ctxMem.listeners === 1)
    assert(ctxBuckets.listeners === 1, "one listener per bucket would hold every bucket's table until task end")
  }

  /** Streamed `(k: int)` on the left, build `(bk: int)` on the right, joined on `k = bk`: q95's shape. */
  private val intSpec = JoinSpec(
    joinType = Inner,
    buildIsLeft = false,
    buildKeys = Array(ColumnRef(0, IntegerType)),
    streamedKeys = Array(ColumnRef(0, IntegerType)),
    condition = None,
    outputAttrs = Array("k" -> IntegerType, "bk" -> IntegerType),
    joinedAttrs = Array("k" -> IntegerType, "bk" -> IntegerType),
    buildTypes = Array(IntegerType),
    streamedWidth = 1
  )

  /**
   * `rows` rows of one int column in `batchRows`-row batches (written straight into Spark's vectors: the
   * millions of rows the memory test needs), `perKey` consecutive rows per key, the keys scattered.
   */
  private def intSide(name: String, rows: Long, perKey: Int, batchRows: Int = 8192)(
      onNext: Long => Unit = _ => ()
  ): Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
    private var emitted = 0L
    private val schema = new StructType().add(name, IntegerType)
    override def hasNext: Boolean = emitted < rows
    override def next(): ColumnarBatch = {
      val n = math.min(batchRows.toLong, rows - emitted).toInt
      val v = OnHeapColumnVector.allocateColumns(n, schema)(0)
      var i = 0
      while (i < n) { v.putInt(i, ((((emitted + i) / perKey) * 2654435761L) & 0x3fffffffL).toInt); i += 1 }
      emitted += n
      onNext(emitted)
      new ColumnarBatch(Array(v), n)
    }
  }

  private def intJoin(
      buildRows: Long,
      streamedRows: Long,
      perKey: Int,
      budgetBytes: Long,
      buckets: Int
  ): (Long, Long) = {
    val ctx = new RecordingTaskContext
    TaskContext.setTaskContext(ctx)
    try {
      val out = rowsOf(
        GraceHashJoin.iterator(
          intSide("bk", buildRows, perKey)(),
          intSide("k", streamedRows, perKey)(),
          intSpec,
          metrics,
          budgetBytes,
          buckets
        ),
        _.getInt(_)
      )
      ctx.complete()
      out
    } finally TaskContext.unset()
  }

  test("#416: a many-to-many join has the same rows in memory and bucketed, whichever batch overflows") {
    // Four build rows and four streamed rows per key, the streamed keys a prefix of the build keys: every
    // streamed row meets four build rows, 16 pairs per key (q95's web_sales self-join shape).
    val expected = intJoin(200000, 100000, perKey = 4, budgetBytes = Long.MaxValue, buckets = 32)
    assert(expected._1 === 100000L * 4)
    // Nothing held (the first batch already exceeds the budget), a hold ending mid-way, a hold of most rows.
    for (budget <- Seq(1L, 64L * 1024, 600L * 1024)) {
      assert(intJoin(200000, 100000, perKey = 4, budgetBytes = budget, buckets = 32) === expected, s"budget $budget")
    }
    assert(intJoin(200000, 100000, perKey = 4, budgetBytes = 64L * 1024, buckets = 3) === expected, "3 buckets")
  }

  test("#416: the held rows are bucketed in bounded slices -- the spill's heap does not grow with them") {
    // A 64 MB budget over an int key: 16 M rows held before the split. Bucketing them all in one call took
    // two int arrays over every held row (128 MB here, 536 MB for q95's 67 M rows) and a mask per bucket;
    // by slices of SpillChunkRows the scratch is a slice's.
    val budget = 64L * 1024 * 1024
    val heldRows = budget / 4
    val mem = ManagementFactory.getMemoryMXBean
    @volatile var window = 0 // 0 before the held spill, 1 during it, 2 after
    var asked = 0
    val build =
      intSide("bk", heldRows + 4 * 8192, perKey = 4)(emitted => if (emitted > heldRows && window == 0) window = 1)
    // Since #771 the hold loop stops at the overflowing batch without asking hasNext again: the held spill lies
    // before the drain loop's first call, and the window closes at its second.
    val wrapped = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        if (window == 1) { asked += 1; if (asked == 2) window = 2 }
        build.hasNext
      }
      override def next(): ColumnarBatch = build.next()
    }
    @volatile var running = true
    @volatile var startUsed = -1L
    @volatile var peakUsed = -1L
    val sampler = new Thread(() => {
      while (running) {
        if (window == 1) {
          val used = mem.getHeapMemoryUsage.getUsed
          if (startUsed < 0) startUsed = used
          peakUsed = math.max(peakUsed, used)
        }
        Thread.sleep(1)
      }
    })
    sampler.setDaemon(true)
    sampler.start()
    val ctx = new RecordingTaskContext
    TaskContext.setTaskContext(ctx)
    try {
      val out = rowsOf(
        GraceHashJoin.iterator(wrapped, intSide("k", 4096, perKey = 4)(), intSpec, metrics, budget, 32),
        _.getInt(_)
      )
      ctx.complete()
      assert(out._1 === 4096L * 4)
    } finally {
      TaskContext.unset()
      running = false
      sampler.join()
    }
    assert(window === 2, "the held spill was not observed")
    assert(startUsed >= 0, "no heap sample fell inside the held spill")
    val growth = (peakUsed - startUsed) / 1048576.0
    // 2 x 1 MB of hash and id scratch, reused across slices, plus per-slice Arrow objects; 128 MB before.
    assert(growth < 32, f"heap grew by $growth%.1f MB while bucketing the held rows")
  }

  test("#771: the batch that overflows the budget is spilled before the build side is asked for another") {
    // Our shuffle reader closes the last batch it handed out when hasNext finds the stream ended
    // (VectorShuffleManager closes the reader, PartitionedIpcFile.close() closes `last`). The hold loop asked
    // hasNext once more after the overflowing batch, so when that batch ended its stream it was released
    // before it was spilled: q14b at 3 TB read past a 0-byte buffer. Every build batch here is the last of
    // its stream and closes on the next hasNext; the result must be the in-memory join's.
    val allocator = new org.apache.arrow.memory.RootAllocator()
    try {
      def streamOfOne(base: Int, n: Int): Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
        private var handed: ColumnarBatch = null
        private var done = false
        override def hasNext: Boolean = {
          if (handed != null && !done) { handed.close(); done = true } // the reader closes `last` at the end
          handed == null
        }
        override def next(): ColumnarBatch = {
          val v = new org.apache.arrow.vector.IntVector("bk", allocator)
          v.allocateNew(n)
          var i = 0
          while (i < n) { v.set(i, ((((base + i) / 4) * 2654435761L) & 0x3fffffffL).toInt); i += 1 }
          v.setValueCount(n)
          handed = new ColumnarBatch(Array(new io.vecruntime.spark.arrow.VectorArrowColumnVector(v)), n)
          handed
        }
      }
      // Four streams of 8192 rows, as AQE's coalesced read chains the shuffle blocks of several map tasks.
      def build(): Iterator[ColumnarBatch] = (0 until 4).iterator.flatMap(s => streamOfOne(s * 8192, 8192))
      def run(budget: Long): (Long, Long) = {
        val ctx = new RecordingTaskContext
        TaskContext.setTaskContext(ctx)
        try {
          val out = rowsOf(
            GraceHashJoin.iterator(build(), intSide("k", 16384, perKey = 4)(), intSpec, metrics, budget, 8),
            _.getInt(_)
          )
          ctx.complete()
          out
        } finally TaskContext.unset()
      }
      val expected = run(Long.MaxValue)
      assert(expected._1 === 16384L * 4)
      // The first batch overflows (1 byte), or a later one does (the second, then the fourth, the last of all).
      for (budget <- Seq(1L, 40L * 1024, 120L * 1024)) assert(run(budget) === expected, s"budget $budget")
    } finally allocator.close()
  }

  test("#875: another consumer's request mid-build sends the join to its buckets, with the same rows") {
    val expected = intJoin(200000, 100000, perKey = 4, budgetBytes = Long.MaxValue, buckets = 32)
    val mm = new org.apache.spark.memory.UnifiedMemoryManager(new org.apache.spark.SparkConf(false), 64L << 20, 0L, 1)
    val tmm = new org.apache.spark.memory.TaskMemoryManager(mm, 0L)
    val ctx = new RecordingTaskContext(tmm)
    TaskContext.setTaskContext(ctx)
    val wasInstalled = TaskNativeMemory.isInstalled
    TaskNativeMemory.uninstall()
    val floor = GraceHashJoin.requestFloorBytes
    GraceHashJoin.requestFloorBytes = 0L
    val before = GraceHashJoin.requestBucketings.get()
    try {
      val other = new org.apache.spark.memory.MemoryConsumer(
        tmm,
        tmm.pageSizeBytes(),
        org.apache.spark.memory.MemoryMode.ON_HEAP
      ) {
        override def spill(size: Long, trigger: org.apache.spark.memory.MemoryConsumer): Long = 0L
      }
      var granted = 0L
      // After ten build batches, pulled between two of the join's appends, another consumer asks for the whole pool.
      val build = intSide("bk", 200000, perKey = 4)(emitted =>
        if (emitted == 8192L * 10 && granted == 0L) granted = other.acquireMemory(64L << 20)
      )
      val out = rowsOf(
        GraceHashJoin.iterator(build, intSide("k", 100000, perKey = 4)(), intSpec, metrics, 1L << 40, 32),
        _.getInt(_)
      )
      assert(out === expected)
      assert(GraceHashJoin.requestBucketings.get() === before + 1, "the request sent the build side to its buckets")
      assert(granted > 0, "the other consumer got the memory the join gave back")
      other.freeMemory(granted)
      ctx.complete()
    } finally {
      GraceHashJoin.requestFloorBytes = floor
      TaskContext.unset()
      if (wasInstalled) TaskNativeMemory.install() else TaskNativeMemory.uninstall()
    }
  }

  test("#875: without a memory manager, a join is never bucketed on request") {
    val before = GraceHashJoin.requestBucketings.get()
    assert(intJoin(200000, 100000, perKey = 4, budgetBytes = 1L << 40, buckets = 32)._1 === 100000L * 4)
    assert(GraceHashJoin.requestBucketings.get() === before)
  }

  /** A task context that records the completion listeners and runs them on `complete()`; nothing else. */
  private final class RecordingTaskContext(tmm: org.apache.spark.memory.TaskMemoryManager = null)
      extends org.apache.spark.sql.vecruntime.shims.TestTaskContextBase {
    private val completion = new ArrayBuffer[TaskCompletionListener]
    def listeners: Int = completion.size
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
    override private[spark] def taskMemoryManager(): org.apache.spark.memory.TaskMemoryManager = tmm
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
