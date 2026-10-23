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

import scala.collection.mutable

import io.vecruntime.spark.agg.{CountMergeAgg, VectorAggFunction}
import io.vecruntime.spark.expr.{ColumnRef, VectorExpr}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import org.scalatest.funsuite.AnyFunSuite

/**
 * #875: a grace-hash aggregate gives its table's memory back when another consumer of the task asks, between two
 * of its input batches, and still produces every group once with the right total.
 */
@io.vecruntime.spark.test.tags.Agg
class AggregateSpillOnRequestSuite extends AnyFunSuite {

  private val Batches = 12
  private val BatchRows = 4096
  private val Keys = 20000L

  private def metrics =
    new VectorMetrics(new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"), new SQLMetric("sum"))

  /** Buffer rows `(k, count = 1)`: the shape a final aggregate reads from the exchange. */
  private def rows(b: Int): ColumnarBatch = {
    val schema = new StructType().add("k", LongType).add("c", LongType)
    VectorRowStages.toBatch(schema, Array.tabulate(BatchRows)(i => InternalRow((b.toLong * BatchRows + i) % Keys, 1L)))
  }

  private def aggregate(in: Iterator[ColumnarBatch], spills: SQLMetric): VectorGroupedAggregateIterator =
    new VectorGroupedAggregateIterator(
      in,
      Array[VectorExpr](ColumnRef(0, LongType)),
      Array[VectorAggFunction](CountMergeAgg(ColumnRef(1, LongType))),
      Array(KeySlot(0), BufferSlot(0, 0)),
      Array("k" -> LongType, "c" -> LongType),
      metrics,
      AggSpillPolicy.GraceHash(1L << 40, 4), // a budget it never reaches: only a request can spill it
      Some((spills, new SQLMetric("sum"))),
      requestFloor = 0L
    )

  /** Every key once, its count the number of rows it had. */
  private def check(it: VectorGroupedAggregateIterator): Unit = {
    val counts = mutable.HashMap.empty[Long, Long]
    while (it.hasNext) {
      val b = it.next()
      var i = 0
      while (i < b.numRows()) {
        val k = b.column(0).getLong(i)
        assert(!counts.contains(k), s"key $k emitted twice")
        counts(k) = b.column(1).getLong(i)
        i += 1
      }
    }
    val total = Batches.toLong * BatchRows
    assert(counts.size === math.min(Keys, total))
    counts.foreach { case (k, c) =>
      val expected = total / Keys + (if (k < total % Keys) 1 else 0)
      assert(c === expected, s"count of key $k")
    }
  }

  private def withPool[T](poolBytes: Long)(body: TaskMemoryManager => T): T = {
    val mm = new UnifiedMemoryManager(new SparkConf(false), poolBytes, 0L, 1)
    val tmm = new TaskMemoryManager(mm, 0L)
    val ctx = new PoolTaskContext(tmm)
    TaskContext.setTaskContext(ctx)
    val wasInstalled = TaskNativeMemory.isInstalled
    TaskNativeMemory.uninstall()
    try {
      val out = body(tmm)
      ctx.complete()
      assert(mm.executionMemoryUsed === 0L, "everything held was returned")
      out
    } finally {
      TaskContext.unset()
      if (wasInstalled) TaskNativeMemory.install() else TaskNativeMemory.uninstall()
    }
  }

  test("#875: another consumer's request spills a grace-hash aggregate's table, and every group comes out right") {
    withPool(8L << 20) { tmm =>
      val other = new MemoryConsumer(tmm, tmm.pageSizeBytes(), MemoryMode.ON_HEAP) {
        override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
      }
      var granted = 0L
      var heldBefore = 0L
      val spills = new SQLMetric("sum")
      var it: VectorGroupedAggregateIterator = null
      val probing = Iterator.tabulate(Batches) { b =>
        // Batch 6 is pulled between two of the aggregate's batches: the request lands at its boundary.
        if (b == 6) {
          heldBefore = tmm.getMemoryConsumptionForThisTask()
          granted = other.acquireMemory(8L << 20) // the whole pool: only the aggregate can free it
        }
        rows(b)
      }
      it = aggregate(probing, spills)
      check(it)
      assert(heldBefore > 0, "the aggregate held memory before the request")
      assert(granted > 0, "the other consumer was granted what the aggregate gave back")
      assert(it.spillsOnRequest >= 1)
      assert(spills.value >= 1, "the table went to its buckets")
      other.freeMemory(granted)
      it.close()
    }
  }

  test("#875: with no request the grace-hash aggregate under its budget never spills") {
    withPool(1L << 30) { _ =>
      val spills = new SQLMetric("sum")
      val it = aggregate(Iterator.tabulate(Batches)(rows), spills)
      check(it)
      assert(it.spillsOnRequest === 0)
      assert(spills.value === 0)
      it.close()
    }
  }

  test("#875: a request below the floor gives nothing back") {
    withPool(8L << 20) { tmm =>
      val other = new MemoryConsumer(tmm, tmm.pageSizeBytes(), MemoryMode.ON_HEAP) {
        override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
      }
      var granted = 0L
      val spills = new SQLMetric("sum")
      val in = Iterator.tabulate(Batches) { b =>
        if (b == 6) granted = other.acquireMemory(8L << 20)
        rows(b)
      }
      val it = new VectorGroupedAggregateIterator(
        in,
        Array[VectorExpr](ColumnRef(0, LongType)),
        Array[VectorAggFunction](CountMergeAgg(ColumnRef(1, LongType))),
        Array(KeySlot(0), BufferSlot(0, 0)),
        Array("k" -> LongType, "c" -> LongType),
        metrics,
        AggSpillPolicy.GraceHash(1L << 40, 4),
        Some((spills, new SQLMetric("sum"))),
        requestFloor = 1L << 40
      )
      check(it)
      assert(it.spillsOnRequest === 0, "a table under the floor is not spilled on request")
      if (granted > 0) other.freeMemory(granted)
      it.close()
    }
  }

  /** A task context over `tmm` that records and runs its completion listeners. */
  private final class PoolTaskContext(tmm: TaskMemoryManager)
      extends org.apache.spark.sql.vecruntime.shims.TestTaskContextBase {
    private val completion = new mutable.ArrayBuffer[TaskCompletionListener]
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
