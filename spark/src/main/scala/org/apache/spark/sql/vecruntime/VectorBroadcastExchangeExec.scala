/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels
import java.util.concurrent.{Future, TimeUnit, TimeoutException}
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.control.NonFatal

import io.vecruntime.spark.arrow.{ArrowOutput, VectorAllocators}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util.{SparkFatalException, ThreadUtils}

/**
 * The broadcast relation of [[VectorBroadcastExchangeExec]]: the build side's batches as Arrow IPC
 * streams, one per input partition (dictionaries included, plain columns as they came). What our
 * broadcast joins read; serialisable as plain bytes, so the executors deserialise it with no Spark
 * row or hash-map format in between.
 */
final class VectorBroadcastBatches(
    val names: Array[String],
    val types: Array[DataType],
    val streams: Array[Array[Byte]],
    val numRows: Long
) extends Serializable {

  def sizeInBytes: Long = streams.iterator.map(_.length.toLong).sum

  @transient private lazy val uniqueByColumn = new java.util.concurrent.ConcurrentHashMap[Integer, java.lang.Boolean]()

  /**
   * Whether column `ordinal` (an integral key) holds no value twice among its non-null rows: the broadcast
   * join's build keys are then unique, so every build-side column is a function of the key (#635,
   * RemoveRedundantGroupKeys). Computed once per column on the process that holds the relation (the
   * driver, for the adaptive rule), by sorting a copy of the keys; `false` when the column is not
   * integral or the relation has more than `maxRows` rows (not worth the sort then).
   */
  def keysUnique(ordinal: Int, maxRows: Long = VectorBroadcastBatches.MaxUniqueCheckRows): Boolean = {
    if (ordinal < 0 || ordinal >= types.length || numRows > maxRows) return false
    types(ordinal) match {
      case _: org.apache.spark.sql.types.IntegerType | _: org.apache.spark.sql.types.LongType |
          _: org.apache.spark.sql.types.ShortType | _: org.apache.spark.sql.types.ByteType |
          _: org.apache.spark.sql.types.DateType =>
      case _ => return false
    }
    uniqueByColumn.computeIfAbsent(ordinal, _ => java.lang.Boolean.valueOf(computeUnique(ordinal))).booleanValue()
  }

  private def computeUnique(ordinal: Int): Boolean = {
    var keys = new Array[Long](math.max(16, math.min(numRows, Int.MaxValue - 8).toInt))
    var n = 0
    val isLong = types(ordinal).isInstanceOf[org.apache.spark.sql.types.LongType]
    val it = batches()
    while (it.hasNext) {
      val b = it.next()
      val v = b.column(ordinal)
      var r = 0
      val rows = b.numRows()
      while (r < rows) {
        if (!v.isNullAt(r)) {
          if (n == keys.length) keys = java.util.Arrays.copyOf(keys, keys.length * 2)
          keys(n) = types(ordinal) match {
            case _: org.apache.spark.sql.types.ShortType => v.getShort(r).toLong
            case _: org.apache.spark.sql.types.ByteType => v.getByte(r).toLong
            case _ => if (isLong) v.getLong(r) else v.getInt(r).toLong
          }
          n += 1
        }
        r += 1
      }
    }
    java.util.Arrays.sort(keys, 0, n)
    var i = 1
    while (i < n) {
      if (keys(i) == keys(i - 1)) return false
      i += 1
    }
    true
  }

  /**
   * Every batch of every stream, as the operators' column vectors. Each batch is closed when the next
   * one is taken or the iterator is exhausted, so a consumer copies what it keeps (the build table does).
   */
  def batches(): Iterator[ColumnarBatch] = {
    val allocator = VectorAllocators.newChild("VectorBroadcastBatches.read")
    new Iterator[ColumnarBatch] {
      private var s = 0
      private var reader: ArrowStreamReader = _
      private var pending: ColumnarBatch = _
      private var last: ColumnarBatch = _
      private var done = false

      private def advance(): Unit = while (!done && pending == null) {
        if (reader == null) {
          if (s == streams.length) {
            done = true
            if (last != null) { last.close(); last = null }
            allocator.close()
          } else {
            reader = new ArrowStreamReader(new ByteArrayInputStream(streams(s)), allocator)
            s += 1
          }
        } else if (reader.loadNextBatch()) {
          pending = VectorBroadcastBatches.toBatch(reader.getVectorSchemaRoot, names, types, allocator)
        } else {
          reader.close()
          reader = null
        }
      }

      override def hasNext: Boolean = { advance(); pending != null }

      override def next(): ColumnarBatch = {
        if (!hasNext) throw new NoSuchElementException
        if (last != null) last.close()
        last = pending
        pending = null
        last
      }
    }
  }
}

object VectorBroadcastBatches {

  /** Above this many rows `keysUnique` answers false without looking (the sort's cost and memory). */
  val MaxUniqueCheckRows: Long = 16L << 20

  private def toBatch(
      root: VectorSchemaRoot,
      names: Array[String],
      types: Array[DataType],
      allocator: org.apache.arrow.memory.BufferAllocator
  ): ColumnarBatch = {
    val out = new Array[ColumnVector](types.length)
    var c = 0
    while (c < types.length) {
      val v = ArrowOutput.newVector(names(c), types(c), allocator)
      root.getVector(c).makeTransferPair(v).transfer()
      out(c) = ArrowOutput.wrap(v, types(c))
      c += 1
    }
    new ColumnarBatch(out, root.getRowCount)
  }

  /**
   * One partition's batches as one Arrow IPC stream: each batch's rows (its selection applied, if it
   * carries one) copied into Arrow vectors and written as a record batch. Returns (rows, bytes).
   */
  def write(iter: Iterator[ColumnarBatch], names: Array[String], types: Array[DataType]): (Long, Array[Byte]) = {
    val allocator = VectorAllocators.newChild("VectorBroadcastExchangeExec.write")
    val bytes = new ByteArrayOutputStream()
    val root = VectorSchemaRoot.of(names.indices.map(c => ArrowOutput.newVector(names(c), types(c), allocator)): _*)
    val writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))
    var rows = 0L
    try {
      writer.start()
      while (iter.hasNext) {
        val batch = iter.next()
        if (batch.numRows() > 0) {
          EvalContexts.withBatch(batch) { ctx =>
            val n = ctx.selectedCount
            if (n > 0) {
              var c = 0
              while (c < types.length) {
                // A dictionary-encoded string column is decoded first: the stream carries plain vectors.
                val raw = ctx.input(c)
                val arena = if (raw.isDictionaryEncoded) java.lang.foreign.Arena.ofConfined() else null
                try {
                  val in = if (arena == null) raw else ArrowOutput.decodeDictionary(raw, arena)
                  val col =
                    if (ctx.selection == null) ArrowOutput.copy(names(c), types(c), in, allocator)
                    else ArrowOutput.compact(names(c), types(c), in, ctx.selection, n, allocator)
                  try AggregateSpill.vectorOf(col).makeTransferPair(root.getVector(c)).transfer()
                  finally col.close()
                } finally if (arena != null) arena.close()
                c += 1
              }
              root.setRowCount(n)
              writer.writeBatch()
              rows += n
            }
          }
        }
      }
      writer.end()
    } finally {
      writer.close()
      root.close()
      allocator.close()
    }
    (rows, bytes.toByteArray)
  }
}

/**
 * Columnar replacement for `BroadcastExchangeExec` over one of our plans (#325). The child runs
 * columnar and each partition's batches travel to the driver as an Arrow IPC stream; the broadcast is
 * those streams ([[VectorBroadcastBatches]]), which our broadcast joins build their tables from with no
 * row pass: no `ColumnarToRow` below the exchange, no `HashedRelation` over rows on the driver.
 *
 * A Spark consumer of the same exchange -- a Spark join above a reused exchange, dynamic partition
 * pruning's `SubqueryBroadcastExec` -- still calls `executeBroadcast` and gets Spark's own relation:
 * `mode.transform` over the batches' rows, built on the driver and broadcast on first use only. Spark's
 * `HashedRelation` is a sealed trait, so this second broadcast is the bridge; with none, it is never
 * built. The node is both columnar and row-based, so the transitions leave its child columnar under
 * either kind of consumer and put no `ColumnarToRow` above it.
 */
case class VectorBroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan)
    extends BroadcastExchangeLike
    with UnaryExecNode
    with VectorPlan {

  override def output: Seq[Attribute] = child.output

  override def supportsRowBased: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"),
    "sparkRelationTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build Spark's relation")
  )

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = VectorBroadcastExchangeExec(mode.canonicalized, child.canonicalized)

  override def runtimeStatistics: Statistics =
    Statistics(metrics("dataSize").value, Some(metrics("numOutputRows").value))

  // Materialised once; its metrics must survive, as Spark's exchange's do.
  override def resetMetrics(): Unit = ()

  @transient private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient override lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] = promise.future

  @transient private val timeout: Long = conf.broadcastTimeout

  /** Spark's limit for a relation it does not key by one long (`BytesToBytesMap`'s capacity / 1.5), kept for both. */
  @transient private lazy val maxBroadcastRows: Long = 341000000L

  private def names: Array[String] = output.indices.map(i => s"c$i").toArray
  private def types: Array[DataType] = output.map(_.dataType).toArray

  @transient override lazy val relationFuture: Future[broadcast.Broadcast[Any]] =
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session,
      VectorBroadcastExchangeExec.executionContext
    ) {
      try {
        sparkContext.addJobTag(jobTag)
        sparkContext.setInterruptOnCancel(true)
        val beforeCollect = System.nanoTime()
        val (ns, ts) = (names, types)
        val parts = child.executeColumnar().mapPartitionsInternal(it =>
          Iterator(VectorBroadcastBatches.write(it, ns, ts))
        ).collect()
        val numRows = parts.iterator.map(_._1).sum
        longMetric("numOutputRows") += numRows
        if (numRows >= maxBroadcastRows)
          throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableRowsError(maxBroadcastRows, numRows)
        val relation = new VectorBroadcastBatches(ns, ts, parts.map(_._2), numRows)
        val dataSize = relation.sizeInBytes
        longMetric("dataSize") += dataSize
        val maxBytes = conf.maxBroadcastTableSizeInBytes
        if (dataSize >= maxBytes)
          throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableBytesError(maxBytes, dataSize)
        val beforeBroadcast = System.nanoTime()
        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeCollect)
        val broadcasted = sparkContext.broadcastInternal[Any](relation, serializedOnly = true)
        longMetric("broadcastTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBroadcast)
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        promise.trySuccess(broadcasted)
        VectorBroadcastExchangeExec.afterCompletionForTests()
        broadcasted
      } catch {
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(QueryExecutionErrors.notEnoughMemoryToBuildAndBroadcastTableError(oe, Nil))
          promise.tryFailure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.tryFailure(ex)
          throw ex
        case e: Throwable =>
          promise.tryFailure(e)
          throw e
      }
    }

  override protected def doPrepare(): Unit = relationFuture

  override protected def doExecute(): RDD[InternalRow] =
    throw QueryExecutionErrors.executeCodePathUnsupportedError("VectorBroadcastExchange")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw QueryExecutionErrors.executeCodePathUnsupportedError("VectorBroadcastExchange")

  /** The batches broadcast, for our joins. */
  def executeVectorBroadcast(): broadcast.Broadcast[VectorBroadcastBatches] =
    try relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[VectorBroadcastBatches]]
    catch {
      case ex: TimeoutException =>
        if (!relationFuture.isDone) {
          sparkContext.cancelJobsWithTag(jobTag, "The corresponding broadcast query has failed.")
          relationFuture.cancel(true)
        }
        throw QueryExecutionErrors.executeBroadcastTimeoutError(timeout, Some(ex))
    }

  /**
   * Spark's relation over the same rows, for a Spark consumer: built and broadcast once, on first use.
   * Not a lazy val and not under this node's monitor: the broadcast job it waits for initialises this
   * node's lazy vals (its metrics) on another thread, which would deadlock on a monitor held here.
   */
  @transient @volatile private var sparkRelation: broadcast.Broadcast[Any] = _

  private def buildSparkRelation(): broadcast.Broadcast[Any] = {
    val batches = executeVectorBroadcast().value
    val start = System.nanoTime()
    logInfo(
      s"spark-vector: a Spark consumer reads the columnar broadcast ${nodeName} [$runId]; building Spark's relation"
    )
    val proj = UnsafeProjection.create(output, output)
    val rows = batches.batches().flatMap { b =>
      import scala.jdk.CollectionConverters._
      b.rowIterator().asScala.map(r => proj(r).copy(): InternalRow)
    }
    val relation = mode.transform(rows, Some(batches.numRows))
    val b = sparkContext.broadcastInternal[Any](relation, serializedOnly = true)
    longMetric("sparkRelationTime") += NANOSECONDS.toMillis(System.nanoTime() - start)
    b
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    if (sparkRelation == null) {
      // The batches first, outside any lock; then one builder of Spark's relation.
      executeVectorBroadcast()
      VectorBroadcastExchangeExec.sparkRelationLock.synchronized {
        if (sparkRelation == null) sparkRelation = buildSparkRelation()
      }
    }
    sparkRelation.asInstanceOf[broadcast.Broadcast[T]]
  }

  override protected def withNewChildInternal(newChild: SparkPlan): VectorBroadcastExchangeExec = copy(child = newChild)
}

object VectorBroadcastExchangeExec {

  /** Serialises the (rare) building of Spark's relation for a Spark consumer; see `doExecuteBroadcast`. */
  private val sparkRelationLock = new Object

  private[vecruntime] val executionContext: scala.concurrent.ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("vecruntime-broadcast-exchange", 128)
    )

  /**
   * Tests only (#697): runs after the completion promise is fulfilled and before `relationFuture`'s body
   * returns, so a test can widen the window in which AQE sees the stage materialised while
   * `relationFuture` is not yet done. A no-op otherwise.
   */
  @volatile private[vecruntime] var afterCompletionForTests: () => Unit = () => ()

  /** Our exchange under a join's build side: bare, as an adaptive query stage, or reused (either way). */
  def unapply(p: SparkPlan): Option[VectorBroadcastExchangeExec] = p match {
    case v: VectorBroadcastExchangeExec => Some(v)
    case s: BroadcastQueryStageExec => unapply(s.broadcast)
    case ReusedExchangeExec(_, e) => unapply(e)
    case _ => None
  }

}
