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
package org.apache.spark.sql.vecruntime.shuffle

import java.lang.foreign.Arena
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

import scala.collection.mutable

import io.vecruntime.kernels.ArrowLayout
import io.vecruntime.shuffle.{PartitionedIpcFile, PartitionedIpcWriter}
import io.vecruntime.spark.arrow.ArrowOutput
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.SparkConf
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerJobEnd,
  SparkListenerJobStart,
  SparkListenerStageCompleted,
  StageInfo
}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.vecruntime.VectorShuffleExchangeExec
import org.apache.spark.sql.vecruntime.shuffle.flight.{FlightBlockStream, FlightLocation, FlightShuffle}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #932: the reduce side reports Spark's standard ShuffleReadMetrics and our block-read SQL metric.
 *
 *  - fetchWaitTime was 0 in every task: `FlightBlockStream` timed only the non-blocking `getStream`,
 *    while the task thread actually blocks in `stream.next()` draining the gRPC stream. A producer
 *    that sleeps before its first message makes that wait visible, and the fix charges it to
 *    fetchWaitTime.
 *  - the remote read reports remote blocks, bytes and records with Spark's meaning.
 *  - end to end on a local shuffle, bytes read equals bytes written (local semantics) and the
 *    exchange's `blockReadTime` metric is nonzero.
 */
class ShuffleReadMetricsSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val schema =
    org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("k", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("s", org.apache.spark.sql.types.StringType)
    ))

  /** A one-partition block of `n` rows, written to `path`, returned as the bytes a transport delivers (#416). */
  private def block(allocator: org.apache.arrow.memory.BufferAllocator, path: Path, n: Int): Array[Byte] = {
    val writer = new PartitionedIpcWriter(schema, 1, allocator, path, 1L << 20, batchRows = 1024)
    val arena = Arena.ofConfined()
    try {
      val ks = Array.tabulate(n)(i => i)
      val ss = Array.tabulate(n)(i => s"v-${i % 101}")
      val buffers = Array(ArrowLayout.ofInts(arena, ks, Array.fill(n)(false)), ArrowLayout.ofStrings(arena, ss))
      val all = arena.allocate(io.vecruntime.kernels.Bitmap.bytesFor(n), 8)
      io.vecruntime.kernels.Bitmap.fill(all, n, true)
      val columns: Array[ColumnVector] = schema.fields.indices.toArray.map { c =>
        ArrowOutput.compact(schema.fields(c).name, schema.fields(c).dataType, buffers(c), all, n, allocator)
      }
      val b = new ColumnarBatch(columns, n)
      try writer.write(b, new Array[Int](n))
      finally b.close()
      writer.finish()
    } finally { arena.close(); writer.close() }
    PartitionedIpcFile.blockBytes(path, 0, 1)
  }

  test(
    "#932: a slowed Flight block source makes fetchWaitTime nonzero; remote blocks, bytes and records are reported"
  ) {
    val allocator = new RootAllocator()
    val dir = Files.createTempDirectory("svmetrics")
    try {
      val bytes = block(allocator, dir.resolve("block.ipc"), 2000)
      val delayMs = 150L
      // A producer that sleeps before serving the block: the task thread blocks in stream.next().
      val producer = new FlightShuffle.Producer(
        (_: Int, _: Long, _: Int) => { Thread.sleep(delayMs); new NioManagedBuffer(ByteBuffer.wrap(bytes)) },
        allocator
      )
      val server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer).build()
      server.start()
      try {
        // With the sleep: fetchWaitTime picks up the block it waited for.
        val slow = new TempShuffleReadMetrics()
        val stream = new FlightBlockStream(
          FlightLocation("127.0.0.1", server.getPort),
          0,
          0L,
          0,
          schema,
          new SparkConf(false),
          FlightShuffle.Clients.allocatorForReads,
          slow
        )
        var rows = 0
        try while (stream.hasNext) rows += stream.next().numRows()
        finally stream.close()
        assert(rows === 2000, "every row read back")
        assert(
          slow.fetchWaitTime >= delayMs - 50,
          s"fetchWaitTime ${slow.fetchWaitTime} ms should reflect the ~$delayMs ms the source slept"
        )
        assert(
          slow.remoteBytesRead === bytes.length.toLong,
          s"remote bytes read ${slow.remoteBytesRead} vs ${bytes.length}"
        )
        assert(slow.remoteBytesReadToDisk === 0L, "the Flight path keeps blocks in memory")
      } finally server.close()
    } finally allocator.close()
  }

  test("#932: an unslowed source keeps fetchWaitTime small, so the metric measures the stall, not a constant") {
    val allocator = new RootAllocator()
    val dir = Files.createTempDirectory("svmetrics")
    try {
      val bytes = block(allocator, dir.resolve("block.ipc"), 2000)
      val producer = new FlightShuffle.Producer(
        (_: Int, _: Long, _: Int) => new NioManagedBuffer(ByteBuffer.wrap(bytes)),
        allocator
      )
      val server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer).build()
      server.start()
      try {
        val fast = new TempShuffleReadMetrics()
        val stream = new FlightBlockStream(
          FlightLocation("127.0.0.1", server.getPort),
          0,
          0L,
          0,
          schema,
          new SparkConf(false),
          FlightShuffle.Clients.allocatorForReads,
          fast
        )
        try while (stream.hasNext) stream.next()
        finally stream.close()
        // A warm in-process server answers in well under the 150 ms the slowed test asserts: the
        // metric reflects the source's latency rather than a fixed overhead of our own.
        assert(
          fast.fetchWaitTime < 100,
          s"a warm source should not report a large fetch wait (${fast.fetchWaitTime} ms)"
        )
      } finally server.close()
    } finally allocator.close()
  }

  // ---- end to end: local shuffle semantics and the block-read SQL metric ----

  private var spark: SparkSession = _
  private var tempDir: Path = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("vecruntime-shuffle-metrics")
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("ShuffleReadMetricsSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.sql.warehouse.dir", tempDir.resolve("wh").toString)
      .config("spark.plugins", "io.vecruntime.spark.VectorPlugin")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .getOrCreate()
    spark.range(0, 20000).selectExpr(
      "cast(id % 97 as int) as k",
      "cast(id % 7 as double) / 3 as x",
      "cast(id % 1000 as string) as s"
    ).write.mode("overwrite").parquet(tempDir.resolve("t").toString)
    spark.read.parquet(tempDir.resolve("t").toString).createOrReplaceTempView("t")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def collectPlan(df: DataFrame): SparkPlan = {
    df.collect()
    df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case p => p
    }
  }

  private def nodes(plan: SparkPlan): Seq[SparkPlan] = plan +: (plan match {
    case q: QueryStageExec => nodes(q.plan)
    case p => p.children.flatMap(nodes)
  })

  test("#932: local shuffle totals match Spark's semantics and blockReadTime is reported") {
    // Count only this test's own stages (the listener bus is async, so earlier tests' stage-completed
    // events can still arrive here -- the PR #972 pattern): tag a job group, record its stage ids on
    // onJobStart, wait for its jobs to end, then read only those stages' totals.
    val group = s"vec-read-metrics-${System.nanoTime()}"
    val ownStages = mutable.Set.empty[Int]
    val runningJobs = mutable.Set.empty[Int]
    val stages = mutable.ArrayBuffer.empty[StageInfo]
    val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit =
        if (e.properties != null && e.properties.getProperty("spark.jobGroup.id") == group) stages.synchronized {
          ownStages ++= e.stageIds
          runningJobs += e.jobId
        }
      override def onJobEnd(e: SparkListenerJobEnd): Unit = stages.synchronized { runningJobs -= e.jobId }
      override def onStageCompleted(e: SparkListenerStageCompleted): Unit =
        stages.synchronized { if (ownStages.contains(e.stageInfo.stageId)) stages += e.stageInfo }
    }
    spark.sparkContext.addSparkListener(listener)
    spark.sparkContext.setJobGroup(group, "read metrics test", interruptOnCancel = false)
    try {
      val df = spark.sql("select k, count(*) c, sum(x) sx from t group by k")
      val plan = collectPlan(df)
      val deadline = System.nanoTime() + 20000000000L
      while (
        System.nanoTime() < deadline && stages.synchronized(
          ownStages.isEmpty || runningJobs.nonEmpty ||
            stages.map(_.taskMetrics.shuffleReadMetrics.localBlocksFetched).sum == 0
        )
      ) Thread.sleep(50)
      val ex = nodes(plan).collectFirst { case e: VectorShuffleExchangeExec => e }.get
      val write = stages.synchronized(stages.map(_.taskMetrics.shuffleWriteMetrics.bytesWritten).sum)
      val read = stages.synchronized(stages.map(_.taskMetrics.shuffleReadMetrics).toIndexedSeq)
      assert(write > 0, "shuffle bytes written")
      // local[4]: every block is local, so remote totals are zero and bytes read == bytes written.
      assert(read.map(_.remoteBlocksFetched).sum === 0, "no remote blocks in local mode")
      assert(read.map(_.remoteBytesRead).sum === 0, "no remote bytes in local mode")
      assert(read.map(_.localBlocksFetched).sum > 0, "local blocks fetched")
      assert(
        read.map(_.totalBytesRead).sum === write,
        s"bytes read ${read.map(_.totalBytesRead).sum} vs written $write (local-fetch semantics)"
      )
      assert(read.map(_.recordsRead).sum > 0, "records read")
      assert(read.map(_.fetchWaitTime).sum >= 0, "fetch wait is a nonnegative duration")
      // The block-read SQL metric is wired and nonzero -- the decode happened on the reduce task.
      assert(
        ex.metrics(VectorShuffleExchangeExec.BlockReadTime).value > 0,
        s"blockReadTime ${ex.metrics(VectorShuffleExchangeExec.BlockReadTime).value} should be nonzero"
      )
    } finally {
      spark.sparkContext.clearJobGroup()
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
