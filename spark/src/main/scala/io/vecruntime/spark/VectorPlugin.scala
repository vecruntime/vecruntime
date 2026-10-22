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
package io.vecruntime.spark

import java.util.Collections

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging

/**
 * Spark plugin entry point (`spark.plugins=io.vecruntime.spark.VectorPlugin`). The driver side
 * appends [[VectorSparkSessionExtensions]] to `spark.sql.extensions` so users need a single config
 * key, and attaches the Vector Acceleration tab to the Spark UI; there is no executor-side
 * component beyond the Comet adapter registration.
 *
 * The JVM must be started with `--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED`
 * on both driver and executors; the plugin fails fast if the Vector API module is missing.
 */
class VectorPlugin extends SparkPlugin {

  override def driverPlugin(): DriverPlugin = new DriverPlugin with Logging {
    override def init(sc: SparkContext, ctx: PluginContext): java.util.Map[String, String] = {
      VectorPlugin.requireVectorApi()
      val key = "spark.sql.extensions"
      val ext = classOf[VectorSparkSessionExtensions].getName
      val existing = ctx.conf().getOption(key).map(_.split(",").map(_.trim).filter(_.nonEmpty)).getOrElse(Array.empty)
      if (!existing.contains(ext)) {
        ctx.conf().set(key, (existing :+ ext).mkString(","))
        logInfo(s"spark-vector: registered $ext in $key")
      }
      // The UI tab needs the SparkContext, which only exists here.
      val get: String => Option[String] = k => ctx.conf().getOption(k)
      // #689: AQE starts from enough partitions for the executors' cores and coalesces down, unless set.
      VectorConf.initialShufflePartitions(get).foreach { n =>
        ctx.conf().set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", n.toString)
        logInfo(
          s"spark-vector: AQE starts from $n shuffle partitions (spark.vecruntime.shuffle.initialPartitionsPerCore)"
        )
      }
      // #788: a stage AQE submitted just before its query returned is cancelled, not left to run under the next.
      if (VectorConf.cancelEndedExecutionJobs(get)) {
        sc.addSparkListener(new org.apache.spark.sql.vecruntime.EndedExecutionJobCanceller((job, reason) =>
          sc.cancelJob(job, reason)
        ))
      }
      org.apache.spark.sql.vecruntime.ui.VectorUi.attach(
        sc,
        enabled = VectorConf.uiEnabled(get),
        retainedExecutions = VectorConf.uiRetainedExecutions(get)
      )
      Collections.emptyMap()
    }

    /** The columnar shuffle's Flight location registry (#288), when its module is on the classpath. */
    override def receive(message: AnyRef): AnyRef =
      org.apache.spark.sql.vecruntime.VectorShuffle.driverReceive(message)
  }

  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
    override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
      VectorPlugin.requireVectorApi()
      io.vecruntime.spark.comet.CometVectorAdapter.tryRegister()
      // #869 step 4: before anything opens the Arrow root -- its buffers from the native pool when on.
      io.vecruntime.kernels.NativePool.configureThreadCache(
        VectorConf.nativePoolThreadCacheDepth(ctx.conf()),
        VectorConf.nativePoolThreadCacheMaxBlockBytes(ctx.conf())
      )
      io.vecruntime.spark.arrow.VectorAllocators.useNativePool(VectorConf.arrowNativePoolEnabled(ctx.conf()))
      // #868: the executor's Arrow budget, and its use as executor metrics (current and peak bytes) for sizing it.
      io.vecruntime.spark.arrow.VectorAllocators.setLimit(VectorConf.arrowMaxBytes(ctx.conf()))
      ctx.metricRegistry().register(
        "arrowAllocatedBytes",
        new com.codahale.metrics.Gauge[Long] {
          override def getValue: Long = io.vecruntime.spark.arrow.VectorAllocators.allocatedBytes()
        }
      )
      ctx.metricRegistry().register(
        "arrowPeakBytes",
        new com.codahale.metrics.Gauge[Long] {
          override def getValue: Long = io.vecruntime.spark.arrow.VectorAllocators.peakBytes()
        }
      )
      // #875: the execution memory pinned by consumers that do not spill on request (join builds, aggregates).
      ctx.metricRegistry().register(
        "pinnedUnspillableBytes",
        new com.codahale.metrics.Gauge[Long] {
          override def getValue: Long = org.apache.spark.sql.vecruntime.PinnedMemory.bytes
        }
      )
      ctx.metricRegistry().register(
        "pinnedUnspillablePeakBytes",
        new com.codahale.metrics.Gauge[Long] {
          override def getValue: Long = org.apache.spark.sql.vecruntime.PinnedMemory.peakBytes
        }
      )
      // #869: the native pool's totals as executor metrics, and (when on) every task's pooled blocks in its budget.
      if (VectorConf.poolAccountingEnabled(ctx.conf())) org.apache.spark.sql.vecruntime.TaskNativeMemory.install()
      // #926: the operators' share of the off-heap pool evaluated per task, from how many tasks run now.
      org.apache.spark.sql.vecruntime.TaskShare.configure(VectorConf.liveTaskShareEnabled(ctx.conf()))
      Seq[(String, () => Long)](
        "nativePoolInUseBytes" -> (() => io.vecruntime.kernels.NativePool.shared().inUseBytes()),
        "nativePoolPeakBytes" -> (() => io.vecruntime.kernels.NativePool.shared().peakInUseBytes()),
        "nativePoolRetainedBytes" -> (() => io.vecruntime.kernels.NativePool.shared().retainedBytes()),
        "nativePoolOverdraftBytes" -> (() => org.apache.spark.sql.vecruntime.TaskNativeMemory.overdraftBytes),
        "nativePoolLeakedBytes" -> (() => org.apache.spark.sql.vecruntime.TaskNativeMemory.leakedBytes),
        // #915: the scan's read-ahead held in execution memory, its peak, and the grants the pool refused.
        "scanReadAheadHeldBytes" -> (() => org.apache.spark.sql.vecruntime.VectorParquetScanExec.readAheadHeldBytes),
        "scanReadAheadPeakBytes" -> (() => org.apache.spark.sql.vecruntime.VectorParquetScanExec.readAheadPeakBytes),
        "scanReadAheadRefusals" -> (() => org.apache.spark.sql.vecruntime.VectorParquetScanExec.readAheadRefusals)
      ).foreach { case (name, read) =>
        ctx.metricRegistry().register(
          name,
          new com.codahale.metrics.Gauge[Long] { override def getValue: Long = read() }
        )
      }
      // Starts the Flight shuffle server on this executor when the shuffle module is present, its
      // manager configured and the backend is Flight (#288).
      org.apache.spark.sql.vecruntime.VectorShuffle.executorInit(ctx)
    }

    override def shutdown(): Unit = org.apache.spark.sql.vecruntime.VectorShuffle.executorShutdown()

    // Spark calls these on the task's own thread once the task ends (#667): the hash tables' per-thread
    // scratch keeps no batch, dictionary or table, and none of the arrays a build side grew it to, into
    // the thread's next task.
    override def onTaskStart(): Unit = org.apache.spark.sql.vecruntime.TaskShare.taskStarted() // #926

    override def onTaskSucceeded(): Unit = {
      org.apache.spark.sql.vecruntime.TaskShare.taskEnded()
      io.vecruntime.kernels.GroupKeyTable.releaseThreadScratch()
    }

    override def onTaskFailed(failureReason: org.apache.spark.TaskFailedReason): Unit = {
      org.apache.spark.sql.vecruntime.TaskShare.taskEnded()
      io.vecruntime.kernels.GroupKeyTable.releaseThreadScratch()
    }
  }
}

object VectorPlugin {
  def requireVectorApi(): Unit = {
    if (!ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent) {
      throw new IllegalStateException(
        "spark-vector requires the Java Vector API: start the JVM with " +
          "--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED " +
          "(spark.driver.extraJavaOptions / spark.executor.extraJavaOptions)"
      )
    }
  }
}
