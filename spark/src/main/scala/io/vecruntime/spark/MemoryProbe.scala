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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext

/**
 * #925: the executor memory gauges as values a benchmark can record per query.
 *
 * The plugin registers these as executor metrics (`VectorPlugin`), which go to Spark's metrics system, not to the
 * driver. [[collect]] reads them on every executor through a small job, one snapshot per executor; [[snapshot]] and
 * [[resetPeaks]] are what that job runs. Peaks restart on reset where the source allows it (the native pool, the
 * pinned bytes, the scan's read-ahead, the JVM's heap pools and the process's peak RSS); Arrow's peak cannot be
 * reset and is reported since the executor started.
 */
object MemoryProbe {

  /** This JVM's values, by name; bytes unless the name says otherwise. */
  def snapshot(): Map[String, Long] = {
    PoolSampler.ensureStarted()
    val pool = io.vecruntime.kernels.NativePool.shared()
    val oldGen = heapPools.filter(p => p.getName.contains("Old") || p.getName.contains("Tenured"))
    val (rss, rssPeak) = processRss()
    Map(
      "offHeapExecutionPeak" -> PoolSampler.offHeapPeak.get(),
      "onHeapExecutionPeak" -> PoolSampler.onHeapPeak.get(),
      "nativePoolInUse" -> pool.inUseBytes(),
      "nativePoolPeak" -> pool.peakInUseBytes(),
      "nativePoolRetained" -> pool.retainedBytes(),
      "nativePoolOverdraft" -> org.apache.spark.sql.vecruntime.TaskNativeMemory.overdraftBytes,
      "nativePoolLeaked" -> org.apache.spark.sql.vecruntime.TaskNativeMemory.leakedBytes,
      "pinned" -> org.apache.spark.sql.vecruntime.PinnedMemory.bytes,
      "pinnedPeak" -> org.apache.spark.sql.vecruntime.PinnedMemory.peakBytes,
      "arrow" -> io.vecruntime.spark.arrow.VectorAllocators.allocatedBytes(),
      "arrowPeakSinceStart" -> io.vecruntime.spark.arrow.VectorAllocators.peakBytes(),
      "prefetchPeak" -> org.apache.spark.sql.vecruntime.VectorParquetScanExec.prefetchPeakBytes,
      "readAheadHeld" -> org.apache.spark.sql.vecruntime.VectorParquetScanExec.readAheadHeldBytes,
      "readAheadPeak" -> org.apache.spark.sql.vecruntime.VectorParquetScanExec.readAheadPeakBytes,
      "readAheadRefusals" -> org.apache.spark.sql.vecruntime.VectorParquetScanExec.readAheadRefusals,
      "heapOldPeak" -> oldGen.map(_.getPeakUsage.getUsed).sum,
      "heapOldAfterGc" -> oldGen.flatMap(p => Option(p.getCollectionUsage)).map(_.getUsed).sum,
      "rss" -> rss,
      "rssPeak" -> rssPeak
    )
  }

  /** Restarts every peak that can be restarted, so the next snapshot's peaks are this interval's. */
  def resetPeaks(): Unit = {
    PoolSampler.reset()
    io.vecruntime.kernels.NativePool.shared().resetPeak()
    org.apache.spark.sql.vecruntime.PinnedMemory.resetPeak()
    org.apache.spark.sql.vecruntime.VectorParquetScanExec.resetPrefetchPeak()
    org.apache.spark.sql.vecruntime.VectorParquetScanExec.resetReadAheadPeak()
    heapPools.foreach(_.resetPeakUsage())
    // Writing 5 to clear_refs restarts the process's VmHWM (Linux 4.0+); where it is refused the peak is since start.
    try java.nio.file.Files.writeString(java.nio.file.Path.of("/proc/self/clear_refs"), "5")
    catch { case _: Exception => () }
  }

  /**
   * One snapshot per executor, by executor id, read by a job of a few tasks per executor (the scheduler spreads idle
   * offers round robin, so every executor gets some); executors no task reached are absent. `reset` restarts the
   * peaks after reading.
   */
  def collect(sc: SparkContext, reset: Boolean): Map[String, Map[String, Long]] = {
    val executors = math.max(1, sc.getExecutorMemoryStatus.size - 1)
    val tasks = executors * 4
    sc.parallelize(0 until tasks, tasks)
      .mapPartitions { _ =>
        val id = org.apache.spark.SparkEnv.get.executorId
        val s = snapshot()
        if (reset) resetPeaks()
        Iterator((id, s))
      }
      .collect()
      .groupBy(_._1)
      .map { case (id, snaps) =>
        // Several tasks of one executor, some reading after another's reset: every value is a peak, a monotone counter
        // or a current level, so the largest read is the interval's.
        id -> snaps.map(_._2).reduce((a, b) => a.map { case (k, v) => k -> math.max(v, b.getOrElse(k, v)) })
      }
  }

  /**
   * The per-query record: for each value its maximum and sum over executors, and for the cumulative counters
   * (overdraft, leaked) the growth since `before`.
   */
  def summarize(before: Map[String, Map[String, Long]], after: Map[String, Map[String, Long]]): Map[String, Long] = {
    if (after.isEmpty) return Map.empty
    val keys = after.values.head.keySet
    val cumulative = Set("nativePoolOverdraft", "nativePoolLeaked", "readAheadRefusals")
    keys.toSeq.flatMap { k =>
      val values = after.toSeq.map { case (id, s) =>
        val v = s.getOrElse(k, 0L)
        if (cumulative(k)) v - before.get(id).flatMap(_.get(k)).getOrElse(0L) else v
      }
      Seq(s"${k}Max" -> values.max, s"${k}Sum" -> values.sum)
    }.toMap + ("executors" -> after.size.toLong)
  }

  private def heapPools: Seq[java.lang.management.MemoryPoolMXBean] =
    java.lang.management.ManagementFactory.getMemoryPoolMXBeans.asScala.toSeq
      .filter(_.getType == java.lang.management.MemoryType.HEAP)

  /** VmRSS and VmHWM of this process in bytes, (0, 0) off Linux. */
  private def processRss(): (Long, Long) =
    try {
      val lines = java.nio.file.Files.readAllLines(java.nio.file.Path.of("/proc/self/status")).asScala
      def kb(key: String): Long =
        lines.find(_.startsWith(key)).map(_.split("\\s+")(1).toLong * 1024L).getOrElse(0L)
      (kb("VmRSS:"), kb("VmHWM:"))
    } catch { case _: Exception => (0L, 0L) }

  /**
   * The peaks of Spark's own execution pools (what `TaskMemoryManager` has granted, on and off heap), which Spark
   * keeps only as current values: a daemon thread samples them every 100 ms once the first snapshot is taken.
   */
  private object PoolSampler {
    val offHeapPeak = new java.util.concurrent.atomic.AtomicLong()
    val onHeapPeak = new java.util.concurrent.atomic.AtomicLong()
    @volatile private var started = false

    def ensureStarted(): Unit = if (!started) synchronized {
      if (!started) {
        val t = new Thread(
          () => {
            while (true) {
              val env = org.apache.spark.SparkEnv.get
              if (env != null) {
                val mm = env.memoryManager
                offHeapPeak.accumulateAndGet(mm.offHeapExecutionMemoryUsed, math.max)
                onHeapPeak.accumulateAndGet(mm.onHeapExecutionMemoryUsed, math.max)
              }
              try Thread.sleep(100)
              catch { case _: InterruptedException => () }
            }
          },
          "vecruntime-memory-probe"
        )
        t.setDaemon(true)
        t.start()
        started = true
      }
    }

    def reset(): Unit = {
      val env = org.apache.spark.SparkEnv.get
      offHeapPeak.set(if (env == null) 0L else env.memoryManager.offHeapExecutionMemoryUsed)
      onHeapPeak.set(if (env == null) 0L else env.memoryManager.onHeapExecutionMemoryUsed)
    }
  }
}
