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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.statistics.IOStatisticsSupport

import org.apache.spark.internal.Logging

/**
 * What the executor's file systems say they read, against what the tasks report (#638). The task input
 * metric adds up the Hadoop per-THREAD byte counts of the task and its prefetch threads; a stream that does
 * its reads on threads of its own (S3A's Analytics Accelerator) is invisible to it. Under
 * `-Dvecruntime.scan.ioStatsEverySecs=N` (off by default) every file system a scan opens is registered here,
 * and a daemon thread logs, every N seconds and at exit, one `#638:` line per file system: the all-thread
 * `FileSystem.Statistics` bytes read and read ops, and the store's own IOStatistics counters that measure
 * reads and requests (`stream_read_bytes`, `action_http_get_request`, ...). Summed over the executors, these
 * are compared with the sum of the tasks' `bytesRead` from the event log.
 */
private[vecruntime] object ScanIoStats extends Logging {

  private val everySecs: Long = java.lang.Long.getLong("vecruntime.scan.ioStatsEverySecs", 0L)
  val enabled: Boolean = everySecs > 0

  private val fileSystems = ConcurrentHashMap.newKeySet[FileSystem]()

  /** Counter names kept: reads, bytes and requests, not the whole (large) IOStatistics map. */
  private def kept(name: String): Boolean = {
    val n = name.toLowerCase(java.util.Locale.ROOT)
    n.contains("read") || n.contains("get_request") || n.contains("bytes") || n.contains("open")
  }

  private lazy val reporter: Unit = {
    val t = new Thread(
      () => {
        try {
          while (true) {
            Thread.sleep(everySecs * 1000L)
            report()
          }
        } catch { case _: InterruptedException => () }
      },
      "vecruntime-scan-io-stats"
    )
    t.setDaemon(true)
    t.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => report(), "vecruntime-scan-io-stats-exit"))
  }

  /** Registers the file system a scan opened (a no-op when off). */
  def register(fs: FileSystem): Unit =
    if (enabled && fs != null && fileSystems.add(fs)) reporter

  private[vecruntime] def line(fs: FileSystem): String = {
    val b = new StringBuilder(s"#638: io t=${System.currentTimeMillis()} fs=${fs.getScheme}")
    // Every Statistics of this scheme, summed: they are keyed by the class that records them, and a wrapper
    // (LocalFileSystem over RawLocalFileSystem) records under the class it wraps, not under its own.
    val all = FileSystem.getAllStatistics.asScala.filter(_.getScheme == fs.getUri.getScheme)
    b.append(s" fsBytesRead=${all.map(_.getBytesRead).sum} fsReadOps=${all.map(_.getReadOps.toLong).sum}")
    val io = IOStatisticsSupport.retrieveIOStatistics(fs)
    if (io != null) {
      io.counters().asScala.toSeq.filter { case (k, v) => v != 0L && kept(k) }.sortBy(_._1).foreach {
        case (k, v) => b.append(' ').append(k).append('=').append(v)
      }
    }
    b.toString
  }

  private def report(): Unit =
    fileSystems.asScala.foreach { fs =>
      try logInfo(line(fs))
      catch { case e: Throwable => logWarning(s"#638: could not read IO statistics: $e") }
    }
}
