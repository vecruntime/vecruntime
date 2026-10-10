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
package org.apache.spark.sql.vecruntime.bench

import java.lang.management.ManagementFactory
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.SparkConf
import org.apache.spark.sql.vecruntime.shuffle.flight.{FlightLocation, FlightShuffle}

/**
 * The Flight shuffle transport across the network: [[FlightBench]]'s fixture on one node, the reduce
 * side on another, so what is measured is what the loopback benchmark cannot see -- bandwidth, the
 * HTTP/2 flow-control window against the link's round trip, connections per peer, server threads
 * against a NIC rather than a memory copy.
 *
 * {{{
 * # node A: write the map files, serve them on ports 47470 and 47471, report CPU every 10 s
 * java ... FlightNetBench server port=47470 advertise=<pod ip> partitions=200 maps=16 rowsPerMap=1000000 threads=10
 * # node B: drive reduce tasks against node A for 60 s, print one JSON line per run
 * java ... FlightNetBench client host=<pod ip> port=47470 partitions=200 maps=16 rowsPerMap=1000000 \
 *          concurrency=5 seconds=60 runs=3 channelsPerPeer=1,2,4 clientWindow=0,4m,16m
 * }}}
 *
 * The data parameters (`partitions`, `maps`, `rowsPerMap`, `strings`, `compression`) must be the
 * same on both sides: the client derives the schema from them and checks that one pass over every
 * partition returns every row the server wrote. Server-side transport parameters: `threads`,
 * `chunkBytes`, `backpressureBytes`. Client-side, each a comma-separated list swept as a grid:
 * `channelsPerPeer`, `clientWindow`, `concurrency` (reduce tasks in flight, the executor's cores),
 * `rangeWidth` (partitions per task, AQE coalescing). Sizes take Spark's suffixes (`4m`).
 */
object FlightNetBench {

  private def params(args: Seq[String]): Map[String, String] = args.map { a =>
    val i = a.indexOf('=')
    require(i > 0, s"expected key=value, got '$a'")
    a.substring(0, i) -> a.substring(i + 1)
  }.toMap

  def main(args: Array[String]): Unit = args.toSeq match {
    case "server" +: rest => server(params(rest))
    case "client" +: rest => client(params(rest))
    case _ =>
      System.err.println("usage: FlightNetBench server|client key=value ...")
      sys.exit(2)
  }

  private final case class Data(partitions: Int, maps: Int, rowsPerMap: Int, strings: String, compression: String)

  private def data(p: Map[String, String]): Data = Data(
    p.getOrElse("partitions", "200").toInt,
    p.getOrElse("maps", "16").toInt,
    p.getOrElse("rowsPerMap", "1000000").toInt,
    p.getOrElse("strings", "mixed"),
    p.getOrElse("compression", "zstd")
  )

  private def cpuSeconds(): Double =
    ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
      .getProcessCpuTime / 1e9

  private def server(p: Map[String, String]): Unit = {
    val d = data(p)
    val conf = new SparkConf(false)
    p.get("backpressureBytes").foreach(conf.set(FlightShuffle.BackpressureBytesKey, _))
    val chunkBytes = conf.getSizeAsBytes(FlightShuffle.ChunkBytesKey, p.getOrElse("chunkBytes", "4m")).toInt
    val threads = p.getOrElse("threads", math.max(4, Runtime.getRuntime.availableProcessors()).toString).toInt
    val started = System.nanoTime()
    val fixture = new FlightBench.Fixture(
      d.partitions,
      d.maps,
      d.rowsPerMap,
      d.strings,
      d.compression,
      threads,
      chunkBytes,
      p.getOrElse("bind", "0.0.0.0"),
      p.getOrElse("advertise", "127.0.0.1"),
      p.getOrElse("port", "47470").toInt,
      conf
    )
    sys.addShutdownHook(fixture.close())
    println(
      f"""{"event":"ready","writeSeconds":${(System.nanoTime() - started) / 1e9}%.1f,"rows":${fixture.rowsWritten},""" +
        s""""locations":"${fixture.locations.map(l => s"${l.host}:${l.port}").mkString(",")}","threads":$threads,""" +
        s""""chunkBytes":$chunkBytes,"backpressureBytes":${p.getOrElse("backpressureBytes", "0")}}"""
    )
    // The server's own CPU, cumulative: the client's runs are timestamped, so a run's server CPU is
    // the difference of the two readings around it.
    while (true) {
      Thread.sleep(10000)
      println(f"""{"event":"cpu","epochMs":${System.currentTimeMillis()},"cpuSeconds":${cpuSeconds()}%.2f}""")
    }
  }

  private def list(p: Map[String, String], key: String, default: String): Seq[String] =
    p.getOrElse(key, default).split(',').map(_.trim).filter(_.nonEmpty).toSeq

  private def client(p: Map[String, String]): Unit = {
    val d = data(p)
    val host = p.getOrElse("host", "127.0.0.1")
    val port = p.getOrElse("port", "47470").toInt
    val locations = (0 until FlightBench.Servers).map(i => FlightLocation(host, port + i))
    val schema = FlightBench.schemaFor(d.strings)
    val codec = FlightBench.codecFor(d.compression)
    val mapIds = (0 until d.maps).map(_.toLong)
    val seconds = p.getOrElse("seconds", "60").toInt
    val runs = p.getOrElse("runs", "3").toInt
    val allocator = new RootAllocator()
    val expected = FlightBench.Servers.toLong * d.maps * d.rowsPerMap

    for {
      channels <- list(p, "channelsPerPeer", "1")
      window <- list(p, "clientWindow", "0")
      concurrency <- list(p, "concurrency", "5").map(_.toInt)
      width <- list(p, "rangeWidth", "1").map(_.toInt)
    } {
      val conf = new SparkConf(false)
        .set(FlightShuffle.ChannelsPerPeerKey, channels)
        .set(FlightShuffle.ClientWindowKey, window)
      def task(reduce: Int, metrics: org.apache.spark.executor.TempShuffleReadMetrics): Long =
        FlightBench.reduceTask(locations, mapIds, reduce, width, schema, codec, conf, allocator, metrics, _ => {})

      // One pass over every partition: every row the server wrote comes back (and warms the channels).
      val check = new org.apache.spark.executor.TempShuffleReadMetrics()
      val read = (0 until d.partitions by width).map(r => task(r, check)).sum
      require(read == expected, s"read $read rows of $expected written: the data parameters differ from the server's")

      (1 to runs).foreach { run =>
        val pool = Executors.newFixedThreadPool(concurrency)
        val next = new AtomicInteger()
        val rows = new AtomicLong()
        val bytes = new AtomicLong()
        val tasks = new AtomicLong()
        val fetchWaitMs = new AtomicLong()
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
        val cpu0 = cpuSeconds()
        val wall0 = System.nanoTime()
        val startedMs = System.currentTimeMillis()
        val workers = (1 to concurrency).map(_ =>
          new Callable[Unit] {
            override def call(): Unit = while (System.nanoTime() < deadline) {
              val reduce = Math.floorMod(next.getAndAdd(width), d.partitions - width + 1)
              val metrics = new org.apache.spark.executor.TempShuffleReadMetrics()
              rows.addAndGet(task(reduce, metrics))
              bytes.addAndGet(metrics.remoteBytesRead)
              fetchWaitMs.addAndGet(metrics.fetchWaitTime)
              tasks.incrementAndGet()
            }
          }
        )
        try pool.invokeAll(workers.asJava).asScala.foreach(_.get())
        finally pool.shutdownNow()
        val wall = (System.nanoTime() - wall0) / 1e9
        val cpu = cpuSeconds() - cpu0
        val gb = bytes.get / 1e9
        println(
          s"""{"event":"run","run":$run,"startMs":$startedMs,"endMs":${System.currentTimeMillis()},""" +
            s""""channelsPerPeer":"$channels","clientWindow":"$window","concurrency":$concurrency,"rangeWidth":$width,""" +
            f""""seconds":$wall%.2f,"tasks":${tasks.get},"rows":${rows.get},"bytes":${bytes.get},""" +
            f""""gbPerSecond":${gb / wall}%.3f,"gbitPerSecond":${gb * 8 / wall}%.2f,""" +
            f""""clientCpuSecondsPerGb":${if (gb > 0) cpu / gb else 0.0}%.2f,"fetchWaitMs":${fetchWaitMs.get}}"""
        )
      }
    }
    allocator.close()
  }
}
