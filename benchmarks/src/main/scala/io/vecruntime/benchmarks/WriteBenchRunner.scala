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
package io.vecruntime.benchmarks

import java.net.URI

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path => HPath}

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

/**
 * The write benchmark of #64: times one source query written through Spark's file writer, so the cost of a
 * write can be split before a columnar writer is built. Each case writes the same source:
 *
 *  - `noop`: Spark's no-op sink. The scan, the source query and the columnar-to-row conversion, no encoding.
 *  - `parquet-<codec>`: Parquet through `FileFormatWriter` with that codec (`snappy`, `zstd`, `lz4_raw`,
 *    `uncompressed`, ...) to `<write-base>/<case>-<iteration>`, deleted after it is measured (kept with `--keep`, to make a source for a later run).
 *  - `parquet-<codec>@<col>[+<col>...]`: the same, partitioned by those columns (`partitionBy`).
 *
 * The difference between a Parquet case and `noop` is the write; between `parquet-uncompressed` and a codec,
 * the compression. Rows are `.jsonl` like the query runners': wall time, executor run and CPU time, GC, and
 * the records and bytes the tasks report written.
 *
 * {{{
 * spark-submit --class io.vecruntime.benchmarks.WriteBenchRunner benchmarks.jar \
 *   --config vector-shuffle --tables s3a://bucket/tpcds/sf1000/parquet --dataset sf1000-parquet \
 *   --out s3a://bucket/results/write --write-base s3a://bucket/scratch/write \
 *   --cases noop,parquet-snappy,parquet-zstd,parquet-uncompressed --iterations 2
 * }}}
 *
 * The query runners' `--cluster`, `--warmup` and `--queries` are accepted, so the runner can be started from
 * the same manifest (`benchmarks/k8s/render-run.sh`); `--hold-secs` keeps the application alive at the end,
 * for copying executor flight recordings.
 */
object WriteBenchRunner {

  final case class Args(
      config: String = "vector-shuffle",
      tables: String = "",
      dataset: String = "",
      out: String = "",
      writeBase: String = "",
      source: String = "SELECT * FROM store_sales",
      sourceTables: Seq[String] = Seq("store_sales"),
      cases: Seq[String] = Seq("noop", "parquet-snappy"),
      iterations: Int = 1,
      label: String = "write",
      holdSecs: Int = 0,
      keep: Boolean = false
  )

  /** Task-level sums of one job group: what the stage metrics of the query runners do not carry for a write. */
  final case class TaskSums(tasks: Long, runMs: Long, cpuMs: Long, gcMs: Long, records: Long, bytes: Long) {
    def json: String =
      s""""tasks":$tasks,"executorRunTimeMs":$runMs,"executorCpuTimeMs":$cpuMs,"gcTimeMs":$gcMs,""" +
        s""""recordsWritten":$records,"bytesWritten":$bytes"""
  }

  final class TaskListener extends SparkListener {
    private val stageGroup = mutable.HashMap.empty[Int, String]
    private val sums = mutable.HashMap.empty[String, TaskSums]

    override def onJobStart(e: SparkListenerJobStart): Unit = synchronized {
      Option(e.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id"))).foreach { g =>
        e.stageIds.foreach(id => stageGroup(id) = g)
      }
    }

    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
      for (g <- stageGroup.get(e.stageId); m <- Option(e.taskMetrics)) {
        val p = sums.getOrElse(g, TaskSums(0, 0, 0, 0, 0, 0))
        sums(g) = TaskSums(
          p.tasks + 1,
          p.runMs + m.executorRunTime,
          p.cpuMs + m.executorCpuTime / 1000000L,
          p.gcMs + m.jvmGCTime,
          p.records + m.outputMetrics.recordsWritten,
          p.bytes + m.outputMetrics.bytesWritten
        )
      }
    }

    def of(group: String): TaskSums = synchronized(sums.getOrElse(group, TaskSums(0, 0, 0, 0, 0, 0)))
  }

  def main(argv: Array[String]): Unit = {
    val a = parse(argv.toList, Args())
    require(a.tables.nonEmpty && a.out.nonEmpty, "--tables and --out are required")
    require(a.writeBase.nonEmpty || a.cases.forall(_ == "noop"), "--write-base is required for a parquet case")
    val spark = SparkSession.builder().getOrCreate()
    val found = ClusterRunner.registerTables(spark, a.sourceTables, a.tables)
    require(found.size == a.sourceTables.size, s"tables missing under ${a.tables}: ${a.sourceTables.diff(found)}")
    val listener = new TaskListener
    spark.sparkContext.addSparkListener(listener)
    val out = ClusterRunner.openOutput(spark, a.out, a.config, a.label)
    try {
      for (it <- 1 to a.iterations; c <- a.cases) {
        val group = s"write-$c-$it"
        val target = s"${a.writeBase.stripSuffix("/")}/$c-$it"
        spark.sparkContext.setJobGroup(group, group, interruptOnCancel = false)
        val t0 = System.nanoTime()
        try {
          val df = spark.sql(a.source)
          c match {
            case "noop" => df.write.format("noop").mode("overwrite").save()
            case p if p.startsWith("parquet-") =>
              // `parquet-<codec>` or `parquet-<codec>@<col>[+<col>]`: the latter partitioned by those columns.
              val spec = p.stripPrefix("parquet-")
              val codec = spec.takeWhile(_ != '@')
              val cols = spec.drop(codec.length + 1).split("\\+").filter(_.nonEmpty).toSeq
              df.write.mode("overwrite").option("compression", codec).partitionBy(cols: _*).parquet(target)
            case other => throw new IllegalArgumentException(s"unknown case $other")
          }
        } finally spark.sparkContext.clearJobGroup()
        val ms = (System.nanoTime() - t0) / 1e6
        org.apache.spark.sql.vecruntime.BenchmarkListenerSync.drain(spark)
        val s = listener.of(group)
        val line = s"""{"config":"${a.config}","dataset":"${a.dataset}","case":"$c","iteration":$it,""" +
          f""""wallMs":$ms%.1f,${s.json}}"""
        println(s"[write] $line")
        out.println(line)
        out.flush()
        if (c != "noop" && !a.keep) {
          val fs = FileSystem.get(new URI(target), spark.sparkContext.hadoopConfiguration)
          fs.delete(new HPath(target), true)
        }
      }
    } finally out.close()
    if (a.holdSecs > 0) {
      println(s"[write] holding ${a.holdSecs} s for the flight recordings")
      Thread.sleep(a.holdSecs * 1000L)
    }
    spark.stop()
  }

  private def parse(argv: List[String], a: Args): Args = argv match {
    case Nil => a
    case "--config" :: v :: rest => parse(rest, a.copy(config = v))
    case "--tables" :: v :: rest => parse(rest, a.copy(tables = v))
    case "--dataset" :: v :: rest => parse(rest, a.copy(dataset = v))
    case "--out" :: v :: rest => parse(rest, a.copy(out = v))
    case "--write-base" :: v :: rest => parse(rest, a.copy(writeBase = v))
    case "--source" :: v :: rest => parse(rest, a.copy(source = v))
    case "--source-tables" :: v :: rest => parse(rest, a.copy(sourceTables = v.split(",").map(_.trim).toSeq))
    case "--cases" :: v :: rest => parse(rest, a.copy(cases = v.split(",").map(_.trim).toSeq))
    case "--iterations" :: v :: rest => parse(rest, a.copy(iterations = v.toInt))
    case "--label" :: v :: rest => parse(rest, a.copy(label = v))
    case "--hold-secs" :: v :: rest => parse(rest, a.copy(holdSecs = v.toInt))
    case "--keep" :: rest => parse(rest, a.copy(keep = true))
    case "--cluster" :: rest => parse(rest, a)
    case ("--warmup" | "--queries") :: _ :: rest => parse(rest, a)
    case other :: _ => throw new IllegalArgumentException(s"unknown argument $other")
  }
}
