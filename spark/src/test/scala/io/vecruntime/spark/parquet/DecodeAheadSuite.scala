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
package io.vecruntime.spark.parquet

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.arrow.VectorAllocators
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.vecruntime.{DecodeAheadIterator, PlanUtils, VectorParquetScanExec}

/**
 * #606: decode-ahead in the native Parquet scan. With `decodeAhead` = K > 0 a producer thread per task decodes
 * up to K batches ahead; the results must be the reader's, in its order, on virtual and platform threads, across
 * row groups, files and per-file fallbacks, and nothing may leak when a LIMIT or a failure ends the task early.
 */
class DecodeAheadSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (64 * 1024).toString, // many row groups per file
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "512"
    )

  private val node = classOf[VectorParquetScanExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id AS INT) AS i,
          |  CASE WHEN id % 7 = 0 THEN NULL ELSE CAST(id * 2654435761 AS BIGINT) END AS l,
          |  CASE WHEN id % 5 = 0 THEN NULL ELSE CONCAT('v', CAST(id % 97 AS STRING)) END AS s,
          |  CAST(id * 1.5 AS DOUBLE) AS d,
          |  CAST(id % 1000 AS DECIMAL(25,2)) AS w
          |FROM range(0, 60000)""".stripMargin
      ).repartition(3).write.mode("overwrite").parquet(newTempPath("da/t"))
      spark.read.parquet(newTempPath("da/t")).createOrReplaceTempView("da_t")
    }
  }

  /** Row groups the native scan read (its `numRowGroups` metric) for `sql`. */
  private def nativeRowGroups(sql: String): Long = withPlugin(enabled = true) {
    val df = spark.sql(sql)
    df.collect()
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
      .map(_.metrics("numRowGroups").value).sum
  }

  private def parquetFiles(dir: String): Int =
    new java.io.File(dir).listFiles().count(_.getName.endsWith(".parquet"))

  test("the test table spans many row groups per file, all decoded natively") {
    val files = parquetFiles(newTempPath("da/t"))
    val groups = nativeRowGroups("SELECT * FROM da_t")
    // 3 files of 20,000 rows under 64 KB blocks: several row groups each, so decode-ahead crosses row-group
    // and file boundaries in the tests below.
    assert(files === 3, s"$files files")
    assert(groups >= 4L * files, s"only $groups row groups in $files files")
    withDecodeAhead(4, "virtual")(assert(nativeRowGroups("SELECT * FROM da_t") === groups))
  }

  private def withDecodeAhead[T](k: Int, threads: String)(f: => T): T =
    withConf(
      VectorConf.ScanNativeParquetDecodeAhead -> k.toString,
      VectorConf.ScanNativeParquetDecodeAheadThreads -> threads
    )(f)

  private val queries = Seq(
    "SELECT * FROM da_t",
    "SELECT i, s FROM da_t WHERE i % 3 = 0",
    "SELECT s, count(*), sum(l), max(d), sum(w) FROM da_t GROUP BY s"
  )

  for (k <- Seq(1, 4); threads <- Seq("virtual", "platform")) {
    test(s"decodeAhead=$k on $threads threads returns the reader's rows across row groups and files") {
      val before = DecodeAheadIterator.started.get()
      val beforeVirtual = DecodeAheadIterator.startedVirtual.get()
      withDecodeAhead(k, threads) {
        queries.foreach(q => checkVectorized(q, Seq(node)))
      }
      val producers = DecodeAheadIterator.started.get() - before
      assert(producers > 0, "no decode-ahead producer ran")
      val virtual = DecodeAheadIterator.startedVirtual.get() - beforeVirtual
      if (threads == "virtual") assert(virtual === producers, s"$virtual of $producers producers were virtual")
      else assert(virtual === 0L, s"$virtual producers were virtual on the platform setting")
    }
  }

  test("decodeAhead=0 (the default) starts no producer") {
    val before = DecodeAheadIterator.started.get()
    spark.conf.unset(VectorConf.ScanNativeParquetDecodeAhead)
    checkVectorized("SELECT i, s FROM da_t WHERE i % 3 = 0", Seq(node))
    assert(DecodeAheadIterator.started.get() === before)
  }

  test("a LIMIT that ends the task early leaks no vectors") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    withDecodeAhead(4, "virtual") {
      checkVectorized("SELECT i, s, w FROM da_t LIMIT 7", Seq(node))
      checkVectorized("SELECT * FROM da_t WHERE i > 100 LIMIT 1000", Seq(node))
    }
    // Every reader's allocator is closed at task end (it throws on a leak); the root is back where it was.
    assert(root.getAllocatedMemory === before, s"allocated ${root.getAllocatedMemory} vs $before before")
  }

  test("a table mixing natively decoded and fallback files (Spark's recycled batches) reads in order") {
    val path = newTempPath("da/mixed")
    withPlugin(enabled = false) {
      val q = "SELECT CAST(id AS INT) AS i, TIMESTAMP_MICROS(id * 1000003) AS ts FROM range(0, 20000)"
      withConf("spark.sql.parquet.outputTimestampType" -> "INT96") {
        spark.sql(q).repartition(2).write.mode("overwrite").parquet(path + "/int96")
      }
      withConf("spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS") {
        spark.sql(q).repartition(2).write.mode("overwrite").parquet(path + "/micros")
      }
      spark.read.parquet(path + "/int96", path + "/micros").createOrReplaceTempView("da_mixed")
    }
    for (threads <- Seq("virtual", "platform")) {
      withDecodeAhead(4, threads) {
        // One split holds both kinds of files.
        withConf("spark.sql.files.maxPartitionBytes" -> (64L << 20).toString) {
          checkVectorized("SELECT * FROM da_mixed", Seq(node))
          checkVectorized("SELECT count(*), sum(i), min(ts), max(ts) FROM da_mixed", Seq(node))
        }
      }
    }
  }

  test("a failure on the producer surfaces on the task thread") {
    val path = newTempPath("da/corrupt")
    withPlugin(enabled = false) {
      withConf("parquet.enable.dictionary" -> "false") {
        spark.range(0, 200000).selectExpr("CAST(id * 7919 AS INT) AS i").repartition(1).write.mode("overwrite")
          .parquet(path)
      }
    }
    // Corrupt a LATER row group, keeping the first one and the footer intact: the first batch (task thread)
    // decodes, and the failure comes from the producer.
    val f = new java.io.File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
    val raf = new java.io.RandomAccessFile(f, "rw")
    try {
      raf.seek(raf.length() * 6 / 10)
      raf.write(Array.fill[Byte](4096)(0x5a.toByte))
    } finally raf.close()
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("da_corrupt"))
    val serial = intercept[Throwable](withPlugin(enabled = true)(spark.sql("SELECT sum(i) FROM da_corrupt").collect()))
    withDecodeAhead(2, "virtual") {
      val ahead = intercept[Throwable](withPlugin(enabled = true)(spark.sql("SELECT sum(i) FROM da_corrupt").collect()))
      assert(ahead.getClass === serial.getClass, s"${ahead.getClass} vs ${serial.getClass}")
    }
  }

  /**
   * Runs `sql` with one task over every file of `da_many` (many small files, several row groups each),
   * returning its rows and the executor's peak read-ahead bytes; every byte reserved is given back after.
   */
  private def readAhead(sql: String, confs: (String, String)*): (Seq[org.apache.spark.sql.Row], Long) = {
    VectorParquetScanExec.resetPrefetchPeak()
    val rows = withConf(
      Seq(
        "spark.sql.files.maxPartitionBytes" -> (1L << 30).toString,
        "spark.sql.files.openCostInBytes" -> "0",
        VectorConf.ScanNativeParquetPrefetchFiles -> "6",
        VectorConf.ScanNativeParquetPrefetchRowGroups -> "4"
      ) ++ confs: _*
    )(withPlugin(enabled = true)(spark.sql(sql).collect().toSeq))
    assert(VectorParquetScanExec.prefetchInFlightBytes === 0, "read-ahead bytes were not given back")
    (rows, VectorParquetScanExec.prefetchPeakBytes)
  }

  private lazy val manyFiles: (String, Long, Long) = {
    val path = newTempPath("da/many")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CONCAT('v', CAST(id % 997 AS STRING)) AS s FROM range(0, 120000)")
        .repartition(12).write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("da_many")
    }
    val sizes = new java.io.File(path).listFiles().filter(_.getName.endsWith(".parquet")).map(_.length)
    (path, sizes.max, sizes.sum)
  }

  test("read-ahead is capped in bytes per task, and gives every byte back (#568)") {
    val (_, largest, total) = manyFiles
    val q = "SELECT i, s FROM da_many"
    val expected = withPlugin(enabled = false)(spark.sql(q).collect().toSeq).map(_.toString).sorted
    val (free, freePeak) = readAhead(q, VectorConf.ScanNativeParquetPrefetchMaxBytes -> "0")
    val (capped, cappedPeak) = readAhead(q, VectorConf.ScanNativeParquetPrefetchMaxBytes -> "1")
    assert(free.map(_.toString).sorted === expected)
    assert(capped.map(_.toString).sorted === expected)
    // Uncapped, six files open ahead; capped at one byte, one file and one row group (always allowed).
    assert(freePeak > 2 * largest, s"uncapped peak $freePeak, largest file $largest")
    assert(cappedPeak <= 2 * largest, s"capped peak $cappedPeak, largest file $largest")
    assert(freePeak <= total, s"peak $freePeak over every file's bytes $total")
  }

  test("the executor-wide read-ahead cap bounds every task together (#568)") {
    val (_, largest, _) = manyFiles
    val q = "SELECT count(*), max(s) FROM da_many"
    val (rows, peak) = readAhead(
      q,
      VectorConf.ScanNativeParquetPrefetchMaxBytes -> "0",
      VectorConf.ScanNativeParquetPrefetchExecutorMaxBytes -> "1"
    )
    assert(rows.map(_.toString) === withPlugin(enabled = false)(spark.sql(q).collect().toSeq).map(_.toString))
    assert(peak <= 2 * largest, s"peak $peak under a one-byte executor cap, largest file $largest")
    withDecodeAhead(2, "virtual") {
      val (rows2, _) = readAhead(q, VectorConf.ScanNativeParquetPrefetchMaxBytes -> "1")
      assert(rows2.map(_.toString) === rows.map(_.toString))
    }
  }

  test("a file whose open ahead fails gives its read-ahead bytes back (#568)") {
    val path = newTempPath("da/badopen")
    withPlugin(enabled = false) {
      spark.range(0, 60000).selectExpr("CAST(id AS INT) AS i").repartition(6).write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("da_badopen")
    }
    // Break the trailing magic of one file after the listing (same length), so opening it ahead fails.
    val files = new java.io.File(path).listFiles().filter(_.getName.endsWith(".parquet")).sortBy(_.getName)
    val raf = new java.io.RandomAccessFile(files(2), "rw")
    try {
      raf.seek(raf.length() - 4)
      raf.write(Array.fill[Byte](4)(0))
    } finally raf.close()
    for (decodeAhead <- Seq(0, 2)) {
      val run = () =>
        withConf(
          "spark.sql.files.maxPartitionBytes" -> (1L << 30).toString,
          "spark.sql.files.openCostInBytes" -> "0",
          "spark.sql.files.minPartitionNum" -> "1", // one task over every file
          VectorConf.ScanNativeParquetPrefetchFiles -> "6"
        )(withPlugin(enabled = true)(spark.sql("SELECT sum(i) FROM da_badopen").collect()))
      if (decodeAhead > 0) withDecodeAhead(decodeAhead, "virtual")(intercept[Throwable](run()))
      else intercept[Throwable](run())
      assert(VectorParquetScanExec.prefetchInFlightBytes === 0, s"decodeAhead=$decodeAhead: bytes not given back")
    }
  }
}
