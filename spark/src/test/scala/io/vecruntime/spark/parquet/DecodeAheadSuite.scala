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
package io.vecruntime.spark.parquet

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.arrow.VectorAllocators
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.vecruntime.{DecodeAheadIterator, VectorParquetScanExec}

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
}
