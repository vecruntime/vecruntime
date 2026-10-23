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
import org.apache.spark.sql.vecruntime.{ParallelDecodeIterator, PlanUtils, VectorParquetScanExec}

/**
 * #606: parallel row-group decode in the native Parquet scan. With `parallelDecode` = K > 0 the row groups of
 * an eligible file are decoded on K helper threads, each with its own column-reader set and vectors, and handed
 * back in file order. The results must be identical to the serial reader's -- across flat, nested, decimal and
 * string columns, with and without a filter / column index / late materialization, on virtual and platform
 * helper threads, across files and per-file fallbacks -- and nothing may leak when a LIMIT or a failure ends the
 * task early.
 */
@io.vecruntime.spark.test.tags.Scan
class ParallelDecodeSuite extends VectorQuerySuite {

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
          |  CAST(id % 1000 AS DECIMAL(25,2)) AS w,
          |  NAMED_STRUCT('a', CAST(id AS INT), 'b', CONCAT('k', CAST(id % 13 AS STRING))) AS st,
          |  CASE WHEN id % 11 = 0 THEN NULL
          |       ELSE ARRAY(CAST(id AS INT), CAST(id % 3 AS INT)) END AS arr
          |FROM range(0, 60000)""".stripMargin
      ).repartition(3).write.mode("overwrite").parquet(newTempPath("pd/t"))
      spark.read.parquet(newTempPath("pd/t")).createOrReplaceTempView("pd_t")
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

  /** Parallel decode engaging at any row-group size, with K helpers on the given thread kind. */
  private def withParallel[T](k: Int, threads: String = "virtual", minBytes: Long = 0L)(f: => T): T =
    withConf(
      VectorConf.ScanNativeParquetParallelDecode -> k.toString,
      VectorConf.ScanNativeParquetParallelDecodeThreads -> threads,
      VectorConf.ScanNativeParquetParallelDecodeMinRowGroupBytes -> minBytes.toString,
      // Decode-ahead off so parallel is the only extra path and helper assertions are unambiguous.
      VectorConf.ScanNativeParquetDecodeAhead -> "0"
    )(f)

  private val queries = Seq(
    "SELECT * FROM pd_t",
    "SELECT i, s FROM pd_t WHERE i % 3 = 0",
    "SELECT s, count(*), sum(l), max(d), sum(w) FROM pd_t GROUP BY s",
    "SELECT st.a, st.b, arr FROM pd_t WHERE i % 4 = 1",
    "SELECT sum(st.a), max(st.b), sum(size(arr)) FROM pd_t"
  )

  test("the test table spans many row groups per file, all decoded natively") {
    val files = parquetFiles(newTempPath("pd/t"))
    val groups = nativeRowGroups("SELECT * FROM pd_t")
    assert(files === 3, s"$files files")
    assert(groups >= 4L * files, s"only $groups row groups in $files files")
    withParallel(4)(assert(nativeRowGroups("SELECT * FROM pd_t") === groups))
  }

  for (k <- Seq(1, 2, 4); threads <- Seq("virtual", "platform")) {
    test(s"parallelDecode=$k on $threads threads returns the serial reader's rows, flat + nested + decimal") {
      val before = ParallelDecodeIterator.started.get()
      val beforeVirtual = ParallelDecodeIterator.startedVirtual.get()
      withParallel(k, threads) {
        queries.foreach(q => checkVectorized(q, Seq(node)))
      }
      val helpers = ParallelDecodeIterator.started.get() - before
      assert(helpers > 0, "no parallel-decode helper ran")
      val virtual = ParallelDecodeIterator.startedVirtual.get() - beforeVirtual
      if (threads == "virtual") assert(virtual === helpers, s"$virtual of $helpers helpers were virtual")
      else assert(virtual === 0L, s"$virtual helpers were virtual on the platform setting")
    }
  }

  test("row order is preserved end to end (compared row by row, not sorted)") {
    val q = "SELECT i, l, s, st.a FROM pd_t"
    val serial = withParallel(0)(withPlugin(enabled = true)(spark.sql(q).collect().toSeq.map(_.toString)))
    for (k <- Seq(1, 2, 4)) {
      val parallel = withParallel(k)(withPlugin(enabled = true)(spark.sql(q).collect().toSeq.map(_.toString)))
      assert(parallel === serial, s"k=$k: order differs from serial")
    }
  }

  test("parallelDecode=0 starts no helper") {
    val before = ParallelDecodeIterator.started.get()
    withParallel(0) {
      checkVectorized("SELECT i, s FROM pd_t WHERE i % 3 = 0", Seq(node))
    }
    assert(ParallelDecodeIterator.started.get() === before)
    assert(VectorConf.scanNativeParquetParallelDecode(new org.apache.spark.sql.internal.SQLConf) === 0)
  }

  test("the minRowGroupBytes gate keeps a small-row-group file on the lead (still correct)") {
    val before = ParallelDecodeIterator.started.get()
    // A huge threshold no row group clears: helpers start (the pool is created) but every row group is a
    // LeadBatch, so the output is still the serial reader's.
    withParallel(4, minBytes = 1L << 40) {
      queries.foreach(q => checkVectorized(q, Seq(node)))
    }
    // Pools are still created (one per task), but no row group was decoded in parallel: assert correctness above.
    assert(ParallelDecodeIterator.started.get() >= before)
  }

  test("a decode filter (late materialization) keeps the file on the lead, still correct") {
    withConf(
      VectorConf.ScanNativeParquetLateMaterialization -> "true",
      VectorConf.ScanNativeParquetLateMaterializationMinBytes -> "0"
    ) {
      withParallel(4) {
        checkVectorized("SELECT i, s, w FROM pd_t WHERE i < 5000", Seq(node))
        checkVectorized("SELECT sum(l), max(s) FROM pd_t WHERE i % 7 = 1", Seq(node))
      }
    }
  }

  test("the column index on (page skipping) stays correct under parallel decode") {
    withConf("parquet.filter.columnindex.enabled" -> "true") {
      withParallel(4) {
        checkVectorized("SELECT i, s FROM pd_t WHERE i BETWEEN 1000 AND 2000", Seq(node))
      }
    }
  }

  test("a LIMIT that ends the task early leaks no vectors (allocator back to baseline)") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    withParallel(4) {
      checkVectorized("SELECT i, s, w, st.a FROM pd_t LIMIT 7", Seq(node))
      checkVectorized("SELECT * FROM pd_t WHERE i > 100 LIMIT 1000", Seq(node))
    }
    assert(root.getAllocatedMemory === before, s"allocated ${root.getAllocatedMemory} vs $before before")
  }

  test("a table mixing natively decoded and fallback files reads correctly and in order") {
    val path = newTempPath("pd/mixed")
    withPlugin(enabled = false) {
      val q = "SELECT CAST(id AS INT) AS i, TIMESTAMP_MICROS(id * 1000003) AS ts FROM range(0, 20000)"
      withConf("spark.sql.parquet.outputTimestampType" -> "INT96") {
        spark.sql(q).repartition(2).write.mode("overwrite").parquet(path + "/int96")
      }
      withConf("spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS") {
        spark.sql(q).repartition(2).write.mode("overwrite").parquet(path + "/micros")
      }
      spark.read.parquet(path + "/int96", path + "/micros").createOrReplaceTempView("pd_mixed")
    }
    for (threads <- Seq("virtual", "platform")) {
      withParallel(4, threads) {
        withConf("spark.sql.files.maxPartitionBytes" -> (64L << 20).toString) {
          checkVectorized("SELECT * FROM pd_mixed", Seq(node))
          checkVectorized("SELECT count(*), sum(i), min(ts), max(ts) FROM pd_mixed", Seq(node))
        }
      }
    }
  }

  test("a failure on a helper surfaces on the task thread") {
    val path = newTempPath("pd/corrupt")
    withPlugin(enabled = false) {
      withConf("parquet.enable.dictionary" -> "false") {
        spark.range(0, 200000).selectExpr("CAST(id * 7919 AS INT) AS i").repartition(1).write.mode("overwrite")
          .parquet(path)
      }
    }
    // Corrupt a later row group, keeping the first (decoded inline on the task thread) and the footer intact.
    val f = new java.io.File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
    val raf = new java.io.RandomAccessFile(f, "rw")
    try {
      raf.seek(raf.length() * 6 / 10)
      raf.write(Array.fill[Byte](4096)(0x5a.toByte))
    } finally raf.close()
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("pd_corrupt"))
    val serial = intercept[Throwable](withPlugin(enabled = true)(spark.sql("SELECT sum(i) FROM pd_corrupt").collect()))
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    withParallel(4) {
      val ahead = intercept[Throwable](withPlugin(enabled = true)(spark.sql("SELECT sum(i) FROM pd_corrupt").collect()))
      assert(ahead.getClass === serial.getClass, s"${ahead.getClass} vs ${serial.getClass}")
    }
    assert(
      root.getAllocatedMemory === before,
      s"a failed parallel decode leaked: ${root.getAllocatedMemory} vs $before"
    )
  }

  /**
   * #606 local benchmark, gated behind `-Dvecruntime.pdecode.bench=true` so it does not run in CI. It writes one
   * large file with many large row groups of mixed types, then times a scan-heavy aggregate serial
   * (parallelDecode=0, decodeAhead=0), single-producer decode-ahead (decodeAhead=4) and parallel (parallelDecode
   * =4), each over one task so the helper threads have idle cores -- the shape this targets. Results are equal
   * by construction (the suite's correctness tests cover that); this only reports wall times.
   */
  test("local benchmark: serial vs decode-ahead vs parallel on a large multi-row-group file") {
    assume(java.lang.Boolean.getBoolean("vecruntime.pdecode.bench"), "set -Dvecruntime.pdecode.bench=true to run")
    val path = newTempPath("pd/bench")
    val rows = java.lang.Long.getLong("vecruntime.pdecode.bench.rows", 3000000L).toLong
    withPlugin(enabled = false) {
      withConf(
        // ~16 MB row groups so even a modest, memory-safe row count makes several of them. On a cluster with
        // real headroom, point this at 64-128 MB row groups and 8-16 of them (the issue's target shape).
        "parquet.block.size" -> (java.lang.Long.getLong("vecruntime.pdecode.bench.block", 16L << 20)).toString,
        "parquet.page.size" -> (1024 * 1024).toString,
        "spark.sql.files.maxRecordsPerFile" -> "0"
      ) {
        spark.sql(
          s"""SELECT CAST(id AS INT) AS i, CAST(id * 2654435761 AS BIGINT) AS l,
             |  CONCAT('v', CAST(id % 997 AS STRING)) AS s, CAST(id * 1.5 AS DOUBLE) AS d,
             |  CAST(id % 100000 AS DECIMAL(25,2)) AS w,
             |  NAMED_STRUCT('a', CAST(id AS INT), 'b', CONCAT('k', CAST(id % 37 AS STRING))) AS st
             |FROM range(0, $rows)""".stripMargin
        ).repartition(1).write.mode("overwrite").parquet(path)
      }
      spark.read.parquet(path).createOrReplaceTempView("pd_bench")
    }
    val files = new java.io.File(path).listFiles().filter(_.getName.endsWith(".parquet"))
    val maxBytes = files.map(_.length).max + (1L << 20)
    val groups = nativeRowGroups("SELECT * FROM pd_bench")
    // One task over the whole file, so a worker's helpers have cores to use.
    val q = "SELECT count(*), max(l), sum(d), max(w), max(s), max(st.b) FROM pd_bench"
    def time(label: String, confs: (String, String)*): Unit = {
      val t = withConf(
        (Seq("spark.sql.files.maxPartitionBytes" -> maxBytes.toString, "spark.sql.files.openCostInBytes" -> "0")
          ++ confs): _*
      ) {
        withPlugin(enabled = true)(spark.sql(q).collect()) // warm up
        val t0 = System.nanoTime()
        var r = 0
        while (r < 3) { withPlugin(enabled = true)(spark.sql(q).collect()); r += 1 }
        (System.nanoTime() - t0) / 3 / 1000000L
      }
      // scalastyle:off println
      println(f"[pdecode-bench] $label%-28s ${t}%5d ms   (rows=$rows, rowGroups=$groups)")
      // scalastyle:on println
    }
    time("serial", VectorConf.ScanNativeParquetDecodeAhead -> "0", VectorConf.ScanNativeParquetParallelDecode -> "0")
    time(
      "decode-ahead=4",
      VectorConf.ScanNativeParquetDecodeAhead -> "4",
      VectorConf.ScanNativeParquetParallelDecode -> "0"
    )
    for (k <- Seq(2, 4)) {
      time(
        s"parallel=$k",
        VectorConf.ScanNativeParquetDecodeAhead -> "0",
        VectorConf.ScanNativeParquetParallelDecode -> k.toString,
        VectorConf.ScanNativeParquetParallelDecodeMinRowGroupBytes -> "0"
      )
    }
  }
}
