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

import java.util.concurrent.atomic.AtomicLong

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.arrow.VectorAllocators
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * #812: two-pass late materialization. Each row group's filter columns are read and filtered first, then the row
 * group again over only the surviving rows, so pages of the other columns without a surviving row are never
 * fetched. Results must be Spark's at every selectivity, for every type, on files sorted by the filter column (where
 * whole pages drop) and not, with several row groups per file, decode-ahead and row-group read-ahead; and a
 * selective filter on sorted data must read fewer bytes than one pass does.
 */
@io.vecruntime.spark.test.tags.Scan
class TwoPassLateMaterializationSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      VectorConf.ScanNativeParquetLateMaterializationMinBytes -> "0",
      VectorConf.ScanNativeParquetLateMaterialization -> "true",
      "parquet.block.size" -> (256 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1000"
    )

  private val select =
    """SELECT id, CAST(id % 1000 AS INT) AS k, CAST(id % 7 AS INT) AS small,
      |  CASE WHEN id % 11 = 0 THEN NULL ELSE id * 3 END AS nv,
      |  CAST(id % 977 AS DECIMAL(9,2)) AS d9, CAST(id AS DECIMAL(20,3)) AS d20,
      |  CASE WHEN id % 13 = 0 THEN NULL ELSE CONCAT('s', CAST(id % 37 AS STRING)) END AS sdict,
      |  CONCAT('long-value-', CAST(id AS STRING), REPEAT('x', CAST(id % 40 AS INT))) AS splain,
      |  id % 3 = 0 AS b, CAST(id * 0.5 AS DOUBLE) AS dbl,
      |  DATE_ADD(DATE '2020-01-01', CAST(id % 400 AS INT)) AS dt,
      |  TIMESTAMP_MICROS(1600000000000000 + id * 1000) AS ts,
      |  named_struct('a', CONCAT('a', CAST(id % 5 AS STRING)), 'c', CAST(id AS INT)) AS st
      |FROM range(0, 120000)""".stripMargin

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false)(withConf("spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS") {
      // Sorted by id in two files: a filter on id keeps whole pages of every column.
      spark.sql(select).repartitionByRange(2, org.apache.spark.sql.functions.col("id"))
        .sortWithinPartitions("id").write.mode("overwrite").parquet(newTempPath("tp/sorted"))
      spark.read.parquet(newTempPath("tp/sorted")).createOrReplaceTempView("tp_sorted")
      // Not sorted: survivors scatter over every page.
      spark.sql(select).repartition(2).write.mode("overwrite").parquet(newTempPath("tp/mixed"))
      spark.read.parquet(newTempPath("tp/mixed")).createOrReplaceTempView("tp_mixed")
    })
  }

  private def scanOf(df: DataFrame): Seq[VectorParquetScanExec] =
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }

  private val columns = "id, k, small, nv, d9, d20, sdict, splain, b, dbl, dt, ts, st"

  private val filters = Seq(
    "id < 0", // none
    "id BETWEEN 5000 AND 5100", // a narrow run (pushed: the column index already drops pages)
    "id * 2 BETWEEN 10000 AND 10200", // the same run, not pushed to Parquet: only two-pass drops pages
    "id BETWEEN 30000 AND 60000", // a quarter, one run
    "id % 4999 = 0", // a few rows, scattered
    "k BETWEEN 100 AND 180", // runs everywhere
    "id % 3 <> 0", // most
    "id >= 0", // all
    "nv IS NULL AND id < 20000", // nulls, two conditions
    "sdict = 's5' AND id > 100000", // a string with a range
    "dt > DATE '2020-12-01' AND id < 9000" // a date with a range
  )

  test("Spark's rows at every selectivity, sorted and not, with decode-ahead and row-group read-ahead") {
    for (
      table <- Seq("tp_sorted", "tp_mixed");
      (ahead, rgAhead) <- Seq(("0", "0"), ("4", "0"), ("0", "2"))
    ) {
      withConf(
        VectorConf.ScanNativeParquetLateMaterializationTwoPass -> "true",
        VectorConf.ScanNativeParquetDecodeAhead -> ahead,
        "spark.vecruntime.scan.nativeParquet.prefetchRowGroups" -> rgAhead
      ) {
        for (f <- filters) {
          val df = checkVectorized(s"SELECT $columns FROM $table WHERE $f", Seq(classOf[VectorParquetScanExec]))
          assert(scanOf(df).exists(_.decodeFilter.isDefined), s"no decode filter for $f")
          checkVectorized(
            s"SELECT small, count(*), sum(nv), max(sdict), min(splain), sum(d20), max(ts), max(st.a) " +
              s"FROM $table WHERE $f GROUP BY small",
            Nil
          )
        }
      }
    }
  }

  /** Bytes the tasks of `f` read, through the input metrics. */
  private def bytesRead(f: => Unit): Long = {
    val bytes = new AtomicLong()
    val tasks = new AtomicLong()
    val l = new SparkListener {
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
        if (e.taskMetrics != null) {
          bytes.addAndGet(e.taskMetrics.inputMetrics.bytesRead)
          tasks.incrementAndGet()
        }
    }
    spark.sparkContext.addSparkListener(l)
    try {
      f
      // The listener bus is asynchronous: wait until the task count stops moving.
      var last = -1L
      var stable = 0
      while (stable < 5) {
        Thread.sleep(100)
        val now = tasks.get()
        if (now == last && now > 0) stable += 1 else stable = 0
        last = now
      }
    } finally spark.sparkContext.removeSparkListener(l)
    bytes.get()
  }

  test("a selective filter on sorted data reads fewer bytes in two passes, and nothing leaks") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    val sql = s"SELECT $columns FROM tp_sorted WHERE id * 2 BETWEEN 10000 AND 10200"
    val run = () => withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    val onePass = bytesRead(run())
    var df: DataFrame = null
    val twoPass = bytesRead(withConf(VectorConf.ScanNativeParquetLateMaterializationTwoPass -> "true") { df = run() })
    assert(twoPass < onePass / 2, s"two-pass read $twoPass bytes, one pass $onePass")
    val skipped = scanOf(df).map(_.metrics("numLateSkippedRows").value).sum
    assert(skipped > 100000, s"$skipped rows skipped before decoding")
    assert(root.getAllocatedMemory === before)
  }
}
