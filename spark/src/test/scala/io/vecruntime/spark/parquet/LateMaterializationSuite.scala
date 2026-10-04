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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * #611: with late materialization on, a filter directly over the native scan is decoded first and the other
 * columns only at its survivors. Results must be Spark's at every selectivity (none, a few rows, runs, most,
 * all), for every type and encoding the scan decodes, with nulls, small pages (runs cross pages) and small
 * batches, and with decode-ahead on.
 */
class LateMaterializationSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      VectorConf.ScanNativeParquetLateMaterializationMinBytes -> "0",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (2 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1000"
    )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // INT64 micros, not INT96: an INT96 file falls over to Spark's reader whole.
    withPlugin(enabled = false)(withConf("spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS") {
      spark.sql(
        """SELECT id, CAST(id % 1000 AS INT) AS k, CAST(id % 7 AS INT) AS small,
          |  CASE WHEN id % 11 = 0 THEN NULL ELSE id * 3 END AS nv,
          |  CAST(id % 977 AS DECIMAL(9,2)) AS d9, CAST(id AS DECIMAL(20,3)) AS d20,
          |  CASE WHEN id % 13 = 0 THEN NULL ELSE CONCAT('s', CAST(id % 37 AS STRING)) END AS sdict,
          |  CONCAT('long-value-', CAST(id AS STRING), REPEAT('x', CAST(id % 40 AS INT))) AS splain,
          |  id % 3 = 0 AS b, CAST(id * 0.5 AS DOUBLE) AS dbl,
          |  DATE_ADD(DATE '2020-01-01', CAST(id % 400 AS INT)) AS dt,
          |  TIMESTAMP_MICROS(1600000000000000 + id * 1000) AS ts,
          |  CAST(id % 120 AS TINYINT) AS ti
          |FROM range(0, 50000)""".stripMargin
      ).repartition(2).write.mode("overwrite").parquet(newTempPath("lm/t"))
      spark.read.parquet(newTempPath("lm/t")).createOrReplaceTempView("lm_t")
    })
  }

  private def scanOf(df: DataFrame): Seq[VectorParquetScanExec] =
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }

  private val columns = "id, k, small, nv, d9, d20, sdict, splain, b, dbl, dt, ts, ti"

  private val filters = Seq(
    "id < 0", // none
    "id % 4999 = 0", // a few rows, scattered
    "k BETWEEN 100 AND 180", // runs
    "id % 3 <> 0", // most, alternating runs of two
    "id >= 0", // all
    "nv IS NULL", // on a nullable column
    "nv > 1000 AND small = 3", // two columns
    "sdict = 's5'", // a dictionary string
    "splain LIKE 'long-value-12%'", // a plain string
    "b", // a boolean
    "dt > DATE '2020-12-01'", // a date
    "d9 < 5.5" // a decimal
  )

  test("Spark's rows at every selectivity, type and encoding, with and without decode-ahead") {
    for (ahead <- Seq("0", "4")) {
      withConf(
        VectorConf.ScanNativeParquetLateMaterialization -> "true",
        VectorConf.ScanNativeParquetDecodeAhead -> ahead
      ) {
        for (f <- filters) {
          val df = checkVectorized(s"SELECT $columns FROM lm_t WHERE $f", Seq(classOf[VectorParquetScanExec]))
          assert(scanOf(df).exists(_.decodeFilter.isDefined), s"no decode filter for $f\n${finalPlan(df).treeString}")
          // (`id < 0` prunes every row group by its statistics.)
          if (f != "id < 0") assert(scanOf(df).map(_.metrics("numRowGroups").value).sum > 0, s"not read natively: $f")
          checkVectorized(
            s"SELECT small, count(*), sum(nv), max(sdict), min(splain), sum(d20), max(ts) FROM lm_t WHERE $f GROUP BY small",
            Nil
          )
        }
      }
    }
  }

  test("rows dropped before decoding are counted, and nothing leaks") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    withConf(VectorConf.ScanNativeParquetLateMaterialization -> "true") {
      checkVectorized(s"SELECT $columns FROM lm_t WHERE k BETWEEN 100 AND 180", Nil)
      val df = withPlugin(enabled = true) {
        val d = spark.sql(s"SELECT $columns FROM lm_t WHERE k BETWEEN 100 AND 180"); d.collect(); d
      }
      val skipped = scanOf(df).map(_.metrics("numLateSkippedRows").value).sum
      assert(skipped > 40000, s"$skipped rows skipped: ${scanOf(df).map(s => s.decodeFilter.toString + " " + s.metrics.map(kv => kv._1 + "=" + kv._2.value).mkString(","))}")
      checkVectorized(s"SELECT $columns FROM lm_t WHERE k < 3 LIMIT 7", Nil)
    }
    assert(root.getAllocatedMemory === before)
  }

  test("no decode filter when it reads every column, or is not deterministic, or the switch is off") {
    withConf(VectorConf.ScanNativeParquetLateMaterialization -> "true") {
      val all = checkVectorized("SELECT k, small FROM lm_t WHERE k + small > 500", Nil)
      assert(scanOf(all).forall(_.decodeFilter.isEmpty))
      val rnd = withPlugin(enabled = true) {
        val d = spark.sql("SELECT k FROM lm_t WHERE rand(7) < 0.1 AND small = 1"); d.collect(); d
      }
      assert(scanOf(rnd).forall(_.decodeFilter.isEmpty))
    }
    val off = checkVectorized("SELECT k, splain FROM lm_t WHERE small = 1", Nil)
    assert(scanOf(off).forall(_.decodeFilter.isEmpty))
    // A scan smaller than minScanBytes gets no decode filter.
    withConf(
      VectorConf.ScanNativeParquetLateMaterialization -> "true",
      VectorConf.ScanNativeParquetLateMaterializationMinBytes -> (1L << 40).toString
    ) {
      val small = checkVectorized("SELECT k, splain FROM lm_t WHERE small = 1", Nil)
      assert(scanOf(small).forall(_.decodeFilter.isEmpty))
    }
  }
}
