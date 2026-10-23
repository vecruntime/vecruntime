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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * #962: `auto` decides late materialization per scan and per row group from the pushed filter's selectivity. The
 * decode filter is attached at plan time only when the filter columns are a small fraction of the projected
 * bytes; the reader then late-materializes a row group only when its estimated surviving fraction is low, and
 * decodes the rest eagerly. Results must be identical for off / on / auto, and the row-group decision metrics
 * (`numRowGroupsLate` / `numRowGroupsEager`) must show late materialization chosen for a selective filter on a
 * wide projection and declined for a non-selective one.
 */
@io.vecruntime.spark.test.tags.Scan
class AutoLateMaterializationSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      VectorConf.ScanNativeParquetLateMaterializationMinBytes -> "0",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1000"
    )

  // A wide projection: a few narrow key columns and several wide string / decimal columns. The selective filter
  // reads only `id` (narrow); the projection decodes the wide columns, which is what late materialization saves.
  private val select =
    """SELECT id, CAST(id % 1000 AS INT) AS k, CAST(id % 7 AS INT) AS small,
      |  CAST(id % 977 AS DECIMAL(9,2)) AS d9, CAST(id AS DECIMAL(20,3)) AS d20,
      |  CASE WHEN id % 13 = 0 THEN NULL ELSE CONCAT('s', CAST(id % 37 AS STRING)) END AS sdict,
      |  CONCAT('long-value-', CAST(id AS STRING), REPEAT('x', CAST(id % 40 AS INT))) AS splain,
      |  CONCAT('more-', CAST(id AS STRING), REPEAT('y', CAST(id % 30 AS INT))) AS splain2
      |FROM range(0, 120000)""".stripMargin

  private val columns = "id, k, small, d9, d20, sdict, splain, splain2"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      // UNSORTED in two files: a narrow range on id is selective WITHIN each row group (each row group's
      // [min, max] spans nearly the whole domain, so a 200-wide band covers a tiny fraction), with survivors
      // scattered -- the case late materialization wins. Several row groups per file.
      spark.sql(select).repartition(2).write.mode("overwrite").parquet(newTempPath("auto/t"))
      spark.read.parquet(newTempPath("auto/t")).createOrReplaceTempView("auto_t")
    }
  }

  private def scanOf(df: DataFrame): Seq[VectorParquetScanExec] =
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }

  /** Run `sql` under a late-materialization mode and return the materialized plan's scans. */
  private def runScans(sql: String, mode: String): Seq[VectorParquetScanExec] =
    withConf(VectorConf.ScanNativeParquetLateMaterialization -> mode) {
      withPlugin(enabled = true) {
        val d = spark.sql(sql); d.collect(); scanOf(d)
      }
    }

  test("auto, on and off return identical results on a selective and a non-selective query") {
    val queries = Seq(
      s"SELECT $columns FROM auto_t WHERE id BETWEEN 5000 AND 5200", // very selective, wide projection
      s"SELECT $columns FROM auto_t WHERE id % 3 <> 0", // non-selective (keeps ~2/3)
      s"SELECT $columns FROM auto_t WHERE sdict = 's5'", // dictionary-ish equality
      s"SELECT $columns FROM auto_t WHERE id >= 0" // keeps all
    )
    for (q <- queries) {
      val off = withConf(VectorConf.ScanNativeParquetLateMaterialization -> "off")(
        withPlugin(enabled = true)(spark.sql(q).collect().toSeq)
      )
      for (mode <- Seq("on", "auto")) {
        val got = withConf(VectorConf.ScanNativeParquetLateMaterialization -> mode)(
          withPlugin(enabled = true)(spark.sql(q).collect().toSeq)
        )
        assert(got.length == off.length, s"$mode changed the row count of: $q")
        assert(got.toSet == off.toSet, s"$mode changed the rows of: $q")
      }
    }
  }

  test("auto late-materializes a selective filter on a wide projection, and declines a non-selective one") {
    // Selective: a narrow range on id -> surviving fraction is tiny in the few row groups statistics keep.
    val selective = runScans(s"SELECT $columns FROM auto_t WHERE id BETWEEN 5000 AND 5200", "auto")
    assert(selective.exists(_.decodeFilter.isDefined), "no decode filter attached for the selective query")
    assert(selective.forall(_.lateAuto), "the selective scan is not in auto mode")
    val late = selective.map(_.metrics("numRowGroupsLate").value).sum
    assert(late > 0, s"auto did not late-materialize any row group of the selective query (late=$late)")

    // Non-selective: keeps ~2/3 of every row group -> every read row group should be decoded eagerly.
    val nonSelective = runScans(s"SELECT $columns FROM auto_t WHERE id % 3 <> 0", "auto")
    assert(nonSelective.exists(_.decodeFilter.isDefined), "no decode filter attached for the non-selective query")
    val nsLate = nonSelective.map(_.metrics("numRowGroupsLate").value).sum
    val nsEager = nonSelective.map(_.metrics("numRowGroupsEager").value).sum
    assert(nsEager > 0, s"auto late-materialized a non-selective query (late=$nsLate eager=$nsEager)")
    assert(nsLate == 0, s"auto late-materialized $nsLate non-selective row groups; expected 0")
  }

  test("auto attaches no decode filter when the filter covers most of the projected bytes") {
    // The filter is on a wide string column and the projection adds only one narrow int, so the filter column is
    // most of the projected bytes: the auto byte-fraction gate declines the decode filter and keeps eager.
    val scans = runScans("SELECT splain, k FROM auto_t WHERE splain LIKE 'long-value-12%'", "auto")
    assert(scans.forall(_.decodeFilter.isEmpty), "auto attached a decode filter despite a filter-heavy projection")
  }

  test("on attaches the decode filter even when the filter covers most projected bytes (no byte gate)") {
    val scans = runScans("SELECT splain, k FROM auto_t WHERE splain LIKE 'long-value-12%'", "on")
    assert(scans.exists(_.decodeFilter.isDefined), "on did not attach a decode filter")
    assert(scans.forall(!_.lateAuto), "on must not mark the scan auto")
  }

  test("off never attaches a decode filter") {
    val scans = runScans(s"SELECT $columns FROM auto_t WHERE id BETWEEN 5000 AND 5200", "off")
    assert(scans.forall(_.decodeFilter.isEmpty), "off attached a decode filter")
  }

  test("auto is the default, and nothing leaks across a late/eager mix") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    // Default (no explicit key) must behave as auto: a selective query late-materializes.
    val df = withPlugin(enabled = true) {
      val d = spark.sql(s"SELECT $columns FROM auto_t WHERE id BETWEEN 5000 AND 5200"); d.collect(); d
    }
    assert(scanOf(df).exists(s => s.decodeFilter.isDefined && s.lateAuto), "default is not auto")
    assert(scanOf(df).map(_.metrics("numRowGroupsLate").value).sum > 0, "default auto did not late-materialize")
    // A query whose row groups mix selectivities, exercising both branches; result must still be Spark's.
    checkVectorized(
      s"SELECT small, count(*), max(splain), min(sdict), sum(d20) FROM auto_t " +
        s"WHERE id BETWEEN 5000 AND 5200 OR id BETWEEN 110000 AND 110050 GROUP BY small",
      Nil
    )
    assert(root.getAllocatedMemory === before)
  }
}
