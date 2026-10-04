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
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorBroadcastHashJoinExec, VectorParquetScanExec}

/**
 * #610: a broadcast hash join's build keys reach the native scan of its streamed side as pushed filters, so
 * row groups with no matching key are not read. The fact table is written sorted on its keys with small
 * row groups, so each row group holds a narrow key range.
 */
class RuntimeFiltersSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (16 * 1024).toString,
      "parquet.page.size" -> (2 * 1024).toString,
      "spark.sql.autoBroadcastJoinThreshold" -> (10L << 20).toString
    )

  private val Bhj = classOf[VectorBroadcastHashJoinExec]
  private val Scan = classOf[VectorParquetScanExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id / 10 AS INT) AS k, id AS lk, CONCAT('c', CAST(id % 50 AS STRING)) AS s,
          |  CAST(id % 4 AS INT) AS p, id * 7 AS v
          |FROM range(0, 60000)""".stripMargin
      ).repartition(1).sortWithinPartitions("k").write.mode("overwrite").parquet(newTempPath("rf/fact"))
      spark.read.parquet(newTempPath("rf/fact")).createOrReplaceTempView("rf_fact")
      // The same rows partitioned on p: a key on a partition column must get no runtime filter.
      spark.read.parquet(newTempPath("rf/fact")).write.partitionBy("p").mode("overwrite")
        .parquet(newTempPath("rf/factp"))
      spark.read.parquet(newTempPath("rf/factp")).createOrReplaceTempView("rf_factp")
      spark.sql(
        """SELECT CAST(id AS INT) AS k, id AS lk, CONCAT('c', CAST(id % 50 AS STRING)) AS s,
          |  CAST(id % 4 AS INT) AS p, CONCAT('d', CAST(id AS STRING)) AS name
          |FROM range(0, 6000)""".stripMargin
      ).write.mode("overwrite").parquet(newTempPath("rf/dim"))
      spark.read.parquet(newTempPath("rf/dim")).createOrReplaceTempView("rf_dim")
    }
  }

  private def scans(df: DataFrame): Seq[VectorParquetScanExec] =
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }

  private def metric(df: DataFrame, name: String): Long = scans(df).map(_.metrics(name).value).sum

  private def run(sql: String, on: Boolean): DataFrame =
    withConf(VectorConf.JoinRuntimeFilters -> on.toString) { checkVectorized(sql, Seq(Bhj, Scan)) }

  test("a selective dimension skips the fact table's row groups, with Spark's rows") {
    // The dimension is filtered on its name, not its key, so Spark infers nothing for the fact side. Keys
    // 100..199 (an IN list), and 1000..1999 past an inMax of 500 (the range).
    for (
      (sql, inMax) <- Seq(
        "SELECT f.k, f.v, d.name FROM rf_fact f JOIN rf_dim d ON f.k = d.k WHERE d.name LIKE 'd1__'" -> "1024",
        "SELECT count(*), sum(f.v) FROM rf_fact f JOIN rf_dim d ON f.k = d.k WHERE d.name LIKE 'd1___'" -> "500",
        "SELECT f.k, sum(f.v) FROM rf_fact f LEFT SEMI JOIN rf_dim d ON f.k = d.k AND d.name LIKE 'd2__' GROUP BY f.k" ->
          "1024"
      )
    ) withConf(VectorConf.JoinRuntimeFiltersInMax -> inMax) {
      val off = run(sql, on = false)
      val on = run(sql, on = true)
      assert(metric(off, "numRuntimeFilters") === 0)
      assert(
        metric(on, "numRuntimeFilters") > 0,
        s"$sql rg=${metric(on, "numRowGroups")} f=${scans(on).map(_.runtimeFilterCount)}\n${finalPlan(on).treeString}"
      )
      assert(
        metric(on, "numRowGroups") < metric(off, "numRowGroups"),
        s"no row group skipped (${metric(on, "numRowGroups")} vs ${metric(off, "numRowGroups")}): $sql"
      )
    }
  }

  test("through a filter, a projection and a second join; long and string keys") {
    withConf("spark.sql.parquet.pushdown.inFilterThreshold" -> "200") {
      for (
        sql <- Seq(
          """SELECT f.k, f.v FROM (SELECT k, lk AS l, v FROM rf_fact WHERE v % 3 <> 0) f
            |JOIN rf_dim d ON f.l = d.lk WHERE d.name LIKE 'd20__'""".stripMargin,
          """SELECT f.k, d1.name, d2.s FROM rf_fact f JOIN rf_dim d1 ON f.k = d1.k
            |JOIN rf_dim d2 ON f.lk = d2.lk WHERE d1.name LIKE 'd4__' AND d2.name LIKE 'd3__'""".stripMargin,
          "SELECT f.s, count(*) FROM rf_fact f JOIN rf_dim d ON f.s = d.s WHERE d.name IN ('d3', 'd4') GROUP BY f.s"
        )
      ) {
        run(sql, on = false)
        val on = run(sql, on = true)
        assert(metric(on, "numRuntimeFilters") > 0, s"$sql\n${finalPlan(on).treeString}")
      }
    }
  }

  test("no runtime filter where it could drop a row the join keeps, or on a partition column") {
    for (
      sql <- Seq(
        "SELECT f.k, d.name FROM rf_fact f LEFT JOIN rf_dim d ON f.k = d.k AND d.name LIKE 'd1__'",
        "SELECT f.k FROM rf_fact f LEFT ANTI JOIN (SELECT * FROM rf_dim WHERE name LIKE 'd1__') d ON f.k = d.k",
        "SELECT f.k, f.v FROM rf_fact f WHERE f.k NOT IN (SELECT k FROM rf_dim WHERE name LIKE 'd1_') AND f.v < 5000",
        "SELECT count(*) FROM rf_factp f JOIN rf_dim d ON f.p = d.p WHERE d.name = 'd5'"
      )
    ) {
      val df = withConf(VectorConf.JoinRuntimeFilters -> "true") { checkVectorized(sql, Nil) }
      assert(metric(df, "numRuntimeFilters") === 0, sql)
    }
  }

  test("no runtime filter from a build side past maxBuildRows") {
    val sql = "SELECT count(*), sum(f.v) FROM rf_fact f JOIN rf_dim d ON f.k = d.k WHERE d.name LIKE 'd1__'"
    val capped = withConf(VectorConf.JoinRuntimeFiltersMaxBuildRows -> "99") { run(sql, on = true) }
    assert(metric(capped, "numRuntimeFilters") === 0, finalPlan(capped).treeString)
    val within = withConf(VectorConf.JoinRuntimeFiltersMaxBuildRows -> "100") { run(sql, on = true) }
    assert(metric(within, "numRuntimeFilters") > 0, finalPlan(within).treeString)
  }

  test("an empty build side") {
    // AQE would replace the join with an empty relation; without it the join runs over an empty build.
    withConf("spark.sql.adaptive.enabled" -> "false") {
      run("SELECT f.k FROM rf_fact f JOIN rf_dim d ON f.k = d.k WHERE d.name = 'none'", on = true)
    }
  }
}
