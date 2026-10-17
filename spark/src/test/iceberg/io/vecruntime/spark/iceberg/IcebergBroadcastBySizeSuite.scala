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
package io.vecruntime.spark.iceberg

import java.nio.file.Files

import io.vecruntime.spark.{VectorConf, VectorPlugin}
import io.vecruntime.spark.test.{IcebergTest, VectorQuerySuite}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Join}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.vecruntime.V2BroadcastBySize

/**
 * #735: `V2BroadcastBySize` -- a small Iceberg dimension, estimated by Iceberg as rows times column widths (above
 * the broadcast threshold), is broadcast by the bytes of its files, as its Parquet copy would be. AQE is off so the
 * plan-time join choice is what runs; every result is checked against Spark with the plugin off.
 */
class IcebergBroadcastBySizeSuite extends VectorQuerySuite {

  private lazy val warehouse = Files.createTempDirectory("vecruntime-iceberg-bcast").toString

  override protected def extraSparkConf: Map[String, String] =
    IcebergTables.catalogConf(warehouse) ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "false"
    )

  private val Db = IcebergTables.Db
  private var threshold = 0L

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $Db")
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.fact USING iceberg AS
         |SELECT id AS f_key, id % 20000 AS f_dim, id % 9 AS f_qty FROM range(0, 400000)""".stripMargin
    )
    // Repetitive strings: a small file, a much larger rows-times-widths estimate.
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.dim USING iceberg AS
         |SELECT id AS d_key, IF(id % 2 = 0, 'M', 'F') AS d_gender,
         |  concat('status-', CAST(id % 4 AS STRING)) AS d_status
         |FROM range(0, 20000)""".stripMargin
    )
    spark.table(s"$Db.fact").createOrReplaceTempView("fact")
    spark.table(s"$Db.dim").createOrReplaceTempView("dim")
    // Between the joined dimension's estimate and its share of the file bytes, as the rule scales them: Spark
    // alone would not broadcast it.
    val plan = withConf(VectorConf.V2BroadcastBySizeEnabled -> "false")(spark.sql(q7).queryExecution.optimizedPlan)
    val side = plan.collectFirst {
      case j: Join if j.right.exists(isDim) => j.right
      case j: Join if j.left.exists(isDim) => j.left
    }.get
    val scan = side.collectFirst { case s: DataSourceV2ScanRelation => s }.get
    val estimate = side.stats.sizeInBytes
    val scaled = BigInt(V2BroadcastBySize.plannedBytes(scan).get) * estimate / scan.stats.sizeInBytes
    assert(scaled * 4 < estimate, s"the test needs the files ($scaled B) well under the estimate ($estimate B)")
    threshold = ((scaled + estimate) / 2).toLong
  }

  private def isDim(p: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Boolean = p match {
    case s: DataSourceV2ScanRelation => s.relation.name.endsWith("dim")
    case _ => false
  }

  private def run(sql: String, extra: (String, String)*): DataFrame =
    withConf(("spark.sql.autoBroadcastJoinThreshold" -> threshold.toString) +: extra: _*) {
      val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
      val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
      assertRowsEqual(expected, df.collect(), 1e-9, sql)
      assert(expected.nonEmpty, s"the test query should return rows: $sql")
      df
    }

  private def broadcastHints(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case j: Join => j.hint }
      .count(h => (h.leftHint ++ h.rightHint).exists(_.strategy.contains(BROADCAST)))

  private def broadcastJoins(df: DataFrame): Int =
    df.queryExecution.executedPlan.collect { case p if p.nodeName.contains("BroadcastHashJoin") => p }.size

  private lazy val q7 =
    """SELECT f_qty, avg(f_key) a, max(d_status) m FROM fact JOIN dim ON f_dim = d_key
      |WHERE d_gender = 'M' AND d_status = 'status-2' GROUP BY f_qty""".stripMargin

  test("a small Iceberg dimension is broadcast by its file bytes", IcebergTest) {
    val df = run(q7)
    assert(broadcastHints(df) === 1, df.queryExecution.optimizedPlan.treeString)
    assert(broadcastJoins(df) === 1, df.queryExecution.executedPlan.treeString)
    val off = run(q7, VectorConf.V2BroadcastBySizeEnabled -> "false")
    assert(broadcastHints(off) === 0)
    assert(broadcastJoins(off) === 0, off.queryExecution.executedPlan.treeString)
  }

  test("the fact itself, larger than the threshold by its files too, is not broadcast", IcebergTest) {
    val df = run("SELECT count(*) FROM fact a JOIN fact b ON a.f_key = b.f_key")
    assert(broadcastHints(df) === 0, df.queryExecution.optimizedPlan.treeString)
  }

  test("an input whose estimate is above maxEstimatedBytes is left alone", IcebergTest) {
    val df = run(q7, VectorConf.V2BroadcastBySizeMaxEstimatedBytes -> "1k")
    assert(broadcastHints(df) === 0)
  }

  test("an outer join broadcasts only a side its type allows", IcebergTest) {
    // A left outer join can build only its right side: the dimension on the left is not hinted.
    val df = run("SELECT count(*) FROM dim LEFT JOIN fact ON d_key = f_dim")
    assert(broadcastHints(df) === 0, df.queryExecution.optimizedPlan.treeString)
  }
}
