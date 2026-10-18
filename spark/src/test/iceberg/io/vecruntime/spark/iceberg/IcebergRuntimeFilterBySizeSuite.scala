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
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.vecruntime.V2BroadcastBySize

/**
 * #735: `V2RuntimeFilterBySize` -- Spark's runtime bloom filter built from a small filtered Iceberg dimension that
 * Iceberg's size estimate (rows times column widths) puts above `creationSideThreshold`, as its Parquet copy would
 * get it (TPC-DS q72's `customer_demographics`). AQE and broadcasts are off and our own fact bloom filter is off, so
 * the filters counted are Spark's; every result is checked against Spark with the plugin off.
 */
class IcebergRuntimeFilterBySizeSuite extends VectorQuerySuite {

  private lazy val warehouse = Files.createTempDirectory("vecruntime-iceberg-rf").toString

  override protected def extraSparkConf: Map[String, String] =
    IcebergTables.catalogConf(warehouse) ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "1k",
      VectorConf.FactBloomFilterEnabled -> "false"
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
    // Between the filtered dimension's estimate and its share of the file bytes: Spark alone declines it.
    val plan =
      withConf(VectorConf.V2RuntimeFilterBySizeEnabled -> "false")(spark.sql(q).queryExecution.optimizedPlan)
    val side = plan.collectFirst {
      case j: Join if j.right.exists(isDim) => j.right
      case j: Join if j.left.exists(isDim) => j.left
    }.get
    val scan = side.collectFirst { case s: DataSourceV2ScanRelation => s }.get
    val estimate = side.stats.sizeInBytes
    val scaled = V2BroadcastBySize.byFiles(side, scan).get
    assert(scaled * 4 < estimate, s"the test needs the files ($scaled B) well under the estimate ($estimate B)")
    threshold = ((scaled + estimate) / 2).toLong
  }

  private def isDim(p: LogicalPlan): Boolean = p match {
    case s: DataSourceV2ScanRelation => s.relation.name.endsWith("dim")
    case _ => false
  }

  private def run(sql: String, extra: (String, String)*): DataFrame =
    withConf(("spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold" -> threshold.toString) +: extra: _*) {
      val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
      val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
      assertRowsEqual(expected, df.collect(), 1e-9, sql)
      assert(expected.nonEmpty, s"the test query should return rows: $sql")
      df
    }

  /** Spark's bloom filters in the optimized plan, and whether each sits directly on a fact scan. */
  private def blooms(df: DataFrame): Seq[Boolean] =
    df.queryExecution.optimizedPlan.collect { case f: Filter => f }.flatMap { f =>
      f.condition.collect { case b: BloomFilterMightContain => b }.map(_ => onFact(f.child))
    }

  private def onFact(p: LogicalPlan): Boolean = p match {
    case s: DataSourceV2ScanRelation => s.relation.name.endsWith("fact")
    case f: Filter => onFact(f.child)
    case _ => false
  }

  private lazy val q =
    """SELECT f_qty, avg(f_key) a, max(d_status) m FROM fact JOIN dim ON f_dim = d_key
      |WHERE d_gender = 'M' AND d_status = 'status-2' GROUP BY f_qty""".stripMargin

  test("a small filtered Iceberg dimension gets Spark's runtime filter on the fact by its file bytes", IcebergTest) {
    val df = run(q)
    assert(blooms(df) === Seq(true), df.queryExecution.optimizedPlan.treeString)
    val off = run(q, VectorConf.V2RuntimeFilterBySizeEnabled -> "false")
    assert(blooms(off).isEmpty, off.queryExecution.optimizedPlan.treeString)
  }

  test("a creation side large by its files too gets no filter", IcebergTest) {
    // The fact filtered as the creation side of a self-join: its files are above the threshold.
    val df = run("SELECT count(*) FROM fact a JOIN fact b ON a.f_key = b.f_key WHERE b.f_qty = 3")
    assert(blooms(df).isEmpty, df.queryExecution.optimizedPlan.treeString)
  }

  test("Spark's cap on runtime filters holds", IcebergTest) {
    val df = run(q, "spark.sql.optimizer.runtimeFilter.number.threshold" -> "0")
    assert(blooms(df).isEmpty)
  }

  test("no filter on a fact's partition column, which partition pruning already prunes (q14a)", IcebergTest) {
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.pfact USING iceberg PARTITIONED BY (p_key) AS
         |SELECT id % 40 AS p_key, id % 9 AS p_qty, id AS p_val FROM range(0, 400000)""".stripMargin
    )
    spark.table(s"$Db.pfact").createOrReplaceTempView("pfact")
    val sql =
      """SELECT p_qty, avg(p_val) a, max(d_status) m FROM pfact JOIN dim ON p_key = d_key
        |WHERE d_gender = 'M' AND d_status = 'status-2' GROUP BY p_qty""".stripMargin
    val df = run(sql)
    val onKey = df.queryExecution.optimizedPlan.collect { case f: Filter => f }.flatMap(_.condition.collect {
      case b: BloomFilterMightContain if b.valueExpression.references.exists(_.name == "p_key") => b
    })
    assert(onKey.isEmpty, df.queryExecution.optimizedPlan.treeString)
  }

  test("a dimension broadcast by its files, over a fact with no shuffle below, gets no filter (q89)", IcebergTest) {
    // As on Parquet, where the dimension's files make the join a broadcast and Spark's rule skips a broadcast join
    // with no shuffle under the fact: the broadcast hint V2BroadcastBySize gives it must be seen first.
    val df = run(q, "spark.sql.autoBroadcastJoinThreshold" -> threshold.toString)
    assert(
      df.queryExecution.optimizedPlan.collect { case j: Join => j.hint }.exists(h =>
        (h.leftHint ++ h.rightHint).exists(_.strategy.contains(org.apache.spark.sql.catalyst.plans.logical.BROADCAST))
      ),
      df.queryExecution.optimizedPlan.treeString
    )
    assert(blooms(df).isEmpty, df.queryExecution.optimizedPlan.treeString)
  }

  test("no filter above a join that is a broadcast by its files (q27)", IcebergTest) {
    // `dim` is large by its estimate and small by its files (q27's customer_demographics): before the broadcast
    // hints Spark's rule takes fact JOIN dim for a shuffle join and filters the fact on the store-like key above it,
    // and FactBloomFilter's transitive filter would plant the same one below that "shuffle join".
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.sdim USING iceberg AS
         |SELECT id AS s_key, concat('ST', CAST(id % 40 AS STRING)) AS s_state FROM range(0, 400)""".stripMargin
    )
    spark.table(s"$Db.sdim").createOrReplaceTempView("sdim")
    val sql =
      """SELECT f_qty, avg(f_key) a, max(d_status) m FROM fact JOIN dim ON f_dim = d_key
        |JOIN sdim ON f_key % 400 = s_key
        |WHERE d_gender = 'M' AND d_status = 'status-2' AND s_state IN ('ST1', 'ST2')
        |GROUP BY f_qty""".stripMargin
    val broadcast = "spark.sql.autoBroadcastJoinThreshold" -> threshold.toString
    val off = run(sql, broadcast, VectorConf.V2RuntimeFilterBySizeEnabled -> "false")
    assume(blooms(off).nonEmpty, s"Spark's rule must add the filter for the test: ${off.queryExecution.optimizedPlan}")
    val df = run(sql, broadcast, VectorConf.FactBloomFilterEnabled -> "true")
    assert(blooms(df).isEmpty, df.queryExecution.optimizedPlan.treeString)
  }

  test("nothing is added when Spark's runtime bloom filter is off", IcebergTest) {
    val df = run(q, "spark.sql.optimizer.runtime.bloomFilter.enabled" -> "false")
    assert(blooms(df).isEmpty)
  }

  test("an unfiltered dimension, which Spark's rule never builds from, gets no filter", IcebergTest) {
    val df = run("SELECT f_qty, count(*) c FROM fact JOIN dim ON f_dim = d_key GROUP BY f_qty")
    assert(blooms(df).isEmpty, df.queryExecution.optimizedPlan.treeString)
  }
}
