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
package io.vecruntime.spark

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}

/** Transitive dynamic partition pruning through a second join key (#634), against Spark with the plugin off. */
class TransitiveDppSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // Two facts partitioned by their date key (60 dates), q72-like: sales and inventory on the same item.
    (0 until 6000).map(i => ((i % 60).toLong, (i % 50).toLong, (i % 9).toLong))
      .toDF("cs_sold_date_sk", "cs_item_sk", "cs_quantity")
      .write.mode("overwrite").partitionBy("cs_sold_date_sk").parquet(newTempPath("tdpp/catalog_sales"))
    spark.read.parquet(newTempPath("tdpp/catalog_sales")).createOrReplaceTempView("catalog_sales")
    (0 until 3000).map(i => ((i % 60).toLong, (i % 50).toLong, (i % 7).toLong))
      .toDF("inv_date_sk", "inv_item_sk", "inv_quantity_on_hand")
      .write.mode("overwrite").partitionBy("inv_date_sk").parquet(newTempPath("tdpp/inventory"))
    spark.read.parquet(newTempPath("tdpp/inventory")).createOrReplaceTempView("inventory")
    (0 until 60).map(d => (d.toLong, d / 7, 2000 + d / 20))
      .toDF("d_date_sk", "d_week_seq", "d_year")
      .write.mode("overwrite").parquet(newTempPath("tdpp/date_dim"))
    spark.read.parquet(newTempPath("tdpp/date_dim")).createOrReplaceTempView("date_dim")
  }

  private def run(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** Pruning subqueries on the inventory scan's partition column. */
  private def inventoryPruning(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f }
      .flatMap(_.condition.collect { case d: DynamicPruningSubquery => d })
      .count(_.pruningKey.references.exists(_.name == "inv_date_sk"))

  private def filesRead(df: DataFrame): Long = {
    var n = 0L
    def walk(p: SparkPlan): Unit = p match {
      case a: AdaptiveSparkPlanExec => walk(a.executedPlan)
      case q: QueryStageExec => walk(q.plan)
      case _ =>
        p.metrics.valuesIterator.filter(_.name.contains("number of files read")).foreach(m => n += m.value)
        p.children.foreach(walk)
    }
    walk(df.queryExecution.executedPlan)
    n
  }

  private def withRuleOff[T](f: => T): T = withConf(VectorConf.TransitiveDppEnabled -> "false")(f)

  private val q72 =
    """SELECT d1.d_week_seq, count(*) c, sum(inv_quantity_on_hand) q
      |FROM catalog_sales JOIN inventory ON cs_item_sk = inv_item_sk
      |JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk
      |JOIN date_dim d2 ON inv_date_sk = d2.d_date_sk
      |WHERE d1.d_week_seq = d2.d_week_seq AND inv_quantity_on_hand < cs_quantity AND d1.d_year = 2001
      |GROUP BY d1.d_week_seq""".stripMargin

  test("q72's shape: inventory is pruned through d2's week from the filtered d1") {
    val on = run(q72)
    assert(inventoryPruning(on) === 1)
    val off = withRuleOff(run(q72))
    assert(inventoryPruning(off) === 0)
    assert(filesRead(on) < filesRead(off), s"pruned ${filesRead(on)} vs ${filesRead(off)} files")
  }

  test("no selective filter on the source dimension: left alone") {
    assert(inventoryPruning(run(q72.replace("AND d1.d_year = 2001", "AND d1.d_year IS NOT NULL"))) === 0)
  }

  test("an outer join to the unfiltered dimension: left alone") {
    val sql =
      """SELECT d1.d_week_seq, count(*) c FROM catalog_sales JOIN inventory ON cs_item_sk = inv_item_sk
        |JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk
        |LEFT JOIN date_dim d2 ON inv_date_sk = d2.d_date_sk AND d1.d_week_seq = d2.d_week_seq
        |WHERE d1.d_year = 2001 GROUP BY d1.d_week_seq""".stripMargin
    assert(inventoryPruning(run(sql)) === 0)
  }

  test("the switch turns the rewrite off") {
    withRuleOff(assert(inventoryPruning(run(q72)) === 0))
  }
}
