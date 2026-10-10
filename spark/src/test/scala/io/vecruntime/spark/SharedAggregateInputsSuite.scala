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
package io.vecruntime.spark

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/** Repeated aggregate subplans differing only by an inferred IsNotNull share their input (#632), against Spark. */
class SharedAggregateInputsSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // Null store and item keys (the inferred IsNotNull matters), several months, small dimensions.
    val sales = (0 until 8000).map { i =>
      val store: java.lang.Long = if (i % 37 == 0) null else Long.box((i % 9).toLong)
      val item: java.lang.Long = if (i % 29 == 0) null else Long.box((i % 113).toLong)
      (store, item, (i % 60).toLong, java.math.BigDecimal.valueOf((i * 31) % 5000, 2))
    }
    sales.toDF("ss_store_sk", "ss_item_sk", "ss_sold_date_sk", "ss_sales_price")
      .coalesce(2).write.mode("overwrite").parquet(newTempPath("shared/store_sales"))
    spark.read.parquet(newTempPath("shared/store_sales")).createOrReplaceTempView("store_sales")
    (0 until 60).map(d => (d.toLong, d / 10)).toDF("d_date_sk", "d_month_seq")
      .write.mode("overwrite").parquet(newTempPath("shared/date_dim"))
    spark.read.parquet(newTempPath("shared/date_dim")).createOrReplaceTempView("date_dim")
    (0 until 100).map(i => (i.toLong, s"item$i")).toDF("i_item_sk", "i_item_desc")
      .write.mode("overwrite").parquet(newTempPath("shared/item"))
    spark.read.parquet(newTempPath("shared/item")).createOrReplaceTempView("item")
    (0 until 9).map(s => (s.toLong, s"store$s")).toDF("s_store_sk", "s_store_name")
      .write.mode("overwrite").parquet(newTempPath("shared/store"))
    spark.read.parquet(newTempPath("shared/store")).createOrReplaceTempView("store")
  }

  private def check(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** `store_sales` scans the executed plan runs; a reused exchange or subquery counts once. */
  private def factScans(df: DataFrame): Int = {
    val seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[SparkPlan, java.lang.Boolean])
    var scans = 0
    def walk(p: SparkPlan): Unit = p match {
      case _: ReusedSubqueryExec | _: ReusedExchangeExec => ()
      case _ if !seen.add(p) => ()
      case a: AdaptiveSparkPlanExec => walk(a.executedPlan)
      case q: QueryStageExec => walk(q.plan)
      case _ =>
        if (
          p.children.isEmpty && p.getClass.getSimpleName.contains("Scan") && p.output.exists(_.name == "ss_sales_price")
        )
          scans += 1
        p.children.foreach(walk)
        p.subqueries.foreach(walk)
    }
    walk(df.queryExecution.executedPlan)
    scans
  }

  private def without(sql: String): Int =
    withPlugin(enabled = false)(factScans { val d = spark.sql(sql); d.collect(); d })

  private val sa =
    """sa AS (SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) revenue
      |  FROM store_sales, date_dim WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1 AND 4
      |  GROUP BY ss_store_sk, ss_item_sk)""".stripMargin

  test("q65's CTE, joined on a grouping key in one use only, is scanned once") {
    val sql =
      s"""WITH $sa
         |SELECT s_store_name, i_item_desc, sc.revenue
         |FROM store, item, (SELECT ss_store_sk, avg(revenue) ave FROM sa GROUP BY ss_store_sk) sb, sa sc
         |WHERE sb.ss_store_sk = sc.ss_store_sk AND sc.revenue <= 0.9 * sb.ave
         |  AND s_store_sk = sc.ss_store_sk AND i_item_sk = sc.ss_item_sk""".stripMargin
    assert(without(sql) === 2)
    assert(factScans(check(sql)) === 1)
  }

  test("the same, with the fact partitioned and pruned by DPP in each use (as at 1 TB)") {
    // Each copy gets its own dynamic pruning subquery, listed in a different place among its filters.
    spark.table("store_sales").write.mode("overwrite").partitionBy("ss_sold_date_sk")
      .parquet(newTempPath("shared/store_sales_p"))
    spark.read.parquet(newTempPath("shared/store_sales_p")).createOrReplaceTempView("store_sales_p")
    val sql =
      s"""WITH ${sa.replace("FROM store_sales,", "FROM store_sales_p,")}
         |SELECT s_store_name, i_item_desc, sc.revenue
         |FROM store, item, (SELECT ss_store_sk, avg(revenue) ave FROM sa GROUP BY ss_store_sk) sb, sa sc
         |WHERE sb.ss_store_sk = sc.ss_store_sk AND sc.revenue <= 0.9 * sb.ave
         |  AND s_store_sk = sc.ss_store_sk AND i_item_sk = sc.ss_item_sk""".stripMargin
    assert(sql.contains("store_sales_p"))
    assert(without(sql) === 2)
    assert(factScans(check(sql)) === 1)
  }

  test("q1's correlated average over the same CTE is scanned once") {
    val sql =
      s"""WITH $sa
         |SELECT ctr1.ss_store_sk, i_item_desc FROM sa ctr1, item
         |WHERE ctr1.revenue > (SELECT avg(revenue) * 1.2 FROM sa ctr2 WHERE ctr1.ss_store_sk = ctr2.ss_store_sk)
         |  AND ctr1.ss_item_sk = i_item_sk""".stripMargin
    assert(factScans(check(sql)) === 1)
  }

  test("copies that differ in another filter, or in their grouping, are left alone") {
    val otherFilter =
      s"""WITH $sa
         |SELECT a.ss_store_sk, i_item_desc, a.revenue FROM sa a, item,
         |  (SELECT ss_store_sk, sum(ss_sales_price) r FROM store_sales, date_dim
         |    WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1 AND 4 AND ss_sales_price > 1
         |    GROUP BY ss_store_sk, ss_item_sk) b
         |WHERE a.ss_item_sk = i_item_sk AND a.ss_store_sk = b.ss_store_sk AND a.revenue < b.r""".stripMargin
    assert(factScans(check(otherFilter)) === 2)
    val otherGrouping =
      s"""WITH $sa
         |SELECT a.ss_store_sk, i_item_desc FROM sa a, item,
         |  (SELECT ss_store_sk, sum(ss_sales_price) r FROM store_sales, date_dim
         |    WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1 AND 4 GROUP BY ss_store_sk) b
         |WHERE a.ss_item_sk = i_item_sk AND a.ss_store_sk = b.ss_store_sk AND a.revenue * 50 < b.r""".stripMargin
    assert(factScans(check(otherGrouping)) === 2)
  }

  test("the switch turns the rewrite off") {
    val sql =
      s"""WITH $sa
         |SELECT s_store_name, i_item_desc, sc.revenue
         |FROM store, item, (SELECT ss_store_sk, avg(revenue) ave FROM sa GROUP BY ss_store_sk) sb, sa sc
         |WHERE sb.ss_store_sk = sc.ss_store_sk AND sc.revenue <= 0.9 * sb.ave
         |  AND s_store_sk = sc.ss_store_sk AND i_item_sk = sc.ss_item_sk""".stripMargin
    withConf(VectorConf.SharedAggregateInputsEnabled -> "false") {
      assert(factScans(check(sql)) === 2)
    }
  }
}
