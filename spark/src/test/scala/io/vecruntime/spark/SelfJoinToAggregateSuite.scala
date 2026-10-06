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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Max, Min}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan}

/** The existence-only self-join to min/max aggregate rewrite (`SelfJoinToAggregate`), against Spark. */
class SelfJoinToAggregateSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // Orders over 1..4 warehouses, with null warehouses and null order numbers mixed in, a
    // double column for the unsupported-type case, and a returns table over some of the orders.
    val sales = (0 until 4000).map { i =>
      val order: java.lang.Long = if (i % 97 == 0) null else Long.box((i / 3).toLong)
      val wh: java.lang.Integer = if (i % 11 == 0) null else Int.box((i * 7) % (1 + (i / 3) % 4))
      (order, wh, (i % 13).toDouble, i.toLong)
    }
    sales.toDF("ws_order_number", "ws_warehouse_sk", "ws_cost", "ws_id")
      .write.mode("overwrite").parquet(newTempPath("selfjoin/web_sales"))
    spark.read.parquet(newTempPath("selfjoin/web_sales")).createOrReplaceTempView("web_sales")
    (0 until 1333 by 4).map(o => Long.box(o.toLong)).toDF("wr_order_number")
      .write.mode("overwrite").parquet(newTempPath("selfjoin/web_returns"))
    spark.read.parquet(newTempPath("selfjoin/web_returns")).createOrReplaceTempView("web_returns")
  }

  private val wsWh =
    """ws_wh AS (SELECT ws1.ws_order_number, ws1.ws_warehouse_sk wh1, ws2.ws_warehouse_sk wh2
      |  FROM web_sales ws1, web_sales ws2
      |  WHERE ws1.ws_order_number = ws2.ws_order_number AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)""".stripMargin

  private def selfJoins(p: LogicalPlan): Int = {
    val here = p.collect { case j @ Join(l, r, Inner, _, _) if l.sameResult(r) => j }.size
    here + p.subqueries.map(selfJoins).sum
  }

  private def minMaxAggregates(p: LogicalPlan): Int = {
    val here = p.collect {
      case a: Aggregate if a.aggregateExpressions.exists(_.exists {
            case AggregateExpression(_: Min, _, _, _, _) => true
            case _ => false
          }) && a.aggregateExpressions.exists(_.exists {
            case AggregateExpression(_: Max, _, _, _, _) => true
            case _ => false
          }) => a
    }.size
    here + p.subqueries.map(minMaxAggregates).sum
  }

  /** Same rows with and without the plugin; returns the plugin's DataFrame. */
  private def check(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true)(spark.sql(sql))
    val actual = withPlugin(enabled = true)(df.collect())
    assertRowsEqual(expected, actual, 0.0, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  private def assertRewritten(df: DataFrame, sql: String): Unit = {
    val p = withPlugin(enabled = true)(df.queryExecution.optimizedPlan)
    assert(selfJoins(p) === 0, s"self-join still in the plan for: $sql\n$p")
    assert(minMaxAggregates(p) > 0, s"no min/max aggregate for: $sql\n$p")
  }

  private def innerJoins(p: LogicalPlan): Int =
    p.collect { case j @ Join(_, _, Inner, _, _) => j }.size + p.subqueries.map(innerJoins).sum

  private def assertKept(df: DataFrame, sql: String): Unit = {
    val p = withPlugin(enabled = true)(df.queryExecution.optimizedPlan)
    assert(innerJoins(p) > 0, s"the join should stay for: $sql\n$p")
    assert(minMaxAggregates(p) === 0, s"no min/max aggregate expected for: $sql\n$p")
  }

  test("q95's ws_wh under IN, both as a plain list and joined with returns") {
    val sql =
      s"""WITH $wsWh
         |SELECT count(DISTINCT ws_order_number) AS orders, sum(ws_cost) AS cost
         |FROM web_sales ws1
         |WHERE ws1.ws_order_number IN (SELECT ws_order_number FROM ws_wh)
         |  AND ws1.ws_order_number IN (SELECT wr_order_number FROM web_returns, ws_wh
         |                              WHERE wr_order_number = ws_wh.ws_order_number)""".stripMargin
    assertRewritten(check(sql), sql)
  }

  test("EXISTS, NOT EXISTS and NOT IN keep their answers") {
    val exists =
      """SELECT count(*), sum(ws_id) FROM web_sales a WHERE EXISTS (
        |  SELECT 1 FROM web_sales b, web_sales c
        |  WHERE b.ws_order_number = c.ws_order_number AND b.ws_warehouse_sk <> c.ws_warehouse_sk
        |    AND b.ws_order_number = a.ws_order_number)""".stripMargin
    assertRewritten(check(exists), exists)
    val notExists = exists.replace("WHERE EXISTS", "WHERE NOT EXISTS")
    assertRewritten(check(notExists), notExists)
    val notIn =
      s"""WITH $wsWh
         |SELECT count(*), sum(ws_id) FROM web_sales WHERE ws_order_number IS NOT NULL
         |  AND ws_order_number NOT IN (SELECT ws_order_number FROM ws_wh)""".stripMargin
    assertRewritten(check(notIn), notIn)
  }

  test("a self-join whose rows are counted, or whose other columns are read, is kept") {
    val counted =
      """SELECT count(*) FROM web_sales ws1, web_sales ws2
        |WHERE ws1.ws_order_number = ws2.ws_order_number AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk""".stripMargin
    assertKept(check(counted), counted)
    val readsV =
      s"""WITH $wsWh
         |SELECT count(*) FROM web_sales WHERE ws_warehouse_sk IN (SELECT wh2 FROM ws_wh)""".stripMargin
    assertKept(check(readsV), readsV)
  }

  test("a floating-point <> column, two <> columns, or different sides keep Spark's plan") {
    val dbl =
      """SELECT count(*) FROM web_sales a WHERE a.ws_order_number IN (
        |  SELECT b.ws_order_number FROM web_sales b, web_sales c
        |  WHERE b.ws_order_number = c.ws_order_number AND b.ws_cost <> c.ws_cost)""".stripMargin
    assertKept(check(dbl), dbl)
    val two =
      """SELECT count(*) FROM web_sales a WHERE a.ws_order_number IN (
        |  SELECT b.ws_order_number FROM web_sales b, web_sales c
        |  WHERE b.ws_order_number = c.ws_order_number AND b.ws_warehouse_sk <> c.ws_warehouse_sk
        |    AND b.ws_id <> c.ws_id)""".stripMargin
    assertKept(check(two), two)
    val sides =
      """SELECT count(*) FROM web_sales a WHERE a.ws_order_number IN (
        |  SELECT b.ws_order_number FROM web_sales b, (SELECT * FROM web_sales WHERE ws_id > 10) c
        |  WHERE b.ws_order_number = c.ws_order_number AND b.ws_warehouse_sk <> c.ws_warehouse_sk)""".stripMargin
    assertKept(check(sides), sides)
  }

  test("the switch turns the rewrite off") {
    val sql =
      s"""WITH $wsWh
         |SELECT count(*) FROM web_sales WHERE ws_order_number IN (SELECT ws_order_number FROM ws_wh)""".stripMargin
    withConf(VectorConf.SelfJoinToAggregateEnabled -> "false") {
      assertKept(check(sql), sql)
    }
  }
}
