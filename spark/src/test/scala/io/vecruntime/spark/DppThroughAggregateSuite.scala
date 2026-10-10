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
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}

/** Dynamic partition pruning through an aggregate (#633), against Spark with the plugin off. */
class DppThroughAggregateSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // Two facts partitioned by their date key (60 dates), a date dimension with weeks and months.
    def fact(name: String, prefix: String, salt: Int): Unit = {
      (0 until 6000).map { i =>
        val store: java.lang.Long = if (i % 41 == 0) null else Long.box(((i + salt) % 7).toLong)
        ((i % 60).toLong, store, java.math.BigDecimal.valueOf(((i + salt) * 37) % 9000, 2))
      }.toDF(s"${prefix}_sold_date_sk", s"${prefix}_store_sk", s"${prefix}_sales_price")
        .write.mode("overwrite").partitionBy(s"${prefix}_sold_date_sk").parquet(newTempPath(s"dppagg/$name"))
      spark.read.parquet(newTempPath(s"dppagg/$name")).createOrReplaceTempView(name)
    }
    fact("store_sales", "ss", 0)
    fact("web_sales", "ws", 3)
    (0 until 60).map(d => (d.toLong, d / 7, d / 30, if (d % 7 == 0) "Sunday" else "Other"))
      .toDF("d_date_sk", "d_week_seq", "d_month_seq", "d_day_name")
      .write.mode("overwrite").parquet(newTempPath("dppagg/date_dim"))
    spark.read.parquet(newTempPath("dppagg/date_dim")).createOrReplaceTempView("date_dim")
  }

  private def run(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** Pruning subqueries the rule (or Spark) put on a fact's partition column. */
  private def factPruning(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f }
      .flatMap(_.condition.collect { case d: DynamicPruningSubquery => d })
      .count(_.pruningKey.references.exists(_.name.endsWith("_sold_date_sk")))

  /** Files the executed scans read (the scans' "number of files read" metric). */
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

  private def withRuleOff[T](f: => T): T = withConf(VectorConf.DppThroughAggregateEnabled -> "false")(f)

  private val wss =
    """(SELECT d_week_seq, ss_store_sk, sum(ss_sales_price) sales
      |  FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
      |  GROUP BY d_week_seq, ss_store_sk) wss""".stripMargin

  test("q59's shape: a fact grouped by a dimension key joined to a filtered dimension is pruned") {
    val sql = s"SELECT wss.* FROM $wss JOIN date_dim d ON wss.d_week_seq = d.d_week_seq WHERE d.d_month_seq = 1"
    val on = run(sql)
    assert(factPruning(on) === 1)
    val off = withRuleOff(run(sql))
    assert(factPruning(off) === 0)
    assert(filesRead(on) < filesRead(off), s"pruned ${filesRead(on)} vs ${filesRead(off)} files")
  }

  test("q2's shape: every branch of a union of facts is pruned") {
    val sql =
      """SELECT w.* FROM
        |  (SELECT d_week_seq, sum(p) sales FROM
        |     (SELECT ss_sold_date_sk sold, ss_sales_price p FROM store_sales
        |      UNION ALL SELECT ws_sold_date_sk sold, ws_sales_price p FROM web_sales) u
        |     JOIN date_dim ON sold = d_date_sk
        |   GROUP BY d_week_seq) w
        |JOIN date_dim d ON w.d_week_seq = d.d_week_seq WHERE d.d_month_seq = 0 AND d.d_day_name = 'Sunday'""".stripMargin
    val on = run(sql)
    assert(factPruning(on) === 2)
    assert(filesRead(on) < withRuleOff(filesRead(run(sql))))
  }

  test("a join on an aggregate output that is not a grouping key is left alone") {
    val sql =
      """SELECT a.* FROM
        |  (SELECT ss_store_sk, max(d_week_seq) mw FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
        |   GROUP BY ss_store_sk) a
        |JOIN date_dim d ON a.mw = d.d_week_seq WHERE d.d_month_seq = 1""".stripMargin
    assert(factPruning(run(sql)) === 0)
  }

  test("an outer join that keeps the aggregate's rows is left alone") {
    val sql =
      s"""SELECT wss.*, d.d_month_seq FROM $wss
         |LEFT JOIN (SELECT * FROM date_dim WHERE d_month_seq = 1) d ON wss.d_week_seq = d.d_week_seq""".stripMargin
    assert(factPruning(run(sql)) === 0)
  }

  test("the switch turns the rewrite off") {
    val sql = s"SELECT wss.* FROM $wss JOIN date_dim d ON wss.d_week_seq = d.d_week_seq WHERE d.d_month_seq = 1"
    withRuleOff(assert(factPruning(run(sql)) === 0))
  }
}
