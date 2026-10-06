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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** Global aggregates with different filters merged into one pass (`MergeFilteredAggregates`), against Spark. */
class MergeFilteredAggregatesSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // A fact table with nulls in every column used by a filter or an aggregate, and two dimensions.
    val sales = (0 until 6000).map { i =>
      val qty: java.lang.Integer = if (i % 53 == 0) null else Int.box(i % 100)
      val price: java.math.BigDecimal = if (i % 41 == 0) null else java.math.BigDecimal.valueOf((i * 37) % 2000, 2)
      (qty, price, Long.box((i % 24).toLong), Long.box((i % 7).toLong), i.toLong)
    }
    sales.toDF("ss_quantity", "ss_list_price", "ss_sold_time_sk", "ss_store_sk", "ss_id")
      .coalesce(1).write.mode("overwrite").parquet(newTempPath("mergeagg/store_sales"))
    spark.read.parquet(newTempPath("mergeagg/store_sales")).createOrReplaceTempView("store_sales")
    (0 until 24).map(t => (t.toLong, t % 24, (t * 13) % 60)).toDF("t_time_sk", "t_hour", "t_minute")
      .write.mode("overwrite").parquet(newTempPath("mergeagg/time_dim"))
    spark.read.parquet(newTempPath("mergeagg/time_dim")).createOrReplaceTempView("time_dim")
    (0 until 7).map(s => (s.toLong, if (s % 2 == 0) "ese" else "ought")).toDF("s_store_sk", "s_store_name")
      .write.mode("overwrite").parquet(newTempPath("mergeagg/store"))
    spark.read.parquet(newTempPath("mergeagg/store")).createOrReplaceTempView("store")
  }

  /** Scans of `store_sales` left in the optimized plan, subqueries included. */
  private def factScans(p: LogicalPlan): Int = {
    val here = p.collect {
      case r: LogicalRelation if r.output.exists(_.name == "ss_quantity") => r
    }.size
    here + p.subqueries.map(factScans).sum
  }

  private def check(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true)(spark.sql(sql))
    val actual = withPlugin(enabled = true)(df.collect())
    assertRowsEqual(expected, actual, 1e-9, sql)
    df
  }

  private def scans(df: DataFrame): Int = withPlugin(enabled = true)(factScans(df.queryExecution.optimizedPlan))

  private def scansWithout(sql: String): Int =
    withPlugin(enabled = false)(factScans(spark.sql(sql).queryExecution.optimizedPlan))

  private def branch(name: String, hour: Int, minuteCond: String): String =
    s"""(SELECT count(*) $name FROM store_sales, time_dim, store
       |  WHERE ss_sold_time_sk = t_time_sk AND ss_store_sk = s_store_sk AND t_hour = $hour
       |    AND t_minute $minuteCond AND s_store_name = 'ese')""".stripMargin

  test("q88's cross-joined counts over a join tree, differing in a dimension filter, read the fact once") {
    val sql =
      s"""SELECT * FROM ${branch("h8", 8, ">= 30")} s1, ${branch("h9a", 9, "< 30")} s2,
         |  ${branch("h9b", 9, ">= 30")} s3, ${branch("h10", 10, "< 30")} s4""".stripMargin
    assert(scansWithout(sql) === 4)
    assert(scans(check(sql)) === 1)
  }

  test("q28's aggregates with DISTINCT and different fact filters, and q90's ratio") {
    def b(n: Int, lo: Int, hi: Int, plo: Int) =
      s"""(SELECT avg(ss_list_price) B${n}_LP, count(ss_list_price) B${n}_CNT, count(DISTINCT ss_list_price) B${n}_CNTD
         |  FROM store_sales WHERE ss_quantity BETWEEN $lo AND $hi
         |    AND (ss_list_price BETWEEN $plo AND $plo + 10 OR ss_quantity > 90)) B$n""".stripMargin
    val q28 = s"SELECT * FROM ${b(1, 0, 5, 1)}, ${b(2, 6, 10, 5)}, ${b(3, 11, 15, 9)}"
    assert(scansWithout(q28) === 3)
    assert(scans(check(q28)) === 1)
    val q90 =
      """SELECT cast(amc AS decimal(15, 4)) / cast(pmc AS decimal(15, 4)) am_pm_ratio
        |FROM (SELECT count(*) amc FROM store_sales, time_dim WHERE ss_sold_time_sk = t_time_sk AND t_hour BETWEEN 8 AND 9) at,
        |     (SELECT count(*) pmc FROM store_sales, time_dim WHERE ss_sold_time_sk = t_time_sk AND t_hour BETWEEN 19 AND 20) pt""".stripMargin
    assert(scans(check(q90)) === 1)
  }

  test("q9's scalar subqueries with different filters are computed once") {
    def bucket(lo: Int, hi: Int, t: Int) =
      s"""CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN $lo AND $hi) > $t
         |  THEN (SELECT avg(ss_list_price) FROM store_sales WHERE ss_quantity BETWEEN $lo AND $hi)
         |  ELSE (SELECT max(ss_list_price) FROM store_sales WHERE ss_quantity BETWEEN $lo AND $hi) END""".stripMargin
    val sql =
      s"SELECT ${bucket(1, 20, 600)} b1, ${bucket(21, 40, 1200)} b2, ${bucket(41, 60, 10)} b3 FROM store WHERE s_store_sk = 1"
    val df = check(sql)
    // Spark's MergeScalarSubqueries already shares each bucket's three; the buckets now share one too.
    assert(executedFactScans(withPlugin(enabled = false) { val d = spark.sql(sql); d.collect(); d }) === 3)
    assert(executedFactScans(df) === 1)
    // Each aggregate keeps its own filter: folding the third and later inputs in must not AND the
    // growing union of the others onto it (it reached ~50 KB of predicate per aggregate).
    def filters(p: LogicalPlan): Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      p.flatMap(_.expressions.flatMap(_.collect {
        case ae: org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression => ae.filter.toSeq
      }.flatten)) ++ p.subqueries.flatMap(filters)
    val sizes = filters(withPlugin(enabled = true)(df.queryExecution.optimizedPlan)).map(_.treeString.length)
    assert(sizes.nonEmpty && sizes.max < 400, s"filter sizes: $sizes")
  }

  /** `store_sales` scans the executed plan runs, subqueries included, each reused one counted once. */
  private def executedFactScans(df: DataFrame): Int = {
    import org.apache.spark.sql.execution.{ReusedSubqueryExec, SparkPlan}
    import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
    import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
    val seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[SparkPlan, java.lang.Boolean])
    var scans = 0
    def walk(p: SparkPlan): Unit = p match {
      case _: ReusedSubqueryExec | _: ReusedExchangeExec => ()
      case _ if !seen.add(p) => ()
      case a: AdaptiveSparkPlanExec => walk(a.executedPlan)
      case q: QueryStageExec => walk(q.plan)
      case _ =>
        if (p.children.isEmpty && p.getClass.getSimpleName.contains("Scan") && p.output.exists(_.name == "ss_quantity"))
          scans += 1
        p.children.foreach(walk)
        p.subqueries.foreach(walk)
    }
    walk(df.queryExecution.executedPlan)
    scans
  }

  test("a grouped FILTER aggregate whose later batches only add groups (found by the q28 merge)") {
    // Small batches: the first holds every passing row, the later ones only new groups.
    withConf("spark.sql.parquet.columnarReaderBatchSize" -> "100") {
      val sql =
        """SELECT ss_id, avg(ss_list_price) FILTER (WHERE ss_id < 50) a, count(*) FILTER (WHERE ss_id < 50) c
          |FROM store_sales GROUP BY ss_id""".stripMargin
      check(sql)
    }
  }

  test("different leaves, an outer join, or a grouped aggregate are not merged") {
    val leaves =
      """SELECT * FROM (SELECT count(*) a FROM store_sales WHERE ss_quantity > 10) x,
        |              (SELECT count(*) b FROM time_dim WHERE t_hour > 10) y""".stripMargin
    check(leaves)
    val outer =
      """SELECT * FROM (SELECT count(t_hour) a FROM store_sales LEFT JOIN time_dim ON ss_sold_time_sk = t_time_sk AND t_hour = 8) x,
        |              (SELECT count(t_hour) b FROM store_sales LEFT JOIN time_dim ON ss_sold_time_sk = t_time_sk AND t_hour = 9) y""".stripMargin
    assert(scans(check(outer)) === 2)
    val grouped =
      """SELECT * FROM (SELECT ss_store_sk, count(*) a FROM store_sales WHERE ss_quantity > 10 GROUP BY ss_store_sk) x
        |JOIN (SELECT ss_store_sk k, count(*) b FROM store_sales WHERE ss_quantity < 10 GROUP BY ss_store_sk) y
        |ON x.ss_store_sk = y.k""".stripMargin
    assert(scans(check(grouped)) === 2)
  }

  test("the switch turns the rewrite off") {
    val sql = s"SELECT * FROM ${branch("h8", 8, ">= 30")} s1, ${branch("h9a", 9, "< 30")} s2"
    withConf(VectorConf.MergeFilteredAggregatesEnabled -> "false") {
      assert(scans(check(sql)) === 2)
    }
  }
}
