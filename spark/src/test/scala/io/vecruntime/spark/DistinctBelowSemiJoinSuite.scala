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
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan}
import org.apache.spark.sql.vecruntime.LocalPreAggregate

/** #840: a left semi join's right side as its distinct join columns, one aggregate per task. */
@io.vecruntime.spark.test.tags.Agg
@io.vecruntime.spark.test.tags.Join
class DistinctBelowSemiJoinSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // q14's cross_items: items, and sales whose (brand, class, category) repeat many times, some null.
    (0 until 2000).map { i =>
      val brand = if (i % 97 == 0) None else Some(i % 13)
      (i, brand, i % 7, i % 5)
    }.toDF("i_sk", "brand", "klass", "category")
      .write.mode("overwrite").parquet(newTempPath("dbs/item"))
    (0 until 40000).map { i =>
      val brand = if (i % 101 == 0) None else Some(i % 11)
      (i % 2000, brand, i % 6, i % 5, (i % 17).toLong)
    }.toDF("s_item", "s_brand", "s_klass", "s_category", "qty")
      .write.mode("overwrite").parquet(newTempPath("dbs/sales"))
    spark.read.parquet(newTempPath("dbs/item")).createOrReplaceTempView("dbs_item")
    spark.read.parquet(newTempPath("dbs/sales")).createOrReplaceTempView("dbs_sales")
  }

  /** Nothing broadcast by size: the sales side is the large, shuffled one, as at 10 TB. */
  private val shuffled = "spark.sql.autoBroadcastJoinThreshold" -> "-1"

  private def run(sql: String, conf: (String, String)*): DataFrame = withConf(conf: _*) {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** The distinct right sides of left semi joins this rule added. */
  private def reduced(p: LogicalPlan): Seq[Aggregate] = p.collect {
    case Join(_, a: Aggregate, LeftSemi, _, _)
        if LocalPreAggregate.isLocal(a) && a.aggregateExpressions == a.groupingExpressions => a
  }

  private val intersect =
    """SELECT brand, klass, category FROM dbs_item
      |INTERSECT SELECT s_brand, s_klass, s_category FROM dbs_sales""".stripMargin

  test("INTERSECT: the right side is reduced to its distinct columns, results unchanged (nulls match)") {
    val df = run(intersect, shuffled)
    val plan = df.queryExecution.optimizedPlan
    val aggs = reduced(plan)
    assert(aggs.size == 1, plan.treeString)
    assert(aggs.head.groupingExpressions.map(_.toString).forall(_.startsWith("s_")), plan.treeString)
  }

  test("q14's chain: a second INTERSECT over the first, and an IN subquery") {
    val chained = run(
      s"""SELECT * FROM ($intersect) x
         |INTERSECT SELECT s_brand, s_klass, s_category FROM dbs_sales WHERE qty > 3""".stripMargin,
      shuffled
    )
    assert(reduced(chained.queryExecution.optimizedPlan).size == 2, chained.queryExecution.optimizedPlan.treeString)
    val in = run(
      "SELECT i_sk, brand FROM dbs_item WHERE (brand, klass) IN (SELECT s_brand, s_klass FROM dbs_sales WHERE qty < 9)",
      shuffled
    )
    assert(reduced(in.queryExecution.optimizedPlan).size == 1, in.queryExecution.optimizedPlan.treeString)
  }

  test("a non-equi condition keeps the columns it reads") {
    val df = run(
      """SELECT i_sk FROM dbs_item i WHERE EXISTS
        |(SELECT 1 FROM dbs_sales s WHERE s.s_item = i.i_sk AND s.qty > i.category)""".stripMargin,
      shuffled
    )
    val aggs = reduced(df.queryExecution.optimizedPlan)
    assert(aggs.size == 1 && aggs.head.groupingExpressions.size == 2, df.queryExecution.optimizedPlan.treeString)
  }

  test("declined: a right side small enough to broadcast, or the rule off") {
    val small = run(intersect, "spark.sql.autoBroadcastJoinThreshold" -> (64L << 20).toString)
    assert(reduced(small.queryExecution.optimizedPlan).isEmpty, small.queryExecution.optimizedPlan.treeString)
    val off = run(intersect, shuffled, VectorConf.DistinctBelowSemiJoinEnabled -> "false")
    assert(reduced(off.queryExecution.optimizedPlan).isEmpty, off.queryExecution.optimizedPlan.treeString)
  }

  // #904: left anti joins.

  /** The distinct right sides of left anti joins this rule added. */
  private def reducedAnti(p: LogicalPlan): Seq[Aggregate] = p.collect {
    case Join(_, a: Aggregate, LeftAnti, _, _)
        if LocalPreAggregate.isLocal(a) && a.aggregateExpressions == a.groupingExpressions => a
  }

  /** As `run`, for a query whose correct answer may be empty (NOT IN over a right side with a null). */
  private def runAny(sql: String, conf: (String, String)*): (DataFrame, Int) = withConf(conf: _*) {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    (df, expected.length)
  }

  test("NOT EXISTS and EXCEPT: the right side is reduced, results unchanged (nulls match in EXCEPT)") {
    val notExists = run(
      """SELECT i_sk FROM dbs_item i WHERE NOT EXISTS
        |(SELECT 1 FROM dbs_sales s WHERE s.s_item = i.i_sk + 1000 AND s.qty > 3)""".stripMargin,
      shuffled
    )
    assert(
      reducedAnti(notExists.queryExecution.optimizedPlan).size == 1,
      notExists.queryExecution.optimizedPlan.treeString
    )
    val except = run(
      "SELECT brand, klass, category FROM dbs_item EXCEPT SELECT s_brand, s_klass, s_category FROM dbs_sales",
      shuffled
    )
    assert(reducedAnti(except.queryExecution.optimizedPlan).size == 1, except.queryExecution.optimizedPlan.treeString)
  }

  test("NOT IN: a right side with a null, without one, and empty") {
    // A null in the right side: NOT IN is never true, the answer is empty with the rule as without.
    val (withNull, n1) =
      runAny("SELECT i_sk FROM dbs_item WHERE brand NOT IN (SELECT s_brand FROM dbs_sales WHERE qty < 9)", shuffled)
    assert(n1 == 0)
    assert(
      reducedAnti(withNull.queryExecution.optimizedPlan).size == 1,
      withNull.queryExecution.optimizedPlan.treeString
    )
    val (noNull, n2) = runAny(
      "SELECT i_sk FROM dbs_item WHERE brand NOT IN (SELECT s_brand FROM dbs_sales WHERE s_brand IS NOT NULL AND qty < 9)",
      shuffled
    )
    assert(n2 > 0)
    assert(reducedAnti(noNull.queryExecution.optimizedPlan).size == 1, noNull.queryExecution.optimizedPlan.treeString)
    // An empty right side: every left row, null keys included.
    val (_, n3) =
      runAny("SELECT i_sk FROM dbs_item WHERE brand NOT IN (SELECT s_brand FROM dbs_sales WHERE qty > 100)", shuffled)
    assert(n3 == 2000)
  }

  test("anti joins declined with their switch off, semi joins unaffected") {
    val sql =
      """SELECT i_sk FROM dbs_item i WHERE NOT EXISTS
        |(SELECT 1 FROM dbs_sales s WHERE s.s_item = i.i_sk + 1000)""".stripMargin
    val off = run(sql, shuffled, VectorConf.DistinctBelowAntiJoinEnabled -> "false")
    assert(reducedAnti(off.queryExecution.optimizedPlan).isEmpty, off.queryExecution.optimizedPlan.treeString)
    val semiStill = run(intersect, shuffled, VectorConf.DistinctBelowAntiJoinEnabled -> "false")
    assert(reduced(semiStill.queryExecution.optimizedPlan).size == 1, semiStill.queryExecution.optimizedPlan.treeString)
  }
}
