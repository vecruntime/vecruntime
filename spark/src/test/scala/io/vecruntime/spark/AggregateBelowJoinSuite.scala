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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan}

class AggregateBelowJoinSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // q4-like: sales by customer and date, many rows per (customer, date); customers with long strings.
    (0 until 30000).map(i =>
      (i % 200, i % 30, BigDecimal(i % 97) / 7, (i % 13).toLong, if (i % 41 == 0) None else Some(i % 5))
    )
      .toDF("s_cust", "s_date", "s_amount", "s_qty", "s_null_cust")
      .write.mode("overwrite").parquet(newTempPath("abj/sales"))
    spark.read.parquet(newTempPath("abj/sales")).write.mode("overwrite").saveAsTable("abj_sales")
    spark.sql("ANALYZE TABLE abj_sales COMPUTE STATISTICS FOR ALL COLUMNS")
    (0 until 200).map(i => (i, s"customer-$i", s"first-${i % 17}", s"last-${i % 23}"))
      .toDF("c_sk", "c_id", "c_first", "c_last")
      .write.mode("overwrite").parquet(newTempPath("abj/customer"))
    spark.read.parquet(newTempPath("abj/customer")).write.mode("overwrite").saveAsTable("abj_customer")
    spark.sql("ANALYZE TABLE abj_customer COMPUTE STATISTICS FOR ALL COLUMNS")
    (0 until 30).map(i => (i, 2000 + i % 3)).toDF("d_sk", "d_year")
      .write.mode("overwrite").parquet(newTempPath("abj/date"))
    spark.read.parquet(newTempPath("abj/date")).write.mode("overwrite").saveAsTable("abj_date")
    spark.sql("ANALYZE TABLE abj_date COMPUTE STATISTICS FOR ALL COLUMNS")
    // The same sales without statistics.
    spark.read.parquet(newTempPath("abj/sales")).createOrReplaceTempView("abj_sales_nostats")
  }

  override protected def afterAll(): Unit = {
    try {
      Seq("abj_sales", "abj_customer", "abj_date").foreach { t =>
        spark.sessionState.catalog.dropTable(
          org.apache.spark.sql.catalyst.TableIdentifier(t),
          ignoreIfNotExists = true,
          purge = false
        )
      }
    } finally super.afterAll()
  }

  private def run(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** An aggregate directly below a join input: the pre-aggregate this rule adds. */
  private def preAggregated(p: LogicalPlan): Boolean = p.exists {
    case j: Join => j.children.exists(_.exists(_.isInstanceOf[Aggregate]))
    case _ => false
  }

  private val q4Shape =
    """SELECT c_id, c_first, c_last, d_year, sum((s_amount - 1) / 2) total, count(s_qty) n, min(s_qty) lo, max(s_amount) hi
      |FROM abj_sales JOIN abj_customer ON s_cust = c_sk JOIN abj_date ON s_date = d_sk
      |GROUP BY c_id, c_first, c_last, d_year""".stripMargin

  test("q4's shape: the fact is aggregated by its join keys before the joins, results unchanged") {
    val df = run(q4Shape)
    assert(preAggregated(df.queryExecution.optimizedPlan), df.queryExecution.optimizedPlan.treeString)
  }

  test("a global aggregate and a count over the fact; null join keys still drop") {
    val df = run(
      "SELECT count(s_qty) n, sum(s_qty) s FROM abj_sales JOIN abj_customer ON s_null_cust = c_sk"
    )
    assert(preAggregated(df.queryExecution.optimizedPlan), df.queryExecution.optimizedPlan.treeString)
  }

  test("an aggregate input that is also the join and grouping key stays available above the pre-aggregate") {
    // SQLQueryTestSuite's order-by-and-having-on-top-of-aggregate-with-join: max over the natural-join key.
    val df = run(
      "SELECT c_first, max(s_cust) m, sum(s_qty) s FROM abj_sales JOIN abj_customer ON s_cust = c_sk GROUP BY c_first, s_cust"
    )
    assert(df.collect().nonEmpty)
  }

  test("declined: grouped by a fact column, a distinct or avg aggregate, a non-equi join column, inputs on two sides") {
    val byFact = run(
      "SELECT s_qty, c_first, sum(s_amount) FROM abj_sales JOIN abj_customer ON s_cust = c_sk GROUP BY s_qty, c_first"
    )
    assert(!preAggregated(byFact.queryExecution.optimizedPlan), byFact.queryExecution.optimizedPlan.treeString)
    val distinct = run(
      "SELECT c_first, count(DISTINCT s_qty) FROM abj_sales JOIN abj_customer ON s_cust = c_sk GROUP BY c_first"
    )
    assert(!preAggregated(distinct.queryExecution.optimizedPlan), distinct.queryExecution.optimizedPlan.treeString)
    val avg = run("SELECT c_first, avg(s_qty) FROM abj_sales JOIN abj_customer ON s_cust = c_sk GROUP BY c_first")
    assert(!preAggregated(avg.queryExecution.optimizedPlan), avg.queryExecution.optimizedPlan.treeString)
    // A selectively filtered dimension: the join prunes the fact at run time, which a pre-aggregate would cost.
    val filtered = run(
      "SELECT c_first, sum(s_qty) FROM abj_sales JOIN abj_customer ON s_cust = c_sk WHERE c_last = 'last-3' GROUP BY c_first"
    )
    assert(!preAggregated(filtered.queryExecution.optimizedPlan), filtered.queryExecution.optimizedPlan.treeString)
    // A fact column in a non-equality join condition (q72's `inv_quantity_on_hand < cs_quantity`).
    val nonEqui = run(
      "SELECT c_first, sum(s_qty) FROM abj_sales JOIN abj_customer ON s_cust = c_sk AND s_qty < c_sk GROUP BY c_first"
    )
    assert(!preAggregated(nonEqui.queryExecution.optimizedPlan), nonEqui.queryExecution.optimizedPlan.treeString)
    // An aggregate input from the dimension side as well: no single side to aggregate.
    val both = run(
      "SELECT d_year, sum(s_qty + c_sk) FROM abj_sales JOIN abj_customer ON s_cust = c_sk JOIN abj_date ON s_date = d_sk GROUP BY d_year"
    )
    assert(!preAggregated(both.queryExecution.optimizedPlan), both.queryExecution.optimizedPlan.treeString)
  }

  test("by default fires without statistics and without a reduction estimate (#675)") {
    val q = q4Shape.replace("abj_sales", "abj_sales_nostats")
    val off = withConf(VectorConf.AggregateBelowJoinEnabled -> "false") { val d = spark.sql(q); d.collect(); d }
    val noStats = run(q)
    assert(preAggregated(noStats.queryExecution.optimizedPlan), noStats.queryExecution.optimizedPlan.treeString)
    assertRowsEqual(off.collect(), noStats.collect(), 1e-9, q)
    // With statistics, no reduction is required unless minReduction asks for one.
    val withStats = run(q4Shape)
    assert(preAggregated(withStats.queryExecution.optimizedPlan), withStats.queryExecution.optimizedPlan.treeString)
  }

  test("declined without statistics, or when the keys reduce the fact's rows less than minReduction, if asked") {
    // 30000 sales rows, at most 200 x 30 = 6000 (customer, date) groups: a 5x reduction.
    val noStats = withConf(VectorConf.AggregateBelowJoinRequireStatistics -> "true")(
      run(q4Shape.replace("abj_sales", "abj_sales_nostats"))
    )
    assert(!preAggregated(noStats.queryExecution.optimizedPlan), noStats.queryExecution.optimizedPlan.treeString)
    val enough = withConf(VectorConf.AggregateBelowJoinMinReduction -> "4")(run(q4Shape))
    assert(preAggregated(enough.queryExecution.optimizedPlan), enough.queryExecution.optimizedPlan.treeString)
    val strict = withConf(VectorConf.AggregateBelowJoinMinReduction -> "10")(run(q4Shape))
    assert(!preAggregated(strict.queryExecution.optimizedPlan), strict.queryExecution.optimizedPlan.treeString)
  }

  test("the switch turns the rewrite off") {
    val df = withConf(VectorConf.AggregateBelowJoinEnabled -> "false")(run(q4Shape))
    assert(!preAggregated(df.queryExecution.optimizedPlan), df.queryExecution.optimizedPlan.treeString)
  }

  // q4/q11/q74 at 1 TB (#675): the year CTE is read twice, each copy pruned to its year by dynamic partition
  // pruning on the date-partitioned fact. A pre-aggregate that loses that pruning reads the whole fact for both
  // copies (q4: 5.05 G input rows against 1.97 G); the result is the same, so only the scans show it.
  private val yearCte =
    """WITH year_total AS (
      |  SELECT c_id, d_year, sum(s_amount) total
      |  FROM abj_psales JOIN abj_customer ON s_cust = c_sk JOIN abj_date ON s_date = d_sk
      |  GROUP BY c_id, d_year)
      |SELECT t1.c_id, t1.total, t2.total FROM year_total t1 JOIN year_total t2 ON t1.c_id = t2.c_id
      |WHERE t1.d_year = 2000 AND t2.d_year = 2001""".stripMargin

  /** The fact scans of the final plan: (partition filters hold a dynamic pruning filter, files read). */
  private def factScans(df: DataFrame): Seq[(Boolean, Long)] = {
    val scans = new scala.collection.mutable.ArrayBuffer[org.apache.spark.sql.execution.FileSourceScanExec]()
    def walk(p: org.apache.spark.sql.execution.SparkPlan): Unit = {
      p match {
        case a: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec => walk(a.executedPlan)
        case s: org.apache.spark.sql.execution.adaptive.QueryStageExec => walk(s.plan)
        case v: org.apache.spark.sql.vecruntime.VectorParquetScanExec => scans += v.scan
        case f: org.apache.spark.sql.execution.FileSourceScanExec => scans += f
        case _ =>
      }
      p.children.foreach(walk)
      p.subqueries.foreach(walk)
    }
    walk(df.queryExecution.executedPlan)
    scans.toSeq.filter(_.tableIdentifier.exists(_.table == "abj_psales")).map { s =>
      val dpp = s.partitionFilters.exists(
        _.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression])
      )
      (dpp, s.metrics.get("numFiles").map(_.value).getOrElse(-1L))
    }
  }

  test("without statistics, a pre-aggregate keeps the dynamic partition pruning of each year copy (#675)") {
    val session = spark
    import session.implicits._
    try {
      spark.read.parquet(
        newTempPath("abj/sales")
      ).write.mode("overwrite").partitionBy("s_date").saveAsTable("abj_psales")
      val off = withConf(VectorConf.AggregateBelowJoinEnabled -> "false") { val d = spark.sql(yearCte); d.collect(); d }
      val on = run(yearCte)
      assertRowsEqual(off.collect(), on.collect(), 1e-9, yearCte)
      assert(preAggregated(on.queryExecution.optimizedPlan), on.queryExecution.optimizedPlan.treeString)
      val (before, after) = (factScans(off), factScans(on))
      assert(before.nonEmpty && before.forall(_._1), s"the reference plan prunes each copy: $before")
      assert(
        after.size == before.size && after.forall(_._1),
        s"rule off $before, rule on $after\n${on.queryExecution.executedPlan}"
      )
      assert(after.map(_._2).sum == before.map(_._2).sum, s"files read: rule off $before, rule on $after")
    } finally {
      spark.sessionState.catalog.dropTable(
        org.apache.spark.sql.catalyst.TableIdentifier("abj_psales"),
        ignoreIfNotExists = true,
        purge = false
      )
    }
  }
}
