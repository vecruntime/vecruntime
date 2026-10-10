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

  /** Every node of the final plan, through adaptive stages and subqueries. */
  private def nodes(df: DataFrame): Seq[org.apache.spark.sql.execution.SparkPlan] = {
    val all = new scala.collection.mutable.ArrayBuffer[org.apache.spark.sql.execution.SparkPlan]()
    def walk(p: org.apache.spark.sql.execution.SparkPlan): Unit = {
      p match {
        case a: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec => walk(a.executedPlan)
        case s: org.apache.spark.sql.execution.adaptive.QueryStageExec => walk(s.plan)
        case _ => all += p
      }
      p.children.foreach(walk)
      p.subqueries.foreach(walk)
    }
    walk(df.queryExecution.executedPlan)
    all.toSeq
  }

  private def isExchange(p: org.apache.spark.sql.execution.SparkPlan): Boolean =
    p.isInstanceOf[org.apache.spark.sql.execution.exchange.Exchange] ||
      p.isInstanceOf[org.apache.spark.sql.execution.adaptive.QueryStageExec]

  /** The local pre-aggregates of the final plan, each checked to read the fact with no exchange in between. */
  private def localPreAggregates(df: DataFrame): Seq[org.apache.spark.sql.vecruntime.VectorHashAggregateExec] = {
    val local = nodes(df).collect {
      case a: org.apache.spark.sql.vecruntime.VectorHashAggregateExec if a.localPreAggregate => a
    }
    local.foreach(a => assert(!a.child.exists(isExchange), s"an exchange below the local pre-aggregate:\n$a"))
    local
  }

  test("#693: the pre-aggregate is one per task -- no exchange of the fact on its keys -- and it reduces") {
    val df = run(q4Shape)
    val local = localPreAggregates(df)
    assert(local.size == 1, nodes(df).mkString("\n"))
    val out = local.head.metrics("numOutputRows").value
    // 30000 rows, 6000 (customer, date) pairs: the tables a task emits hold far fewer rows than it read.
    assert(out > 0 && out < 30000 / 2, s"$out groups out of 30000 rows")
    // Spark's two stages are what `local=false` restores: a Partial, the exchange, a Final.
    val global = withConf(VectorConf.AggregateBelowJoinLocal -> "false")(run(q4Shape))
    assert(localPreAggregates(global).isEmpty)
    assert(preAggregated(global.queryExecution.optimizedPlan), global.queryExecution.optimizedPlan.treeString)
  }

  // q47's shape at 1 TB: grouped by (item, store, date) the fact barely repeats a key. Here every row has its own
  // (s_item, s_cust) pair, so the pre-aggregate cannot reduce anything.
  private val uniqueKeys =
    """SELECT c_first, i_brand, sum(s_amount) total, count(s_qty) n, max(s_qty) hi
      |FROM abj_usales JOIN abj_customer ON s_cust = c_sk JOIN abj_item ON s_item = i_sk
      |GROUP BY c_first, i_brand""".stripMargin

  private def withUniqueKeyTables[T](body: => T): T = {
    val session = spark
    import session.implicits._
    try {
      (0 until 30000).map(i => (i, i % 200, BigDecimal(i % 89) / 3, (i % 11).toLong))
        .toDF("s_item", "s_cust", "s_amount", "s_qty")
        .repartition(1)
        .write.mode("overwrite").saveAsTable("abj_usales")
      spark.sql("ANALYZE TABLE abj_usales COMPUTE STATISTICS FOR ALL COLUMNS")
      (0 until 30000).map(i => (i, s"brand-${i % 50}")).toDF("i_sk", "i_brand")
        .write.mode("overwrite").saveAsTable("abj_item")
      spark.sql("ANALYZE TABLE abj_item COMPUTE STATISTICS FOR ALL COLUMNS")
      body
    } finally {
      Seq("abj_usales", "abj_item").foreach { t =>
        spark.sessionState.catalog.dropTable(
          org.apache.spark.sql.catalyst.TableIdentifier(t),
          ignoreIfNotExists = true,
          purge = false
        )
      }
    }
  }

  test("#693: keys that do not repeat -- the local pre-aggregate gives up after its first rows, results unchanged") {
    withUniqueKeyTables {
      // A probe of 2000 rows: the first table goes out after the first batch and the rest passes through.
      val df = withConf(VectorConf.AggregateBelowJoinProbeRows -> "2000")(
        run(uniqueKeys)
      )
      val local = localPreAggregates(df)
      assert(local.size == 1, nodes(df).mkString("\n"))
      assert(local.head.localProbe, "statistics cannot prove a reduction here: the aggregate is judged early")
      assert(
        local.head.metrics("spills").value >= 1,
        s"the first table went out at the probe: ${local.head.metrics.map { case (k, v) => k -> v.value }}\n${df.queryExecution.executedPlan}"
      )
      assert(local.head.metrics("numOutputRows").value == 30000L, "nothing reduced, every row passed")
    }
  }

  test("#693: with statistics, a proven reduction is not judged early; without them, or unproven, it is") {
    // With statistics: at most 200 x 30 = 6000 groups for 30000 rows, 5x -- proven, judged at the budget only.
    val proven = run(q4Shape)
    assert(localPreAggregates(proven).map(_.localProbe) == Seq(false), proven.queryExecution.executedPlan.toString)
    // The same data without statistics: nothing proven, the first rows decide.
    val q = q4Shape.replace("abj_sales", "abj_sales_nostats")
    val unproven = run(q)
    assert(localPreAggregates(unproven).map(_.localProbe) == Seq(true), unproven.queryExecution.executedPlan.toString)
    // Statistics that bound the groups above the rows prove nothing either (q47's case, and q4's at 1 TB).
    withUniqueKeyTables {
      val unique = run(uniqueKeys)
      assert(localPreAggregates(unique).map(_.localProbe) == Seq(true), unique.queryExecution.executedPlan.toString)
    }
  }

  test("#693: an aggregate used twice in one copy of a CTE keeps the copies alike, so the exchange is reused") {
    // q47's shape: `sum(...)` is a result and the input of a window over the same aggregate. The copy that keeps
    // the window has the sum twice (two result ids); a pre-aggregate with a partial per copy of it made that copy's
    // exchange differ from the other's, and the copy ran again instead of reusing the exchange.
    val q =
      """WITH v AS (
        |  SELECT c_first, d_year, sum(s_qty) s, avg(sum(s_qty)) OVER (PARTITION BY c_first) w
        |  FROM abj_sales JOIN abj_customer ON s_cust = c_sk JOIN abj_date ON s_date = d_sk
        |  GROUP BY c_first, d_year)
        |SELECT a.c_first, a.d_year, a.s, a.w, b.s FROM v a JOIN v b ON a.c_first = b.c_first AND a.d_year = b.d_year + 1""".stripMargin
    def computedPreAggregates(df: DataFrame): Int = {
      var n = 0
      def walk(p: org.apache.spark.sql.execution.SparkPlan): Unit = p match {
        case a: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec => walk(a.executedPlan)
        case s: org.apache.spark.sql.execution.adaptive.QueryStageExec => walk(s.plan)
        case _: org.apache.spark.sql.execution.exchange.ReusedExchangeExec => // computed elsewhere
        case other =>
          other match {
            case a: org.apache.spark.sql.vecruntime.VectorHashAggregateExec if a.localPreAggregate => n += 1
            case _ =>
          }
          other.children.foreach(walk)
      }
      walk(df.queryExecution.executedPlan)
      n
    }
    val df = run(q)
    val pre = df.queryExecution.optimizedPlan.collect {
      case a: Aggregate => a.aggregateExpressions.count(_.name.startsWith("_pre_agg_"))
    }.filter(_ > 0)
    assert(pre.nonEmpty && pre.forall(_ == 1), s"one partial per distinct function: $pre")
    assert(computedPreAggregates(df) == 1, df.queryExecution.executedPlan.toString)
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
