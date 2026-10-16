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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BROADCAST, Join, LogicalPlan, RepartitionByExpression}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.vecruntime.LocalPreAggregate

/** #692: a join input that only supplies grouping columns, collapsed to its distinct rows and their count. */
class CountBelowJoinSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // q24's shape: sales of customers with a birth country, in stores with a zip; addresses joined on the pair
    // (country, zip) alone, so each sale meets every address of its zip -- 400 per zip here.
    (0 until 20000).map { i =>
      val amount = if (i % 53 == 0) None else Some(BigDecimal(i % 997) / 100)
      (s"name-${i % 37}", s"COUNTRY-${i % 2}", s"zip-${i % 20}", amount, (i % 11).toLong, (i % 7).toDouble)
    }.toDF("c_name", "c_country", "s_zip", "net_paid", "qty", "price")
      // decimal(7, 2) as ss_net_paid: Spark sums it as a long of unscaled values, which the rule can weigh.
      .withColumn("net_paid", org.apache.spark.sql.functions.col("net_paid").cast("decimal(7,2)"))
      .withColumn("wide_paid", org.apache.spark.sql.functions.col("net_paid").cast("decimal(17,2)"))
      .withColumn("wider_paid", org.apache.spark.sql.functions.col("net_paid").cast("decimal(18,2)"))
      .write.mode("overwrite").parquet(newTempPath("cbj/sales"))
    (0 until 16000).map { i =>
      // A few null zips: dropped by the join, as each of their rows would be.
      val zip = if (i % 997 == 0) None else Some(s"zip-${i % 20}")
      (i, s"country-${(i / 20) % 2}", zip, s"state-${i % 5}", (i % 13).toLong)
    }.toDF("a_sk", "a_country", "a_zip", "a_state", "a_n")
      .write.mode("overwrite").parquet(newTempPath("cbj/address"))
    spark.read.parquet(newTempPath("cbj/sales")).createOrReplaceTempView("cbj_sales")
    spark.read.parquet(newTempPath("cbj/address")).createOrReplaceTempView("cbj_address")
    // The same address table with statistics.
    spark.read.parquet(newTempPath("cbj/address")).write.mode("overwrite").saveAsTable("cbj_address_stats")
    spark.sql("ANALYZE TABLE cbj_address_stats COMPUTE STATISTICS FOR ALL COLUMNS")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sessionState.catalog.dropTable(
        org.apache.spark.sql.catalyst.TableIdentifier("cbj_address_stats"),
        ignoreIfNotExists = true,
        purge = false
      )
    } finally super.afterAll()
  }

  /** No side is broadcast by size: the address table is the shuffled, exploding side, as at 3 TB. */
  private val shuffled = "spark.sql.autoBroadcastJoinThreshold" -> "-1"

  private def run(sql: String, conf: (String, String)*): DataFrame = withConf(conf: _*) {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  private def collapsed(p: LogicalPlan): Seq[Aggregate] = p.collect {
    case a: Aggregate if a.aggregateExpressions.exists(_.name == "_cbj_count") => a
  }

  private def repartitioned(p: LogicalPlan): Boolean = p.exists(_.isInstanceOf[RepartitionByExpression])

  private def broadcastHinted(p: LogicalPlan): Boolean = p.exists {
    case j: Join => Seq(j.hint.leftHint, j.hint.rightHint).flatten.exists(_.strategy.contains(BROADCAST))
    case _ => false
  }

  private def exchanges(df: DataFrame): Int = nodesOf[ShuffleExchangeLike](df).size

  private val q24Shape =
    """SELECT c_name, a_state, sum(net_paid) paid, count(*) n, count(net_paid) n_paid, min(qty) lo, max(price) hi
      |FROM cbj_sales JOIN cbj_address ON c_country = upper(a_country) AND s_zip = a_zip
      |GROUP BY c_name, a_state""".stripMargin

  test("q24's shape: the address side collapses to (country, zip, state) and a count, results unchanged") {
    val df = run(q24Shape, shuffled)
    val plan = df.queryExecution.optimizedPlan
    val aggs = collapsed(plan)
    assert(aggs.size == 1, plan.treeString)
    // Grouped by the columns the plan above reads (zip, state) and the key expression's column; nothing else.
    assert(aggs.head.groupingExpressions.size == 3, plan.treeString)
    assert(repartitioned(plan) && !broadcastHinted(plan), plan.treeString)
    // Without statistics: one aggregate per task, judged on its first rows.
    assert(aggs.head.getTagValue(LocalPreAggregate.Tag).exists(!_.provenReduction), plan.treeString)
  }

  test("the collapse costs no exchange: the repartition is the one the join needs") {
    val on = run(q24Shape, shuffled)
    val off = withConf(shuffled, VectorConf.CountBelowJoinEnabled -> "false") {
      val d = spark.sql(q24Shape); d.collect(); d
    }
    assert(collapsed(off.queryExecution.optimizedPlan).isEmpty)
    assert(exchanges(on) == exchanges(off), s"${finalPlan(on).treeString}\n----\n${finalPlan(off).treeString}")
  }

  test("filters and grouping over the collapsed side's columns, a global aggregate, a sum of doubles") {
    run(
      """SELECT a_state, sum(price) s, count(qty) n FROM cbj_sales JOIN cbj_address ON s_zip = a_zip
        |WHERE a_state <> 'state-3' AND c_name > upper(a_country) GROUP BY a_state""".stripMargin,
      shuffled
    )
    val global = run(
      "SELECT sum(net_paid), count(*), max(qty) FROM cbj_sales JOIN cbj_address ON s_zip = a_zip",
      shuffled
    )
    assert(collapsed(global.queryExecution.optimizedPlan).nonEmpty)
  }

  test("declined: an input from the side, avg or distinct, an outer join, a side small enough to broadcast") {
    def none(sql: String, conf: (String, String)*): Unit = {
      val df = run(sql, conf: _*)
      assert(collapsed(df.queryExecution.optimizedPlan).isEmpty, df.queryExecution.optimizedPlan.treeString)
    }
    // The side holding the aggregate input stays; the sales side, which then only supplies c_name, may collapse.
    val input =
      run("SELECT c_name, sum(a_n) FROM cbj_sales JOIN cbj_address ON s_zip = a_zip GROUP BY c_name", shuffled)
    assert(
      collapsed(input.queryExecution.optimizedPlan).forall(_.output.forall(!_.name.startsWith("a_"))),
      input.queryExecution.optimizedPlan.treeString
    )
    none("SELECT a_state, avg(qty) FROM cbj_sales JOIN cbj_address ON s_zip = a_zip GROUP BY a_state", shuffled)
    // A decimal whose product with the count could overflow 38 digits is left alone (#762).
    none("SELECT a_state, sum(wider_paid) FROM cbj_sales JOIN cbj_address ON s_zip = a_zip GROUP BY a_state", shuffled)
    none(
      "SELECT a_state, count(DISTINCT qty) FROM cbj_sales JOIN cbj_address ON s_zip = a_zip GROUP BY a_state",
      shuffled
    )
    none(
      "SELECT a_state, sum(qty) FROM cbj_sales LEFT JOIN cbj_address ON s_zip = a_zip GROUP BY a_state",
      shuffled
    )
    // Broadcast whole anyway: a repartition would only add a shuffle.
    none(
      "SELECT a_state, sum(qty) FROM cbj_sales JOIN cbj_address ON s_zip = a_zip GROUP BY a_state",
      "spark.sql.autoBroadcastJoinThreshold" -> (64L << 20).toString
    )
  }

  test("a decimal(17,2) sum is weighted in 38 digits and cast back, every aggregate stage ours (#762)") {
    // q23a's shape at 3 TB: a sum too wide for Spark's long rewrite, alone and beside a long sum and a count.
    for (ansi <- Seq("false", "true")) {
      val sql = "SELECT a_state, sum(wide_paid) w, sum(qty) q, count(*) n FROM cbj_sales JOIN cbj_address " +
        "ON s_zip = a_zip GROUP BY a_state"
      // AggregateBelowJoin pre-aggregates the sales side of this join first; off, the address side is collapsed
      // and the sums are weighted. On, both rules apply as at 3 TB: the plan must stay ours either way.
      val weighted =
        run(sql, shuffled, "spark.sql.ansi.enabled" -> ansi, VectorConf.AggregateBelowJoinEnabled -> "false")
      assert(
        collapsed(weighted.queryExecution.optimizedPlan).size == 1,
        weighted.queryExecution.optimizedPlan.treeString
      )
      for (df <- Seq(weighted, run(sql, shuffled, "spark.sql.ansi.enabled" -> ansi))) {
        assert(df.schema("w").dataType == org.apache.spark.sql.types.DecimalType(27, 2), df.schema.treeString)
        assert(
          nodesOf[org.apache.spark.sql.execution.aggregate.BaseAggregateExec](df).isEmpty,
          s"a Spark aggregate is left: ${finalPlan(df).treeString}"
        )
      }
    }
  }

  private val withStats = q24Shape.replace("cbj_address", "cbj_address_stats")

  test("statistics: a proven reduction is not judged on the first rows") {
    val df = run(withStats, shuffled)
    val aggs = collapsed(df.queryExecution.optimizedPlan)
    assert(aggs.size == 1 && repartitioned(df.queryExecution.optimizedPlan))
    assert(aggs.head.getTagValue(LocalPreAggregate.Tag).exists(_.provenReduction))
  }

  test("statistics: a side joined on its unique key is left alone") {
    val df = run(
      "SELECT a_state, sum(qty) FROM cbj_sales JOIN cbj_address_stats ON qty = a_sk GROUP BY a_state",
      shuffled
    )
    assert(collapsed(df.queryExecution.optimizedPlan).isEmpty, df.queryExecution.optimizedPlan.treeString)
  }

  test("statistics: a side small once collapsed is aggregated and broadcast, the large side keeps no exchange") {
    // The raw side does not fit the threshold; 2 x 21 x 5 groups of four columns do.
    val sizeOfSide = spark.table("cbj_address_stats").queryExecution.optimizedPlan.stats.sizeInBytes
    val threshold = 20000L
    assert(sizeOfSide > threshold, s"the address table must be too large to broadcast: $sizeOfSide")
    val df = run(withStats, "spark.sql.autoBroadcastJoinThreshold" -> threshold.toString)
    val plan = df.queryExecution.optimizedPlan
    assert(collapsed(plan).size == 1 && !repartitioned(plan) && broadcastHinted(plan), plan.treeString)
    assert(collapsed(plan).head.getTagValue(LocalPreAggregate.Tag).isEmpty)
    val off = withConf(
      "spark.sql.autoBroadcastJoinThreshold" -> threshold.toString,
      VectorConf.CountBelowJoinEnabled -> "false"
    ) { val d = spark.sql(withStats); d.collect(); d }
    // The sales side is no longer exchanged for the join (the collapsed side's own exchange is small).
    assert(exchanges(df) <= exchanges(off), s"${finalPlan(df).treeString}\n----\n${finalPlan(off).treeString}")
  }
}
