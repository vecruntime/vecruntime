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
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain

/** A bloom filter from a smaller fact onto a larger one it joins (#641), against Spark with the plugin off. */
class FactBloomFilterSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // q93-like: sales with many tickets, returns for one ticket in twenty (about 20x smaller).
    // A wide, incompressible column makes the sales file well over 10x the returns file, as at 1 TB.
    (0 until 40000).map(i => (i.toLong / 4, (i % 97).toLong, (i % 13).toLong, f"${i * 2654435761L}%040d"))
      .toDF("ss_ticket_number", "ss_item_sk", "ss_quantity", "ss_pad")
      .write.mode("overwrite").parquet(newTempPath("fbloom/store_sales"))
    spark.read.parquet(newTempPath("fbloom/store_sales")).createOrReplaceTempView("store_sales")
    (0 until 2000).map(i => (i.toLong * 5, (i % 97).toLong, (i % 3).toLong))
      .toDF("sr_ticket_number", "sr_item_sk", "sr_return_quantity")
      .write.mode("overwrite").parquet(newTempPath("fbloom/store_returns"))
    spark.read.parquet(newTempPath("fbloom/store_returns")).createOrReplaceTempView("store_returns")
  }

  /** Shuffle joins only, and no minimum scan size, so the small test tables qualify. */
  private def shuffleOnly[T](f: => T): T = withConf(
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0"
  )(f)

  private def run(sql: String): DataFrame = shuffleOnly {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** Bloom filters on the sales side's keys in the optimized plan. */
  private def salesBlooms(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f }
      .flatMap(_.condition.collect { case b: BloomFilterMightContain => b })
      .count(_.valueExpression.references.exists(_.name.startsWith("ss_")))

  private val q93 =
    """SELECT ss_item_sk, sum(ss_quantity - sr_return_quantity) q, count(*) c, max(ss_pad) p
      |FROM store_sales JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
      |GROUP BY ss_item_sk""".stripMargin

  test("q93's shape: the larger fact gets bloom filters from the smaller one, results unchanged") {
    val df = run(q93)
    assert(salesBlooms(df) >= 1, df.queryExecution.optimizedPlan.treeString)
    // Spark's own runtime filter adds none here: the creation side has no selective filter.
    val off = withConf(VectorConf.FactBloomFilterEnabled -> "false")(run(q93))
    assert(salesBlooms(off) === 0, off.queryExecution.optimizedPlan.treeString)
  }

  test("the filter's Final aggregate over a row exchange stays Spark's, not ours over RowToColumnar (#647)") {
    import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, Final}
    import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
    object Plans extends org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
    // One partial filter per creation-side map task; Spark's RowToColumnarExec batches them by row count,
    // which overflows a 2 GB vector at 1 TB. Many small files give the creation side many map tasks here.
    val df = withConf("spark.sql.files.maxPartitionBytes" -> "4096", "spark.sql.files.openCostInBytes" -> "0")(run(q93))
    val all: Seq[SparkPlan] = Plans.collectWithSubqueries(df.queryExecution.executedPlan) { case p => p }
    def isBloomFinal(p: SparkPlan): Boolean = {
      val exprs = p match {
        case a: org.apache.spark.sql.execution.aggregate.BaseAggregateExec => a.aggregateExpressions
        case v: org.apache.spark.sql.vecruntime.VectorHashAggregateExec => v.aggregateExpressions
        case _ => Nil
      }
      exprs.exists(e => e.mode == Final && e.aggregateFunction.isInstanceOf[BloomFilterAggregate])
    }
    val finals = all.filter(isBloomFinal)
    assert(finals.nonEmpty, "no bloom filter aggregate found:\n" + all.headOption.map(_.treeString).getOrElse(""))
    val overR2C = finals.filter(_.children.exists(_.isInstanceOf[RowToColumnarExec]))
    assert(overR2C.isEmpty, "a bloom aggregate reads RowToColumnarExec:\n" + overR2C.map(_.treeString).mkString("\n"))
  }

  test("sides of similar size get no filter") {
    val sql =
      "SELECT count(*), max(a.ss_pad) FROM store_sales a JOIN store_sales b ON a.ss_ticket_number = b.ss_ticket_number"
    assert(salesBlooms(run(sql)) === 0)
  }

  test("a broadcastable creation side gets no filter") {
    val expected = withPlugin(enabled = false)(spark.sql(q93).collect())
    val df = withConf("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0") {
      withPlugin(enabled = true) { val d = spark.sql(q93); d.collect(); d }
    }
    assertRowsEqual(expected, df.collect(), 1e-9, q93)
    assert(salesBlooms(df) === 0)
  }

  test("the preserved side of an outer join gets no filter") {
    val sql =
      """SELECT count(*), count(sr_ticket_number) FROM store_sales LEFT JOIN store_returns
        |ON ss_ticket_number = sr_ticket_number""".stripMargin
    assert(salesBlooms(run(sql)) === 0)
  }

  test("the switch turns the rewrite off") {
    withConf(VectorConf.FactBloomFilterEnabled -> "false")(assert(salesBlooms(run(q93)) === 0))
  }
}
