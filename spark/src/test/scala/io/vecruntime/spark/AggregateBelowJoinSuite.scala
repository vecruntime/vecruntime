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
    spark.read.parquet(newTempPath("abj/sales")).createOrReplaceTempView("abj_sales")
    (0 until 200).map(i => (i, s"customer-$i", s"first-${i % 17}", s"last-${i % 23}"))
      .toDF("c_sk", "c_id", "c_first", "c_last")
      .write.mode("overwrite").parquet(newTempPath("abj/customer"))
    spark.read.parquet(newTempPath("abj/customer")).createOrReplaceTempView("abj_customer")
    (0 until 30).map(i => (i, 2000 + i % 3)).toDF("d_sk", "d_year")
      .write.mode("overwrite").parquet(newTempPath("abj/date"))
    spark.read.parquet(newTempPath("abj/date")).createOrReplaceTempView("abj_date")
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

  test("the switch turns the rewrite off") {
    val df = withConf(VectorConf.AggregateBelowJoinEnabled -> "false")(run(q4Shape))
    assert(!preAggregated(df.queryExecution.optimizedPlan), df.queryExecution.optimizedPlan.treeString)
  }
}
