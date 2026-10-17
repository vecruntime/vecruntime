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
package io.vecruntime.spark.iceberg

import java.nio.file.Files

import io.vecruntime.spark.{VectorConf, VectorPlugin}
import io.vecruntime.spark.test.{IcebergTest, VectorQuerySuite}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * #735: our dynamic partition pruning rules on Iceberg tables -- `DppThroughAggregate` (q59, q2) and `TransitiveDpp`
 * (q72) -- prune a DSv2 scan by the columns it takes runtime filters on (an Iceberg identity partition), as they
 * prune a partitioned file table. Every result is checked against Spark with the plugin off.
 */
class IcebergDppSuite extends VectorQuerySuite {

  private lazy val warehouse = Files.createTempDirectory("vecruntime-iceberg-dpp").toString

  override protected def extraSparkConf: Map[String, String] =
    IcebergTables.catalogConf(warehouse) ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "true"
    )

  private val Db = IcebergTables.Db

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $Db")
    def fact(name: String, prefix: String, salt: Int): Unit =
      spark.sql(
        s"""CREATE OR REPLACE TABLE $Db.$name USING iceberg PARTITIONED BY (${prefix}_sold_date_sk) AS
           |SELECT id % 60 AS ${prefix}_sold_date_sk, IF(id % 41 = 0, NULL, (id + $salt) % 7) AS ${prefix}_store_sk,
           |  (id + $salt) % 50 AS ${prefix}_item_sk, (id + $salt) % 9 AS ${prefix}_quantity,
           |  CAST(((id + $salt) * 37) % 9000 AS DECIMAL(7, 2)) / 100 AS ${prefix}_sales_price
           |FROM range(0, 6000)""".stripMargin
      )
    fact("store_sales", "ss", 0)
    fact("web_sales", "ws", 3)
    fact("catalog_sales", "cs", 5)
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.inventory USING iceberg PARTITIONED BY (inv_date_sk) AS
         |SELECT id % 60 AS inv_date_sk, id % 50 AS inv_item_sk, id % 7 AS inv_quantity_on_hand
         |FROM range(0, 3000)""".stripMargin
    )
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.date_dim USING iceberg AS
         |SELECT id AS d_date_sk, CAST(id / 7 AS INT) AS d_week_seq, CAST(id / 30 AS INT) AS d_month_seq,
         |  CAST(2000 + id / 20 AS INT) AS d_year, IF(id % 7 = 0, 'Sunday', 'Other') AS d_day_name
         |FROM range(0, 60)""".stripMargin
    )
    Seq("store_sales", "web_sales", "catalog_sales", "inventory", "date_dim").foreach { t =>
      spark.table(s"$Db.$t").createOrReplaceTempView(t)
    }
  }

  private def run(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** Pruning subqueries over an Iceberg scan on a column named like `suffix`. */
  private def pruning(df: DataFrame, suffix: String): Int =
    df.queryExecution.optimizedPlan.collect { case f @ Filter(_, _: DataSourceV2ScanRelation) => f }
      .flatMap(_.condition.collect { case d: DynamicPruningSubquery => d })
      .count(_.pruningKey.references.exists(_.name.endsWith(suffix)))

  private val wss =
    """(SELECT d_week_seq, ss_store_sk, sum(ss_sales_price) sales
      |  FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
      |  GROUP BY d_week_seq, ss_store_sk) wss""".stripMargin

  test("q59's shape on Iceberg: the fact grouped by a dimension key is pruned through the aggregate", IcebergTest) {
    val sql = s"SELECT wss.* FROM $wss JOIN date_dim d ON wss.d_week_seq = d.d_week_seq WHERE d.d_month_seq = 1"
    assert(pruning(run(sql), "_sold_date_sk") === 1)
    withConf(VectorConf.DppThroughAggregateEnabled -> "false") {
      assert(pruning(run(sql), "_sold_date_sk") === 0)
    }
  }

  test("q2's shape on Iceberg: every branch of a union of facts is pruned", IcebergTest) {
    val sql =
      """SELECT w.* FROM
        |  (SELECT d_week_seq, sum(p) sales FROM
        |     (SELECT ss_sold_date_sk sold, ss_sales_price p FROM store_sales
        |      UNION ALL SELECT ws_sold_date_sk sold, ws_sales_price p FROM web_sales) u
        |     JOIN date_dim ON sold = d_date_sk
        |   GROUP BY d_week_seq) w
        |JOIN date_dim d ON w.d_week_seq = d.d_week_seq WHERE d.d_month_seq = 0 AND d.d_day_name = 'Sunday'""".stripMargin
    assert(pruning(run(sql), "_sold_date_sk") === 2)
  }

  test("q72's shape on Iceberg: inventory is pruned through d2's week from the filtered d1", IcebergTest) {
    val sql =
      """SELECT d1.d_week_seq, count(*) c, sum(inv_quantity_on_hand) q
        |FROM catalog_sales JOIN inventory ON cs_item_sk = inv_item_sk
        |JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk
        |JOIN date_dim d2 ON inv_date_sk = d2.d_date_sk
        |WHERE d1.d_week_seq = d2.d_week_seq AND inv_quantity_on_hand < cs_quantity AND d1.d_year = 2001
        |GROUP BY d1.d_week_seq""".stripMargin
    assert(pruning(run(sql), "inv_date_sk") === 1)
    withConf(VectorConf.TransitiveDppEnabled -> "false") {
      assert(pruning(run(sql), "inv_date_sk") === 0)
    }
  }

  test("a column that is not an identity partition gets no pruning subquery", IcebergTest) {
    // Grouped by the store, which is not a partition column: the rule has nothing to prune by.
    val sql =
      """SELECT a.* FROM
        |  (SELECT ss_store_sk, max(d_week_seq) mw FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
        |   GROUP BY ss_store_sk) a
        |JOIN date_dim d ON a.mw = d.d_week_seq WHERE d.d_month_seq = 1""".stripMargin
    assert(pruning(run(sql), "_sold_date_sk") === 0)
  }
}
