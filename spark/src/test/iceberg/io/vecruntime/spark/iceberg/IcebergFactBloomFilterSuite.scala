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
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * #735: `FactBloomFilter` on Iceberg tables -- the probe goes on the DSv2 scan of the larger fact as it goes on a
 * file scan (TPC-DS q93's shape), and the creation side's rows come from the scan's own statistics. Results are
 * checked against Spark with the plugin off.
 */
class IcebergFactBloomFilterSuite extends VectorQuerySuite {

  private lazy val warehouse = Files.createTempDirectory("vecruntime-iceberg-bloom").toString

  override protected def extraSparkConf: Map[String, String] =
    IcebergTables.catalogConf(warehouse) ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "true",
      VectorConf.FactBloomFilterEnabled -> "true"
    )

  private val Db = IcebergTables.Db

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $Db")
    // q93-like, as FactBloomFilterSuite: sales with many tickets, returns for one ticket in twenty, and a wide
    // incompressible column so the sales are well over ten times the returns.
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.store_sales USING iceberg AS
         |SELECT CAST(id / 4 AS BIGINT) AS ss_ticket_number, id % 97 AS ss_item_sk, id % 13 AS ss_quantity,
         |  lpad(CAST(id * 2654435761 AS STRING), 40, '0') AS ss_pad
         |FROM range(0, 40000)""".stripMargin
    )
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.store_returns USING iceberg AS
         |SELECT id * 5 AS sr_ticket_number, id % 97 AS sr_item_sk, id % 3 AS sr_return_quantity
         |FROM range(0, 2000)""".stripMargin
    )
    Seq("store_sales", "store_returns").foreach(t => spark.table(s"$Db.$t").createOrReplaceTempView(t))
  }

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

  /** Bloom filters directly above an Iceberg scan of the sales. */
  private def salesBlooms(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case f @ Filter(_, _: DataSourceV2ScanRelation) => f }
      .flatMap(_.condition.collect { case b: BloomFilterMightContain => b })
      .count(_.valueExpression.references.exists(_.name.startsWith("ss_")))

  private val q93 =
    """SELECT ss_item_sk, sum(ss_quantity - sr_return_quantity) q, count(*) c, max(ss_pad) p
      |FROM store_sales JOIN (SELECT * FROM store_returns WHERE isnotnull(nullif(sr_return_quantity, 0))) r
      |  ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
      |GROUP BY ss_item_sk""".stripMargin

  test("q93's shape on Iceberg: the larger fact's scan gets a bloom filter, results unchanged", IcebergTest) {
    val df = run(q93)
    assert(salesBlooms(df) >= 1, df.queryExecution.optimizedPlan.treeString)
    val off = withConf(VectorConf.FactBloomFilterEnabled -> "false")(run(q93))
    assert(salesBlooms(off) === 0, off.queryExecution.optimizedPlan.treeString)
  }

  test("a fact read twice (a self-join) gets no probe, so its reads stay identical", IcebergTest) {
    val sql =
      """SELECT a.ss_item_sk, count(*) c
        |FROM store_sales a JOIN store_sales b ON a.ss_ticket_number = b.ss_ticket_number
        |JOIN (SELECT * FROM store_returns WHERE isnotnull(nullif(sr_return_quantity, 0))) r
        |  ON a.ss_ticket_number = sr_ticket_number GROUP BY a.ss_item_sk""".stripMargin
    run(sql)
  }
}
