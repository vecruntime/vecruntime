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
@io.vecruntime.spark.test.tags.Iceberg
@io.vecruntime.spark.test.tags.Join
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

  test("a fact read again with other filters gets no probe, as its Parquet copy (q14a, #735)", IcebergTest) {
    // The sales read twice, each read with its own pushed filter: two DSv2 scans that differ only in the filters
    // pushed into them. A file scan's copies are the same relation under different filters.
    val sql =
      s"""SELECT * FROM (
         |  SELECT ss_item_sk, sum(ss_quantity - sr_return_quantity) q, count(*) c, max(ss_pad) p
         |  FROM (SELECT * FROM store_sales WHERE ss_quantity > 2) s
         |  JOIN (SELECT * FROM store_returns WHERE isnotnull(nullif(sr_return_quantity, 0))) r
         |    ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
         |  GROUP BY ss_item_sk) a
         |JOIN (SELECT ss_item_sk k, count(*) n FROM store_sales WHERE ss_quantity < 5 GROUP BY ss_item_sk) b
         |  ON a.ss_item_sk = b.k""".stripMargin
    val df = run(sql)
    assert(salesBlooms(df) === 0, df.queryExecution.optimizedPlan.treeString)
  }

  test("a fact read twice (a self-join) gets no probe, so its reads stay identical", IcebergTest) {
    val sql =
      """SELECT a.ss_item_sk, count(*) c
        |FROM store_sales a JOIN store_sales b ON a.ss_ticket_number = b.ss_ticket_number
        |JOIN (SELECT * FROM store_returns WHERE isnotnull(nullif(sr_return_quantity, 0))) r
        |  ON a.ss_ticket_number = sr_ticket_number GROUP BY a.ss_item_sk""".stripMargin
    run(sql)
  }

  test("a dimension small by its files is a broadcast, so the fact gets no filter through it (q7, #735)", IcebergTest) {
    // q7's shape: the fact joined to a dimension whose Iceberg estimate (rows times default column widths) is above
    // the broadcast threshold while its files are far below it, then to a small, weakly filtered dimension. Its
    // Parquet copy broadcasts both, so the fact gets no bloom filter. On Iceberg, Spark's own runtime filter rule ran
    // before the file-size broadcast hints, took the first join for a shuffle join and probed the fact with both
    // dimensions' keys; the fact bloom filter judged the join by the same estimate.
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.q7_cdemo USING iceberg AS
         |SELECT id AS cd_demo_sk, IF(id % 2 = 0, 'M', 'F') AS cd_gender, IF(id % 3 = 0, 'S', 'M') AS cd_marital,
         |  'College' AS cd_education, 'Good' AS cd_credit
         |FROM range(0, 20000)""".stripMargin
    )
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.q7_promo USING iceberg AS
         |SELECT id AS p_promo_sk, IF(id % 200 = 0, 'Y', 'N') AS p_channel FROM range(0, 1800)""".stripMargin
    )
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.q7_sales USING iceberg AS
         |SELECT id % 20000 AS ss_cdemo_sk, id % 1800 AS ss_promo_sk, id % 13 AS ss_quantity,
         |  lpad(CAST(id * 2654435761 AS STRING), 40, '0') AS ss_pad
         |FROM range(0, 60000)""".stripMargin
    )
    def scanOf(t: String) = spark.table(s"$Db.$t").queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    val cdemo = scanOf("q7_cdemo")
    val cdemoFiles = org.apache.spark.sql.vecruntime.V2BroadcastBySize.plannedBytes(cdemo).get
    val salesFiles = org.apache.spark.sql.vecruntime.V2BroadcastBySize.plannedBytes(scanOf("q7_sales")).get
    // A threshold above the dimension's files, below its estimate and the fact's files.
    val threshold = cdemoFiles * 4
    assert(
      cdemo.stats.sizeInBytes > threshold && salesFiles > threshold,
      s"cdemo files $cdemoFiles, estimate ${cdemo.stats.sizeInBytes}; sales files $salesFiles"
    )
    val sql =
      s"""SELECT ss_quantity, count(*) c, max(ss_pad) p
         |FROM $Db.q7_sales JOIN $Db.q7_cdemo ON ss_cdemo_sk = cd_demo_sk
         |JOIN $Db.q7_promo ON ss_promo_sk = p_promo_sk
         |WHERE cd_gender = 'M' AND cd_marital = 'S' AND p_channel = 'N'
         |GROUP BY ss_quantity""".stripMargin
    withConf(
      "spark.sql.autoBroadcastJoinThreshold" -> threshold.toString,
      "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0"
    ) {
      val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
      val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
      assertRowsEqual(expected, df.collect(), 1e-9, sql)
      assert(expected.nonEmpty, sql)
      assert(salesBlooms(df) === 0, df.queryExecution.optimizedPlan.treeString)
    }
  }
}
