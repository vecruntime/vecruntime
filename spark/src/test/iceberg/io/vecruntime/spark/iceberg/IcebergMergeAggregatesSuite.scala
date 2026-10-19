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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * #735: `MergeFilteredAggregates` over an Iceberg table. Its scalar subqueries reach the rule with their scans
 * pushed down (each with its own filters and columns); they are merged as a file table's are, and the merged
 * aggregate's scan pushed down again. TPC-DS q9 read `store_sales` fifteen times on Iceberg without this.
 */
@io.vecruntime.spark.test.tags.Iceberg
@io.vecruntime.spark.test.tags.Agg
class IcebergMergeAggregatesSuite extends VectorQuerySuite {

  private lazy val warehouse = Files.createTempDirectory("vecruntime-iceberg-merge").toString

  override protected def extraSparkConf: Map[String, String] =
    IcebergTables.catalogConf(warehouse) ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "true"
    )

  private val T = s"${IcebergTables.Db}.t_q9"
  private val P = s"${IcebergTables.Db}.t_q9_parted"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ${IcebergTables.Db}")
    val rows = "SELECT CAST(id AS INT) AS q, CAST(id % 7 AS INT) AS k, CAST(id AS DOUBLE) / 3 AS p, id % 2 = 0 AS b " +
      "FROM range(0, 20000, 1, 4)"
    spark.sql(s"CREATE OR REPLACE TABLE $T USING iceberg AS $rows")
    spark.sql(s"CREATE OR REPLACE TABLE $P USING iceberg PARTITIONED BY (b) AS $rows")
  }

  /** Distinct scans of the optimized plan, subqueries included. */
  private def scans(df: DataFrame): Seq[Scan] = {
    def all(p: LogicalPlan): Seq[Scan] =
      p.collect { case s: DataSourceV2ScanRelation => s.scan } ++ p.flatMap(_.subqueries.flatMap(all))
    all(df.queryExecution.optimizedPlan).distinct
  }

  private def q9(t: String): String =
    s"""SELECT CASE WHEN (SELECT count(*) FROM $t WHERE q BETWEEN 1 AND 2000) > 10
       |    THEN (SELECT avg(p) FROM $t WHERE q BETWEEN 1 AND 2000) ELSE (SELECT avg(k) FROM $t WHERE q BETWEEN 1 AND 2000) END a,
       |  CASE WHEN (SELECT count(*) FROM $t WHERE q BETWEEN 2001 AND 4000) > 10
       |    THEN (SELECT avg(p) FROM $t WHERE q BETWEEN 2001 AND 4000) ELSE 0 END b,
       |  (SELECT max(k) FROM $t WHERE q > 19000) c""".stripMargin

  test("q9's scalar subqueries over an Iceberg table read it once", IcebergTest) {
    val df = checkVectorized(q9(T), Nil)
    assert(scans(df).size === 1, df.queryExecution.optimizedPlan.treeString)
    // The merged scan carries the ORed ranges, so Iceberg still prunes by them; without the rule each filter and
    // column set is its own scan.
    withConf(VectorConf.MergeFilteredAggregatesEnabled -> "false") {
      val off = spark.sql(q9(T))
      assert(scans(off).size > 3, off.queryExecution.optimizedPlan.treeString)
      assertRowsEqual(off.collect(), df.collect(), 1e-9, q9(T))
    }
  }

  test("a subquery whose filter Iceberg absorbed (an identity partition) is not merged away", IcebergTest) {
    // `b` is an identity partition: Iceberg answers `b` and `NOT b` from the partitions and Spark drops them from
    // the filter above, so these scans cannot go back to the relation; the rows stay Spark's either way.
    val sql =
      s"""SELECT (SELECT count(*) FROM $P WHERE b AND q < 5000) x, (SELECT sum(q) FROM $P WHERE NOT b AND q < 5000) y,
         |  (SELECT avg(p) FROM $P WHERE q < 100) z""".stripMargin
    checkVectorized(sql, Nil)
    checkVectorized(q9(P), Nil)
  }
}
