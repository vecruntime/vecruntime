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
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorIcebergScanExec}

/**
 * #806: our dynamic partition pruning rules (`DppThroughAggregate`, `TransitiveDpp`) prune an Iceberg scan on the
 * source column of every partition transform Iceberg's runtime filtering projects -- identity, years, months, days,
 * hours, bucket and truncate -- and across partition-spec evolution: the scan projects the pruning filter through
 * each data file's own spec. Each case runs q59's shape (pruned through an aggregate) and q72's (pruned
 * transitively through a second dimension), checks the result against Spark with the plugin off, asserts the
 * pruning subquery on the scan, and counts the files the scan reads with the rule on and off.
 */
class IcebergTransformDppSuite extends VectorQuerySuite {

  private lazy val warehouse = Files.createTempDirectory("vecruntime-iceberg-dpp-transforms").toString

  override protected def extraSparkConf: Map[String, String] =
    IcebergTables.catalogConf(warehouse) ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "true",
      // The native Iceberg scan counts the files it reads.
      VectorConf.ScanNativeIceberg -> "true",
      "spark.sql.session.timeZone" -> "UTC"
    )

  private val Db = IcebergTables.Db

  /**
   * One partitioned fact `ev_<name>` keyed by `ev_k`, a dimension `cal_<name>` with one row per key value (`c_grp`:
   * `grp` consecutive keys, `c_flt`: `flt` consecutive keys) and an unpartitioned fact `o_<name>` for q72's shape.
   * `key(i)` is the SQL of the i-th key value.
   */
  private case class Case(name: String, partitionBy: String, keys: Int, grp: Int, flt: Int, key: String => String)

  private val Base = 1704067200L // 2024-01-01 00:00:00 UTC
  private def ts(stepSeconds: Long): String => String = i => s"timestamp_seconds($Base + ($i) * $stepSeconds)"

  private val cases = Seq(
    Case("hours", "hours(ev_k)", keys = 72, grp = 6, flt = 12, ts(3600)),
    Case("days", "days(ev_k)", keys = 60, grp = 7, flt = 14, ts(86400)),
    Case("months", "months(ev_k)", keys = 48, grp = 4, flt = 8, ts(15 * 86400)),
    Case("years", "years(ev_k)", keys = 30, grp = 5, flt = 10, ts(73 * 86400)),
    Case("bucket", "bucket(8, ev_k)", keys = 60, grp = 3, flt = 3, i => s"CAST($i AS BIGINT)"),
    Case(
      "truncate",
      "truncate(2, ev_k)",
      keys = 60,
      grp = 3,
      flt = 3,
      i => s"concat('k', lpad(CAST($i AS STRING), 2, '0'))"
    )
  )

  private def factSql(c: Case, from: Int, to: Int): String =
    s"""SELECT ${c.key(s"id % ${c.keys}")} AS ev_k, id % 50 AS ev_item, id % 9 AS ev_qty,
       |  CAST((id * 37) % 9000 AS DECIMAL(7, 2)) / 100 AS ev_price
       |FROM range($from, $to)""".stripMargin

  private def createDims(c: Case): Unit = {
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.cal_${c.name} USING iceberg AS
         |SELECT ${c.key("id")} AS c_k, CAST(id / ${c.grp} AS INT) AS c_grp, CAST(id / ${c.flt} AS INT) AS c_flt
         |FROM range(0, ${c.keys})""".stripMargin
    )
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.o_${c.name} USING iceberg AS
         |SELECT ${c.key(s"(id + 5) % ${c.keys}")} AS o_k, (id + 5) % 50 AS o_item, (id + 5) % 9 AS o_qty
         |FROM range(0, 3000)""".stripMargin
    )
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $Db")
    cases.foreach { c =>
      spark.sql(
        s"CREATE OR REPLACE TABLE $Db.ev_${c.name} USING iceberg PARTITIONED BY (${c.partitionBy}) AS ${factSql(c, 0, 3000)}"
      )
      createDims(c)
    }
  }

  private def run(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** Pruning subqueries on `ev_k` over an Iceberg scan. */
  private def pruning(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case f @ Filter(_, _: DataSourceV2ScanRelation) => f }
      .flatMap(_.condition.collect { case d: DynamicPruningSubquery => d })
      .count(_.pruningKey.references.exists(_.name == "ev_k"))

  /** Files the scans of the partitioned fact read. */
  private def files(df: DataFrame): Long = {
    val scans = PlanUtils.allNodes(finalPlan(df)).collect {
      case s: VectorIcebergScanExec if s.output.exists(_.name == "ev_k") => s
    }
    assert(scans.nonEmpty, s"expected a VectorIcebergScanExec over the fact:\n${finalPlan(df).treeString}")
    scans.map(_.metrics("numFiles").value).sum
  }

  private def q59(t: String): String =
    s"""SELECT a.* FROM
       |  (SELECT c_grp, sum(ev_price) s, count(*) n FROM $Db.ev_$t JOIN $Db.cal_$t ON ev_k = c_k GROUP BY c_grp) a
       |JOIN $Db.cal_$t d ON a.c_grp = d.c_grp WHERE d.c_flt = 1""".stripMargin

  private def q72(t: String): String =
    s"""SELECT d1.c_grp, count(*) c, sum(e.ev_qty) q
       |FROM $Db.o_$t o JOIN $Db.ev_$t e ON o.o_item = e.ev_item
       |JOIN $Db.cal_$t d1 ON o.o_k = d1.c_k
       |JOIN $Db.cal_$t d2 ON e.ev_k = d2.c_k
       |WHERE d1.c_grp = d2.c_grp AND e.ev_qty < o.o_qty AND d1.c_flt = 1
       |GROUP BY d1.c_grp""".stripMargin

  /** The shape pruned with `rule` on reads fewer files of the fact than with it off, where it has no subquery. */
  private def assertPrunes(sql: String, rule: String): Unit = {
    val on = run(sql)
    assert(pruning(on) === 1, s"expected one pruning subquery on ev_k:\n${on.queryExecution.optimizedPlan}")
    withConf(rule -> "false") {
      val off = run(sql)
      assert(pruning(off) === 0)
      assert(files(on) < files(off), s"pruned ${files(on)} files, unpruned ${files(off)}")
    }
  }

  cases.foreach { c =>
    test(
      s"${c.partitionBy}: q59's shape is pruned through the aggregate by the transform's source column",
      IcebergTest
    ) {
      assertPrunes(q59(c.name), VectorConf.DppThroughAggregateEnabled)
    }
    test(s"${c.partitionBy}: q72's shape is pruned transitively by the transform's source column", IcebergTest) {
      assertPrunes(q72(c.name), VectorConf.TransitiveDppEnabled)
    }
  }

  test("a spec evolved from identity to days(ts) prunes the files written under both specs", IcebergTest) {
    val c = cases.find(_.name == "days").get.copy(name = "evolved")
    spark.sql(s"CREATE OR REPLACE TABLE $Db.ev_evolved USING iceberg PARTITIONED BY (ev_k) AS ${factSql(c, 0, 1500)}")
    spark.sql(s"ALTER TABLE $Db.ev_evolved REPLACE PARTITION FIELD ev_k WITH days(ev_k)")
    spark.sql(s"INSERT INTO $Db.ev_evolved ${factSql(c, 1500, 3000)}")
    createDims(c)
    val specs = spark.sql(s"SELECT DISTINCT spec_id FROM $Db.ev_evolved.files").count()
    assert(specs === 2, "the table should have files under two specs")
    assertPrunes(q59("evolved"), VectorConf.DppThroughAggregateEnabled)
    assertPrunes(q72("evolved"), VectorConf.TransitiveDppEnabled)
  }

  test("a column dropped from the current spec still prunes the files of the spec that had it", IcebergTest) {
    val c = cases.find(_.name == "days").get.copy(name = "dropped")
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.ev_dropped USING iceberg PARTITIONED BY (days(ev_k)) AS ${factSql(c, 0, 3000)}"
    )
    // The current spec partitions by the item only: ev_k is a partition source in the older spec alone.
    spark.sql(s"ALTER TABLE $Db.ev_dropped DROP PARTITION FIELD days(ev_k)")
    spark.sql(s"ALTER TABLE $Db.ev_dropped ADD PARTITION FIELD bucket(4, ev_item)")
    spark.sql(s"INSERT INTO $Db.ev_dropped ${factSql(c, 3000, 3600)}")
    createDims(c)
    assertPrunes(q59("dropped"), VectorConf.DppThroughAggregateEnabled)
  }

  test("a fact not partitioned by the join key gets no pruning subquery on it", IcebergTest) {
    val c = cases.find(_.name == "days").get.copy(name = "voided")
    spark.sql(s"CREATE OR REPLACE TABLE $Db.ev_voided USING iceberg PARTITIONED BY (ev_item) AS ${factSql(c, 0, 3000)}")
    createDims(c)
    assert(pruning(run(q59("voided"))) === 0)
  }
}
