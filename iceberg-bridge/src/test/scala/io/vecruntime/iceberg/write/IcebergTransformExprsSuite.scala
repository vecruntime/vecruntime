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
package io.vecruntime.iceberg.write

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Iceberg's Spark functions (`system.bucket`, `system.truncate`, `system.years` ...) compiled by the engine
 * (IcebergTransformExprs) give the values Iceberg's own `invoke` methods give, row for row: the same query
 * over a Parquet table with the engine on (a columnar projection) and off. These are the keys of the
 * exchange and the sort under a hash- or range-distributed Iceberg write.
 */
class IcebergTransformExprsSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val dir: Path = Files.createTempDirectory("icefn")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("iceberg-functions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", dir.resolve("ice").toString)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.vecruntime.explainFallback.enabled", "true")
      .getOrCreate()
    // Negative values, dates before 1970, timestamps before the epoch, nulls, multi-byte strings, and a
    // dictionary-encoded string column (few distinct values).
    spark.sql(
      """SELECT id,
        |  if(id % 23 = 0, null, cast(id * 7919 - 400000 as int)) as i,
        |  if(id % 29 = 0, null, id * 1000003 - 50000000000) as l,
        |  if(id % 31 = 0, null, date_add(date'1969-06-01', cast(id % 2000 - 1000 as int))) as day,
        |  if(id % 37 = 0, null, timestamp_micros(id * 3600000123 - 40000000000000)) as ts,
        |  if(id % 41 = 0, null, cast(timestamp_micros(id * 3600000123 - 40000000000000) as timestamp_ntz)) as ntz,
        |  if(id % 43 = 0, null, concat('é', cast(id * 31 as string), 'ü')) as s,
        |  if(id % 47 = 0, null, concat('k', cast(id % 5 as string))) as ds,
        |  if(id % 53 = 0, null, cast(id * 13 - 3000 as decimal(9, 2)) / 3) as dec
        |FROM range(5000)""".stripMargin
    ).write.parquet(dir.resolve("src").toString)
    spark.read.parquet(dir.resolve("src").toString).createOrReplaceTempView("src")
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def nodes(p: SparkPlan): Seq[SparkPlan] = {
    val nested = p.getClass.getSimpleName match {
      case "AdaptiveSparkPlanExec" =>
        p.getClass.getMethods.find(m => m.getName == "executedPlan" && m.getParameterCount == 0)
          .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
      case n if n.endsWith("QueryStageExec") =>
        p.getClass.getMethods.find(m => m.getName == "plan" && m.getParameterCount == 0)
          .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
      case _ => Seq.empty
    }
    p +: (p.children ++ nested).flatMap(nodes)
  }

  private def check(exprs: String*): Unit = {
    val q = s"SELECT id, ${exprs.mkString(", ")} FROM src ORDER BY id"
    spark.conf.set("spark.vecruntime.enabled", "false")
    val expected = spark.sql(q).collect().toSeq
    spark.conf.set("spark.vecruntime.enabled", "true")
    val df = spark.sql(q)
    val got = df.collect().toSeq
    assert(
      nodes(df.queryExecution.executedPlan).exists(_.getClass.getSimpleName == "VectorProjectExec"),
      s"not a columnar projection: ${df.queryExecution.executedPlan}"
    )
    assert(got.length == expected.length)
    got.zip(expected).foreach { case (g, e) => assert(g == e, s"$q: ${g.get(0)}") }
  }

  test("years, months, days over dates, timestamps and timestamp_ntz") {
    check(
      "ice.system.years(day)",
      "ice.system.months(day)",
      "ice.system.days(day)",
      "ice.system.years(ts)",
      "ice.system.months(ts)",
      "ice.system.days(ts)",
      "ice.system.years(ntz)",
      "ice.system.months(ntz)",
      "ice.system.days(ntz)"
    )
  }

  test("hours over timestamps and timestamp_ntz") {
    check("ice.system.hours(ts)", "ice.system.hours(ntz)")
  }

  test("bucket over every source type") {
    check(
      "ice.system.bucket(16, i)",
      "ice.system.bucket(7, l)",
      "ice.system.bucket(5, day)",
      "ice.system.bucket(1000, ts)",
      "ice.system.bucket(3, ntz)",
      "ice.system.bucket(16, s)",
      "ice.system.bucket(16, ds)",
      "ice.system.bucket(9, dec)"
    )
  }

  test("truncate over ints, longs, decimals and strings") {
    check(
      "ice.system.truncate(10, i)",
      "ice.system.truncate(1000, l)",
      "ice.system.truncate(50, dec)",
      "ice.system.truncate(3, s)",
      "ice.system.truncate(1, ds)"
    )
  }
}
