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
import org.apache.spark.sql.vecruntime.{
  VectorFilterExec,
  VectorHashAggregateExec,
  VectorParquetScanExec,
  VectorProjectExec,
  VectorTakeOrderedAndProjectExec,
  VectorWindowExec
}

/**
 * #812: `time` (nanoseconds of the day) and the ANSI intervals (`interval day to second`, micros; `interval year to
 * month`, months) ride the INT64 / INT32 lanes: read natively from Parquet, then carried, compared, null-tested,
 * grouped, sorted, joined and windowed by the operators, min / max / count over them included. Everything that reads
 * them as a time or an interval -- arithmetic, casts, functions, sum and avg -- stays Spark's, with Spark's answer.
 */
class VectorTimeIntervalSuite extends VectorQuerySuite {
  private val Filter = classOf[VectorFilterExec]
  private val Project = classOf[VectorProjectExec]
  private val Agg = classOf[VectorHashAggregateExec]
  private val TopK = classOf[VectorTakeOrderedAndProjectExec]
  private val Window = classOf[VectorWindowExec]
  private val Scan = classOf[VectorParquetScanExec]

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(VectorConf.ScanNativeParquet -> "true", "spark.sql.timeType.enabled" -> "true")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      spark.range(0, 30000).selectExpr(
        "cast(id as int) as i",
        "cast(id % 23 as int) as g",
        // Whole seconds and fractions, the whole day, nulls.
        "case when id % 17 = 0 then null else " +
          "make_time(cast(id % 24 as int), cast(id % 60 as int), cast((id * 7) % 60000 as decimal(16,6)) / 1000) end as t",
        "make_time(cast(id % 3 as int), 0, 0) as t2",
        // Signed intervals, nulls.
        "case when id % 13 = 0 then null else make_dt_interval(0, cast(id % 50 as int) - 25, cast(id % 60 as int), " +
          "cast(id % 997 as decimal(18,6))) end as dt",
        "case when id % 11 = 0 then null else make_ym_interval(cast(id % 7 as int) - 3, cast(id % 12 as int)) end as ym"
      ).repartition(3).write.mode("overwrite").parquet(newTempPath("ti/t"))
      spark.read.parquet(newTempPath("ti/t")).createOrReplaceTempView("ti")
    }
  }

  test("time and intervals are read natively, filtered, compared and carried through projections") {
    checkVectorized("SELECT i, t, dt, ym FROM ti", Seq(Scan))
    checkVectorized("SELECT i, t FROM ti WHERE t > t2 OR t IS NULL", Seq(Filter))
    checkVectorized("SELECT i, dt, ym FROM ti WHERE dt <= dt AND ym IS NOT NULL AND ym < ym", Nil)
    checkVectorized(
      "SELECT i, coalesce(t, t2) AS tt, t <=> t2 AS ns, CASE WHEN g > 10 THEN dt END AS x FROM ti",
      Seq(Project)
    )
  }

  test("time and interval grouping keys, min / max / count, top-k, joins and windows") {
    checkVectorized(
      "SELECT t2, count(*) AS n, min(t) AS lo, max(t) AS hi, max(dt) AS mdt FROM ti GROUP BY t2",
      Seq(Agg)
    )
    checkVectorized("SELECT ym, count(t) AS n, min(dt) AS lo, max(i) AS mi FROM ti GROUP BY ym", Seq(Agg))
    checkVectorized("SELECT i, t, dt FROM ti ORDER BY t DESC NULLS LAST, i LIMIT 700", Seq(TopK))
    checkVectorized("SELECT i, ym FROM ti ORDER BY ym NULLS FIRST, i LIMIT 700", Seq(TopK))
    checkVectorized(
      "SELECT a.i, b.i AS j FROM ti a JOIN ti b ON a.t = b.t AND a.ym = b.ym WHERE a.i < 2000 AND b.i < 2000",
      Nil
    )
    checkVectorized(
      "SELECT i, rank() OVER (PARTITION BY g ORDER BY t, i) AS r, lag(dt, 1) OVER (PARTITION BY ym ORDER BY i) AS prev " +
        "FROM ti WHERE i < 5000",
      Seq(Window)
    )
  }

  test("arithmetic, casts, functions, sum and avg over time and intervals give Spark's answer") {
    checkVectorized(
      "SELECT i, t + make_dt_interval(0, 0, 0, 1.5) AS t3, dt + dt AS d2, ym * 2 AS y2 FROM ti WHERE i < 3000",
      Nil
    )
    checkVectorized(
      "SELECT i, cast(t AS string) AS s, cast(dt AS string) AS d, hour(t) AS h FROM ti WHERE i < 3000",
      Nil
    )
    checkVectorized("SELECT g, sum(dt) AS sd, avg(ym) AS ay, sum(ym) AS sy FROM ti GROUP BY g", Nil)
  }
}
