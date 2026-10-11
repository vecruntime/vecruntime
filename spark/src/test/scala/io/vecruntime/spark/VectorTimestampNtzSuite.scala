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
import org.apache.spark.sql.vecruntime.{VectorFilterExec, VectorHashAggregateExec, VectorProjectExec, VectorWindowExec}

/**
 * TIMESTAMP_NTZ (#705): the column rides the INT64 lane of micros, like TIMESTAMP, and operators that
 * only compare, hash, sort or carry it stay columnar. Expressions that read the session time zone
 * (casts to and from strings and dates, hour/minute/second, date_trunc, ...) still accept only
 * TIMESTAMP, so over NTZ they fall back to Spark and must give Spark's answer under any session zone.
 */
class VectorTimestampNtzSuite extends VectorQuerySuite {
  private val Filter = classOf[VectorFilterExec]
  private val Project = classOf[VectorProjectExec]
  private val Agg = classOf[VectorHashAggregateExec]
  private val Window = classOf[VectorWindowExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Micros from 1969 to 2034 (pre-epoch values included), a few duplicates per value, and nulls.
    val df = spark.range(0, 20000).selectExpr(
      "cast(id as int) as i",
      "cast(id % 37 as int) as g",
      "case when id % 17 = 0 then null else " +
        "cast(timestamp_micros((id % 4000 - 300) * 517000000003L) as timestamp_ntz) end as n",
      "cast(timestamp_micros((id % 500) * 86400000000L + 3600000000L) as timestamp_ntz) as d",
      "concat('k', id % 40) as k"
    )
    df.repartition(3).write.mode("overwrite").option(
      "parquet.enable.dictionary",
      "true"
    ).parquet(newTempPath("ntz/dict"))
    df.repartition(3).write.mode("overwrite").option(
      "parquet.enable.dictionary",
      "false"
    ).parquet(newTempPath("ntz/plain"))
    spark.read.parquet(newTempPath("ntz/dict")).createOrReplaceTempView("ntz_dict")
    spark.read.parquet(newTempPath("ntz/plain")).createOrReplaceTempView("ntz_plain")
  }

  Seq("ntz_dict", "ntz_plain").foreach { t =>
    test(s"$t: filters and projections over timestamp_ntz columns and literals") {
      checkVectorized(s"SELECT i, n FROM $t WHERE n > TIMESTAMP_NTZ '2001-02-03 04:05:06.789'", Seq(Filter))
      checkVectorized(
        s"SELECT i, n, d FROM $t WHERE n IS NULL OR n BETWEEN TIMESTAMP_NTZ '1969-12-31 00:00:00' AND d",
        Seq(Filter)
      )
      checkVectorized(
        s"SELECT i FROM $t WHERE d IN (TIMESTAMP_NTZ '1970-01-02 01:00:00', TIMESTAMP_NTZ '1971-01-01 01:00:00')",
        Seq(Filter)
      )
      checkVectorized(s"SELECT i, n AS nn, n = d AS eq, coalesce(n, d) AS c FROM $t", Seq(Project))
    }

    test(s"$t: timestamp_ntz grouping keys, min / max and count distinct") {
      checkVectorized(s"SELECT d, count(*) AS c, min(n) AS mn, max(n) AS mx FROM $t GROUP BY d", Seq(Agg))
      checkVectorized(s"SELECT g, min(n) AS mn, max(d) AS mx, count(DISTINCT n) AS cd FROM $t GROUP BY g", Seq(Agg))
      checkVectorized(s"SELECT n, k, count(*) AS c FROM $t GROUP BY n, k", Seq(Agg))
    }

    test(s"$t: timestamp_ntz sort keys and window order") {
      val TopK = classOf[org.apache.spark.sql.vecruntime.VectorTakeOrderedAndProjectExec]
      checkVectorized(s"SELECT i, n FROM $t ORDER BY n NULLS LAST, i LIMIT 700", Seq(TopK))
      checkVectorized(s"SELECT i, n FROM $t ORDER BY n DESC NULLS FIRST, i LIMIT 700", Seq(TopK))
      checkVectorized(
        s"SELECT i, g, rank() OVER (PARTITION BY g ORDER BY n, i) AS r, " +
          s"lag(n, 1) OVER (PARTITION BY g ORDER BY i) AS prev FROM $t WHERE i < 4000",
        Seq(Window)
      )
      checkVectorized(
        s"SELECT i, d, max(n) OVER (PARTITION BY d ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m " +
          s"FROM $t WHERE i < 4000",
        Seq(Window)
      )
    }
  }

  test("timestamp_ntz join keys through our exchange and joins") {
    withConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      checkVectorized(
        "SELECT a.i, a.d, b.c FROM ntz_dict a JOIN (SELECT d, count(*) AS c FROM ntz_plain GROUP BY d) b " +
          "ON a.d = b.d WHERE a.i < 3000",
        Seq(Agg)
      )
    }
    checkVectorized(
      "SELECT a.i, a.n, b.k FROM ntz_dict a JOIN (SELECT DISTINCT n, k FROM ntz_plain WHERE i < 500) b ON a.n = b.n",
      Seq(Agg)
    )
  }

  test("zone-dependent expressions over timestamp_ntz give Spark's answer under any session zone") {
    Seq("UTC", "America/Los_Angeles", "Asia/Kolkata").foreach { zone =>
      withConf("spark.sql.session.timeZone" -> zone) {
        val q =
          """SELECT i, cast(n AS string) AS s, cast(n AS date) AS dt, hour(n) AS h, minute(n) AS mi,
            |  cast(n AS timestamp) AS tz, cast(cast(n AS timestamp) AS timestamp_ntz) AS back,
            |  date_trunc('HOUR', n) AS tr, cast(d AS date) AS dd
            |FROM ntz_dict WHERE i < 2000""".stripMargin
        val expected = withPlugin(enabled = false)(spark.sql(q).collect())
        val actual = withPlugin(enabled = true)(spark.sql(q).collect())
        assertRowsEqual(expected, actual, 1e-9, s"zone $zone: $q")
      }
    }
  }
}
