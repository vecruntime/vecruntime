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
  VectorProjectExec,
  VectorSortExec,
  VectorWindowExec
}

/**
 * FLOAT / REAL (#712): float columns ride the FLOAT64 lane, widened exactly, and Spark reads the
 * declared type back through `VectorFloatColumnVector`. Every answer must be Spark's to the last bit
 * (tolerance 0): filters, float arithmetic rounded to single precision after every operation (values
 * where unrounded double arithmetic would differ), casts in and out under legacy and ANSI, grouping
 * keys (NaN and -0.0 normalised), min / max / sum / avg, sorts and top-k, joins and windows -- over the
 * edge values NaN, +-0.0, +-Infinity, subnormals and Float.MAX_VALUE, in dictionary and plain Parquet.
 */
class VectorFloatSuite extends VectorQuerySuite {
  private val Filter = classOf[VectorFilterExec]
  private val Project = classOf[VectorProjectExec]
  private val Agg = classOf[VectorHashAggregateExec]
  private val Sort = classOf[VectorSortExec]
  private val Window = classOf[VectorWindowExec]
  private val TopK = classOf[org.apache.spark.sql.vecruntime.VectorTakeOrderedAndProjectExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val df = spark.range(0, 20000).selectExpr(
      "cast(id as int) as i",
      "cast(id as bigint) as l",
      "cast((id % 1000) / 7.0 as double) as d",
      "cast(id % 300 - 150 as smallint) as s",
      "case when id % 101 = 1 then cast('NaN' as float) " +
        "when id % 101 = 2 then cast(0.0 as float) when id % 101 = 3 then cast(-0.0 as float) " +
        "when id % 101 = 4 then cast('Infinity' as float) when id % 101 = 5 then cast('-Infinity' as float) " +
        "when id % 101 = 6 then cast(1.4e-45 as float) when id % 101 = 7 then cast(3.4028235e38 as float) " +
        "when id % 101 = 8 then cast(16777217 as float) when id % 13 = 0 then null " +
        "else cast((id % 977) / 13.0 - 30.1 as float) end as f",
      "cast((id % 7) * 1.1 + 0.3 as float) as g",
      "case when id % 9 = 0 then cast('NaN' as float) when id % 9 = 1 then cast(-0.0 as float) " +
        "else cast(id % 5 as float) / 3 end as h",
      "concat('k', id % 40) as k"
    )
    df.repartition(3).write.mode("overwrite").option("parquet.enable.dictionary", "true")
      .parquet(newTempPath("float/dict"))
    df.repartition(3).write.mode("overwrite").option("parquet.enable.dictionary", "false")
      .parquet(newTempPath("float/plain"))
    spark.read.parquet(newTempPath("float/dict")).createOrReplaceTempView("tf_dict")
    spark.read.parquet(newTempPath("float/plain")).createOrReplaceTempView("tf_plain")
  }

  private def exact(sql: String, ops: Seq[Class[_ <: org.apache.spark.sql.execution.SparkPlan]]): Unit =
    checkVectorized(sql, ops, tolerance = 0.0)

  Seq("tf_dict", "tf_plain").foreach { t =>
    test(s"$t: float columns filter, project and read back as float") {
      exact(s"SELECT i, f, g FROM $t WHERE f > cast(1.5 as float) OR g < cast(1.0 as float)", Seq(Filter))
      exact(s"SELECT i, f FROM $t WHERE f IS NULL OR isnan(f) OR f = cast(0.0 as float)", Seq(Filter))
      exact(s"SELECT i, f AS ff, g = f AS eq, f < g AS lt, greatest(f, g) gt FROM $t", Seq(Project))
    }

    test(s"$t: float arithmetic is rounded to single precision after every operation") {
      exact(
        s"SELECT i, f + g a, f * g m, f - g * f c, (f + cast(1.1 as float)) * cast(3.3 as float) x, " +
          "-f n, abs(f) ab, g * g * g * g * g p, f % g r FROM " + t,
        Seq(Project)
      )
    }

    test(s"$t: casts into and out of float, legacy and ANSI") {
      Seq("false", "true").foreach { ansi =>
        withConf("spark.sql.ansi.enabled" -> ansi) {
          exact(
            s"SELECT i, cast(i AS float) fi, cast(d AS float) fd, cast(s AS float) fs, cast(f AS double) df, " +
              "cast(f AS double) * 3 dd, cast(i * 1009 AS float) big FROM " + t,
            Seq(Project)
          )
        }
      }
      checkFallback(s"SELECT cast(l * 1000003 AS float) FROM $t", Seq(Project), "#712")
    }

    test(s"$t: float grouping keys (NaN and -0.0 normalised), min, max, sum and avg") {
      exact(s"SELECT h, count(*) c, min(f) mn, max(f) mx FROM $t GROUP BY h", Seq(Agg))
      exact(s"SELECT k, min(f) mn, max(f) mx, sum(f) sm, avg(g) av, count(f) n FROM $t GROUP BY k", Seq(Agg))
      exact(s"SELECT count(DISTINCT f) d, min(g) mg, max(h) mh FROM $t", Seq(Agg))
    }

    test(s"$t: sorts, top-k and windows over float") {
      exact(s"SELECT i, f FROM $t ORDER BY f DESC NULLS LAST, i LIMIT 40", Seq(TopK))
      exact(s"SELECT i, f, g FROM $t SORT BY g, f NULLS FIRST, i", Seq(Sort)) // a local sort: no range exchange here
      exact(
        s"SELECT i, k, sum(f) OVER (PARTITION BY k ORDER BY i ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) w, " +
          "max(g) OVER (PARTITION BY k) mg FROM " + t,
        Seq(Window)
      )
    }

    test(s"$t: float join keys and payload") {
      withConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
        exact(
          s"SELECT a.i, a.f, b.g FROM $t a JOIN (SELECT DISTINCT g, h FROM $t) b ON a.g = b.g AND a.h = b.h",
          Nil
        )
      }
      exact(s"SELECT a.i, a.g, b.k FROM $t a JOIN (SELECT g, max(k) k FROM $t GROUP BY g) b ON a.g = b.g", Nil)
    }
  }
}
