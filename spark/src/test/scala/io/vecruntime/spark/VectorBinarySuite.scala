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
  VectorTakeOrderedAndProjectExec,
  VectorWindowExec
}

/**
 * BINARY (#705): the column rides the UTF8 lane (offsets and bytes). Equality, ordering (unsigned
 * bytes in both Spark and the kernels), grouping, joins and windows stay columnar; functions that read
 * characters (length, substring, lpad, hex, ...) count bytes over binary in Spark, so the compiler
 * refuses them and they stay Spark's.
 */
class VectorBinarySuite extends VectorQuerySuite {
  private val Filter = classOf[VectorFilterExec]
  private val Project = classOf[VectorProjectExec]
  private val Agg = classOf[VectorHashAggregateExec]
  private val TopK = classOf[VectorTakeOrderedAndProjectExec]
  private val Window = classOf[VectorWindowExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Bytes above 0x7f (unsigned order differs from signed), an empty value, nulls, and values that
    // are not valid UTF-8, so a character-wise reading would differ from Spark's.
    val df = spark.range(0, 20000).selectExpr(
      "cast(id as int) as i",
      "cast(id % 23 as int) as g",
      "case when id % 19 = 0 then null when id % 31 = 0 then x'' " +
        "else unhex(lpad(hex((id * 2654435761) % 4093), 6, 'F')) end as b",
      "unhex(hex(id % 300)) as c",
      "concat('k', id % 40) as k"
    )
    df.repartition(3).write.mode("overwrite").option("parquet.enable.dictionary", "true")
      .parquet(newTempPath("bin/dict"))
    df.repartition(3).write.mode("overwrite").option("parquet.enable.dictionary", "false")
      .parquet(newTempPath("bin/plain"))
    spark.read.parquet(newTempPath("bin/dict")).createOrReplaceTempView("bin_dict")
    spark.read.parquet(newTempPath("bin/plain")).createOrReplaceTempView("bin_plain")
  }

  Seq("bin_dict", "bin_plain").foreach { t =>
    test(s"$t: binary columns filter, compare and pass through projections") {
      checkVectorized(s"SELECT i, b FROM $t WHERE b IS NULL OR b > c", Seq(Filter))
      checkVectorized(s"SELECT i, b, c FROM $t WHERE b = c OR b < c", Seq(Filter))
      checkVectorized(s"SELECT i, b AS bb, coalesce(b, c) AS bc, b <=> c AS ns FROM $t", Seq(Project))
      checkVectorized(s"SELECT i, CASE WHEN g > 10 THEN b ELSE c END AS x FROM $t", Seq(Project))
    }

    test(s"$t: binary grouping keys, joins, top-k and windows") {
      checkVectorized(s"SELECT b, count(*) AS n, max(i) AS mi FROM $t GROUP BY b", Seq(Agg))
      checkVectorized(s"SELECT c, k, count(*) AS n FROM $t GROUP BY c, k", Seq(Agg))
      checkVectorized(s"SELECT i, b FROM $t ORDER BY b DESC NULLS LAST, i LIMIT 900", Seq(TopK))
      checkVectorized(s"SELECT i, b FROM $t ORDER BY b NULLS FIRST, i LIMIT 900", Seq(TopK))
      checkVectorized(
        s"SELECT i, g, rank() OVER (PARTITION BY c ORDER BY b, i) AS r, " +
          s"lag(b, 1) OVER (PARTITION BY g ORDER BY i) AS prev FROM $t WHERE i < 4000",
        Seq(Window)
      )
    }

    test(s"$t: functions that read characters over binary give Spark's (byte) answer") {
      val q =
        s"""SELECT i, length(b) AS len, octet_length(b) AS ol, substr(b, 2, 2) AS s, hex(b) AS h,
           |  lpad(c, 4, x'00') AS lp, base64(b) AS b64, cast(c AS string) AS cs, b = x'FFF001' AS eqlit,
           |  concat(b, c) AS cc, startswith(b, x'FF') AS sw
           |FROM $t WHERE i < 3000""".stripMargin
      val expected = withPlugin(enabled = false)(spark.sql(q).collect())
      val actual = withPlugin(enabled = true)(spark.sql(q).collect())
      assertRowsEqual(expected, actual, 1e-9, q)
    }
  }

  test("binary join keys") {
    withConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      checkVectorized(
        "SELECT a.i, a.c, b.n FROM bin_dict a JOIN (SELECT c, count(*) AS n FROM bin_plain GROUP BY c) b " +
          "ON a.c = b.c WHERE a.i < 3000",
        Seq(Agg)
      )
    }
    checkVectorized(
      "SELECT a.i, a.b, b.k FROM bin_dict a JOIN (SELECT DISTINCT b, k FROM bin_plain WHERE i < 600) b ON a.b = b.b",
      Seq(Agg)
    )
  }
}
