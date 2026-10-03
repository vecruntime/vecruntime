/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import io.vecruntime.spark.expr.DictionaryMemoExpr
import io.vecruntime.spark.test.VectorQuerySuite

/**
 * Velox's evalWithMemo over dictionary-encoded strings: an expression of one string column is evaluated
 * once per dictionary entry and reused for every batch of the row group. Spark's rows with the memo on
 * and off, for each function it covers, over nulls, a filter, a grouping and a projection.
 */
class DictionaryMemoSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "spark.sql.parquet.columnarReaderBatchSize" -> "1000"
    )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT id, CASE WHEN id % 17 = 0 THEN NULL
          |  ELSE CONCAT(ELT(CAST(id % 5 AS INT) + 1, 'alpha', 'Beta', ' gamma ', 'déjà', 'x'), '-', CAST(id % 23 AS STRING)) END AS s
          |FROM range(0, 30000)""".stripMargin
      ).repartition(2).write.mode("overwrite").parquet(newTempPath("memo/t"))
      spark.read.parquet(newTempPath("memo/t")).createOrReplaceTempView("memo_t")
    }
  }

  private val queries = Seq(
    "SELECT id, upper(s), lower(s), substr(s, 2, 4), length(s), trim(s) FROM memo_t",
    "SELECT id, concat(s, '#'), replace(s, 'a', 'A') FROM memo_t",
    "SELECT id FROM memo_t WHERE s LIKE 'al%' OR s LIKE '%-1_' OR startswith(s, 'x') OR endswith(s, '-3')",
    "SELECT id FROM memo_t WHERE substr(s, 1, 4) IN ('alph', 'Beta') AND NOT contains(s, '-2')",
    "SELECT substr(s, 1, 3) k, count(*), max(id) FROM memo_t GROUP BY substr(s, 1, 3)",
    "SELECT upper(substr(s, 1, 2)) = 'AL', count(*) FROM memo_t GROUP BY 1",
    "SELECT id FROM memo_t WHERE s = 'Beta-4' OR s IN ('x-1', NULL)"
  )

  test("Spark's rows with the memo on and off") {
    for (on <- Seq("true", "false")) {
      withConf(VectorConf.ExprDictionaryMemo -> on) {
        queries.foreach(q => checkVectorized(q, Nil))
      }
    }
  }

  test("a row group's dictionary is evaluated once and reused by its batches") {
    withConf(VectorConf.ExprDictionaryMemo -> "true") {
      val builds = DictionaryMemoExpr.BUILDS.sum()
      val hits = DictionaryMemoExpr.HITS.sum()
      checkVectorized("SELECT id, upper(s) FROM memo_t WHERE length(s) > 3", Nil)
      val b = DictionaryMemoExpr.BUILDS.sum() - builds
      val h = DictionaryMemoExpr.HITS.sum() - hits
      assert(b > 0 && h > b, s"$b memos built, $h batches reused one")
    }
  }
}
