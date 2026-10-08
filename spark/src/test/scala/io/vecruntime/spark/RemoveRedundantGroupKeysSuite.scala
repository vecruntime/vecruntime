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

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/** #635: grouping keys a unique broadcast key determines are dropped at run time, results unchanged. */
class RemoveRedundantGroupKeysSuite extends VectorQuerySuite with AdaptiveSparkPlanHelper {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // q23a's frequent_ss_items: sales by item and date, items with a long description.
    (0 until 40000).map(i => (i % 500, i % 37, if (i % 53 == 0) None else Some(i % 500)))
      .toDF("s_item", "s_date", "s_null_item")
      .write.mode("overwrite").parquet(newTempPath("rrgk/sales"))
    spark.read.parquet(newTempPath("rrgk/sales")).createOrReplaceTempView("rrgk_sales")
    (0 until 500).map(i => (i, s"description of item ${i % 120} with a long tail ${i * 7}", i % 9))
      .toDF("i_sk", "i_desc", "i_class")
      .write.mode("overwrite").parquet(newTempPath("rrgk/item"))
    spark.read.parquet(newTempPath("rrgk/item")).createOrReplaceTempView("rrgk_item")
    // The same items with one key repeated (two rows for item 7, different descriptions).
    (spark.read.parquet(newTempPath("rrgk/item")).as[(Int, String, Int)].collect().toSeq :+
      ((7, "a second description for item 7", 3)))
      .toDF("i_sk", "i_desc", "i_class")
      .write.mode("overwrite").parquet(newTempPath("rrgk/item_dup"))
    spark.read.parquet(newTempPath("rrgk/item_dup")).createOrReplaceTempView("rrgk_item_dup")
  }

  private def run(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** The grouping-key counts of every aggregate in the final adaptive plan (inside query stages too). */
  private def aggregateKeyCounts(df: DataFrame): Seq[Int] =
    collect(df.queryExecution.executedPlan) {
      case node
          if node.getClass.getSimpleName.contains("Aggregate") &&
            node.getClass.getMethods.exists(m => m.getName == "groupingExpressions" && m.getParameterCount == 0) =>
        node.getClass.getMethod("groupingExpressions").invoke(node).asInstanceOf[Seq[_]].size
    }

  private def frequentItems(item: String) =
    s"""SELECT item_sk, d, cnt FROM (
       |  SELECT /*+ BROADCAST($item) */ substr(i_desc, 1, 30) itemdesc, i_sk item_sk, s_date d, count(*) cnt
       |  FROM rrgk_sales JOIN $item ON s_item = i_sk
       |  GROUP BY substr(i_desc, 1, 30), i_sk, s_date) WHERE cnt > 1""".stripMargin

  test("q23a's frequent_ss_items: the description key goes when the item keys are unique") {
    val df = run(frequentItems("rrgk_item"))
    val counts = aggregateKeyCounts(df)
    assert(counts.nonEmpty && counts.forall(_ <= 2), s"grouping keys per aggregate: $counts")
  }

  test("a repeated build key keeps every grouping key") {
    val df = run(frequentItems("rrgk_item_dup"))
    val counts = aggregateKeyCounts(df)
    assert(counts.exists(_ == 3), s"grouping keys per aggregate: $counts")
  }

  test("the stream key equal to the build key determines the build columns too; a dropped key is still output") {
    val df = run(
      """SELECT /*+ BROADCAST(rrgk_item) */ s_item, i_desc, i_class, s_date, count(*) n
        |FROM rrgk_sales JOIN rrgk_item ON s_item = i_sk
        |GROUP BY s_item, i_desc, i_class, s_date""".stripMargin
    )
    val counts = aggregateKeyCounts(df)
    assert(counts.nonEmpty && counts.forall(_ <= 2), s"grouping keys per aggregate: $counts")
  }

  test("an outer join's null-extended build side: results unchanged") {
    run(
      """SELECT /*+ BROADCAST(rrgk_item) */ i_sk, i_desc, s_date, count(*) n
        |FROM rrgk_sales LEFT JOIN rrgk_item ON s_null_item = i_sk
        |GROUP BY i_sk, i_desc, s_date""".stripMargin
    )
  }

  test("off by configuration") {
    withConf(VectorConf.RemoveRedundantGroupKeysEnabled -> "false") {
      val df = run(frequentItems("rrgk_item"))
      assert(aggregateKeyCounts(df).exists(_ == 3))
    }
  }
}
