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
import org.apache.spark.sql.vecruntime.{VectorBroadcastHashJoinExec, VectorFilterExec, VectorSortExec}

/**
 * Collated strings (#705). A `STRING COLLATE UTF8_LCASE` / `UNICODE_CI` column rides the UTF8 lane to be
 * carried -- filtered, sorted and joined as payload -- and every key, comparison, hash, order and function
 * over it stays with Spark's collation-aware implementation. The data holds values that are equal under
 * the collations and differ in bytes ('a' / 'A', 'É' / 'é', trailing case), so a byte comparison anywhere
 * changes the answer, which every query here compares against Spark's.
 */
class VectorCollatedStringSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as k",
      "collate(case when id % 13 = 0 then null else element_at(array('a', 'A', 'b', 'B', 'ab', 'AB', 'aB', " +
        "'é', 'É', 'x'), cast(id % 10 as int) + 1) end, 'UTF8_LCASE') as s",
      "collate(element_at(array('Straße', 'STRASSE', 'café', 'CAFÉ', 'z', 'Z'), cast(id % 6 as int) + 1), " +
        "'UNICODE_CI') as u",
      "element_at(array('a', 'A', 'b'), cast(id % 3 as int) + 1) as p",
      "array(collate(element_at(array('q', 'Q'), cast(id % 2 as int) + 1), 'UTF8_LCASE')) as arr",
      "named_struct('c', collate(element_at(array('m', 'M'), cast(id % 2 as int) + 1), 'UTF8_LCASE'), " +
        "'n', cast(id % 5 as int)) as st"
    ).repartition(3).write.mode("overwrite").parquet(newTempPath("collated/t"))
    spark.read.parquet(newTempPath("collated/t")).createOrReplaceTempView("ct")
    spark.read.parquet(newTempPath("collated/t")).repartition(1).write.mode("overwrite")
      .parquet(newTempPath("collated/t1"))
    spark.read.parquet(newTempPath("collated/t1")).createOrReplaceTempView("ct1")
    spark.range(0, 400).selectExpr(
      "cast(id * 7 as int) as d",
      "collate(element_at(array('A', 'b', 'AB', 'É'), cast(id % 4 as int) + 1), 'UTF8_LCASE') as s2"
    ).write.mode("overwrite").parquet(newTempPath("collated/dim"))
    spark.read.parquet(newTempPath("collated/dim")).createOrReplaceTempView("cd")
  }

  test("the schema keeps its collations through the parquet round trip") {
    val types = spark.table("ct").schema.map(f => f.name -> f.dataType.sql).toMap
    assert(types("s").contains("UTF8_LCASE") && types("u").contains("UNICODE_CI"), types.toString)
  }

  test("carried: a filter on another column, a null test, a sort and joins move collated columns as bytes") {
    checkVectorized("SELECT k, s, u FROM ct WHERE k % 3 = 1", Seq(classOf[VectorFilterExec]))
    checkVectorized("SELECT k, s FROM ct WHERE s IS NOT NULL AND k < 2000", Seq(classOf[VectorFilterExec]))
    val sorted = "SELECT k, s, u, arr, st FROM ct1 SORT BY k"
    val df = checkVectorized(sorted, Seq(classOf[VectorSortExec]))
    assert(df.collect().toSeq == withPlugin(enabled = false)(spark.sql(sorted).collect().toSeq))
    checkVectorized(
      "SELECT /*+ BROADCAST(cd) */ k, s, s2 FROM ct JOIN cd ON k = d",
      Seq(classOf[VectorBroadcastHashJoinExec])
    )
    // The local tests shuffle through Spark's row exchange, whose rows Spark's RowToColumnarExec cannot convert
    // when they hold a collated string: these joins stay Spark's here (our columnar shuffle carries them).
    checkVectorized("SELECT /*+ SHUFFLE_HASH(cd) */ k, u, s2 FROM ct LEFT JOIN cd ON k = d", Nil)
    withConf(VectorConf.SortMergeJoinMode -> "merge") {
      checkVectorized("SELECT /*+ MERGE(cd) */ k, s, s2 FROM ct JOIN cd ON k = d", Nil)
    }
    checkVectorized("SELECT k, s FROM ct UNION ALL SELECT d, s2 FROM cd", Nil)
    checkVectorized("SELECT k, s, u FROM ct LIMIT 25", Nil)
  }

  test("keys and comparisons follow the collation: grouping, distinct, joins, sorts, windows, filters") {
    Seq(
      "SELECT lower(s), count(*), sum(k) FROM ct GROUP BY s",
      "SELECT lower(u), count(*) FROM ct GROUP BY u",
      "SELECT lower(s) FROM (SELECT DISTINCT s FROM ct)",
      "SELECT count(DISTINCT s), count(DISTINCT u) FROM ct",
      "SELECT lower(min(s)), lower(max(s)), lower(min(u)), lower(max(u)) FROM ct",
      "SELECT k % 7, lower(min(s)), lower(max(u)) FROM ct GROUP BY k % 7",
      "SELECT lower(arr[0]), count(*) FROM ct GROUP BY arr",
      "SELECT lower(st.c), st.n, count(*) FROM ct GROUP BY st",
      "SELECT /*+ BROADCAST(cd) */ k, s, d FROM ct JOIN cd ON s = s2",
      "SELECT /*+ SHUFFLE_HASH(cd) */ k, d FROM ct JOIN cd ON s = s2",
      "SELECT k FROM ct LEFT ANTI JOIN cd ON s = s2",
      "SELECT k, s FROM ct WHERE s = 'A'",
      "SELECT k, s FROM ct WHERE s IN ('AB', 'É')",
      "SELECT k, u FROM ct WHERE u = 'strasse'",
      "SELECT k, s FROM ct WHERE s < 'b'",
      "SELECT k, upper(s), lower(u), length(s) FROM ct WHERE k < 50",
      "SELECT k, s FROM ct WHERE s LIKE 'a%'",
      "SELECT k, s, rank() OVER (PARTITION BY s ORDER BY k) FROM ct",
      "SELECT k, lower(min(s) OVER (PARTITION BY k % 4 ORDER BY k)) FROM ct",
      "SELECT s, k FROM (SELECT * FROM ct DISTRIBUTE BY s)"
    ).foreach(q => checkVectorized(q, Nil))
    withConf(VectorConf.SortMergeJoinMode -> "merge") {
      checkVectorized("SELECT /*+ MERGE(cd) */ k, d FROM ct JOIN cd ON s = s2", Nil)
    }
    // An ordering by the collation, compared in order: ties broken by k.
    val ordered = "SELECT s, k FROM ct ORDER BY s, k LIMIT 200"
    val df = checkVectorized(ordered, Nil)
    assert(df.collect().toSeq == withPlugin(enabled = false)(spark.sql(ordered).collect().toSeq))
    val sortedWithin = "SELECT u, k FROM ct1 SORT BY u DESC, k"
    val df2 = checkVectorized(sortedWithin, Nil)
    assert(df2.collect().toSeq == withPlugin(enabled = false)(spark.sql(sortedWithin).collect().toSeq))
  }

  test("collation keys (step 2): grouping, sorts, top-N and broadcast joins on collated keys are ours") {
    val Agg = classOf[org.apache.spark.sql.vecruntime.VectorHashAggregateExec]
    // Grouping by the collation key, each group output as one of its values: compared through lower().
    checkVectorized("SELECT lower(s), c FROM (SELECT s, count(*) c FROM ct GROUP BY s)", Seq(Agg))
    checkVectorized("SELECT lower(u), c, sk FROM (SELECT u, count(*) c, sum(k) sk FROM ct GROUP BY u)", Seq(Agg))
    checkVectorized("SELECT count(*) FROM (SELECT DISTINCT s, u FROM ct)", Seq(Agg))
    // A join on a collated equality: Spark keys it on collationkey(), a binary key ours compares.
    checkVectorized(
      "SELECT /*+ BROADCAST(cd) */ k, s, d FROM ct JOIN cd ON s = s2",
      Seq(classOf[VectorBroadcastHashJoinExec])
    )
    checkVectorized(
      "SELECT /*+ BROADCAST(cd) */ k FROM ct LEFT ANTI JOIN cd ON s = s2",
      Seq(classOf[VectorBroadcastHashJoinExec])
    )
    // Sorts in the collation's order, ties broken by k: compared in order.
    for (
      q <- Seq(
        "SELECT s, k FROM ct1 SORT BY s, k",
        "SELECT u, s, k FROM ct1 SORT BY u DESC NULLS LAST, s, k"
      )
    ) {
      val df = checkVectorized(q, Seq(classOf[VectorSortExec]))
      assert(df.collect().toSeq == withPlugin(enabled = false)(spark.sql(q).collect().toSeq), q)
    }
    val topN = "SELECT s, u, k FROM ct ORDER BY u, s DESC, k LIMIT 40"
    val df = checkVectorized(topN, Seq(classOf[org.apache.spark.sql.vecruntime.VectorTakeOrderedAndProjectExec]))
    assert(df.collect().toSeq == withPlugin(enabled = false)(spark.sql(topN).collect().toSeq))
  }
}
