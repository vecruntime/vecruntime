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
import org.apache.spark.sql.vecruntime.VectorSortExec

/**
 * Array, map and struct columns through our sort (#705): payload columns copied by the column builders' nested
 * lanes and gathered in the sorted order, nulls and empty values kept; such a partition is one in-memory run.
 */
@io.vecruntime.spark.test.tags.Nested
@io.vecruntime.spark.test.tags.SortWindow
class VectorNestedSortSuite extends VectorQuerySuite {
  private val Sort = classOf[VectorSortExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.range(0, 5000).selectExpr(
      "cast(id as int) as i",
      "cast((id * 7919) % 5000 as int) as k",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else array(cast(id as int), cast(null as int), cast(id % 3 as int)) end as a",
      "case when id % 5 = 0 then null else named_struct('s', concat('v', id), 'l', array(cast(id % 4 as bigint))) end as st",
      "case when id % 6 = 0 then null else map(concat('k', id % 9), cast(id as double)) end as m"
    ).repartition(3).write.mode("overwrite").parquet(newTempPath("nested-sort"))
    spark.read.parquet(newTempPath("nested-sort")).createOrReplaceTempView("ns")
    // The same rows in one file: one partition, so a SORT BY is the total order the checks compare.
    spark.read.parquet(newTempPath("nested-sort")).repartition(1).write.mode("overwrite")
      .parquet(newTempPath("nested-sort-1"))
    spark.read.parquet(newTempPath("nested-sort-1")).createOrReplaceTempView("ns1")
  }

  private def checkSortedNested(sql: String): Unit = {
    val df = checkVectorized(sql, Seq(Sort))
    // Ordered output: compare in order, not as a set.
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect().toSeq)
    assert(df.collect().toSeq == expected, sql)
  }

  // One partition sorted within itself (ns1): a total order with no exchange (the local tests run Spark's
  // row shuffle, which a global sort's range exchange would read through a row-to-column conversion).
  test("a sort carries array, struct and map columns in the sorted order") {
    checkSortedNested("SELECT k, a, st, m FROM ns1 SORT BY k")
    checkSortedNested("SELECT k, i, a FROM ns1 SORT BY k DESC NULLS LAST, i")
  }

  test("a sort within partitions, and a sort under a filter's selection") {
    checkVectorized("SELECT k, a, st FROM ns SORT BY k", Seq(Sort))
    checkSortedNested("SELECT k, st, m FROM ns1 WHERE i % 3 = 1 SORT BY k")
  }

  test("a tiny spill budget: a partition holding nested columns stays in memory, the order unchanged") {
    withConf("spark.vecruntime.sort.spillBytes" -> "1k") {
      checkSortedNested("SELECT k, a, st FROM ns1 SORT BY k")
    }
  }

  test("array and struct sort keys order as Spark orders them, both directions and null orders") {
    // Keys with prefixes (shorter first), null elements and fields (first), empty arrays, NaN / -0.0 / 0.0,
    // strings with prefixes, empty strings and NUL bytes, negative decimals; `i` breaks ties for a total order.
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 13 = 0 then null when id % 11 = 0 then array() " +
        "else slice(array(cast(id % 5 as int) - 2, case when id % 7 = 0 then null else cast(id % 3 as int) end, " +
        "cast(id % 2 as int)), 1, cast(1 + id % 3 as int)) end as ka",
      "array(case id % 5 when 0 then cast('NaN' as double) when 1 then -0.0D when 2 then 0.0D " +
        "when 3 then -1.5D else 2.5D end) as kd",
      "case when id % 17 = 0 then null else named_struct('s', case id % 4 when 0 then '' when 1 then 'a' " +
        "when 2 then 'ab' else concat('a', char(0), 'b') end, 'n', case when id % 9 = 0 then null " +
        "else cast((id % 7) - 3 as decimal(20,2)) end) end as ks"
    ).repartition(1).write.mode("overwrite").parquet(newTempPath("nested-sort-keys"))
    spark.read.parquet(newTempPath("nested-sort-keys")).createOrReplaceTempView("nk")
    checkSortedNested("SELECT ka, i FROM nk SORT BY ka, i")
    checkSortedNested("SELECT ka, i FROM nk SORT BY ka DESC NULLS LAST, i")
    checkSortedNested("SELECT ks, i FROM nk SORT BY ks ASC NULLS LAST, i DESC")
    checkSortedNested("SELECT ks, ka, i FROM nk SORT BY ks DESC, ka, i")
    // Floats compared as strings: the harness's row equality cannot see NaN equal to NaN inside an array.
    checkSortedNested("SELECT cast(kd AS string) d, i FROM (SELECT kd, i FROM nk SORT BY kd, i)")
  }

  test("a top-N: nested keys, nested payload and nested outputs (Spark's projection over the surviving rows)") {
    val topN = Seq(classOf[org.apache.spark.sql.vecruntime.VectorTakeOrderedAndProjectExec])
    def check(sql: String): Unit = {
      val df = checkVectorized(sql, topN)
      val expected = withPlugin(enabled = false)(spark.sql(sql).collect().toSeq)
      assert(df.collect().toSeq == expected, sql)
    }
    check("SELECT ka, ks, i FROM nk ORDER BY ka DESC NULLS LAST, i LIMIT 40")
    check("SELECT i, ks FROM nk ORDER BY ks, i DESC LIMIT 40")
    // Payload of every nested kind, a map included, over a lane key; a projection building a nested output.
    check("SELECT k, a, st, m FROM ns ORDER BY k DESC LIMIT 30")
    check("SELECT k, named_struct('x', a, 'y', m) AS p FROM ns ORDER BY k LIMIT 30")
  }
}
