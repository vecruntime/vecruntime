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
}
