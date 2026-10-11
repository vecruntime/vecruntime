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
import org.apache.spark.sql.vecruntime.{VectorFilterExec, VectorProjectExec}

/**
 * Array results (#705, nested results): `map_keys`, `map_values`, `transform` and `filter` compile into
 * our project and hand Spark the arrays it computes -- null and empty arrays, null elements, lambdas
 * with an index and with outer columns, string elements, struct fields holding arrays -- whole or
 * under a filter's selection.
 */
class VectorNestedResultsSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("results")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else array(cast(id % 10 as int), null, cast(id % 4 as int), cast(-id as int)) end as ai",
      "case when id % 5 = 0 then null else array(concat('k', id % 20), null, '', 'zz') end as sa",
      "case when id % 6 = 0 then null when id % 8 = 0 then map() " +
        "else map(concat('a', id % 50), id, 'b', cast(null as bigint)) end as m",
      "case when id % 4 = 0 then null else named_struct('y', array(cast(id % 13 as bigint), null)) end as st",
      "cast(id % 12 as int) as t"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("results")
  }

  test("map_keys and map_values") {
    checkVectorized("SELECT i, map_keys(m) k, map_values(m) v FROM results", Seq(Project))
  }

  test("transform: element, index and outer-column lambdas, string elements, a struct field") {
    checkVectorized(
      "SELECT i, transform(ai, x -> x + 1) a1, transform(ai, (x, j) -> x * j) aj, " +
        "transform(ai, x -> x + t) at, transform(sa, s -> upper(s)) su, " +
        "transform(sa, s -> length(s) > 1) sl, transform(st.y, y -> y * 2) y2 FROM results",
      Seq(Project)
    )
  }

  test("filter: predicates over the element, its index and outer columns; nulls are dropped") {
    checkVectorized(
      "SELECT i, filter(ai, x -> x > 2) f2, filter(ai, (x, j) -> j % 2 = 0) fe, " +
        "filter(ai, x -> x < t) ft, filter(sa, s -> s IS NOT NULL AND s <> '') fs, " +
        "filter(st.y, y -> y IS NULL) fn FROM results",
      Seq(Project)
    )
  }

  test("array results under a filter's selection") {
    checkVectorized(
      "SELECT i, transform(ai, x -> x * 10) a, filter(sa, s -> s = 'zz') z, map_keys(m) k " +
        "FROM results WHERE i % 9 = 4",
      Seq(Filter, Project)
    )
  }

  test("a lambda with a nested result keeps Spark's operator, with the reason") {
    checkFallback(
      "SELECT transform(ai, x -> array(x)) FROM results",
      Seq(Project),
      "not supported"
    )
  }
}
