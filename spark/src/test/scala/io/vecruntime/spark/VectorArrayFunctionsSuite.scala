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
 * Element scans over nested lanes (#705, nested step 4): `array_contains`, `array_position`,
 * `array_min`, `array_max` and `map_contains_key` compile into our project and filter and answer as
 * Spark does -- null arrays, null elements (contains is then null, not false), empty arrays, null
 * values to look for, strings ordered by bytes, struct fields holding arrays.
 */
@io.vecruntime.spark.test.tags.Expr
@io.vecruntime.spark.test.tags.Nested
class VectorArrayFunctionsSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("arrays")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "when id % 3 = 0 then array(cast(id % 10 as int), cast(-id % 4 as int)) " +
        "else array(cast(id % 10 as int), null, cast(id % 4 as int), cast(id % 10 as int)) end as ai",
      "case when id % 5 = 0 then null when id % 9 = 0 then array(cast(null as string)) " +
        "else array(concat('k', id % 20), 'é', '', concat('k', id % 3), 'zz') end as sa",
      "array(id % 13, id * 2, cast(null as bigint)) as al",
      "array(id % 2 = 0, id % 3 = 0) as ab",
      "array(date_add(date'2020-01-01', cast(id % 40 as int)), null) as adt",
      "case when id % 6 = 0 then null else map(concat('a', id % 50), id, 'b', cast(null as bigint)) end as m",
      "map(cast(id % 9 as int), concat('v', id)) as mi",
      "case when id % 4 = 0 then null else named_struct('y', array(cast(id % 13 as bigint), null)) end as st",
      "cast(id % 12 as int) as v",
      "case when id % 17 = 0 then null else concat('k', id % 20) end as sv",
      "array(cast(id as double)) as adbl"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("arrays")
  }

  test("array_contains and array_position over int, long, string, bool and date elements") {
    checkVectorized(
      "SELECT i, array_contains(ai, 3) c3, array_contains(ai, v) cv, array_position(ai, v) pv, " +
        "array_position(ai, 3) p3, array_contains(sa, sv) cs, array_position(sa, 'zz') pz, " +
        "array_contains(sa, '') ce, array_contains(al, 4L) cl, array_position(al, cast(i * 2 as bigint)) pl, " +
        "array_contains(ab, true) cb, array_contains(adt, date'2020-01-05') cd, " +
        "array_contains(ai, cast(null as int)) cn FROM arrays",
      Seq(Project)
    )
  }

  test("array_min and array_max skip nulls; empty and all-null arrays are null") {
    checkVectorized(
      "SELECT i, array_min(ai) mn, array_max(ai) mx, array_min(sa) smn, array_max(sa) smx, " +
        "array_min(al) lmn, array_max(ab) bmx, array_max(adt) dmx, array_min(st.y) ymn FROM arrays",
      Seq(Project)
    )
  }

  test("map_contains_key, and scans as filter predicates") {
    checkVectorized(
      "SELECT i, map_contains_key(m, 'b') kb, map_contains_key(m, concat('a', i % 50)) ka, " +
        "map_contains_key(mi, v) kv FROM arrays",
      Seq(Project)
    )
    checkVectorized(
      "SELECT i, array_max(ai) mx FROM arrays WHERE array_contains(ai, 3) AND array_position(sa, 'zz') = 5",
      Seq(Filter, Project)
    )
  }

  test("double elements compare too") {
    checkVectorized(
      "SELECT array_contains(adbl, 1.0d) c FROM arrays",
      Seq(Project)
    ) // doubles compare since #705 (VectorFractionalElementsSuite)
  }
}
