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
 * Nested lambdas and lambdas over nested elements (#705), each checked against Spark: a lambda
 * whose variable is itself an array or a struct, a lambda inside a lambda body that reads the outer
 * lambda's variable, and an outer array read from inside a lambda -- with nulls at every level (a
 * null outer array, a null inner array, a null leaf).
 */
class VectorNestedLambdaSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("nlam")
    spark.range(0, 2000).selectExpr(
      "cast(id as int) as i",
      // array<array<int>>: a null outer array, an empty one, a null inner array, null leaves.
      "case when id % 9 = 0 then null when id % 13 = 0 then array() " +
        "else array(array(cast(id % 5 as int), null), " +
        "case when id % 4 = 0 then null else array(cast(id % 7 as int), cast(id % 3 as int)) end, " +
        "array()) end as aa",
      // array<struct<k: int, v: array<string>>>
      "case when id % 11 = 0 then null else array(" +
        "named_struct('k', cast(id % 6 as int), 'v', array(concat('s', id % 4), null)), " +
        "case when id % 5 = 0 then null else named_struct('k', cast(-1 as int), 'v', cast(null as array<string>)) end" +
        ") end as as1",
      "case when id % 8 = 0 then null else array(cast(id % 5 as int), cast(id % 3 as int), null) end as ai",
      "array(cast(id % 4 as int), cast(id % 6 as int)) as aj",
      "cast(id % 5 as int) as t",
      // map<int, array<int>>: a null map, a null value array.
      "case when id % 10 = 0 then null else map(cast(id % 3 as int), " +
        "case when id % 6 = 0 then null else array(cast(id as int), null) end) end as mv"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("nlam")
  }

  test("lambdas whose variable is an array: size, element access, array functions, null tests") {
    checkVectorized(
      "SELECT i, transform(aa, x -> size(x)) sz, transform(aa, x -> get(x, 0)) x0, " +
        "exists(aa, x -> array_contains(x, 3)) c3, forall(aa, x -> x IS NOT NULL) nn, " +
        "filter(aa, x -> size(x) > 1) f, transform(aa, x -> array_max(x)) mx FROM nlam",
      Seq(Project)
    )
  }

  test("a lambda inside a lambda over the inner array, reading the outer variable and columns") {
    checkVectorized(
      "SELECT i, exists(aa, x -> exists(x, y -> y = t)) e, forall(aa, x -> forall(x, y -> y < 7)) f, " +
        "transform(aa, x -> aggregate(x, 0, (acc, y) -> acc + coalesce(y, 0))) s, " +
        "transform(ai, x -> exists(aj, y -> y = x)) ij FROM nlam",
      Seq(Project)
    )
  }

  test("lambdas over an array of structs: fields, and a lambda over a field's array") {
    checkVectorized(
      "SELECT i, transform(as1, s -> s.k) k, transform(as1, s -> size(s.v)) n, " +
        "exists(as1, s -> exists(s.v, v -> v = 's1')) e, filter(as1, s -> s.k > 2) f FROM nlam",
      Seq(Project)
    )
  }

  test("aggregate over arrays of arrays, and a nested lambda as a filter predicate") {
    checkVectorized(
      "SELECT i, aggregate(aa, 0, (acc, x) -> acc + size(x)) total FROM nlam",
      Seq(Project)
    )
    checkVectorized("SELECT i FROM nlam WHERE exists(aa, x -> exists(x, y -> y = t))", Seq(Filter))
  }

  test("lambdas with a nested result: arrays of arrays, of structs and maps of arrays (#705)") {
    checkVectorized(
      "SELECT i, transform(aa, x -> x) same, transform(aa, x -> filter(x, y -> y IS NOT NULL)) nn, " +
        "transform(aa, x -> transform(x, y -> y * t)) scaled, transform(aa, x -> slice(x, 1, 1)) head, " +
        "transform(ai, x -> transform(aj, y -> y + x)) cross FROM nlam",
      Seq(Project)
    )
    checkVectorized(
      "SELECT i, transform(as1, s -> s.v) vs, transform(as1, s -> s) ss, " +
        "transform(as1, s -> filter(s.v, v -> v IS NOT NULL)) fv FROM nlam",
      Seq(Project)
    )
    checkVectorized(
      "SELECT i, transform_values(mv, (k, v) -> transform(v, x -> x + k)) tv, " +
        "zip_with(aa, aa, (x, y) -> x) zw FROM nlam",
      Seq(Project)
    )
  }

  test("a nested result where no element is live (null and empty outer arrays) is Spark's") {
    checkVectorized(
      "SELECT i, transform(aa, x -> transform(x, y -> y)) r FROM nlam WHERE i % 9 = 0 OR i % 13 = 0",
      Seq(Filter, Project)
    )
  }
}
