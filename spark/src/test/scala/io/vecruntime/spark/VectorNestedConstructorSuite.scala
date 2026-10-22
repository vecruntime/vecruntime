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
import org.apache.spark.sql.vecruntime.VectorProjectExec

/**
 * Nested constructors in a projection (#842): `array(...)` and `map(...)` over scalar lanes, and
 * `if(p, null, x)` / `if(p, x, null)` over any nested `x` (a struct, an array, a map), compile into our
 * project and answer as Spark does -- null elements, null predicates (the else branch), multi-byte
 * strings, booleans, dates, decimals. Maps whose keys may be null or repeat keep Spark's projection.
 */
@io.vecruntime.spark.test.tags.Expr
@io.vecruntime.spark.test.tags.Nested
class VectorNestedConstructorSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("ctor")
    spark.range(0, 3000).selectExpr(
      "id",
      "if(id % 13 = 0, null, cast(id % 1000 as int)) as i",
      "if(id % 17 = 0, null, concat('é', id % 300)) as s",
      "if(id % 19 = 0, null, date_add(date'1969-12-01', cast(id % 90 as int))) as day",
      "if(id % 23 = 0, null, cast(id % 10000 as decimal(9,2)) / 7) as dec",
      "if(id % 29 = 0, null, id % 3 = 0) as b"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("ctor")
  }

  test("array(...) over int, long, string, date, decimal and boolean lanes, with null elements") {
    checkVectorized(
      "SELECT id, array(i, cast(id % 6 as int), i) ai, array(id, id * 2) al, array(s, 'x', s) sa, " +
        "array(day, day) ad, array(dec) adec, array(b, not b) ab FROM ctor",
      Seq(Project)
    )
  }

  test("map(...) with one non-null key, or distinct literal keys") {
    checkVectorized(
      "SELECT id, map(concat('k', id % 4), id) m1, map('a', i, 'b', cast(id % 5 as int)) m2, " +
        "map(cast(id % 9 as int), s) m3 FROM ctor",
      Seq(Project)
    )
  }

  test("if(p, null, x) and if(p, x, null) over structs, arrays and maps; a null predicate takes the else branch") {
    checkVectorized(
      "SELECT id, if(id % 7 = 0, null, named_struct('x', cast(id % 50 as int), 'y', concat('y', id % 9))) st, " +
        "if(id % 11 = 0, null, array(cast(id % 5 as int), cast(id % 6 as int))) xs, " +
        "if(id % 13 = 0, null, map(concat('k', id % 4), id)) m, " +
        "if(i > 500, array(s), null) pa, if(b, null, named_struct('d', day)) ns FROM ctor",
      Seq(Project)
    )
  }

  test("a null map key fails as Spark's does") {
    val e = intercept[Exception](
      withPlugin(enabled = true)(spark.sql("SELECT map(if(id = 5, null, concat('k', id)), id) m FROM ctor").collect())
    )
    assert(
      Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
        .exists(t => String.valueOf(t.getMessage).contains("NULL_MAP_KEY")),
      e
    )
  }

  test("a map whose keys may repeat keeps Spark's projection") {
    // The keys never repeat here, but the compiler cannot know it.
    val df = spark.sql("SELECT map(concat('a', id % 2), id, concat('b', id % 3), id) m FROM ctor")
    df.collect()
    assert(!df.queryExecution.executedPlan.exists(_.isInstanceOf[VectorProjectExec]))
  }
}
