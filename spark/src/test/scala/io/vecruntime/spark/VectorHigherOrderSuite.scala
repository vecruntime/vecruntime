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
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.vecruntime.{VectorFilterExec, VectorProjectExec}

/**
 * Higher-order predicates over nested lanes (#705, nested step 5): `exists` and `forall` with the
 * lambda compiled over the element lane answer as Spark does -- three-valued over null elements and
 * null lambda answers, empty and null arrays, lambdas reading outer columns, string elements, struct
 * fields holding arrays, both settings of `followThreeValuedLogicInArrayExists` -- and a lambda's ANSI
 * error is raised only for rows Spark evaluates.
 */
class VectorHigherOrderSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("hof")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "when id % 3 = 0 then array(cast(id % 10 as int), cast(-id % 4 as int)) " +
        "else array(cast(id % 10 as int), null, cast(id % 4 as int)) end as ai",
      "case when id % 5 = 0 then null else array(concat('k', id % 20), null, '', 'zz') end as sa",
      "array(id % 13, id * 2) as al",
      "case when id % 4 = 0 then null else named_struct('y', array(cast(id % 13 as bigint), null)) end as st",
      "cast(id % 12 as int) as t",
      "case when id % 9 = 0 then null else concat('k', id % 20) end as sv"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("hof")
  }

  test("exists and forall: literal and outer-column lambdas, null elements, empty and null arrays") {
    Seq("true", "false").foreach { follow =>
      withConf("spark.sql.legacy.followThreeValuedLogicInArrayExists" -> follow) {
        checkVectorized(
          "SELECT i, exists(ai, x -> x > 5) e5, exists(ai, x -> x = t) et, forall(ai, x -> x < 9) f9, " +
            "forall(ai, x -> x >= 0 OR x IS NULL) fn, exists(ai, x -> x IS NULL) en, " +
            "exists(sa, s -> s = sv) es, forall(sa, s -> length(s) < 4) fs, " +
            "exists(al, l -> l % 3 = cast(t as bigint)) el, forall(st.y, y -> y < 10) fy FROM hof",
          Seq(Project)
        )
      }
    }
  }

  test("as filter predicates, and under a filter") {
    checkVectorized("SELECT i FROM hof WHERE exists(ai, x -> x = t)", Seq(Filter))
    checkVectorized(
      "SELECT i, forall(ai, x -> x + t < 20) f FROM hof WHERE i % 5 = 2",
      Seq(Filter, Project)
    )
  }

  test("a lambda's ANSI error is Spark's, and rows a filter drops raise nothing") {
    withConf("spark.sql.ansi.enabled" -> "true") {
      val sql = "SELECT exists(ai, x -> 10 / x > 1) FROM hof WHERE ai IS NOT NULL"
      def condition(enabled: Boolean): String = withPlugin(enabled) {
        val e = intercept[Throwable](spark.sql(sql).collect())
        Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collectFirst { case s: SparkThrowable =>
          s.getCondition
        }.getOrElse(e.toString)
      }
      assert(condition(enabled = true) == condition(enabled = false))
      // Rows whose elements are all non-zero: the division never sees a zero.
      checkVectorized(
        "SELECT i, exists(ai, x -> 10 div x > 1) d FROM hof WHERE i % 10 <> 0 AND i % 4 <> 0 AND i % 3 <> 0",
        Seq(Filter, Project)
      )
    }
  }

  test("a nested lambda reading the outer lambda's variable and an outer array runs vectorized") {
    checkVectorized("SELECT i, exists(ai, x -> exists(al, l -> l = x)) e FROM hof", Seq(Project))
  }

  test("a lambda with a nested result runs vectorized (#705)") {
    checkVectorized("SELECT transform(al, l -> transform(ai, x -> x + l)) FROM hof", Seq(Project))
  }
}
