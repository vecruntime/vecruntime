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
 * `aggregate`, `zip_with`, `map_filter` and `transform_values` (#705) compile into our project and
 * answer as Spark does: folds over arrays of different lengths (empty, null, null elements), with and
 * without a finish, accumulators of int, long, double and string, merges that read outer columns;
 * zips of unequal lengths (the shorter side null); map filters and value transforms over keys and
 * values, null values and empty maps -- whole and under a filter's selection, and a merge's ANSI error
 * raised only for the rows Spark folds.
 */
@io.vecruntime.spark.test.tags.Expr
@io.vecruntime.spark.test.tags.Nested
class VectorHigherOrderMoreSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("hof-more")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else slice(array(cast(id % 10 as int), null, cast(id % 4 as int), cast(-id % 9 as int), 3), 1, cast(id % 5 as int) + 1) end as ai",
      "case when id % 5 = 0 then null else array(concat('k', id % 20), null, '', 'zz') end as sa",
      "array(id % 13, id * 2, id % 3) as al",
      "case when id % 6 = 0 then null when id % 8 = 0 then map() " +
        "else map(concat('a', id % 50), id, 'b', cast(null as bigint), 'c', id % 7) end as m",
      "cast(id % 12 as int) as t"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("hm")
  }

  private def off[T](f: => T): T = withConf("spark.sql.ansi.enabled" -> "false")(f)

  test("aggregate: int, long, double and string accumulators, with and without finish, outer columns") {
    off {
      checkVectorized(
        "SELECT i, aggregate(ai, 0, (a, x) -> a + coalesce(x, 0)) s, " +
          "aggregate(ai, cast(0 as bigint), (a, x) -> a + x) sn, " +
          "aggregate(al, cast(1 as bigint), (a, x) -> a * 2 + x, a -> a % 1000) f, " +
          "aggregate(ai, 0.5d, (a, x) -> a + cast(coalesce(x, t) as double) / 3) d, " +
          "aggregate(sa, '', (a, x) -> concat(a, coalesce(x, '_'))) c, " +
          "aggregate(ai, t, (a, x) -> greatest(a, coalesce(x, a))) g FROM hm",
        Seq(Project)
      )
    }
  }

  test("zip_with over arrays of unequal lengths; the shorter side reads null") {
    checkVectorized(
      "SELECT i, zip_with(ai, al, (x, y) -> coalesce(x, 0) + y) z, " +
        "zip_with(sa, ai, (x, y) -> concat(coalesce(x, '?'), cast(y as string))) zs, " +
        "zip_with(al, al, (x, y) -> x * y + t) zz FROM hm",
      Seq(Project)
    )
  }

  test("map_filter and transform_values over keys, values and outer columns") {
    checkVectorized(
      "SELECT i, map_filter(m, (k, v) -> v > 3) f, map_filter(m, (k, v) -> k <> 'b' AND v IS NOT NULL) fk, " +
        "transform_values(m, (k, v) -> v + t) tv, transform_values(m, (k, v) -> concat(k, '=')) tk FROM hm",
      Seq(Project)
    )
  }

  test("under a filter's selection, and a merge's ANSI error only for the rows Spark folds") {
    off {
      checkVectorized(
        "SELECT i, aggregate(ai, 0, (a, x) -> a + coalesce(x, 1)) s, map_filter(m, (k, v) -> v % 2 = 0) f " +
          "FROM hm WHERE i % 9 = 4",
        Seq(Filter, Project)
      )
    }
    withConf("spark.sql.ansi.enabled" -> "true") {
      val sql = "SELECT aggregate(ai, 100, (a, x) -> a div x) FROM hm WHERE ai IS NOT NULL"
      def condition(enabled: Boolean): String = withPlugin(enabled) {
        val e = intercept[Throwable](spark.sql(sql).collect())
        Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collectFirst { case t: SparkThrowable =>
          t.getCondition
        }.getOrElse(e.toString)
      }
      assert(condition(enabled = true) == condition(enabled = false))
      // Rows whose elements are all non-zero and non-null: the division never sees a zero.
      checkVectorized(
        "SELECT i, aggregate(al, 1000000L, (a, x) -> a div (x + 1)) q FROM hm WHERE i % 13 <> 12",
        Seq(Filter, Project)
      )
    }
  }
}
