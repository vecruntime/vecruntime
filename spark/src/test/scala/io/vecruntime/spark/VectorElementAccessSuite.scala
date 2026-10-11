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
 * Element access over nested lanes (#705, nested step 3): `arr[i]`, `element_at(arr, i)`, `map[k]`
 * and `element_at(map, k)` compile into our project and filter and answer as Spark does -- null
 * containers, null elements, null and out-of-range indexes, negative `element_at` indexes, missing
 * keys, struct fields holding arrays -- and under ANSI raise Spark's own errors for active rows only.
 */
class VectorElementAccessSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("access")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else array(cast(id as int), null, cast(-id as int), cast(id % 5 as int)) end as ai",
      "case when id % 5 = 0 then null else array(concat('k', id), null, '', 'δ😀') end as sa",
      "array(id, id * 2) as al",
      "array(cast(id / 7 as decimal(10,2)), null) as ad",
      "array(date_add(date'2020-01-01', cast(id % 400 as int)), null) as adt",
      "array(id % 2 = 0, null) as ab",
      "case when id % 6 = 0 then null when id % 8 = 0 then map() " +
        "else map(concat('a', id % 50), cast(id as double) / 3, 'b', null, 'zz7', -1.0) end as m",
      "map(cast(id % 9 as int), concat('v', id), 100, 'hundred') as mi",
      "case when id % 4 = 0 then null else named_struct('x', id, 'y', array(cast(id % 13 as bigint), null)) end as st",
      "cast(id % 6 as int) - 1 as k",
      "concat('a', id % 50) as key",
      "array(array(cast(id as int)), null) as aa",
      "map(cast(id % 3 as double), cast(id as int)) as md"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("access")
  }

  private def off[T](f: => T): T = withConf("spark.sql.ansi.enabled" -> "false")(f)

  test("arr[i] over every leaf lane, literal and column indexes, out of range is null") {
    off {
      checkVectorized(
        "SELECT i, ai[0] a0, ai[1] a1, ai[3] a3, ai[7] a7, ai[-1] an, ai[k] ak, sa[0] s0, sa[3] s3, " +
          "al[1] l1, ad[0] d0, adt[0] t0, ab[0] b0, ab[1] b1, ai[cast(null as int)] nn FROM access",
        Seq(Project)
      )
    }
  }

  test("element_at over arrays: one-based, negative from the end, out of range is null") {
    off {
      checkVectorized(
        "SELECT i, element_at(ai, 1) e1, element_at(ai, -1) em1, element_at(ai, 4) e4, element_at(ai, 5) e5, " +
          "element_at(ai, -5) em5, element_at(sa, -2) s, element_at(al, k + 2) ek FROM access",
        Seq(Project)
      )
    }
  }

  test("map lookups: string and int keys, literal and column keys, missing keys and null values") {
    Seq("true", "false").foreach { ansi =>
      withConf("spark.sql.ansi.enabled" -> ansi) {
        checkVectorized(
          "SELECT i, m['b'] mb, m['zz7'] m7, m['zz'] mz, m[key] mk, element_at(m, key) ek, " +
            "mi[100] h, mi[i % 9] v, element_at(mi, 3) e3 FROM access",
          Seq(Project)
        )
      }
    }
  }

  test("through a struct field, under a filter, and as a filter predicate") {
    off {
      checkVectorized("SELECT i, st.y[0] y0, element_at(st.y, -1) yl FROM access", Seq(Project))
      checkVectorized(
        "SELECT i, ai[0] + 1 a, upper(sa[0]) s FROM access WHERE i % 5 = 1 AND ai[3] = 1",
        Seq(Filter, Project)
      )
      checkVectorized("SELECT i FROM access WHERE m[key] > 3.0", Seq(Filter))
    }
  }

  test("under ANSI an in-range access compiles, and out of range raises Spark's error") {
    withConf("spark.sql.ansi.enabled" -> "true") {
      checkVectorized(
        "SELECT i, ai[0] a0, element_at(ai, -1) l FROM access WHERE size(ai) > 1",
        Seq(Project)
      )
      def condition(sql: String, enabled: Boolean): String = withPlugin(enabled) {
        val t = intercept[Throwable](spark.sql(sql).collect())
        Iterator.iterate(t)(_.getCause).takeWhile(_ != null).collectFirst { case s: SparkThrowable =>
          s.getCondition
        }.getOrElse(t.toString)
      }
      Seq(
        "SELECT ai[3] FROM access WHERE ai IS NOT NULL",
        "SELECT element_at(ai, 9) FROM access",
        "SELECT element_at(ai, 0) FROM access WHERE ai IS NOT NULL"
      ).foreach { sql =>
        val ours = condition(sql, enabled = true)
        assert(ours == condition(sql, enabled = false), sql)
        assert(ours.startsWith("INVALID_"), s"$sql: $ours")
      }
      // A row the filter drops raises nothing: only rows of size 4 are read at index 3.
      checkVectorized("SELECT i, ai[3] a3 FROM access WHERE size(ai) = 4", Seq(Filter, Project))
    }
  }

  test("a nested result and a map with an unsupported key keep Spark's operator, with the reason") {
    // An element that is itself an array is a nested result (VectorNestedAccessSuite); a wide decimal one is not.
    checkVectorized("SELECT i, aa[0] a FROM access", Seq(Project))
    checkFallback("SELECT i, md[1.0d] v FROM access", Seq(Project), "key not supported")
  }
}
