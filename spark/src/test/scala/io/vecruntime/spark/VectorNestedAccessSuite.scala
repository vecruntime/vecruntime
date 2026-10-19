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
 * Nested results of element access and `slice` (#705): `arr[i]`, `element_at` and `map[k]` whose
 * element is itself an array, a map or a struct, and `slice` over any element lane, gathered as nested
 * lanes and handed to Spark -- null containers and elements, out-of-range indexes, negative `slice`
 * starts, lengths past the end, column bounds, under a filter's selection -- and `slice`'s start-0 and
 * negative-length errors are Spark's.
 */
@io.vecruntime.spark.test.tags.Nested
class VectorNestedAccessSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("nested-access")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else array(cast(id as int), null, cast(id % 4 as int), cast(-id as int), 5) end as ai",
      "case when id % 13 = 0 then null else array(array(id, null), array(), null, array(id * 2)) end as aa",
      "case when id % 5 = 0 then null else array(named_struct('x', id, 'y', array(concat('s', id % 9), null)), " +
        "null, named_struct('x', cast(null as bigint), 'y', cast(null as array<string>))) end as sa",
      "case when id % 6 = 0 then null else map('k', array(cast(id as int), null), concat('a', id % 20), " +
        "cast(array() as array<int>)) end as m",
      "map(cast(id % 4 as int), map('p', id, 'q', cast(null as bigint))) as mm",
      "cast(id % 7 as int) - 3 as s",
      "cast(id % 5 as int) as l",
      "concat('a', id % 20) as key"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("na")
  }

  private def off[T](f: => T): T = withConf("spark.sql.ansi.enabled" -> "false")(f)

  test("element access with array, struct and map elements") {
    off {
      checkVectorized(
        "SELECT i, aa[0] a0, aa[2] a2, aa[9] a9, element_at(aa, -1) al, sa[0] s0, sa[2] s2, " +
          "m['k'] mk, m[key] mkey, m['zz'] mz, element_at(mm, 1) e1, mm[i % 4] mi FROM na",
        Seq(Project)
      )
    }
  }

  test("slice: literal and column bounds, negative starts, lengths past the end, nested elements") {
    checkVectorized(
      "SELECT i, slice(ai, 2, 2) s22, slice(ai, -2, 5) sn, slice(ai, 9, 1) s9, slice(ai, -9, 2) sm, " +
        "slice(ai, 1, 0) s0, slice(aa, 1, 2) aas, slice(sa, -1, 1) sas, " +
        "slice(ai, case when s = 0 then 1 else s end, l) sc FROM na",
      Seq(Project)
    )
  }

  test("nested results under a filter's selection") {
    off {
      checkVectorized(
        "SELECT i, aa[0] a0, slice(ai, 1, 3) s, m['k'] mk FROM na WHERE i % 9 = 4",
        Seq(Filter, Project)
      )
    }
  }

  test("slice's start 0 and negative length raise Spark's errors; rows a filter drops raise nothing") {
    def condition(sql: String, enabled: Boolean): String = withPlugin(enabled) {
      val e = intercept[Throwable](spark.sql(sql).collect())
      Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collectFirst { case t: SparkThrowable =>
        t.getCondition
      }.getOrElse(e.toString)
    }
    Seq(
      "SELECT slice(ai, s, 1) FROM na WHERE ai IS NOT NULL",
      "SELECT slice(ai, 1, s) FROM na WHERE ai IS NOT NULL"
    ).foreach { sql =>
      val ours = condition(sql, enabled = true)
      assert(ours == condition(sql, enabled = false), sql)
      assert(ours.startsWith("INVALID_PARAMETER_VALUE"), s"$sql: $ours")
    }
    checkVectorized("SELECT i, slice(ai, s, 2) x FROM na WHERE s > 0", Seq(Filter, Project))
  }
}
