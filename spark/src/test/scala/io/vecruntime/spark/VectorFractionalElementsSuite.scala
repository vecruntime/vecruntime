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
 * Double and decimal elements in the element comparisons (#705): `array_contains`, `array_position`,
 * `array_min` / `array_max` and map lookups answer with Spark's ordering -- -0.0 equals 0.0, NaN
 * equals NaN and is the greatest -- and decimals of both widths compare on their unscaled values,
 * null elements and null arrays included.
 */
class VectorFractionalElementsSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("fractional")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() else array(" +
        "case when id % 5 = 0 then cast('NaN' as double) else cast(id % 9 as double) / 4 end, " +
        "case when id % 3 = 0 then -0.0d else 0.0d end, cast(null as double), " +
        "case when id % 13 = 0 then cast('Infinity' as double) else -cast(id % 6 as double) end) end as ad",
      "array(cast(cast(id % 50 as decimal(10,2)) / 4 as decimal(10,2)), cast(null as decimal(10,2)), cast(-1.25 as decimal(10,2))) as ds",
      "case when id % 4 = 0 then null else array(cast(cast(id as decimal(30,4)) * 1000000000000 as decimal(30,4)), " +
        "cast(-7.5 as decimal(30,4)), cast(null as decimal(30,4))) end as dw",
      "map(case when id % 2 = 0 then -0.0d else cast('NaN' as double) end, concat('v', id), 2.5d, 'two') as m",
      "cast(id % 9 as double) / 4 as x",
      "cast(cast(id as decimal(30,4)) * 1000000000000 as decimal(30,4)) as w",
      "map(cast(id % 3 as int), cast(cast(id as decimal(30,4)) * 1000000000000 as decimal(30,4)), " +
        "cast(id % 3 + 1 as int), cast(null as decimal(30,4))) as mw"
    ).repartition(3).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("fe")
  }

  test("double elements: contains and position with Spark's NaN and signed-zero equality, min and max") {
    checkVectorized(
      "SELECT i, array_contains(ad, 0.0d) z, array_contains(ad, -0.0d) nz, array_contains(ad, cast('NaN' as double)) n, " +
        "array_contains(ad, x) cx, array_position(ad, cast('NaN' as double)) pn, array_position(ad, -0.0d) pz, " +
        "array_min(ad) mn, array_max(ad) mx FROM fe",
      Seq(Project),
      tolerance = 0.0
    )
  }

  test("decimal elements of both widths: contains, position, min and max") {
    checkVectorized(
      "SELECT i, array_contains(ds, cast(2.5 as decimal(10,2))) c, array_position(ds, cast(-1.25 as decimal(10,2))) p, " +
        "array_min(ds) mn, array_max(ds) mx, array_contains(dw, w) cw, " +
        "array_position(dw, cast(-7.5 as decimal(30,4))) pw FROM fe",
      Seq(Project)
    )
  }

  test("double map keys: -0.0 finds 0.0, NaN finds NaN; under a filter") {
    checkVectorized(
      "SELECT i, m[0.0d] z, m[cast('NaN' as double)] n, element_at(m, 2.5d) t, m[x] mx FROM fe",
      Seq(Project)
    )
    checkVectorized("SELECT i FROM fe WHERE array_contains(ad, cast('NaN' as double)) AND i % 3 = 0", Seq(Filter))
  }

  test("wide decimal elements as results: array_min / array_max, element access, a map value; read by a filter") {
    checkVectorized(
      "SELECT i, array_min(dw) mn, array_max(dw) mx, dw[0] first, element_at(dw, -2) second, dw[2] third, " +
        "mw[i % 3] mv, mw[7] mnone FROM fe",
      Seq(Project)
    )
    // The gathered DECIMAL128 element compared and filtered on, as a wide column would be.
    checkVectorized("SELECT i FROM fe WHERE array_max(dw) > cast(5 as decimal(30,4)) AND dw[0] = w", Seq(Filter))
    // Lambdas over wide elements: kept by a comparison, returned as they are.
    checkVectorized(
      "SELECT i, filter(dw, x -> x > cast(0 as decimal(30,4))) pos, transform(dw, x -> x) same, " +
        "exists(dw, x -> x = w) hit FROM fe",
      Seq(Project)
    )
  }
}
