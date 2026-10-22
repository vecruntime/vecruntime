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
 * Nested constructors in a projection (#842, #901 R2): `named_struct(...)`, `array(...)` and `map(...)`
 * build a nested output column from compiled children -- scalar lanes, moved nested columns, null
 * literals or other constructors, to any depth (an array of maps, a map of struct values, a struct of
 * an array of maps) -- and compile into our project, answering as Spark does: null elements, null
 * predicates (the else branch), multi-byte strings, booleans, dates, decimals, and a selection before
 * the project. Maps whose keys may be null or repeat keep Spark's projection.
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

    // A source with nested columns to move into a constructor field (a struct, an array, a map,
    // and a nested array-of-map), with nulls at every level.
    val nestedPath = newTempPath("ctor_nested")
    spark.range(0, 3000).selectExpr(
      "id",
      "if(id % 11 = 0, null, named_struct('a', cast(id as int), 't', if(id % 3 = 0, null, concat('t', id)))) as st",
      "if(id % 13 = 0, null, array(concat('x', id), null, concat('y', id % 5))) as arr",
      "if(id % 17 = 0, null, map(concat('k', id % 7), id, 'z', id * 2)) as mp",
      "if(id % 19 = 0, null, array(map('a', cast(id as int)), map('b', cast(id % 4 as int)))) as aom"
    ).repartition(3).write.mode("overwrite").parquet(nestedPath)
    spark.read.parquet(nestedPath).createOrReplaceTempView("ctor_nested")
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

  test("map keys that may repeat no longer decline: LAST_WIN keeps the last value, per Spark, plugin on vs off") {
    withConf("spark.sql.mapKeyDedupPolicy" -> "LAST_WIN") {
      // Duplicate keys across rows (literal) and within one row; a key that is a column.
      checkVectorized(
        "SELECT id, map(concat('a', id % 2), id, concat('b', id % 3), id, concat('a', id % 2), id * 10) dup, " +
          "map('k', id, 'k', id + 1, 'k', id + 2) same, " +
          "map(cast(id % 3 as int), id, cast(id % 3 as int), id * 7) colkey FROM ctor",
        Seq(Project)
      )
    }
  }

  test("map key equality is Spark's: -0.0 == 0.0, under LAST_WIN, plugin on vs off") {
    withConf("spark.sql.mapKeyDedupPolicy" -> "LAST_WIN") {
      // -0.0 and 0.0 collide; last value wins at the first position. (NaN-key equality is covered by the
      // EXCEPTION test below -- two NaN keys collide and raise -- since a NaN-keyed Map does not compare
      // equal with Scala's `==` here even when the two sides are identical.)
      checkVectorized(
        "SELECT id, map(cast(-0.0 as double), id, cast(0.0 as double), id + 1) zero, " +
          "map(cast(id % 2 as double), id, cast(id % 2 as double), id + 5) colkey FROM ctor",
        Seq(Project)
      )
    }
  }

  test("nested map keys compare structurally, under LAST_WIN, plugin on vs off") {
    withConf("spark.sql.mapKeyDedupPolicy" -> "LAST_WIN") {
      checkVectorized(
        "SELECT id, map(named_struct('a', cast(id % 3 as int)), id, named_struct('a', cast(id % 3 as int)), id + 1) sk, " +
          "map(array(cast(id % 2 as int), 1), id, array(cast(id % 2 as int), 1), id + 2) ak FROM ctor",
        Seq(Project)
      )
    }
  }

  test("under EXCEPTION (the default), a duplicate map key raises Spark's DUPLICATED_MAP_KEY") {
    // A row whose two keys collide (id % 2 repeats for even/odd pairs is avoided; force a guaranteed dup).
    def duplicatedMapKey(sql: String): Boolean = {
      val e = intercept[Exception](withPlugin(enabled = true)(spark.sql(sql).collect()))
      Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).exists { t =>
        t match {
          case st: org.apache.spark.SparkThrowable => st.getCondition == "DUPLICATED_MAP_KEY"
          case _ => false
        }
      }
    }
    // Literal duplicate within a row, a column duplicate, and a NaN/-0.0 collision all raise under EXCEPTION.
    assert(duplicatedMapKey("SELECT map('k', id, 'k', id + 1) m FROM ctor"), "literal dup did not raise")
    assert(duplicatedMapKey("SELECT map(cast(id % 1 as int), id, cast(id % 1 as int), id) m FROM ctor"), "column dup")
    assert(
      duplicatedMapKey("SELECT map(cast('NaN' as double), id, sqrt(cast(-1 as double)), id) m FROM ctor"),
      "NaN collision did not raise"
    )
    // Spark itself raises the same class for the same query (plugin off).
    val sparkErr = intercept[Exception](
      withPlugin(enabled = false)(spark.sql("SELECT map('k', id, 'k', id + 1) m FROM ctor").collect())
    )
    assert(
      Iterator.iterate[Throwable](sparkErr)(_.getCause).takeWhile(_ != null).exists {
        case st: org.apache.spark.SparkThrowable => st.getCondition == "DUPLICATED_MAP_KEY"
        case _ => false
      },
      "Spark did not raise DUPLICATED_MAP_KEY for the same query"
    )
  }

  test("named_struct(...) over scalar lanes, with null fields and a null-guarded struct") {
    checkVectorized(
      "SELECT id, named_struct('a', i, 's', s, 'day', day, 'dec', dec, 'b', b) st, " +
        "named_struct('n', cast(null as int), 'k', concat('k', id % 5)) nst, " +
        "if(s is null, null, named_struct('v', s, 'len', length(s))) gst FROM ctor",
      Seq(Project)
    )
  }

  test("nesting: a struct of an array and a map, and a struct of structs, with nulls inside") {
    checkVectorized(
      "SELECT id, " +
        "named_struct('arr', array(s, 'x', s), 'm', map(concat('k', id % 4), id)) outer1, " +
        "named_struct('inner', named_struct('a', i, 's', s), 'xs', array(i, cast(id % 7 as int))) outer2, " +
        "array(named_struct('a', i), named_struct('a', cast(id % 3 as int))) arr_of_struct FROM ctor",
      Seq(Project)
    )
  }

  test(
    "a selection before the project (a filter forwarding a SelectedColumnarBatch) builds the nested value on live rows"
  ) {
    checkVectorized(
      "SELECT id, named_struct('a', i, 's', s) st, array(s, 'x') xs, map(concat('k', id % 4), id) m " +
        "FROM ctor WHERE i IS NOT NULL AND id % 3 = 0",
      Seq(Project)
    )
  }

  test("an array of maps built by constructors, with null elements and a null map inside") {
    checkVectorized(
      "SELECT id, array(map(concat('k', id % 4), id), map('z', id * 2)) aom, " +
        "array(if(id % 5 = 0, null, map('a', cast(id % 3 as int)))) aom_null FROM ctor",
      Seq(Project)
    )
  }

  test("a map of struct values built by constructors, with null fields and a distinct literal key set") {
    checkVectorized(
      "SELECT id, map('p', named_struct('a', i, 's', s), 'q', named_struct('a', cast(id % 4 as int), 's', 'x')) ms, " +
        "map(concat('k', id % 6), named_struct('v', id, 'd', day)) ms1 FROM ctor",
      Seq(Project)
    )
  }

  test("a struct of an array of maps, built by constructors to three levels") {
    checkVectorized(
      "SELECT id, named_struct('aom', array(map('a', i), map('b', cast(id % 7 as int))), 'tag', s) soam, " +
        "named_struct('deep', array(named_struct('m', map(concat('k', id % 3), id)))) deep, " +
        "map(concat('k', id), id, 'z', id * 2) insert_shape FROM ctor",
      Seq(Project)
    )
  }

  test("moved nested columns as constructor fields and elements (a struct, array, map and array-of-map moved in)") {
    checkVectorized(
      "SELECT id, named_struct('s', st, 'a', arr, 'm', mp, 'aom', aom) packed, " +
        "array(st, named_struct('a', cast(id as int), 't', 'lit')) structs, " +
        "map('moved', mp, 'built', map('b', id)) maps_of_maps, " +
        "array(arr, array(concat('z', id))) arr_of_arr FROM ctor_nested",
      Seq(Project)
    )
  }

  test("a folded nested literal (all-literal named_struct / array / map) builds in our project") {
    checkVectorized(
      "SELECT id, named_struct('a', 1, 's', 'x', 'inner', named_struct('n', cast(2 as int))) lit_st, " +
        "array(array(1, 2), array(3)) lit_aa, map('k', array(1, 2), 'j', array(3)) lit_ma FROM ctor",
      Seq(Project)
    )
  }
}
