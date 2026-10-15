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
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.vecruntime.VectorHashAggregateExec

/**
 * Array and struct grouping keys (#705): the aggregate groups by each key's canonical bytes on the UTF8
 * key lane and decodes them back for its output, in both stages -- null keys, empty arrays, null
 * elements and fields, string, long and wide decimal leaves, a struct holding an array, beside a narrow key.
 */
class VectorNestedGroupingSuite extends VectorQuerySuite {
  private val Agg = classOf[VectorHashAggregateExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val path = newTempPath("nested-grouping")
    spark.range(0, 6000).selectExpr(
      "cast(id as int) as i",
      // Few distinct arrays, so groups repeat: nulls, empty arrays, null elements.
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else array(cast(id % 3 as int), case when id % 5 = 0 then null else cast(id % 4 as int) end) end as ai",
      "case when id % 9 = 0 then null else array(concat('k', id % 6), case when id % 4 = 0 then null else '' end) end as sa",
      "case when id % 8 = 0 then null else named_struct('a', cast(id % 5 as bigint), " +
        "'b', case when id % 3 = 0 then null else concat('s', id % 2) end, " +
        "'c', array(cast(id % 2 as decimal(30,2)), cast(null as decimal(30,2)))) end as st",
      "array(id % 4 = 0, id % 3 = 0) as ab",
      // Float leaves Spark normalises before grouping: -0.0 and 0.0, NaN of either sign.
      "array(CASE id % 4 WHEN 0 THEN -0.0D WHEN 1 THEN 0.0D WHEN 2 THEN cast('NaN' AS double) " +
        "ELSE -cast('NaN' AS double) END, 1.5D) as af",
      "named_struct('d', CASE WHEN id % 2 = 0 THEN -0.0D ELSE 0.0D END, " +
        "'f', cast(CASE WHEN id % 3 = 0 THEN 'NaN' ELSE '-0.0' END AS float)) as sf",
      "cast(id % 13 as int) as t"
    ).repartition(4).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("ng")
  }

  private def bothStagesOurs(sql: String): Unit = {
    val df = checkVectorized(sql, Seq(Agg))
    assert(nodesOf[BaseAggregateExec](df).isEmpty, s"a Spark aggregate is left: ${finalPlan(df).treeString}")
    assert(nodesOf[VectorHashAggregateExec](df).size >= 2, finalPlan(df).treeString)
  }

  test("group by an array of ints, of strings and of booleans") {
    bothStagesOurs("SELECT ai, count(*) c, sum(i) s FROM ng GROUP BY ai")
    bothStagesOurs("SELECT sa, count(*) c, max(i) m FROM ng GROUP BY sa")
    bothStagesOurs("SELECT ab, count(*) c FROM ng GROUP BY ab")
  }

  test("group by a struct holding a string and an array of wide decimals") {
    bothStagesOurs("SELECT st, count(*) c, min(i) m FROM ng GROUP BY st")
  }

  test("a nested key beside a narrow key, and distinct over a nested column") {
    bothStagesOurs("SELECT t, ai, count(*) c FROM ng GROUP BY t, ai")
    bothStagesOurs("SELECT DISTINCT sa FROM ng")
    bothStagesOurs("SELECT count(DISTINCT ai) FROM ng")
  }

  test("a computed nested key") {
    bothStagesOurs("SELECT transform(ai, x -> x + 1) k, count(*) c FROM ng GROUP BY transform(ai, x -> x + 1)")
  }

  test("float leaves group as Spark normalises them: -0.0 with 0.0, every NaN together") {
    // Scalars read out of each group: the harness compares rows with `==`, under which a NaN inside an
    // array is not equal to itself. Two groups each: [0.0, 1.5] and [NaN, 1.5]; (0.0, NaN) and (0.0, 0.0).
    bothStagesOurs("SELECT isnan(af[0]) n, af[0] = 0.0D z, count(*) c FROM ng GROUP BY af")
    bothStagesOurs("SELECT sf.d = 0.0D d, isnan(sf.f) n, sf.f = 0.0F z, count(*) c FROM ng GROUP BY sf")
  }

  test("a tiny spill threshold: nested keys are not bucketed, the table keeps them and the answer is unchanged") {
    withConf(
      org.apache.spark.sql.vecruntime.AggSpillPolicy.ThresholdKey -> "8k",
      org.apache.spark.sql.vecruntime.AggSpillPolicy.BucketsKey -> "4"
    ) {
      bothStagesOurs("SELECT st, ai, count(*) c, sum(i) s FROM ng GROUP BY st, ai")
    }
  }
}
