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

import io.vecruntime.kernels.{GroupedAccumulators, NativePool}
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.vecruntime.VectorHashAggregateExec

/**
 * #941: grouped aggregates whose tables pass 16,384 groups, with the accumulators' state in native pool
 * blocks (`spark.vecruntime.agg.nativeState`), answer exactly as Spark does -- long sums and counts, the
 * 128-bit decimal sums, averages through their merge, with nulls -- and give every block back. The flag is
 * set in-process here (local mode shares the JVM with the executor), as the executor plugin sets it from the
 * conf.
 */
@io.vecruntime.spark.test.tags.Agg
class NativeAggStateSuite extends VectorQuerySuite {

  private val Agg = classOf[VectorHashAggregateExec]
  private var before = false

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    before = GroupedAccumulators.nativeState()
    GroupedAccumulators.setNativeState(true)
    // 200k rows over 60k keys per partition-less shuffle task: every final table passes the threshold.
    spark
      .range(0, 200000, 1, 4)
      .selectExpr(
        "id % 60000 AS k",
        "CASE WHEN id % 11 = 0 THEN NULL ELSE id * 7 - 1000000 END AS l",
        "CAST(id % 9973 AS INT) AS i",
        "CAST(CASE WHEN id % 13 = 0 THEN NULL ELSE id * 1234567.89 END AS DECIMAL(30, 2)) AS dec",
        "CAST(id AS DOUBLE) / 3.0 AS d"
      )
      .createOrReplaceTempView("nat")
  }

  override protected def afterAll(): Unit = {
    GroupedAccumulators.setNativeState(before)
    super.afterAll()
  }

  test("long sums, counts and averages past the threshold match Spark") {
    checkVectorized(
      "SELECT k, sum(l), count(l), count(*), sum(i), avg(l) FROM nat GROUP BY k",
      Seq(Agg)
    )
  }

  test("128-bit decimal sums and averages past the threshold match Spark") {
    checkVectorized("SELECT k, sum(dec), avg(dec), count(dec) FROM nat GROUP BY k", Seq(Agg))
  }

  test("every native block is given back once the queries finish") {
    val inUse = NativePool.shared().inUseBytes()
    checkVectorized("SELECT k, sum(l), sum(dec), count(*) FROM nat GROUP BY k", Seq(Agg))
    assert(NativePool.shared().inUseBytes() <= inUse, "pool blocks still in use after the query")
  }
}
