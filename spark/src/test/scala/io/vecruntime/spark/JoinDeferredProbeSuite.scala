/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import io.vecruntime.spark.arrow.{DeferredGatherColumnVector, VectorAllocators}
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.vecruntime.VectorBroadcastHashJoinExec

/**
 * #603: a hash join emits its probe-side lane columns as views over its input (row ids, not copies), and a
 * chain of joins composes the ids, so a column carried through the chain is gathered once by whichever
 * operator reads it. Results must be Spark's with the views on and off, for every join type that probes.
 */
class JoinDeferredProbeSuite extends VectorQuerySuite {

  private val Bhj = classOf[VectorBroadcastHashJoinExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id % 97 AS INT) AS k1, CAST(id % 53 AS INT) AS k2, CAST(id % 31 AS INT) AS k3,
          |  CASE WHEN id % 19 = 0 THEN NULL ELSE id * 3 END AS v,
          |  CAST(id % 1000 AS DECIMAL(12,2)) AS m,
          |  CASE WHEN id % 7 = 0 THEN NULL ELSE CONCAT('s', CAST(id % 211 AS STRING)) END AS s
          |FROM range(0, 40000)""".stripMargin
      ).repartition(3).write.mode("overwrite").parquet(newTempPath("dp/fact"))
      spark.read.parquet(newTempPath("dp/fact")).createOrReplaceTempView("dp_fact")
      for ((name, n) <- Seq("d1" -> 90, "d2" -> 53, "d3" -> 40)) {
        spark.sql(s"SELECT CAST(id AS INT) AS k, CONCAT('$name-', CAST(id AS STRING)) AS name FROM range(0, $n)")
          .write.mode("overwrite").parquet(newTempPath(s"dp/$name"))
        spark.read.parquet(newTempPath(s"dp/$name")).createOrReplaceTempView(s"dp_$name")
      }
    }
  }

  private val chain =
    """SELECT d3.name, count(*) c, sum(f.v) sv, sum(f.m) sm, max(f.s) ms
      |FROM dp_fact f JOIN dp_d1 d1 ON f.k1 = d1.k JOIN dp_d2 d2 ON f.k2 = d2.k JOIN dp_d3 d3 ON f.k3 = d3.k
      |GROUP BY d3.name""".stripMargin

  test("a chain of broadcast joins composes the probe views and gathers each carried column once") {
    val created = DeferredGatherColumnVector.CREATED.sum()
    val gathered = DeferredGatherColumnVector.GATHERED.sum()
    checkVectorized(chain, Seq(Bhj))
    val c = DeferredGatherColumnVector.CREATED.sum() - created
    val g = DeferredGatherColumnVector.GATHERED.sum() - gathered
    assert(c > 0, "no deferred probe column")
    // v, m and s ride through three joins and are read once by the aggregate; k2 and k3 are read by the next
    // join's probe. Gathered views are a strict subset of the created ones.
    assert(g < c, s"$g of $c views were gathered")
  }

  test("join types that probe return Spark's rows with the views on and off") {
    val queries = Seq(
      chain,
      "SELECT f.k1, f.v, f.s, d1.name FROM dp_fact f JOIN dp_d1 d1 ON f.k1 = d1.k WHERE f.v % 5 = 0",
      "SELECT f.k1, f.v, d1.name FROM dp_fact f LEFT JOIN dp_d1 d1 ON f.k1 = d1.k AND d1.k < 50",
      "SELECT f.k1, f.s FROM dp_fact f LEFT SEMI JOIN dp_d1 d1 ON f.k1 = d1.k AND d1.k % 2 = 0",
      "SELECT f.k1, f.v FROM dp_fact f LEFT ANTI JOIN dp_d3 d3 ON f.k3 = d3.k",
      "SELECT f.k1, f.v, d1.name, d2.name FROM dp_fact f JOIN dp_d1 d1 ON f.k1 = d1.k " +
        "JOIN dp_d2 d2 ON f.k2 = d2.k AND f.v > d2.k * 100",
      "SELECT d1.name, f.s, count(*) FROM dp_fact f FULL OUTER JOIN dp_d1 d1 ON f.k1 = d1.k GROUP BY d1.name, f.s"
    )
    for (on <- Seq("true", "false")) {
      withConf(VectorConf.JoinDeferredProbe -> on) {
        queries.foreach(q => checkVectorized(q, Nil))
      }
    }
  }

  test("a LIMIT over the chain leaks no gathered vectors") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    checkVectorized(chain.replace("GROUP BY d3.name", "GROUP BY d3.name ORDER BY d3.name LIMIT 3"), Seq(Bhj))
    checkVectorized(
      "SELECT f.v, f.s, d1.name FROM dp_fact f JOIN dp_d1 d1 ON f.k1 = d1.k JOIN dp_d2 d2 ON f.k2 = d2.k LIMIT 17",
      Nil
    )
    assert(root.getAllocatedMemory === before, s"allocated ${root.getAllocatedMemory} vs $before before")
  }
}
