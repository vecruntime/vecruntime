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
package org.apache.spark.sql.vecruntime.shuffle

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.vecruntime.{ShuffledColumnarRDD, VectorShuffleExchangeExec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #559: our reduce tasks report no preferred locations unless `spark.vecruntime.shuffle.reduceLocality.enabled`
 * is set -- the preference collapsed whole post-shuffle stages onto one host behind the locality wait at 1 TB.
 */
class ReduceLocalitySuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReduceLocalitySuite")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.plugins", "io.vecruntime.spark.VectorPlugin")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def nodes(plan: SparkPlan): Seq[SparkPlan] = plan +: (plan match {
    case q: QueryStageExec => nodes(q.plan)
    case p => p.children.flatMap(nodes)
  })

  private def exchange(): VectorShuffleExchangeExec = {
    val df = spark.sql("select cast(id % 97 as int) k, count(*) c from range(0, 20000) group by 1")
    df.collect()
    val plan = df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case p => p
    }
    nodes(
      plan
    ).collectFirst { case e: VectorShuffleExchangeExec => e }.getOrElse(fail(s"no exchange of ours in\n$plan"))
  }

  private def preferred(ex: VectorShuffleExchangeExec): Seq[Seq[String]] = {
    val rdd = new ShuffledColumnarRDD(ex.shuffleDependency, Map.empty)
    rdd.partitions.toSeq.map(rdd.preferredLocations)
  }

  test("reduce tasks report no preferred locations by default") {
    assert(!VectorShuffleExchangeExec.reduceLocality(SparkEnv.get.conf))
    val prefs = preferred(exchange())
    assert(prefs.nonEmpty)
    assert(prefs.forall(_.isEmpty), s"preferred locations reported: $prefs")
  }

  test("with spark.vecruntime.shuffle.reduceLocality.enabled the map-output hosts are preferred again") {
    val conf = SparkEnv.get.conf
    conf.set(VectorShuffleExchangeExec.ReduceLocalityKey, "true")
    try {
      val prefs = preferred(exchange())
      // One local executor holds every map output, so every non-empty reduce partition prefers its host.
      assert(prefs.exists(_.nonEmpty), s"no preferred locations with the switch on: $prefs")
    } finally conf.remove(VectorShuffleExchangeExec.ReduceLocalityKey)
  }
}
