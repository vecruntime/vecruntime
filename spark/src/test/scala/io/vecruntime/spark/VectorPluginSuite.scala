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

import io.vecruntime.spark.test.{SparkVectorFunSuite, TestTables}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFilterExec}

/** The `spark.plugins` entry point must register the extension without any other configuration. */
class VectorPluginSuite extends SparkVectorFunSuite {

  override protected def extraSparkConf: Map[String, String] =
    Map("spark.plugins" -> classOf[VectorPlugin].getName)

  test("plugin registers the session extension") {
    assert(
      spark.sparkContext.getConf.get("spark.sql.extensions").contains(classOf[VectorSparkSessionExtensions].getName)
    )
    TestTables.createMixed(spark, newTempPath("plugin/t"), rows = 2000)
    val df = spark.sql("SELECT i FROM t WHERE i > 100")
    assert(df.count() === 1899)
    assert(PlanUtils.allNodes(df.queryExecution.executedPlan).exists(_.isInstanceOf[VectorFilterExec]))
  }

  test("AQE starts from executor cores x initialPartitionsPerCore partitions (#689)") {
    def init(conf: (String, String)*): Option[Int] = VectorConf.initialShufflePartitions(conf.toMap.get)
    val cluster = Seq("spark.executor.instances" -> "23", "spark.executor.cores" -> "5")
    // 23 x 5 x 8 = 920 > 200
    assert(init(cluster: _*) === Some(920))
    assert(init(cluster :+ ("spark.vecruntime.shuffle.initialPartitionsPerCore" -> "4"): _*) === Some(460))
    // never below what the user asked for, and never over the user's own initialPartitionNum
    assert(init(cluster :+ ("spark.sql.shuffle.partitions" -> "2000"): _*) === None)
    assert(init(cluster :+ ("spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> "300"): _*) === None)
    // AQE or its coalescing off: the partition count is final, leave it
    assert(init(cluster :+ ("spark.sql.adaptive.enabled" -> "false"): _*) === None)
    assert(init(cluster :+ ("spark.sql.adaptive.coalescePartitions.enabled" -> "false"): _*) === None)
    assert(init(cluster :+ ("spark.vecruntime.shuffle.initialPartitionsPerCore" -> "0"): _*) === None)
    // cores unknown up front: local mode, dynamic allocation without a maximum
    assert(init() === None)
    assert(init("spark.dynamicAllocation.enabled" -> "true", "spark.executor.cores" -> "4") === None)
    assert(
      init(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.maxExecutors" -> "10",
        "spark.executor.cores" -> "4"
      ) === Some(320)
    )
    // this session (local mode) is left as it was
    assert(!spark.sparkContext.getConf.contains("spark.sql.adaptive.coalescePartitions.initialPartitionNum"))
  }
}
