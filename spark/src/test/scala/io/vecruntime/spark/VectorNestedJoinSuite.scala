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
import org.apache.spark.sql.vecruntime.{
  VectorBroadcastHashJoinExec,
  VectorShuffledHashJoinExec,
  VectorSortMergeJoinExec
}

/**
 * Array and struct columns through our joins (#705): as payload on either side of the broadcast and shuffled hash
 * joins under every join type Spark plans for them -- the build side's in a row store (#547), which keeps a shuffled
 * build side in memory -- and of the merge join, whose column builders copy nested lanes.
 */
class VectorNestedJoinSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.range(0, 4000).selectExpr(
      "cast(id as int) as k",
      "cast(id % 9 as int) as g",
      "case when id % 7 = 0 then null else array(cast(id as int), cast(null as int), cast(id % 3 as int)) end as fa",
      "case when id % 5 = 0 then null else named_struct('s', concat('f', id), 'v', array(cast(id % 4 as bigint))) end as fs"
    ).repartition(4).write.mode("overwrite").parquet(newTempPath("nj/fact"))
    spark.range(0, 600).selectExpr(
      "cast(id * 3 as int) as dk",
      "cast(id % 9 as int) as dg",
      "case when id % 4 = 0 then null else array(concat('d', id), '') end as da",
      "named_struct('n', cast(id as int), 'm', case when id % 3 = 0 then null else array(1.5D, cast(id as double)) end) as ds"
    ).repartition(2).write.mode("overwrite").parquet(newTempPath("nj/dim"))
    spark.read.parquet(newTempPath("nj/fact")).createOrReplaceTempView("nf")
    spark.read.parquet(newTempPath("nj/dim")).createOrReplaceTempView("nd")
  }

  private val payload = "SELECT k, fa, fs, da, ds FROM nf %s JOIN nd ON k = dk"

  test("broadcast hash join: nested payload on both sides, inner, outer, semi and anti") {
    // FULL and RIGHT (with the right side broadcast) are not broadcast joins in Spark: the shuffled tests cover them.
    for (jt <- Seq("", "LEFT")) {
      checkVectorized(
        payload.format(jt).replace("SELECT", "SELECT /*+ BROADCAST(nd) */"),
        Seq(classOf[VectorBroadcastHashJoinExec])
      )
    }
    checkVectorized(
      "SELECT /*+ BROADCAST(nd) */ k, fa FROM nf LEFT SEMI JOIN nd ON k = dk",
      Seq(classOf[VectorBroadcastHashJoinExec])
    )
    checkVectorized(
      "SELECT /*+ BROADCAST(nd) */ k, fs FROM nf LEFT ANTI JOIN nd ON k = dk",
      Seq(classOf[VectorBroadcastHashJoinExec])
    )
  }

  test("shuffled hash join: nested payload on both sides, the build side held in memory") {
    for (jt <- Seq("", "LEFT", "RIGHT", "FULL")) {
      checkVectorized(
        payload.format(jt).replace("SELECT", "SELECT /*+ SHUFFLE_HASH(nd) */"),
        Seq(classOf[VectorShuffledHashJoinExec])
      )
    }
    // A spill budget the build side passes: with a nested payload it is not bucketed, the answer is unchanged.
    withConf(io.vecruntime.spark.VectorConf.JoinSpillBytes -> "1k") {
      checkVectorized(
        payload.format("").replace("SELECT", "SELECT /*+ SHUFFLE_HASH(nd) */"),
        Seq(classOf[VectorShuffledHashJoinExec])
      )
    }
  }

  test("sort-merge join: nested payload on both sides, inner, outer and semi, runs spanning batches") {
    for (jt <- Seq("", "LEFT", "RIGHT", "FULL")) {
      checkVectorized(
        payload.format(jt).replace("SELECT", "SELECT /*+ MERGE(nd) */"),
        Seq(classOf[VectorSortMergeJoinExec])
      )
    }
    // A semi join under the hint is re-planned as our shuffled hash join (the sort-merge pre-pass's choice): ours either way.
    checkVectorized(
      "SELECT /*+ MERGE(nd) */ k, fa FROM nf LEFT SEMI JOIN nd ON k = dk",
      Seq(classOf[VectorShuffledHashJoinExec])
    )
    // Many rows per key (g, dg take 9 values): runs longer than a batch are copied by the nested column builders.
    withConf("spark.sql.parquet.columnarReaderBatchSize" -> "64", VectorConf.ScanNativeParquet -> "false") {
      checkVectorized(
        "SELECT /*+ MERGE(nd) */ g, fa, ds FROM nf JOIN nd ON g = dg WHERE k < 300",
        Seq(classOf[VectorSortMergeJoinExec])
      )
    }
  }

}
