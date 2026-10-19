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
@io.vecruntime.spark.test.tags.Nested
@io.vecruntime.spark.test.tags.Join
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

  test("shuffled hash join: array and struct keys by their bytes, every join type, null keys never matching") {
    // Struct (k % 50, 's' || k % 7) and array (k % 40, null) keys on both sides, nulls on both; Spark's key equality
    // is field by field and element by element, a null field equal to a null field.
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as k",
      "case when id % 11 = 0 then null else named_struct('a', cast(id % 50 as int), 'b', concat('s', id % 7)) end as sk",
      "case when id % 13 = 0 then null else array(cast(id % 40 as int), cast(null as int)) end as ak"
    ).repartition(3).write.mode("overwrite").parquet(newTempPath("nj/keys_left"))
    spark.range(0, 400).selectExpr(
      "cast(id as int) as d",
      "case when id % 9 = 0 then null else named_struct('a', cast(id % 50 as int), 'b', concat('s', id % 7)) end as sk2",
      "case when id % 5 = 0 then null else array(cast(id % 40 as int), cast(null as int)) end as ak2"
    ).repartition(2).write.mode("overwrite").parquet(newTempPath("nj/keys_right"))
    spark.read.parquet(newTempPath("nj/keys_left")).createOrReplaceTempView("nkl")
    spark.read.parquet(newTempPath("nj/keys_right")).createOrReplaceTempView("nkr")
    for (jt <- Seq("", "LEFT", "RIGHT", "FULL")) {
      checkVectorized(
        s"SELECT /*+ SHUFFLE_HASH(nkr) */ k, sk, d FROM nkl $jt JOIN nkr ON sk = sk2",
        Seq(classOf[VectorShuffledHashJoinExec])
      )
    }
    // An array key beside a narrow one; an anti join on a struct key.
    checkVectorized(
      "SELECT /*+ SHUFFLE_HASH(nkr) */ k, ak, d FROM nkl JOIN nkr ON ak = ak2 AND k % 50 = d % 50",
      Seq(classOf[VectorShuffledHashJoinExec])
    )
    checkVectorized(
      "SELECT /*+ SHUFFLE_HASH(nkr) */ k FROM nkl LEFT ANTI JOIN nkr ON sk = sk2",
      Seq(classOf[VectorShuffledHashJoinExec])
    )
    // The broadcast join: the same keys, the build side's key column read from its row store.
    for (jt <- Seq("", "LEFT", "LEFT SEMI", "LEFT ANTI")) {
      checkVectorized(
        s"SELECT /*+ BROADCAST(nkr) */ k, sk FROM nkl $jt JOIN nkr ON sk = sk2",
        Seq(classOf[VectorBroadcastHashJoinExec])
      )
    }
    checkVectorized(
      "SELECT /*+ BROADCAST(nkl) */ k, d, ak2 FROM nkl RIGHT JOIN nkr ON ak = ak2",
      Seq(classOf[VectorBroadcastHashJoinExec])
    )
    checkVectorized(
      "SELECT /*+ BROADCAST(nkr) */ k, ak, d FROM nkl JOIN nkr ON ak = ak2 AND k % 50 = d % 50",
      Seq(classOf[VectorBroadcastHashJoinExec])
    )
    // The merge join (#705): the same keys by their order-preserving bytes, both sides sorted on them by our sort.
    withConf(io.vecruntime.spark.VectorConf.SortMergeJoinMode -> "merge") {
      for (jt <- Seq("", "LEFT", "RIGHT", "FULL")) {
        checkVectorized(
          s"SELECT /*+ MERGE(nkr) */ k, sk, d FROM nkl $jt JOIN nkr ON sk = sk2",
          Seq(classOf[VectorSortMergeJoinExec])
        )
      }
      checkVectorized(
        "SELECT /*+ MERGE(nkr) */ k, ak, d FROM nkl JOIN nkr ON ak = ak2 AND k % 50 = d % 50",
        Seq(classOf[VectorSortMergeJoinExec])
      )
      checkVectorized(
        "SELECT /*+ MERGE(nkr) */ k FROM nkl LEFT ANTI JOIN nkr ON sk = sk2",
        Seq(classOf[VectorSortMergeJoinExec])
      )
      // The left side's unmatched rows carry its nested payload (the left-only emit gathers nested lanes).
      checkVectorized(
        "SELECT /*+ MERGE(nkr) */ k, sk, ak FROM nkl LEFT ANTI JOIN nkr ON ak = ak2",
        Seq(classOf[VectorSortMergeJoinExec])
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
