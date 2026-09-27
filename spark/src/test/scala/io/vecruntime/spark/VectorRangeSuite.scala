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

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.plans.physical.{RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{RangeExec, RowToColumnarExec, SortExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.vecruntime.{
  VectorBroadcastHashJoinExec,
  VectorFilterExec,
  VectorHashAggregateExec,
  VectorProjectExec,
  VectorRangeExec,
  VectorRangePlanner,
  VectorShuffledHashJoinExec,
  VectorSortMergeJoinExec
}

/**
 * The columnar `range()` leaf must return exactly Spark's rows in exactly Spark's partitions: every
 * shape below is compared row for row with the plugin off, and the partition-dependent ones
 * (`spark_partition_id()`, `monotonically_increasing_id()`, a `GROUP BY id` that needs no exchange
 * because the range is already range-partitioned on `id`, a sort on `id` the planner elides) pin the
 * split and the partitioning contract. The batch size is set small in some tests so a partition spans
 * several batches with a short tail.
 */
class VectorRangeSuite extends VectorQuerySuite {

  private val Range_ = classOf[VectorRangeExec]
  private val Filter = classOf[VectorFilterExec]
  private val Project = classOf[VectorProjectExec]
  private val Agg = classOf[VectorHashAggregateExec]

  private val SmallBatches = "spark.sql.inMemoryColumnarStorage.batchSize" -> "7"

  /** `checkVectorized` plus: Spark's leaf is gone and no `RowToColumnarExec` sits above ours. */
  private def checkRange(sql: String, more: Class[_ <: org.apache.spark.sql.execution.SparkPlan]*): DataFrame = {
    val df = checkVectorized(sql, Range_ +: more)
    assert(nodesOf[RangeExec](df).isEmpty, finalPlan(df).treeString)
    assert(
      nodesOf[RowToColumnarExec](df).forall(!_.child.isInstanceOf[VectorRangeExec]),
      s"RowToColumnarExec above our range:\n${finalPlan(df).treeString}"
    )
    df
  }

  private def rows(df: DataFrame): Long =
    nodesOf[VectorRangeExec](df).map(_.metrics("numOutputRows").value).sum

  test("range with and without a step, forwards and backwards, in one and several slices") {
    for (
      sql <- Seq(
        "SELECT id FROM range(10)",
        "SELECT id FROM range(0, 10)",
        "SELECT id FROM range(5, 100, 7)",
        "SELECT id FROM range(100, 5, -7)",
        "SELECT id FROM range(-50, 50, 3)",
        "SELECT id FROM range(50, -50, -3)",
        "SELECT id FROM range(0, 10, 3, 1)",
        "SELECT id FROM range(0, 10, 3, 4)",
        "SELECT id FROM range(0, 10, 3, 32)", // more slices than rows: empty partitions
        "SELECT id FROM range(0, 100000, 1, 3)",
        "SELECT id FROM range(1, 100000, 13, 5)",
        "SELECT id FROM range(100000, -1, -13, 5)"
      )
    ) {
      val df = checkRange(sql)
      assert(rows(df) === withPlugin(enabled = false)(spark.sql(sql).count()), s"numOutputRows for $sql")
    }
    withConf(SmallBatches) {
      checkRange("SELECT id FROM range(0, 100, 1, 3)")
      checkRange("SELECT id FROM range(0, 99, 4, 2)")
      checkRange("SELECT id FROM range(99, 0, -4, 2)")
    }
  }

  test("empty ranges plan our leaf and produce no partitions") {
    for (
      sql <- Seq(
        "SELECT id FROM range(0, 0)",
        "SELECT id FROM range(5, 5, -1)",
        "SELECT id FROM range(0, 10, -1)",
        "SELECT id FROM range(10, 0)",
        "SELECT id FROM range(10, 0, 1, 4)"
      )
    ) {
      val df = checkRange(sql)
      assert(df.collect().isEmpty)
      assert(rows(df) === 0L)
      assert(nodesOf[VectorRangeExec](df).head.outputPartitioning === UnknownPartitioning(0))
    }
  }

  test("ranges at the Long bounds: the last partition's clamped end yields Spark's rows") {
    val max = Long.MaxValue
    val min = Long.MinValue
    for (
      sql <- Seq(
        s"SELECT id FROM range(${max - 7}, $max)",
        s"SELECT id FROM range(${max - 7}, $max, 3)",
        s"SELECT id FROM range(${max - 7}, $max, 3, 4)",
        s"SELECT id FROM range(${max - 100}, $max, 7, 3)",
        s"SELECT id FROM range($max, ${max - 7}, -1)",
        s"SELECT id FROM range($max, ${max - 7}, -3, 2)",
        s"SELECT id FROM range(${max - 5}, $max, ${max / 2})"
      )
    ) checkRange(sql)
    // Long.MinValue is not a bigint literal in SQL (the parser reads -9223372036854775808 as a decimal
    // negated); these go through the DataFrame API and a view.
    spark.range(min, min + 7).createOrReplaceTempView("r_min")
    spark.range(min, min + 7, 3, 4).createOrReplaceTempView("r_min_step")
    spark.range(min + 7, min, -3, 2).createOrReplaceTempView("r_min_back")
    spark.range(min + 100, min, -7, 3).createOrReplaceTempView("r_min_back3")
    for (v <- Seq("r_min", "r_min_step", "r_min_back", "r_min_back3")) {
      checkRange(s"SELECT id FROM $v")
    }
    // A step so large that a batch of it overflows a Long: Spark's generated range (`batchEnd +=
    // nextBatchTodo * step`) wraps there and loses the rows of the last partition -- range(MinValue,
    // MaxValue, MaxValue / 2) returns 2 rows under whole-stage codegen and its 5 rows without it. Ours
    // computes the exact count from the clamped bounds, as Spark's row path does, so the comparison runs
    // against that path.
    spark.range(min, max, max / 2).createOrReplaceTempView("r_span")
    spark.range(max, min, -(max / 2), 3).createOrReplaceTempView("r_span_back")
    withConf("spark.sql.codegen.wholeStage" -> "false") {
      assert(checkRange("SELECT id FROM r_span").count() === 5)
      checkRange("SELECT id FROM r_span_back")
    }
    val codegen = withPlugin(enabled = false)(spark.sql("SELECT id FROM r_span").count())
    assert(
      codegen === 2L,
      s"Spark's generated range over r_span returned $codegen rows; if this is 5 the divergence above is gone"
    )
  }

  test("a range partition matches Spark's element by element (the split, slice by slice)") {
    // Spark's RangeExec.doExecute arithmetic, applied here by hand, against VectorRangeExec.partition.
    def sparkPartition(i: Int, slices: Int, start: Long, end: Long, step: Long): Seq[Long] = {
      val numElements = Range(start, end, step, Some(slices)).numElements
      def safe(bi: BigInt): Long = if (bi.isValidLong) bi.toLong else if (bi > 0) Long.MaxValue else Long.MinValue
      val s = safe((i * numElements) / slices * step + start)
      val e = safe(((i + 1) * numElements) / slices * step + start)
      val out = scala.collection.mutable.ArrayBuffer.empty[Long]
      var number = s
      var overflow = false
      while (!overflow && (if (step > 0) number < e else number > e)) {
        val ret = number
        number += step
        if (number < ret ^ step < 0) overflow = true
        out += ret
      }
      out.toSeq
    }
    for (
      (start, end, step, slices) <- Seq(
        (0L, 10L, 1L, 4),
        (0L, 10L, 3L, 4),
        (0L, 10L, 3L, 32),
        (100L, 5L, -7L, 3),
        (Long.MaxValue - 7, Long.MaxValue, 3L, 4),
        (Long.MaxValue - 100, Long.MaxValue, 7L, 3),
        (Long.MinValue + 7, Long.MinValue, -3L, 2),
        (Long.MinValue, Long.MaxValue, Long.MaxValue / 2, 3),
        (Long.MaxValue, Long.MinValue, -(Long.MaxValue / 2), 3),
        (Long.MinValue, Long.MinValue + 1, 1L, 1),
        (Long.MaxValue - 1, Long.MaxValue, 1L, 1)
      )
    ) {
      val numElements = Range(start, end, step, Some(slices)).numElements
      for (i <- 0 until slices) {
        val expected = sparkPartition(i, slices, start, end, step)
        val (first, n) = VectorRangeExec.partition(i, slices, numElements, start, step)
        assert(n === expected.length.toLong, s"rows of partition $i of range($start, $end, $step, $slices)")
        if (n > 0) assert(first === expected.head, s"first of partition $i of range($start, $end, $step, $slices)")
      }
    }
  }

  test("a range feeds our filter, projection and aggregates without a transition") {
    withConf(SmallBatches) {
      checkRange("SELECT id FROM range(0, 1000, 1, 4) WHERE id % 7 = 3", Filter)
      checkRange("SELECT id * 2 AS d, id + 1 AS i1, cast(id AS double) AS dd FROM range(0, 1000, 3, 4)", Project)
      checkRange(
        "SELECT sum(id) AS s, count(*) AS n, min(id) AS mn, max(id) AS mx, avg(id) AS a FROM range(0, 100000, 1, 5)",
        Agg
      )
      checkRange("SELECT id % 3 AS k, count(*) AS n, sum(id) AS s FROM range(0, 10000, 1, 4) GROUP BY id % 3", Agg)
      checkRange("SELECT sum(id) AS s FROM range(0, 10) WHERE id > 100", Filter, Agg) // every row dropped
    }
    // A single-slice range is a single partition, so the global limit sits directly on our leaf.
    checkRange("SELECT id FROM range(0, 100, 1, 1) LIMIT 5")
    checkRange("SELECT count(*) AS n FROM range(0, 10000, 1, 1)", Agg)
  }

  test("the range's ordering and partitioning are kept, so the planner's elided sorts and exchanges stay valid") {
    // Spark's RangeExec reports outputOrdering = id ASC and RangePartitioning(id, slices); the plan above
    // was built on that before the rule ran, so a sort on id is elided and a GROUP BY id needs no exchange.
    val sorted = checkRange("SELECT id FROM range(0, 1000, 1, 4) ORDER BY id")
    assert(nodesOf[SortExec](sorted).isEmpty, finalPlan(sorted).treeString)
    val grouped = checkRange("SELECT id, count(*) AS n FROM range(0, 1000, 1, 4) GROUP BY id", Agg)
    assert(nodesOf[ShuffleExchangeExec](grouped).isEmpty, finalPlan(grouped).treeString)
    assert(nodesOf[RowToColumnarExec](grouped).isEmpty, finalPlan(grouped).treeString)
    val leaf = nodesOf[VectorRangeExec](grouped).head
    assert(leaf.outputPartitioning.isInstanceOf[RangePartitioning] && leaf.outputPartitioning.numPartitions === 4)
    assert(nodesOf[VectorRangeExec](
      checkRange("SELECT id FROM range(0, 10, 1, 1)")
    ).head.outputPartitioning === SinglePartition)
    assert(nodesOf[VectorRangeExec](checkRange("SELECT id FROM range(0, 10, 1, 4)")).head.outputOrdering.nonEmpty)
  }

  test("spark_partition_id() and monotonically_increasing_id() over a range see Spark's partitions") {
    withConf(SmallBatches) {
      // spark_partition_id() is compiled (SparkPartitionIdExpr): the projection over it is ours too,
      // and the values name the same partitions as over Spark's RangeExec.
      checkRange("SELECT spark_partition_id() AS p, id FROM range(0, 1000, 1, 7)", Project)
      checkRange(
        "SELECT spark_partition_id() AS p, count(*) AS n, min(id) AS mn, max(id) AS mx FROM range(0, 1000, 3, 7) GROUP BY spark_partition_id()",
        Project
      )
      checkRange("SELECT spark_partition_id() AS p, id FROM range(0, 10, 3, 32) WHERE id > 2", Filter, Project) // empty partitions
      checkRange("SELECT monotonically_increasing_id() AS m, id FROM range(0, 1000, 1, 7)", Project)
      checkRange(
        "SELECT monotonically_increasing_id() AS m, id FROM range(1000, 0, -1, 5) WHERE id % 2 = 0",
        Filter,
        Project
      )
      checkRange("SELECT monotonically_increasing_id() AS m, id FROM range(0, 10, 3, 32)", Project) // empty partitions
    }
  }

  test("ranges on both sides of a join") {
    withConf(VectorConf.SortMergeJoinMode -> "off") {
      checkRange(
        "SELECT /*+ SHUFFLE_HASH(b) */ a.id, b.id AS bid FROM range(0, 1000) a JOIN range(500, 1500) b ON a.id = b.id",
        classOf[VectorShuffledHashJoinExec]
      )
    }
    withConf(VectorConf.SortMergeJoinMode -> "merge") {
      checkRange(
        "SELECT /*+ MERGE(b) */ a.id, b.id AS bid FROM range(0, 1000, 1, 3) a LEFT JOIN range(0, 2000, 2, 5) b ON a.id = b.id",
        classOf[VectorSortMergeJoinExec]
      )
    }
    // The small side is broadcast; the streamed side is our leaf.
    checkRange(
      "SELECT /*+ BROADCAST(b) */ a.id, b.id AS bid FROM range(0, 100000, 1, 4) a JOIN range(0, 100) b ON a.id = b.id",
      classOf[VectorBroadcastHashJoinExec]
    )
    checkRange(
      "SELECT count(*) AS n, sum(a.id) AS s FROM range(0, 10000, 1, 4) a JOIN range(0, 10000, 7, 2) b ON a.id = b.id",
      Agg
    )
  }

  test("refused shapes and the switch") {
    // A slice count Spark rejects at execution ("Positive number of partitions required") stays Spark's.
    assert(VectorRangePlanner.plan(RangeExec(Range(0L, 10L, 1L, Some(0)))) === Left("range with 0 slices"))
    assert(VectorRangePlanner.plan(RangeExec(Range(
      0L,
      10L,
      1L,
      Some(2),
      isStreaming = true
    ))) === Left("streaming range"))
    withConf(VectorConf.RangeEnabled -> "false") {
      val df = withPlugin(enabled = true) {
        val d = spark.sql("SELECT sum(id) AS s FROM range(0, 1000, 1, 4)"); d.collect(); d
      }
      // The old shape: Spark's leaf and, above it, Spark's partial aggregate (a row child converts nothing
      // of ours); only the Final aggregate over the shuffle is ours, behind a RowToColumnarExec.
      assert(nodesOf[VectorRangeExec](df).isEmpty && nodesOf[RangeExec](df).nonEmpty, finalPlan(df).treeString)
      assert(nodesOf[HashAggregateExec](df).nonEmpty, finalPlan(df).treeString)
      assert(nodesOf[VectorHashAggregateExec](df).nonEmpty, finalPlan(df).treeString)
      assert(nodesOf[RowToColumnarExec](df).nonEmpty, finalPlan(df).treeString)
    }
  }
}
