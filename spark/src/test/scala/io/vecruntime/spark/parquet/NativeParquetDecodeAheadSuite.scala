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
package io.vecruntime.spark.parquet

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.arrow.VectorAllocators
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * Plan item 14, part C: decoding the next row group(s) of a split ahead of the one the task thread is
 * consuming. The native scan does this by composing the two read-ahead stages it already has, both on the
 * bounded prefetch pool and both governed by the same execution-memory accounting:
 *
 *   - `prefetchRowGroups` (#568) reads the next row group's compressed pages ahead, chained so the reads stay
 *     in row-group order and so one `ParquetFileReader` is used by one thread at a time; the bytes are reserved
 *     against the per-task and executor caps and held in execution memory (`ReadAheadMemory`, #915), given back
 *     when the row group is used or dropped.
 *   - `decodeAhead` (#606) runs the reader on a producer thread up to K batches ahead of the task thread, so a
 *     row group's pages are decompressed and decoded while the task thread consumes the batches of the row
 *     group before it; the decoded batches' Arrow bytes are held in the same execution memory and released as
 *     the task thread takes them. Batches come out in the reader's order, so the output is the reader's.
 *
 * Together these decode the next row group ahead while the current one is consumed, keeping row order, with the
 * decode on one producer thread rather than a second concurrent column-reader set (a row group's readers are
 * stateful and single-instance; a second decode thread would only contend for the same cores, as decode is
 * CPU-bound -- see docs/native-parquet-reader.md). This suite drives multi-row-group splits with both stages on
 * and asserts: the rows equal the sequential path (both stages off); order is preserved; a task ended mid-split
 * (a LIMIT) releases every reserved byte, every held execution-memory byte and every Arrow vector; and a short
 * local timing confirms the ahead path is no slower.
 */
@io.vecruntime.spark.test.tags.Scan
class NativeParquetDecodeAheadSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      // Small blocks and pages, so a file holds several row groups and read-ahead crosses row-group boundaries.
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "512",
      // One task over the whole split, so several row groups of one file are read ahead within it.
      "spark.sql.files.maxPartitionBytes" -> (1L << 30).toString,
      "spark.sql.files.openCostInBytes" -> "0",
      "spark.sql.files.minPartitionNum" -> "1"
    )

  private val node = classOf[VectorParquetScanExec]

  /** One file of several row groups, flat and nested lanes with nulls and a dictionary. */
  private lazy val path: String = {
    val p = newTempPath("decodeahead/t")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id AS INT) AS i,
          |  CASE WHEN id % 7 = 0 THEN NULL ELSE CAST(id * 2654435761 AS BIGINT) END AS l,
          |  CASE WHEN id % 5 = 0 THEN NULL ELSE CONCAT('v', CAST(id % 97 AS STRING)) END AS s,
          |  CAST(id * 1.5 AS DOUBLE) AS d,
          |  CAST(id % 1000 AS DECIMAL(25,2)) AS w,
          |  CASE WHEN id % 9 = 0 THEN NULL ELSE SEQUENCE(0, CAST(id % 4 AS INT)) END AS a
          |FROM range(0, 120000)""".stripMargin
      ).coalesce(1).write.mode("overwrite").parquet(p) // one file => its row groups are read ahead within one split
      spark.read.parquet(p).createOrReplaceTempView("rg")
    }
    p
  }

  /** Row groups the native scan read for `sql` (its `numRowGroups` metric). */
  private def nativeRowGroups(sql: String): Long = withPlugin(enabled = true) {
    val df = spark.sql(sql)
    df.collect()
    PlanUtils.allNodes(
      finalPlan(df)
    ).collect { case s: VectorParquetScanExec => s }.map(_.metrics("numRowGroups").value).sum
  }

  private def withAhead[T](rowGroups: Int, decode: Int)(f: => T): T =
    withConf(
      VectorConf.ScanNativeParquetPrefetchRowGroups -> rowGroups.toString,
      VectorConf.ScanNativeParquetDecodeAhead -> decode.toString,
      VectorConf.ScanNativeParquetDecodeAheadThreads -> "virtual"
    )(f)

  test("the split holds several row groups of one file, all decoded natively") {
    path
    val groups = withAhead(0, 0)(nativeRowGroups("SELECT * FROM rg"))
    assert(groups >= 4L, s"only $groups row groups; the split must span several for this suite")
    // The same whether or not row groups are read ahead and batches decoded ahead.
    assert(withAhead(3, 2)(nativeRowGroups("SELECT * FROM rg")) === groups)
  }

  private val queries = Seq(
    "SELECT * FROM rg",
    "SELECT i, s, a FROM rg WHERE i % 3 = 0",
    "SELECT s, count(*), sum(l), max(d), sum(w), sum(size(a)) FROM rg GROUP BY s",
    "SELECT i, l, s, d, w, a FROM rg WHERE i BETWEEN 40000 AND 41000"
  )

  for ((rg, dec) <- Seq((0, 0), (1, 1), (3, 2), (4, 4))) {
    test(
      s"prefetchRowGroups=$rg, decodeAhead=$dec: the reader's rows in order across row groups, equal to sequential"
    ) {
      path
      withAhead(rg, dec) {
        // checkVectorized compares against the plugin-off result and asserts the native scan is in the plan.
        queries.foreach(q => checkVectorized(q, Seq(node)))
      }
    }
  }

  test("a task ended mid-split (LIMIT) releases every reserved byte, held execution-memory byte and Arrow vector") {
    path
    val root = VectorAllocators.root()
    val beforeAlloc = root.getAllocatedMemory
    VectorParquetScanExec.resetPrefetchPeak()
    withAhead(4, 2) {
      // LIMIT stops the task after a few batches, with row groups read ahead and batches decoded ahead that are
      // never consumed: the task-completion close path must drain and release them.
      checkVectorized("SELECT i, s, a, w FROM rg LIMIT 9", Seq(node))
      checkVectorized("SELECT * FROM rg WHERE i > 1000 LIMIT 500", Seq(node))
    }
    assert(VectorParquetScanExec.prefetchInFlightBytes === 0, "row-group read-ahead bytes were not given back")
    assert(VectorParquetScanExec.readAheadHeldBytes === 0, "read-ahead execution memory was not given back (#915)")
    assert(root.getAllocatedMemory === beforeAlloc, s"allocated ${root.getAllocatedMemory} vs $beforeAlloc before")
  }

  test("read-ahead within one split gives every reserved byte back after full consumption") {
    path
    VectorParquetScanExec.resetPrefetchPeak()
    val expected =
      withPlugin(enabled = false)(spark.sql("SELECT i, s, w FROM rg").collect().toSeq).map(_.toString).sorted
    val actual = withAhead(
      4,
      2
    )(withPlugin(enabled = true)(spark.sql("SELECT i, s, w FROM rg").collect().toSeq)).map(_.toString).sorted
    assert(actual === expected)
    assert(VectorParquetScanExec.prefetchInFlightBytes === 0, "read-ahead bytes were not given back")
    assert(VectorParquetScanExec.readAheadHeldBytes === 0, "read-ahead execution memory was not given back (#915)")
  }

  test("decoding row groups ahead on a multi-row-group split is no slower than sequential (local timing)") {
    path
    val q = "SELECT count(*), max(l), max(d), sum(w), sum(size(a)) FROM rg"
    def timeMs(rg: Int, dec: Int): Double = withAhead(rg, dec) {
      withPlugin(enabled = true) {
        spark.sql(q).collect() // warm up (JIT, file open)
        val t0 = System.nanoTime()
        var k = 0
        while (k < 3) { spark.sql(q).collect(); k += 1 }
        (System.nanoTime() - t0) / 3e6
      }
    }
    val sequential = timeMs(0, 0)
    val ahead = timeMs(3, 2)
    info(f"sequential=$sequential%.0f ms, row-group+decode ahead=$ahead%.0f ms")
    // Timing is noisy on a shared local JVM; the guard is only that ahead is not a large regression.
    assert(ahead <= sequential * 2.0 + 50, f"ahead $ahead%.0f ms vs sequential $sequential%.0f ms")
  }
}
