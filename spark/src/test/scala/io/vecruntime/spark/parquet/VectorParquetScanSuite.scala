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
package io.vecruntime.spark.parquet

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.arrow.VectorAllocators
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorBroadcastBatches, VectorParquetScanExec}

/**
 * Compares our native Parquet scan ([[VectorParquetScanExec]]) with Spark's own reader row for row, and
 * pins the recorded fallback reasons. The plugin-off run uses Spark's reader (the flag does nothing without
 * the plugin), the plugin-on run uses our node -- so `checkVectorized` asserts identical results AND that
 * the node is in the plan. Files are written with small block/page sizes so the 4 flag-on tables span
 * several row groups and pages; each supported type is covered, with nulls and dictionary vs plain columns,
 * plus partition columns, a pushed filter that prunes row groups, and a dynamic-partition-pruning query.
 */
class VectorParquetScanSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      // Small blocks/pages so a table spans several row groups and pages.
      "parquet.block.size" -> (128 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024"
    )

  private val node = classOf[VectorParquetScanExec]

  private def writeTable(name: String, sql: String): Unit = {
    val path = newTempPath(name)
    withPlugin(enabled = false) {
      spark.sql(sql).write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView(name)
    }
  }

  test("all supported types, dictionary and plain, with nulls, several row groups") {
    // i32 (dict), i64 (plain, high card), f64 (plain), dec7 (int32-physical), dec15 (int64-physical),
    // dt (date), s (dict string), each ~15% null; 20000 rows over small blocks => many row groups.
    writeTable(
      "t_types",
      """SELECT
        |  CAST(id AS INT) AS i32,
        |  CAST(id * 2654435761 AS BIGINT) AS i64,
        |  CAST(id * 1.5 AS DOUBLE) AS f64,
        |  CAST((id % 100000) / 100.0 AS DECIMAL(7,2)) AS dec7,
        |  CAST(id AS DECIMAL(15,2)) AS dec15,
        |  DATE_ADD(DATE'2000-01-01', CAST(id % 3000 AS INT)) AS dt,
        |  CASE WHEN id % 7 = 0 THEN NULL ELSE CONCAT('v', CAST(id % 50 AS STRING)) END AS s
        |FROM range(0, 20000)""".stripMargin
    )
    // Introduce nulls in the numeric columns via a second view (range() has no nulls).
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CASE WHEN i32 % 11 = 0 THEN NULL ELSE i32 END AS i32,
          |  CASE WHEN i32 % 13 = 0 THEN NULL ELSE i64 END AS i64,
          |  CASE WHEN i32 % 5 = 0 THEN NULL ELSE f64 END AS f64,
          |  CASE WHEN i32 % 9 = 0 THEN NULL ELSE dec7 END AS dec7,
          |  CASE WHEN i32 % 17 = 0 THEN NULL ELSE dec15 END AS dec15,
          |  dt, s
          |FROM t_types""".stripMargin
      ).write.mode("overwrite").parquet(newTempPath("t_types_nulls"))
      spark.read.parquet(newTempPath("t_types_nulls")).createOrReplaceTempView("t_types_nulls")
    }
    checkVectorized("SELECT * FROM t_types_nulls", Seq(node))
    checkVectorized("SELECT i32, s FROM t_types_nulls", Seq(node)) // projection prunes columns
    checkVectorized("SELECT dec7, dec15, dt FROM t_types_nulls", Seq(node))
  }

  test("page v2 (writer version PARQUET_2_0)") {
    val path = newTempPath("t_v2")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CONCAT('s', CAST(id % 40 AS STRING)) AS s FROM range(0, 8000)")
        .write.option("parquet.writer.version", "v2").mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_v2")
    }
    checkVectorized("SELECT * FROM t_v2", Seq(node))
  }

  test("partition columns are read as constant columns") {
    val path = newTempPath("t_part")
    withPlugin(enabled = false) {
      spark.sql(
        "SELECT CAST(id AS INT) AS v, CAST(id % 4 AS INT) AS p FROM range(0, 12000)"
      ).write.partitionBy("p").mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_part")
    }
    checkVectorized("SELECT v, p FROM t_part", Seq(node))
    checkVectorized("SELECT v FROM t_part WHERE p = 2", Seq(node)) // partition filter + partition column
  }

  test("a pushed data filter prunes row groups") {
    val path = newTempPath("t_filter")
    withPlugin(enabled = false) {
      // Sorted so row-group min/max stats are tight and the filter skips whole row groups.
      spark.sql("SELECT CAST(id AS INT) AS k, CAST(id AS BIGINT) AS v FROM range(0, 50000)")
        .sort("k").write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_filter")
    }
    checkVectorized("SELECT k, v FROM t_filter WHERE k > 49000", Seq(node))
    checkVectorized("SELECT count(*) AS c, sum(v) AS s FROM t_filter WHERE k BETWEEN 100 AND 200", Seq(node))
  }

  test("dynamic partition pruning query keeps the node on the pruned scan") {
    val factPath = newTempPath("dpp_fact")
    val dimPath = newTempPath("dpp_dim")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS v, CAST(id % 10 AS INT) AS pk FROM range(0, 40000)")
        .write.partitionBy("pk").mode("overwrite").parquet(factPath)
      spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact")
      spark.sql("SELECT CAST(id AS INT) AS pk, CONCAT('d', CAST(id AS STRING)) AS label FROM range(0, 10)")
        .write.mode("overwrite").parquet(dimPath)
      spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim")
    }
    withConf(
      "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> (10 * 1024 * 1024).toString
    ) {
      // The dim filter prunes the fact partitions at runtime; the fact scan is our node.
      checkVectorized(
        "SELECT f.v FROM dpp_fact f JOIN dpp_dim d ON f.pk = d.pk WHERE d.label = 'd3'",
        Seq(node)
      )
    }
  }

  test("empty result and all-null column still match Spark") {
    val path = newTempPath("t_edge")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS k, CAST(NULL AS STRING) AS allnull FROM range(0, 5000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_edge")
    }
    checkVectorized("SELECT k, allnull FROM t_edge WHERE k < 0", Seq(node)) // empty
    checkVectorized("SELECT allnull FROM t_edge", Seq(node)) // all-null column
  }

  test("tiny table with nulls (conditional-functions.sql regression): nanvl over c1 double, c2 int") {
    val path = newTempPath("t_tiny")
    withPlugin(enabled = false) {
      spark.sql(
        "SELECT c1, c2 FROM VALUES(1d, 0),(2d, 1),(CAST(NULL AS DOUBLE), 1),(CAST('NaN' AS DOUBLE), 0) AS t(c1, c2)"
      )
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_tiny")
    }
    checkVectorized("SELECT c1, c2 FROM t_tiny", Seq(node))
    checkVectorized("SELECT nanvl(c2, c1/c2 + c1/c2) FROM t_tiny", Seq(node))
  }

  test("fallback: a boolean column is not decoded in slice 1 (shared support source of truth)") {
    val path = newTempPath("t_bool")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, (id % 2 = 0) AS b FROM range(0, 2000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_bool")
    }
    checkFallback("SELECT i, b FROM t_bool", Seq(node), reasonContains = "unsupported column type")
  }

  test("fallback: a timestamp column is not decoded in slice 1") {
    val path = newTempPath("t_ts")
    withPlugin(enabled = false) {
      spark.sql(
        "SELECT CAST(id AS INT) AS i, TIMESTAMP'2020-01-01 00:00:00' + MAKE_INTERVAL(0,0,0,0,0,0,id) AS ts FROM range(0, 1000)"
      )
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_ts")
    }
    checkFallback("SELECT i, ts FROM t_ts", Seq(node), reasonContains = "unsupported column type")
  }

  test("early termination (LIMIT) does not leak the reused vectors") {
    // A file large enough to span several row groups; a small LIMIT abandons the iterator mid-file, so the
    // task-completion listener must close the reused vectors and the allocator exactly once (no Arrow leak).
    val path = newTempPath("t_limit")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CONCAT('s', CAST(id % 100 AS STRING)) AS s FROM range(0, 60000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_limit")
    }
    checkVectorized("SELECT i, s FROM t_limit LIMIT 10", Seq(node))
    checkVectorized("SELECT i FROM t_limit WHERE i >= 0 LIMIT 5000", Seq(node))
  }

  test("scan -> filter(IsNotNull) -> project over nulls does NOT take the adapter copy path") {
    val path = newTempPath("t_nocopy")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CASE WHEN id % 4 = 0 THEN NULL ELSE CAST(id AS INT) END AS i,
          |  CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id * 3 AS BIGINT) END AS l,
          |  CASE WHEN id % 6 = 0 THEN NULL ELSE CAST(id * 1.5 AS DOUBLE) END AS d,
          |  CASE WHEN id % 7 = 0 THEN NULL ELSE CONCAT('s', CAST(id % 30 AS STRING)) END AS s
          |FROM range(0, 30000)""".stripMargin
      )
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_nocopy")
    }
    // The scan feeds our filter and project directly; the adapter must take every column zero-copy
    // (SparkColumnVectorBuffers.copy would bulk-read Arrow null slots and throw). Assert the copy counter
    // did not advance while our operators consumed the node's batches.
    val before = io.vecruntime.spark.adapter.ColumnVectorAdapters.copiedColumns()
    checkVectorized(
      "SELECT i, l, d, s FROM t_nocopy WHERE i IS NOT NULL AND s IS NOT NULL",
      Seq(node, classOf[org.apache.spark.sql.vecruntime.VectorFilterExec])
    )
    val after = io.vecruntime.spark.adapter.ColumnVectorAdapters.copiedColumns()
    assert(
      after == before,
      s"the adapter copy path was taken ${after - before} times; the scan's batches must adapt zero-copy"
    )
  }

  test("Spark consumer (ColumnarToRow) over the node's batches with nulls, per type") {
    val path = newTempPath("t_sparkcons")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CASE WHEN id % 3 = 0 THEN NULL ELSE CAST(id AS INT) END AS i,
          |  CASE WHEN id % 4 = 0 THEN NULL ELSE CAST(id AS BIGINT) END AS l,
          |  CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS DOUBLE) END AS d,
          |  CASE WHEN id % 6 = 0 THEN NULL ELSE CAST((id % 100000)/100.0 AS DECIMAL(9,2)) END AS dec,
          |  CASE WHEN id % 7 = 0 THEN NULL ELSE DATE_ADD(DATE'2001-01-01', CAST(id % 500 AS INT)) END AS dt,
          |  CASE WHEN id % 8 = 0 THEN NULL ELSE CONCAT('v', CAST(id % 40 AS STRING)) END AS s
          |FROM range(0, 20000)""".stripMargin
      )
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_sparkcons")
    }
    // A bare SELECT * with no operator of ours above the scan: the node's batches go straight to
    // ColumnarToRowExec (a Spark consumer), which must read null slots without Arrow's get() throwing.
    checkVectorized("SELECT i, l, d, dec, dt, s FROM t_sparkcons", Seq(node))
  }

  test("a scalar-subquery partition filter is awaited (ReusedSubquery has-not-finished regression)") {
    // The scan's partition pruning uses a scalar subquery in the partition predicate. VectorParquetScanExec
    // wraps the scan and reads its selected partitions directly, so it must WAIT for the scan's subqueries
    // (not just DPP) -- else the subquery is "started but not finished" and reading the partitions throws an
    // IllegalArgumentException that awaitResult wraps in a null-condition SparkException (which broke a
    // SQL-scripting golden case). Two forms: an equality on a scalar subquery, and IN a scalar subquery.
    val factPath = newTempPath("sub_fact")
    val dimPath = newTempPath("sub_dim")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS v, CAST(id % 8 AS INT) AS pk FROM range(0, 40000)")
        .write.partitionBy("pk").mode("overwrite").parquet(factPath)
      spark.read.parquet(factPath).createOrReplaceTempView("sub_fact")
      spark.sql("SELECT CAST(id AS INT) AS k FROM range(3, 5)").write.mode("overwrite").parquet(dimPath)
      spark.read.parquet(dimPath).createOrReplaceTempView("sub_dim")
    }
    withConf("spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "false") {
      checkVectorized("SELECT v FROM sub_fact WHERE pk = (SELECT MIN(k) FROM sub_dim)", Seq(node))
      checkVectorized("SELECT v FROM sub_fact WHERE pk IN (SELECT k FROM sub_dim)", Seq(node))
    }
  }

  test("a file split into several PartitionedFiles reads each row group once (no duplicates)") {
    // Write ONE parquet file with many small row groups, then force Spark to split it into >= 2
    // PartitionedFiles (small maxPartitionBytes). Each split must read only the row groups whose midpoint
    // falls in its byte range; without the range, every split reads the whole file -> duplicate rows.
    val path = newTempPath("t_split")
    withConf("parquet.block.size" -> (64 * 1024).toString, "parquet.page.size" -> (4 * 1024).toString) {
      withPlugin(enabled = false) {
        spark.sql("SELECT CAST(id AS INT) AS i, CAST(id * 7 AS BIGINT) AS l FROM range(0, 200000)")
          .coalesce(1).write.mode("overwrite").parquet(path)
        spark.read.parquet(path).createOrReplaceTempView("t_split")
      }
    }
    withConf("spark.sql.files.maxPartitionBytes" -> (256 * 1024).toString) {
      // Confirm the read actually splits (more than one partition), else the test proves nothing.
      val parts = withPlugin(enabled = true)(spark.sql("SELECT i FROM t_split").rdd.getNumPartitions)
      assert(parts >= 2, s"expected the file to split into >= 2 partitions, got $parts")
      checkVectorized("SELECT count(*) AS c FROM t_split", Seq(node))
      checkVectorized("SELECT count(*) AS c, sum(l) AS s, min(i) AS mn, max(i) AS mx FROM t_split", Seq(node))
      // Row-for-row (ordered) equality across splits, flag on vs off.
      checkVectorized("SELECT i, l FROM t_split ORDER BY i", Seq(node))
    }
  }

  test("a SlicedColumnVector over a large row-group vector adapts and serializes to only its slice rows") {
    // Regression for the 1 TB driver OOM: a batch is a SlicedColumnVector over a reused row-group vector of
    // (say) 1,000,000 rows; when adapted/serialized only the slice's rows must travel, not the whole vector.
    val allocator = VectorAllocators.newChild("slice-ser")
    try {
      val rgRows = 1000000
      val v = new org.apache.arrow.vector.IntVector("i", allocator)
      v.allocateNew(rgRows)
      var k = 0
      while (k < rgRows) { v.set(k, k); k += 1 }
      v.setValueCount(rgRows)
      val whole = new io.vecruntime.spark.arrow.VectorArrowColumnVector(v)
      val batchN = 4096
      val sliced = io.vecruntime.spark.arrow.SlicedColumnVector.of(whole, 512000, batchN, rgRows)
      val arena = java.lang.foreign.Arena.ofConfined()
      try {
        val vb = io.vecruntime.spark.adapter.ColumnVectorAdapters.adapt(sliced, batchN, arena)
        assert(vb.length() == batchN, s"adapted length ${vb.length()} should be the slice's $batchN, not $rgRows")
        assert(vb.getInt(0) == 512000 && vb.getInt(batchN - 1) == 512000 + batchN - 1, "slice reads the right rows")
        val batch = new org.apache.spark.sql.vectorized.ColumnarBatch(Array[ColumnVector](sliced), batchN)
        val (rows, bytes) = VectorBroadcastBatches.write(
          Iterator.single(batch),
          Array("i"),
          Array[org.apache.spark.sql.types.DataType](org.apache.spark.sql.types.IntegerType)
        )
        assert(rows == batchN, s"serialized $rows rows, expected $batchN")
        assert(
          bytes.length <= batchN.toLong * 8 + 8192,
          s"serialized ${bytes.length} bytes for $batchN rows: the row group leaked"
        )
      } finally arena.close()
      v.close()
    } finally {
      allocator.close()
    }
  }

  test("broadcast hash join with our multi-row-group scan as the build side: correct rows, bounded bytes") {
    // q24a shape in miniature: a large fact scanned by our node joined to a small dimension broadcast. The
    // build side (dimension) is our scan of a multi-row-group table; the broadcast relation must ship only
    // the dimension's rows, not a row group per emitted batch (the 1 TB driver OOM). Owned-buffer batches
    // guarantee that.
    val factPath = newTempPath("bj_fact")
    val dimPath = newTempPath("bj_dim")
    withConf("parquet.block.size" -> (1 << 20).toString, "parquet.page.size" -> (16 * 1024).toString) {
      withPlugin(enabled = false) {
        spark.sql(
          "SELECT CAST(id AS INT) AS f_id, CAST(id % 500 AS INT) AS d_id, CAST(id AS BIGINT) AS amt FROM range(0, 300000)"
        )
          .coalesce(1).write.mode("overwrite").parquet(factPath)
        spark.read.parquet(factPath).createOrReplaceTempView("bj_fact")
        // A dimension of 500 rows written into many small row groups (so a per-row-group broadcast would blow up).
        spark.sql("SELECT CAST(id AS INT) AS d_id, CONCAT('name', CAST(id AS STRING)) AS d_name FROM range(0, 500)")
          .coalesce(1).write.mode("overwrite").parquet(dimPath)
        spark.read.parquet(dimPath).createOrReplaceTempView("bj_dim")
      }
    }
    withConf(
      VectorConf.ScanNativeParquet -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> (10 * 1024 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "4096"
    ) {
      checkVectorized(
        "SELECT d.d_name, count(*) AS c, sum(f.amt) AS s FROM bj_fact f JOIN bj_dim d ON f.d_id = d.d_id " +
          "GROUP BY d.d_name HAVING sum(f.amt) > (SELECT min(amt) FROM bj_fact) ORDER BY d.d_name",
        Seq(node)
      )
    }
  }

  // ---------------------------------------------------------------- fallbacks

  test("fallback: a nested (struct) column keeps Spark's scan with a recorded reason") {
    val path = newTempPath("t_nested")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, named_struct('a', id, 'b', CAST(id AS STRING)) AS st FROM range(0, 1000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_nested")
    }
    checkFallback("SELECT i, st FROM t_nested", Seq(node), reasonContains = "nested or complex column")
  }

  test("fallback: a binary column is not a supported lane in slice 1") {
    val path = newTempPath("t_bin")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CAST(CONCAT('b', CAST(id AS STRING)) AS BINARY) AS b FROM range(0, 1000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_bin")
    }
    checkFallback("SELECT i, b FROM t_bin", Seq(node), reasonContains = "unsupported column type")
  }
}
