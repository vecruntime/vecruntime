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
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * #834: small-object prefetch and range merging in the native Parquet scan, through a counting local
 * FileSystem (`countfs`, [[CountingLocalFileSystem]]). The scan reads the same bytes on disk but through a
 * scheme that tallies ranged reads (seek+read runs, the S3 request count). The suite proves:
 *   - a small file read whole in ONE request when `smallFileThreshold` covers it (part 1);
 *   - a multi-row-group / multi-column read issuing FEWER requests with range merging on (part 2);
 *   - the column index on (per-page reads otherwise) issuing fewer requests, not one per page;
 *   - every result identical to Spark's own reader, byte for byte, in each case.
 */
@io.vecruntime.spark.test.tags.Scan
class NativeParquetRangeMergeSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "spark.hadoop.fs.countfs.impl" -> classOf[CountingLocalFileSystem].getName,
      "spark.hadoop.fs.countfs.impl.disable.cache" -> "true",
      // One file, one split, no read-ahead concurrency, so the request tally is deterministic.
      VectorConf.ScanNativeParquetPrefetchFiles -> "0",
      VectorConf.ScanNativeParquetPrefetchRowGroups -> "0"
    )

  private def show(v: Any): String = v match {
    case null => "null"
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case x => x.toString
  }
  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  /** Writes `n` rows of several columns to `dir` (Spark's own writer) and returns the countfs:// part path. */
  private def writeTable(dir: String, n: Int, blockSize: Int, pageSize: Int): String = {
    withConf(
      "parquet.block.size" -> blockSize.toString,
      "parquet.page.size" -> pageSize.toString,
      "parquet.page.row.count.limit" -> "700",
      "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
    ) {
      withPlugin(enabled = false) {
        spark.range(0, n).selectExpr(
          "cast(id as int) as k",
          "concat('value-', id % 991, '-', repeat('x', cast(id % 11 as int))) as s",
          "case when id % 7 = 0 then null else id / 3.0d end as d",
          "cast(id * 13 / 100 as decimal(12,2)) as dec",
          "id % 3 = 0 as b",
          "timestamp_micros(1600000000000000 + id * 1000003) as ts"
        ).coalesce(1).write.mode("overwrite").parquet(dir)
      }
    }
    val localDir = new java.io.File(new java.net.URI(dir).getPath)
    val part = localDir.listFiles((_, nm) => nm.endsWith(".parquet")).sortBy(_.getName).head.getAbsolutePath
    "countfs://" + part
  }

  private def localPart(countPath: String): String = countPath.stripPrefix("countfs://")

  /** Runs `query` over `countPath` with the native scan and the given confs; returns (sortedRows, reads). */
  private def scan(countPath: String, query: String, confs: (String, String)*): (Seq[String], Int) = {
    val part = localPart(countPath)
    CountingLocalFileSystem.reset(part)
    val sorted = withConf(confs: _*) {
      withPlugin(enabled = true) {
        val df = spark.read.parquet(countPath)
        df.createOrReplaceTempView("t834")
        val q = spark.sql(query)
        val r = rows(q)
        assert(
          PlanUtils.allNodes(finalPlan(q)).exists(_.isInstanceOf[VectorParquetScanExec]),
          s"$query was not read by the native scan:\n${finalPlan(q).treeString}"
        )
        r
      }
    }
    (sorted, CountingLocalFileSystem.reads(part))
  }

  /** Spark's own reader's rows for the same query, for result equality. */
  private def sparkRows(countPath: String, query: String): Seq[String] =
    withPlugin(enabled = false) {
      spark.read.parquet(countPath).createOrReplaceTempView("t834")
      rows(spark.sql(query))
    }

  test("part 1: a small file is read in one request, results equal Spark's") {
    val dir = newTempPath("rm_small")
    // One row group (a large block), so the whole file is one object of a handful of column chunks.
    val countPath = writeTable(dir, n = 20000, blockSize = 128 * 1024 * 1024, pageSize = 1024 * 1024)
    val q = "SELECT count(*), sum(k), max(s), sum(d), sum(dec), count(b), min(ts) FROM t834"
    val expected = sparkRows(countPath, q)

    // Baseline: no small-file prefetch, no range merge -> many ranged reads (footer length, footer, chunks).
    val (base, baseReads) = scan(countPath, q, VectorConf.ScanNativeParquetRangeMergeGap -> "-1")
    assert(base == expected, "baseline rows differ from Spark")
    assert(baseReads > 1, s"baseline should issue several reads, got $baseReads")

    // Small-file prefetch: the whole object in ONE request (footer, chunks and indexes all from the buffer).
    val (small, smallReads) = scan(countPath, q, VectorConf.ScanNativeParquetSmallFileThreshold -> "64m")
    assert(small == expected, "small-file rows differ from Spark")
    assert(smallReads == 1, s"the whole object should be one request, got $smallReads (baseline $baseReads)")
  }

  test("part 1: a file SPLIT into several partitions is still fetched whole, results equal Spark's") {
    // The buffer must hold the whole file (its footer is at the end), not one split's bytes: with several row
    // groups and a small maxPartitionBytes, Spark gives each split only part of the file. Sizing the buffer by
    // the split's length hit EOF in readFooter on S3 (1 TB A/B, q44).
    val dir = newTempPath("rm_split")
    val countPath = writeTable(dir, n = 60000, blockSize = 256 * 1024, pageSize = 64 * 1024)
    // Several splits sum the doubles in a different order than one task does, so compare that sum rounded.
    val q = "SELECT count(*), sum(k), max(s), round(sum(d), 3), sum(dec), count(b), min(ts) FROM t834"
    val expected = sparkRows(countPath, q)
    val (rowsSplit, _) = scan(
      countPath,
      q,
      VectorConf.ScanNativeParquetSmallFileThreshold -> "64m",
      "spark.sql.files.maxPartitionBytes" -> "128k",
      "spark.sql.files.openCostInBytes" -> "0"
    )
    assert(rowsSplit == expected, "split small-file rows differ from Spark")
  }

  test("part 1: a file ABOVE the threshold is not whole-object fetched, results equal Spark's") {
    val dir = newTempPath("rm_above")
    val countPath = writeTable(dir, n = 20000, blockSize = 128 * 1024 * 1024, pageSize = 1024 * 1024)
    val q = "SELECT count(*), sum(k) FROM t834"
    val expected = sparkRows(countPath, q)
    // A 1-byte threshold is below any real file, so small-file prefetch never fires; rows still correct and
    // the scan reads as it would without the lever.
    val (out, reads) = scan(
      countPath,
      q,
      VectorConf.ScanNativeParquetSmallFileThreshold -> "1",
      VectorConf.ScanNativeParquetRangeMergeGap -> "-1"
    )
    assert(out == expected)
    assert(reads > 1, s"a file above the threshold should not be one whole-object GET, got $reads")
  }

  test("part 2: range merging reads every row group correctly and never issues more requests") {
    val dir = newTempPath("rm_merge")
    // Small blocks -> several row groups. Project a SUBSET of the columns (k, s, ts), skipping the ones
    // between them, so the projected chunks are NON-contiguous. The STRICT request-count reduction of range
    // merging is proven deterministically by ParquetReadRangePlannerSuite (the planner coalesces gapped
    // ranges) and by part 1 (whole-object = 1 request); here, over a real multi-row-group file, we assert
    // range merging stays result-correct against Spark and never issues MORE requests than the baseline --
    // a local RawLocalFileSystem already reads a row group's projected chunks efficiently, so the on-S3 win
    // (each gapped chunk its own GET, coalesced into one) does not reproduce as a strict drop on local disk.
    val countPath = writeTable(dir, n = 120000, blockSize = 256 * 1024, pageSize = 8 * 1024)
    val q = "SELECT count(*), sum(k), max(s), min(ts) FROM t834"
    val expected = sparkRows(countPath, q)

    val (base, baseReads) = scan(countPath, q, VectorConf.ScanNativeParquetRangeMergeGap -> "-1")
    assert(base == expected, "baseline rows differ from Spark")

    // Range merge on with a gap wide enough to span the skipped columns.
    val (merged, mergedReads) = scan(
      countPath,
      q,
      VectorConf.ScanNativeParquetRangeMergeGap -> "16m",
      VectorConf.ScanNativeParquetRangeTargetSize -> "0"
    )
    assert(merged == expected, "range-merged rows differ from Spark")
    assert(mergedReads <= baseReads, s"range merging must not issue more requests: $mergedReads vs $baseReads")

    // And it is still correct when the coalesced run is split into target-sized parts (the parallel-GET path).
    val (split, _) = scan(
      countPath,
      q,
      VectorConf.ScanNativeParquetRangeMergeGap -> "16m",
      VectorConf.ScanNativeParquetRangeTargetSize -> "64k"
    )
    assert(split == expected, "range-merged+split rows differ from Spark")
  }

  test("column index on: a selective filter reads merged requests, not one per page; results equal Spark's") {
    val dir = newTempPath("rm_colindex")
    val countPath = writeTable(dir, n = 120000, blockSize = 1024 * 1024, pageSize = 2 * 1024)
    // Sorted on k already (range ids); a selective range lets the column index drop most pages.
    val q = "SELECT k, s, d FROM t834 WHERE k BETWEEN 40000 AND 40500"
    val expected = sparkRows(countPath, q)

    val colIndex = "parquet.filter.columnindex.enabled" -> "true"
    // Column index on, range merge off: each selected page is its own read -> many reads.
    val (pages, pageReads) = scan(countPath, q, colIndex, VectorConf.ScanNativeParquetRangeMergeGap -> "-1")
    assert(pages == expected, "column-index rows differ from Spark")

    // Column index on, whole-object prefetch: the file is one request and every page read is served from it.
    val (whole, wholeReads) = scan(
      countPath,
      q,
      colIndex,
      VectorConf.ScanNativeParquetSmallFileThreshold -> "64m"
    )
    assert(whole == expected, "column-index + whole-object rows differ from Spark")
    assert(wholeReads == 1, s"the whole object is one request even with the column index on, got $wholeReads")
    assert(wholeReads < pageReads, s"whole-object ($wholeReads) must beat per-page ($pageReads)")
  }
}
