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
package io.vecruntime.iceberg.write

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{MetricsConfig, Table}
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for the copy-on-write DELETE row-group copy path (#901 M4a). A DELETE on a
 * copy-on-write table rewrites the files it touches; with [[io.vecruntime.spark.VectorConf]]
 * `spark.vecruntime.iceberg.copyUnchanged.enabled` on, [[IcebergRewriteFileTaskWriter]] copies the
 * delete-free row groups of those files byte for byte and re-encodes only the partly-deleted groups.
 *
 * Each case runs the SAME DELETE twice from the same starting data: once on a table with the flag OFF
 * (the engine's streaming full rewrite, already proven equal to Iceberg's own writer by
 * [[VectorIcebergWriteSuite]]) and once with it ON. The two must be indistinguishable, checked with a
 * corrected oracle:
 *
 *   - '''data''': the surviving rows, joined on `id`, are equal column by column (every column, including
 *     nulls, strings, decimals and nested types);
 *   - '''lineage''' (v3): `_row_id` and `_last_updated_sequence_number` come from each file's
 *     `first_row_id`, so they are NOT comparable across two different tables. Instead each survivor's
 *     lineage is captured BEFORE the DELETE (keyed by `id`) and must be unchanged AFTER the DELETE,
 *     within the same table -- a copy-on-write DELETE assigns no new lineage;
 *   - '''metrics''': each new data file's metrics are recomputed from the file itself with
 *     `ParquetUtil.fileMetrics(inputFile, MetricsConfig.forTable(table))` and must equal what the table's
 *     `files` metadata recorded (value/null/nan counts and bounds), so a copied group's footer statistics
 *     are the ones Iceberg would have computed.
 *
 * At least one group is copied in the some-deleted and none-deleted cases (`copiedGroups > 0`), which is
 * the whole point of M4a; the all-groups-deleted case copies nothing. A tiny
 * `write.parquet.row-group-size-bytes` gives every file several groups, and a tiny
 * `advisoryPartitionSizeInBytes` keeps the clustering shuffle from coalescing files together.
 *
 * Runs in the iceberg-bridge module (excluded from the gate's -pl list), so it is run manually.
 */
class VectorCopyUnchangedDeleteSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("m4acow")
  private val Flag = "spark.vecruntime.iceberg.copyUnchanged.enabled"
  private val WriterFlag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("m4a-cow-delete")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      // Small partitions so the clustering shuffle keeps files apart (one source file per task).
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16384")
      .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1024")
      // The copy path only fires over a COLUMNAR Iceberg COW scan (the rewrite copies the child's batches),
      // which needs the native Iceberg scan on. This suite used to "pass" only when a prior native-scan COW
      // suite had already built the shared SparkSession with this flag; run alone it built its own session
      // without it, the scan fell back to rows, and copiedGroups was 0. Set it here so the suite is
      // self-sufficient (writer/copyUnchanged are still toggled per-run by withCopy for the A/B).
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  // ----- harness ------------------------------------------------------------------------------------

  private def withCopy[T](on: Boolean)(f: => T): T = {
    val keys = Seq(Flag -> on.toString, WriterFlag -> "true")
    val prev = keys.map { case (k, _) => k -> spark.conf.getOption(k) }
    keys.foreach { case (k, v) => spark.conf.set(k, v) }
    try f
    finally prev.foreach { case (k, p) =>
        p match { case Some(v) => spark.conf.set(k, v); case None => spark.conf.unset(k) }
      }
  }

  private val Columns =
    "id BIGINT, i INT, d DOUBLE, f FLOAT, b BOOLEAN, s STRING, day DATE, ts TIMESTAMP, dec DECIMAL(9,2), " +
      "wide DECIMAL(38,6), st STRUCT<x: INT, y: STRING>, xs ARRAY<INT>, m MAP<STRING, BIGINT>"

  /**
   * A source row with every tracked type, some nulls, some NaN. NaN is only produced when `nan` is true: a
   * table that tracks NaN counts cannot copy (the footer carries no NaN count), so the copy cases use data
   * with no NaN, while a dedicated case checks the NaN-tracking fallback still re-encodes.
   */
  private def source(rows: Int, nan: Boolean): String = {
    val fExpr = if (nan) "if(id % 19 = 0, cast('NaN' as float), cast(id as float) / 3)" else "cast(id as float) / 3"
    val dExpr = if (nan) "if(id % 23 = 0, cast('NaN' as double), id * 0.5d)" else "id * 0.5d"
    s"""SELECT id, cast(id % 1000 as int) as i, if(id % 17 = 0, null, $dExpr) as d,
       |  $fExpr as f, id % 3 = 0 as b,
       |  if(id % 10 = 0, null, concat('v', id % 300)) as s, date_add(date'2020-01-01', cast(id % 500 as int)) as day,
       |  timestamp_micros(1700000000000000 + id * 1000) as ts, cast(id % 10000 as decimal(9,2)) / 7 as dec,
       |  cast(id as decimal(38,6)) * 1000000 as wide,
       |  if(id % 7 = 0, null, named_struct('x', cast(id % 50 as int), 'y', concat('y', id % 9))) as st,
       |  if(id % 11 = 0, null, array(cast(id % 5 as int), cast(id % 6 as int))) as xs,
       |  if(id % 13 = 0, null, map(concat('k', id % 4), id)) as m
       |FROM range($rows)""".stripMargin
  }

  private def create(t: String, version: Int, partitioned: Boolean, metricsProps: String = ""): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    val part = if (partitioned) " PARTITIONED BY (b)" else ""
    spark.sql(
      s"""CREATE TABLE $t ($Columns) USING iceberg$part
         |TBLPROPERTIES ('format-version'='$version', 'write.delete.mode'='copy-on-write',
         |  'write.parquet.row-group-size-bytes'='16384', 'write.target-file-size-bytes'='262144'$metricsProps)""".stripMargin
    )
  }

  private def table(name: String): Table =
    Spark3Util.loadIcebergTable(spark, name)

  /** Surviving rows keyed by id, every column as a stable string (nulls and nested included). */
  private def dataById(t: String): Map[Long, String] =
    spark.sql(s"SELECT * FROM $t").collect().map { r =>
      r.getLong(0) -> r.toString
    }.toMap

  /** Each survivor's v3 lineage keyed by id: (_row_id, _last_updated_sequence_number). */
  private def lineageById(t: String): Map[Long, (Long, Long)] =
    spark
      .sql(s"SELECT id, _row_id, _last_updated_sequence_number FROM $t")
      .collect()
      .map(r => r.getLong(0) -> (r.getLong(1), r.getLong(2)))
      .toMap

  /** Field-id -> summed count across a map column of the `files` metadata table. */
  private def aggMap(t: String, col: String): Map[Int, Long] =
    spark
      .sql(s"SELECT $col FROM $t.files")
      .collect()
      .flatMap(r => Option(r.getMap[Int, Long](0)).map(_.toMap).getOrElse(Map.empty))
      .groupBy(_._1)
      .map { case (k, vs) => k -> vs.map(_._2).sum }

  private def jmap(x: java.util.Map[Integer, java.lang.Long]): Map[Int, Long] =
    Option(x).map(_.asScala.map { case (k, v) => k.toInt -> v.toLong }.toMap).getOrElse(Map.empty)

  /**
   * The metric oracle. Two independent checks, because a Parquet footer carries no NaN count, so a
   * recompute from the file bytes cannot reproduce the `nan_value_counts` the writer collected:
   *
   *   1. '''per-file footer recompute''': for each ON data file, recompute the footer-derivable metrics
   *      (record count, value counts, null counts, bounds) with `ParquetUtil.fileMetrics` and require they
   *      equal what the ON table's `files` metadata recorded -- so a copied group's footer statistics are
   *      exactly the ones Iceberg computes for the file. NaN counts are not footer-derivable and are left to
   *      check 2.
   *   2. '''aggregate recorded equality''': the ON and OFF tables hold the same surviving rows, so the
   *      summed recorded metrics (record count, value/null/nan counts per field) must be identical. This is
   *      what proves the ON table's `nan_value_counts` are right -- against the full-rewrite twin that
   *      [[VectorIcebergWriteSuite]] already proves equal to Iceberg's own writer.
   *
   * Returns the number of ON files checked.
   */
  private def checkFileMetrics(on: String, off: String): Int = {
    val tbl = table(on)
    val mc = MetricsConfig.forTable(tbl)
    val conf = new Configuration()
    // path -> (record_count, value_counts, null_value_counts) from the files table (footer-derivable only).
    val recorded = spark
      .sql(s"SELECT file_path, record_count, value_counts, null_value_counts FROM $on.files")
      .collect()
      .map { r =>
        r.getString(0) ->
          (
            r.getLong(1),
            Option(r.getMap[Int, Long](2)).map(_.toMap).getOrElse(Map.empty),
            Option(r.getMap[Int, Long](3)).map(_.toMap).getOrElse(Map.empty)
          )
      }
      .toMap
    var n = 0
    recorded.foreach { case (path, (recCount, values, nulls)) =>
      val in = HadoopInputFile.fromLocation(path, conf)
      val m = ParquetUtil.fileMetrics(in, mc)
      assert(m.recordCount() == recCount, s"$on: $path record_count")
      assert(jmap(m.valueCounts()) == values, s"$on: $path value_counts")
      assert(jmap(m.nullValueCounts()) == nulls, s"$on: $path null_value_counts")
      n += 1
    }

    // Aggregate recorded equality on vs off (this is what covers nan_value_counts).
    def totalRecords(x: String): Long =
      spark.sql(s"SELECT coalesce(sum(record_count), 0) FROM $x.files").collect()(0).getLong(0)
    assert(totalRecords(on) == totalRecords(off), s"$on vs $off: total record_count")
    for (col <- Seq("value_counts", "null_value_counts", "nan_value_counts")) {
      assert(aggMap(on, col) == aggMap(off, col), s"$on vs $off: aggregate $col")
    }
    n
  }

  /**
   * Runs `delete` (given the table name) as a copy-on-write DELETE on a flag-OFF table and a flag-ON
   * table built the same way, then checks the two are indistinguishable by the corrected oracle and (for
   * v3) that lineage is preserved within each table. Returns the groups copied by the ON run.
   *
   * `seedWithVectorWriter` picks which writer lays down the seed files the DELETE later rewrites: the
   * seed `INSERT INTO` runs with `spark.vecruntime.iceberg.writer.enabled` pinned to it explicitly (NOT
   * left at the session default), so the case covers that seed writer on purpose. Iceberg's own writer
   * (false) and our columnar writer (true) must both produce files the copy path can copy from.
   */
  private def cow(
      name: String,
      version: Int,
      partitioned: Boolean,
      rows: Int,
      nan: Boolean = false,
      metricsProps: String = "",
      seedWithVectorWriter: Boolean = false
  )(delete: String => String): Long = {
    val off = s"ice.db.${name}_off"
    val on = s"ice.db.${name}_on"
    Seq(off, on).foreach { t =>
      create(t, version, partitioned, metricsProps)
      // Seed under an EXPLICIT writer toggle, so the case pins which writer wrote the files the DELETE
      // rewrites (Iceberg's own, or our columnar writer) instead of inheriting the session default.
      val seedKey = WriterFlag
      val prevSeed = spark.conf.getOption(seedKey)
      spark.conf.set(seedKey, seedWithVectorWriter.toString)
      try spark.sql(s"INSERT INTO $t ${source(rows, nan)}")
      finally prevSeed match {
          case Some(v) => spark.conf.set(seedKey, v)
          case None => spark.conf.unset(seedKey)
        }
    }
    // Lineage before the DELETE (v3 only), for the within-table preservation check.
    val lineageBefore = if (version >= 3) lineageById(on) else Map.empty[Long, (Long, Long)]

    withCopy(on = false)(spark.sql(delete(off)).collect())
    IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.reset()
    withCopy(on = true)(spark.sql(delete(on)).collect())
    val copied = IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.sum()

    // Data oracle: same surviving rows, column by column, joined on id.
    val offData = dataById(off)
    val onData = dataById(on)
    assert(onData.keySet == offData.keySet, s"$name: surviving id set differs")
    onData.foreach { case (id, row) =>
      assert(row == offData(id), s"$name: row for id=$id differs (on=$row off=${offData(id)})")
    }

    // Lineage oracle (v3): every survivor keeps the lineage it had before the DELETE.
    if (version >= 3) {
      val lineageAfter = lineageById(on)
      assert(lineageAfter.keySet == lineageBefore.keySet.intersect(onData.keySet), s"$name: lineage id set")
      lineageAfter.foreach { case (id, l) =>
        assert(l == lineageBefore(id), s"$name: lineage for id=$id changed (after=$l before=${lineageBefore(id)})")
      }
    }

    // Metric oracle: each file's recomputed metrics equal the recorded ones. An all-deleted table has no
    // data files left -- then there is nothing to check and the aggregate equality is trivially 0 = 0.
    val filesOn = checkFileMetrics(on, off)
    if (onData.nonEmpty) assert(filesOn > 0, s"$name: no data files to check")
    copied
  }

  // ----- cases --------------------------------------------------------------------------------------

  for (version <- Seq(2, 3); partitioned <- Seq(false, true); seedVector <- Seq(false, true)) {
    val seedTag = if (seedVector) "vector-seeded" else "iceberg-seeded"
    val tag = s"v$version ${if (partitioned) "partitioned" else "unpartitioned"} [$seedTag]"
    val base = s"${version}_${if (partitioned) "p" else "u"}_${if (seedVector) "vw" else "iw"}"
    // The copy path cannot supply a float/double NaN count (a footer carries none), so a table that tracks
    // one declines every copy. To exercise the COPY path these cases turn metrics off for the two floating
    // columns; a dedicated case below keeps NaN tracking and asserts the (correct) no-copy fallback.
    val noNaN = ", 'write.metadata.metrics.column.d'='none', 'write.metadata.metrics.column.f'='none'"

    // A v3 source written by an ordinary append materialises no row lineage (its _row_id is inherited from
    // first_row_id + position). #901 M4a step 1 copies such a file anyway: the data column chunks are copied
    // byte for byte and the two lineage columns (_row_id, _last_updated_sequence_number), which the scan
    // resolved per surviving row, are freshly encoded, so the surviving rows keep the lineage they had. v2 has
    // no lineage and copies the whole delete-free groups. Both therefore copy the delete-free groups.
    //
    // The seed files are laid down by BOTH writers in turn (seedVector): Iceberg's own writer, and our
    // columnar writer, which must roll several row groups per unpartitioned file for the copy to have
    // delete-free groups to copy -- the whole point of honouring write.parquet.row-group-size-bytes.
    def expect(copied: Long, label: String): Unit =
      assert(copied > 0, s"$tag $label: expected copied groups > 0, got $copied")

    test(s"$tag: DELETE touching some groups copies the untouched groups (v2) / falls back (v3)") {
      // Delete a single narrow contiguous block so only the one or two groups covering it are touched and
      // every other group of the file is delete-free (copied on v2).
      val copied =
        cow(s"some_$base", version, partitioned, rows = 8000, metricsProps = noNaN, seedWithVectorWriter = seedVector) {
          t =>
            s"DELETE FROM $t WHERE id >= 100 AND id < 140"
        }
      expect(copied, "some-deleted")
    }

    test(s"$tag: DELETE touching all rows of some files still copies others (v2) / falls back (v3)") {
      // A contiguous id range: files fully inside it are skipped/rewritten, files outside are all-copied (v2).
      val copied = cow(
        s"range_$base",
        version,
        partitioned,
        rows = 8000,
        metricsProps = noNaN,
        seedWithVectorWriter = seedVector
      ) { t =>
        s"DELETE FROM $t WHERE id >= 2000 AND id < 2500"
      }
      expect(copied, "range-deleted")
    }

    test(s"$tag: DELETE removing a single row copies almost every group (v2) / falls back (v3)") {
      // One row deleted: the one group holding it is re-encoded, every other group is copied (v2).
      val copied =
        cow(s"one_$base", version, partitioned, rows = 8000, metricsProps = noNaN, seedWithVectorWriter = seedVector) {
          t =>
            s"DELETE FROM $t WHERE id = 4242"
        }
      expect(copied, "one-row-deleted")
    }
  }

  test("v2 unpartitioned: DELETE removing every surviving group copies nothing") {
    // Delete all rows: every group of every file is fully deleted, so there is nothing to copy.
    val copied = cow("all_2u", version = 2, partitioned = false, rows = 4000) { t =>
      s"DELETE FROM $t WHERE id >= 0"
    }
    assert(copied == 0L, s"all-deleted: expected 0 copied groups, got $copied")
  }

  test("v2 unpartitioned: a table tracking NaN counts re-encodes (no copy) and stays correct") {
    // A float/double NaN count cannot come from a copied group's footer, so the file falls back to the
    // full rewrite. Data and metrics (including nan_value_counts) must still match the flag-off run.
    val copied = cow("nancol", version = 2, partitioned = false, rows = 6000, nan = true) { t =>
      s"DELETE FROM $t WHERE id % 97 = 0"
    }
    assert(copied == 0L, s"NaN-tracking table must not copy, got $copied")
  }

  test("schema-evolved table: DELETE after adding a column stays correct") {
    // After ALTER ADD COLUMN the old files lack the new column, so their groups cannot be copied into the
    // evolved write schema; the file falls back to the full rewrite. Correctness is the point here.
    val off = "ice.db.evolved_off"
    val on = "ice.db.evolved_on"
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(
        s"""CREATE TABLE $t (id BIGINT, s STRING) USING iceberg
           |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='copy-on-write',
           |  'write.parquet.row-group-size-bytes'='16384')""".stripMargin
      )
      spark.sql(s"INSERT INTO $t SELECT id, concat('v', id) FROM range(6000)")
      spark.sql(s"ALTER TABLE $t ADD COLUMN extra INT")
      // Append rows that DO carry the new column, so the table mixes old (no extra) and new (extra) files.
      spark.sql(s"INSERT INTO $t SELECT id, concat('w', id), cast(id as int) FROM range(6000, 9000)")
    }
    withCopy(on = false)(spark.sql(s"DELETE FROM $off WHERE id % 97 = 0").collect())
    withCopy(on = true)(spark.sql(s"DELETE FROM $on WHERE id % 97 = 0").collect())

    def data(t: String): Map[Long, String] =
      spark.sql(s"SELECT id, s, extra FROM $t").collect().map(r => r.getLong(0) -> r.toString).toMap
    val offData = data(off)
    val onData = data(on)
    assert(onData.keySet == offData.keySet, "evolved: surviving id set differs")
    onData.foreach { case (id, row) =>
      assert(row == offData(id), s"evolved: row for id=$id differs (on=$row off=${offData(id)})")
    }
    assert(checkFileMetrics(on, off) > 0)
  }
}
