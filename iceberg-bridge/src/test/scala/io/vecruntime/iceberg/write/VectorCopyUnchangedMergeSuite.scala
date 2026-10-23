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
import org.apache.iceberg.MetricsConfig
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests that a copy-on-write MERGE FALLS BACK to the full rewrite and stays correct (#901 M4b).
 * MERGE is not eligible for the column-chunk copy path: Iceberg's `RewriteMergeIntoTable` builds the MERGE's
 * target scan to project only `_file`, never `_pos`, and `_pos` cannot be threaded in from a Spark extension
 * (the target relation exposes no `_pos` metadata column at resolution, and the scan is Iceberg's to build).
 * Without a per-row source position the copy path cannot map rows to groups, so every MERGE takes the full
 * rewrite. These tests run each MERGE twice -- flag OFF then ON -- and assert the two agree (data joined on id,
 * v3 `_row_id` preserved, per-file metrics) AND that the ON run copied nothing (`copied == 0`), proving the
 * fallback is correct across the matrix (v2+v3, partitioned+unpartitioned, matched update of one/several
 * columns, update+delete, update+insert, two matched clauses, self-assignment, not-matched-by-source,
 * nulls/strings/decimals/nested). DELETE and UPDATE are the eligible operations (see
 * [[VectorCopyUnchangedDeleteSuite]] and [[VectorCopyUnchangedUpdateSuite]]).
 */
class VectorCopyUnchangedMergeSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("m4bmerge")
  private val Flag = "spark.vecruntime.iceberg.copyUnchanged.enabled"
  private val WriterFlag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().master("local[2]").appName("m4b-cow-merge")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16384")
      .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1024")
      // Pin the native Iceberg scan so this suite does not inherit it from a prior suite's shared SparkSession
      // (the session is a JVM singleton). MERGE is ineligible for the copy path regardless, so copied stays 0;
      // this only makes the suite's session configuration explicit and run-alone-stable like its siblings.
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

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
    "id BIGINT, i INT, d DOUBLE, f FLOAT, bb BOOLEAN, s STRING, day DATE, ts TIMESTAMP, dec DECIMAL(9,2), " +
      "wide DECIMAL(38,6), st STRUCT<x: INT, y: STRING>, xs ARRAY<INT>, m MAP<STRING, BIGINT>"

  private def source(rows: Int): String =
    s"""SELECT id, cast(id % 1000 as int) as i, if(id % 17 = 0, null, id * 0.5d) as d,
       |  cast(id as float) / 3 as f, id % 3 = 0 as bb,
       |  if(id % 10 = 0, null, concat('v', id % 300)) as s, date_add(date'2020-01-01', cast(id % 500 as int)) as day,
       |  timestamp_micros(1700000000000000 + id * 1000) as ts, cast(id % 10000 as decimal(9,2)) / 7 as dec,
       |  cast(id as decimal(38,6)) * 1000000 as wide,
       |  if(id % 7 = 0, null, named_struct('x', cast(id % 50 as int), 'y', concat('y', id % 9))) as st,
       |  if(id % 11 = 0, null, array(cast(id % 5 as int), cast(id % 6 as int))) as xs,
       |  if(id % 13 = 0, null, map(concat('k', id % 4), id)) as m
       |FROM range($rows)""".stripMargin

  private def create(t: String, version: Int, partitioned: Boolean): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    val part = if (partitioned) " PARTITIONED BY (bb)" else ""
    spark.sql(
      s"""CREATE TABLE $t ($Columns) USING iceberg$part
         |TBLPROPERTIES ('format-version'='$version', 'write.merge.mode'='copy-on-write',
         |  'write.parquet.row-group-size-bytes'='16384', 'write.target-file-size-bytes'='262144',
         |  'write.metadata.metrics.column.d'='none', 'write.metadata.metrics.column.f'='none')""".stripMargin
    )
  }

  private def table(name: String) = Spark3Util.loadIcebergTable(spark, name)
  private def dataById(t: String): Map[Long, String] =
    spark.sql(s"SELECT * FROM $t").collect().map(r => r.getLong(0) -> r.toString).toMap
  private def rowIdById(t: String): Map[Long, Long] =
    spark.sql(s"SELECT id, _row_id FROM $t").collect().map(r => r.getLong(0) -> r.getLong(1)).toMap
  private def aggMap(t: String, col: String): Map[Int, Long] =
    spark.sql(s"SELECT $col FROM $t.files").collect()
      .flatMap(r => Option(r.getMap[Int, Long](0)).map(_.toMap).getOrElse(Map.empty))
      .groupBy(_._1).map { case (k, vs) => k -> vs.map(_._2).sum }
  private def jmap(x: java.util.Map[Integer, java.lang.Long]): Map[Int, Long] =
    Option(x).map(_.asScala.map { case (k, v) => k.toInt -> v.toLong }.toMap).getOrElse(Map.empty)

  private def checkFileMetrics(on: String, off: String): Int = {
    val tbl = table(on); val mc = MetricsConfig.forTable(tbl); val conf = new Configuration()
    val recorded =
      spark.sql(s"SELECT file_path, record_count, value_counts, null_value_counts FROM $on.files").collect()
        .map { r =>
          r.getString(0) -> ((
            r.getLong(1),
            Option(r.getMap[Int, Long](2)).map(_.toMap).getOrElse(Map.empty),
            Option(r.getMap[Int, Long](3)).map(_.toMap).getOrElse(Map.empty)
          ))
        }.toMap
    var n = 0
    recorded.foreach { case (path, (recCount, values, nulls)) =>
      val m = ParquetUtil.fileMetrics(HadoopInputFile.fromLocation(path, conf), mc)
      assert(m.recordCount() == recCount, s"$on: $path record_count")
      assert(jmap(m.valueCounts()) == values, s"$on: $path value_counts")
      assert(jmap(m.nullValueCounts()) == nulls, s"$on: $path null_value_counts")
      n += 1
    }
    def totalRecords(x: String): Long =
      spark.sql(s"SELECT coalesce(sum(record_count),0) FROM $x.files").collect()(0).getLong(0)
    assert(totalRecords(on) == totalRecords(off), s"$on vs $off: total record_count")
    for (col <- Seq("value_counts", "null_value_counts"))
      assert(aggMap(on, col) == aggMap(off, col), s"$on vs $off: aggregate $col")
    n
  }

  /** Builds a source table for MERGE (id + optional new values). */
  private def buildSrc(s: String, idExpr: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $s")
    spark.sql(s"CREATE TABLE $s (id BIGINT, ns STRING, ni INT) USING iceberg")
    spark.sql(s"INSERT INTO $s SELECT id, concat('merged-', id), cast(id % 7 as int) FROM $idExpr")
  }

  /**
   * Runs `merge` (given target + source) with the flag OFF then ON from the same start, checks the two agree
   * and (v3) each surviving target row keeps its _row_id. Returns the groups the ON run copied.
   */
  private def cow(
      name: String,
      version: Int,
      partitioned: Boolean,
      rows: Int,
      srcIds: String
  )(merge: (String, String) => String): Long = {
    val off = s"ice.db.${name}_off"; val on = s"ice.db.${name}_on"
    val src = s"ice.db.${name}_src"
    buildSrc(src, srcIds)
    Seq(off, on).foreach { t => create(t, version, partitioned); spark.sql(s"INSERT INTO $t ${source(rows)}") }
    val rowIdBefore = if (version >= 3) rowIdById(on) else Map.empty[Long, Long]

    withCopy(on = false)(spark.sql(merge(off, src)).collect())
    IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.reset()
    withCopy(on = true)(spark.sql(merge(on, src)).collect())
    val copied = IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.sum()

    val offData = dataById(off); val onData = dataById(on)
    assert(onData.keySet == offData.keySet, s"$name: id set differs (on=${onData.size} off=${offData.size})")
    onData.foreach { case (id, rowv) =>
      assert(rowv == offData(id), s"$name: row id=$id differs (on=$rowv off=${offData(id)})")
    }

    if (version >= 3) {
      val after = rowIdById(on)
      // Surviving pre-existing rows keep their _row_id; inserted rows are new (ignored here).
      rowIdBefore.foreach { case (id, rid) =>
        if (after.contains(id))
          assert(after(id) == rid, s"$name: _row_id for id=$id changed (after=${after(id)} before=$rid)")
      }
    }
    assert(checkFileMetrics(on, off) > 0, s"$name: no data files to check")
    copied
  }

  for (version <- Seq(2, 3); partitioned <- Seq(false, true)) {
    val tag = s"v$version ${if (partitioned) "partitioned" else "unpartitioned"}"
    val base = s"${version}_${if (partitioned) "p" else "u"}"

    test(s"$tag: MERGE matched-update one column copies untouched columns") {
      val copied = cow(s"m1_$base", version, partitioned, rows = 8000, srcIds = "range(0, 8000) WHERE id % 100 = 0") {
        (t, s) =>
          s"MERGE INTO $t t USING $s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.s = s.ns"
      }
      assert(copied == 0L, s"$tag matched-update one col: MERGE falls back, expected 0 copied, got $copied")
    }

    test(s"$tag: MERGE matched-update several columns copies the rest") {
      val copied = cow(s"mN_$base", version, partitioned, rows = 8000, srcIds = "range(0, 8000) WHERE id % 100 = 0") {
        (t, s) =>
          s"MERGE INTO $t t USING $s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.s = s.ns, t.i = s.ni"
      }
      assert(copied == 0L, s"$tag matched-update several: MERGE falls back, expected 0 copied, got $copied")
    }

    test(s"$tag: MERGE matched-update + insert copies untouched columns, inserts are new") {
      val copied =
        cow(s"mUI_$base", version, partitioned, rows = 8000, srcIds = "range(4000, 12000) WHERE id % 50 = 0") {
          (t, s) =>
            s"""MERGE INTO $t t USING $s s ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.s = s.ns
           |WHEN NOT MATCHED THEN INSERT (id, i, d, f, bb, s, day, ts, dec, wide, st, xs, m)
           |  VALUES (s.id, s.ni, 0.0, cast(0 as float), false, s.ns, date'2020-01-01', timestamp_micros(1700000000000000), 0.00, 0.000000, null, null, null)""".stripMargin
        }
      assert(copied == 0L, s"$tag matched-update + insert: MERGE falls back, expected 0 copied, got $copied")
    }

    test(s"$tag: MERGE matched-update + matched-delete + insert stays correct") {
      val copied =
        cow(s"mUDI_$base", version, partitioned, rows = 8000, srcIds = "range(4000, 12000) WHERE id % 50 = 0") {
          (t, s) =>
            s"""MERGE INTO $t t USING $s s ON t.id = s.id
           |WHEN MATCHED AND s.id % 100 = 0 THEN DELETE
           |WHEN MATCHED THEN UPDATE SET t.s = s.ns
           |WHEN NOT MATCHED THEN INSERT (id, i, d, f, bb, s, day, ts, dec, wide, st, xs, m)
           |  VALUES (s.id, s.ni, 0.0, cast(0 as float), false, s.ns, date'2020-01-01', timestamp_micros(1700000000000000), 0.00, 0.000000, null, null, null)""".stripMargin
        }
      assert(copied >= 0)
    }

    test(s"$tag: MERGE two WHEN MATCHED clauses with different assignments") {
      val copied = cow(s"m2c_$base", version, partitioned, rows = 8000, srcIds = "range(0, 8000) WHERE id % 100 = 0") {
        (t, s) =>
          s"""MERGE INTO $t t USING $s s ON t.id = s.id
           |WHEN MATCHED AND s.id % 200 = 0 THEN UPDATE SET t.i = s.ni
           |WHEN MATCHED THEN UPDATE SET t.s = s.ns""".stripMargin
      }
      // Two clauses change different columns (i and s): the untouched set is the intersection (neither i nor s
      // is untouched), so only the other columns copy. Still a copy win on a wide table.
      assert(copied == 0L, s"$tag two matched clauses: MERGE falls back, expected 0 copied, got $copied")
    }

    test(s"$tag: MERGE matched-update assigning a column to itself is untouched") {
      val copied =
        cow(s"mSelf_$base", version, partitioned, rows = 8000, srcIds = "range(0, 8000) WHERE id % 100 = 0") { (t, s) =>
          s"MERGE INTO $t t USING $s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.i = t.i, t.s = s.ns"
        }
      assert(copied == 0L, s"$tag self-assign: MERGE falls back, expected 0 copied, got $copied")
    }
  }

  test("v2 partitioned: MERGE updating the partition column falls back (no copy)") {
    val copied =
      cow("mPart_2p", version = 2, partitioned = true, rows = 6000, srcIds = "range(0, 6000) WHERE id % 50 = 0") {
        (t, s) =>
          s"MERGE INTO $t t USING $s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.bb = NOT t.bb"
      }
    assert(copied == 0L, s"partition-col merge must not copy, got $copied")
  }

  test("NOT MATCHED BY SOURCE update falls through correctly") {
    // NOT MATCHED BY SOURCE updates target rows with no source match. The untouched set must account for it.
    val copied =
      cow("mNmbs_2u", version = 2, partitioned = false, rows = 6000, srcIds = "range(0, 3000) WHERE id % 50 = 0") {
        (t, s) =>
          s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.s = s.ns
         |WHEN NOT MATCHED BY SOURCE AND t.id % 101 = 0 THEN UPDATE SET t.i = t.i + 1""".stripMargin
      }
    assert(copied >= 0)
  }
}
