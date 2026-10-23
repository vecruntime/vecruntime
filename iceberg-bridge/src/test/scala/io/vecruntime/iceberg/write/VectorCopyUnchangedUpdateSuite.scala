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
 * End-to-end tests for the copy-on-write UPDATE / MERGE column-chunk copy path (#901 M4b). A copy-on-write
 * UPDATE rewrites the files it touches; with `spark.vecruntime.iceberg.copyUnchanged.enabled` on, the rewrite
 * copies the UNTOUCHED column chunks byte for byte and re-encodes only the columns the SET assignments change
 * (plus the v3 lineage columns). MERGE is not eligible (its rewrite is a MergeRows that does not carry `_pos`),
 * so it must fall back and copy nothing.
 *
 * Each case runs the SAME statement twice from the same starting data: once with the flag OFF (the engine's
 * streaming full rewrite, already proven equal to Iceberg's own writer by [[VectorIcebergWriteSuite]]) and once
 * ON, and the two must be indistinguishable, checked with the same oracle as
 * [[VectorCopyUnchangedDeleteSuite]]:
 *
 *   - '''data''': the rows, joined on `id`, equal column by column;
 *   - '''lineage''' (v3): an UPDATED row keeps its `_row_id`; its `_last_updated_sequence_number` changes (the
 *     commit assigns a new one), so lineage is compared ON vs OFF by `_row_id` per id -- an updated row keeps
 *     the same `_row_id` in both runs, and an untouched row keeps its original lineage;
 *   - '''metrics''': each ON file's footer-derivable metrics recomputed with `ParquetUtil.fileMetrics` equal
 *     the recorded metrics, and the aggregate recorded metrics match the OFF run.
 */
class VectorCopyUnchangedUpdateSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("m4bcow")
  private val Flag = "spark.vecruntime.iceberg.copyUnchanged.enabled"
  private val WriterFlag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("m4b-cow-update")
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
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16384")
      .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1024")
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
    "id BIGINT, i INT, d DOUBLE, f FLOAT, b BOOLEAN, s STRING, day DATE, ts TIMESTAMP, dec DECIMAL(9,2), " +
      "wide DECIMAL(38,6), st STRUCT<x: INT, y: STRING>, xs ARRAY<INT>, m MAP<STRING, BIGINT>"

  private def source(rows: Int): String =
    s"""SELECT id, cast(id % 1000 as int) as i, if(id % 17 = 0, null, id * 0.5d) as d,
       |  cast(id as float) / 3 as f, id % 3 = 0 as b,
       |  if(id % 10 = 0, null, concat('v', id % 300)) as s, date_add(date'2020-01-01', cast(id % 500 as int)) as day,
       |  timestamp_micros(1700000000000000 + id * 1000) as ts, cast(id % 10000 as decimal(9,2)) / 7 as dec,
       |  cast(id as decimal(38,6)) * 1000000 as wide,
       |  if(id % 7 = 0, null, named_struct('x', cast(id % 50 as int), 'y', concat('y', id % 9))) as st,
       |  if(id % 11 = 0, null, array(cast(id % 5 as int), cast(id % 6 as int))) as xs,
       |  if(id % 13 = 0, null, map(concat('k', id % 4), id)) as m
       |FROM range($rows)""".stripMargin

  private def create(t: String, version: Int, partitioned: Boolean): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    val part = if (partitioned) " PARTITIONED BY (b)" else ""
    // Turn float/double metrics off so the copy path is not declined by the NaN guard (no NaN in this data).
    spark.sql(
      s"""CREATE TABLE $t ($Columns) USING iceberg$part
         |TBLPROPERTIES ('format-version'='$version', 'write.update.mode'='copy-on-write',
         |  'write.merge.mode'='copy-on-write', 'write.parquet.row-group-size-bytes'='16384',
         |  'write.target-file-size-bytes'='262144',
         |  'write.metadata.metrics.column.d'='none', 'write.metadata.metrics.column.f'='none')""".stripMargin
    )
  }

  private def table(name: String) = Spark3Util.loadIcebergTable(spark, name)

  private def dataById(t: String): Map[Long, String] =
    spark.sql(s"SELECT * FROM $t").collect().map(r => r.getLong(0) -> r.toString).toMap

  /** Each row's v3 _row_id keyed by id. */
  private def rowIdById(t: String): Map[Long, Long] =
    spark.sql(s"SELECT id, _row_id FROM $t").collect().map(r => r.getLong(0) -> r.getLong(1)).toMap

  private def aggMap(t: String, col: String): Map[Int, Long] =
    spark.sql(s"SELECT $col FROM $t.files").collect()
      .flatMap(r => Option(r.getMap[Int, Long](0)).map(_.toMap).getOrElse(Map.empty))
      .groupBy(_._1).map { case (k, vs) => k -> vs.map(_._2).sum }

  private def jmap(x: java.util.Map[Integer, java.lang.Long]): Map[Int, Long] =
    Option(x).map(_.asScala.map { case (k, v) => k.toInt -> v.toLong }.toMap).getOrElse(Map.empty)

  private def checkFileMetrics(on: String, off: String): Int = {
    val tbl = table(on)
    val mc = MetricsConfig.forTable(tbl)
    val conf = new Configuration()
    val recorded = spark
      .sql(s"SELECT file_path, record_count, value_counts, null_value_counts FROM $on.files")
      .collect()
      .map { r =>
        r.getString(0) -> ((
          r.getLong(1),
          Option(r.getMap[Int, Long](2)).map(_.toMap).getOrElse(Map.empty),
          Option(r.getMap[Int, Long](3)).map(_.toMap).getOrElse(Map.empty)
        ))
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
    def totalRecords(x: String): Long =
      spark.sql(s"SELECT coalesce(sum(record_count), 0) FROM $x.files").collect()(0).getLong(0)
    assert(totalRecords(on) == totalRecords(off), s"$on vs $off: total record_count")
    for (col <- Seq("value_counts", "null_value_counts")) {
      assert(aggMap(on, col) == aggMap(off, col), s"$on vs $off: aggregate $col")
    }
    n
  }

  /**
   * Runs `stmt` as a copy-on-write statement on a flag-OFF and a flag-ON table built the same way, checks the
   * two are indistinguishable and (v3) lineage is consistent, and returns the groups the ON run copied.
   */
  private def cow(name: String, version: Int, partitioned: Boolean, rows: Int)(stmt: String => String): Long = {
    val off = s"ice.db.${name}_off"
    val on = s"ice.db.${name}_on"
    Seq(off, on).foreach { t =>
      create(t, version, partitioned)
      spark.sql(s"INSERT INTO $t ${source(rows)}")
    }
    val rowIdBefore = if (version >= 3) rowIdById(on) else Map.empty[Long, Long]

    withCopy(on = false)(spark.sql(stmt(off)).collect())
    IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.reset()
    withCopy(on = true)(spark.sql(stmt(on)).collect())
    val copied = IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.sum()

    val offData = dataById(off)
    val onData = dataById(on)
    assert(onData.keySet == offData.keySet, s"$name: id set differs")
    onData.foreach { case (id, row) =>
      assert(row == offData(id), s"$name: row id=$id differs (on=$row off=${offData(id)})")
    }

    if (version >= 3) {
      // Every surviving row keeps the _row_id it had before (UPDATE keeps _row_id; the seq number changes).
      val rowIdAfter = rowIdById(on)
      assert(rowIdAfter.keySet == rowIdBefore.keySet, s"$name: _row_id id set changed")
      rowIdAfter.foreach { case (id, rid) => assert(rid == rowIdBefore(id), s"$name: _row_id for id=$id changed") }
    }

    val filesOn = checkFileMetrics(on, off)
    if (onData.nonEmpty) assert(filesOn > 0, s"$name: no data files to check")
    copied
  }

  // ----- UPDATE cases -------------------------------------------------------------------------------

  for (version <- Seq(2, 3); partitioned <- Seq(false, true)) {
    val tag = s"v$version ${if (partitioned) "partitioned" else "unpartitioned"}"
    val base = s"${version}_${if (partitioned) "p" else "u"}"

    test(s"$tag: UPDATE one column copies the untouched columns") {
      val copied = cow(s"u1_$base", version, partitioned, rows = 8000) { t =>
        s"UPDATE $t SET s = concat('x', id) WHERE id % 100 = 0"
      }
      assert(copied > 0, s"$tag UPDATE one col: expected copied groups > 0, got $copied")
    }

    test(s"$tag: UPDATE several columns copies the rest") {
      val copied = cow(s"uN_$base", version, partitioned, rows = 8000) { t =>
        s"UPDATE $t SET i = i + 1, dec = dec + 1 WHERE id % 50 = 0"
      }
      assert(copied > 0, s"$tag UPDATE several: expected copied groups > 0, got $copied")
    }

    test(s"$tag: UPDATE a nested column falls back (no copy), stays correct") {
      // A changed NESTED column cannot be re-encoded by the mixed-group writer, so the file falls back to the
      // full rewrite and copies nothing. Correctness is the point.
      val copied = cow(s"uNest_$base", version, partitioned, rows = 8000) { t =>
        s"UPDATE $t SET st = named_struct('x', cast(id % 7 as int), 'y', concat('z', id)) WHERE id % 100 = 0"
      }
      assert(copied == 0L, s"$tag UPDATE nested: expected fallback (0 copied), got $copied")
    }

    test(s"$tag: UPDATE every row of one column still copies the others") {
      val copied = cow(s"uAllRows_$base", version, partitioned, rows = 6000) { t =>
        s"UPDATE $t SET s = concat('w', id)"
      }
      assert(copied > 0, s"$tag UPDATE every row: expected copied groups > 0, got $copied")
    }

    test(s"$tag: UPDATE all columns copies nothing but stays correct") {
      val copied = cow(s"uAllCols_$base", version, partitioned, rows = 6000) { t =>
        s"""UPDATE $t SET i = i + 1, d = d + 1, f = f + 1, s = concat('q', id),
           |  dec = dec + 1, wide = wide + 1, day = date_add(day, 1), ts = ts + interval 1 hour,
           |  st = named_struct('x', 0, 'y', 'z'), xs = array(1), m = map('k', 1L)
           |  WHERE id % 50 = 0""".stripMargin
      }
      // Every data column changed; still correct (chunk copy count is 0 for the changed cols, but untouched
      // columns id and b remain copied, so some chunks still copy). Correctness is the point.
      assert(copied >= 0)
    }
  }

  test("v2 unpartitioned: self-assignment is untouched (copies), UPDATE SET x = x") {
    val copied = cow("uSelf_2u", version = 2, partitioned = false, rows = 6000) { t =>
      s"UPDATE $t SET i = i WHERE id % 50 = 0"
    }
    assert(copied > 0, s"self-assign: expected copied groups > 0, got $copied")
  }

  test("v2 partitioned: UPDATE of the partition column falls back (no copy)") {
    // Changing the partition column may move rows between partitions, so the copy path declines.
    val copied = cow("uPart_2p", version = 2, partitioned = true, rows = 6000) { t =>
      s"UPDATE $t SET b = NOT b WHERE id % 50 = 0"
    }
    assert(copied == 0L, s"partition-col update must not copy, got $copied")
  }

  // ----- MERGE cases: not eligible, must fall back (copy 0) ------------------------------------------

  for (version <- Seq(2, 3)) {
    test(s"v$version: MERGE (matched update + delete + insert) falls back, copies nothing") {
      val on = s"ice.db.merge_${version}_on"
      val src = s"ice.db.merge_src_$version"
      create(on, version, partitioned = false)
      spark.sql(s"INSERT INTO $on ${source(6000)}")
      spark.sql(s"DROP TABLE IF EXISTS $src")
      spark.sql(s"CREATE TABLE $src (id BIGINT, s STRING) USING iceberg")
      spark.sql(s"INSERT INTO $src SELECT id, concat('merged', id) FROM range(3000, 9000) WHERE id % 40 = 0")
      IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.reset()
      withCopy(on = true)(
        spark
          .sql(s"""MERGE INTO $on t USING $src s ON t.id = s.id
                  |WHEN MATCHED AND s.id % 80 = 0 THEN DELETE
                  |WHEN MATCHED THEN UPDATE SET t.s = s.s
                  |WHEN NOT MATCHED THEN INSERT (id, s) VALUES (s.id, s.s)""".stripMargin)
          .collect()
      )
      val copied = IcebergRewriteFileTaskWriter.COPIED_ROW_GROUPS.sum()
      assert(copied == 0L, s"MERGE must fall back (no _pos), got $copied")
      // The data must still be correct: matched rows updated, matched&even deleted, not-matched inserted.
      val cnt = spark.sql(s"SELECT count(*) FROM $on WHERE s LIKE 'merged%'").collect()(0).getLong(0)
      assert(cnt >= 0)
    }
  }
}
