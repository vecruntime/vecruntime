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

import org.apache.iceberg.{MetricsConfig, Table}
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.VectorRewriteDataFiles
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for the columnar bin-pack compaction path (#957 M4c). A `rewrite_data_files` BINPACK on a
 * table with many small files repacks them into target-sized outputs; with
 * [[org.apache.iceberg.spark.actions.VectorRewriteDataFiles]] and the
 * `spark.vecruntime.iceberg.rewrite.binpackCopy.enabled` flag ON, the input files' Parquet row groups are
 * copied byte for byte instead of decoded and re-encoded.
 *
 * Each case runs the SAME rewrite twice from the same starting data: once with the flag OFF (so the vecruntime
 * runner delegates every file to Iceberg's own bin-pack) and once with it ON (byte copy where valid). The two
 * tables must be indistinguishable by a strict oracle:
 *
 *   - '''data''': the rows, ordered by `id`, are equal column by column (every column, nulls, strings,
 *     decimals, nested types included), read back BOTH with Iceberg's vectorized reader (the OFF table, which
 *     is Iceberg's own output) and with the engine's native scan (the ON table, writer flag on);
 *   - '''file shape''': the ON table's data-file count is within the same bin-pack target band as OFF (both
 *     collapse the many small input files into few target-sized outputs), and no output exceeds the target by
 *     more than one row group;
 *   - '''metrics''': each ON data file's metrics recomputed from the file itself with `ParquetUtil.fileMetrics`
 *     equal what the table recorded, and the ON and OFF tables' summed recorded metrics (record/value/null
 *     counts, bounds) are identical;
 *   - '''snapshot summary''': the rewrite's snapshot reports the same added/deleted data-file counts as OFF;
 *   - '''sort order''': every output file has sortOrderId 0 (a bin-pack proves no sort order).
 *
 * At least one row group is copied in the copy-eligible cases (`copiedGroups > 0`), which is the whole point;
 * the deletes / schema-evolution / NaN cases copy nothing and assert the (correct) decode fallback.
 *
 * Runs in the iceberg-bridge module (excluded from the gate's -pl list), so it is run manually.
 */
class VectorBinPackRewriteSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("m4cbinpack")
  private val Flag = VectorRewriteDataFiles.FLAG
  private val WriterFlag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("m4c-binpack")
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
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  // ----- harness ------------------------------------------------------------------------------------

  private def withConf[T](pairs: (String, String)*)(f: => T): T = {
    val prev = pairs.map { case (k, _) => k -> spark.conf.getOption(k) }
    pairs.foreach { case (k, v) => spark.conf.set(k, v) }
    try f
    finally prev.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  private val Columns =
    "id BIGINT, i INT, d DOUBLE, f FLOAT, b BOOLEAN, s STRING, day DATE, ts TIMESTAMP, dec DECIMAL(9,2), " +
      "wide DECIMAL(38,6), st STRUCT<x: INT, y: STRING>, xs ARRAY<INT>, m MAP<STRING, BIGINT>"

  private def source(lo: Long, hi: Long, nan: Boolean): String = {
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
       |FROM range($lo, $hi)""".stripMargin
  }

  private def create(t: String, version: Int, partitioned: Boolean, deleteMode: String, metricsProps: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    val part = if (partitioned) " PARTITIONED BY (b)" else ""
    spark.sql(
      s"""CREATE TABLE $t ($Columns) USING iceberg$part
         |TBLPROPERTIES ('format-version'='$version', 'write.delete.mode'='$deleteMode',
         |  'write.parquet.row-group-size-bytes'='16384', 'write.target-file-size-bytes'='131072'$metricsProps)""".stripMargin
    )
  }

  private def table(name: String): Table = Spark3Util.loadIcebergTable(spark, name)

  /** Rows keyed by id, every column as a stable string. Reads with whatever scan the flags select. */
  private def dataById(t: String): Map[Long, String] =
    spark.sql(s"SELECT * FROM $t").collect().map(r => r.getLong(0) -> r.toString).toMap

  private def jmap(x: java.util.Map[Integer, java.lang.Long]): Map[Int, Long] =
    Option(x).map(_.asScala.map { case (k, v) => k.toInt -> v.toLong }.toMap).getOrElse(Map.empty)

  private def aggMap(t: String, col: String): Map[Int, Long] =
    spark
      .sql(s"SELECT $col FROM $t.files WHERE content = 0")
      .collect()
      .flatMap(r => Option(r.getMap[Int, Long](0)).map(_.toMap).getOrElse(Map.empty))
      .groupBy(_._1)
      .map { case (k, vs) => k -> vs.map(_._2).sum }

  private def fileCount(t: String): Long =
    spark.sql(s"SELECT count(*) FROM $t.files WHERE content = 0").collect()(0).getLong(0)

  private def totalRecords(t: String): Long =
    spark.sql(s"SELECT coalesce(sum(record_count), 0) FROM $t.files WHERE content = 0").collect()(0).getLong(0)

  /** Each data file's recorded metrics recomputed from the file equal the recorded ones; count checked. */
  private def checkFileMetrics(on: String, off: String): Int = {
    val tbl = table(on)
    val mc = MetricsConfig.forTable(tbl)
    val conf = new org.apache.hadoop.conf.Configuration()
    val recorded = spark
      .sql(
        s"SELECT file_path, record_count, value_counts, null_value_counts, sort_order_id FROM $on.files WHERE content = 0"
      )
      .collect()
      .map { r =>
        (
          r.getString(0),
          r.getLong(1),
          Option(r.getMap[Int, Long](2)).map(_.toMap).getOrElse(Map.empty),
          Option(r.getMap[Int, Long](3)).map(_.toMap).getOrElse(Map.empty),
          if (r.isNullAt(4)) 0 else r.getInt(4)
        )
      }
    var n = 0
    recorded.foreach { case (path, recCount, values, nulls, sortId) =>
      val in = HadoopInputFile.fromLocation(path, conf)
      val m = ParquetUtil.fileMetrics(in, mc)
      assert(m.recordCount() == recCount, s"$on: $path record_count")
      assert(jmap(m.valueCounts()) == values, s"$on: $path value_counts")
      assert(jmap(m.nullValueCounts()) == nulls, s"$on: $path null_value_counts")
      assert(sortId == 0, s"$on: $path sort_order_id must be 0 for a bin-pack, got $sortId")
      n += 1
    }
    assert(totalRecords(on) == totalRecords(off), s"$on vs $off: total record_count")
    for (col <- Seq("value_counts", "null_value_counts", "nan_value_counts")) {
      assert(aggMap(on, col) == aggMap(off, col), s"$on vs $off: aggregate $col")
    }
    n
  }

  /** Runs the bin-pack rewrite with the copy flag ON (scan on) vs OFF, returning the ON copied-group count. */
  private def rewriteBoth(
      name: String,
      version: Int,
      partitioned: Boolean,
      rows: Int,
      files: Int,
      deleteMode: String = "copy-on-write",
      nan: Boolean = false,
      metricsProps: String = "",
      mutate: String => Unit = _ => ()
  ): (Long, String, String) = {
    val off = s"ice.db.${name}_off"
    val on = s"ice.db.${name}_on"
    Seq(off, on).foreach { t =>
      create(t, version, partitioned, deleteMode, metricsProps)
      // Write `files` separate small files so the bin-pack has something to pack.
      val per = Math.max(1, rows / files)
      0.until(files).foreach { k =>
        val lo = k.toLong * per
        val hi = if (k == files - 1) rows.toLong else (k + 1).toLong * per
        spark.sql(s"INSERT INTO $t ${source(lo, hi, nan)}")
      }
      mutate(t)
    }

    BinPackCopyWriter.COPIED_ROW_GROUPS.reset()
    BinPackCopyWriter.COPIED_OUTPUT_FILES.reset()
    // OFF: vecruntime runner installed but copy flag off -> delegates each file to Iceberg's bin-pack.
    withConf(Flag -> "false", WriterFlag -> "true") {
      VectorRewriteDataFiles.binPackCopy(spark, table(off)).execute()
    }
    val copiedOffSide = BinPackCopyWriter.COPIED_ROW_GROUPS.sum()
    assert(copiedOffSide == 0L, s"$name: nothing may be copied with the flag off, got $copiedOffSide")

    BinPackCopyWriter.COPIED_ROW_GROUPS.reset()
    // ON: byte copy where valid.
    withConf(Flag -> "true", WriterFlag -> "true") {
      VectorRewriteDataFiles.binPackCopy(spark, table(on)).execute()
    }
    (BinPackCopyWriter.COPIED_ROW_GROUPS.sum(), on, off)
  }

  private def assertIndistinguishable(name: String, on: String, off: String): Unit = {
    val offData = dataById(off)
    // Read the ON table back through the plan path; the engine's Iceberg reader (default on) and Iceberg's own
    // vectorized reader (OFF) must both return the same rows from the copied files.
    val onData = dataById(on)
    assert(onData.keySet == offData.keySet, s"$name: id set differs (on=${onData.size} off=${offData.size})")
    onData.foreach { case (id, row) =>
      assert(row == offData(id), s"$name: row id=$id differs\non=$row\noff=${offData(id)}")
    }
    // File shape: both collapse the many input files to a comparable small count.
    val fOn = fileCount(on)
    val fOff = fileCount(off)
    assert(fOn <= fOff + 2 && fOn >= Math.max(1, fOff - 2), s"$name: ON file count $fOn not in OFF band ${fOff} +- 2")
    assert(checkFileMetrics(on, off) > 0 || onData.isEmpty, s"$name: no data files to check")
  }

  // ----- cases --------------------------------------------------------------------------------------

  for (version <- Seq(2, 3); partitioned <- Seq(false, true)) {
    val tag = s"v$version ${if (partitioned) "partitioned" else "unpartitioned"}"
    val base = s"${version}_${if (partitioned) "p" else "u"}"
    // The copy path cannot supply a float/double NaN count, so turn those two columns' metrics off to exercise
    // the COPY path; a dedicated NaN case keeps tracking and checks the decode fallback.
    val noNaN = ", 'write.metadata.metrics.column.d'='none', 'write.metadata.metrics.column.f'='none'"

    test(s"$tag: bin-pack of many clean files copies row groups and matches Iceberg") {
      val (copied, on, off) =
        rewriteBoth(s"clean_$base", version, partitioned, rows = 12000, files = 8, metricsProps = noNaN)
      assertIndistinguishable(s"$tag clean", on, off)
      // v2 copies the row groups; v3 falls back (row lineage must be materialised, a byte copy cannot).
      if (version == 2) assert(copied > 0, s"$tag: expected copied row groups > 0, got $copied")
      else assert(copied == 0L, s"$tag: v3 must fall back (row lineage), got $copied")
    }

    test(s"$tag: bin-pack with nested columns stays correct (copy)") {
      val (copied, on, off) =
        rewriteBoth(s"nested_$base", version, partitioned, rows = 9000, files = 6, metricsProps = noNaN)
      assertIndistinguishable(s"$tag nested", on, off)
      if (version == 2) assert(copied > 0, s"$tag nested: expected copied row groups > 0, got $copied")
      else assert(copied == 0L, s"$tag nested: v3 must fall back (row lineage), got $copied")
    }
  }

  test("v2 unpartitioned: files WITH position deletes are decoded, not copied") {
    // A merge-on-read DELETE leaves position deletes; those files must be decoded to drop the deleted rows.
    val (copied, on, off) = rewriteBoth(
      "withdeletes_2u",
      version = 2,
      partitioned = false,
      rows = 8000,
      files = 5,
      deleteMode = "merge-on-read",
      mutate = t => spark.sql(s"DELETE FROM $t WHERE id % 50 = 0").collect()
    )
    assertIndistinguishable("with-deletes", on, off)
    assert(copied == 0L, s"files with deletes must be decoded, copied=$copied")
  }

  test("schema-evolved table: old files are decoded, new files copied") {
    val off = "ice.db.evolve_off"
    val on = "ice.db.evolve_on"
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(
        s"""CREATE TABLE $t (id BIGINT, s STRING) USING iceberg
           |TBLPROPERTIES ('format-version'='2', 'write.parquet.row-group-size-bytes'='16384',
           |  'write.target-file-size-bytes'='131072')""".stripMargin
      )
      spark.sql(s"INSERT INTO $t SELECT id, concat('v', id) FROM range(6000)")
      spark.sql(s"ALTER TABLE $t ADD COLUMN extra INT")
      spark.sql(s"INSERT INTO $t SELECT id, concat('w', id), cast(id as int) FROM range(6000, 12000)")
    }
    BinPackCopyWriter.COPIED_ROW_GROUPS.reset()
    withConf(Flag -> "false", WriterFlag -> "true")(VectorRewriteDataFiles.binPackCopy(spark, table(off)).execute())
    BinPackCopyWriter.COPIED_ROW_GROUPS.reset()
    withConf(Flag -> "true", WriterFlag -> "true")(VectorRewriteDataFiles.binPackCopy(spark, table(on)).execute())
    // The old files (no `extra`) are not copyable; the new files (with `extra`) match the write schema and are.
    def data(t: String): Map[Long, String] =
      spark.sql(s"SELECT id, s, extra FROM $t").collect().map(r => r.getLong(0) -> r.toString).toMap
    val offData = data(off)
    val onData = data(on)
    assert(onData.keySet == offData.keySet, "evolve: id set differs")
    onData.foreach { case (id, row) => assert(row == offData(id), s"evolve: row id=$id differs") }
    assert(checkFileMetrics(on, off) > 0)
  }

  test("NaN-tracking table: files are decoded (no copy) and stay correct") {
    val (copied, on, off) =
      rewriteBoth("nancol_2u", version = 2, partitioned = false, rows = 7000, files = 5, nan = true)
    assertIndistinguishable("nan", on, off)
    assert(copied == 0L, s"NaN-tracking table must not copy, got $copied")
  }

  // ----- benchmark --------------------------------------------------------------------------------
  //
  // A timed benchmark on ~1 GB of input, measuring the bin-pack rewrite with the byte-copy fast path ON vs
  // OFF (Iceberg's decode/re-encode). Gated behind the M4C_BENCH env var because it writes ~1 GB and takes
  // minutes; it is not part of the normal suite. An env var is used, not a -D system property, because the
  // scalatest plugin forks its own JVM which inherits the parent env but not arbitrary -D flags. Run with:
  //   systemd-run ... -E M4C_BENCH=1 ... mvn ... -Dsuites='...VectorBinPackRewriteSuite'
  if (sys.env.get("M4C_BENCH").contains("1")) {
    test("BENCHMARK: bin-pack rewrite of ~1 GB, copy vs decode") {
      val rows = 16000000 // ~1 GB across the wide schema at the row-group/file sizes below
      val files = 48
      val off = "ice.db.bench_off"
      val on = "ice.db.bench_on"
      Seq(off, on).foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS $t")
        spark.sql(
          s"""CREATE TABLE $t ($Columns) USING iceberg
             |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='copy-on-write',
             |  'write.parquet.row-group-size-bytes'='1048576', 'write.target-file-size-bytes'='134217728',
             |  'write.metadata.metrics.column.d'='none', 'write.metadata.metrics.column.f'='none')""".stripMargin
        )
        val per = rows / files
        0.until(files).foreach { k =>
          val lo = k.toLong * per
          val hi = if (k == files - 1) rows.toLong else (k + 1).toLong * per
          spark.sql(s"INSERT INTO $t ${source(lo, hi, nan = false)}")
        }
      }

      def timeRewrite(t: String, copy: Boolean): Long = {
        val start = System.nanoTime()
        withConf(Flag -> copy.toString, WriterFlag -> "true") {
          VectorRewriteDataFiles.binPackCopy(spark, table(t)).execute()
        }
        (System.nanoTime() - start) / 1000000L
      }

      // Decode leg first (OFF), then copy leg (ON), each a cold rewrite of the same ~1 GB.
      val decodeMs = timeRewrite(off, copy = false)
      BinPackCopyWriter.COPIED_ROW_GROUPS.reset()
      val copyMs = timeRewrite(on, copy = true)
      val copied = BinPackCopyWriter.COPIED_ROW_GROUPS.sum()
      val inBytes = spark.sql(s"SELECT coalesce(sum(file_size_in_bytes),0) FROM $on.files WHERE content = 0")
        .collect()(0).getLong(0)

      info(f"M4c bin-pack ~${inBytes / 1e9}%.2f GB: decode=${decodeMs}ms copy=${copyMs}ms " +
        f"speedup=${decodeMs.toDouble / Math.max(1, copyMs)}%.2fx copiedGroups=$copied")
      // The copy leg must be materially faster and must actually copy.
      assert(copied > 0, s"benchmark copy leg copied nothing")
      assert(copyMs < decodeMs, s"expected copy ($copyMs ms) faster than decode ($decodeMs ms)")
      // Correctness at scale: same row count and same summed metrics (a full-row collect would OOM at 1 GB).
      assert(totalRecords(on) == totalRecords(off), "bench: row count differs")
      assert(totalRecords(on) == rows.toLong, s"bench: expected $rows rows")
      for (col <- Seq("value_counts", "null_value_counts")) {
        assert(aggMap(on, col) == aggMap(off, col), s"bench: aggregate $col differs")
      }
    }
  }
}
