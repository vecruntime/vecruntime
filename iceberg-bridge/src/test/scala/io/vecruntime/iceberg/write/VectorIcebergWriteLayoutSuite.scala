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

import org.apache.iceberg.{Snapshot, Table}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.{VectorBinPackFileRewriteRunner, VectorRewriteDataFiles}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Reproduces three file-layout / metadata differences surfaced by Apache Iceberg 1.12's own Spark 4.1 suites
 * when run with the vecruntime plugin and our writers on. The data is always correct; what differs is the
 * number of data files a rewrite produces, or a metadata id a rewritten file carries. Each case mirrors the
 * statements of the corresponding Iceberg test and checks the OBSERVABLE output (snapshot added-data-files,
 * DataFile.sortOrderId, rewrite added-files count) against what Iceberg's own writer produces -- which the
 * tests exercise by running the SAME command with our features off (the oracle) and on.
 *
 *   (A) copy-on-write DELETE / UPDATE with AQE coalescing: Iceberg requests a RANGE distribution and AQE's
 *       CoalesceShufflePartitions fuses the small shuffle blocks into ONE task, so the rewrite adds ONE file.
 *   (B) copy-on-write DELETE on a sort-ordered table: the rewritten data files carry the table's sort order id.
 *   (C) bin-pack rewrite of mixed-size files: combines to the same file count as Iceberg's own bin-pack.
 *
 * Runs in the iceberg-bridge module (excluded from the gate's -pl list), so it is run manually.
 */
class VectorIcebergWriteLayoutSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("writelayout")

  private val WriterFlag = "spark.vecruntime.iceberg.writer.enabled"
  private val CopyFlag = "spark.vecruntime.iceberg.copyUnchanged.enabled"
  private val NativeScanFlag = "spark.vecruntime.scan.nativeIceberg.enabled"
  private val BinPackCopyFlag = VectorRewriteDataFiles.FLAG

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("write-layout")
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

  private def withWriters[T](on: Boolean)(f: => T): T =
    withConf(WriterFlag -> on.toString, CopyFlag -> on.toString, NativeScanFlag -> on.toString)(f)

  private def table(t: String): Table = Spark3Util.loadIcebergTable(spark, t)

  private def latest(t: String): Snapshot = {
    val tbl = table(t)
    tbl.currentSnapshot()
  }

  private def summaryProp(t: String, key: String): String =
    latest(t).summary().getOrDefault(key, "<absent>")

  // ----- (A) copy-on-write coalesce -----------------------------------------------------------------
  //
  // Iceberg's TestDelete.testCoalesceDelete / TestUpdate.testCoalesceUpdate on an UNPARTITIONED copy-on-write
  // table: four appends of 100 rows (four data files), RANGE delete distribution, AQE coalescing with a large
  // advisory size. Iceberg requests a RANGE distribution by `_file`,`_pos`; AQE coalesces the small shuffle
  // blocks into ONE task, so the DELETE adds ONE data file. We assert added-data-files == 1.

  private def seedCoalesceTable(t: String, mode: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"CREATE TABLE $t (id INT, dep STRING) USING iceberg " +
        s"TBLPROPERTIES ('write.delete.mode'='$mode', 'write.update.mode'='$mode')"
    )
    for (_ <- 0 until 4) {
      val rows = (0 until 100).map(i => s"($i, 'hr')").mkString(", ")
      spark.sql(s"INSERT INTO $t VALUES $rows")
    }
    // Reproduce the Iceberg test's table properties: a separate scan task per file, RANGE distribution,
    // partition-scoped deletes.
    spark.sql(
      s"ALTER TABLE $t SET TBLPROPERTIES (" +
        s"'read.split.open-file-cost'='${Int.MaxValue}', " +
        s"'write.delete.distribution-mode'='range', " +
        s"'write.update.distribution-mode'='range', " +
        s"'write.delete.granularity'='partition')"
    )
  }

  private def runCoalesce(mode: String, update: Boolean): String = {
    val t = s"ice.db.coalesce_${mode.replace('-', '_')}_${if (update) "upd" else "del"}"
    seedCoalesceTable(t, mode)
    withConf(
      "spark.sql.shuffle.partitions" -> "200",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "100",
      "spark.sql.iceberg.advisory-partition-size" -> String.valueOf(256 * 1024 * 1024)
    ) {
      if (update) spark.sql(s"UPDATE $t SET id = -1 WHERE mod(id, 2) = 0")
      else spark.sql(s"DELETE FROM $t WHERE mod(id, 2) = 0")
    }
    summaryProp(t, "added-data-files")
  }

  test("A: copy-on-write DELETE coalesces to one added data file (off == on == 1)") {
    val off = withWriters(on = false)(runCoalesce("copy-on-write", update = false))
    val on = withWriters(on = true)(runCoalesce("copy-on-write", update = false))
    info(s"A/DELETE added-data-files: off=$off on=$on")
    assert(off == "1", s"oracle (plugin off) should add 1 file, got $off")
    assert(on == "1", s"plugin on should add 1 file (AQE coalesce), got $on")
  }

  test("A: copy-on-write UPDATE coalesces to one added data file (off == on == 1)") {
    val off = withWriters(on = false)(runCoalesce("copy-on-write", update = true))
    val on = withWriters(on = true)(runCoalesce("copy-on-write", update = true))
    info(s"A/UPDATE added-data-files: off=$off on=$on")
    assert(off == "1", s"oracle (plugin off) should add 1 file, got $off")
    assert(on == "1", s"plugin on should add 1 file (AQE coalesce), got $on")
  }

  // ----- (B) sort order id on rewritten files -------------------------------------------------------
  //
  // Iceberg's TestCopyOnWriteDelete.testCopyOnWriteDeleteSetsSortOrderIdOnRewrittenDataFiles: a partitioned,
  // sort-ordered table; a copy-on-write DELETE; the rewritten data files must carry the table sort order id.

  private def runSortOrder(): (Int, Set[Integer]) = {
    val t = "ice.db.sortorder_del"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"CREATE TABLE $t (id INT, dep STRING) USING iceberg PARTITIONED BY (dep) " +
        s"TBLPROPERTIES ('write.delete.mode'='copy-on-write')"
    )
    spark.sql(s"INSERT INTO $t VALUES (1, 'hr'), (2, 'hr')")
    // ALTER TABLE ... WRITE ORDERED BY needs Iceberg's SQL extensions; set the sort order through the Java
    // API instead so this suite keeps only our extension registered.
    table(t).replaceSortOrder().asc("id").commit()
    spark.sql(s"DELETE FROM $t WHERE id = 1")
    val tbl = table(t)
    val expected = tbl.sortOrder().orderId()
    val ids = tbl
      .currentSnapshot()
      .addedDataFiles(tbl.io())
      .asScala
      .map(f => f.sortOrderId())
      .toSet
    (expected, ids)
  }

  test("B: copy-on-write DELETE stamps the table sort order id on rewritten files") {
    val (expected, offIds) = withWriters(on = false)(runSortOrder())
    val (_, onIds) = withWriters(on = true)(runSortOrder())
    info(s"B sortOrderId: expected=$expected off=$offIds on=$onIds")
    assert(offIds == Set[Integer](expected), s"oracle should carry sort order id $expected, got $offIds")
    assert(onIds == Set[Integer](expected), s"plugin on should carry sort order id $expected, got $onIds")
  }

  // ----- (C) bin-pack honours Iceberg's target file size -------------------------------------------
  //
  // Mirrors the shape of Iceberg's TestRewriteDataFilesAction.testBinPackCombineMixedFiles. A byte copy moves
  // WHOLE row groups: it can COMBINE small files up to the target, but it cannot SPLIT a row group larger than
  // the target (Iceberg's stock bin-pack decodes and re-encodes, so it splits). The M4c copy runner must
  //   (C1) pack copyable small files to Iceberg's OWN input split size, producing the same file count as the
  //        pure-Iceberg oracle for a combine-only group (and actually copy, not fall back); and
  //   (C2) when a file holds a row group larger than the group's input split size, decline the byte copy for
  //        the WHOLE group (it cannot reproduce Iceberg's split-and-combine layout) and hand it to the decode
  //        path, so no row group is copied.
  //
  // (C2) asserts only that the copy is DECLINED (the fix's contract); the exact decoded file count then depends
  // on the native scan honouring the bin-pack's per-scan read.split.target-size option, a scan-layer concern
  // tracked separately (PR #988, VectorIcebergRDD.packed), not the copy runner's.

  private val SmallScale = 120000

  private def seedCombineOnly(t: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    // A small target file size and row-group size so many tiny files genuinely bin-pack together and no single
    // row group approaches the split size (the pure combine case: a byte copy both applies and matches
    // Iceberg's own bin-pack, as VectorBinPackRewriteSuite also exercises).
    spark.sql(
      s"CREATE TABLE $t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
        s"TBLPROPERTIES ('write.parquet.row-group-size-bytes'='16384', 'write.target-file-size-bytes'='1048576')"
    )
    // Eight small single-row-group files, each far below the 1 MB target so they combine.
    for (_ <- 0 until 8) {
      spark
        .range(0, SmallScale / 4)
        .selectExpr("cast(id as int) as c1", "cast(id as string) as c2", "concat('foo', cast(id as string)) as c3")
        .repartition(1)
        .writeTo(t)
        .append()
    }
  }

  private def seedMixed(t: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(s"CREATE TABLE $t (c1 INT, c2 STRING, c3 STRING) USING iceberg")
    def write(rows: Int): Unit =
      spark
        .range(0, rows)
        .selectExpr("cast(id as int) as c1", "cast(id as string) as c2", "concat('foo', cast(id as string)) as c3")
        .repartition(1)
        .writeTo(t)
        .append()
    write(SmallScale)
    write(SmallScale)
    write(SmallScale * 8) // one file whose single row group is far larger than a few-small-files target
  }

  private def dataFileSizes(t: String): List[Long] = {
    val tbl = table(t)
    tbl.newScan().planFiles().iterator().asScala.toList.map(_.file().fileSizeInBytes())
  }

  private def dataFileCount(t: String): Int = {
    val tbl = table(t)
    tbl.newScan().planFiles().iterator().asScala.map(_.file().location()).toSet.size
  }

  /** Bin-pack with the given copy flag using the table's DEFAULT rewrite options (target from the table
    * property), so the stock and copy runners pack to the same size. Returns (added-files, final count). */
  private def runBinPackDefault(t: String, copyOn: Boolean): (Long, Int) = {
    val tbl = table(t)
    val result = withConf(BinPackCopyFlag -> copyOn.toString) {
      VectorRewriteDataFiles
        .binPackCopy(spark, tbl)
        .option("min-input-files", "1")
        .execute()
    }
    (result.addedDataFilesCount().toLong, dataFileCount(t))
  }

  private def runBinPack(t: String, copyOn: Boolean, target: Long, min: Long, max: Long): (Long, Int) = {
    val tbl = table(t)
    val result = withConf(BinPackCopyFlag -> copyOn.toString) {
      VectorRewriteDataFiles
        .binPackCopy(spark, tbl)
        // Mirror Iceberg's basicRewrite: always compact regardless of input file count.
        .option("min-input-files", "1")
        .option("target-file-size-bytes", String.valueOf(target))
        .option("min-file-size-bytes", String.valueOf(min))
        .option("max-file-size-bytes", String.valueOf(max))
        .execute()
    }
    (result.addedDataFilesCount().toLong, dataFileCount(t))
  }

  test("C1: combine-only bin-pack copy matches the stock bin-pack and actually copies") {
    val off = "ice.db.binpack_c1_off"
    val on = "ice.db.binpack_c1_on"
    withWriters(on = false) {
      seedCombineOnly(off)
      seedCombineOnly(on)
    }
    val sizes = dataFileSizes(off).sorted
    // Oracle: pure Iceberg stock bin-pack. On: our byte-copy runner. Default options -> both pack to the
    // table's 1 MB target, so the small files combine identically.
    val (offAdded, offFinal) = withWriters(on = false)(runBinPackDefault(off, copyOn = false))
    VectorBinPackFileRewriteRunner.COPIED_ROW_GROUPS.reset()
    val (onAdded, onFinal) = withWriters(on = true)(runBinPackDefault(on, copyOn = true))
    val copied = VectorBinPackFileRewriteRunner.COPIED_ROW_GROUPS.sum()
    info(
      s"C1 combine: sizes=$sizes off(added=$offAdded final=$offFinal) on(added=$onAdded final=$onFinal) copiedGroups=$copied"
    )
    assert(offAdded < 8L, s"precondition: the stock bin-pack should combine the 8 files (added=$offAdded)")
    assert(onAdded == offAdded, s"combine-only: plugin added=$onAdded must equal the stock oracle added=$offAdded")
    assert(onFinal == offFinal, s"combine-only: final count must match the oracle off=$offFinal on=$onFinal")
    assert(copied > 0, "the byte copy must actually run for a combine-only group (no fallback)")
  }

  test("C2: bin-pack copy declines a group needing a split (row group larger than the split size)") {
    val on = "ice.db.binpack_c2_on"
    withWriters(on = false)(seedMixed(on))
    val sizes = dataFileSizes(on).sorted
    val largest = sizes.max
    // Target well below the large file so its single row group exceeds the group's split size -> needs split.
    val target = largest / 3
    val min = (target * 0.75).toLong
    val max = (target * 1.8).toLong
    VectorBinPackFileRewriteRunner.COPIED_OUTPUT_FILES.reset()
    VectorBinPackFileRewriteRunner.COPIED_ROW_GROUPS.reset()
    val (onAdded, onFinal) = withWriters(on = true)(runBinPack(on, copyOn = true, target, min, max))
    val copiedFiles = VectorBinPackFileRewriteRunner.COPIED_OUTPUT_FILES.sum()
    val copiedGroups = VectorBinPackFileRewriteRunner.COPIED_ROW_GROUPS.sum()
    info(
      s"C2 split: sizes=$sizes target=$target on(added=$onAdded final=$onFinal) copiedFiles=$copiedFiles copiedGroups=$copiedGroups"
    )
    // The whole group must fall back to the decode path: the byte copy is declined, so NOTHING is copied.
    assert(
      copiedFiles == 0L && copiedGroups == 0L,
      s"a group needing a split must decline the copy entirely, but copiedFiles=$copiedFiles copiedGroups=$copiedGroups"
    )
  }
}
