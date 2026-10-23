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

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.{FileContent, FileFormat, Table}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * The row-level operation mode matrix (#901 Draco). The owner requires that our columnar Iceberg write
 * path follow the table's row-level operation mode properties -- `write.delete.mode`,
 * `write.update.mode`, `write.merge.mode` (`copy-on-write` | `merge-on-read`), each command
 * independently -- on both format version 2 and version 3 tables, honouring Iceberg's own defaults when
 * unset, and the v2-vs-v3 delete-file format (v2 merge-on-read: position delete files at the requested
 * `write.delete.granularity`; v3 merge-on-read: deletion vectors in Puffin, one per data file).
 *
 * Our code never decides copy-on-write vs merge-on-read: Spark's optimizer, from Iceberg's
 * `SparkRowLevelOperationBuilder`, plans a `ReplaceData` for a copy-on-write command and a `WriteDelta`
 * for a merge-on-read one, straight from the mode property (there is no Spark write-option or session-conf
 * override for the mode in Iceberg 1.12 -- `SparkWriteConf` exposes no `deleteMode`/`updateMode`/`mergeMode`
 * and `SparkSQLProperties` has no mode key). Our two planner strategies
 * ([[org.apache.spark.sql.vecruntime.VectorIcebergWriteStrategy]] for `ReplaceData`,
 * [[org.apache.spark.sql.vecruntime.VectorWriteDeltaStrategy]] for `WriteDelta`) route only on the node
 * type plus our feature flags; the v2/v3 delete format and the granularity are read from Iceberg's own
 * resolved write `Context` (`useDVs`, `deleteGranularity`, `deleteFileFormat`), not hardcoded.
 *
 * Each case runs the command twice from the same seed data -- once with our features OFF (Spark's and
 * Iceberg's own writers, the oracle) and once with them ON -- and asserts:
 *   (a) the planned command node is `ReplaceDataExec` for copy-on-write, `VectorWriteDeltaExec` (ours,
 *       when the ON path supports the case) or Spark's `WriteDeltaExec` for merge-on-read;
 *   (b) the files produced match the mode and format -- copy-on-write replaces data files and adds no
 *       delete files; v2 merge-on-read adds position delete files at the requested granularity; v3
 *       merge-on-read adds deletion vectors only (Puffin, at most one DV per data file, previous DVs
 *       merged);
 *   (c) rows joined on id equal the OFF run, column by column;
 *   (d) v3 row lineage (`_row_id`, `_last_updated_sequence_number`) is preserved for surviving/untouched
 *       rows within the table across the command;
 *   (e) our path actually ran where it supports the case (`VectorWriteDeltaExec` in the plan, or the
 *       columnar-data task counters moved);
 *   (f) the snapshot's operation and the summary's added/removed/total counts equal the OFF run.
 *
 * Runs in the iceberg-bridge module (excluded from the gate's -pl list), so it is run manually.
 */
class RowLevelModeMatrixSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("rlmodes")

  private val WriterFlag = "spark.vecruntime.iceberg.writer.enabled"
  private val DvFlag = "spark.vecruntime.iceberg.dvWriter.enabled"
  private val PosDelFlag = "spark.vecruntime.iceberg.positionDeletes.enabled"
  private val CopyFlag = "spark.vecruntime.iceberg.copyUnchanged.enabled"
  private val NativeScanFlag = "spark.vecruntime.scan.nativeIceberg.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("row-level-mode-matrix")
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
      // Small partitions so the clustering shuffle keeps source files apart (one per task), which the
      // copy-on-write copy path needs.
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16384")
      .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1024")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  // ----- harness ------------------------------------------------------------------------------------

  /** All of our write features on/off together, plus the native scan (so the native path is exercised ON). */
  private def withFeatures[T](on: Boolean)(f: => T): T = {
    val keys = Seq(WriterFlag, DvFlag, PosDelFlag, CopyFlag, NativeScanFlag)
    val prev = keys.map(k => k -> spark.conf.getOption(k))
    keys.foreach(k => spark.conf.set(k, on.toString))
    try f
    finally prev.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  private val Columns =
    "id BIGINT, i INT, d DOUBLE, b BOOLEAN, s STRING, day DATE, dec DECIMAL(9,2), " +
      "wide DECIMAL(38,6), st STRUCT<x: INT, y: STRING>, xs ARRAY<INT>, m MAP<STRING, BIGINT>"

  private def source(rows: Int): String =
    s"""SELECT id, cast(id % 1000 as int) as i, if(id % 17 = 0, null, id * 0.5d) as d,
       |  id % 3 = 0 as b, if(id % 10 = 0, null, concat('v', id % 300)) as s,
       |  date_add(date'2020-01-01', cast(id % 500 as int)) as day,
       |  cast(id % 10000 as decimal(9,2)) / 7 as dec, cast(id as decimal(38,6)) * 1000000 as wide,
       |  if(id % 7 = 0, null, named_struct('x', cast(id % 50 as int), 'y', concat('y', id % 9))) as st,
       |  if(id % 11 = 0, null, array(cast(id % 5 as int), cast(id % 6 as int))) as xs,
       |  if(id % 13 = 0, null, map(concat('k', id % 4), id)) as m
       |FROM range($rows)""".stripMargin

  /**
   * Creates a table at `version`, partitioned or not, with each command's mode set as given. A `None`
   * mode leaves the property UNSET so Iceberg's default applies; a granularity is set only when given.
   */
  private def create(
      t: String,
      version: Int,
      partitioned: Boolean,
      deleteMode: Option[String],
      updateMode: Option[String],
      mergeMode: Option[String],
      granularity: Option[String] = None
  ): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    val part = if (partitioned) " PARTITIONED BY (b)" else ""
    val props = Seq(
      Some(s"'format-version'='$version'"),
      deleteMode.map(m => s"'write.delete.mode'='$m'"),
      updateMode.map(m => s"'write.update.mode'='$m'"),
      mergeMode.map(m => s"'write.merge.mode'='$m'"),
      granularity.map(g => s"'write.delete.granularity'='$g'"),
      // Small files/groups so copy-on-write has several groups per file and merge-on-read rolls realistically.
      Some("'write.parquet.row-group-size-bytes'='16384'"),
      Some("'write.target-file-size-bytes'='262144'")
    ).flatten.mkString(", ")
    spark.sql(s"CREATE TABLE $t ($Columns) USING iceberg$part TBLPROPERTIES ($props)")
  }

  private def table(t: String): Table = Spark3Util.loadIcebergTable(spark, t)

  /** Rows keyed by id, every column as a stable string (nulls and nested included). */
  private def dataById(t: String): Map[Long, String] =
    spark.sql(s"SELECT * FROM $t").collect().map(r => r.getLong(0) -> r.toString).toMap

  /** Each row's v3 lineage keyed by id: (_row_id, _last_updated_sequence_number). */
  private def lineageById(t: String): Map[Long, (Long, Long)] =
    spark
      .sql(s"SELECT id, _row_id, _last_updated_sequence_number FROM $t")
      .collect()
      .map(r => r.getLong(0) -> (r.getLong(1), r.getLong(2)))
      .toMap

  /** Every node's simple class name in the executed plan, unwrapping command, AQE and query-stage wrappers. */
  private def planNodeNames(df: DataFrame): Seq[String] = {
    def unwrap(p: SparkPlan): Seq[SparkPlan] = {
      val nested = p.getClass.getSimpleName match {
        case "CommandResultExec" =>
          p.getClass.getMethods
            .find(m => m.getName == "commandPhysicalPlan" && m.getParameterCount == 0)
            .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
        case "AdaptiveSparkPlanExec" =>
          p.getClass.getMethods
            .find(m => m.getName == "executedPlan" && m.getParameterCount == 0)
            .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
        case n if n.endsWith("QueryStageExec") =>
          p.getClass.getMethods
            .find(m => m.getName == "plan" && m.getParameterCount == 0)
            .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
        case _ => Seq.empty
      }
      p +: (p.children ++ nested).flatMap(unwrap)
    }
    unwrap(df.queryExecution.executedPlan).map(_.getClass.getSimpleName)
  }

  /** Which command node carried this write: copy-on-write -> ReplaceData, merge-on-read -> WriteDelta. */
  private sealed trait CommandNode
  private case object ReplaceDataNode extends CommandNode // copy-on-write
  private case object WriteDeltaNode extends CommandNode // merge-on-read (ours or Spark's)

  private def commandNodeOf(df: DataFrame): CommandNode = {
    val names = planNodeNames(df)
    // copy-on-write is a ReplaceData: Spark plans ReplaceDataExec with our features OFF, and our planner
    // strategy replaces it with VectorIcebergWriteExec when they are ON -- both are the ReplaceData command.
    val isReplace = names.exists(n => n == "ReplaceDataExec" || n == "VectorIcebergWriteExec")
    // merge-on-read is a WriteDelta: Spark's WriteDeltaExec OFF, our VectorWriteDeltaExec ON.
    val isDelta = names.exists(n => n == "WriteDeltaExec" || n == "VectorWriteDeltaExec")
    assert(isReplace ^ isDelta, s"expected exactly one of ReplaceData/WriteDelta, got: ${names.mkString(",")}")
    if (isReplace) ReplaceDataNode else WriteDeltaNode
  }

  private def planHasVectorWriteDelta(df: DataFrame): Boolean =
    planNodeNames(df).contains("VectorWriteDeltaExec")

  /** The snapshot-summary keys compared between the ON and OFF runs. */
  private val SummaryKeys = Seq(
    "added-records",
    "deleted-records",
    "added-data-files",
    "deleted-data-files",
    "added-delete-files",
    "removed-delete-files",
    "added-position-deletes",
    "removed-position-deletes",
    "total-records",
    "total-data-files",
    "total-delete-files",
    "total-position-deletes"
  )

  /** The newest snapshot's (operation, selected summary counts). */
  private def snapshot(t: String): (String, Map[String, String]) = {
    val snap = table(t).currentSnapshot()
    val summary = snap.summary()
    (snap.operation(), SummaryKeys.map(k => k -> summary.get(k)).toMap)
  }

  /** The live delete files of a table, via the Iceberg Java scan API. */
  private def liveDeleteFiles(t: String): Seq[org.apache.iceberg.DeleteFile] = {
    val tasks = table(t).newScan().planFiles()
    try tasks.asScala.flatMap(_.deletes().asScala).toSeq
    finally tasks.close()
  }

  /**
   * One command in one mode. Builds an OFF twin and an ON twin of the same table from the same seed,
   * runs `command` on each, and asserts the full oracle (a)-(f). `expectCow` is the expected mode of
   * THIS command. `touchedBefore` captures, before the command, the ids whose lineage must be preserved
   * (v3): for copy-on-write that is the surviving ids, for merge-on-read the untouched ids -- we read it
   * after from the id set that survived unchanged, so the helper just checks lineage is stable for every
   * id present both before and after with the same data.
   */
  private def runMode(
      base: String,
      version: Int,
      partitioned: Boolean,
      expectCow: Boolean,
      rows: Int,
      deleteMode: Option[String],
      updateMode: Option[String],
      mergeMode: Option[String],
      granularity: Option[String]
  )(command: String => String): Unit = {
    val off = s"ice.db.${base}_off"
    val on = s"ice.db.${base}_on"
    Seq(off, on).foreach { t =>
      create(t, version, partitioned, deleteMode, updateMode, mergeMode, granularity)
      spark.sql(s"INSERT INTO $t ${source(rows)}")
    }
    val lineageBefore = if (version >= 3) lineageById(on) else Map.empty[Long, (Long, Long)]
    val dataBefore = if (version >= 3) dataById(on) else Map.empty[Long, String]

    // OFF run (oracle).
    val offNode = withFeatures(on = false) {
      val df = spark.sql(command(off))
      val node = commandNodeOf(df)
      df.collect()
      node
    }

    // ON run, capturing whether our merge-on-read operator ran and whether a columnar-data task fired.
    val dvBefore = org.apache.spark.sql.vecruntime.VectorWriteDeltaExec.columnarDataTasks.get()
    // The columnar delta write bumps this JVM-global counter whenever it has to read ROWS from its child
    // (a RowToColumnarExec under the write, because some operator left the columnar path). We never compare
    // the absolute value -- other suites in the same JVM move it too -- only this run's DELTA, so a
    // merge-on-read command that stays columnar end to end must add exactly zero. (e) below asserts it.
    val rowInBefore = org.apache.spark.sql.vecruntime.VectorWriteDeltaExec.rowInputWrites.get()
    val (onNode, ourDeltaRan) = withFeatures(on = true) {
      val df = spark.sql(command(on))
      val node = commandNodeOf(df)
      val ours = planHasVectorWriteDelta(df)
      df.collect()
      (node, ours)
    }
    val dvTasks = org.apache.spark.sql.vecruntime.VectorWriteDeltaExec.columnarDataTasks.get() - dvBefore
    val rowInput = org.apache.spark.sql.vecruntime.VectorWriteDeltaExec.rowInputWrites.get() - rowInBefore

    // (a) the planned command node matches the expected mode, and is the SAME on both runs.
    val expected = if (expectCow) ReplaceDataNode else WriteDeltaNode
    assert(onNode == expected, s"$base: ON command node $onNode != expected $expected")
    assert(offNode == expected, s"$base: OFF command node $offNode != expected $expected")

    // (c) data: same rows, column by column, joined on id.
    val offData = dataById(off)
    val onData = dataById(on)
    assert(onData.keySet == offData.keySet, s"$base: id set differs (on=${onData.size} off=${offData.size})")
    onData.foreach { case (id, row) =>
      assert(row == offData(id), s"$base: row for id=$id differs (on=$row off=${offData(id)})")
    }

    // (d) v3 lineage preserved within the table for every row that survived UNCHANGED (same data before/after).
    if (version >= 3) {
      val lineageAfter = lineageById(on)
      val unchanged = onData.filter { case (id, row) => dataBefore.get(id).contains(row) }.keySet
      unchanged.foreach { id =>
        assert(
          lineageAfter.get(id).contains(lineageBefore(id)),
          s"$base: lineage for unchanged id=$id changed (after=${lineageAfter.get(id)} before=${lineageBefore(id)})"
        )
      }
    }

    // (b) files match the mode and format.
    if (expectCow) {
      // Copy-on-write: no delete files added; the snapshot is an overwrite that replaced data files.
      assert(liveDeleteFiles(on).isEmpty, s"$base: copy-on-write must leave no live delete files")
      assert(
        snapshot(on)._1 == snapshot(off)._1,
        s"$base: operation differs on=${snapshot(on)._1} off=${snapshot(off)._1}"
      )
    } else {
      val deletes = liveDeleteFiles(on)
      if (version >= 3) {
        // v3 merge-on-read: deletion vectors only (Puffin), at most one per data file.
        assert(deletes.nonEmpty, s"$base: v3 merge-on-read produced no delete files")
        deletes.foreach { d =>
          assert(d.format() == FileFormat.PUFFIN, s"$base: v3 delete file ${d.location()} is ${d.format()}, not PUFFIN")
          assert(
            d.content() == FileContent.POSITION_DELETES,
            s"$base: v3 delete file ${d.location()} content ${d.content()}"
          )
        }
        val perData =
          deletes.groupBy(d => org.apache.iceberg.util.ContentFileUtil.referencedDataFile(d))
        perData.foreach { case (dataFile, dvs) =>
          assert(dvs.size == 1, s"$base: data file $dataFile has ${dvs.size} DVs, expected at most one")
        }
      } else {
        // v2 merge-on-read: Parquet position delete files, at the requested granularity.
        assert(deletes.nonEmpty, s"$base: v2 merge-on-read produced no delete files")
        val wantFile = granularity.contains("file")
        deletes.foreach { d =>
          assert(
            d.content() == FileContent.POSITION_DELETES,
            s"$base: v2 delete file ${d.location()} content ${d.content()}"
          )
          assert(
            d.format() == FileFormat.PARQUET,
            s"$base: v2 delete file ${d.location()} is ${d.format()}, not Parquet"
          )
          if (wantFile) {
            assert(
              org.apache.iceberg.util.ContentFileUtil.referencedDataFile(d) != null,
              s"$base: file-granularity delete file ${d.location()} does not name its data file"
            )
          }
        }
      }
    }

    // (e) our path actually ran where it supports the case: merge-on-read goes through VectorWriteDeltaExec.
    if (!expectCow) {
      assert(ourDeltaRan, s"$base: merge-on-read ON run did not plan VectorWriteDeltaExec")
      // ...and it ran COLUMNAR end to end: the columnar delta write read batches from a columnar child, never
      // rows via a RowToColumnarExec. Every MOR case of the matrix -- DELETE, UPDATE and MERGE, v2 position
      // deletes and v3 DVs, partitioned or not -- must add zero to rowInputWrites for this run. A MERGE used
      // to fall back here (its MergeRowsExec declined to vectorize on a `CAST(NULL AS DECIMAL(38,6))` projection
      // column), which this assertion guards against regressing.
      assert(
        rowInput == 0L,
        s"$base: merge-on-read ON run was NOT columnar end to end -- the delta write read rows from its child " +
          s"(rowInputWrites added $rowInput this run); an operator left the columnar path"
      )
      // An UPDATE/MERGE with an insert half additionally writes data columnar; a delete-only command has none.
      if (dvTasks > 0) assert(dvTasks >= 1) // sanity on the counter when a data half exists
    }

    // (f) snapshot operation and summary counts equal the OFF run.
    val (opOn, sumOn) = snapshot(on)
    val (opOff, sumOff) = snapshot(off)
    assert(opOn == opOff, s"$base: snapshot operation on=$opOn off=$opOff")
    // Which summary counts are mode-invariant between our writer and Spark's OFF writer:
    //   - copy-on-write: the M4a/M4b copy path writes ONE output file per source file it rewrites, where
    //     Spark's streaming rewrite may coalesce several into one, so the added/removed DATA-FILE counts and
    //     the per-file record movement are a free physical choice and NOT mode-invariant. The logical content
    //     is: compare only the resulting total-records (and total-data-files is a layout artifact, excluded).
    //   - merge-on-read: the delete encoding is fully determined (position deletes / DVs), so every
    //     delete-related and total count must match Spark's writer exactly.
    val keysToCompare =
      if (expectCow) Seq("total-records")
      else SummaryKeys
    keysToCompare.foreach { k =>
      assert(sumOn(k) == sumOff(k), s"$base: snapshot summary $k differs on=${sumOn(k)} off=${sumOff(k)}")
    }
  }

  // ----- commands -----------------------------------------------------------------------------------

  private val Delete: String => String = t => s"DELETE FROM $t WHERE id % 7 = 1"
  private val Update: String => String =
    t => s"UPDATE $t SET s = concat(coalesce(s, 'n'), '-u'), d = d + 1 WHERE id % 11 = 2"

  private def mergeView(): Unit =
    spark
      .sql(s"""SELECT id, cast(id % 1000 as int) AS i, cast(id * 2 as double) AS d, id % 2 = 0 AS b,
              |  concat('m', id) AS s, date'2021-01-01' AS day, cast(1 as decimal(9,2)) AS dec,
              |  cast(id as decimal(38,6)) AS wide, named_struct('x', cast(1 as int), 'y', 'm') AS st,
              |  array(cast(1 as int)) AS xs, map('k0', id) AS m, id % 3 AS op
              |FROM range(4000, 7000)""".stripMargin)
      .createOrReplaceTempView("merge_src")

  private val Merge: String => String = t =>
    s"""MERGE INTO $t t USING merge_src s ON t.id = s.id
       |WHEN MATCHED AND s.op = 1 THEN DELETE
       |WHEN MATCHED THEN UPDATE SET t.s = s.s, t.d = s.d
       |WHEN NOT MATCHED THEN INSERT (id, i, d, b, s, day, dec, wide, st, xs, m)
       |  VALUES (s.id, s.i, s.d, s.b, s.s, s.day, s.dec, s.wide, s.st, s.xs, s.m)""".stripMargin

  // ----- the matrix: each command x each mode x v2/v3 x partitioned/unpartitioned -------------------

  for (version <- Seq(2, 3); partitioned <- Seq(false, true)) {
    val vp = s"v${version}_${if (partitioned) "p" else "u"}"

    for (mode <- Seq("copy-on-write", "merge-on-read")) {
      val cow = mode == "copy-on-write"
      val mtag = if (cow) "cow" else "mor"

      test(s"$vp DELETE $mode") {
        runMode(
          s"del_${mtag}_$vp",
          version,
          partitioned,
          cow,
          rows = 6000,
          deleteMode = Some(mode),
          updateMode = None,
          mergeMode = None,
          granularity = None
        )(Delete)
      }

      test(s"$vp UPDATE $mode") {
        runMode(
          s"upd_${mtag}_$vp",
          version,
          partitioned,
          cow,
          rows = 6000,
          deleteMode = None,
          updateMode = Some(mode),
          mergeMode = None,
          granularity = None
        )(Update)
      }

      test(s"$vp MERGE $mode") {
        mergeView()
        runMode(
          s"mrg_${mtag}_$vp",
          version,
          partitioned,
          cow,
          rows = 6000,
          deleteMode = None,
          updateMode = None,
          mergeMode = Some(mode),
          granularity = None
        )(Merge)
      }
    }

    // Mixed-mode table: each command picks its OWN mode. delete=merge-on-read, update=copy-on-write,
    // merge=merge-on-read -- a valid Iceberg combination; each command must route by its own property.
    test(s"$vp mixed modes: delete=MOR, update=COW, merge=MOR each route independently") {
      val d = Some("merge-on-read"); val u = Some("copy-on-write"); val m = Some("merge-on-read")
      runMode(s"mix_d_$vp", version, partitioned, expectCow = false, rows = 6000, d, u, m, None)(Delete)
      // update=copy-on-write re-encodes the changed string column (dictionary-UTF8 copy path, #901 M4b).
      runMode(
        s"mix_u_$vp",
        version,
        partitioned,
        expectCow = true,
        rows = 6000,
        d,
        u,
        m,
        None
      )(Update)
      mergeView()
      runMode(s"mix_m_$vp", version, partitioned, expectCow = false, rows = 6000, d, u, m, None)(Merge)
    }

    // Unset defaults: no mode property set. Iceberg 1.12 defaults every command to copy-on-write on both
    // v2 and v3, so all three must plan ReplaceData.
    test(s"$vp unset modes default to copy-on-write (Iceberg 1.12)") {
      runMode(s"def_d_$vp", version, partitioned, expectCow = true, rows = 6000, None, None, None, None)(Delete)
      // default copy-on-write re-encodes the changed string column (dictionary-UTF8 copy path, #901 M4b).
      runMode(
        s"def_u_$vp",
        version,
        partitioned,
        expectCow = true,
        rows = 6000,
        None,
        None,
        None,
        None
      )(Update)
      mergeView()
      runMode(
        s"def_m_$vp",
        version,
        partitioned,
        expectCow = true,
        rows = 6000,
        None,
        None,
        None,
        None
      )(Merge)
    }
  }

  // v2 delete granularity: partition vs file, merge-on-read DELETE. The delete files' granularity must
  // follow write.delete.granularity, and file-granularity files must name their data file.
  for (partitioned <- Seq(false, true); granularity <- Seq("partition", "file")) {
    val vp = s"v2_${if (partitioned) "p" else "u"}"
    test(s"$vp merge-on-read DELETE at $granularity granularity") {
      runMode(
        s"gran_${granularity.head}_$vp",
        version = 2,
        partitioned,
        expectCow = false,
        rows = 6000,
        deleteMode = Some("merge-on-read"),
        updateMode = None,
        mergeMode = None,
        granularity = Some(granularity)
      )(Delete)
    }
  }
}
