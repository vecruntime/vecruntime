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
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Merge-on-read `MERGE INTO` on tables carrying map columns, stressing exactly the map shapes the
 * [[RowLevelModeMatrixSuite]] does not: a `map<string, bigint>`, a `map<string, struct<...>>`, and a
 * map nested inside a struct (`struct<..., mm: map<string, bigint>>`). Each shape runs on both format
 * versions (v2 position deletes, v3 deletion vectors), partitioned and unpartitioned, and from both
 * source shapes a `MERGE` can have -- an ordinary table scan and a `VALUES`/`LocalTableScan` -- with a
 * matched UPDATE, a matched DELETE and a not-matched INSERT clause all present, so every clause
 * projects the map columns through our [[org.apache.spark.sql.vecruntime.VectorMergeRowsExec]].
 *
 * The oracle is Iceberg's own writer: a twin `_off` table runs the identical `MERGE` with our write
 * features off, and the `_on` table's rows (read back through Iceberg's own reader, features off) must
 * equal it column by column, with the same snapshot summary and delete-file shape. The suite then
 * asserts the `_on` run stayed columnar end to end -- `VectorWriteDeltaExec` in the plan and a
 * suite-local `rowInputWrites` delta of exactly zero -- which is the property the docs row
 * "Merge-on-read on tables with map columns" was about: a `MERGE` whose map-typed projection used to
 * drop to a row input (`RowToColumnarExec` under the delta write).
 *
 * Runs in the iceberg-bridge module (excluded from the gate's -pl list), so it is run manually.
 */
class MorMergeMapColumnSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("mor-merge-map")

  private val OnFlags = Seq(
    "spark.vecruntime.iceberg.writer.enabled",
    "spark.vecruntime.iceberg.dvWriter.enabled",
    "spark.vecruntime.iceberg.positionDeletes.enabled",
    "spark.vecruntime.scan.nativeIceberg.enabled",
    // A VALUES source is a LocalTableScan; with it columnar the MERGE's join, project and row operator
    // convert too, so the delta write reads our columnar exchange rather than a row round trip. In
    // production the source is an ordinary scan, already columnar; the table-scan twin exercises that.
    "spark.vecruntime.exec.localTableScan.enabled"
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("mor-merge-map")
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

  // ----- harness ------------------------------------------------------------------------------------

  private def withFeatures[T](on: Boolean)(f: => T): T = {
    val prev = OnFlags.map(k => k -> spark.conf.getOption(k))
    OnFlags.foreach(k => spark.conf.set(k, on.toString))
    try f
    finally prev.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

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

  private def rows(t: String): Map[Long, String] =
    spark.sql(s"SELECT * FROM $t").collect().map(r => r.getLong(0) -> r.toString).toMap

  private def summary(t: String): Map[String, String] =
    Spark3Util.loadIcebergTable(spark, t).currentSnapshot().summary().asScala.toMap

  private def liveDeleteFiles(t: String): Seq[org.apache.iceberg.DeleteFile] = {
    val tasks = Spark3Util.loadIcebergTable(spark, t).newScan().planFiles()
    try tasks.asScala.flatMap(_.deletes().asScala).toSeq
    finally tasks.close()
  }

  private val SummaryKeys = Seq(
    "added-records",
    "deleted-records",
    "added-data-files",
    "added-delete-files",
    "removed-delete-files",
    "added-position-deletes",
    "removed-position-deletes",
    "total-records",
    "total-delete-files",
    "total-position-deletes"
  )

  /**
   * The seed, its map column(s), the source projection and the MERGE statement all vary by the map
   * shape under test. `columns` is the table DDL body, `seed` the `SELECT` producing `rows` seed rows,
   * `srcProject` the source columns' SQL, and `insertCols`/`insertVals` the not-matched INSERT clause.
   */
  private case class Shape(
      name: String,
      columns: String,
      seed: Int => String,
      srcProject: String,
      insertCols: String,
      insertVals: String,
      // The matched UPDATE assignments: they must touch a map column so the UPDATE clause projects a map.
      updateSet: String
  )

  private val MapBigint = Shape(
    name = "map_bigint",
    columns = "id BIGINT, i INT, s STRING, m MAP<STRING, BIGINT>",
    seed = n => s"""SELECT id, cast(id % 100 as int) as i, concat('v', id % 7) as s,
                   |  if(id % 13 = 0, null, map(concat('k', id % 4), id, 'c', id * 2)) as m
                   |FROM range($n)""".stripMargin,
    srcProject = "s.id, s.i, s.s, s.m",
    insertCols = "id, i, s, m",
    insertVals = "s.id, s.i, s.s, s.m",
    updateSet = "t.s = s.s, t.m = s.m"
  )

  private val MapStruct = Shape(
    name = "map_struct",
    columns = "id BIGINT, i INT, s STRING, m MAP<STRING, STRUCT<x: INT, y: STRING>>",
    seed = n =>
      s"""SELECT id, cast(id % 100 as int) as i, concat('v', id % 7) as s,
                   |  if(id % 13 = 0, null,
                   |     map(concat('k', id % 4), named_struct('x', cast(id % 5 as int), 'y', concat('y', id % 3)))) as m
                   |FROM range($n)""".stripMargin,
    srcProject = "s.id, s.i, s.s, s.m",
    insertCols = "id, i, s, m",
    insertVals = "s.id, s.i, s.s, s.m",
    updateSet = "t.s = s.s, t.m = s.m"
  )

  private val MapInStruct = Shape(
    name = "map_in_struct",
    columns = "id BIGINT, i INT, s STRING, st STRUCT<x: INT, mm: MAP<STRING, BIGINT>>",
    seed = n => s"""SELECT id, cast(id % 100 as int) as i, concat('v', id % 7) as s,
                   |  if(id % 13 = 0, null,
                   |     named_struct('x', cast(id % 9 as int), 'mm', map(concat('k', id % 4), id))) as st
                   |FROM range($n)""".stripMargin,
    srcProject = "s.id, s.i, s.s, s.st",
    insertCols = "id, i, s, st",
    insertVals = "s.id, s.i, s.s, s.st",
    updateSet = "t.s = s.s, t.st = s.st"
  )

  private val Shapes = Seq(MapBigint, MapStruct, MapInStruct)

  private def create(t: String, shape: Shape, version: Int, partitioned: Boolean): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    val part = if (partitioned) " PARTITIONED BY (i)" else ""
    spark.sql(
      s"""CREATE TABLE $t (${shape.columns}) USING iceberg$part
         |TBLPROPERTIES ('format-version'='$version',
         |  'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read',
         |  'write.merge.mode'='merge-on-read',
         |  'write.parquet.row-group-size-bytes'='16384', 'write.target-file-size-bytes'='262144')""".stripMargin
    )
  }

  /**
   * One MERGE case. Seeds an `_off` twin (Iceberg's writer) and an `_on` twin (ours) from the same
   * `/*+ REPARTITION(1) */` seed, runs the identical MERGE on each, and asserts equal rows, summary and
   * delete-file shape, our operator ran, and the on run was columnar end to end (suite-local
   * `rowInputWrites` delta == 0).
   */
  private def runMerge(shape: Shape, version: Int, partitioned: Boolean, valuesSource: Boolean): Unit = {
    val tag = s"${shape.name}_v${version}_${if (partitioned) "p" else "u"}_${if (valuesSource) "val" else "tbl"}"
    val off = s"ice.db.mm_${tag}_off"
    val on = s"ice.db.mm_${tag}_on"
    val seedRows = 4000

    Seq(off, on).foreach { t =>
      create(t, shape, version, partitioned)
      spark.sql(s"INSERT INTO $t SELECT /*+ REPARTITION(1) */ * FROM (${shape.seed(seedRows)})")
    }

    // The source overlaps the lower half of the table (matched: UPDATE or DELETE by op) and extends past
    // it (not matched: INSERT). `op` drives DELETE vs UPDATE on matched rows.
    val srcSelect =
      s"""SELECT id, cast(id % 100 as int) AS i, concat('m', id) AS s,
         |  ${mapExprFor(shape)} AS ${mapColOf(shape)}, cast(id % 3 as int) AS op
         |FROM range(2000, 6000)""".stripMargin

    val mergeSql = (src: String) =>
      s"""MERGE INTO $src t USING merge_src s ON t.id = s.id
         |WHEN MATCHED AND s.op = 1 THEN DELETE
         |WHEN MATCHED THEN UPDATE SET ${shape.updateSet}
         |WHEN NOT MATCHED THEN INSERT (${shape.insertCols}) VALUES (${shape.insertVals})""".stripMargin

    // OFF (oracle): Iceberg's own writer.
    withFeatures(on = false) {
      prepareSource(srcSelect, valuesSource)
      spark.sql(mergeSql(off)).collect()
    }

    // ON: ours. Snapshot the JVM-global counter HERE, immediately before, and assert this run's delta only.
    val (ourRan, rowInputDelta) = withFeatures(on = true) {
      prepareSource(srcSelect, valuesSource)
      val df = spark.sql(mergeSql(on))
      val ran = planNodeNames(df).contains("VectorWriteDeltaExec")
      val before = org.apache.spark.sql.vecruntime.VectorWriteDeltaExec.rowInputWrites.get()
      df.collect()
      val after = org.apache.spark.sql.vecruntime.VectorWriteDeltaExec.rowInputWrites.get()
      (ran, after - before)
    }

    // (a) our merge-on-read operator ran.
    assert(ourRan, s"$tag: MERGE did not plan VectorWriteDeltaExec")

    // (b) columnar end to end: the delta write never read rows (no RowToColumnarExec under it).
    assert(
      rowInputDelta == 0L,
      s"$tag: merge-on-read MERGE was NOT columnar end to end -- the delta write read rows " +
        s"(rowInputWrites added $rowInputDelta this run); a map-typed projection left the columnar path"
    )

    // (c) rows equal Iceberg's writer, column by column, read back through Iceberg's own reader.
    val onData = withFeatures(on = false)(rows(on))
    val offData = withFeatures(on = false)(rows(off))
    assert(onData.keySet == offData.keySet, s"$tag: id set differs (on=${onData.size} off=${offData.size})")
    onData.foreach { case (id, row) =>
      assert(row == offData(id), s"$tag: row for id=$id differs\n on=$row\noff=${offData(id)}")
    }

    // (d) v2 position deletes / v3 deletion vectors, matching Iceberg's delete-file shape.
    val onDeletes = liveDeleteFiles(on)
    assert(onDeletes.nonEmpty, s"$tag: merge-on-read produced no delete files")
    if (version >= 3) {
      onDeletes.foreach(d =>
        assert(
          d.format() == org.apache.iceberg.FileFormat.PUFFIN,
          s"$tag: v3 delete file ${d.location()} is ${d.format()}, not PUFFIN"
        )
      )
    }

    // (e) snapshot operation and summary counts equal the OFF run.
    assert(
      Spark3Util.loadIcebergTable(spark, on).currentSnapshot().operation() ==
        Spark3Util.loadIcebergTable(spark, off).currentSnapshot().operation(),
      s"$tag: snapshot operation differs"
    )
    val (sumOn, sumOff) = (summary(on), summary(off))
    SummaryKeys.foreach(k =>
      assert(
        sumOn.get(k) == sumOff.get(k),
        s"$tag: snapshot summary $k differs on=${sumOn.get(k)} off=${sumOff.get(k)}"
      )
    )
  }

  private def mapColOf(shape: Shape): String = if (shape eq MapInStruct) "st" else "m"

  /** The source's map/struct column expression, matching the table's declared type for that shape. */
  private def mapExprFor(shape: Shape): String = shape match {
    case MapBigint => "map(concat('mk', id % 3), id, 'mc', id + 1)"
    case MapStruct => "map(concat('mk', id % 3), named_struct('x', cast(id % 4 as int), 'y', concat('sy', id % 2)))"
    case MapInStruct => "named_struct('x', cast(id % 6 as int), 'mm', map(concat('mk', id % 3), id))"
    case _ => sys.error("unknown shape")
  }

  /**
   * The MERGE source `merge_src`: either a `VALUES`/`LocalTableScan` or an ordinary table-backed temp
   * view (a scan). The VALUES branch collects the seed rows to the driver and rebuilds them as a local
   * `Seq[Row]` relation, which Spark plans as a `LocalTableScanExec`; the scan branch keeps the query
   * (a `BatchScan`/range leaf). Both carry every column the clauses read.
   */
  private def prepareSource(srcSelect: String, valuesSource: Boolean): Unit = {
    val df = spark.sql(srcSelect)
    if (!valuesSource) df.createOrReplaceTempView("merge_src")
    else {
      val localRows: Seq[Row] = df.collect().toSeq
      // A local Seq[Row] relation -> LocalRelation -> LocalTableScanExec at execution.
      spark.createDataFrame(localRows.asJava, df.schema).createOrReplaceTempView("merge_src")
    }
  }

  // ----- the matrix ---------------------------------------------------------------------------------

  for (shape <- Shapes; version <- Seq(2, 3); partitioned <- Seq(false, true); valuesSource <- Seq(false, true)) {
    val src = if (valuesSource) "VALUES source" else "table source"
    val vp = s"v$version ${if (partitioned) "partitioned" else "unpartitioned"}"
    test(s"MERGE merge-on-read, ${shape.name}, $vp, $src: columnar end to end, rows match Iceberg") {
      runMerge(shape, version, partitioned, valuesSource)
    }
  }
}
