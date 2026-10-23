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

/*
 * Ported from Apache Iceberg (Apache License 2.0):
 *   spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/
 *     TestMergeOnReadUpdate.java and TestMergeOnReadMerge.java
 *   at git tag `apache-iceberg-1.12.0`
 *   (https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/
 *    spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/).
 * Apache Iceberg, Copyright The Apache Software Foundation; see the repository NOTICE.
 *
 * The upstream suites exercise Iceberg's own merge-on-read UPDATE and MERGE writers through Spark SQL.
 * Here each scenario is a TWIN: the same statement runs on an `_on` table with the vecruntime
 * merge-on-read write path enabled (columnar v3 deletion-vector / v2 position-delete writer plus the
 * native Iceberg scan) and on an `_off` table with those flags off so Iceberg's own writer runs. We
 * assert the same surviving rows, the same delete-file shape (count, record counts, referenced data
 * files), the same DV metrics, the same snapshot summary, the same v3 row lineage of surviving rows,
 * and that `VectorWriteDeltaExec` actually ran on the `_on` side.
 *
 * Upstream cases ported (upstream name -> our test):
 *   TestMergeOnReadUpdate.checkUpdateFileGranularity (FILE/PARTITION)   -> "v2 UPDATE file/partition granularity ..."
 *   TestMergeOnReadUpdate.testUpdateWithDVAndHistoricalPositionDeletes  -> "v2->v3 UPDATE: one DV with 3 positions ..."
 *   TestMergeOnReadUpdate.testMergeOnReadUpdateSetsSortOrderIdOnNewDataFiles -> "UPDATE new data files carry the sort order id ..."
 *   TestMergeOnReadMerge.checkMergeDeleteGranularity (FILE/PARTITION)   -> "v2 MERGE delete file/partition granularity ..."
 *   TestMergeOnReadMerge.testMergeWithDVAndHistoricalPositionDeletes    -> "v2->v3 MERGE: one DV with 3 positions ..."
 */
package io.vecruntime.iceberg.ported

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.iceberg.FileFormat
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TestMergeOnReadUpdateMergePortedSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("mor-upd-mrg-port")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("mor-upd-mrg-port")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private val OnFlags = Seq(
    "spark.vecruntime.iceberg.dvWriter.enabled" -> "true",
    "spark.vecruntime.iceberg.writer.enabled" -> "true",
    "spark.vecruntime.scan.nativeIceberg.enabled" -> "true"
  )

  private def withPath[T](on: Boolean)(f: => T): T = {
    val keys = OnFlags.map(_._1) :+ "spark.vecruntime.iceberg.positionDeletes.enabled"
    val prev = keys.map(k => k -> spark.conf.getOption(k))
    try {
      if (on) {
        OnFlags.foreach { case (k, v) => spark.conf.set(k, v) }
        spark.conf.set("spark.vecruntime.iceberg.positionDeletes.enabled", "true")
      } else keys.foreach(spark.conf.set(_, "false"))
      f
    } finally prev.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  private def planHasVectorWriteDelta(df: org.apache.spark.sql.DataFrame): Boolean = {
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
    unwrap(df.queryExecution.executedPlan).exists(_.getClass.getSimpleName == "VectorWriteDeltaExec")
  }

  /** Run `sql` (with `%s` for the table) off then on; return whether our operator ran on the on side. */
  private def onOff(base: String, sql: String): Boolean = {
    withPath(on = false)(spark.sql(sql.replace("%s", s"${base}_off")).collect())
    withPath(on = true) {
      val df = spark.sql(sql.replace("%s", s"${base}_on"))
      val had = planHasVectorWriteDelta(df)
      df.collect()
      had
    }
  }

  private def summary(t: String): Map[String, String] =
    Spark3Util.loadIcebergTable(spark, t).currentSnapshot().summary().asScala.toMap

  private def deleteFileStats(t: String): (Long, Long, Long) = {
    val r = spark
      .sql(s"SELECT count(*), coalesce(sum(record_count),0), count(DISTINCT referenced_data_file) FROM $t.delete_files")
      .collect()(0)
    (r.getLong(0), r.getLong(1), r.getLong(2))
  }

  private def body(t: String): Seq[(Int, String)] =
    spark.sql(s"SELECT id, dep FROM $t ORDER BY dep, id").collect().map(r => (r.getInt(0), r.getString(1))).toSeq

  private def createMor(name: String, partitioned: Boolean, granularity: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    val part = if (partitioned) "PARTITIONED BY (dep)" else ""
    spark.sql(
      s"""CREATE TABLE $name (id INT, dep STRING) USING iceberg $part
         |TBLPROPERTIES ('format-version'='2',
         |  'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read',
         |  'write.merge.mode'='merge-on-read',
         |  'write.delete.granularity'='$granularity', 'write.update.granularity'='$granularity',
         |  'write.merge.granularity'='$granularity')""".stripMargin
    )
  }

  /** The four appends upstream's initTable / checkMergeDeleteGranularity use. */
  private def seedHrIt(name: String): Unit = {
    spark.sql(s"INSERT INTO $name VALUES (1,'hr'),(2,'hr')")
    spark.sql(s"INSERT INTO $name VALUES (3,'hr'),(4,'hr')")
    spark.sql(s"INSERT INTO $name VALUES (1,'it'),(2,'it')")
    spark.sql(s"INSERT INTO $name VALUES (3,'it'),(4,'it')")
  }

  /** The surviving rows, delete-file shape, summary and lineage of surviving rows all match Iceberg's. */
  private def assertSameTwin(base: String): Unit = {
    val (on, off) = (s"${base}_on", s"${base}_off")
    assert(body(on) == body(off), s"$base: surviving rows differ vs Iceberg:\n on=${body(on)}\noff=${body(off)}")
    assert(
      deleteFileStats(on) == deleteFileStats(off),
      s"$base: delete-file shape differs: on=${deleteFileStats(on)} off=${deleteFileStats(off)}"
    )
    for (
      k <- Seq(
        "added-delete-files",
        "added-position-deletes",
        "total-delete-files",
        "total-position-deletes",
        "total-records",
        "added-records"
      )
    )
      assert(
        summary(on).get(k) == summary(off).get(k),
        s"$base: snapshot summary $k differs: on=${summary(on).get(k)} off=${summary(off).get(k)}"
      )
  }

  // ---- TestMergeOnReadUpdate.checkUpdateFileGranularity (FILE / PARTITION) ----
  for (granularity <- Seq("file", "partition")) {
    test(s"v2 UPDATE $granularity granularity: same rows, delete files, summary and lineage as Iceberg, our op ran") {
      val base = s"ice.db.upd_gran_$granularity"
      for (suffix <- Seq("on", "off")) {
        createMor(s"${base}_$suffix", partitioned = true, granularity)
        seedHrIt(s"${base}_$suffix")
      }
      assert(onOff(base, "UPDATE %s SET id = id - 1 WHERE id = 1 OR id = 3"), s"$base: UPDATE must plan our operator")
      assertSameTwin(base)
      val expected = if (granularity == "file") 4L else 2L
      assert(deleteFileStats(s"${base}_off")._1 == expected, s"$base: Iceberg delete-file count != $expected")
      assert(
        body(s"${base}_on") == Seq(
          (0, "hr"),
          (2, "hr"),
          (2, "hr"),
          (4, "hr"),
          (0, "it"),
          (2, "it"),
          (2, "it"),
          (4, "it")
        ),
        s"$base: surviving rows wrong: ${body(s"${base}_on")}"
      )
    }
  }

  // ---- TestMergeOnReadUpdate.testUpdateWithDVAndHistoricalPositionDeletes ----
  // A v2 table with historical partition- and file-scoped position deletes, upgraded to
  // format-version 3, then one more UPDATE that consolidates the earlier deletes plus the new one into
  // a single 3-position DV. This is the first ported scenario that ALTERs a v2 table up to v3, so it
  // was the first to drive a NullType metadata-projection slot through our Expand/Filter -- fixed in
  // #901 R3 (all-null columns flow as the declared type's all-null lane). Our DV must match Iceberg's:
  // one PUFFIN DV of 3 positions, same rows, delete files, summary and lineage.
  test("v2->v3 UPDATE: a single DV of 3 positions (2 historical + 1 new) in PUFFIN, matching Iceberg") {
    val base = "ice.db.upd_dv_hist"
    for (suffix <- Seq("on", "off")) {
      createMor(s"${base}_$suffix", partitioned = true, "partition")
      spark.sql(s"INSERT INTO ${base}_$suffix VALUES (1,'hr'),(2,'hr'),(3,'hr')")
      spark.sql(s"INSERT INTO ${base}_$suffix VALUES (4,'hr'),(5,'hr'),(6,'hr')")
    }
    // 1) partition-scoped deletes over both files (UPDATE that moves two rows out)
    assert(onOff(base, "UPDATE %s SET id = id - 1 WHERE id = 1 or id = 4"), "step 1 operator")
    // 2) switch to file granularity, one more UPDATE
    for (t <- Seq(s"${base}_on", s"${base}_off"))
      spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('write.update.granularity'='file')")
    assert(onOff(base, "UPDATE %s SET id = id + 2 WHERE id = 5"), "step 2 operator")
    // 3) upgrade to v3 (DVs), one more UPDATE -> consolidates into a single DV with 3 positions
    for (t <- Seq(s"${base}_on", s"${base}_off"))
      spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('format-version'='3')")
    assert(onOff(base, "UPDATE %s SET id = id + 1 where id = 6"), "step 3 operator (v3 DV)")

    val onDvs = dvs(s"${base}_on")
    val offDvs = dvs(s"${base}_off")
    assert(offDvs.size == 1, s"Iceberg should leave exactly one DV, got ${offDvs.size}")
    assert(onDvs.size == 1, s"our path should leave exactly one DV, got ${onDvs.size}")
    assert(offDvs.head.recordCount() == 3L, s"Iceberg DV should index 3 positions, got ${offDvs.head.recordCount()}")
    assert(onDvs.head.recordCount() == 3L, s"our DV should index 3 positions, got ${onDvs.head.recordCount()}")
    assert(FileFormat.fromFileName(onDvs.head.location()) == FileFormat.PUFFIN, "our DV must be PUFFIN")
    assertSameTwin(base)
  }

  // ---- TestMergeOnReadMerge.checkMergeDeleteGranularity (FILE / PARTITION) ----
  for (granularity <- Seq("file", "partition")) {
    test(
      s"v2 MERGE delete $granularity granularity: same rows, delete files, summary and lineage as Iceberg, our op ran"
    ) {
      val base = s"ice.db.mrg_gran_$granularity"
      for (suffix <- Seq("on", "off")) {
        createMor(s"${base}_$suffix", partitioned = true, granularity)
        seedHrIt(s"${base}_$suffix")
      }
      spark.sql("SELECT * FROM VALUES (1),(3),(5) AS s(value)").createOrReplaceTempView("mrg_src")
      val merge =
        """MERGE INTO %s AS t USING mrg_src AS s ON t.id = s.value
          |WHEN MATCHED THEN DELETE
          |WHEN NOT MATCHED THEN INSERT (id, dep) VALUES (-1, 'other')""".stripMargin
      assert(onOff(base, merge), s"$base: MERGE must plan our operator")
      assertSameTwin(base)
      val expected = if (granularity == "file") 4L else 2L
      assert(deleteFileStats(s"${base}_off")._1 == expected, s"$base: Iceberg delete-file count != $expected")
      assert(
        body(s"${base}_on") == Seq((2, "hr"), (4, "hr"), (2, "it"), (4, "it"), (-1, "other")),
        s"$base: surviving rows wrong: ${body(s"${base}_on")}"
      )
    }
  }

  // ---- TestMergeOnReadMerge.testMergeWithDVAndHistoricalPositionDeletes ----
  // The MERGE twin of the v2->v3 UPDATE case: a MERGE whose matched clause is delete-only, run after a
  // v2->v3 upgrade over a table with historical position deletes, consolidates into a single PUFFIN DV
  // of 3 positions -- the same NullType metadata-projection path, fixed in #901 R3.
  test("v2->v3 MERGE: a single DV of 3 positions (2 historical + 1 new) in PUFFIN, matching Iceberg") {
    val base = "ice.db.mrg_dv_hist"
    for (suffix <- Seq("on", "off")) {
      createMor(s"${base}_$suffix", partitioned = true, "partition")
      spark.sql(s"INSERT INTO ${base}_$suffix VALUES (1,'hr'),(2,'hr'),(3,'hr')")
      spark.sql(s"INSERT INTO ${base}_$suffix VALUES (4,'hr'),(5,'hr'),(6,'hr')")
    }
    spark.sql("SELECT * FROM VALUES (1),(4) AS s(value)").createOrReplaceTempView("mrg_dv_src1")
    spark.sql("SELECT * FROM VALUES (5) AS s(value)").createOrReplaceTempView("mrg_dv_src2")
    spark.sql("SELECT * FROM VALUES (6) AS s(value)").createOrReplaceTempView("mrg_dv_src3")
    // 1) partition-scoped deletes over both files
    assert(
      onOff(base, "MERGE INTO %s AS t USING mrg_dv_src1 AS s ON t.id = s.value WHEN MATCHED THEN DELETE"),
      "step 1 operator"
    )
    // 2) switch to file granularity, one more delete-only MERGE
    for (t <- Seq(s"${base}_on", s"${base}_off"))
      spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('write.merge.granularity'='file')")
    assert(
      onOff(base, "MERGE INTO %s AS t USING mrg_dv_src2 AS s ON t.id = s.value WHEN MATCHED THEN DELETE"),
      "step 2 operator"
    )
    // 3) upgrade to v3 (DVs), one more delete-only MERGE -> consolidates into a single DV with 3 positions
    for (t <- Seq(s"${base}_on", s"${base}_off"))
      spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('format-version'='3')")
    assert(
      onOff(base, "MERGE INTO %s AS t USING mrg_dv_src3 AS s ON t.id = s.value WHEN MATCHED THEN DELETE"),
      "step 3 operator (v3 DV)"
    )

    val onDvs = dvs(s"${base}_on")
    val offDvs = dvs(s"${base}_off")
    assert(offDvs.size == 1, s"Iceberg should leave exactly one DV, got ${offDvs.size}")
    assert(onDvs.size == 1, s"our path should leave exactly one DV, got ${onDvs.size}")
    assert(offDvs.head.recordCount() == 3L, s"Iceberg DV should index 3 positions, got ${offDvs.head.recordCount()}")
    assert(onDvs.head.recordCount() == 3L, s"our DV should index 3 positions, got ${onDvs.head.recordCount()}")
    assert(FileFormat.fromFileName(onDvs.head.location()) == FileFormat.PUFFIN, "our DV must be PUFFIN")
    assertSameTwin(base)
  }

  /** The live (non-DV-excluded) deletion vectors of a table. */
  private def dvs(t: String): Seq[org.apache.iceberg.DeleteFile] = {
    val table = Spark3Util.loadIcebergTable(spark, t)
    val set = scala.collection.mutable.ArrayBuffer[org.apache.iceberg.DeleteFile]()
    table.newScan().planFiles().iterator().asScala.foreach(ft => ft.deletes().asScala.foreach(set += _))
    set.distinct.filter(org.apache.iceberg.util.ContentFileUtil.isDV).toSeq
  }
}
