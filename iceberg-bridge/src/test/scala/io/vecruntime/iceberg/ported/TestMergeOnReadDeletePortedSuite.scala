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
 *     TestMergeOnReadDelete.java
 *   at git tag `apache-iceberg-1.12.0`
 *   (https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/
 *    spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestMergeOnReadDelete.java)
 * Apache Iceberg, Copyright The Apache Software Foundation; see the repository NOTICE.
 *
 * The upstream suite exercises Iceberg's own merge-on-read DELETE writer through Spark SQL. Here each
 * scenario is re-expressed as a TWIN: the same DELETE is run on an `_on` table with the vecruntime
 * merge-on-read write path enabled (our columnar v3 deletion-vector writer / v2 position-delete writer
 * via `spark.vecruntime.iceberg.dvWriter.enabled` + `spark.vecruntime.iceberg.writer.enabled`, and the
 * native Iceberg scan via `spark.vecruntime.scan.nativeIceberg.enabled`) and on an `_off` table with
 * those flags off so Iceberg's own writer and reader run. We then assert that our path produces the
 * SAME surviving rows (joined on id), the SAME delete-file shape (count, content type, record counts,
 * referenced data files), the SAME DV metrics, the SAME snapshot summaries, and -- unlike the pure
 * twin -- that `VectorWriteDeltaExec` actually ran on the `_on` side.
 *
 * Upstream cases ported (upstream name -> our test):
 *   testDeleteFileGranularity / testDeletePartitionGranularity -> "v2 DELETE file vs partition granularity ..."
 *   testPositionDeletesAreMaintainedDuringDelete                -> "v2 file-granularity DELETE maintains ..." (partitioned)
 *   testUnpartitionedPositionDeletesAreMaintainedDuringDelete   -> "v2 file-granularity DELETE maintains ..." (unpartitioned)
 *   testDeleteWithDVAndHistoricalPositionDeletes                -> "v2->v3 DELETE: one DV with 3 positions ..."
 *   testAggregatePushDownInMergeOnReadDelete                    -> "aggregate push-down is disabled over MoR deletes ..."
 */
package io.vecruntime.iceberg.ported

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.util.ContentFileUtil
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TestMergeOnReadDeletePortedSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("mor-delete-port")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("mor-delete-port")
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

  // ---- flag plumbing: the vecruntime merge-on-read write path, both the delete and data halves ----

  private val OnFlags = Seq(
    "spark.vecruntime.iceberg.dvWriter.enabled" -> "true",
    "spark.vecruntime.iceberg.writer.enabled" -> "true",
    "spark.vecruntime.scan.nativeIceberg.enabled" -> "true"
  )

  /** Run `f` with our write path on (`_on`) or forced off so Iceberg's own writer runs (`_off`). */
  private def withPath[T](on: Boolean)(f: => T): T = {
    val keys = OnFlags.map(_._1) :+ "spark.vecruntime.iceberg.positionDeletes.enabled"
    val prev = keys.map(k => k -> spark.conf.getOption(k))
    try {
      if (on) {
        OnFlags.foreach { case (k, v) => spark.conf.set(k, v) }
        spark.conf.set("spark.vecruntime.iceberg.positionDeletes.enabled", "true")
      } else {
        // Off: Iceberg's own writer and reader on both the delete and data halves.
        keys.foreach(spark.conf.set(_, "false"))
      }
      f
    } finally prev.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  private def rows(t: String): Array[Row] = spark.sql(s"SELECT * FROM $t ORDER BY id, dep").collect()
  private def count(sql: String): Long = spark.sql(sql).collect()(0).getLong(0)

  /** True if VectorWriteDeltaExec ran, unwrapping command/AQE/query-stage wrappers (as the live suites do). */
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

  /** Run a DELETE off then on; return whether our operator ran on the on side. */
  private def deleteOnOff(base: String, whereClause: String): Boolean = {
    withPath(on = false)(spark.sql(s"DELETE FROM ${base}_off WHERE $whereClause").collect())
    withPath(on = true) {
      val df = spark.sql(s"DELETE FROM ${base}_on WHERE $whereClause")
      val had = planHasVectorWriteDelta(df)
      df.collect()
      had
    }
  }

  private def createGran(name: String, partitioned: Boolean, fmt: Int, granularity: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    val part = if (partitioned) "PARTITIONED BY (dep)" else ""
    spark.sql(
      s"""CREATE TABLE $name (id INT, dep STRING) USING iceberg $part
         |TBLPROPERTIES ('format-version'='$fmt', 'write.delete.mode'='merge-on-read',
         |  'write.delete.granularity'='$granularity')""".stripMargin
    )
  }

  private def append(name: String, rows: Seq[(Int, String)]): Unit = {
    val vals = rows.map { case (i, d) => s"($i, '$d')" }.mkString(", ")
    spark.sql(s"INSERT INTO $name VALUES $vals")
  }

  /** The current snapshot's summary of a table. */
  private def summary(t: String): Map[String, String] =
    Spark3Util.loadIcebergTable(spark, t).currentSnapshot().summary().asScala.toMap

  /** Count of live delete files, and whether all are DVs (Puffin). */
  private def deleteFileStats(t: String): (Long, Long, Long) = {
    val r = spark
      .sql(s"SELECT count(*), coalesce(sum(record_count),0), count(DISTINCT referenced_data_file) FROM $t.delete_files")
      .collect()(0)
    (r.getLong(0), r.getLong(1), r.getLong(2))
  }

  private def assertSameTwin(base: String): Unit = {
    val (on, off) = (s"${base}_on", s"${base}_off")
    assert(rows(on).sameElements(rows(off)), s"$base: surviving rows differ between our path and Iceberg's")
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
        "total-records"
      )
    )
      assert(
        summary(on).get(k) == summary(off).get(k),
        s"$base: snapshot summary $k differs: on=${summary(on).get(k)} off=${summary(off).get(k)}"
      )
  }

  // ---- testDeleteFileGranularity / testDeletePartitionGranularity ----
  for (granularity <- Seq("file", "partition")) {
    test(s"v2 DELETE $granularity granularity: identical rows, delete files and summary vs Iceberg, our op ran") {
      val base = s"ice.db.del_gran_$granularity"
      for (suffix <- Seq("on", "off")) createGran(s"${base}_$suffix", partitioned = true, fmt = 2, granularity)
      for (t <- Seq(s"${base}_on", s"${base}_off")) {
        append(t, Seq((1, "hr"), (2, "hr")))
        append(t, Seq((3, "hr"), (4, "hr")))
        append(t, Seq((1, "hardware"), (2, "hardware")))
        append(t, Seq((3, "hardware"), (4, "hardware")))
      }
      val had = deleteOnOff(base, "id = 1 OR id = 3")
      assert(had, s"$base: VectorWriteDeltaExec must run on a v2 MoR DELETE")
      assertSameTwin(base)
      // Upstream asserts FILE -> 4 delete files, PARTITION -> 2; our path must match Iceberg's, which
      // this twin checks directly, and we also pin the absolute count so a regression in either path shows.
      val expected = if (granularity == "file") 4L else 2L
      assert(deleteFileStats(s"${base}_off")._1 == expected, s"$base: Iceberg delete-file count != $expected")
      assert(
        rows(s"${base}_on").map(r => (r.getInt(0), r.getString(1))).toSeq ==
          Seq((2, "hardware"), (2, "hr"), (4, "hardware"), (4, "hr")),
        s"$base: surviving rows wrong"
      )
    }
  }

  // ---- testPositionDeletesAreMaintainedDuringDelete / ...Unpartitioned... ----
  for (partitioned <- Seq(true, false)) {
    val kind = if (partitioned) "partitioned" else "unpartitioned"
    test(s"v2 $kind file-granularity DELETE maintains prior position deletes (one removed-delete-file)") {
      // Upstream's (un)partitioned variants use schema (id, data), the partitioned one PARTITIONED BY (id),
      // and write the five rows coalesced to a single data file, then delete by `data` (a non-partition
      // column) so each delete is genuinely row-level and lands as a file-scoped position delete that the
      // next delete rewrites. Deleting by the partition column would instead let Iceberg prove the delete
      // from partition stats and plan no row-level writer at all.
      val base = s"ice.db.del_maint_${if (partitioned) "p" else "u"}"
      val part = if (partitioned) "PARTITIONED BY (id)" else ""
      for (suffix <- Seq("on", "off")) {
        val t = s"${base}_$suffix"
        spark.sql(s"DROP TABLE IF EXISTS $t")
        spark.sql(
          s"""CREATE TABLE $t (id INT, data STRING) USING iceberg $part
             |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
             |  'write.delete.granularity'='file')""".stripMargin
        )
        // coalesce(1) so the five rows share one data file, as upstream does.
        spark
          .sql("SELECT * FROM VALUES (1,'a'),(1,'b'),(1,'c'),(2,'d'),(2,'e') AS t(id, data)")
          .coalesce(1)
          .writeTo(t)
          .append()
      }
      // Three successive deletes against the same data file; the file-scoped delete is rewritten each time.
      for (w <- Seq("id = 1 and data='a'", "id = 2 and data='d'", "id = 1 and data='c'"))
        assert(deleteOnOff(base, w), s"$base: our operator must run for '$w'")
      // Rows, delete-file shape and summary match Iceberg; compare on (id, data).
      def body(t: String): Seq[(Int, String)] =
        spark.sql(s"SELECT id, data FROM $t ORDER BY id, data").collect().map(r => (r.getInt(0), r.getString(1))).toSeq
      assert(body(s"${base}_on") == body(s"${base}_off"), s"$base: surviving rows differ vs Iceberg")
      assert(deleteFileStats(s"${base}_on") == deleteFileStats(s"${base}_off"), s"$base: delete-file shape differs")
      // The latest snapshot removed exactly one (rewritten) delete file, on both paths.
      assert(
        summary(s"${base}_on").get("removed-delete-files") == summary(s"${base}_off").get("removed-delete-files"),
        s"$base: removed-delete-files differ"
      )
      assert(
        summary(s"${base}_off").getOrElse("removed-delete-files", "0") == "1",
        s"$base: Iceberg should remove exactly one delete file"
      )
      assert(body(s"${base}_on") == Seq((1, "b"), (2, "e")), s"$base: surviving rows wrong")
    }
  }

  // ---- testDeleteWithDVAndHistoricalPositionDeletes ----
  test("v2->v3 DELETE: a single DV of 3 positions (2 historical + 1 new) in PUFFIN, matching Iceberg") {
    val base = "ice.db.del_dv_hist"
    for (suffix <- Seq("on", "off")) createGran(s"${base}_$suffix", partitioned = true, fmt = 2, "partition")
    for (t <- Seq(s"${base}_on", s"${base}_off")) {
      spark.sql(s"INSERT INTO $t VALUES (1,'hr'),(2,'hr'),(3,'hr')")
      spark.sql(s"INSERT INTO $t VALUES (4,'hr'),(5,'hr'),(6,'hr')")
    }

    // 1) partition-scoped deletes over both files
    assert(deleteOnOff(base, "id = 1 or id = 4"), "step 1 operator")
    // 2) switch to file granularity, one more delete
    for (t <- Seq(s"${base}_on", s"${base}_off"))
      spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('write.delete.granularity'='file')")
    assert(deleteOnOff(base, "id = 5"), "step 2 operator")
    // 3) upgrade to v3 (DVs), one more delete -> consolidates into a single DV with 3 positions on the 2nd file
    for (t <- Seq(s"${base}_on", s"${base}_off"))
      spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('format-version'='3')")
    assert(deleteOnOff(base, "id = 6"), "step 3 operator (v3 DV)")

    def dvs(t: String): Seq[org.apache.iceberg.DeleteFile] = {
      val table = Spark3Util.loadIcebergTable(spark, t)
      val snap = table.currentSnapshot()
      import org.apache.iceberg.util.SnapshotUtil
      SnapshotUtil.latestSnapshot(table, null.asInstanceOf[String])
      val set = scala.collection.mutable.ArrayBuffer[org.apache.iceberg.DeleteFile]()
      table.newScan().planFiles().iterator().asScala.foreach { ft =>
        ft.deletes().asScala.foreach(set += _)
      }
      set.distinct.filter(ContentFileUtil.isDV).toSeq
    }

    val onDvs = dvs(s"${base}_on")
    val offDvs = dvs(s"${base}_off")
    assert(offDvs.size == 1, s"Iceberg should leave exactly one DV, got ${offDvs.size}")
    assert(onDvs.size == 1, s"our path should leave exactly one DV, got ${onDvs.size}")
    assert(offDvs.head.recordCount() == 3L, s"Iceberg DV should index 3 positions, got ${offDvs.head.recordCount()}")
    assert(onDvs.head.recordCount() == 3L, s"our DV should index 3 positions, got ${onDvs.head.recordCount()}")
    import org.apache.iceberg.FileFormat
    assert(FileFormat.fromFileName(onDvs.head.location()) == FileFormat.PUFFIN, "our DV must be PUFFIN")
    assertSameTwin(base)
  }

  // ---- testAggregatePushDownInMergeOnReadDelete ----
  test("aggregate push-down is disabled over MoR deletes; min/max/count match Iceberg on our scan") {
    val base = "ice.db.del_aggpd"
    for (suffix <- Seq("on", "off")) {
      val t = s"${base}_$suffix"
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(
        s"""CREATE TABLE $t (id BIGINT, data INT) USING iceberg
           |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')""".stripMargin
      )
      spark.sql(s"INSERT INTO $t VALUES (1,1111),(1,2222),(2,3333),(2,4444),(3,5555),(3,6666)")
    }
    assert(deleteOnOff(base, "data = 1111"), "operator must run")
    val select = "SELECT max(data), min(data), count(data) FROM %s"
    // Over a table with deletes, min/max/count must NOT be pushed to the scan (they would ignore deletes).
    val explain = spark.sql(("EXPLAIN " + select).replace("%s", s"${base}_on")).collect()(0).getString(0).toLowerCase
    assert(
      !(explain.contains("max(data)") || explain.contains("min(data)") || explain.contains("count(data)")),
      s"aggregates must not be pushed down over MoR deletes on our scan:\n$explain"
    )
    val onAgg = spark.sql(select.replace("%s", s"${base}_on")).collect()(0)
    val offAgg = spark.sql(select.replace("%s", s"${base}_off")).collect()(0)
    assert(onAgg == offAgg, s"aggregate result differs: on=$onAgg off=$offAgg")
    assert(onAgg == Row(6666, 2222, 5L), s"aggregate over deleted table wrong: $onAgg")
  }
}
