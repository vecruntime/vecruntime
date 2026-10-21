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
package io.vecruntime.iceberg.bridge

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #901 M2: merge-on-read on a format v2 table, its deletes written as columnar Parquet position delete files,
 * compared with Spark's own writer statement by statement -- DELETE, a second DELETE (which, with `file`
 * granularity, merges and replaces each data file's previous delete file), UPDATE and MERGE -- under both
 * delete granularities, on unpartitioned and partitioned tables: the contents, the snapshot summary's file and
 * record counts, the delete files' kind and format, and, with `file` granularity, that every delete file names
 * its one data file through its `file_path` bounds, as Iceberg's writer leaves them.
 */
class VectorPositionDeleteWriteSuite extends AnyFunSuite with BeforeAndAfterAll {

  /** Whether the "on" side also writes the data half columnar (#901 M1). */
  protected def columnarData: Boolean = false

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("posdel")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("position-deletes")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      // The columnar exchange, so the clustering Iceberg asks of a delta write runs on batches.
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def withFlag[T](on: Boolean)(f: => T): T = {
    val keys = Seq("spark.vecruntime.iceberg.dvWriter.enabled" -> on) ++
      (if (columnarData) Seq("spark.vecruntime.iceberg.writer.enabled" -> on) else Nil)
    val prev = keys.map { case (k, _) => k -> spark.conf.getOption(k) }
    keys.foreach { case (k, v) => spark.conf.set(k, v.toString) }
    try f
    finally prev.foreach { case (k, p) => p.fold(spark.conf.unset(k))(v => spark.conf.set(k, v)) }
  }

  private def rows(t: String): Array[Row] = spark.sql(s"SELECT * FROM $t ORDER BY id").collect()

  private def create(name: String, partitioned: Boolean, granularity: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    val part = if (partitioned) "PARTITIONED BY (p)" else ""
    spark.sql(
      s"""CREATE TABLE $name (id BIGINT, p INT, v STRING) USING iceberg $part
         |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
         |  'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read',
         |  'write.delete.granularity'='$granularity', 'write.target-file-size-bytes'='4096')""".stripMargin
    )
    spark.sql(
      s"""INSERT INTO $name
         |SELECT id, cast(id % 4 as int) as p, if(id % 10 = 0, null, concat('v', id)) as v
         |FROM range(3000)""".stripMargin
    )
  }

  private def planHas(df: DataFrame): Boolean = {
    import org.apache.spark.sql.execution.SparkPlan
    def unwrap(p: SparkPlan): Seq[SparkPlan] = {
      val nested = p.getClass.getSimpleName match {
        case "CommandResultExec" | "AdaptiveSparkPlanExec" =>
          val name = if (p.getClass.getSimpleName == "CommandResultExec") "commandPhysicalPlan" else "executedPlan"
          p.getClass.getMethods
            .find(m => m.getName == name && m.getParameterCount == 0)
            .map(_.invoke(p).asInstanceOf[SparkPlan])
            .toSeq
        case _ => Seq.empty
      }
      p +: (p.children ++ nested).flatMap(unwrap)
    }
    unwrap(df.queryExecution.executedPlan).exists(_.getClass.getSimpleName == "VectorWriteDeltaExec")
  }

  private def table(t: String): org.apache.iceberg.Table =
    org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t)

  private val counts = Seq(
    "added-records",
    "added-position-deletes",
    "removed-position-deletes",
    "added-delete-files",
    "removed-delete-files",
    "total-records",
    "total-position-deletes",
    "total-delete-files",
    "total-data-files"
  )

  /** Runs `sql` on `<base>_off` (Spark's writer) and `<base>_on` (ours), then compares the two tables. */
  private def step(base: String, sql: String, fileGranularity: Boolean): Unit = {
    val (on, off) = (s"${base}_on", s"${base}_off")
    withFlag(on = false)(spark.sql(sql.replace("%s", off)).collect())
    withFlag(on = true) {
      val before = ColumnarPositionDeleteWriter.FILES_WRITTEN.get()
      val df = spark.sql(sql.replace("%s", on))
      assert(planHas(df), s"$base: our operator did not run for: $sql")
      df.collect()
      val added = table(on).currentSnapshot().summary().getOrDefault("added-delete-files", "0").toLong
      assert(
        ColumnarPositionDeleteWriter.FILES_WRITTEN.get() - before == added,
        s"$base: the $added delete files added were not all written by the columnar writer"
      )
    }
    assert(rows(on).sameElements(rows(off)), s"$base: contents differ after: $sql")
    val (sOn, sOff) = (table(on).currentSnapshot().summary(), table(off).currentSnapshot().summary())
    for (k <- counts)
      assert(sOn.get(k) == sOff.get(k), s"$base: snapshot $k differs after $sql: on=${sOn.get(k)} off=${sOff.get(k)}")
    val tasks = table(on).newScan().planFiles()
    try {
      val deletes = tasks.asScala.flatMap(_.deletes().asScala).toSeq
      assert(deletes.nonEmpty, s"$base: no live delete files")
      for (d <- deletes) {
        assert(d.content() == org.apache.iceberg.FileContent.POSITION_DELETES, s"$base: ${d.location()} kind")
        assert(d.format() == org.apache.iceberg.FileFormat.PARQUET, s"$base: ${d.location()} format")
        if (fileGranularity) {
          assert(
            org.apache.iceberg.util.ContentFileUtil.referencedDataFile(d) != null,
            s"$base: a file-granularity delete file does not name its data file"
          )
        }
      }
    } finally tasks.close()
  }

  for (granularity <- Seq("file", "partition"); partitioned <- Seq(false, true)) {
    val kind = s"${if (partitioned) "partitioned" else "unpartitioned"} v2, $granularity granularity"
    test(s"DELETE, DELETE, UPDATE and MERGE on $kind: identical to Spark's writer") {
      val base = s"ice.db.pd_${granularity.head}_${if (partitioned) "p" else "u"}"
      create(s"${base}_on", partitioned, granularity)
      create(s"${base}_off", partitioned, granularity)
      val file = granularity == "file"
      step(base, "DELETE FROM %s WHERE id % 3 = 0", file)
      step(base, "DELETE FROM %s WHERE id % 5 = 0", file)
      step(base, "UPDATE %s SET v = concat(coalesce(v, 'n'), '-u') WHERE id % 7 = 1", file)
      spark
        .sql("SELECT id, cast(id % 4 as int) AS p, concat('s', id) AS v, id % 3 AS op FROM range(2500, 3500)")
        .createOrReplaceTempView("pd_src")
      step(
        base,
        """MERGE INTO %s t USING pd_src s ON t.id = s.id
          |WHEN MATCHED AND s.op = 1 THEN DELETE
          |WHEN MATCHED THEN UPDATE SET t.v = s.v
          |WHEN NOT MATCHED THEN INSERT (id, p, v) VALUES (s.id, s.p, s.v)""".stripMargin,
        file
      )
    }
  }

  test("a v2 delete file over the roll size rolls into several, still identical to Spark's writer") {
    val base = "ice.db.pd_roll"
    for (t <- Seq(s"${base}_on", s"${base}_off")) {
      create(t, partitioned = false, granularity = "partition")
      spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('write.delete.target-file-size-bytes'='1')")
    }
    withFlag(on = false)(spark.sql(s"DELETE FROM ${base}_off WHERE id % 2 = 0").collect())
    withFlag(on = true)(spark.sql(s"DELETE FROM ${base}_on WHERE id % 2 = 0").collect())
    assert(rows(s"${base}_on").sameElements(rows(s"${base}_off")), "contents differ")
    val summary = table(s"${base}_on").currentSnapshot().summary()
    assert(summary.get("added-position-deletes") == "1500", s"position deletes: $summary")
    assert(summary.get("added-delete-files").toInt > 1, s"the delete file did not roll: $summary")
  }
}
