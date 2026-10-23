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
package io.vecruntime.iceberg.spark

import java.nio.file.{Files, Path}

import org.apache.iceberg.{FileFormat, Table}
import org.apache.iceberg.data.{GenericFileWriterFactory, GenericRecord}
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Focused test for the native-scan equality-delete-on-a-dropped-column fix in VectorIcebergScanExec /
 * IcebergTasks (equalityColumnsOf + equalityFilter now resolve equality field ids against the table's
 * SCHEMA HISTORY, not only the current schema).
 *
 * An equality delete keyed on a column that is later DROPPED still applies to the data files written
 * before the drop (they carry the field id), and must NOT delete rows of files written after the drop
 * (which have no such column, so the key is null there and only a null delete key would match). Before
 * the fix the whole file set fell back to Iceberg's reader (equalityColumnsOf saw null for the dropped
 * id in the current schema and declined); now our native scan reads the dropped column as a hidden,
 * probe-only column by field id and applies the delete itself.
 *
 * Each case asserts the EXPLICIT expected rows AND that VectorIcebergScanExec ran with no task group
 * handed to Iceberg's reader (numIcebergGroups == 0) -- so the deletes are applied on our native path.
 */
class TestEqualityDeleteDroppedColumnSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("eqdel-dropped")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("eqdel-dropped")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.plugins", "io.vecruntime.spark.VectorPlugin")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.resolve("ice").toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def nodes(p: SparkPlan): Seq[SparkPlan] = {
    val nested = p.getClass.getSimpleName match {
      case "AdaptiveSparkPlanExec" =>
        p.getClass.getMethods.find(m => m.getName == "executedPlan" && m.getParameterCount == 0)
          .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
      case n if n.endsWith("QueryStageExec") =>
        p.getClass.getMethods.find(m => m.getName == "plan" && m.getParameterCount == 0)
          .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
      case _ => Seq.empty
    }
    p +: (p.children ++ nested).flatMap(nodes)
  }

  /** Collect `q`, asserting a VectorIcebergScanExec ran and handed no group to Iceberg's reader. */
  private def nativeCollect(q: String): Seq[Row] = {
    val df: DataFrame = spark.sql(q)
    val got = df.collect().toSeq
    val scans = nodes(df.queryExecution.executedPlan).filter(_.getClass.getSimpleName == "VectorIcebergScanExec")
    assert(scans.nonEmpty, s"not the native scan for [$q]:\n${df.queryExecution.executedPlan}")
    assert(scans.map(_.metrics("numIcebergGroups").value).sum == 0, s"a group went to Iceberg's reader for [$q]")
    got
  }

  private def withConf[T](kv: (String, String))(f: => T): T = {
    val prev = spark.conf.getOption(kv._1)
    spark.conf.set(kv._1, kv._2)
    try f
    finally prev match { case Some(v) => spark.conf.set(kv._1, v); case None => spark.conf.unset(kv._1) }
  }

  private def icebergIds(q: String): Seq[Int] =
    withConf("spark.vecruntime.enabled" -> "false")(spark.sql(q).collect().toSeq).map(_.getInt(0)).sorted

  private def ids(rows: Seq[Row]): Seq[Int] = rows.map(_.getInt(0)).sorted

  /** One equality delete file keyed on `columns` of `t`, rows are the Iceberg Java values (nulls allowed). */
  private def addEqualityDeletes(t: Table, columns: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    val schema = t.schema()
    val deleteSchema = schema.select(columns: _*)
    val factory = new GenericFileWriterFactory.Builder(t)
      .deleteFileFormat(FileFormat.PARQUET)
      .equalityFieldIds(columns.map(c => schema.findField(c).fieldId()).toArray)
      .equalityDeleteRowSchema(deleteSchema)
      .build()
    val out = OutputFileFactory.builderFor(t, 1, 1).format(FileFormat.PARQUET).build().newOutputFile()
    val writer = factory.newEqualityDeleteWriter(out, t.spec(), null)
    try rows.foreach { r =>
        val rec = GenericRecord.create(deleteSchema)
        columns.zip(r).foreach { case (c, v) => rec.setField(c, v) }
        writer.write(rec)
      }
    finally writer.close()
    t.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
  }

  private def refresh(name: String): Unit = spark.sql(s"REFRESH TABLE $name")

  // ---------------------------------------------------------------------------------------------
  // A top-level equality delete on a since-dropped column, applied natively (the gap item 2 fixes).
  test("equality delete on a since-dropped top-level column applies on the native scan") {
    val name = "ice.db.eqdrop_top"
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(
      s"""CREATE TABLE $name (id INT, data STRING, status STRING) USING iceberg
         |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')""".stripMargin
    )
    spark.sql(s"INSERT INTO $name VALUES (1,'a','ACTIVE'),(2,'b','INACTIVE'),(3,'c','ACTIVE'),(4,'d','INACTIVE')")
    val t = Spark3Util.loadIcebergTable(spark, name)
    addEqualityDeletes(t, Seq("status"), Seq(Seq("INACTIVE"))) // removes ids 2 and 4
    spark.sql(s"ALTER TABLE $name DROP COLUMN status")
    refresh(name)
    val got = nativeCollect(s"SELECT id, data FROM $name")
    assert(ids(got) == Seq(1, 3), s"only the INACTIVE rows should be gone: ${ids(got)}")
    assert(ids(got) == icebergIds(s"SELECT id, data FROM $name"), "native result must match Iceberg's reader")
  }

  // Pre-drop and post-drop data files together: the delete only touches the pre-drop file; post-drop
  // rows have a null `status` value, which equals only a null delete key -- not the 'INACTIVE' key.
  test("a post-drop data file (null key) is untouched by the dropped-column delete, native scan") {
    val name = "ice.db.eqdrop_mix"
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(
      s"""CREATE TABLE $name (id INT, data STRING, status STRING) USING iceberg
         |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')""".stripMargin
    )
    // pre-drop file: ids 1..4 with a status
    spark.sql(s"INSERT INTO $name VALUES (1,'a','ACTIVE'),(2,'b','INACTIVE'),(3,'c','ACTIVE'),(4,'d','INACTIVE')")
    val t = Spark3Util.loadIcebergTable(spark, name)
    addEqualityDeletes(t, Seq("status"), Seq(Seq("INACTIVE"))) // removes ids 2 and 4 of the pre-drop file
    spark.sql(s"ALTER TABLE $name DROP COLUMN status")
    // post-drop file: ids 5..7, no status column at all (its value reads null)
    spark.sql(s"INSERT INTO $name VALUES (5,'e'),(6,'f'),(7,'g')")
    refresh(name)
    val got = nativeCollect(s"SELECT id, data FROM $name")
    // pre-drop INACTIVE rows (2,4) gone; post-drop rows (5,6,7) all kept (null status != 'INACTIVE').
    assert(ids(got) == Seq(1, 3, 5, 6, 7), s"post-drop rows must survive: ${ids(got)}")
    assert(ids(got) == icebergIds(s"SELECT id, data FROM $name"), "native result must match Iceberg's reader")
  }

  // Partitioned table: the dropped-column equality delete applies per partition on the native scan.
  test("equality delete on a since-dropped column, partitioned table, native scan") {
    val name = "ice.db.eqdrop_part"
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(
      s"""CREATE TABLE $name (id INT, data STRING, status STRING) USING iceberg PARTITIONED BY (data)
         |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')""".stripMargin
    )
    spark.sql(
      s"INSERT INTO $name VALUES (1,'x','ACTIVE'),(2,'x','INACTIVE'),(3,'y','ACTIVE'),(4,'y','INACTIVE')"
    )
    val t = Spark3Util.loadIcebergTable(spark, name)
    // one equality delete per partition, keyed on the (soon-dropped) status, matching that partition's value
    addEqualityDeletesPerPartition(t, Seq("status"), Seq(Seq("INACTIVE")), Seq("x", "y"))
    spark.sql(s"ALTER TABLE $name DROP COLUMN status")
    refresh(name)
    val got = nativeCollect(s"SELECT id, data FROM $name")
    assert(ids(got) == Seq(1, 3), s"the INACTIVE row of each partition should be gone: ${ids(got)}")
    assert(ids(got) == icebergIds(s"SELECT id, data FROM $name"), "native result must match Iceberg's reader")
  }

  /** One equality delete file per `data` partition value, scoped to that partition via the table's spec. */
  private def addEqualityDeletesPerPartition(
      t: Table,
      columns: Seq[String],
      rows: Seq[Seq[Any]],
      partitionValues: Seq[String]
  ): Unit = {
    val schema = t.schema()
    val deleteSchema = schema.select(columns: _*)
    val factory = new GenericFileWriterFactory.Builder(t)
      .deleteFileFormat(FileFormat.PARQUET)
      .equalityFieldIds(columns.map(c => schema.findField(c).fieldId()).toArray)
      .equalityDeleteRowSchema(deleteSchema)
      .build()
    partitionValues.foreach { pv =>
      val partition = GenericRecord.create(t.spec().partitionType())
      partition.set(0, pv) // identity(data): the partition value is the data string itself
      val out = OutputFileFactory.builderFor(t, 1, 1).format(FileFormat.PARQUET).build().newOutputFile()
      val writer = factory.newEqualityDeleteWriter(out, t.spec(), partition)
      try rows.foreach { r =>
          val rec = GenericRecord.create(deleteSchema)
          columns.zip(r).foreach { case (c, v) => rec.setField(c, v) }
          writer.write(rec)
        }
      finally writer.close()
      t.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
    }
  }
}
