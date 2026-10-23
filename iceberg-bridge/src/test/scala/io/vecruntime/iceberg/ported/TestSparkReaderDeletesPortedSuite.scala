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
 *   spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/source/TestSparkReaderDeletes.java
 *   (+ its base data/.../org/apache/iceberg/data/DeleteReadTests.java)
 *   at git tag `apache-iceberg-1.12.0`
 *   https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/spark/src/test/java/
 *     org/apache/iceberg/spark/source/TestSparkReaderDeletes.java
 * Apache Iceberg, Copyright The Apache Software Foundation; see the repository NOTICE.
 *
 * WHAT THIS PORTS. The upstream suite reads a table carrying position and equality delete files and
 * asserts the surviving rows, the `_deleted` metadata column, and the NumDeletes metric. Here every
 * scenario is re-expressed as a READ against vecruntime's native Iceberg scan
 * (`spark.vecruntime.scan.nativeIceberg.enabled=true`): we build the SAME table (the DeleteReadTests
 * 7-row dataset, ids 29/43/61/89/100/121/122 = data a..g, all in bucket(data,16)=0), add the SAME
 * delete files through Iceberg's own writers, and then assert:
 *   (1) the rows our native scan returns equal the EXPLICIT expected rows the upstream asserts (never
 *       just "ours == Iceberg's reader" -- Iceberg 1.11/1.12 have their own equality-delete bugs on
 *       fixed/uuid/nested keys, apache/iceberg#18618/#18619/#18620, so we pin absolute answers), and
 *   (2) the plan really used `VectorIcebergScanExec` with NO task group handed to Iceberg's reader
 *       (`numIcebergGroups == 0`), i.e. no silent fallback.
 * We cross-check against Iceberg's own reader (plugin off) only as a secondary equality, never as the
 * primary oracle.
 *
 * Upstream cases ported (upstream name -> our test):
 *   testPosDeletesAllRowsInBatch                 -> "v2 position deletes: all 4 rows of the first batch gone"
 *   testPosDeletesWithDeletedColumn              -> "v2 position deletes: survivors native; _deleted via Iceberg"
 *   testEqualityDeleteWithDeletedColumn          -> "v2 equality deletes: survivors native; _deleted via Iceberg"
 *   testMixedPosAndEqDeletesWithDeletedColumn    -> "v2 mixed position + equality deletes: survivors native; ..."
 *   testFilterOnDeletedMetadataColumn            -> "filter on _deleted = true / false partitions the rows"
 *   testIsDeletedColumnWithoutDeleteFile         -> "_deleted is all-false when the table has no delete file"
 *   testEqualityDeleteWithFilter                 -> "a predicate selecting only a deleted row returns nothing"
 *   testEqualityDeleteWithSchemaEvolution        -> "an equality delete on a since-dropped column still applies"
 *   testPosDeletesOnParquetFileWithMultipleRowGroups
 *                                                -> "position deletes across row groups of one parquet file"
 *   (+ a v3 deletion-vector survivor case)       -> "v3 deletion-vector deletes: survivors correct on native scan"
 *
 * Both features that once fell back to Iceberg's reader now run NATIVELY:
 *   - Projecting Iceberg's `_deleted` metadata column: PR #956 (#901 item 3) makes the native scan keep the
 *     deleted rows and mark `_deleted=true` (VectorIcebergScanExec.markDeleted / withAllRows) instead of
 *     dropping them. The per-row _deleted is asserted on the native scan against the explicit upstream set.
 *   - An equality delete keyed on a column DROPPED from the table: the field id is resolved against the
 *     table's schema history and the column read as a hidden probe column -- see the dedicated
 *     TestEqualityDeleteDroppedColumnSuite and the eq-delete schema-evolution case below.
 *
 * Runtime note: the repo's iceberg-spark-runtime jar reports "Apache Iceberg 1.12.0" at run time (matching
 * the ported tag). Position/equality delete files are written with Iceberg's own `GenericFileWriterFactory`
 * writers (the same path `IcebergEqualityDeletesSuite` uses), because the test-only `FileHelpers` is not on
 * the runtime classpath.
 */
package io.vecruntime.iceberg.ported

import java.nio.file.{Files, Path}

import org.apache.iceberg.{FileFormat, Table}
import org.apache.iceberg.data.{GenericFileWriterFactory, GenericRecord, Record}
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TestSparkReaderDeletesPortedSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("reader-deletes-port")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("reader-deletes-port")
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

  // ---------- plan / native-scan assertions ----------

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
    assert(
      scans.map(_.metrics("numIcebergGroups").value).sum == 0,
      s"a task group went to Iceberg's reader for [$q]"
    )
    got
  }

  private def withConf[T](kv: (String, String))(f: => T): T = {
    val prev = spark.conf.getOption(kv._1)
    spark.conf.set(kv._1, kv._2)
    try f
    finally prev match { case Some(v) => spark.conf.set(kv._1, v); case None => spark.conf.unset(kv._1) }
  }

  /** Iceberg's own reader as a secondary oracle (plugin fully off). */
  private def icebergRows(q: String): Seq[Row] =
    withConf("spark.vecruntime.enabled" -> "false")(spark.sql(q).collect().toSeq)

  /** (id, data, _deleted) read through Iceberg's own reader, for the _deleted-column fallback cases. */
  private def icebergDeleted(q: String): Seq[(Int, String, Boolean)] =
    deletedTriples(withConf("spark.vecruntime.enabled" -> "false")(spark.sql(q).collect().toSeq))

  /** (id, data, _deleted) read through our NATIVE scan, asserting VectorIcebergScanExec ran with no fallback. */
  private def nativeDeleted(q: String): Seq[(Int, String, Boolean)] = deletedTriples(nativeCollect(q))

  private def sortById(rows: Seq[Row]): Seq[(Int, String)] =
    rows.map(r => (r.getInt(0), r.getString(1))).sortBy(x => (x._1, x._2))

  private def deletedTriples(rows: Seq[Row]): Seq[(Int, String, Boolean)] =
    rows.map(r => (r.getInt(0), r.getString(1), r.getBoolean(2))).sortBy(x => (x._1, x._2))

  // ---------- the DeleteReadTests dataset ----------

  // id -> data; the exact 7 rows upstream builds, all in bucket(data,16)=0.
  private val DATA = Seq(29 -> "a", 43 -> "b", 61 -> "c", 89 -> "d", 100 -> "e", 121 -> "f", 122 -> "g")

  /**
   * Build the table from the DeleteReadTests dataset (ids 29/43/61/89/100/121/122 = data a..g). Upstream
   * partitions by bucket(data,16) and hand-picks those ids so every row lands in bucket 0 and the low-level
   * `FileHelpers` writer emits ONE data file; our port writes through SQL, which re-buckets and would split
   * the rows across partition files. The read-side feature under test -- the `_deleted` metadata column and
   * position / equality / DV deletes -- is partition-independent, so we keep the table UNPARTITIONED to pin
   * one data file with deterministic row positions 0..6 (= ids 29..122). Vectorization batch size 4 so the
   * first batch holds positions 0..3 (ids 29/43/61/89), exactly as upstream relies on.
   */
  private def buildTable(name: String, formatVersion: Int = 2): (Table, String) = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(
      s"""CREATE TABLE $name (id INT, data STRING) USING iceberg
         |TBLPROPERTIES ('format-version'='$formatVersion', 'read.parquet.vectorization.batch-size'='4',
         |  'write.delete.mode'='merge-on-read')""".stripMargin
    )
    val vals = DATA.map { case (i, d) => s"($i,'$d')" }.mkString(", ")
    spark.sql(s"SELECT * FROM VALUES $vals AS t(id, data)").coalesce(1).writeTo(name).append()
    (Spark3Util.loadIcebergTable(spark, name), name)
  }

  private def dataFileLocation(t: Table): String = {
    val it = t.newScan().planFiles().iterator()
    try it.next().file().location()
    finally it.close()
  }

  // ---------- delete-file writers (Iceberg's own, as the eq-delete live suite does) ----------

  private def addPositionDeletes(t: Table, dataFile: String, positions: Seq[Long]): Unit = {
    val out = OutputFileFactory.builderFor(t, 1, 1).format(FileFormat.PARQUET).build().newOutputFile()
    val factory = new GenericFileWriterFactory.Builder(t).deleteFileFormat(FileFormat.PARQUET).build()
    val writer = factory.newPositionDeleteWriter(out, t.spec(), partition0(t))
    val pd = PositionDelete.create[Record]()
    try positions.foreach(p => writer.write(pd.set(dataFile, p)))
    finally writer.close()
    t.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
  }

  private def addEqualityDeletes(t: Table, columns: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    val schema = t.schema()
    val deleteSchema = schema.select(columns: _*)
    val factory = new GenericFileWriterFactory.Builder(t)
      .deleteFileFormat(FileFormat.PARQUET)
      .equalityFieldIds(columns.map(c => schema.findField(c).fieldId()).toArray)
      .equalityDeleteRowSchema(deleteSchema)
      .build()
    val out = OutputFileFactory.builderFor(t, 1, 1).format(FileFormat.PARQUET).build().newOutputFile()
    val writer = factory.newEqualityDeleteWriter(out, t.spec(), partition0(t))
    try rows.foreach { r =>
        val rec = GenericRecord.create(deleteSchema)
        columns.zip(r).foreach { case (c, v) => rec.setField(c, v) }
        writer.write(rec)
      }
    finally writer.close()
    t.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
  }

  /** The single partition value the 7-row bucket-0 table lives in (bucket(data,16)=0). */
  private def partition0(t: Table): org.apache.iceberg.StructLike =
    if (t.spec().isUnpartitioned) null
    else {
      val r = GenericRecord.create(t.spec().partitionType())
      r.set(0, Integer.valueOf(0))
      r
    }

  private def refresh(name: String): Unit = spark.sql(s"REFRESH TABLE $name")

  // ---------------------------------------------------------------------------------------------
  // testPosDeletesAllRowsInBatch
  test("v2 position deletes: all 4 rows of the first batch gone, our native scan ran") {
    val (t, name) = buildTable("ice.db.pd_batch")
    val f = dataFileLocation(t)
    addPositionDeletes(t, f, Seq(0L, 1L, 2L, 3L)) // ids 29,43,61,89
    refresh(name)
    val expected = DATA.filterNot { case (i, _) => Set(29, 43, 61, 89).contains(i) }.sortBy(_._1)
    val got = nativeCollect(s"SELECT id, data FROM $name")
    assert(sortById(got) == expected, s"survivors wrong: ${sortById(got)}")
    assert(sortById(got) == Seq(100 -> "e", 121 -> "f", 122 -> "g"))
    assert(sortById(icebergRows(s"SELECT id, data FROM $name")) == expected, "Iceberg reader disagrees")
  }

  // testPosDeletesWithDeletedColumn
  //
  // #956 (#901 item 3): projecting Iceberg's `_deleted` column now runs NATIVELY -- our scan keeps the
  // deleted rows and marks `_deleted = true` instead of dropping them (VectorIcebergScanExec.markDeleted /
  // withAllRows). So these cases assert the native scan (VectorIcebergScanExec, numIcebergGroups == 0) with
  // the explicit per-row _deleted the upstream pins.
  test("v2 position deletes surface through the _deleted column, our native scan ran") {
    val (t, name) = buildTable("ice.db.pd_delcol")
    addPositionDeletes(t, dataFileLocation(t), Seq(0L, 1L, 2L, 3L))
    refresh(name)
    val del = Set(29, 43, 61, 89)
    val expected = DATA.map { case (i, d) => (i, d, del.contains(i)) }.sortBy(x => (x._1, x._2))
    assert(nativeDeleted(s"SELECT id, data, _deleted FROM $name") == expected, "native _deleted wrong")
    assert(icebergDeleted(s"SELECT id, data, _deleted FROM $name") == expected, "must match Iceberg's reader")
  }

  // testEqualityDeleteWithDeletedColumn
  test("v2 equality deletes surface through the _deleted column, our native scan ran") {
    val (t, name) = buildTable("ice.db.eq_delcol")
    addEqualityDeletes(t, Seq("data"), Seq(Seq("a"), Seq("d"), Seq("g"))) // ids 29,89,122
    refresh(name)
    val del = Set(29, 89, 122)
    val expected = DATA.map { case (i, d) => (i, d, del.contains(i)) }.sortBy(x => (x._1, x._2))
    assert(nativeDeleted(s"SELECT id, data, _deleted FROM $name") == expected, "native _deleted wrong")
    assert(icebergDeleted(s"SELECT id, data, _deleted FROM $name") == expected, "must match Iceberg's reader")
  }

  // testMixedPosAndEqDeletesWithDeletedColumn
  test("v2 mixed position + equality deletes through _deleted, our native scan ran") {
    val (t, name) = buildTable("ice.db.mix_delcol")
    val f = dataFileLocation(t)
    addEqualityDeletes(t, Seq("data"), Seq(Seq("a"), Seq("d"), Seq("g"))) // 29,89,122
    addPositionDeletes(t, f, Seq(3L, 5L)) // 89,121
    refresh(name)
    val del = Set(29, 89, 121, 122) // union
    val expected = DATA.map { case (i, d) => (i, d, del.contains(i)) }.sortBy(x => (x._1, x._2))
    assert(nativeDeleted(s"SELECT id, data, _deleted FROM $name") == expected, "native _deleted wrong")
    assert(icebergDeleted(s"SELECT id, data, _deleted FROM $name") == expected, "must match Iceberg's reader")
  }

  // testFilterOnDeletedMetadataColumn -- a filter on _deleted partitions the rows, on our native scan.
  test("filter on _deleted = true / false partitions the rows, our native scan ran") {
    val (t, name) = buildTable("ice.db.filt_delcol")
    addPositionDeletes(t, dataFileLocation(t), Seq(0L, 1L, 2L, 3L)) // 29,43,61,89
    refresh(name)
    val del = Set(29, 43, 61, 89)
    val live = nativeDeleted(s"SELECT id, data, _deleted FROM $name WHERE _deleted = false")
    assert(
      live == DATA.filterNot { case (i, _) => del.contains(i) }
        .map { case (i, d) => (i, d, false) }.sortBy(x => (x._1, x._2)),
      "live rows wrong"
    )
    val gone = nativeDeleted(s"SELECT id, data, _deleted FROM $name WHERE _deleted = true")
    assert(
      gone == DATA.filter { case (i, _) => del.contains(i) }
        .map { case (i, d) => (i, d, true) }.sortBy(x => (x._1, x._2)),
      "deleted rows wrong"
    )
  }

  // testIsDeletedColumnWithoutDeleteFile -- _deleted is all-false, on our native scan.
  test("_deleted is all-false when the table has no delete file, our native scan ran") {
    val (_, name) = buildTable("ice.db.nodel")
    val expected = DATA.map { case (i, d) => (i, d, false) }.sortBy(x => (x._1, x._2))
    assert(nativeDeleted(s"SELECT id, data, _deleted FROM $name") == expected, "native _deleted should be all false")
  }

  // testEqualityDeleteWithFilter
  test("a predicate selecting only a deleted row returns nothing, our native scan ran") {
    val (t, name) = buildTable("ice.db.eq_filter")
    addEqualityDeletes(t, Seq("data"), Seq(Seq("a"), Seq("d"), Seq("g"))) // 29,89,122
    refresh(name)
    val got = nativeCollect(s"SELECT id, data FROM $name WHERE data = 'a'")
    assert(got.isEmpty, s"deleted row 'a' must not come back: $got")
  }

  // testEqualityDeleteWithSchemaEvolution
  //
  // The equality delete here is keyed on `status`, which is then DROPPED from the table. Our native scan
  // resolves the delete's equality field id against the table's SCHEMA HISTORY (not only the current
  // schema), reads the dropped column from the pre-drop data files as a hidden probe-only column by field
  // id, and applies the delete itself -- so this now runs on VectorIcebergScanExec with no fallback. (The
  // dedicated native-scan coverage of this gap lives in TestEqualityDeleteDroppedColumnSuite.)
  test("an equality delete on a since-dropped column still applies, our native scan ran") {
    val (_, name) = buildTable("ice.db.eq_evo")
    spark.sql(s"ALTER TABLE $name ADD COLUMN status STRING")
    spark.sql(s"INSERT INTO $name VALUES (200,'h','ACTIVE'),(201,'i','INACTIVE'),(202,'j','ACTIVE')")
    val t2 = Spark3Util.loadIcebergTable(spark, name)
    addEqualityDeletes(t2, Seq("status"), Seq(Seq("INACTIVE"))) // removes id 201
    spark.sql(s"ALTER TABLE $name DROP COLUMN status")
    refresh(name)
    val got = nativeCollect(s"SELECT id, data FROM $name")
    val expected = (DATA ++ Seq(200 -> "h", 202 -> "j")).sortBy(_._1)
    assert(sortById(got) == expected, s"only id 201 should be gone: ${sortById(got)}")
    assert(!got.map(_.getInt(0)).contains(201))
    assert(sortById(got) == sortById(icebergRows(s"SELECT id, data FROM $name")), "must match Iceberg's reader")
  }

  // testPosDeletesOnParquetFileWithMultipleRowGroups (SQL-level analog: one big data file, deletes scattered)
  test("position deletes across row groups of one parquet file, our native scan ran") {
    val name = "ice.db.pd_rowgroups"
    spark.sql(s"DROP TABLE IF EXISTS $name")
    // Small row-group size so 200 rows span several groups within one file; one file via coalesce(1).
    spark.sql(
      s"""CREATE TABLE $name (id INT, data STRING) USING iceberg
         |TBLPROPERTIES ('format-version'='2', 'write.parquet.row-group-size-bytes'='2048',
         |  'write.delete.mode'='merge-on-read')""".stripMargin
    )
    spark.sql("SELECT cast(id AS INT) AS id, concat('r', id) AS data FROM range(0, 200)")
      .coalesce(1).writeTo(name).append()
    val t = Spark3Util.loadIcebergTable(spark, name)
    val f = dataFileLocation(t)
    val positions = Seq(97L, 98L, 99L, 101L, 103L, 107L, 109L)
    addPositionDeletes(t, f, positions)
    refresh(name)
    val got = nativeCollect(s"SELECT id FROM $name")
    assert(got.size == 193, s"expected 193 survivors, got ${got.size}")
    val goneIds = positions.map(_.toInt).toSet
    assert(!got.map(_.getInt(0)).exists(goneIds.contains), "a deleted position came back")
    assert(icebergRows(s"SELECT id FROM $name").size == 193, "Iceberg reader disagrees on count")
  }

  // v3 deletion vectors: a merge-on-read DELETE lands as a DV in PUFFIN; the native scan applies it and
  // returns the survivors. (The _deleted metadata column itself falls back to Iceberg's reader, covered
  // by the v2 cases above; here we assert the DV survivors on the native path.)
  test("v3 deletion-vector deletes: survivors correct on the native scan") {
    val (_, name) = buildTable("ice.db.dv_delcol", formatVersion = 3)
    spark.sql(s"DELETE FROM $name WHERE id IN (29, 61)")
    refresh(name)
    val del = Set(29, 61)
    val expected = DATA.filterNot { case (i, _) => del.contains(i) }.sortBy(_._1)
    assert(sortById(nativeCollect(s"SELECT id, data FROM $name")) == expected, "v3 DV survivors wrong")
    assert(
      sortById(nativeCollect(s"SELECT id, data FROM $name")) ==
        sortById(icebergRows(s"SELECT id, data FROM $name")),
      "v3 DV survivors differ from Iceberg's reader"
    )
  }
}
