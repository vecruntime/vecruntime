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

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Integration tests for the columnar v3 deletion-vector writer (#20, slice 4-live), DELETE on v3.
 * They load the plugin's operator, planner strategy and session extensions (vecruntime-spark is a
 * test dependency of this module) and assert: the table is identical with the writer on and off, our
 * `VectorWriteDeltaExec` is in the plan when on, a v2 table falls back to Spark's writer, the DV files
 * are readable by Spark's metadata tables and the Iceberg Java API, and the snapshot summary counts
 * match Spark's. Runs in the iceberg-bridge module (which the gate's -pl list excludes), so it is run
 * manually -- see docs/iceberg-dv-writer.md.
 */
class VectorDvWriteSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("dvwrite")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("dv-write")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def withFlag[T](on: Boolean)(f: => T): T = {
    val key = "spark.vecruntime.iceberg.dvWriter.enabled"
    val prev = spark.conf.getOption(key)
    spark.conf.set(key, on.toString)
    try f
    finally prev match { case Some(v) => spark.conf.set(key, v); case None => spark.conf.unset(key) }
  }

  private def rows(t: String): Array[Row] = spark.sql(s"SELECT * FROM $t ORDER BY id").collect()
  private def count(sql: String): Long = spark.sql(sql).collect()(0).getLong(0)

  private def createV3(name: String, fmtVersion: Int = 3): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(
      s"""CREATE TABLE $name (id BIGINT, p INT, v STRING) USING iceberg
         |TBLPROPERTIES ('format-version'='$fmtVersion', 'write.delete.mode'='merge-on-read',
         |  'write.target-file-size-bytes'='4096')""".stripMargin
    )
    spark.sql(
      s"""INSERT INTO $name
         |SELECT id, cast(id % 2 as int) as p, if(id % 10 = 0, null, concat('v', id)) as v
         |FROM range(2000)""".stripMargin
    )
  }

  private def planHasVectorWriteDelta(df: org.apache.spark.sql.DataFrame): Boolean = {
    // A row-level DELETE plans to a V2CommandExec, which Spark wraps in a CommandResultExec and runs
    // eagerly, caching the rows. CommandResultExec is a leaf in the executed-plan tree -- it holds the
    // real physical command in `commandPhysicalPlan`, not as a tree child -- and that command may in
    // turn be an AdaptiveSparkPlanExec that hides its body behind `executedPlan`. A plain tree walk of
    // `executedPlan` therefore never sees VectorWriteDeltaExec even when it ran. Unwrap both.
    import org.apache.spark.sql.execution.SparkPlan
    def unwrap(p: SparkPlan): Seq[SparkPlan] = {
      val nested = p.getClass.getSimpleName match {
        case "CommandResultExec" =>
          p.getClass.getMethods
            .find(m => m.getName == "commandPhysicalPlan" && m.getParameterCount == 0)
            .map(_.invoke(p).asInstanceOf[SparkPlan])
            .toSeq
        case "AdaptiveSparkPlanExec" =>
          p.getClass.getMethods
            .find(m => m.getName == "executedPlan" && m.getParameterCount == 0)
            .map(_.invoke(p).asInstanceOf[SparkPlan])
            .toSeq
        case _ => Seq.empty
      }
      p +: (p.children ++ nested).flatMap(unwrap)
    }
    unwrap(df.queryExecution.executedPlan).exists(_.getClass.getSimpleName == "VectorWriteDeltaExec")
  }

  test("bridge + eligibility preconditions hold on a v3 table") {
    createV3("ice.db.dv_pre")
    assert(
      org.apache.spark.sql.vecruntime.IcebergDvBridge.isAvailable,
      "bridge must be on the classpath in this module"
    )
    val tbl = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, "ice.db.dv_pre")
    assert(org.apache.spark.sql.vecruntime.IcebergDvBridge.isDvEligible(tbl), "v3 table must be DV-eligible")
    val df = withFlag(on = true)(spark.sql("DELETE FROM ice.db.dv_pre WHERE id % 3 = 0"))
    assert(planHasVectorWriteDelta(df), "our operator must be planned when every gate passes")
    df.collect()
  }

  test("DELETE on v3: identical on/off, our operator ran, DVs readable, snapshot counts match") {
    createV3("ice.db.dv_on")
    createV3("ice.db.dv_off")

    withFlag(on = false)(spark.sql("DELETE FROM ice.db.dv_off WHERE id % 3 = 0").collect())
    val onPlanHad = withFlag(on = true) {
      val df = spark.sql("DELETE FROM ice.db.dv_on WHERE id % 3 = 0")
      val had = planHasVectorWriteDelta(df)
      df.collect()
      had
    }
    assert(onPlanHad, "VectorWriteDeltaExec must be in the DELETE plan when the flag is on")

    val off = rows("ice.db.dv_off")
    val on = rows("ice.db.dv_on")
    assert(on.length == off.length, s"row count differs: on=${on.length} off=${off.length}")
    assert(on.sameElements(off), "table contents differ between writer on and off")
    assert(count("SELECT count(*) FROM ice.db.dv_on WHERE id % 3 = 0") == 0L, "deleted rows still present")

    assert(count("SELECT count(*) FROM ice.db.dv_on.all_delete_files") >= 1L, "no delete files visible to Spark")
    val table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, "ice.db.dv_on")
    val summary = table.currentSnapshot().summary()
    val offTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, "ice.db.dv_off")
    val offSummary = offTable.currentSnapshot().summary()
    assert(
      summary.get("added-position-deletes") == offSummary.get("added-position-deletes"),
      s"added-position-deletes differ: on=${summary.get("added-position-deletes")} off=${offSummary.get("added-position-deletes")}"
    )
    assert(
      summary.get("added-delete-files") == offSummary.get("added-delete-files"),
      s"added-delete-files differ: on=${summary.get("added-delete-files")} off=${offSummary.get("added-delete-files")}"
    )
  }

  test("empty delete set on v3: no-op, identical on/off, our operator still planned") {
    createV3("ice.db.dv_empty_on")
    createV3("ice.db.dv_empty_off")
    // The condition must match no row yet survive planning. A range past the column's max
    // (`id > 100000`), or a value outside a column's min/max (`v = 'nonexistent'` -- every v starts
    // with 'v'), lets Iceberg prove the DELETE empty from file statistics and run it as a metadata
    // delete, with no row-level writer -- ours or Spark's -- planned. `length(v) > 100` cannot be
    // pushed to Iceberg's stats, so the row-level plan is kept and runs with zero rows to delete.
    withFlag(on = false)(spark.sql("DELETE FROM ice.db.dv_empty_off WHERE length(v) > 100").collect())
    withFlag(on = true) {
      val df = spark.sql("DELETE FROM ice.db.dv_empty_on WHERE length(v) > 100")
      assert(planHasVectorWriteDelta(df), "operator should still be planned for an empty delete")
      df.collect()
    }
    assert(rows("ice.db.dv_empty_on").sameElements(rows("ice.db.dv_empty_off")))
    assert(count("SELECT count(*) FROM ice.db.dv_empty_on") == 2000L)
  }

  test("v2 table falls back to Spark's WriteDeltaExec (our operator absent)") {
    createV3("ice.db.dv_v2", fmtVersion = 2)
    val had = withFlag(on = true) {
      val df = spark.sql("DELETE FROM ice.db.dv_v2 WHERE id % 3 = 0")
      val h = planHasVectorWriteDelta(df)
      df.collect()
      h
    }
    assert(!had, "v2 must fall back to Spark's writer; VectorWriteDeltaExec must be absent")
    assert(count("SELECT count(*) FROM ice.db.dv_v2 WHERE id % 3 = 0") == 0L)
  }

  test("repeated DELETEs on v3: every one accelerates, previous DVs merged, one DV per file") {
    createV3("ice.db.dv_rep_on")
    createV3("ice.db.dv_rep_off")
    val hadPlanned = scala.collection.mutable.ArrayBuffer[Boolean]()
    for (m <- Seq(3, 4, 5)) {
      withFlag(on = false)(spark.sql(s"DELETE FROM ice.db.dv_rep_off WHERE id % $m = 0").collect())
      val had = withFlag(on = true) {
        val df = spark.sql(s"DELETE FROM ice.db.dv_rep_on WHERE id % $m = 0")
        val h = planHasVectorWriteDelta(df)
        df.collect()
        h
      }
      hadPlanned += had
    }
    // Each later DELETE merges the file's committed DV into its new one and replaces it, as Iceberg's
    // own writer does; without the merge Iceberg rejects the commit ("Can't index multiple DVs").
    assert(hadPlanned.forall(identity), s"every DELETE must use the columnar operator: $hadPlanned")
    assert(rows("ice.db.dv_rep_on").sameElements(rows("ice.db.dv_rep_off")), "repeated-delete contents differ")
    val expected = (0L until 2000L).count(i => i % 3 != 0 && i % 4 != 0 && i % 5 != 0).toLong
    assert(count("SELECT count(*) FROM ice.db.dv_rep_on") == expected, "live row count wrong")
    // At most one live DV per data file, and the live DVs index as many deleted rows as Spark's.
    assert(
      count("SELECT count(*) - count(DISTINCT referenced_data_file) FROM ice.db.dv_rep_on.delete_files") == 0L,
      "a data file has more than one live DV"
    )
    assert(
      count("SELECT sum(record_count) FROM ice.db.dv_rep_on.delete_files") ==
        count("SELECT sum(record_count) FROM ice.db.dv_rep_off.delete_files"),
      "live DV cardinality differs from Spark's"
    )
  }

  test("partitioned v3 DELETE: our operator runs, each DV in its file's partition, result correct") {
    def createPart(name: String): Unit = {
      spark.sql(s"DROP TABLE IF EXISTS $name")
      spark.sql(
        s"""CREATE TABLE $name (id BIGINT, p INT, v STRING) USING iceberg PARTITIONED BY (p)
           |TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
           |  'write.target-file-size-bytes'='4096')""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO $name
           |SELECT id, cast(id % 4 as int) as p, if(id % 10 = 0, null, concat('v', id)) as v
           |FROM range(4000)""".stripMargin
      )
    }
    createPart("ice.db.dv_part_on")
    createPart("ice.db.dv_part_off")
    withFlag(on = false)(spark.sql("DELETE FROM ice.db.dv_part_off WHERE id % 3 = 0").collect())
    val onHad = withFlag(on = true) {
      val df = spark.sql("DELETE FROM ice.db.dv_part_on WHERE id % 3 = 0")
      val h = planHasVectorWriteDelta(df)
      df.collect()
      h
    }
    // Partitioned targets are supported: the spec id and partition tuple are read from the metadata
    // projection, so each DV is committed under its data file's partition, as Spark's writer does.
    assert(onHad, "partitioned v3 DELETE must plan our operator")
    assert(rows("ice.db.dv_part_on").sameElements(rows("ice.db.dv_part_off")), "partitioned contents differ")
    assert(count("SELECT count(*) FROM ice.db.dv_part_on WHERE id % 3 = 0") == 0L, "deleted rows still present")
    // Every DV lands in its data file's partition: the delete-file partitions match Spark's.
    def delParts(t: String): Seq[Row] =
      spark
        .sql(s"SELECT partition.p, count(*), sum(record_count) FROM $t.delete_files GROUP BY 1 ORDER BY 1")
        .collect()
        .toSeq
    assert(delParts("ice.db.dv_part_on") == delParts("ice.db.dv_part_off"), "delete files per partition differ")
    for (p <- 0 until 4) {
      assert(
        count(s"SELECT count(*) FROM ice.db.dv_part_on WHERE p = $p") ==
          count(s"SELECT count(*) FROM ice.db.dv_part_off WHERE p = $p"),
        s"partition $p differs"
      )
    }
  }

  test("a failing task aborts: nothing is committed, table unchanged") {
    createV3("ice.db.dv_abort")
    val before = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, "ice.db.dv_abort")
    val snapBefore = before.currentSnapshot().snapshotId()
    val liveBefore = count("SELECT count(*) FROM ice.db.dv_abort")

    // A UDF that throws mid-scan forces the delete-write RDD job to fail, so VectorWriteDeltaExec's
    // catch path must call Iceberg's DeltaBatchWrite.abort and rethrow -- no snapshot may be created.
    spark.udf.register("dv_boom", (id: Long) => if (id == 123L) throw new RuntimeException("boom") else id % 3 == 0)
    val ex = intercept[Exception] {
      withFlag(on = true)(spark.sql("DELETE FROM ice.db.dv_abort WHERE dv_boom(id)").collect())
    }
    assert(ex != null, "the failing task must surface an exception")

    val after = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, "ice.db.dv_abort")
    assert(after.currentSnapshot().snapshotId() == snapBefore, "a failed delete must not create a new snapshot")
    assert(
      count("SELECT count(*) FROM ice.db.dv_abort") == liveBefore,
      "the table must be unchanged after an aborted delete"
    )
  }

  // ---- UPDATE / MERGE: deletes columnar, the insert half through Iceberg's own writer ----

  /** A v3 table in merge-on-read mode for DELETE, UPDATE and MERGE alike, optionally partitioned. */
  private def createMor(name: String, partitioned: Boolean): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    val part = if (partitioned) "PARTITIONED BY (p)" else ""
    spark.sql(
      s"""CREATE TABLE $name (id BIGINT, p INT, v STRING) USING iceberg $part
         |TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
         |  'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read',
         |  'write.target-file-size-bytes'='4096')""".stripMargin
    )
    spark.sql(
      s"""INSERT INTO $name
         |SELECT id, cast(id % 4 as int) as p, if(id % 10 = 0, null, concat('v', id)) as v
         |FROM range(2000)""".stripMargin
    )
  }

  /** Runs `sql` (with `%s` for the table) on `<base>_off` with the writer off and `<base>_on` with it on. */
  private def onOff(base: String, sql: String): Boolean = {
    // Plain substitution, not String.format: the SQL itself uses `%` as the modulo operator.
    withFlag(on = false)(spark.sql(sql.replace("%s", s"${base}_off")).collect())
    withFlag(on = true) {
      val df = spark.sql(sql.replace("%s", s"${base}_on"))
      val had = planHasVectorWriteDelta(df)
      df.collect()
      had
    }
  }

  /** Contents, live DV shape and the v3 row lineage of the surviving original rows all match Spark's. */
  private def assertSame(base: String): Unit = {
    val (on, off) = (s"${base}_on", s"${base}_off")
    assert(rows(on).sameElements(rows(off)), s"$base: table contents differ between writer on and off")
    assert(
      count(s"SELECT count(*) - count(DISTINCT referenced_data_file) FROM $on.delete_files") == 0L,
      s"$base: a data file has more than one live DV"
    )
    assert(
      count(s"SELECT coalesce(sum(record_count), 0) FROM $on.delete_files") ==
        count(s"SELECT coalesce(sum(record_count), 0) FROM $off.delete_files"),
      s"$base: live DV cardinality differs from Spark's"
    )
    // A reinsert keeps the row's lineage: an updated original row keeps its _row_id, as with Spark's writer.
    def lineage(t: String): Array[Row] =
      spark.sql(s"SELECT id, _row_id FROM $t WHERE id < 2000 ORDER BY id").collect()
    assert(lineage(on).sameElements(lineage(off)), s"$base: _row_id of the original rows differs")
    val sOn = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, on).currentSnapshot().summary()
    val sOff = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, off).currentSnapshot().summary()
    for (k <- Seq("added-records", "added-position-deletes", "total-records", "total-position-deletes"))
      assert(sOn.get(k) == sOff.get(k), s"$base: snapshot summary $k differs: on=${sOn.get(k)} off=${sOff.get(k)}")
  }

  for (partitioned <- Seq(false, true)) {
    val kind = if (partitioned) "partitioned" else "unpartitioned"

    test(s"UPDATE on $kind v3: our operator runs, deletes as DVs, reinserts via Iceberg, lineage kept") {
      val base = s"ice.db.dv_upd_${if (partitioned) "p" else "u"}"
      createMor(s"${base}_on", partitioned)
      createMor(s"${base}_off", partitioned)
      // Twice, so the second UPDATE also merges the DVs the first one committed.
      assert(onOff(base, "UPDATE %s SET v = concat(coalesce(v, 'n'), '-u') WHERE id % 7 = 0"), "UPDATE not columnar")
      assert(onOff(base, "UPDATE %s SET v = 'again' WHERE id % 5 = 0"), "second UPDATE not columnar")
      assertSame(base)
      assert(count(s"SELECT count(*) FROM ${base}_on WHERE v = 'again'") == 400L)
    }

    test(s"MERGE on $kind v3: update, delete and insert clauses, identical on/off") {
      val base = s"ice.db.dv_mrg_${if (partitioned) "p" else "u"}"
      createMor(s"${base}_on", partitioned)
      createMor(s"${base}_off", partitioned)
      spark
        .sql("SELECT id, cast(id % 4 as int) AS p, concat('s', id) AS v, id % 3 AS op FROM range(1500, 2500)")
        .createOrReplaceTempView("dv_src")
      val merge =
        """MERGE INTO %s t USING dv_src s ON t.id = s.id
          |WHEN MATCHED AND s.op = 0 THEN DELETE
          |WHEN MATCHED THEN UPDATE SET t.v = s.v
          |WHEN NOT MATCHED THEN INSERT (id, p, v) VALUES (s.id, s.p, s.v)""".stripMargin
      assert(onOff(base, merge), "MERGE not columnar")
      assertSame(base)
      // 500 matched: a third deleted, the rest updated; 500 inserted.
      val deleted = (1500L until 2000L).count(_ % 3 == 0).toLong
      assert(count(s"SELECT count(*) FROM ${base}_on") == 2000L - deleted + 500L, "live row count wrong")
      assert(count(s"SELECT count(*) FROM ${base}_on WHERE id >= 2000") == 500L, "inserted rows missing")
    }
  }

  test("MERGE with only a NOT MATCHED insert clause leaves no DVs and matches Spark's writer") {
    val base = "ice.db.dv_mrg_ins"
    createMor(s"${base}_on", partitioned = false)
    createMor(s"${base}_off", partitioned = false)
    spark.sql("SELECT id, 0 AS p, 'x' AS v FROM range(1990, 2010)").createOrReplaceTempView("dv_src_ins")
    val merge =
      """MERGE INTO %s t USING dv_src_ins s ON t.id = s.id
        |WHEN NOT MATCHED THEN INSERT (id, p, v) VALUES (s.id, s.p, s.v)""".stripMargin
    onOff(base, merge) // An insert-only MERGE may plan as an append; either way the result must match.
    assert(rows(s"${base}_on").sameElements(rows(s"${base}_off")), "insert-only MERGE contents differ")
    assert(count(s"SELECT count(*) FROM ${base}_on") == 2010L)
  }
}
