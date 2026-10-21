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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for the columnar Iceberg data-file writer (#842, W2), through SQL with the plugin's session
 * extensions loaded. Every write runs twice, into a twin table with the writer off (Iceberg's own row writer)
 * and into one with it on, and the two tables must hold the same rows and the same snapshot record counts; the
 * columnar exec must be in the plan when on, and the writes it does not cover must fall back to Spark's.
 */
class VectorIcebergWriteSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("icewrite")
  private val Flag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("iceberg-write")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .config("spark.vecruntime.explainFallback.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def withWriter[T](on: Boolean)(f: => T): T = {
    val prev = spark.conf.getOption(Flag)
    spark.conf.set(Flag, on.toString)
    try f
    finally prev match { case Some(v) => spark.conf.set(Flag, v); case None => spark.conf.unset(Flag) }
  }

  /** Every physical node the query ran, through command results and adaptive plans. */
  private def nodes(df: DataFrame): Seq[SparkPlan] = {
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
    unwrap(df.queryExecution.executedPlan)
  }

  private def columnar(df: DataFrame): Boolean = nodes(df).exists(_.getClass.getSimpleName == "VectorIcebergWriteExec")

  /**
   * Whether `f` ran the columnar exec in any query it executed. CTAS and RTAS write through an `AppendData`
   * Spark plans and runs in a query execution of its own, which the statement's plan does not show.
   */
  private def ranColumnar(f: => Unit): Boolean = {
    val seen = new java.util.concurrent.atomic.AtomicBoolean(false)
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(name: String, qe: org.apache.spark.sql.execution.QueryExecution, ns: Long): Unit = {
        def walk(p: SparkPlan): Boolean =
          p.getClass.getSimpleName == "VectorIcebergWriteExec" || p.children.exists(walk) ||
            (p match {
              case a: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec => walk(a.executedPlan)
              case c: org.apache.spark.sql.execution.CommandResultExec => walk(c.commandPhysicalPlan)
              case _ => false
            })
        if (walk(qe.executedPlan)) seen.set(true)
      }
      override def onFailure(name: String, qe: org.apache.spark.sql.execution.QueryExecution, e: Exception): Unit = ()
    }
    spark.listenerManager.register(listener)
    try {
      f
      // The listeners run on Spark's listener bus, after the query: wait for them, up to a bound.
      val deadline = System.nanoTime() + 20L * 1000 * 1000 * 1000
      while (!seen.get && System.nanoTime() < deadline) Thread.sleep(100)
    } finally spark.listenerManager.unregister(listener)
    seen.get
  }

  private def rows(t: String): Seq[Row] = spark.sql(s"SELECT * FROM $t ORDER BY id").collect().toSeq

  private def summary(t: String, key: String): String =
    spark.sql(s"SELECT summary['$key'] FROM $t.snapshots ORDER BY committed_at DESC LIMIT 1").collect()(0).getString(0)

  private val Columns =
    "id BIGINT, i INT, d DOUBLE, f FLOAT, b BOOLEAN, s STRING, day DATE, ts TIMESTAMP, dec DECIMAL(9,2), " +
      "wide DECIMAL(38,6), st STRUCT<x: INT, y: STRING>, xs ARRAY<INT>, m MAP<STRING, BIGINT>"

  private val Source =
    """SELECT id, cast(id % 1000 as int) as i, if(id % 17 = 0, null, id * 0.5d) as d,
      |  if(id % 19 = 0, cast('NaN' as float), cast(id as float) / 3) as f, id % 3 = 0 as b,
      |  if(id % 10 = 0, null, concat('v', id % 300)) as s, date_add(date'2020-01-01', cast(id % 500 as int)) as day,
      |  timestamp_micros(1700000000000000 + id * 1000) as ts, cast(id % 10000 as decimal(9,2)) / 7 as dec,
      |  cast(id as decimal(38,6)) * 1000000 as wide,
      |  if(id % 7 = 0, null, named_struct('x', cast(id % 50 as int), 'y', concat('y', id % 9))) as st,
      |  if(id % 11 = 0, null, array(cast(id % 5 as int), cast(id % 6 as int))) as xs,
      |  if(id % 13 = 0, null, map(concat('k', id % 4), id)) as m
      |FROM range(ROWS)""".stripMargin

  private def source(rows: Int): String = Source.replace("ROWS", rows.toString)

  private def create(t: String, props: String = "", version: Int = 2): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"""CREATE TABLE $t ($Columns) USING iceberg
         |TBLPROPERTIES ('format-version'='$version', 'write.target-file-size-bytes'='65536'$props)""".stripMargin
    )
  }

  /** Runs `sql` (given the table name) on a writer-off twin and a writer-on table and compares them. */
  private def twin(name: String, props: String = "", version: Int = 2)(setup: String => Unit)(
      sql: String => String
  ): DataFrame = {
    val (off, on) = (s"ice.db.${name}_off", s"ice.db.${name}_on")
    Seq(off, on).foreach { t =>
      create(t, props, version)
      withWriter(on = false)(setup(t))
    }
    withWriter(on = false)(spark.sql(sql(off)).collect())
    val df = withWriter(on = true)(spark.sql(sql(on)))
    df.collect()
    assert(rows(on) == rows(off), s"$name: the table differs from the one Iceberg's writer wrote")
    Seq("added-records", "deleted-records", "total-records").foreach { k =>
      assert(summary(on, k) == summary(off, k), s"$name: snapshot $k")
    }
    df
  }

  test("INSERT INTO writes columnar, with the rows and record counts Iceberg's writer gives") {
    val df = twin("append")(_ => ())(t => s"INSERT INTO $t ${source(20000)}")
    assert(columnar(df), "VectorIcebergWriteExec must plan the append")
    val files = spark.sql("SELECT count(*), sum(record_count) FROM ice.db.append_on.files").collect()(0)
    assert(files.getLong(0) > 1, "files must roll at write.target-file-size-bytes")
    assert(files.getLong(1) == 20000L)
  }

  test("the files' metrics let Iceberg prune on them") {
    val t = "ice.db.prune_on"
    create(t)
    withWriter(on = true)(spark.sql(s"INSERT INTO $t ${source(20000)}").collect())
    val all = spark.sql(s"SELECT count(*) FROM $t.files").collect()(0).getLong(0)
    val df = spark.sql(s"SELECT count(*) FROM $t WHERE id < 100")
    assert(df.collect()(0).getLong(0) == 100L)
    val bounds = spark.sql(s"SELECT count(*) FROM $t.files WHERE lower_bounds[1] IS NOT NULL").collect()(0).getLong(0)
    assert(bounds == all, "every file must carry bounds for id")
  }

  test("INSERT OVERWRITE writes columnar") {
    val df = twin("overwrite")(t => spark.sql(s"INSERT INTO $t ${source(3000)}").collect())(t =>
      s"INSERT OVERWRITE $t ${source(5000)}"
    )
    assert(columnar(df))
  }

  private val CopyOnWrite =
    ", 'write.delete.mode'='copy-on-write', 'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'"

  test("copy-on-write DELETE writes columnar") {
    val df = twin("cow_delete", CopyOnWrite)(t => spark.sql(s"INSERT INTO $t ${source(10000)}").collect())(t =>
      s"DELETE FROM $t WHERE id % 3 = 0"
    )
    assert(columnar(df))
  }

  test("copy-on-write UPDATE writes columnar") {
    val df = twin("cow_update", CopyOnWrite)(t => spark.sql(s"INSERT INTO $t ${source(10000)}").collect())(t =>
      s"UPDATE $t SET s = 'updated', i = i + 1 WHERE id % 4 = 1"
    )
    assert(columnar(df))
  }

  test("copy-on-write MERGE INTO writes columnar") {
    val df = twin("cow_merge", CopyOnWrite)(t => spark.sql(s"INSERT INTO $t ${source(10000)}").collect())(t =>
      s"""MERGE INTO $t t USING (${source(15000)}) src
         |ON t.id = src.id AND src.id >= 5000
         |WHEN MATCHED AND src.id % 2 = 0 THEN UPDATE SET t.s = 'merged'
         |WHEN MATCHED AND src.id % 5 = 0 THEN DELETE
         |WHEN NOT MATCHED THEN INSERT *""".stripMargin
    )
    assert(columnar(df))
  }

  test("INSERT INTO a format v3 table writes columnar; Iceberg assigns the new rows' lineage") {
    val df = twin("append_v3", version = 3)(_ => ())(t => s"INSERT INTO $t ${source(5000)}")
    assert(columnar(df))
    val ids = spark.sql("SELECT count(DISTINCT _row_id), min(_row_id) FROM ice.db.append_v3_on").collect()(0)
    assert(ids.getLong(0) == 5000L && ids.getLong(1) == 0L)
  }

  private def lineage(t: String): Seq[Row] =
    spark.sql(s"SELECT id, _row_id, _last_updated_sequence_number FROM $t ORDER BY id").collect().toSeq

  for (
    (op, sql) <- Seq(
      "DELETE" -> ((t: String) => s"DELETE FROM $t WHERE id % 3 = 0"),
      "UPDATE" -> ((t: String) => s"UPDATE $t SET s = 'updated' WHERE id % 4 = 1"),
      "MERGE INTO" -> ((t: String) =>
        s"""MERGE INTO $t t USING (${source(4000)}) src ON t.id = src.id AND src.id >= 1000
           |WHEN MATCHED AND src.id % 2 = 0 THEN UPDATE SET t.s = 'merged'
           |WHEN MATCHED AND src.id % 5 = 0 THEN DELETE
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      )
    )
  ) {
    test(s"copy-on-write $op on a format v3 table writes columnar and keeps the rows' lineage") {
      val name = s"cow_v3_${op.replace(' ', '_').toLowerCase}"
      val df = twin(name, CopyOnWrite, version = 3)(t => spark.sql(s"INSERT INTO $t ${source(3000)}").collect())(sql)
      assert(columnar(df))
      assert(lineage(s"ice.db.${name}_on") == lineage(s"ice.db.${name}_off"), "row lineage")
    }
  }

  test("INSERT INTO from a Parquet table reads the scan's batches, not rows") {
    spark.sql("DROP TABLE IF EXISTS pq_src")
    spark.sql(
      "CREATE TABLE pq_src USING parquet AS SELECT id, cast(id % 1000 as int) as i, id * 0.5d as d, " +
        "concat('v', id % 300) as s, cast(id % 10000 as decimal(9,2)) as dec FROM range(20000)"
    )
    val t = "ice.db.from_parquet"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(s"CREATE TABLE $t (id BIGINT, i INT, d DOUBLE, s STRING, dec DECIMAL(9,2)) USING iceberg")
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $t SELECT * FROM pq_src"))
    df.collect()
    val exec = nodes(df).find(_.getClass.getSimpleName == "VectorIcebergWriteExec")
    assert(exec.isDefined)
    assert(exec.get.metrics("rowInput").value == 0L, "the write must read the native scan's batches")
    assert(spark.sql(s"SELECT * FROM $t ORDER BY id").collect().toSeq ==
      spark.sql("SELECT * FROM pq_src ORDER BY id").collect().toSeq)
  }

  test("a partitioned table falls back to Iceberg's writer, unchanged") {
    val (off, on) = ("ice.db.part_off", "ice.db.part_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(s"CREATE TABLE $t ($Columns) USING iceberg PARTITIONED BY (bucket(4, id))")
    }
    withWriter(on = false)(spark.sql(s"INSERT INTO $off ${source(3000)}").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on ${source(3000)}"))
    df.collect()
    assert(!columnar(df))
    assert(rows(on) == rows(off))
  }

  test("CTAS and RTAS write columnar, through the append Spark runs for them") {
    val (off, on) = ("ice.db.ctas_off", "ice.db.ctas_on")
    Seq(off, on).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))
    withWriter(on = false)(
      spark.sql(s"CREATE TABLE $off USING iceberg TBLPROPERTIES ('format-version'='2') AS ${source(4000)}").collect()
    )
    val ctas = ranColumnar(withWriter(on = true)(
      spark.sql(s"CREATE TABLE $on USING iceberg TBLPROPERTIES ('format-version'='2') AS ${source(4000)}").collect()
    ))
    assert(ctas, "CTAS")
    assert(rows(on) == rows(off))
    withWriter(on = false)(
      spark.sql(s"REPLACE TABLE $off USING iceberg TBLPROPERTIES ('format-version'='2') AS ${source(2500)}").collect()
    )
    val rtas = ranColumnar(withWriter(on = true)(
      spark.sql(s"REPLACE TABLE $on USING iceberg TBLPROPERTIES ('format-version'='2') AS ${source(2500)}").collect()
    ))
    assert(rtas, "RTAS")
    assert(rows(on) == rows(off))
  }

  test("with the writer off nothing changes") {
    val t = "ice.db.flag_off"
    create(t)
    val df = withWriter(on = false)(spark.sql(s"INSERT INTO $t ${source(100)}"))
    df.collect()
    assert(!columnar(df))
  }
}
