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

import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField.optional
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for writing the v3 `unknown` type with the columnar Iceberg writer (plan item 8).
 *
 * The v3 spec says an `unknown` field "Must be optional with `null` defaults; not stored in data files" and
 * its Parquet mapping is "None -- Omit from data files" ("When reading an `unknown` column, any corresponding
 * column must be ignored and replaced with `null` values"). Iceberg's own writer realises this in
 * `TypeToMessageType`, which omits an unknown field from the Parquet schema at the top level and inside a
 * struct, and rejects an unknown list element or map key/value. The columnar writer matches it exactly:
 *
 *  - a top-level or struct `unknown` column writes natively (the column is simply absent from the data file,
 *    no bounds / counts for its field id), and both our native scan and Iceberg's own vectorized reader read
 *    it back as all nulls;
 *  - an `unknown` list element or map value is declined, so the write falls back to Iceberg's own writer
 *    (which would itself throw on such a schema);
 *  - `unknown` cannot be a partition source -- Iceberg refuses to build such a spec, so the question never
 *    reaches the writer.
 *
 * Spark SQL cannot declare an `unknown` column, so the tables are created through Iceberg's catalog (as the
 * `fixed` / `uuid` tests in [[VectorIcebergWriteSuite]] do). Each case is written twice -- into a writer-off
 * twin (Iceberg's own row writer) and a writer-on table -- and the two must hold the same rows, the same file
 * metrics and the same snapshot record counts.
 */
class VectorIcebergUnknownWriteSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("iceunknown")
  private val Flag = "spark.vecruntime.iceberg.writer.enabled"
  private val DvFlag = "spark.vecruntime.iceberg.dvWriter.enabled"
  private val CopyFlag = "spark.vecruntime.iceberg.copyUnchanged.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("iceberg-unknown-write")
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
        case n if n.endsWith("QueryStageExec") =>
          p.getClass.getMethods
            .find(m => m.getName == "plan" && m.getParameterCount == 0)
            .map(_.invoke(p).asInstanceOf[SparkPlan])
            .toSeq
        case _ => Seq.empty
      }
      p +: (p.children ++ nested).flatMap(unwrap)
    }
    unwrap(df.queryExecution.executedPlan)
  }

  private def columnar(df: DataFrame): Boolean =
    nodes(df).exists(p =>
      p.getClass.getSimpleName == "VectorIcebergWriteExec" || p.getClass.getSimpleName == "VectorWriteDeltaExec"
    )

  /** Whether `f` ran a columnar Iceberg write (append or delta) in any query it executed. */
  private def ranColumnar(f: => Unit): Boolean = {
    val seen = new java.util.concurrent.atomic.AtomicBoolean(false)
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(name: String, qe: org.apache.spark.sql.execution.QueryExecution, ns: Long): Unit = {
        def walk(p: SparkPlan): Boolean =
          p.getClass.getSimpleName == "VectorIcebergWriteExec" ||
            p.getClass.getSimpleName == "VectorWriteDeltaExec" || p.children.exists(walk) ||
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
      val deadline = System.nanoTime() + 20L * 1000 * 1000 * 1000
      while (!seen.get && System.nanoTime() < deadline) Thread.sleep(100)
    } finally spark.listenerManager.unregister(listener)
    seen.get
  }

  private def catalog: HadoopCatalog =
    new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse.toString)

  private def recreate(name: String, schema: Schema, spec: PartitionSpec, props: Map[String, String]): Unit = {
    val id = TableIdentifier.of("db", name)
    val c = catalog
    c.dropTable(id)
    import scala.jdk.CollectionConverters._
    c.createTable(id, schema, spec, (props + ("format-version" -> "3")).asJava)
  }

  private def rowsOf(t: String, cols: String): Seq[Row] =
    spark.sql(s"SELECT $cols FROM ice.db.$t ORDER BY id").collect().toSeq

  private def metricsOf(t: String): Seq[String] =
    spark
      .sql(
        s"""SELECT record_count, value_counts, null_value_counts, nan_value_counts,
           |  transform_values(lower_bounds, (k, v) -> hex(v)), transform_values(upper_bounds, (k, v) -> hex(v))
           |FROM ice.db.$t.files""".stripMargin
      )
      .collect()
      .map(_.toString)
      .sorted
      .toSeq

  private def summary(t: String, key: String): String =
    spark
      .sql(s"SELECT summary['$key'] FROM ice.db.$t.snapshots ORDER BY committed_at DESC LIMIT 1")
      .collect()(0)
      .getString(0)

  /** The field ids that carry any metric map entry across the table's data files. */
  private def metricFieldIds(t: String): Set[Int] = {
    val rows = spark
      .sql(
        s"""SELECT map_keys(value_counts), map_keys(null_value_counts), map_keys(lower_bounds), map_keys(upper_bounds)
           |FROM ice.db.$t.files""".stripMargin
      )
      .collect()
    rows.flatMap { r =>
      (0 until 4).flatMap { c =>
        Option(r.getList[Int](c)).map(_.toArray(Array.empty[Integer]).map(_.toInt).toSeq).getOrElse(Seq.empty)
      }
    }.toSet
  }

  /** The field ids of every `unknown`-typed field in the table's current schema (top level and nested). */
  private def unknownFieldIds(t: String): Set[Int] = {
    val table = catalog.loadTable(TableIdentifier.of("db", t))
    val ids = scala.collection.mutable.Set.empty[Int]
    def walk(tp: org.apache.iceberg.types.Type): Unit = tp match {
      case s: Types.StructType =>
        s.fields().forEach { f =>
          if (f.`type`().typeId() == org.apache.iceberg.types.Type.TypeID.UNKNOWN) ids += f.fieldId()
          else walk(f.`type`())
        }
      case l: Types.ListType => walk(l.elementType())
      case m: Types.MapType => walk(m.keyType()); walk(m.valueType())
      case _ => ()
    }
    walk(table.schema().asStruct())
    ids.toSet
  }

  // ---------------------------------------------------------------- top-level + struct

  /** id(1), unk(2: unknown), s(3: struct{k int(4), su unknown(5)}), i(6: int). */
  private def topAndStructSchema =
    new Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "unk", Types.UnknownType.get()),
      optional(
        3,
        "s",
        Types.StructType.of(
          optional(4, "k", Types.IntegerType.get()),
          optional(5, "su", Types.UnknownType.get())
        )
      ),
      optional(6, "i", Types.IntegerType.get())
    )

  private val topAndStructSrc =
    """SELECT id, null AS unk,
      |  if(id % 7 = 0, null, named_struct('k', CAST(id % 9 AS INT), 'su', null)) AS s,
      |  CAST(id % 1000 AS INT) AS i
      |FROM range(3000)""".stripMargin

  test("an unknown column, at the top level and inside a struct, writes columnar equal to Iceberg's writer") {
    for (name <- Seq("unk_on", "unk_off"))
      recreate(name, topAndStructSchema, PartitionSpec.unpartitioned(), Map.empty)

    withWriter(on = false)(spark.sql(s"INSERT INTO ice.db.unk_off $topAndStructSrc").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO ice.db.unk_on $topAndStructSrc"))
    df.collect()
    assert(columnar(df), "an unknown column must write columnar, not fall back")

    // Same rows as Iceberg's writer (unknown reads back as all nulls on both sides).
    val cols = "id, unk, s.k, s.su, i"
    assert(rowsOf("unk_on", cols) == rowsOf("unk_off", cols))
    assert(rowsOf("unk_on", cols).forall(r => r.isNullAt(1) && r.isNullAt(3)), "unknown columns must be null")

    // Same file metrics as Iceberg's writer, and no metric entry for any unknown field id (Iceberg renumbers
    // field ids on create, so read the unknown ids from the table rather than hardcoding them).
    assert(metricsOf("unk_on") == metricsOf("unk_off"), "file metrics differ from Iceberg's writer")
    val unknownIds = unknownFieldIds("unk_on")
    val ids = metricFieldIds("unk_on")
    assert(unknownIds.nonEmpty, "the table must have unknown fields")
    assert(unknownIds.intersect(ids).isEmpty, s"no metrics for unknown field ids $unknownIds, got $ids")
    assert(ids.nonEmpty, "the real columns must still carry metrics")

    Seq("added-records", "total-records").foreach(k => assert(summary("unk_on", k) == summary("unk_off", k)))
  }

  test("Iceberg's own vectorized reader reads our files' unknown columns back as nulls") {
    // Read the writer-on table (written above) with the native scan off, so Iceberg's own reader is used.
    val prev = spark.conf.getOption("spark.vecruntime.enabled")
    spark.conf.set("spark.vecruntime.enabled", "false")
    try {
      val read = spark.sql("SELECT id, unk, s.su FROM ice.db.unk_on ORDER BY id").collect().toSeq
      assert(read.nonEmpty && read.forall(r => r.isNullAt(1) && r.isNullAt(2)))
    } finally prev match {
        case Some(v) => spark.conf.set("spark.vecruntime.enabled", v)
        case None => spark.conf.unset("spark.vecruntime.enabled")
      }
  }

  // ---------------------------------------------------------------- list element / map value decline

  test("an unknown list element or map value is declined and falls back to Iceberg's writer") {
    val listSchema = new Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "l", Types.ListType.ofOptional(3, Types.UnknownType.get()))
    )
    val mapSchema = new Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "m", Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.UnknownType.get()))
    )
    recreate("unk_list", listSchema, PartitionSpec.unpartitioned(), Map.empty)
    recreate("unk_map", mapSchema, PartitionSpec.unpartitioned(), Map.empty)

    // Iceberg's own writer cannot write an unknown list element or map value either -- TypeToMessageType fails
    // the Parquet-schema conversion ("Cannot convert element/value Parquet: unknown"). So the columnar writer
    // DECLINES such a schema and falls back to Iceberg's writer, which then raises exactly that error. The
    // point of the test is that we do not run columnar and we do not fail in some different way.
    def insertFails(sql: String): Throwable = {
      val e = intercept[Exception](withWriter(on = true)(spark.sql(sql).collect()))
      assert(
        Iterator
          .iterate[Throwable](e)(_.getCause)
          .takeWhile(_ != null)
          .exists(t =>
            String.valueOf(t.getMessage).contains("Cannot convert") &&
              String.valueOf(t.getMessage).contains("unknown")
          ),
        s"expected Iceberg's 'Cannot convert ... unknown', got: ${e.getMessage}"
      )
      e
    }
    val ranList = ranColumnar(
      try insertFails("INSERT INTO ice.db.unk_list SELECT id, array(null, null) FROM range(100)")
      catch { case _: Throwable => () }
    )
    assert(!ranList, "an unknown list element must be declined, not written columnar")
    val ranMap = ranColumnar(
      try insertFails("INSERT INTO ice.db.unk_map SELECT id, map('k', null) FROM range(100)")
      catch { case _: Throwable => () }
    )
    assert(!ranMap, "an unknown map value must be declined, not written columnar")
  }

  // ---------------------------------------------------------------- partitioning

  test("unknown as a partition source: bucket rejected by Iceberg; void is unpartitioned; identity falls back") {
    val schema = topAndStructSchema
    // Iceberg 1.12 rejects a bucket (and truncate / time) transform on unknown outright.
    val bucket = intercept[Exception](PartitionSpec.builderFor(schema).bucket("unk", 4).build())
    assert(
      String.valueOf(bucket.getMessage).toLowerCase.contains("unknown"),
      s"expected Iceberg to reject bucket(unknown), got: ${bucket.getMessage}"
    )

    // A void(unknown) spec is treated by Iceberg as UNPARTITIONED (the partition value is always null), so the
    // columnar writer writes it as an ordinary unpartitioned table, columnar, with the unknown column dropped.
    recreate("unk_vpart", schema, PartitionSpec.builderFor(schema).alwaysNull("unk").build(), Map.empty)
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO ice.db.unk_vpart $topAndStructSrc"))
    df.collect()
    assert(columnar(df), "a void(unknown) spec is unpartitioned and must write columnar")
    assert(spark.sql("SELECT count(*) FROM ice.db.unk_vpart").collect()(0).getLong(0) == 3000L)
    assert(rowsOf("unk_vpart", "id, unk, s.su").forall(r => r.isNullAt(1) && r.isNullAt(2)))
    assert(unknownFieldIds("unk_vpart").intersect(metricFieldIds("unk_vpart")).isEmpty)

    // A genuine identity(unknown) spec IS partitioned on an unknown source. The columnar writer carries no
    // unknown lane, so it declines such a spec and the write falls back to Iceberg's own writer -- which writes
    // the single (all-null) partition. Confirm the fall back and that the data is still correct.
    recreate("unk_ipart", schema, PartitionSpec.builderFor(schema).identity("unk").build(), Map.empty)
    val ran = ranColumnar(
      withWriter(on = true)(spark.sql(s"INSERT INTO ice.db.unk_ipart $topAndStructSrc").collect())
    )
    assert(!ran, "a table partitioned by identity(unknown) must fall back to Iceberg's writer")
    assert(spark.sql("SELECT count(*) FROM ice.db.unk_ipart").collect()(0).getLong(0) == 3000L)
    assert(rowsOf("unk_ipart", "id, unk, s.su").forall(r => r.isNullAt(1) && r.isNullAt(2)))
  }

  test("a table partitioned on a normal column, with an unknown data column, writes columnar equal to Iceberg's") {
    val schema = topAndStructSchema
    val spec = PartitionSpec.builderFor(schema).identity("i").build()
    recreate("unk_part", schema, spec, Map.empty)
    recreate("unk_part_off", schema, spec, Map.empty)
    withWriter(on = false)(spark.sql(s"INSERT INTO ice.db.unk_part_off $topAndStructSrc").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO ice.db.unk_part $topAndStructSrc"))
    df.collect()
    assert(columnar(df), "a partitioned table with an unknown data column must write columnar")
    val cols = "id, unk, s.k, s.su, i"
    assert(rowsOf("unk_part", cols) == rowsOf("unk_part_off", cols))
    assert(rowsOf("unk_part", cols).forall(r => r.isNullAt(1) && r.isNullAt(3)))
    assert(metricsOf("unk_part") == metricsOf("unk_part_off"), "partitioned file metrics differ from Iceberg's")
    assert(unknownFieldIds("unk_part").intersect(metricFieldIds("unk_part")).isEmpty)
  }

  // ---------------------------------------------------------------- MOR (DV) and COW on an unknown column

  test("a merge-on-read DELETE and a copy-on-write DELETE keep the unknown column, equal to Iceberg's writer") {
    val mor = Map(
      "write.delete.mode" -> "merge-on-read",
      "write.update.mode" -> "merge-on-read",
      "write.merge.mode" -> "merge-on-read"
    )
    val cow = Map(
      "write.delete.mode" -> "copy-on-write",
      "write.update.mode" -> "copy-on-write",
      "write.merge.mode" -> "copy-on-write"
    )
    val cols = "id, unk, s.k, s.su, i"

    for ((suffix, props, dv, copy) <- Seq(("mor", mor, true, false), ("cow", cow, false, true))) {
      val on = s"unk_${suffix}_on"
      val off = s"unk_${suffix}_off"
      for (name <- Seq(on, off)) recreate(name, topAndStructSchema, PartitionSpec.unpartitioned(), props)
      // Seed both with Iceberg's writer so the starting files are identical.
      Seq(on, off).foreach(t => withWriter(on = false)(spark.sql(s"INSERT INTO ice.db.$t $topAndStructSrc").collect()))

      def runDelete(t: String, on: Boolean): Unit = {
        val prevDv = spark.conf.getOption(DvFlag)
        val prevCopy = spark.conf.getOption(CopyFlag)
        if (dv) spark.conf.set(DvFlag, on.toString)
        if (copy) spark.conf.set(CopyFlag, on.toString)
        try withWriter(on)(spark.sql(s"DELETE FROM ice.db.$t WHERE id % 3 = 0").collect())
        finally {
          prevDv match { case Some(v) => spark.conf.set(DvFlag, v); case None => spark.conf.unset(DvFlag) }
          prevCopy match { case Some(v) => spark.conf.set(CopyFlag, v); case None => spark.conf.unset(CopyFlag) }
        }
      }

      runDelete(off, on = false)
      val ran = ranColumnar(runDelete(on, on = true))
      assert(ran, s"$suffix: the delete on an unknown-column table must run columnar")
      assert(rowsOf(on, cols) == rowsOf(off, cols), s"$suffix: rows differ from Iceberg's writer after delete")
      assert(rowsOf(on, cols).forall(r => r.isNullAt(1) && r.isNullAt(3)), s"$suffix: unknown must stay null")
      // The surviving data and the record count must match Iceberg's writer. (Per-file metric maps are not
      // compared: our writer may roll files differently, which changes only the per-file split, not the data.)
      assert(
        unknownFieldIds(on).intersect(metricFieldIds(on)).isEmpty,
        s"$suffix: no metrics for the unknown field id"
      )
      assert(summary(on, "total-records") == summary(off, "total-records"), s"$suffix: total-records after delete")
    }
  }
}
