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
package io.vecruntime.iceberg.ported

/*
 * Cross-engine writer-stats twins. Two upstream sources are the model (both Apache License 2.0):
 *
 *  - Meta/Velox main @ cbe4bec92e311c86ef4a929f86dc1e01efc39625:
 *      velox/connectors/hive/iceberg/tests/IcebergParquetStatsTest.cpp
 *    Velox's C++ gtest checks the per-column Iceberg stats its Parquet writer records
 *    (valueCount / nullValueCount / bounds / splitOffsets) for mixedNull, bigint, decimal, varchar,
 *    varbinary (incl. a bucket transform) and multipleDataTypes. There is no JVM harness to port, so
 *    each scenario is expressed here as a Spark-SQL twin: the same rows written with Iceberg's own row
 *    writer (our columnar writer off) and with ours on, and the `files` metadata table of the two tables
 *    must be byte-identical. The upstream scenario name is preserved on each ported case.
 *
 *  - Apache DataFusion Comet main @ a0cbb0fa06515acd5647538025a3a894be71b9bd:
 *      spark/src/test/scala/org/apache/comet/CometIcebergWriteDetectionSuite.scala
 *    Comet detects when its native writer is compatible with an Iceberg write and otherwise falls back
 *    to Spark. The analog for our writer is: a clean Parquet append runs columnar, while a write our
 *    writer does not cover (a shredded variant, a nested partition source) falls back to Iceberg's own
 *    writer and still produces identical data. These are the `writeDetection_*` cases below.
 */

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PortedWriterStatsTwinSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("ported-statstwin")
  private val Flag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ported-writer-stats-twin")
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
    nodes(df).exists(_.getClass.getSimpleName == "VectorIcebergWriteExec")

  private def mapOf(r: Row, i: Int): Map[Int, Long] =
    Option(r.getJavaMap[Int, Long](i)).map(_.asScala.toMap).getOrElse(Map.empty)

  private def hexMapOf(r: Row, i: Int): Map[Int, String] =
    Option(r.getJavaMap[Int, Array[Byte]](i))
      .map(_.asScala.toMap.map { case (k, v) => k -> v.map(b => f"$b%02x").mkString })
      .getOrElse(Map.empty)

  private final case class FileStats(
      recordCount: Long,
      valueCounts: Map[Int, Long],
      nullCounts: Map[Int, Long],
      nanCounts: Map[Int, Long],
      lowerBounds: Map[Int, String],
      upperBounds: Map[Int, String],
      splitOffsets: Seq[Long]
  )

  private def files(t: String): Seq[FileStats] =
    spark
      .sql(
        s"""SELECT record_count, value_counts, null_value_counts, nan_value_counts,
           |  lower_bounds, upper_bounds, split_offsets
           |FROM $t.files ORDER BY file_path""".stripMargin
      )
      .collect()
      .toSeq
      .map { r =>
        FileStats(
          r.getLong(0),
          mapOf(r, 1),
          mapOf(r, 2),
          mapOf(r, 3),
          hexMapOf(r, 4),
          hexMapOf(r, 5),
          Option(r.getSeq[Long](6)).map(_.toSeq).getOrElse(Seq.empty)
        )
      }

  /** Write `insert` into a writer-off twin and a writer-on table with the same DDL; return (off, on) file stats. */
  private def twin(name: String, columns: String, insert: String, props: String = "'format-version'='2'")
      : (Seq[FileStats], Seq[FileStats]) = {
    val (off, on) = (s"ice.db.${name}_off", s"ice.db.${name}_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(s"CREATE TABLE $t ($columns) USING iceberg TBLPROPERTIES ($props)")
    }
    withWriter(on = false)(spark.sql(s"INSERT INTO $off $insert").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $insert"))
    df.collect()
    assert(columnar(df), s"$name: VectorIcebergWriteExec must plan the append")
    (files(off), files(on))
  }

  // ---- Velox IcebergParquetStatsTest twins ------------------------------------------------------

  test("mixedNull") {
    // a column with some nulls and some values: null counts and value counts recorded, bounds over non-nulls.
    val (off, on) = twin(
      "mixedNull",
      "c BIGINT",
      "SELECT if(id % 3 = 0, cast(null as bigint), id) FROM range(300)"
    )
    assert(on == off, "mixed-null stats must equal Iceberg's writer")
    on.foreach(f => assert(f.nullCounts.nonEmpty && f.valueCounts.nonEmpty))
  }

  test("bigint") {
    val (off, on) = twin("bigint", "c BIGINT", "SELECT id * 1000 FROM range(500)")
    assert(on == off, "bigint stats must equal Iceberg's writer")
    on.foreach { f =>
      assert(f.lowerBounds.contains(1) && f.upperBounds.contains(1), "bigint carries bounds")
    }
  }

  test("decimal") {
    // several decimal precisions, as Velox sweeps decimal widths.
    for ((p, s) <- Seq((9, 0), (18, 4), (38, 10))) {
      val (off, on) = twin(
        s"decimal_${p}_$s",
        s"c DECIMAL($p,$s)",
        s"SELECT cast(id as decimal($p,$s)) / 7 FROM range(400)"
      )
      assert(on == off, s"decimal($p,$s) stats must equal Iceberg's writer")
    }
  }

  test("varchar") {
    val (off, on) =
      twin("varchar", "c STRING", "SELECT concat('row-', lpad(cast(id as string), 6, '0')) FROM range(400)")
    assert(on == off, "varchar stats must equal Iceberg's writer")
    on.foreach(f => assert(f.lowerBounds.contains(1) && f.upperBounds.contains(1)))
  }

  test("varbinary") {
    val (off, on) = twin("varbinary", "c BINARY", "SELECT cast(concat('b', id % 97) as binary) FROM range(400)")
    assert(on == off, "varbinary stats must equal Iceberg's writer")
  }

  test("varbinaryWithTransform") {
    // Velox partitions varbinary through a bucket transform; stats on the data column must still match.
    val (off, on) = {
      val (o, n) = ("ice.db.vbt_off", "ice.db.vbt_on")
      Seq(o, n).foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS $t")
        spark.sql(
          s"CREATE TABLE $t (id BIGINT, c BINARY) USING iceberg PARTITIONED BY (bucket(4, c)) " +
            s"TBLPROPERTIES ('format-version'='2')"
        )
      }
      val src = "SELECT id, cast(concat('k', id % 20) as binary) FROM range(400)"
      withWriter(on = false)(spark.sql(s"INSERT INTO $o $src").collect())
      val df = withWriter(on = true)(spark.sql(s"INSERT INTO $n $src"))
      df.collect()
      assert(columnar(df), "varbinaryWithTransform: VectorIcebergWriteExec must plan the append")
      (files(o), files(n))
    }
    assert(on == off, "varbinary-with-bucket-transform stats must equal Iceberg's writer")
  }

  test("multipleDataTypes") {
    val (off, on) = twin(
      "multipleDataTypes",
      "id BIGINT, c_bigint BIGINT, c_dec DECIMAL(18,4), c_varchar STRING, c_varbinary BINARY",
      """SELECT id,
        |  if(id % 5 = 0, cast(null as bigint), id * 3),
        |  cast(id % 9999 as decimal(18,4)) / 11,
        |  if(id % 7 = 0, cast(null as string), concat('v', id % 300)),
        |  cast(concat('x', id % 50) as binary)
        |FROM range(600)""".stripMargin
    )
    assert(on == off, "multiple-data-type stats must equal Iceberg's writer")
  }

  test("splitOffsets") {
    // a file large enough to roll into multiple row groups must report the same split offsets as Iceberg.
    val (off, on) = twin(
      "splitOffsets",
      "id BIGINT, s STRING",
      "SELECT id, concat('v', id % 300) FROM range(50000)",
      "'format-version'='2', 'write.parquet.row-group-size-bytes'='65536'"
    )
    assert(on == off, "split offsets + stats must equal Iceberg's writer")
    // The meaningful check is that the split offsets (and all stats) our writer records are byte-identical to
    // Iceberg's, which `on == off` asserts over the `split_offsets` column of the `files` metadata table.
    // (Appender-level split-offset presence is covered by IcebergParquetAppenderSuite.)
  }

  // ---- Comet CometIcebergWriteDetectionSuite analogs --------------------------------------------

  test("writeDetection_cleanParquetAppendRunsColumnar") {
    val df = withWriter(on = true) {
      spark.sql("DROP TABLE IF EXISTS ice.db.wd_clean")
      spark.sql("CREATE TABLE ice.db.wd_clean (id BIGINT, s STRING) USING iceberg")
      spark.sql("INSERT INTO ice.db.wd_clean SELECT id, concat('v', id) FROM range(1000)")
    }
    df.collect()
    assert(columnar(df), "a clean Parquet append must use our columnar writer, not fall back")
    assert(spark.sql("SELECT count(*) FROM ice.db.wd_clean").collect()(0).getLong(0) == 1000L)
  }

  test("writeDetection_shreddedVariantFallsBack") {
    // Comet falls back on a variant column; our writer falls back only when the variant is SHREDDED.
    val t = "ice.db.wd_variant"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"""CREATE TABLE $t (id BIGINT, v VARIANT) USING iceberg
         |TBLPROPERTIES ('format-version'='3', 'write.parquet.shred-variants'='true')""".stripMargin
    )
    val df =
      withWriter(on = true)(spark.sql(s"INSERT INTO $t SELECT id, parse_json(cast(id as string)) FROM range(20)"))
    df.collect()
    assert(!columnar(df), "a shredded variant must fall back to Iceberg's writer")
    assert(spark.sql(s"SELECT count(*) FROM $t").collect()(0).getLong(0) == 20L)
  }

  test("writeDetection_nestedPartitionSourceFallsBack") {
    // our writer resolves only top-level partition sources (PartitionKeys.unsupported); a nested source falls back.
    val (off, on) = ("ice.db.wd_np_off", "ice.db.wd_np_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(
        s"CREATE TABLE $t (id BIGINT, s STRUCT<k: STRING>) USING iceberg PARTITIONED BY (s.k)"
      )
    }
    val src = "SELECT id, named_struct('k', concat('p', id % 4)) FROM range(40)"
    withWriter(on = false)(spark.sql(s"INSERT INTO $off $src").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $src"))
    df.collect()
    assert(!columnar(df), "a nested partition source must fall back to Iceberg's writer")
    def text(t: String) = spark.sql(s"SELECT id, s.k FROM $t ORDER BY id").collect().map(_.toString).toSeq
    assert(text(on) == text(off), "the fallback writer's rows must still match Iceberg's writer")
  }
}
