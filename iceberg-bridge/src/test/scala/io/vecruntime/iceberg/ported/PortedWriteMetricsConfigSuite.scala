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
 * Ported from Apache Iceberg 1.12.0 (Apache License 2.0):
 *   spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/source/TestWriteMetricsConfig.java
 * Upstream verifies that the `write.metadata.metrics.*` table properties shape the per-column
 * statistics (null/value counts, lower/upper bounds) recorded in a data file's manifest entry.
 * Here each upstream case is run twice -- once with Iceberg's own row writer (our columnar writer
 * off) and once with it on -- and the `files` metadata table of the two tables must be identical
 * for every metrics mode. The original test name is preserved on each ported case. This asserts
 * that our columnar Parquet data-file writer honours MetricsConfig byte-for-byte against Iceberg's.
 */

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PortedWriteMetricsConfigSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("ported-writemetrics")
  private val Flag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ported-write-metrics-config")
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

  /** A data file's per-column stats from the `files` metadata table, keyed by field id. */
  private final case class FileStats(
      recordCount: Long,
      valueCounts: Map[Int, Long],
      nullCounts: Map[Int, Long],
      lowerBounds: Map[Int, String],
      upperBounds: Map[Int, String]
  )

  private def mapOf(r: Row, i: Int): Map[Int, Long] =
    Option(r.getJavaMap[Int, Long](i)).map(_.asScala.toMap).getOrElse(Map.empty)

  private def hexMapOf(r: Row, i: Int): Map[Int, String] =
    Option(r.getJavaMap[Int, Array[Byte]](i))
      .map(_.asScala.toMap.map { case (k, v) => k -> v.map(b => f"$b%02x").mkString })
      .getOrElse(Map.empty)

  /** The `files` table, one FileStats per data file, ordered so a twin comparison is stable. */
  private def files(t: String): Seq[FileStats] =
    spark
      .sql(
        s"""SELECT record_count, value_counts, null_value_counts, lower_bounds, upper_bounds
           |FROM $t.files ORDER BY file_path""".stripMargin
      )
      .collect()
      .toSeq
      .map(r => FileStats(r.getLong(0), mapOf(r, 1), mapOf(r, 2), hexMapOf(r, 3), hexMapOf(r, 4)))

  /**
   * Create a writer-off twin and a writer-on table with the same DDL (columns + TBLPROPERTIES),
   * write the same rows into each (Iceberg's writer into the off table, ours into the on table),
   * assert the columnar exec ran on the on side, and return (off stats, on stats).
   */
  private def twinFiles(
      name: String,
      columns: String,
      props: String,
      insert: String
  ): (Seq[FileStats], Seq[FileStats]) = {
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

  private val Simple = "id INT, data STRING"
  // coalesce(1) in the upstream test keeps everything in a single data file; a tiny row set here does too.
  private val SimpleRows = "SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c') AS t(id, data)"

  test("testFullMetricsCollectionForParquet") {
    val (off, on) = twinFiles(
      "full",
      Simple,
      "'format-version'='2', 'write.metadata.metrics.default'='full'",
      SimpleRows
    )
    assert(on == off, "full metrics must equal Iceberg's writer")
    on.foreach { f =>
      assert(f.nullCounts.size == 2)
      assert(f.valueCounts.size == 2)
      assert(f.lowerBounds.size == 2)
      assert(f.upperBounds.size == 2)
    }
  }

  test("testCountMetricsCollectionForParquet") {
    val (off, on) = twinFiles(
      "counts",
      Simple,
      "'format-version'='2', 'write.metadata.metrics.default'='counts'",
      SimpleRows
    )
    assert(on == off, "counts metrics must equal Iceberg's writer")
    on.foreach { f =>
      assert(f.nullCounts.size == 2)
      assert(f.valueCounts.size == 2)
      assert(f.lowerBounds.isEmpty, "counts mode records no bounds")
      assert(f.upperBounds.isEmpty, "counts mode records no bounds")
    }
  }

  test("testNoMetricsCollectionForParquet") {
    val (off, on) = twinFiles(
      "none",
      Simple,
      "'format-version'='2', 'write.metadata.metrics.default'='none'",
      SimpleRows
    )
    assert(on == off, "none metrics must equal Iceberg's writer")
    on.foreach { f =>
      assert(f.nullCounts.isEmpty)
      assert(f.valueCounts.isEmpty)
      assert(f.lowerBounds.isEmpty)
      assert(f.upperBounds.isEmpty)
    }
  }

  test("testCustomMetricCollectionForParquet") {
    // default counts, but column `id` overridden to full: only id carries bounds.
    val (off, on) = twinFiles(
      "custom",
      Simple,
      "'format-version'='2', 'write.metadata.metrics.default'='counts', 'write.metadata.metrics.column.id'='full'",
      SimpleRows
    )
    assert(on == off, "custom per-column override must equal Iceberg's writer")
    // field id of `id` is 1 in a fresh two-column schema (id, data).
    on.foreach { f =>
      assert(f.nullCounts.size == 2)
      assert(f.valueCounts.size == 2)
      assert(f.lowerBounds.keySet == Set(1), s"only id (field 1) has a lower bound, got ${f.lowerBounds.keySet}")
      assert(f.upperBounds.keySet == Set(1), s"only id (field 1) has an upper bound, got ${f.upperBounds.keySet}")
    }
  }

  test("testCustomMetricCollectionForNestedParquet") {
    // none default; longCol->counts, record.id->full, record.data->truncate(2); partitioned by identity(strCol).
    val columns =
      "longCol INT, strCol STRING, record STRUCT<id: INT, data: STRING>"
    val props =
      "'format-version'='2', 'write.metadata.metrics.default'='none', " +
        "'write.metadata.metrics.column.longCol'='counts', " +
        "'write.metadata.metrics.column.record.id'='full', " +
        "'write.metadata.metrics.column.record.data'='truncate(2)'"
    val (off, on) = {
      val (o, n) = (s"ice.db.nested_off", s"ice.db.nested_on")
      Seq(o, n).foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS $t")
        spark.sql(
          s"CREATE TABLE $t ($columns) USING iceberg PARTITIONED BY (strCol) TBLPROPERTIES ($props)"
        )
      }
      val src =
        """SELECT cast(id as int), concat('g', id % 3),
          |  named_struct('id', cast(id as int), 'data', concat('d', id)) FROM range(10)""".stripMargin
      withWriter(on = false)(spark.sql(s"INSERT INTO $o $src").collect())
      val df = withWriter(on = true)(spark.sql(s"INSERT INTO $n $src"))
      df.collect()
      assert(columnar(df), "nested: VectorIcebergWriteExec must plan the append")
      (files(o), files(n))
    }
    assert(on == off, "nested per-column overrides must equal Iceberg's writer")
    // field ids: longCol=1, strCol=2, record=3, record.id=4, record.data=5.
    on.foreach { f =>
      assert(f.nullCounts.keySet == Set(1, 4, 5), s"null counts keys ${f.nullCounts.keySet}")
      assert(f.valueCounts.keySet == Set(1, 4, 5), s"value counts keys ${f.valueCounts.keySet}")
      // record.id full -> bounds; record.data truncate(2) -> bounds truncated to 2 bytes; longCol counts -> none.
      assert(f.lowerBounds.keySet == Set(4, 5), s"lower bounds keys ${f.lowerBounds.keySet}")
      assert(f.upperBounds.keySet == Set(4, 5), s"upper bounds keys ${f.upperBounds.keySet}")
      // truncate(2): the record.data bound is at most 2 bytes = 4 hex chars.
      assert(f.lowerBounds(5).length <= 4, s"record.data lower bound truncated: ${f.lowerBounds(5)}")
      assert(f.upperBounds(5).length <= 4, s"record.data upper bound truncated: ${f.upperBounds(5)}")
    }
  }

  test("testMaxInferredColumnDefaults") {
    // write.metadata.metrics.max-inferred-column-defaults bounds how many columns get inferred bounds.
    val columns = "c1 INT, c2 INT, c3 INT"
    val (off, on) = twinFiles(
      "maxinferred",
      columns,
      "'format-version'='2', 'write.metadata.metrics.max-inferred-column-defaults'='2'",
      "SELECT cast(id as int), cast(id as int), cast(id as int) FROM range(100)"
    )
    assert(on == off, "inferred-default limit must equal Iceberg's writer")
    on.foreach { f =>
      // counts are kept for all columns; bounds only for the first two inferred (fields 1,2), not field 3.
      assert(f.lowerBounds.keySet == Set(1, 2), s"only first 2 columns inferred bounds, got ${f.lowerBounds.keySet}")
      assert(f.upperBounds.keySet == Set(1, 2), s"only first 2 columns inferred bounds, got ${f.upperBounds.keySet}")
    }
  }
}
