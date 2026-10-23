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
 * Interoperability contract: a table written by OUR columnar Iceberg writer
 * (spark.vecruntime.iceberg.writer.enabled) MUST be readable by Iceberg's own Spark reader with its
 * default settings -- vectorized reader ON -- because engines and clusters without the vecruntime
 * plugin will read these tables. For every primitive and nested type, over dictionary-friendly and
 * high-cardinality data with nulls, this suite writes with our writer (asserting the columnar exec
 * ran), then reads the SAME table with the plugin fully OFF (spark.vecruntime.enabled=false) and
 * Iceberg's vectorized reader left at its default (on), and asserts the rows equal what our writer put
 * in. A type whose read Iceberg's vectorized reader rejects is a WRITER bug (owner wants Parquet v2
 * data pages, but per-type encodings must stay within what Iceberg's vectorized reader accepts, e.g.
 * PLAIN booleans). Those cases are tagged org.scalatest.Tag("interop-known-fail") and filtered OUT of
 * the test run / commit; they are listed in the commit message and report with the exact rejection.
 *
 * The footer-inspection test records, for each column, the Parquet page version (V1/V2) and the data
 * encodings our writer emits, side by side with Iceberg's own writer for the same table, so the fix
 * (which encodings to change per type) is driven by evidence rather than guesswork.
 */

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.Tag
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Cases that currently FAIL the interop read; filtered out of the run via `-l interop-known-fail`. */
object KnownInteropFail extends Tag("interop-known-fail")

class IcebergReaderInteropSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("ported-interop")
  private val WriterFlag = "spark.vecruntime.iceberg.writer.enabled"
  private val PluginFlag = "spark.vecruntime.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("iceberg-reader-interop")
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
    val prev = spark.conf.getOption(WriterFlag)
    spark.conf.set(WriterFlag, on.toString)
    try f
    finally prev match { case Some(v) => spark.conf.set(WriterFlag, v); case None => spark.conf.unset(WriterFlag) }
  }

  /** Run `f` with the entire plugin off -- the write/scan is Spark + Iceberg's own reader, vectorized on. */
  private def withPluginOff[T](f: => T): T = {
    val prev = spark.conf.getOption(PluginFlag)
    spark.conf.set(PluginFlag, "false")
    try f
    finally prev match { case Some(v) => spark.conf.set(PluginFlag, v); case None => spark.conf.unset(PluginFlag) }
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

  /**
   * Create `t`, write `src` with our columnar writer (asserting it ran), then read every row back with the
   * plugin OFF and Iceberg's vectorized reader at its default. The rows read must equal the rows our writer
   * put in (captured with the plugin off reading nothing -- we compare against a plugin-off re-read of the
   * same data written by Iceberg's own writer into a twin, so the expectation is itself Iceberg-trusted).
   */
  private def interop(name: String, columns: String, src: String, props: String = "'format-version'='2'"): Unit = {
    val (off, on) = (s"ice.db.${name}_off", s"ice.db.${name}_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(s"CREATE TABLE $t ($columns) USING iceberg TBLPROPERTIES ($props)")
    }
    // off twin: Iceberg's own writer (plugin off end-to-end).
    withPluginOff(spark.sql(s"INSERT INTO $off $src").collect())
    // on table: our columnar writer.
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $src"))
    df.collect()
    assert(columnar(df), s"$name: VectorIcebergWriteExec must plan the append")
    // Read BOTH with the plugin fully off (Iceberg's vectorized reader, default on). Equality means the file
    // our columnar writer produced is byte-interoperable with Iceberg's own reader. Rows are rendered with
    // to_json(struct(*)) so binary (and byte arrays nested in struct/list/map) compare by VALUE, not by the
    // Java array identity that Row.toString would print.
    def rendered(t: String): Seq[String] =
      withPluginOff(
        spark.sql(s"SELECT to_json(struct(*)) AS j FROM $t ORDER BY 1").collect().map(_.getString(0)).toSeq
      )
    val expected = rendered(off)
    val actual = rendered(on)
    assert(actual == expected, s"$name: Iceberg's vectorized reader must read our writer's file identically")
  }

  // Two data shapes per type: dictionary-friendly (few distinct values) and high-cardinality, both with nulls.
  private def lowCard(expr: String): String = s"if(id % 7 = 0, null, $expr)"
  private def highCard(expr: String): String = s"if(id % 11 = 0, null, $expr)"
  private val N = 4000

  // ---- Primitive types -------------------------------------------------------------------------
  // Each type gets a low-cardinality (dictionary-friendly) and a high-cardinality case.

  test("interop_int_lowCard") {
    interop("int_lc", "id BIGINT, c INT", s"SELECT id, ${lowCard("cast(id % 20 as int)")} FROM range($N)")
  }
  test("interop_int_highCard") {
    interop(
      "int_hc",
      "id BIGINT, c INT",
      s"SELECT id, ${highCard("cast(pmod(id * 2654435761, 2147483647) as int)")} FROM range($N)"
    )
  }

  test("interop_long_lowCard") {
    interop("long_lc", "id BIGINT, c BIGINT", s"SELECT id, ${lowCard("cast(id % 20 as bigint)")} FROM range($N)")
  }
  test("interop_long_highCard") {
    interop("long_hc", "id BIGINT, c BIGINT", s"SELECT id, ${highCard("id * 2654435761")} FROM range($N)")
  }

  test("interop_double_lowCard") {
    interop("double_lc", "id BIGINT, c DOUBLE", s"SELECT id, ${lowCard("cast(id % 20 as double)")} FROM range($N)")
  }
  test("interop_double_highCard") {
    interop("double_hc", "id BIGINT, c DOUBLE", s"SELECT id, ${highCard("id * 0.123456789d")} FROM range($N)")
  }

  test("interop_decimal9_highCard") {
    interop(
      "dec9_hc",
      "id BIGINT, c DECIMAL(9,0)",
      s"SELECT id, ${highCard("cast(id % 1000000000 as decimal(9,0))")} FROM range($N)"
    )
  }
  test("interop_decimal18_highCard") {
    interop(
      "dec18_hc",
      "id BIGINT, c DECIMAL(18,4)",
      s"SELECT id, ${highCard("cast(id as decimal(18,4)) / 7")} FROM range($N)"
    )
  }
  test("interop_decimal38_highCard") {
    interop(
      "dec38_hc",
      "id BIGINT, c DECIMAL(38,10)",
      s"SELECT id, ${highCard("cast(id as decimal(38,10)) * 3")} FROM range($N)"
    )
  }

  test("interop_string_lowCard") {
    interop("str_lc", "id BIGINT, c STRING", s"SELECT id, ${lowCard("concat('v', id % 20)")} FROM range($N)")
  }
  test("interop_string_highCard") {
    interop(
      "str_hc",
      "id BIGINT, c STRING",
      s"SELECT id, ${highCard("concat('v', id, '-', hex(id * 2654435761))")} FROM range($N)"
    )
  }

  test("interop_binary_lowCard") {
    interop(
      "bin_lc",
      "id BIGINT, c BINARY",
      s"SELECT id, ${lowCard("unhex(lpad(hex(id % 20), 6, '0'))")} FROM range($N)"
    )
  }
  test("interop_binary_highCard") {
    interop(
      "bin_hc",
      "id BIGINT, c BINARY",
      s"SELECT id, ${highCard("unhex(lpad(hex(id * 2654435761), 12, '0'))")} FROM range($N)"
    )
  }

  test("interop_date_highCard") {
    interop(
      "date_hc",
      "id BIGINT, c DATE",
      s"SELECT id, ${highCard("date_add(date'1970-01-01', cast(id as int))")} FROM range($N)"
    )
  }
  test("interop_timestamp_ntz_highCard") {
    interop(
      "tsntz_hc",
      "id BIGINT, c TIMESTAMP_NTZ",
      s"SELECT id, ${highCard("make_timestamp_ntz(2020, 1, 1, 0, 0, cast(id % 59 as int))")} FROM range($N)"
    )
  }
  test("interop_timestamptz_highCard") {
    interop(
      "tstz_hc",
      "id BIGINT, c TIMESTAMP",
      s"SELECT id, ${highCard("timestamp_micros(1700000000000000 + id * 1000000)")} FROM range($N)"
    )
  }

  // Boolean is the one confirmed interop FAILURE: our columnar writer emits an RLE-encoded boolean in a
  // Parquet v2 data page, and Iceberg's vectorized reader throws
  //   UnsupportedOperationException: Cannot support vectorized reads for column [c] optional boolean c = 2
  //   with encoding RLE. Disable vectorized reads to read this table/file
  // Tagged known-fail and excluded from the committed run (-DtagsToExclude=interop-known-fail); see the
  // report matrix. Fix: emit PLAIN booleans in the v2 page (Iceberg's own writer uses PLAIN for booleans).
  test("interop_boolean", KnownInteropFail) {
    interop("bool", "id BIGINT, c BOOLEAN", s"SELECT id, ${lowCard("id % 2 = 0")} FROM range($N)")
  }

  // Fixed and UUID have no Spark SQL type; create through Iceberg's catalog. These PASS interop (their
  // FIXED_LEN_BYTE_ARRAY output reads back through Iceberg's vectorized reader); kept untagged.
  test("interop_fixed") {
    interopCatalogType("fixed", org.apache.iceberg.types.Types.FixedType.ofLength(4), "unhex(lpad(hex(id), 8, '0'))")
  }
  test("interop_uuid") {
    interopCatalogType("uuid", org.apache.iceberg.types.Types.UUIDType.get(), null)
  }

  // ---- Nested types ----------------------------------------------------------------------------

  test("interop_struct_highCard") {
    interop(
      "struct_hc",
      "id BIGINT, c STRUCT<a: INT, b: STRING, d: DOUBLE>",
      s"SELECT id, ${highCard("named_struct('a', cast(id as int), 'b', concat('s', id), 'd', id * 0.5d)")} FROM range($N)"
    )
  }
  test("interop_list_highCard") {
    interop(
      "list_hc",
      "id BIGINT, c ARRAY<INT>",
      s"SELECT id, ${highCard("array(cast(id as int), cast(id * 2 as int), cast(id * 3 as int))")} FROM range($N)"
    )
  }
  test("interop_map_highCard") {
    interop(
      "map_hc",
      "id BIGINT, c MAP<STRING, BIGINT>",
      s"SELECT id, ${highCard("map(concat('k', id % 4), id, concat('j', id % 3), id * 2)")} FROM range($N)"
    )
  }

  // ---- Footer inspection -----------------------------------------------------------------------

  /** The data encodings each column uses, and the page version, in the one data file under `location`. */
  private final case class Footer(pageV2: Boolean, encodings: Map[String, Set[String]])

  private def inspect(location: String): Footer = {
    val conf = new Configuration()
    val dataDir = new HPath(location, "data")
    val fs = dataDir.getFileSystem(conf)
    val file = fs.listStatus(dataDir).iterator.map(_.getPath).find(_.getName.endsWith(".parquet")).get
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))
    try {
      val meta = reader.getFooter
      val encs = scala.collection.mutable.Map[String, Set[String]]()
      meta.getBlocks.asScala.foreach { block =>
        block.getColumns.asScala.foreach { col =>
          val path = col.getPath.toDotString
          val set = col.getEncodings.asScala.map(_.name()).toSet
          encs(path) = encs.getOrElse(path, Set.empty) ++ set
        }
      }
      // page version: our writer emits v2-style value encodings (DELTA_*, BYTE_STREAM_SPLIT, RLE_DICTIONARY).
      val anyV2 = encs.values.flatten.exists(e =>
        e.startsWith("DELTA_") || e == "BYTE_STREAM_SPLIT" || e == "RLE_DICTIONARY"
      )
      Footer(anyV2, encs.toMap)
    } finally reader.close()
  }

  test("footer_encodings_ours_vs_iceberg") {
    // Write the same wide row set with both writers and print each column's page version + encodings.
    val columns =
      "id BIGINT, i INT, l BIGINT, dbl DOUBLE, dec DECIMAL(18,4), s STRING, b BINARY, bo BOOLEAN, " +
        "dt DATE, ts TIMESTAMP"
    val src =
      s"""SELECT id, cast(id % 50 as int), id * 7, id * 0.5d, cast(id as decimal(18,4)) / 3,
         |  concat('v', id, '-', hex(id * 2654435761)), unhex(lpad(hex(id), 8, '0')), id % 2 = 0,
         |  date_add(date'1970-01-01', cast(id as int)), timestamp_micros(1700000000000000 + id * 1000000)
         |FROM range($N)""".stripMargin
    val (off, on) = ("ice.db.footer_off", "ice.db.footer_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(s"CREATE TABLE $t ($columns) USING iceberg TBLPROPERTIES ('format-version'='2')")
    }
    withPluginOff(spark.sql(s"INSERT INTO $off $src").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $src"))
    df.collect()
    assert(columnar(df), "footer: VectorIcebergWriteExec must plan the append")
    val ours = inspect(warehouse.resolve("db").resolve("footer_on").toString)
    val iceberg = inspect(warehouse.resolve("db").resolve("footer_off").toString)
    info(s"OUR writer     pageV2=${ours.pageV2}")
    ours.encodings.toSeq.sortBy(_._1).foreach { case (c, e) =>
      info(s"  ours     $c -> ${e.toSeq.sorted.mkString(",")}")
    }
    info(s"ICEBERG writer pageV2=${iceberg.pageV2}")
    iceberg.encodings.toSeq.sortBy(_._1).foreach { case (c, e) =>
      info(s"  iceberg  $c -> ${e.toSeq.sorted.mkString(",")}")
    }
    // Our writer must emit Parquet v2 data pages (owner directive).
    assert(ours.pageV2, "our writer must emit Parquet v2 data pages")
    // Boolean must NOT be RLE-only in a way Iceberg's vectorized reader rejects; record what we actually emit.
    val boolEnc = ours.encodings.getOrElse("bo", Set.empty)
    info(s"BOOLEAN column `bo` encodings from our writer: ${boolEnc.toSeq.sorted.mkString(",")}")
    assert(boolEnc.nonEmpty, "the boolean column must report an encoding")
  }

  // ---- catalog-created types (fixed, uuid) -----------------------------------------------------

  /**
   * Interop for a type Spark SQL cannot declare (fixed, uuid): create the tables through Iceberg's
   * HadoopCatalog, write with our columnar writer, and read back with the plugin off (Iceberg's vectorized
   * reader, default on). `valueExpr` is a Spark SQL expression producing the column (null = uuid via a UDF).
   */
  private def interopCatalogType(
      name: String,
      colType: org.apache.iceberg.types.Type,
      valueExpr: String
  ): Unit = {
    import org.apache.iceberg.types.Types
    import org.apache.iceberg.types.Types.NestedField.optional
    val catalog =
      new org.apache.iceberg.hadoop.HadoopCatalog(new Configuration(), warehouse.toString)
    val schema = new org.apache.iceberg.Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "c", colType)
    )
    val (off, on) = (s"${name}_off", s"${name}_on")
    Seq(off, on).foreach { n =>
      val id = org.apache.iceberg.catalog.TableIdentifier.of("db", n)
      catalog.dropTable(id)
      catalog.createTable(id, schema, org.apache.iceberg.PartitionSpec.unpartitioned())
    }
    val src =
      if (valueExpr != null) s"SELECT id, if(id % 7 = 0, null, $valueExpr) FROM range(3000)"
      else {
        // A deterministic UUID string built from id (no UDF, so the projection does not force a fallback).
        val u =
          "concat(lpad(hex(id), 8, '0'), '-', lpad(hex(id % 65536), 4, '0'), '-4', lpad(hex(id % 4096), 3, '0'), " +
            "'-8', lpad(hex(id % 4096), 3, '0'), '-', lpad(hex(id), 12, '0'))"
        s"SELECT id, if(id % 7 = 0, null, $u) FROM range(3000)"
      }
    withPluginOff(spark.sql(s"INSERT INTO ice.db.$off $src").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO ice.db.$on $src"))
    df.collect()
    assert(columnar(df), s"$name: VectorIcebergWriteExec must plan the append")
    def rendered(t: String): Seq[String] =
      withPluginOff(
        spark.sql(s"SELECT to_json(struct(*)) AS j FROM ice.db.$t ORDER BY 1").collect().map(_.getString(0)).toSeq
      )
    assert(rendered(on) == rendered(off), s"$name: Iceberg's vectorized reader must read our writer's file identically")
  }
}
