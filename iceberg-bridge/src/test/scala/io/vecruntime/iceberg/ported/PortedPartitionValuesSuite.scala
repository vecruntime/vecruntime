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
 *   spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/source/TestPartitionValues.java
 * Upstream writes partitioned data and reads it back, checking the value that lands in each
 * partition for null keys, reordered columns, nested string partition keys, and every supported
 * primitive type as an identity partition column. Here each upstream case is a twin: the same
 * rows are written with Iceberg's own row writer (our columnar writer off) and with ours on, and
 * the rows read back plus the `partitions` metadata table must be identical. The original test
 * names are preserved. (The upstream FileFormat/vectorized parameter grid is not ported: our
 * writer only produces Parquet, and the read-back path is the engine's own columnar reader.)
 */

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PortedPartitionValuesSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("ported-partvalues")
  private val Flag = "spark.vecruntime.iceberg.writer.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ported-partition-values")
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

  private def rows(t: String, order: String): Seq[Row] =
    spark.sql(s"SELECT * FROM $t ORDER BY $order").collect().toSeq

  /** The partitions metadata table: (partition-as-string, record_count), ordered. */
  private def partitions(t: String): Seq[Row] =
    spark.sql(s"SELECT cast(partition as string), record_count FROM $t.partitions ORDER BY 1").collect().toSeq

  test("testNullPartitionValue") {
    val (off, on) = ("ice.db.nullpv_off", "ice.db.nullpv_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(s"CREATE TABLE $t (id INT, data STRING) USING iceberg PARTITIONED BY (data)")
    }
    val src = "SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c'), (4, cast(null as string)) AS t(id, data)"
    withWriter(on = false)(spark.sql(s"INSERT INTO $off $src").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $src"))
    df.collect()
    assert(columnar(df), "VectorIcebergWriteExec must plan the null-partition append")
    assert(rows(on, "id") == rows(off, "id"), "rows (including the null-partition row) must match Iceberg's writer")
    assert(partitions(on) == partitions(off), "the null partition must carry the same record count")
    // the null-key row is readable
    assert(spark.sql(s"SELECT count(*) FROM $on WHERE data IS NULL").collect()(0).getLong(0) == 1L)
  }

  test("testReorderedColumns") {
    // write the columns in a different order than the table schema (data, id vs id, data).
    val (off, on) = ("ice.db.reorder_off", "ice.db.reorder_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(s"CREATE TABLE $t (id INT, data STRING) USING iceberg PARTITIONED BY (data)")
    }
    // INSERT BY NAME lets the source present (data, id) and still land correctly, like upstream's check-ordering=false.
    val src = "SELECT * FROM VALUES ('a',1), ('b',2), ('c',3) AS t(data, id)"
    withWriter(on = false)(spark.sql(s"INSERT INTO $off BY NAME $src").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on BY NAME $src"))
    df.collect()
    assert(columnar(df), "VectorIcebergWriteExec must plan the reordered append")
    assert(rows(on, "id") == rows(off, "id"), "reordered-column rows must match Iceberg's writer")
    assert(partitions(on) == partitions(off))
  }

  test("testPartitionedByNestedString") {
    // identity partition on a string inside a struct; upstream guards a ClassCastException building the path.
    // Since #955 nested partition sources are carried on lanes, so this append runs columnar.
    val (off, on) = ("ice.db.nestedstr_off", "ice.db.nestedstr_on")
    Seq(off, on).foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
      spark.sql(
        s"CREATE TABLE $t (struct STRUCT<string: STRING>) USING iceberg PARTITIONED BY (struct.string)"
      )
    }
    val src =
      "SELECT named_struct('string', concat('nested_string_value_', cast(id % 3 as string))) FROM range(9)"
    withWriter(on = false)(spark.sql(s"INSERT INTO $off $src").collect())
    val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $src"))
    df.collect()
    assert(columnar(df), "a nested partition source must run on the columnar writer")
    def text(t: String) =
      spark.sql(s"SELECT struct.string FROM $t ORDER BY struct.string").collect().map(_.toString).toSeq
    assert(text(on) == text(off), "nested-string-partition rows must match Iceberg's writer")
    assert(partitions(on) == partitions(off))
  }

  test("testPartitionValueTypes") {
    // identity partition over every supported primitive type, one column at a time, comparing rows + partitions.
    // Rows are read back with Iceberg's vectorized reader at its default (on): since #963 booleans are written
    // PLAIN, so every type our writer emits is readable by it.
    val columns =
      "id BIGINT, b BOOLEAN, i INT, l BIGINT, f FLOAT, d DOUBLE, dt DATE, ts TIMESTAMP, s STRING, " +
        "bytes BINARY, dec9 DECIMAL(9,0), dec11 DECIMAL(11,2), dec38 DECIMAL(38,10)"
    val src =
      """SELECT id,
        |  id % 2 = 0 as b, cast(id % 100 as int) as i, id * 7 as l,
        |  cast(id as float) / 3 as f, id * 0.25d as d,
        |  date_add(date'2020-01-01', cast(id % 60 as int)) as dt,
        |  timestamp_micros(1700000000000000 + id * 1000000) as ts,
        |  concat('s', id % 7) as s,
        |  cast(concat('b', id % 5) as binary) as bytes,
        |  cast(id % 1000 as decimal(9,0)) as dec9,
        |  cast(id % 10000 as decimal(11,2)) / 7 as dec11,
        |  cast(id as decimal(38,10)) * 3 as dec38
        |FROM range(40)""".stripMargin
    val partCols = Seq("b", "i", "l", "f", "d", "dt", "ts", "s", "bytes", "dec9", "dec11", "dec38")
    for (col <- partCols) {
      val (off, on) = (s"ice.db.pvt_${col}_off", s"ice.db.pvt_${col}_on")
      Seq(off, on).foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS $t")
        spark.sql(
          s"CREATE TABLE $t ($columns) USING iceberg PARTITIONED BY ($col) " +
            s"TBLPROPERTIES ('write.distribution-mode'='none', 'write.spark.fanout.enabled'='false')"
        )
      }
      withWriter(on = false)(spark.sql(s"INSERT INTO $off $src").collect())
      val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $src"))
      df.collect()
      assert(columnar(df), s"partition by $col: VectorIcebergWriteExec must plan the append")
      assert(rows(on, "id") == rows(off, "id"), s"partition by $col: rows must match Iceberg's writer")
      assert(partitions(on) == partitions(off), s"partition by $col: partitions must match Iceberg's writer")
    }
  }

  test("testNestedPartitionValues") {
    // identity partition over a primitive nested inside a struct, for a representative set of types.
    // Asserts only data/partition equality with Iceberg's writer, which holds whether our columnar writer
    // handles the write directly or routes it through Iceberg's. Nested partition sources are carried on
    // lanes and take the columnar path once #955 lands.
    val columns =
      "id BIGINT, nested STRUCT<i: INT, s: STRING, d: DOUBLE, dt: DATE, dec: DECIMAL(11,2)>"
    val src =
      """SELECT id, named_struct(
        |  'i', cast(id % 50 as int),
        |  's', concat('n', id % 6),
        |  'd', id * 0.5d,
        |  'dt', date_add(date'2021-06-01', cast(id % 30 as int)),
        |  'dec', cast(id % 5000 as decimal(11,2)) / 3
        |) FROM range(40)""".stripMargin
    for (col <- Seq("nested.i", "nested.s", "nested.d", "nested.dt", "nested.dec")) {
      val tag = col.replace('.', '_')
      val (off, on) = (s"ice.db.npv_${tag}_off", s"ice.db.npv_${tag}_on")
      Seq(off, on).foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS $t")
        spark.sql(
          s"CREATE TABLE $t ($columns) USING iceberg PARTITIONED BY ($col) " +
            s"TBLPROPERTIES ('write.distribution-mode'='none', 'write.spark.fanout.enabled'='false')"
        )
      }
      withWriter(on = false)(spark.sql(s"INSERT INTO $off $src").collect())
      val df = withWriter(on = true)(spark.sql(s"INSERT INTO $on $src"))
      df.collect()
      def text(t: String) =
        spark.sql(s"SELECT id, nested FROM $t ORDER BY id").collect().map(_.toString).toSeq
      assert(text(on) == text(off), s"partition by $col: rows must match Iceberg's writer")
      assert(partitions(on) == partitions(off), s"partition by $col: partitions must match Iceberg's writer")
    }
  }
}
