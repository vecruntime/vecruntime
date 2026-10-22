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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vecruntime.ParquetFileResolver
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #901 R1, end to end: the native Iceberg scan with Iceberg's own column resolution (SparkParquetFileResolver over
 * IcebergReadSpecs) reads every file of an evolved table natively -- no file handed to Spark's reader, no task
 * group to Iceberg's -- and returns the rows Iceberg's own reader returns. The schema evolution matrix: renamed,
 * reordered, added and dropped columns, a new column taking a renamed one's name, promotions, nested evolution in
 * structs, lists and maps, v3 initial defaults, files without field ids read through the name mapping or Iceberg's
 * positional fallback ids.
 */
class IcebergReadSpecsScanSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("icereadspecs")
  private val Specs = "spark.vecruntime.iceberg.readSpecs.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("iceberg-read-specs")
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

  private def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)

  /**
   * `q` read by Iceberg's own reader (the plugin off), by the native scan with Iceberg's resolution, and by the
   * native scan with Spark's; the first two must agree row for row, the resolution handing no file to Spark's
   * reader and no task group to Iceberg's. Returns the files Spark's resolution handed to Spark's reader.
   */
  private def check(q: String, previousFails: Boolean = false): Long = {
    val expected = withConf("spark.vecruntime.enabled" -> "false")(sorted(spark.sql(q).collect().toSeq))
    val before = ParquetFileResolver.sparkReaderFiles.get()
    val df: DataFrame = spark.sql(q)
    val got = sorted(df.collect().toSeq)
    val toSpark = ParquetFileResolver.sparkReaderFiles.get() - before
    val scans = nodes(df.queryExecution.executedPlan).filter(_.getClass.getSimpleName == "VectorIcebergScanExec")
    assert(scans.nonEmpty, s"not the native scan: ${df.queryExecution.executedPlan}")
    assert(scans.map(_.metrics("numIcebergGroups").value).sum == 0, "a task group went to Iceberg's reader")
    assert(toSpark == 0, s"$toSpark file(s) went to Spark's reader")
    assert(got == expected, s"$q\nnative: ${got.take(5)}\nIceberg: ${expected.take(5)}")
    val beforeOld = ParquetFileResolver.sparkReaderFiles.get()
    if (previousFails) {
      intercept[Exception](withConf(Specs -> "false")(spark.sql(q).collect()))
      return ParquetFileResolver.sparkReaderFiles.get() - beforeOld
    }
    val old = withConf(Specs -> "false")(sorted(spark.sql(q).collect().toSeq))
    assert(old == expected, s"$q: the previous resolution differs from Iceberg's")
    ParquetFileResolver.sparkReaderFiles.get() - beforeOld
  }

  private def withConf[T](kv: (String, String))(f: => T): T = {
    val prev = spark.conf.getOption(kv._1)
    spark.conf.set(kv._1, kv._2)
    try f
    finally prev match { case Some(v) => spark.conf.set(kv._1, v); case None => spark.conf.unset(kv._1) }
  }

  test("renamed, reordered, added and dropped columns; a new column taking a renamed one's name") {
    val t = "ice.db.evolved"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(s"CREATE TABLE $t (a INT, b STRING, c BIGINT, d DOUBLE) USING iceberg")
    spark.sql(s"INSERT INTO $t SELECT cast(id AS INT), concat('b', id), id * 3, id / 2.0 FROM range(0, 2000)")
    spark.sql(s"ALTER TABLE $t RENAME COLUMN b TO b_old")
    spark.sql(s"ALTER TABLE $t ADD COLUMN b STRING") // takes the renamed column's old name
    spark.sql(s"ALTER TABLE $t DROP COLUMN c")
    spark.sql(s"ALTER TABLE $t ALTER COLUMN d FIRST")
    spark.sql(s"ALTER TABLE $t ADD COLUMN e INT AFTER a")
    spark.sql(
      s"INSERT INTO $t SELECT id / 4.0, cast(id AS INT), cast(id % 7 AS INT), concat('o', id), concat('n', id) " +
        "FROM range(2000, 3000)"
    )
    val clashes = check(s"SELECT * FROM $t")
    check(s"SELECT b, a FROM $t WHERE a % 5 = 0")
    check(s"SELECT b_old, count(*) FROM $t GROUP BY b_old")
    // The old files hold a column named `b` (now b_old) beside the new `b`: Spark's resolution hands them to
    // Spark's reader (#568 idNameClash); Iceberg's reads them natively.
    assert(clashes > 0, "expected the previous resolution to hand the old files to Spark's reader")
  }

  test("promoted columns: int to bigint, float to double, decimal precision") {
    val t = "ice.db.promoted"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(s"CREATE TABLE $t (i INT, f FLOAT, dm DECIMAL(9, 2), dw DECIMAL(9, 2)) USING iceberg")
    spark.sql(s"INSERT INTO $t SELECT cast(id AS INT) - 1000, cast(id AS FLOAT) / 3, " +
      "cast(id AS DECIMAL(9, 2)) / 7, cast(-id AS DECIMAL(9, 2)) / 3 FROM range(0, 2000)")
    spark.sql(s"ALTER TABLE $t ALTER COLUMN i TYPE BIGINT")
    spark.sql(s"ALTER TABLE $t ALTER COLUMN f TYPE DOUBLE")
    spark.sql(s"ALTER TABLE $t ALTER COLUMN dm TYPE DECIMAL(18, 2)")
    spark.sql(s"ALTER TABLE $t ALTER COLUMN dw TYPE DECIMAL(30, 2)")
    spark.sql(s"INSERT INTO $t SELECT id * 100000000000, id / 3.0, cast(id AS DECIMAL(18, 2)), " +
      "cast(id AS DECIMAL(30, 2)) * 1000000000000 FROM range(0, 500)")
    check(s"SELECT * FROM $t")
    check(s"SELECT sum(i), sum(dm), sum(dw), max(f) FROM $t")
  }

  test("nested evolution: struct fields renamed and added, inside lists and maps too") {
    val t = "ice.db.nested"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"CREATE TABLE $t (k INT, st STRUCT<x: INT, y: STRING>, xs ARRAY<STRUCT<p: INT, q: STRING>>, " +
        "m MAP<STRING, STRUCT<v: INT>>) USING iceberg"
    )
    spark.sql(
      s"""INSERT INTO $t SELECT cast(id AS INT), named_struct('x', cast(id % 9 AS INT), 'y', concat('y', id)),
         |  array(named_struct('p', cast(id AS INT), 'q', 'q'), named_struct('p', cast(-id AS INT), 'q', concat('q', id))),
         |  map(concat('k', id % 3), named_struct('v', cast(id AS INT)))
         |FROM range(0, 1500)""".stripMargin
    )
    spark.sql(s"ALTER TABLE $t RENAME COLUMN st.y TO y2")
    spark.sql(s"ALTER TABLE $t ADD COLUMN st.z BIGINT")
    spark.sql(s"ALTER TABLE $t ADD COLUMN xs.element.r DOUBLE")
    spark.sql(s"ALTER TABLE $t RENAME COLUMN xs.element.q TO q2")
    spark.sql(s"ALTER TABLE $t ADD COLUMN m.value.w STRING")
    spark.sql(
      s"""INSERT INTO $t SELECT cast(id AS INT), named_struct('x', 1, 'y2', 'n', 'z', id),
         |  array(named_struct('p', 2, 'q2', 'n', 'r', id / 2.0)), map('n', named_struct('v', 3, 'w', 'w'))
         |FROM range(1500, 2000)""".stripMargin
    )
    check(s"SELECT * FROM $t")
    check(s"SELECT k, st.y2, st.z FROM $t")
    // The new nested fields alone: the old files hold `st` but neither of them.
    check(s"SELECT k, st.z FROM $t")
  }

  test("v3 initial defaults fill the files written before their columns were added") {
    val t = "ice.db.defaults"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(s"CREATE TABLE $t (k INT, s STRING) USING iceberg TBLPROPERTIES ('format-version' = '3')")
    spark.sql(s"INSERT INTO $t SELECT cast(id AS INT), concat('s', id % 5) FROM range(0, 2000)")
    val table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t)
    table.updateSchema()
      .addColumn(
        "n",
        org.apache.iceberg.types.Types.IntegerType.get(),
        "",
        org.apache.iceberg.expressions.Literal.of(7)
      )
      .addColumn(
        "str",
        org.apache.iceberg.types.Types.StringType.get(),
        "",
        org.apache.iceberg.expressions.Literal.of("dflt")
      )
      .commit()
    spark.sql(s"REFRESH TABLE $t")
    spark.sql(s"INSERT INTO $t SELECT cast(id AS INT), 'x', cast(id AS INT), 'y' FROM range(2000, 2500)")
    check(s"SELECT * FROM $t")
  }

  /** Plain Parquet files (no field ids) added to an Iceberg table with `add_files`. */
  private def imported(t: String, mapping: Boolean): Unit = {
    val src = warehouse.resolve(s"plain_${t.replace('.', '_')}").toString
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql("SELECT cast(id AS INT) AS a, concat('s', id) AS s, named_struct('x', cast(id % 4 AS INT)) AS st " +
      "FROM range(0, 1200)").write.mode("overwrite").parquet(src)
    spark.sql(s"CREATE TABLE $t (a INT, s STRING, st STRUCT<x: INT>) USING iceberg")
    spark.sql(s"CALL ice.system.add_files(table => '${t.stripPrefix("ice.")}', source_table => '`parquet`.`$src`')")
    val table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t)
    if (!mapping) table.updateProperties().remove("schema.name-mapping.default").commit()
    else {
      val json = org.apache.iceberg.mapping.NameMappingParser.toJson(
        org.apache.iceberg.mapping.MappingUtil.create(table.schema())
      )
      table.updateProperties().set("schema.name-mapping.default", json).commit()
    }
    spark.sql(s"REFRESH TABLE $t")
  }

  test("files without field ids are read through the table's name mapping") {
    val t = "ice.db.mapped"
    imported(t, mapping = true)
    check(s"SELECT * FROM $t")
    check(s"SELECT st.x, count(*) FROM $t GROUP BY st.x")
  }

  test("files without field ids and no name mapping are read with Iceberg's positional fallback ids") {
    val t = "ice.db.unmapped"
    imported(t, mapping = false)
    // Iceberg numbers only the top-level columns by position, so `st.x` is missing in these files and `st` is a
    // present struct of nulls. The previous resolution hands these files to Spark's reader, which fails on a file
    // without the field ids the read schema expects; Iceberg's reads them.
    check(s"SELECT * FROM $t", previousFails = true)
    check(s"SELECT a, s FROM $t", previousFails = true)
  }

  test("#901 item 4: a nested field added with an initial default returns the default from old files") {
    import org.apache.iceberg.expressions.Literal
    import org.apache.iceberg.types.Types
    val t = "ice.db.nested_defaults"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    // A struct with a field present from the start, so the old files hold the struct (its nulls come from `a`).
    spark.sql(
      s"CREATE TABLE $t (k INT, st STRUCT<a: INT>) USING iceberg TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(s"INSERT INTO $t SELECT cast(id AS INT), named_struct('a', cast(id % 9 AS INT)) FROM range(0, 2000)")
    val table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, t)
    // Add fields inside the struct, each with an initial default -- the nested-default shape item 4 names.
    table.updateSchema()
      .addColumn("st", "ci", Types.IntegerType.get(), "", Literal.of(7))
      .addColumn("st", "cs", Types.StringType.get(), "", Literal.of("it's"))
      .addColumn("st", "cd", Types.DecimalType.of(9, 2), "", Literal.of(new java.math.BigDecimal("12.34")))
      .commit()
    spark.sql(s"REFRESH TABLE $t")
    spark.sql(
      s"""INSERT INTO $t SELECT cast(id AS INT),
         |  named_struct('a', cast(id % 9 AS INT), 'ci', cast(id AS INT), 'cs', 'x', 'cd', CAST(id / 100.0 AS DECIMAL(9,2)))
         |FROM range(2000, 2500)""".stripMargin
    )
    // Native read equals Iceberg's reader, no file to Spark's reader, no group to Iceberg's.
    check(s"SELECT k, st.a, st.ci, st.cs, st.cd FROM $t")
    check(s"SELECT k, st.ci, st.cs FROM $t WHERE k % 7 = 0")
    // The new nested fields alone: the old files hold the struct but none of these three.
    check(s"SELECT k, st.ci FROM $t")

    // An independently computed expected set (not only Iceberg's reader): old rows take the defaults, new ones
    // their inserted values. Iceberg 1.12's own reader agrees here, but the assertion stands on its own.
    val rows = spark.sql(s"SELECT k, st.ci, st.cs, st.cd FROM $t").collect()
      .map(r => (r.getInt(0), r.getInt(1), r.getString(2), r.getDecimal(3))).toSet
    val expected = ((0 until 2000).map(k => (k, 7, "it's", new java.math.BigDecimal("12.34"))) ++
      (2000 until 2500).map { k =>
        (k, k, "x", new java.math.BigDecimal(k).divide(new java.math.BigDecimal(100)).setScale(2))
      }).toSet
    assert(rows == expected, s"nested defaults differ:\n${rows.diff(expected).take(5)}\n${expected.diff(rows).take(5)}")
  }
}
