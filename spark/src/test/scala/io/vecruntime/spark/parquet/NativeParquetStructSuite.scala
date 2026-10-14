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
package io.vecruntime.spark.parquet

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * Struct columns in the native Parquet scan (#568, work item 8). Every query is compared with Spark's
 * row-based parquet-java reader (vectorized reader off, plugin off): the same rows, as text, binary by
 * content. And a query over struct columns only must be read by [[VectorParquetScanExec]] itself, with row
 * groups decoded natively (no per-file fallback).
 *
 * The files come from three writers: Spark's own (v1 and v2 pages, dictionary on and off, every lane type
 * as a struct field, nulls at the struct and at the leaf, a struct of structs); parquet-java's
 * `ExampleParquetWriter` with the group shapes Spark does not write (required groups, required leaves under
 * optional groups, an optional group under a required one); and Hardwood's (hardwood-hq/hardwood)
 * `NestedSchemaTest` struct-of-struct shapes plus the `apache/parquet-testing` deep struct from arrow-rs.
 */
class NativeParquetStructSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024"
    )

  private def show(v: Any): String = v match {
    case null => "null"
    case b: Array[Byte] => b.map("%02x".format(_)).mkString("0x", "", "")
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case d: java.lang.Double => if (d.isNaN) "NaN" else d.toString
    case f: java.lang.Float => if (f.isNaN) "NaN" else f.toString
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  /** Spark's row-based reader: the reference. */
  private def reference(query: String): Seq[String] =
    withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
      withPlugin(enabled = false)(rows(spark.sql(query)))
    }

  /** `query` returns Spark's rows; with `native`, the native scan read it and decoded row groups itself. */
  private def check(query: String, native: Boolean = true): Unit = {
    val expected = reference(query)
    val (actual, scans) = withPlugin(enabled = true) {
      val df = spark.sql(query)
      val r = rows(df)
      (r, PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s })
    }
    assert(actual == expected, s"$query\nexpected ${expected.take(5)}\nactual   ${actual.take(5)}")
    if (native) {
      assert(scans.nonEmpty, s"$query was not read by the native scan")
      assert(scans.map(_.metrics("numRowGroups").value).sum > 0, s"$query: no row group decoded natively")
    }
  }

  private def writeSpark(path: String, v2: Boolean, dictionary: Boolean)(df: => DataFrame): Unit =
    withConf(
      "parquet.writer.version" -> (if (v2) "v2" else "v1"),
      "parquet.enable.dictionary" -> dictionary.toString,
      // Spark writes INT96 timestamps by default, and an INT96 file goes to Spark's reader (its own test below).
      "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
    ) {
      df.repartition(2).write.mode("overwrite").parquet(path)
    }

  /** Every lane type as a struct field, with nulls at each level, and a struct of structs. */
  private def allTypes: DataFrame =
    spark.range(0, 6000).selectExpr(
      "cast(id as int) as id",
      """case when id % 7 = 0 then null else named_struct(
        |  'i', case when id % 5 = 0 then null else cast(id as int) end,
        |  'l', id * 1000003,
        |  'd', case when id % 11 = 0 then null else id / 3.0 end,
        |  'str', case when id % 4 = 0 then null else concat('k', id % 37) end,
        |  'b', case when id % 9 = 0 then null else id % 2 = 0 end,
        |  'dec', cast(id * 7 / 100 as decimal(10,2)),
        |  'wdec', case when id % 6 = 0 then null else cast(id * 123456789012 as decimal(25,3)) end,
        |  'dt', date_add(date'2020-01-01', cast(id % 400 as int)),
        |  'ts', case when id % 13 = 0 then null else timestamp_micros(1600000000000000 + id * 1000) end,
        |  'f', cast(id / 7.0 as float),
        |  'ti', cast(id % 120 as tinyint),
        |  'si', case when id % 3 = 0 then null else cast(id % 30000 as smallint) end,
        |  'bin', case when id % 8 = 0 then null else cast(concat('b', id % 19) as binary) end) end as s""".stripMargin,
      """case when id % 10 = 0 then null else named_struct(
        |  'a', cast(id % 17 as int),
        |  'inner', case when id % 3 = 1 then null else named_struct(
        |     'x', case when id % 4 = 1 then null else concat('x', id % 23) end,
        |     'y', id - 3000) end) end as n""".stripMargin
    )

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"Spark-written structs of every lane type, nulls at every level (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"structs-$v2-$dictionary")
      writeSpark(path, v2, dictionary)(allTypes)
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT s, n FROM t")
      check("SELECT id, s.str, s.dec, s.wdec, n.inner.y FROM t") // nested schema pruning
      check("SELECT n.inner FROM t")
      check("SELECT id, s FROM t WHERE n.inner IS NULL")
      check("SELECT id, n FROM t WHERE s.i > 100 AND s.str LIKE 'k1%'")
      check("SELECT count(*), sum(s.l), max(n.inner.x), min(s.ts), sum(s.dec), count(s.bin) FROM t WHERE s.b")
      check("SELECT s.str, count(*), sum(n.a) FROM t GROUP BY s.str")
      check("SELECT * FROM t DISTRIBUTE BY id % 5")
    }
  }

  test("parquet-java's writer: required and optional groups, required leaves under optional groups, v1 and v2") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      """message m {
        |  required int32 id;
        |  optional group a { required int32 x; optional binary s (STRING); }
        |  required group b { optional int64 y; optional group c { required double z; optional boolean f; } }
        |  optional group d { optional group e { optional int32 v; required binary w (STRING); } }
        |}""".stripMargin
    )
    for (version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0); dict <- Seq(true, false)) {
      val path = newTempPath(s"pj-$version-$dict")
      new java.io.File(path).mkdirs()
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
        .withConf(new org.apache.hadoop.conf.Configuration())
        .withType(schema)
        .withWriterVersion(version)
        .withDictionaryEncoding(dict)
        .withPageSize(1024)
        .withRowGroupSize(32 * 1024L)
        .build()
      val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
      try {
        for (k <- 0 until 5000) {
          val g = factory.newGroup().append("id", k)
          if (k % 6 != 0) {
            val a = g.addGroup("a").append("x", k * 3)
            if (k % 4 != 0) a.append("s", s"s${k % 41}")
          }
          val b = g.addGroup("b")
          if (k % 5 != 0) b.append("y", k.toLong * -7L)
          if (k % 3 != 0) {
            val c = b.addGroup("c").append("z", k / 9.0)
            if (k % 7 != 0) c.append("f", k % 2 == 0)
          }
          if (k % 8 != 1) {
            val d = g.addGroup("d")
            if (k % 3 != 2) {
              val e = d.addGroup("e").append("w", s"w${k % 13}")
              if (k % 10 != 0) e.append("v", k % 101)
            }
          }
          writer.write(g)
        }
      } finally writer.close()
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      val what = s"parquet-java $version dictionary=$dict"
      withClue(what) {
        check("SELECT * FROM t")
        check("SELECT a, b, d FROM t")
        check("SELECT id, a.s, b.c.z, d.e.w FROM t")
        check("SELECT b.c FROM t WHERE b.c.f")
        check("SELECT count(*), count(a), count(b.c), count(d.e), sum(d.e.v), max(a.s) FROM t")
        check("SELECT id FROM t WHERE d IS NOT NULL AND d.e IS NULL")
      }
    }
  }

  test("Hardwood's NestedSchemaTest struct of structs, and the parquet-testing deep struct from arrow-rs") {
    val path = newTempPath("hardwood-structs")
    spark.range(0, 3000).selectExpr(
      "cast(id as int) as id",
      "case when id % 11 = 0 then null else named_struct('name', concat('c', id % 50), 'address', " +
        "case when id % 4 = 0 then null else named_struct('street', concat(id, ' Main St'), 'city', 'Townsville', " +
        "'zip', case when id % 3 = 0 then null else '12345' end) end) end as customer"
    ).write.mode("overwrite").parquet(path)
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
    check("SELECT * FROM t")
    check("SELECT id, customer.address.city, customer.address.zip FROM t")
    check("SELECT customer.name, count(*) FROM t WHERE customer.address.zip IS NULL GROUP BY customer.name")

    // arrow-rs's deep struct (ARROW-11452): structs of structs over unsigned ints, which Spark reads as
    // decimal(20, 0) and no lane holds as stored, so the file goes to Spark's reader: the same rows.
    val url = getClass.getClassLoader.getResource("parquet-testing/data/nested_structs.rust.parquet")
    val rust = new java.io.File(url.toURI).getAbsolutePath
    withPlugin(enabled = false)(spark.read.parquet(rust).createOrReplaceTempView("t"))
    check("SELECT * FROM t", native = false)
    // Without the unsigned field: required groups of required leaves, read natively.
    check("SELECT roll_num.min, roll_num.max, PC_CUR.sum, BIA_3.mean, BIA_3.variance FROM t")
  }

  test("a struct holding an array and a map is read natively, with Spark's rows (#568)") {
    val path = newTempPath("struct-of-list")
    spark.range(0, 500).selectExpr(
      "cast(id as int) as id",
      "named_struct('tags', array(cast(id as int), 2), 'm', map('k', id)) as s",
      "named_struct('a', cast(id as int)) as flat"
    ).write.mode("overwrite").parquet(path)
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
    check("SELECT * FROM t")
    check("SELECT id, s.tags, s.m['k'], flat FROM t")
  }

  test("a struct field the file lacks (schema evolution) falls the file over to Spark's reader, with nulls") {
    val path = newTempPath("struct-evolve")
    spark.range(0, 800).selectExpr("cast(id as int) as id", "named_struct('a', cast(id as int)) as s")
      .write.mode("overwrite").parquet(path)
    withPlugin(enabled = false) {
      spark.read.schema("id int, s struct<a: int, b: string>").parquet(path).createOrReplaceTempView("t")
    }
    check("SELECT * FROM t", native = false)
    check("SELECT s.b, count(*) FROM t GROUP BY s.b", native = false)
  }

  test("an INT96 timestamp inside a struct falls the file over to Spark's reader, with its rebase") {
    val path = newTempPath("struct-int96")
    withConf("spark.sql.parquet.outputTimestampType" -> "INT96") {
      spark.range(0, 600).selectExpr(
        "cast(id as int) as id",
        "named_struct('ts', timestamp_micros(1600000000000000 + id * 1000), 'k', cast(id as int)) as s"
      ).write.mode("overwrite").parquet(path)
    }
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
    check("SELECT * FROM t", native = false)
    check("SELECT id, s.k FROM t") // the timestamp pruned away: no INT96 left to read
  }

  test("struct field names resolve case-insensitively, as Spark resolves them") {
    val path = newTempPath("struct-case")
    spark.range(
      0,
      800
    ).selectExpr("cast(id as int) as id", "named_struct('Alpha', cast(id as int), 'Beta', concat('b', id)) as S")
      .write.mode("overwrite").parquet(path)
    withPlugin(enabled = false) {
      spark.read.schema("id int, s struct<alpha: int, beta: string>").parquet(path).createOrReplaceTempView("t")
    }
    check("SELECT * FROM t")
    check("SELECT s.beta FROM t WHERE s.alpha % 3 = 0")
  }
}
