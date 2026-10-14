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
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * INT96 timestamps and calendar rebase in the native Parquet scan (#568, work item 6), every query compared
 * with Spark's row-based reader and the native cases asserted to decode row groups themselves:
 *   - INT96, Spark's default timestamp layout: v1 and v2 pages, dictionary on and off, nulls, values before
 *     1900 and 1582, filters and aggregates; inside a struct and a list; parquet-java's and parquet-testing's;
 *   - INT96 time-zone conversion (`int96TimestampConversion`) for a file not written by parquet-mr;
 *   - LEGACY files (dates and timestamps written in the hybrid calendar, `INT64` and `INT96`), with values
 *     before and after the switch, and a pushed-down filter on such a file;
 *   - EXCEPTION: a file without Spark's metadata reads natively when no value needs a rebase, and fails with
 *     Spark's own error when one does; CORRECTED reads it as stored.
 */
class NativeParquetRebaseSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024"
    )

  private def show(v: Any): String = v match {
    case null => "null"
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  private def reference(query: String): Seq[String] =
    withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
      withPlugin(enabled = false)(rows(spark.sql(query)))
    }

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

  /** The innermost Spark error condition a query fails with (Spark's scan wraps it in FAILED_READ_FILE), or the rows. */
  private def outcome(query: String, plugin: Boolean): Either[String, Seq[String]] =
    withConf("spark.sql.parquet.enableVectorizedReader" -> (if (plugin) "true" else "false")) {
      withPlugin(enabled = plugin) {
        try Right(rows(spark.sql(query)))
        catch {
          case e: Throwable =>
            Left(Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collect { case s: SparkThrowable =>
              s.getCondition
            }.filter(_ != null).toSeq.lastOption.getOrElse(e.getClass.getName))
        }
      }
    }

  // Timestamps from 1000-01-01 to 2100 and dates from 0800 to 2100, both sides of the 1582 and 1900 switches.
  private val timestamps =
    "case when id % 13 = 0 then null else timestamp_micros(cast(-30610224000000000 + id * 1123456789012 as bigint)) end"
  private val dates = "case when id % 11 = 0 then null else date_add(date'0800-01-01', cast(id * 241 as int)) end"

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"INT96 timestamps, Spark's default layout, read natively (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"int96-$v2-$dictionary")
      withConf(
        "spark.sql.parquet.outputTimestampType" -> "INT96",
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString,
        "spark.sql.parquet.int96RebaseModeInWrite" -> "CORRECTED"
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 3000).selectExpr(
            "id",
            s"$timestamps as ts",
            "case when id % 7 = 0 then null else timestamp_micros(1700000000000000 + id * 1000003) end as recent",
            "named_struct('t', timestamp_micros(1600000000000000 + id), 'n', id) as st",
            "array(timestamp_micros(1500000000000000 + id), cast(null as timestamp)) as lt"
          ).repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, recent FROM t WHERE recent > timestamp'2023-11-14 22:20:00'")
      check("SELECT min(ts), max(ts), count(recent), max(st.t), max(lt[0]) FROM t")
    }
  }

  test("INT96 and LEGACY dates and timestamps deep in a list of structs are rebased by the tree reader") {
    for (
      (name, conf) <- Seq(
        "int96" -> Seq(
          "spark.sql.parquet.outputTimestampType" -> "INT96",
          "spark.sql.parquet.int96RebaseModeInWrite" -> "LEGACY"
        ),
        "legacy" -> Seq(
          "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS",
          "spark.sql.parquet.datetimeRebaseModeInWrite" -> "LEGACY"
        )
      )
    ) {
      val path = newTempPath(s"deep-rebase-$name")
      withConf(conf: _*) {
        withPlugin(enabled = false) {
          spark.range(0, 2000).selectExpr(
            "id",
            s"array(named_struct('d', $dates, 'ts', array($timestamps, cast(null as timestamp)))) as l"
          ).repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT min(l[0].d), max(l[0].ts[0]) FROM t")
    }
  }

  test("INT96 written by parquet-java and by Spark in parquet-testing, read natively") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message m { required int32 id; optional int96 ts; }"
    )
    for (version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0)) {
      val path = newTempPath(s"pj-int96-$version")
      new java.io.File(path).mkdirs()
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
        .withConf(new org.apache.hadoop.conf.Configuration())
        .withType(schema)
        .withWriterVersion(version)
        .withPageSize(1024)
        .withRowGroupSize(16 * 1024L)
        .build()
      val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
      try {
        for (k <- 0 until 3000) {
          val g = factory.newGroup().append("id", k)
          if (k % 9 != 0) {
            // 8 little-endian bytes of nanos in the day, then the little-endian Julian day (2440588 = 1970-01-01).
            val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            buf.putLong((k.toLong * 1234567891L) % 86400000000000L).putInt(2440588 + 18000 + k)
            g.append("ts", org.apache.parquet.io.api.Binary.fromConstantByteArray(buf.array()))
          }
          writer.write(g)
        }
      } finally writer.close()
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, ts FROM t WHERE id % 5 = 1")
    }
    val corpus = getClass.getResource("/parquet-testing/data/int96_from_spark.parquet").getPath
    withPlugin(enabled = false)(spark.read.parquet(corpus).createOrReplaceTempView("t"))
    check("SELECT * FROM t")
  }

  test("INT96 time-zone conversion for a file not written by parquet-mr matches Spark's") {
    val corpus = getClass.getResource("/parquet-testing/data/int96_from_spark.parquet").getPath
    for (tz <- Seq("America/Los_Angeles", "Asia/Kolkata")) {
      withConf("spark.sql.parquet.int96TimestampConversion" -> "true", "spark.sql.session.timeZone" -> tz) {
        withPlugin(enabled = false)(spark.read.parquet(corpus).createOrReplaceTempView("t"))
        check("SELECT * FROM t")
      }
    }
  }

  for (int96 <- Seq(false, true)) {
    test(
      s"LEGACY files: dates and timestamps in the hybrid calendar are rebased as Spark rebases them (int96=$int96)"
    ) {
      val path = newTempPath(s"legacy-$int96")
      withConf(
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "LEGACY",
        "spark.sql.parquet.int96RebaseModeInWrite" -> "LEGACY",
        "spark.sql.parquet.outputTimestampType" -> (if (int96) "INT96" else "TIMESTAMP_MICROS")
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 3000).selectExpr("id", s"$dates as d", s"$timestamps as ts")
            .repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, d FROM t WHERE d < date'1582-10-15'")
      check("SELECT id, d, ts FROM t WHERE d = date'1100-03-29' OR d > date'2000-01-01'")
      check("SELECT min(d), max(d), min(ts), max(ts) FROM t")
    }
  }

  test("EXCEPTION: a file without Spark's metadata reads natively, and an ancient value fails as Spark fails") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message m { required int32 id; optional int32 d (DATE); optional int64 ts (TIMESTAMP(MICROS,true)); }"
    )
    def write(name: String, firstDay: Int): String = {
      val path = newTempPath(name)
      new java.io.File(path).mkdirs()
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
        .withConf(new org.apache.hadoop.conf.Configuration())
        .withType(schema)
        .withWriterVersion(WriterVersion.PARQUET_1_0)
        .build()
      val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
      try {
        for (k <- 0 until 2000) {
          val g = factory.newGroup().append("id", k)
          if (k % 7 != 0) g.append("d", firstDay + k * 13)
          if (k % 5 != 0) g.append("ts", (firstDay + k * 13).toLong * 86400000000L + k)
          writer.write(g)
        }
      } finally writer.close()
      path
    }
    val modern = write("exc-modern", 0) // 1970 onwards: no value needs a rebase
    withPlugin(enabled = false)(spark.read.parquet(modern).createOrReplaceTempView("t"))
    check("SELECT * FROM t")
    val ancient = write("exc-ancient", -200000) // 1422 onwards: below the switch
    withPlugin(enabled = false)(spark.read.parquet(ancient).createOrReplaceTempView("t"))
    withConf("spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION") {
      check(s"SELECT * FROM parquet.`$modern`") // no value needs a rebase: native, no error
      val off = outcome("SELECT * FROM t", plugin = false)
      val on = outcome("SELECT * FROM t", plugin = true)
      assert(off.isLeft, s"Spark's reader should fail an ancient value under EXCEPTION: $off")
      assert(on == off, s"native $on, Spark $off")
    }
    withConf("spark.sql.parquet.datetimeRebaseModeInRead" -> "CORRECTED") {
      check("SELECT * FROM t")
    }
    withConf("spark.sql.parquet.datetimeRebaseModeInRead" -> "LEGACY") {
      check("SELECT * FROM t")
    }
  }

  test("INT96 EXCEPTION: an ancient INT96 value in a file without Spark's metadata fails as Spark fails") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message m { required int32 id; optional int96 ts; }"
    )
    val path = newTempPath("int96-ancient")
    new java.io.File(path).mkdirs()
    val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
      .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
      .withConf(new org.apache.hadoop.conf.Configuration())
      .withType(schema)
      .withWriterVersion(WriterVersion.PARQUET_1_0)
      .build()
    val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
    try {
      for (k <- 0 until 2000) {
        val g = factory.newGroup().append("id", k)
        if (k % 9 != 0) {
          val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
          // Julian day 2000000 is in the year 763: before the switch.
          buf.putLong((k.toLong * 987654321L) % 86400000000000L).putInt(2000000 + k * 97)
          g.append("ts", org.apache.parquet.io.api.Binary.fromConstantByteArray(buf.array()))
        }
        writer.write(g)
      }
    } finally writer.close()
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
    withConf("spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
      val off = outcome("SELECT * FROM t", plugin = false)
      val on = outcome("SELECT * FROM t", plugin = true)
      assert(off.isLeft, s"Spark's reader should fail an ancient INT96 value under EXCEPTION: $off")
      assert(on == off, s"native $on, Spark $off")
    }
    for (mode <- Seq("CORRECTED", "LEGACY")) {
      withConf("spark.sql.parquet.int96RebaseModeInRead" -> mode) {
        check("SELECT * FROM t")
      }
    }
  }
}
