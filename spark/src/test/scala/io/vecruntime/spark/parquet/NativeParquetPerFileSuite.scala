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
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.io.api.Binary
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * #812: cases the native scan used to hand to Spark's reader, file by file, now decoded natively. Each query
 * is compared with Spark's own reader and must decode its row groups natively:
 *   - unsigned integers (`UINT_8/16/32/64`, read as short, int, long and decimal(20, 0)), flat, in a list and
 *     in a struct, v1 and v2 pages, dictionary on and off, with the extremes;
 *   - INT32 and INT64 columns (annotated decimals and plain integers) read as decimals wider than 18 digits,
 *     at the stored scale and rescaled;
 *   - `BYTE_STREAM_SPLIT` on `FIXED_LEN_BYTE_ARRAY` decimals and binaries, v1 and v2 pages;
 *   - ANSI intervals (year-month and day-time), flat, in a list and in a struct;
 *   - an encrypted file (parquet-java decrypts it), read with and without its keys: Spark's outcome either way.
 */
class NativeParquetPerFileSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "spark.sql.parquet.columnarReaderBatchSize" -> "1000"
    )

  private def show(v: Any): String = v match {
    case null => "null"
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case m: scala.collection.Map[_, _] => m.toSeq.map(show).sorted.mkString("{", ",", "}")
    case (a, b) => s"${show(a)}->${show(b)}"
    case b: Array[Byte] => b.mkString("x", ".", "")
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  /** Spark's rows, or the innermost Spark error condition. */
  private def outcome(f: => DataFrame): Either[String, Seq[String]] =
    try Right(rows(f))
    catch {
      case e: Throwable =>
        Left(
          Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collect { case s: SparkThrowable =>
            s.getCondition
          }.toSeq.lastOption.getOrElse(e.getClass.getName)
        )
    }

  /**
   * `query` returns Spark's rows (from the row-based reader when `rowBased`: Spark's vectorized reader refuses
   * some encodings), and the native scan decodes row groups itself.
   */
  private def check(query: String, rowBased: Boolean = false): Unit = {
    val expected = withConf("spark.sql.parquet.enableVectorizedReader" -> (!rowBased).toString) {
      withPlugin(enabled = false)(outcome(spark.sql(query)))
    }
    var native = 0L
    val actual = withPlugin(enabled = true) {
      outcome {
        val df = spark.sql(query)
        df.collect()
        native = PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec =>
          s.metrics("numRowGroups").value
        }.sum
        df
      }
    }
    assert(actual == expected, s"$query\nexpected ${expected.map(_.take(5))}\nactual   ${actual.map(_.take(5))}")
    assert(expected.isRight, s"$query: Spark's reader failed with ${expected.left.getOrElse("")}")
    assert(native > 0, s"$query: no row group decoded natively")
  }

  private def writeGroups(path: String, schema: String, v2: Boolean, dictionary: Boolean, n: Int)(
      fill: (org.apache.parquet.example.data.Group, Int) => Unit
  ): Unit = {
    val t = org.apache.parquet.schema.MessageTypeParser.parseMessageType(schema)
    new java.io.File(path).mkdirs()
    val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
      .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
      .withConf(new org.apache.hadoop.conf.Configuration())
      .withType(t)
      .withWriterVersion(if (v2) WriterVersion.PARQUET_2_0 else WriterVersion.PARQUET_1_0)
      .withDictionaryEncoding(dictionary)
      .withPageSize(2048)
      .withRowGroupSize(32 * 1024L)
      .build()
    val factory = new SimpleGroupFactory(t)
    try {
      for (k <- 0 until n) {
        val g = factory.newGroup()
        fill(g, k)
        writer.write(g)
      }
    } finally writer.close()
  }

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"unsigned integers decode natively, flat, in a list and in a struct (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"uint-$v2-$dictionary")
      writeGroups(
        path,
        """message t {
          |  required int32 id;
          |  optional int32 u8 (INTEGER(8,false));
          |  optional int32 u16 (INTEGER(16,false));
          |  optional int32 u32 (INTEGER(32,false));
          |  optional int64 u64 (INTEGER(64,false));
          |  optional group l (LIST) { repeated group list { optional int32 element (INTEGER(32,false)); } }
          |  optional group s { optional int64 a (INTEGER(64,false)); optional int32 b (INTEGER(32,false)); }
          |}""".stripMargin,
        v2,
        dictionary,
        5000
      ) { (g, k) =>
        val u32 = Array(0, 1, Int.MaxValue, Int.MinValue, -1, k * 7919)(k % 6) // -1 is 4294967295
        val u64 = Array(0L, 1L, Long.MaxValue, Long.MinValue, -1L, k * 104729L)(k % 6)
        g.append("id", k)
        if (k % 7 != 0) g.append("u8", Array(0, 255, 128, k % 256)(k % 4))
        if (k % 9 != 0) g.append("u16", Array(0, 65535, 32768, k % 65536)(k % 4))
        if (k % 5 != 0) g.append("u32", if (dictionary) u32 % 50 else u32)
        if (k % 11 != 0) g.append("u64", u64)
        if (k % 13 != 0) {
          val l = g.addGroup("l")
          for (j <- 0 until k % 4) {
            val e = l.addGroup("list")
            if (j != 2) e.append("element", u32 + j)
          }
        }
        if (k % 17 != 0) {
          val s = g.addGroup("s")
          if (k % 3 != 0) s.append("a", u64 ^ k)
          s.append("b", -k)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("u"))
      check("SELECT * FROM u")
      check("SELECT id, u32, u64 FROM u WHERE u32 > 2147483647 OR u64 > 9223372036854775807")
      check("SELECT sum(u8), sum(u16), sum(u32), sum(u64), max(u64), min(u32), sum(s.a), sum(s.b), " +
        "sum(aggregate(l, 0L, (x, y) -> x + coalesce(y, 0)))  FROM u")
    }
  }

  test("INT32 and INT64 read as decimals wider than 18 digits decode natively, rescaled") {
    for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
      val path = newTempPath(s"widedec-$v2-$dictionary")
      writeGroups(
        path,
        """message t {
          |  required int32 id;
          |  optional int32 d9 (DECIMAL(9,2));
          |  optional int64 d18 (DECIMAL(18,3));
          |  optional int32 i;
          |  optional int64 j;
          |  optional group l (LIST) { repeated group list { optional int64 element (DECIMAL(18,3)); } }
          |}""".stripMargin,
        v2,
        dictionary,
        4000
      ) { (g, k) =>
        g.append("id", k)
        if (k % 7 != 0) g.append("d9", Array(999999999, -999999999, 0, k * 12345)(k % 4))
        if (k % 5 != 0)
          g.append("d18", Array(999999999999999999L, -999999999999999999L, 0L, k * 1234567891L)(k % 4))
        if (k % 3 != 0) g.append("i", Array(Int.MaxValue, Int.MinValue, 0, k)(k % 4))
        if (k % 4 != 0) g.append("j", Array(Long.MaxValue, Long.MinValue, 0L, -k.toLong)(k % 4))
        if (k % 6 != 0) {
          val l = g.addGroup("l")
          for (q <- 0 until k % 3) l.addGroup("list").append("element", k * 1000L + q)
        }
      }
      for (
        schema <- Seq(
          "id int, d9 decimal(25,2), d18 decimal(30,3), i decimal(20,0), j decimal(25,0), l array<decimal(19,3)>",
          "id int, d9 decimal(30,7), d18 decimal(38,10), i decimal(25,5), j decimal(38,18), l array<decimal(30,8)>"
        )
      ) {
        withPlugin(enabled = false)(spark.read.schema(schema).parquet(path).createOrReplaceTempView("w"))
        check("SELECT * FROM w")
        check("SELECT max(d9), min(d18), max(i), min(j), count(l) FROM w WHERE id % 3 = 1")
      }
    }
  }

  test("BYTE_STREAM_SPLIT on FIXED_LEN_BYTE_ARRAY decimals and binaries decodes natively") {
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      """message t {
        |  optional fixed_len_byte_array(16) d30 (DECIMAL(30,4));
        |  optional fixed_len_byte_array(8) d18 (DECIMAL(18,2));
        |  optional fixed_len_byte_array(5) b;
        |}""".stripMargin
    )
    val dir = newTempPath("bss-flba")
    new java.io.File(dir).mkdirs()
    def fixed(v: java.math.BigInteger, width: Int): Binary = {
      val raw = v.toByteArray
      val out = Array.fill[Byte](width)(if (v.signum < 0) -1 else 0)
      System.arraycopy(raw, 0, out, width - raw.length, raw.length)
      Binary.fromConstantByteArray(out)
    }
    for ((v2, f) <- Seq(false -> "v1.parquet", true -> "v2.parquet")) {
      val factory = new SimpleGroupFactory(schema)
      val rnd = new scala.util.Random(if (v2) 7 else 3)
      val rows = Iterator.tabulate(9000) { i =>
        val g = factory.newGroup()
        if (i % 11 != 0) {
          val big = new java.math.BigInteger(96, rnd.self)
          g.append("d30", fixed(if (i % 2 == 0) big.negate() else big, 16))
        }
        if (i % 7 != 0) g.append("d18", fixed(java.math.BigInteger.valueOf(rnd.nextLong() % 999999999999999999L), 8))
        if (i % 5 != 0) g.append("b", Binary.fromConstantByteArray(Array.tabulate[Byte](5)(j => (i * 31 + j).toByte)))
        g
      }
      org.apache.parquet.hadoop.V2EncodingWriter.write(new java.io.File(dir, f).getAbsolutePath, schema, rows, v2)
    }
    val encodings = new java.io.File(dir).listFiles((_, nm) => nm.endsWith(".parquet")).flatMap { f =>
      val in = org.apache.parquet.hadoop.util.HadoopInputFile
        .fromPath(new org.apache.hadoop.fs.Path(f.getAbsolutePath), new org.apache.hadoop.conf.Configuration())
      val r = org.apache.parquet.hadoop.ParquetFileReader.open(in)
      try {
        val cols = new scala.collection.mutable.ArrayBuffer[(String, String)]()
        r.getFooter.getBlocks.forEach(b =>
          b.getColumns.forEach(c => c.getEncodings.forEach(e => cols += ((c.getPath.toDotString, e.name()))))
        )
        cols
      } finally r.close()
    }.toSet
    for (c <- Seq("d30", "d18", "b")) assert(encodings.contains((c, "BYTE_STREAM_SPLIT")), s"$c: $encodings")
    withPlugin(enabled = false)(spark.read.parquet(dir).createOrReplaceTempView("bss"))
    for (batch <- Seq("1000", "77")) {
      withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
        check("SELECT * FROM bss", rowBased = true)
        check("SELECT sum(d30), sum(d18), count(b), max(hex(b)) FROM bss WHERE d18 > 0", rowBased = true)
      }
    }
  }

  test("an encrypted file returns Spark's outcome, with and without its keys (parquet-java decrypts)") {
    val path = newTempPath("encrypted")
    val keys = Map(
      "parquet.crypto.factory.class" -> "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory",
      "parquet.encryption.kms.client.class" -> classOf[TestOnlyKmsClient].getName
    )
    val write = keys ++ Map(
      "parquet.encryption.footer.key" -> "kf",
      "parquet.encryption.column.keys" -> "kc:s,d"
    )
    withPlugin(enabled = false) {
      spark.range(0, 3000).selectExpr("id", "concat('s', id % 17) as s", "id * 0.5d as d")
        .coalesce(1).write.options(write).mode("overwrite").parquet(path)
    }
    def q(opts: Map[String, String]): DataFrame =
      spark.read.options(opts).parquet(path).where("id % 3 = 0").selectExpr("id", "s", "d")
    for (opts <- Seq(keys, Map.empty[String, String])) {
      val expected = withPlugin(enabled = false)(outcome(q(opts)))
      val actual = withPlugin(enabled = true)(outcome(q(opts)))
      assert(
        actual == expected,
        s"with ${opts.keySet}: expected ${expected.map(_.take(3))}, actual ${actual.map(_.take(3))}"
      )
    }
    assert(withPlugin(enabled = false)(outcome(q(keys))).isRight, "the test file should read with its keys")
    val native = withPlugin(enabled = true) {
      val df = q(keys)
      df.collect()
      PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s.metrics("numRowGroups").value }.sum
    }
    assert(native > 0, "parquet-java decrypts the pages the native scan decodes")
  }

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"ANSI intervals decode natively, flat, in a list and in a struct (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"intervals-$v2-$dictionary")
      withConf(
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 6000).selectExpr(
            "id",
            "case when id % 7 = 0 then null else make_ym_interval(cast(id % 50 as int), cast(id % 12 as int)) end as ym",
            "case when id % 5 = 0 then null else make_dt_interval(cast(id % 9 as int), cast(id % 24 as int), " +
              "cast(id % 60 as int), cast(id * 1.000001 % 60 as decimal(18,6))) end as dt",
            "case when id % 11 = 0 then null else array(make_ym_interval(1, cast(id % 12 as int)), " +
              "cast(null as interval year to month)) end as lym",
            "case when id % 13 = 0 then null else named_struct('d', make_dt_interval(0, 1, cast(id % 60 as int), 0), " +
              "'m', interval '3' month) end as st",
            "cast(id % 100 as interval day) as dd",
            "cast(id % 30 as interval minute to second) as ms"
          ).coalesce(1).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("iv"))
      check("SELECT * FROM iv")
      check("SELECT id, ym, dt FROM iv WHERE ym > interval '20' year AND dt IS NOT NULL")
      check("SELECT sum(ym), sum(dt), max(dd), min(ms), count(lym), sum(st.d) FROM iv")
    }
  }
}
