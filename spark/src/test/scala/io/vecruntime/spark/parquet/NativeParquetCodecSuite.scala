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
 * Plan item 14, part A: the compression codecs the native Parquet scan decodes without ever handing a file to
 * Spark's reader. Compression is not a vecruntime concern -- the scan installs {@link ReusingCodecFactory},
 * which decompresses ZSTD and SNAPPY through one reused {@code byte[]} per chunk (#735) and delegates every
 * other codec to parquet-java's own {@code CodecFactory} (aircompressor's pure-Java codecs on the classpath).
 * A codec is therefore never a fall-back reason: {@code hasUnsupportedEncodingInFooter} checks only encodings
 * and physical types, so a file in any codec these libraries decode is read natively.
 *
 * This pins that guarantee for every codec Spark can write on this classpath -- SNAPPY, ZSTD, GZIP, LZ4
 * (Hadoop framing), LZ4_RAW and UNCOMPRESSED -- across the page and encoding shapes the decoder has to resume
 * through: v1 and v2 data pages, dictionary and plain pages, flat columns (one page live at a time) and nested
 * lists, maps and a struct (a repeated leaf queues several pages). Every query is compared row for row with
 * Spark's own row-based parquet-java reader, and the native scan must decode at least one row group -- it must
 * not have fallen back. BROTLI and LZO are deliberately absent: no Brotli or LZO codec is on this classpath
 * (not in aircompressor, parquet-java or Spark), so neither Spark's reader nor ours can read such a file; see
 * docs/native-parquet-reader.md.
 */
@io.vecruntime.spark.test.tags.Scan
class NativeParquetCodecSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      // Several row groups and many pages per file, so dictionary, plain and page-boundary resume are all hit,
      // not one big page that a single decompress would cover.
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "512"
    )

  /** Every codec Spark can write whose decompressor is on this classpath; Brotli/LZO are not (see the javadoc). */
  private val codecs = Seq("snappy", "zstd", "gzip", "lz4", "lz4_raw", "uncompressed")

  private def show(v: Any): String = v match {
    case null => "null"
    case b: Array[Byte] => b.map("%02x".format(_)).mkString("0x", "", "")
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case m: scala.collection.Map[_, _] =>
      m.map { case (k, x) => show(k) + "->" + show(x) }.toSeq.sorted.mkString("{", ",", "}")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case d: java.lang.Double => if (d.isNaN) "NaN" else d.toString
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  /**
   * Runs `query`, asserts it is read by the native scan with no fallback (a row group decoded), and that every
   * row equals Spark's own row-based parquet-java reader.
   */
  private def check(query: String): Unit = {
    val expected = withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
      withPlugin(enabled = false)(rows(spark.sql(query)))
    }
    val (actual, scans) = withPlugin(enabled = true) {
      val df = spark.sql(query)
      val r = rows(df)
      (r, PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s })
    }
    assert(actual == expected, s"$query\nexpected ${expected.take(5)}\nactual   ${actual.take(5)}")
    assert(scans.nonEmpty, s"$query was not read by the native scan (it fell back to Spark's reader)")
    assert(scans.map(_.metrics("numRowGroups").value).sum > 0, s"$query: no row group decoded natively")
  }

  /** Flat lanes with nulls, a string dictionary, decimals, booleans, timestamps, dates, and nested shapes. */
  private def data: DataFrame =
    spark.range(0, 20000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null else id * 1000003 end as l",
      "case when id % 5 = 0 then null else concat('s', cast(id % 41 as string)) end as s",
      "case when id % 3 = 0 then null else id / 7.0d end as d",
      "case when id % 11 = 0 then null else id % 2 = 0 end as b",
      "cast(id * 3 / 100 as decimal(9,2)) as dec",
      "cast(id * 13 as decimal(30,4)) as dec38",
      "timestamp_micros(1600000000000000L + id * 1000003L) as ts",
      "date_add(date'2001-02-03', cast(id % 4000 as int)) as dt",
      "case when id % 9 = 0 then null else transform(sequence(0, cast(id % 4 as int)), " +
        "k -> case when k = 2 then null else cast(id + k as int) end) end as ai",
      "case when id % 13 = 0 then null else map_from_arrays(transform(sequence(0, cast(id % 3 as int)), " +
        "k -> concat('k', k)), transform(sequence(0, cast(id % 3 as int)), k -> id + k)) end as m",
      "named_struct('x', case when id % 4 = 0 then null else cast(id as int) end, " +
        "'y', concat('y', cast(id % 3 as string))) as st"
    )

  for (version <- Seq("v1", "v2"); codec <- codecs; dict <- Seq(true, false)) {
    test(s"$codec ($version pages, dictionary=$dict): read natively, equal to Spark, no fallback") {
      val path = newTempPath(s"codec-$codec-$version-$dict")
      withConf(
        "spark.sql.parquet.compression.codec" -> codec,
        "parquet.writer.version" -> version,
        "parquet.enable.dictionary" -> dict.toString,
        "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
      ) {
        data.coalesce(2).write.mode("overwrite").parquet(path)
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT i, l, s, d, b, dec, dec38, ts, dt FROM t") // flat, one page live at a time
      check("SELECT i, ai, m, st FROM t") // nested: a repeated leaf queues several pages
      check("SELECT s, count(*), sum(l), max(d), count(b), sum(dec) FROM t WHERE i % 3 = 1 GROUP BY s")
      check("SELECT i, l, s, d, b, dec, dec38, ts, dt, ai, m, st FROM t WHERE i BETWEEN 5000 AND 5300")
    }
  }
}
