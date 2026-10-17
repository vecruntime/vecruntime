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
 * #786: data pages read where parquet-java leaves them -- a v1 page or a v2 page's level and value sections in
 * place when they are heap buffers, copied otherwise (a decompressor's stream, a list of buffers). Files written by
 * Spark with v1 and v2 pages under each codec (snappy decompresses to a stream, zstd and gzip to an array,
 * uncompressed pages stay in the chunk's buffer), flat and nested columns with nulls, dictionary on and off.
 * Every query is compared with Spark's row-based parquet-java reader. The `apache/parquet-testing` v2 files are
 * read by ParquetTestingCorpusSuite.
 */
class NativeParquetPageInPlaceSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "777"
    )

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
    assert(scans.nonEmpty, s"$query was not read by the native scan")
    assert(scans.map(_.metrics("numRowGroups").value).sum > 0, s"$query: no row group decoded natively")
  }

  /** Flat columns of several lanes with nulls, a string dictionary, and nested lists, maps and a struct. */
  private def data: DataFrame =
    spark.range(0, 20000).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null else id * 1000003 end as l",
      "case when id % 5 = 0 then null else concat('s', cast(id % 41 as string)) end as s",
      "case when id % 3 = 0 then null else id / 7.0 end as d",
      "case when id % 11 = 0 then null else id % 2 = 0 end as b",
      "cast(id * 3 / 100 as decimal(9,2)) as dec",
      "case when id % 9 = 0 then null else transform(sequence(0, cast(id % 4 as int)), " +
        "k -> case when k = 2 then null else cast(id + k as int) end) end as ai",
      "case when id % 13 = 0 then null else map_from_arrays(transform(sequence(0, cast(id % 3 as int)), " +
        "k -> concat('k', k)), transform(sequence(0, cast(id % 3 as int)), k -> id + k)) end as m",
      "named_struct('x', case when id % 4 = 0 then null else id end, 'y', concat('y', cast(id % 3 as string))) as st"
    )

  for (version <- Seq("v1", "v2"); codec <- Seq("snappy", "zstd", "gzip", "uncompressed"); dict <- Seq(true, false)) {
    test(s"$version pages, $codec, dictionary=$dict: the same rows as Spark's reader") {
      val path = newTempPath(s"inplace-$version-$codec-$dict")
      withConf(
        "spark.sql.parquet.compression.codec" -> codec,
        "parquet.writer.version" -> version,
        "parquet.enable.dictionary" -> dict.toString
      ) {
        data.coalesce(2).write.mode("overwrite").parquet(path)
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT i, l, s, d, b, dec FROM t")
      check("SELECT i, ai, m, st FROM t")
      check("SELECT s, count(*), sum(l), max(d), count(b) FROM t WHERE i % 3 = 1 GROUP BY s")
    }
  }
}
