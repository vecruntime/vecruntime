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
 * Column-index page filtering in the native Parquet scan (#568). A selective filter over sorted data lets
 * Parquet drop the pages outside the filter's row ranges; every flat column then emits only the selected rows,
 * though each column's pages start at different rows. Every query is compared with Spark's own scan, and the
 * native scan must output fewer rows than the row groups it reads (pages were dropped):
 *   - every flat type (ints, a sorted key, strings, doubles with nulls, decimals, booleans, timestamps, dates)
 *     and a struct of them, v1 and v2 pages, dictionary on and off;
 *   - a range, several ranges (an OR), a range at a row group's end, and a filter no page matches;
 *   - `_metadata.row_index` of the selected rows;
 *   - Parquet's own switch off, and a file holding a list (whole row groups): Spark's rows either way.
 */
class NativeParquetColumnIndexSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (1024 * 1024).toString,
      "parquet.page.size" -> (2 * 1024).toString,
      "parquet.page.row.count.limit" -> "700",
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024"
    )

  private def show(v: Any): String = v match {
    case null => "null"
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  /** Checks `query` against Spark's scan; returns the native scan's output rows. */
  private def check(query: String): Long = {
    val expected = withPlugin(enabled = false)(rows(spark.sql(query)))
    val (actual, scans) = withPlugin(enabled = true) {
      val df = spark.sql(query)
      val r = rows(df)
      (r, PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s })
    }
    assert(actual == expected, s"$query\nexpected ${expected.take(5)}\nactual   ${actual.take(5)}")
    assert(scans.nonEmpty, s"$query was not read by the native scan")
    scans.map(_.metrics("numOutputRows").value).sum
  }

  private val total = 60000

  private def write(path: String, v2: Boolean, dictionary: Boolean, extra: String = ""): Unit =
    withConf(
      "parquet.writer.version" -> (if (v2) "v2" else "v1"),
      "parquet.enable.dictionary" -> dictionary.toString,
      "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
    ) {
      withPlugin(enabled = false) {
        spark.range(0, total).selectExpr(
          Seq(
            "cast(id as int) as k",
            "concat('value-', id % 977, '-', repeat('x', cast(id % 13 as int))) as s",
            "case when id % 7 = 0 then null else id / 3.0d end as d",
            "cast(id * 13 / 100 as decimal(12,2)) as dec",
            "id % 3 = 0 as b",
            "timestamp_micros(1600000000000000 + id * 1000003) as ts",
            "date_add(date'2000-01-01', cast(id % 5000 as int)) as dt",
            "case when id % 11 = 0 then null else named_struct('x', cast(id as int), 'y', concat('y', id % 5)) end as st"
          ) ++ (if (extra.isEmpty) Nil else Seq(extra)): _*
        ).coalesce(1).write.mode("overwrite").parquet(path)
      }
    }

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"pages outside the filter's row ranges are dropped, every column aligned (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"colindex-$v2-$dictionary")
      write(path, v2, dictionary)
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      val q1 = "SELECT * FROM t WHERE k BETWEEN 21000 AND 21500"
      val one = check(q1)
      val oneWhole = wholeGroups(q1)
      assert(one < oneWhole / 4, s"one range: the scan output $one rows, whole row groups $oneWhole")
      val q2 = "SELECT k, s, d, st FROM t WHERE k < 800 OR k BETWEEN 40000 AND 40100 OR k >= 59900"
      val two = check(q2)
      assert(two < wholeGroups(q2) / 4, s"three ranges: the scan output $two rows")
      check("SELECT count(*), sum(d), max(s), min(ts), max(dt), sum(dec), count(st), sum(st.x) FROM t " +
        "WHERE k > 30000 AND k <= 33000")
      check("SELECT k, b, ts FROM t WHERE k = 12345")
      check("SELECT * FROM t WHERE k > 100000") // no page matches
      check(s"SELECT k, _metadata.row_index FROM parquet.`$path` WHERE k BETWEEN 51000 AND 51200")
    }
  }

  /** The native scan's output rows for `query` with Parquet's column index off (whole row groups). */
  private def wholeGroups(query: String): Long =
    withConf("parquet.filter.columnindex.enabled" -> "false")(check(query))

  test("Parquet's switch off, and a file holding a list, read whole row groups with Spark's rows") {
    val path = newTempPath("colindex-off")
    write(path, v2 = false, dictionary = true)
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
    val q = "SELECT * FROM t WHERE k BETWEEN 21000 AND 21500"
    val whole = wholeGroups(q)
    assert(check(q) < whole / 4, s"the column index on should drop pages: whole row groups are $whole rows")
    val listed = newTempPath("colindex-list")
    write(listed, v2 = false, dictionary = true, extra = "array(cast(id as int), cast(null as int)) as l")
    withPlugin(enabled = false)(spark.read.parquet(listed).createOrReplaceTempView("l"))
    val ql = "SELECT k, l, s FROM l WHERE k BETWEEN 21000 AND 21500"
    val n = check(ql)
    assert(n == wholeGroups(ql), s"a file with a list keeps whole row groups: $n")
    // The list pruned away, the same file's flat columns are page-filtered.
    val qf = "SELECT k, s FROM l WHERE k BETWEEN 21000 AND 21500"
    assert(check(qf) < wholeGroups(qf) / 4)
  }
}
