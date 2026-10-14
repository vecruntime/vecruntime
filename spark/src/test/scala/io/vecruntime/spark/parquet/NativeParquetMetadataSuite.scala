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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFallback, VectorParquetScanExec}

/**
 * The native Parquet scan's metadata columns (#568, work item 9), every query compared with Spark's
 * row-based reader: the file-constant `_metadata` fields (`file_path`, `file_name`, `file_size`,
 * `file_block_start`, `file_block_length`, `file_modification_time`) and the whole `_metadata` struct, over
 * several files, a partitioned table, files split into several ranges, a decode filter, and a file the scan
 * hands to Spark's reader; `input_file_name()` and its block siblings; and `_metadata.row_index`. Files come
 * from Spark's writer and from parquet-java's `ExampleParquetWriter`.
 */
class NativeParquetMetadataSuite extends VectorQuerySuite {

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
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  private def reference(query: String): Seq[String] =
    withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
      withPlugin(enabled = false)(rows(spark.sql(query)))
    }

  /** Spark's rows, read by the native scan (decoding row groups itself unless `native` is false). */
  private def check(query: String, native: Boolean = true): Unit = {
    val expected = reference(query)
    val (actual, scans) = withPlugin(enabled = true) {
      val df = spark.sql(query)
      val r = rows(df)
      (r, PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s })
    }
    assert(actual == expected, s"$query\nexpected ${expected.take(5)}\nactual   ${actual.take(5)}")
    assert(scans.nonEmpty, s"$query was not read by the native scan")
    if (native) assert(scans.map(_.metrics("numRowGroups").value).sum > 0, s"$query: no row group decoded natively")
  }

  private val fields = "_metadata.file_path, _metadata.file_name, _metadata.file_size, " +
    "_metadata.file_block_start, _metadata.file_block_length, _metadata.file_modification_time"

  private lazy val flat: String = {
    val path = newTempPath("meta-flat")
    withPlugin(enabled = false) {
      spark.range(0, 20000).selectExpr("cast(id as int) i", "id % 7 as j", "concat('s', id % 13) s")
        .repartition(4).write.mode("overwrite").parquet(path)
    }
    path
  }

  test("file-constant _metadata fields and the whole struct, over several files") {
    check(s"SELECT i, $fields FROM parquet.`$flat`")
    check(s"SELECT i, s, _metadata FROM parquet.`$flat`")
    check(s"SELECT _metadata.file_name, count(*), sum(j) FROM parquet.`$flat` GROUP BY _metadata.file_name")
  }

  test("_metadata with a filter on a data column, and a filter on a metadata field") {
    check(s"SELECT i, _metadata.file_name, _metadata.file_size FROM parquet.`$flat` WHERE i % 97 = 3")
    check(s"SELECT i, s FROM parquet.`$flat` WHERE _metadata.file_size > 0 AND j = 2")
  }

  test("_metadata over files split into several ranges: each split's block start and length") {
    withConf("spark.sql.files.maxPartitionBytes" -> (16 * 1024).toString) {
      check(s"SELECT i, $fields FROM parquet.`$flat`")
    }
  }

  test("_metadata after partition columns") {
    val path = newTempPath("meta-part")
    withPlugin(enabled = false) {
      spark.range(0, 6000).selectExpr("cast(id as int) i", "id % 3 as p", "id % 2 as q")
        .write.partitionBy("p", "q").mode("overwrite").parquet(path)
    }
    check(s"SELECT i, p, q, $fields FROM parquet.`$path`")
    check(s"SELECT p, _metadata.file_path, count(*) FROM parquet.`$path` WHERE q = 1 GROUP BY p, _metadata.file_path")
  }

  test("_metadata on a file Spark's reader takes (an INT96 timestamp), next to native files") {
    val path = newTempPath("meta-mixed")
    withPlugin(enabled = false) {
      withConf("spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS") {
        spark.range(0, 3000).selectExpr("cast(id as int) i", "timestamp_seconds(id) ts")
          .write.mode("overwrite").parquet(path)
      }
      withConf("spark.sql.parquet.outputTimestampType" -> "INT96") {
        spark.range(3000, 4000).selectExpr("cast(id as int) i", "timestamp_seconds(id) ts")
          .write.mode("append").parquet(path)
      }
    }
    check(s"SELECT i, ts, $fields FROM parquet.`$path`")
    check(s"SELECT i, _metadata FROM parquet.`$path`")
  }

  test("parquet-java files: _metadata fields and the struct") {
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message m { required int32 id; optional binary s (STRING); optional int64 v; }"
    )
    val path = newTempPath("meta-pj")
    new java.io.File(path).mkdirs()
    for ((version, f) <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0).zipWithIndex) {
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(new org.apache.hadoop.fs.Path(s"$path/part-$f.parquet"))
        .withConf(new org.apache.hadoop.conf.Configuration())
        .withType(schema)
        .withWriterVersion(version)
        .withPageSize(1024)
        .withRowGroupSize(16 * 1024L)
        .build()
      val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
      try {
        for (k <- 0 until 4000) {
          val g = factory.newGroup().append("id", k + f * 4000)
          if (k % 5 != 0) g.append("s", s"s${k % 17}")
          if (k % 3 != 0) g.append("v", k.toLong * 11)
          writer.write(g)
        }
      } finally writer.close()
    }
    check(s"SELECT id, s, v, $fields FROM parquet.`$path`")
    check(s"SELECT id, _metadata FROM parquet.`$path` WHERE v IS NULL")
    check(s"SELECT id, $fields FROM parquet.`$path` WHERE v IS NULL")
  }

  test("input_file_name() and its block siblings, without decode-ahead") {
    check(s"SELECT i, input_file_name(), input_file_block_start(), input_file_block_length() FROM parquet.`$flat`")
    withConf("spark.sql.files.maxPartitionBytes" -> (16 * 1024).toString) {
      check(s"SELECT i, input_file_name(), input_file_block_start() FROM parquet.`$flat`")
    }
  }

  test("input_file_name() under decode-ahead keeps Spark's scan, with the reason") {
    withConf(VectorConf.ScanNativeParquetDecodeAhead -> "2") {
      val q = s"SELECT i, input_file_name() FROM parquet.`$flat`"
      val expected = reference(q)
      val df = withPlugin(enabled = true)(spark.sql(q))
      assert(rows(df) == expected)
      val plan = finalPlan(df)
      assert(PlanUtils.allNodes(plan).collect { case s: VectorParquetScanExec => s }.isEmpty)
      val reasons = VectorFallback.reasons(plan).map(_._2).mkString("; ")
      assert(reasons.contains("input_file_name() under decode-ahead"), reasons)
    }
  }

  test("_metadata.row_index: the row group's first row index plus the position, across row groups and splits") {
    check(s"SELECT i, _metadata.row_index FROM parquet.`$flat`")
    check(s"SELECT i, _metadata.row_index, _metadata.file_name FROM parquet.`$flat` WHERE j = 4")
    withConf("spark.sql.files.maxPartitionBytes" -> (16 * 1024).toString) {
      check(s"SELECT i, _metadata.row_index FROM parquet.`$flat`")
    }
    check(s"SELECT _metadata.file_name, max(_metadata.row_index), count(*) FROM parquet.`$flat` GROUP BY 1")
  }
}
