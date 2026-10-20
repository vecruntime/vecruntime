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
package io.vecruntime.spark

import scala.jdk.CollectionConverters._

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFallback, VectorParquetScanExec, VectorWriteFilesExec}

/**
 * #900 phase 0: files written by our columnar writer, read back by our own Parquet scan, against
 * Spark's reader over the same files. `VectorWriteSuite` checks the writer against Spark's writer
 * and reads with Spark; this suite pins the writer and the reader to each other, so moving both
 * into one codec module (#900) cannot change what one of them produces for the other. The write
 * configurations together cover every encoding the writer emits (asserted from the footers):
 * `PLAIN`, `RLE_DICTIONARY`, `DELTA_BINARY_PACKED` and `DELTA_BYTE_ARRAY`, with small pages so
 * a column chunk spans many pages and a dictionary that overflows into its fallback.
 */
@io.vecruntime.spark.test.tags.Write
class VectorWriteReadRoundTripSuite extends VectorQuerySuite {

  @volatile private var lastWritePlan: SparkPlan = _

  private val listener = new QueryExecutionListener {
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
      if (PlanUtils.allNodes(qe.executedPlan).exists(_.nodeName.contains("Write"))) lastWritePlan = qe.executedPlan
    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.listenerManager.register(listener)
  }

  override protected def afterAll(): Unit = {
    spark.listenerManager.unregister(listener)
    super.afterAll()
  }

  private val Rows = 30000

  /**
   * Flat columns of every type the writer takes, with the values that stress each encoding:
   * the integer extremes (overflowing deltas), constant and ramp columns (width-0 and fixed-width
   * miniblocks), low- and high-cardinality strings (dictionary and its fallback), empty and long
   * strings, NaN and signed zero, and null patterns from none to all.
   */
  private def flat: DataFrame =
    spark
      .range(0, Rows, 1, 3)
      .selectExpr(
        "id",
        "cast(id as int) as i",
        "case when id % 5 = 0 then 9223372036854775807L when id % 5 = 1 then -9223372036854775808L " +
          "else id * 7919 end as lx",
        "case when id % 3 = 0 then 2147483647 when id % 3 = 1 then -2147483648 else cast(id as int) end as ix",
        "42L as konst",
        "if(id % 4 = 0, null, id * 3) as ln",
        "if(id % 2 = 0, null, cast(id as int)) as half_null",
        "cast(null as bigint) as all_null",
        "cast(id % 100 as tinyint) as by",
        "cast(id % 30000 as smallint) as sh",
        "case when id % 97 = 1 then cast('NaN' as double) when id % 97 = 2 then -0.0d " +
          "else (cast(id as double) % 1000) / 7 end as d",
        "cast(id % 13 as float) / 4 as f",
        "id % 3 = 0 as b",
        "if(id % 9 = 0, null, id % 5 = 1) as bn",
        "date_add(date '1970-01-01', cast(id % 40000 as int) - 20000) as dt",
        "timestamp_micros(id * 1000003 - 15000000000) as ts",
        "cast(timestamp_micros(id * 1000003 - 15000000000) as timestamp_ntz) as tsn",
        "cast(id as decimal(7,2)) as dec7",
        "cast(id * 1000 as decimal(15,2)) as dec15",
        "cast(id * 1000000 as decimal(30,6)) as dec30",
        "concat('k', id % 20) as s_low",
        "if(id % 10 = 0, null, concat('s', id % 50)) as s_low_null",
        "concat('unique-', id, '-', sha1(cast(id as string))) as s_high",
        "case when id % 7 = 0 then '' when id % 7 = 1 then null else repeat('x', cast(id % 300 as int)) end as s_len",
        "cast(concat('b', id % 31) as binary) as bin"
      )

  /** Struct, array and map columns, empty and null at each level. */
  private def nested: DataFrame =
    spark
      .range(0, Rows, 1, 3)
      .selectExpr(
        "id",
        "if(id % 11 = 0, null, named_struct('a', cast(id as int), 'b', if(id % 3 = 0, null, concat('v', id % 40)))) as st",
        "case when id % 13 = 0 then null when id % 13 = 1 then array() " +
          "else sequence(0L, id % 6) end as arr",
        "if(id % 17 = 0, null, transform(sequence(0, cast(id % 4 as int)), x -> if(x = 2, null, concat('e', x)))) as arr_s",
        "case when id % 19 = 0 then null when id % 19 = 1 then map() " +
          "else map(concat('k', id % 5), id, 'z', if(id % 2 = 0, null, id * 2)) end as m",
        "if(id % 23 = 0, null, array(named_struct('x', id, 'y', array(cast(id % 7 as int))))) as deep"
      )

  /** Write configurations: the writer's defaults, no dictionary, and small pages with a tiny dictionary. */
  private val configs: Seq[(String, Map[String, String])] = Seq(
    "defaults" -> Map.empty,
    "no dictionary" -> Map("parquet.enable.dictionary" -> "false"),
    "small pages" -> Map(
      "parquet.page.size" -> "4096",
      "parquet.page.row.count.limit" -> "700",
      "parquet.dictionary.page.size" -> "2048",
      "parquet.block.size" -> (1 << 20).toString
    )
  )

  /**
   * The rows staged through Spark's writer, so that what our writer consumes is our own columnar
   * scan of them (the generating projections -- `named_struct`, `sha1`, `transform` -- are not all
   * columnar, and the writer takes a columnar child).
   */
  private def staged(name: String, df: => DataFrame): String = {
    val src = newTempPath(s"roundtrip-source/$name")
    withPlugin(enabled = false)(df.write.mode("overwrite").parquet(src))
    src
  }

  private def writeOurs(src: String, path: String, options: Map[String, String]): Unit = {
    lastWritePlan = null
    withConf(VectorConf.WriteColumnar -> "true") {
      spark.read.parquet(src).write.mode("overwrite").options(options).parquet(path)
    }
    org.apache.spark.sql.vecruntime.TestListenerSync.drain(spark)
    assert(lastWritePlan != null, "no write plan captured")
    assert(
      PlanUtils.allNodes(lastWritePlan).exists(_.isInstanceOf[VectorWriteFilesExec]),
      s"our writer did not write; fallbacks: ${PlanUtils.allNodes(lastWritePlan).flatMap(VectorFallback.reason).mkString("; ")}"
    )
  }

  private def encodings(path: String): Set[Encoding] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val p = new Path(path)
    val files = p.getFileSystem(conf).listFiles(p, true)
    Iterator
      .continually(files)
      .takeWhile(_.hasNext)
      .map(_.next())
      .filter(_.getPath.getName.endsWith(".parquet"))
      .flatMap { st =>
        val r = ParquetFileReader.open(HadoopInputFile.fromStatus(st, conf))
        try r.getFooter.getBlocks.asScala.flatMap(_.getColumns.asScala.flatMap(_.getEncodings.asScala)).toSet
        finally r.close()
      }
      .toSet
  }

  /** Reads `path` with our scan (asserting it is the scan) and with Spark's; the rows must match. */
  private def checkReadBack(path: String, context: String): Unit = {
    val expected = withPlugin(enabled = false)(spark.read.parquet(path).collect())
    val df = withPlugin(enabled = true) {
      val d = spark.read.parquet(path)
      d.collect()
      d
    }
    val actual = withPlugin(enabled = true)(df.collect())
    assert(
      nodesOf[VectorParquetScanExec](df).nonEmpty,
      s"$context: our Parquet scan did not read the files; fallbacks: " +
        VectorFallback.reasons(finalPlan(df)).map(_._2).mkString("; ")
    )
    assert(expected.length == Rows, s"$context: Spark read ${expected.length} rows")
    assertRowsEqual(expected, actual, 0.0, context)
  }

  private def roundTrip(name: String, df: => DataFrame): Set[Encoding] = {
    val src = staged(name, df)
    configs.flatMap { case (label, options) =>
      val path = newTempPath(s"roundtrip/$name/${label.replace(' ', '-')}")
      writeOurs(src, path, options)
      checkReadBack(path, s"$name, $label")
      encodings(path)
    }.toSet
  }

  test("flat columns written by our writer read back by our scan as Spark reads them, in every encoding") {
    val seen = roundTrip("flat", flat)
    Seq(Encoding.PLAIN, Encoding.RLE_DICTIONARY, Encoding.DELTA_BINARY_PACKED, Encoding.DELTA_BYTE_ARRAY).foreach {
      e => assert(seen.contains(e), s"no column chunk used $e; seen: ${seen.mkString(", ")}")
    }
  }

  test("nested columns written by our writer read back by our scan as Spark reads them") {
    roundTrip("nested", nested)
  }

  test("a projection and a filter over our files read the same through our scan") {
    val path = newTempPath("roundtrip/projection")
    writeOurs(staged("projection", flat), path, configs(2)._2)
    val q = (s: org.apache.spark.sql.SparkSession) =>
      s.read.parquet(path).where("ix > 0 and s_low_null is not null").select("id", "lx", "s_high", "dec30", "tsn")
    val expected = withPlugin(enabled = false)(q(spark).collect())
    val df = withPlugin(enabled = true) {
      val d = q(spark)
      d.collect()
      d
    }
    assert(nodesOf[VectorParquetScanExec](df).nonEmpty, "our Parquet scan did not read the files")
    assertRowsEqual(expected, withPlugin(enabled = true)(df.collect()), 0.0, "projection and filter")
  }
}
