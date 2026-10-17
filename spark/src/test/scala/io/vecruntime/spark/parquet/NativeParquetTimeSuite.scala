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
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.types.TimeType
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * TIME (#787): Spark 4.1's `TimeType` (nanoseconds of the day), stored by Spark as INT64 `TIME(MICROS)`. The native
 * scan decodes it on the INT64 lane, scales micros to nanos as Spark's `LongAsNanosUpdater` does, and emits an Arrow
 * `TimeNanoVector`; no engine lane holds it, so operators carry it as a foreign column. Compared with Spark's reader
 * row for row: flat, inside a list and inside a struct, dictionary and plain, v1 and v2 pages, with nulls.
 */
class NativeParquetTimeSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "spark.sql.timeType.enabled" -> "true",
      "parquet.block.size" -> (128 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024"
    )

  private val node = classOf[VectorParquetScanExec]

  private def nativeRowGroupsOf(sql: String): Long = {
    val df = spark.sql(sql)
    df.collect()
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
      .map(_.metrics("numRowGroups").value).sum
  }

  // Seconds of the day spread over the whole range, with fractional micros; every 7th row null. `make_time` takes
  // the seconds as DECIMAL(16, 6), so micros survive.
  private val timeExpr =
    "CASE WHEN id % 7 = 0 THEN NULL ELSE make_time(CAST(id % 24 AS INT), CAST(id * 7 % 60 AS INT), " +
      "CAST((id * 13 % 60000000) / 1000000.0 AS DECIMAL(16, 6))) END"

  test("TIME decodes natively, flat, in a list and in a struct (#787)") {
    val query =
      s"""SELECT CAST(id AS INT) AS i,
         |  $timeExpr AS t,
         |  CASE WHEN id % 11 = 0 THEN NULL ELSE array($timeExpr, make_time(23, 59, 59.999999), NULL) END AS ts,
         |  named_struct('a', CAST(id % 5 AS INT), 'b', $timeExpr) AS st
         |FROM range(0, 20000)""".stripMargin
    for (
      (version, dict, view) <-
        Seq(("v1", "true", "t_time_v1d"), ("v1", "false", "t_time_v1p"), ("v2", "false", "t_time_v2"))
    ) {
      val path = newTempPath(view)
      withPlugin(enabled = false) {
        spark.sql(query).repartition(2).sortWithinPartitions("i")
          .write.option("parquet.writer.version", version).option("parquet.enable.dictionary", dict)
          .mode("overwrite").parquet(path)
        spark.read.parquet(path).createOrReplaceTempView(view)
      }
      for (batch <- Seq("1024", "100")) {
        withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
          checkVectorized(s"SELECT * FROM $view", Seq(node))
          checkVectorized(s"SELECT i, t FROM $view WHERE i % 4 = 1", Seq(node))
          checkVectorized(s"SELECT i, t, ts, st FROM $view WHERE t > make_time(12, 0, 0)", Seq(node))
          checkVectorized(s"SELECT i % 10, count(t), min(t), max(t) FROM $view GROUP BY i % 10", Seq(node))
        }
      }
      assert(nativeRowGroupsOf(s"SELECT i, t, ts, st FROM $view") > 0, s"$view: not decoded natively")
    }
  }

  test("TIME as a partition column is a constant column (#787)") {
    val path = newTempPath("t_time_part")
    withPlugin(enabled = false) {
      spark.sql(s"SELECT CAST(id AS INT) AS i, make_time(CAST(id % 3 AS INT) + 8, 30, 15.25) AS p FROM range(0, 3000)")
        .write.partitionBy("p").mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_time_part")
    }
    checkVectorized("SELECT i, p FROM t_time_part WHERE i % 3 = 0", Seq(node))
  }

  test("only INT64 TIME(MICROS) matches the TIME lane; other units fall over per file (#787)") {
    def int64(unit: LogicalTypeAnnotation.TimeUnit) =
      Types.required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timeType(false, unit)).named("t")
    assert(VectorParquetScanExec.physicalMatches(TimeType(6), int64(LogicalTypeAnnotation.TimeUnit.MICROS)))
    assert(!VectorParquetScanExec.physicalMatches(TimeType(6), int64(LogicalTypeAnnotation.TimeUnit.NANOS)))
    val millis = Types.required(PrimitiveTypeName.INT32)
      .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("t")
    assert(!VectorParquetScanExec.physicalMatches(TimeType(6), millis))
    assert(!VectorParquetScanExec.physicalMatches(TimeType(6), Types.required(PrimitiveTypeName.INT64).named("t")))
  }
}
