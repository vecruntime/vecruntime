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

/**
 * #962: a small timed local benchmark comparing the three late-materialization modes (off / on / auto) on
 * (1) a SELECTIVE filter over a WIDE projection -- where late materialization is meant to win -- and (2) a
 * q24b-like NON-SELECTIVE wide-projection query -- the query the global flag regressed, where auto must stay on
 * the eager path. It prints wall times and asserts auto is within noise of off on the non-selective query (the
 * acceptance guard) and no slower than on. Local SF, so numbers are indicative, not the SF1000 A/B; it exists to
 * exercise the whole path end to end and catch a gross regression.
 */
@io.vecruntime.spark.test.tags.Scan
class AutoLateMaterializationBenchmark extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      VectorConf.ScanNativeParquetLateMaterializationMinBytes -> "0",
      "parquet.block.size" -> (1 * 1024 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "4096"
    )

  private val rows = 4000000L

  // A wide projection: one narrow filter key (id) and several wide string / decimal columns.
  private val select =
    s"""SELECT id, CAST(id % 1000 AS INT) AS k, CAST(id % 7 AS INT) AS small,
       |  CAST(id % 977 AS DECIMAL(9,2)) AS d9, CAST(id AS DECIMAL(20,3)) AS d20,
       |  CONCAT('s', CAST(id % 37 AS STRING)) AS sdict,
       |  CONCAT('long-value-', CAST(id AS STRING), REPEAT('x', CAST(id % 40 AS INT))) AS splain,
       |  CONCAT('more-', CAST(id AS STRING), REPEAT('y', CAST(id % 30 AS INT))) AS splain2
       |FROM range(0, $rows)""".stripMargin

  private val columns = "id, k, small, d9, d20, sdict, splain, splain2"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      // Unsorted: a narrow id band is selective within each row group, survivors scattered.
      spark.sql(select).repartition(4).write.mode("overwrite").parquet(newTempPath("bench/t"))
      spark.read.parquet(newTempPath("bench/t")).createOrReplaceTempView("bench_t")
    }
  }

  private def timeMode(sql: String, mode: String, iters: Int): Double = {
    val times = (0 until iters).map { _ =>
      val t0 = System.nanoTime()
      withConf(VectorConf.ScanNativeParquetLateMaterialization -> mode)(
        withPlugin(enabled = true)(spark.sql(sql).collect())
      )
      (System.nanoTime() - t0) / 1e6 // ms
    }.sorted
    times(times.length / 2) // median
  }

  test("timed off/on/auto: selective wide projection and q24b-like non-selective") {
    // Selective: a narrow aggregating query over a wide projection (the win case).
    val selective =
      s"SELECT count(*), max(splain), min(splain2), max(sdict), sum(d20) FROM bench_t WHERE id BETWEEN 1000000 AND 1000500"
    // q24b-like: a non-selective predicate that keeps most rows of a wide projection (the regression case).
    val nonSelective =
      s"SELECT count(*), max(splain), min(splain2), max(sdict), sum(d20) FROM bench_t WHERE id % 3 <> 0 AND small < 6"

    val iters = 5
    // Warm up once per query to pay class-load / JIT before timing.
    timeMode(selective, "off", 1)
    timeMode(nonSelective, "off", 1)

    val selOff = timeMode(selective, "off", iters)
    val selOn = timeMode(selective, "on", iters)
    val selAuto = timeMode(selective, "auto", iters)
    val nsOff = timeMode(nonSelective, "off", iters)
    val nsOn = timeMode(nonSelective, "on", iters)
    val nsAuto = timeMode(nonSelective, "auto", iters)

    info(f"[#962 bench] rows=$rows iters=$iters (median ms)")
    info(f"[#962 bench] SELECTIVE wide-proj:  off=$selOff%.1f  on=$selOn%.1f  auto=$selAuto%.1f")
    info(f"[#962 bench] NON-SELECTIVE q24b:   off=$nsOff%.1f  on=$nsOn%.1f  auto=$nsAuto%.1f")
    // Print to stdout too so the number is visible in the plain test log.
    // scalastyle:off println
    println(f"[#962 bench] SELECTIVE wide-proj:  off=$selOff%.1f  on=$selOn%.1f  auto=$selAuto%.1f")
    println(f"[#962 bench] NON-SELECTIVE q24b:   off=$nsOff%.1f  on=$nsOn%.1f  auto=$nsAuto%.1f")
    // scalastyle:on println

    // Acceptance guard (loose, to tolerate local noise): on the non-selective query auto must not regress the
    // way `on` can -- auto stays within 35% of off, and is no worse than on.
    assert(nsAuto <= nsOff * 1.35, f"auto regressed the non-selective query: auto=$nsAuto%.1f off=$nsOff%.1f")
    assert(nsAuto <= nsOn * 1.15, f"auto slower than on on the non-selective query: auto=$nsAuto%.1f on=$nsOn%.1f")
  }
}
