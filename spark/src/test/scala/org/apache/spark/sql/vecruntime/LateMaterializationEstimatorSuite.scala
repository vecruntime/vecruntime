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
package org.apache.spark.sql.vecruntime

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.sources._

/**
 * #962: unit tests for the per-row-group selectivity estimator, run against the real footer of a Parquet file so
 * the statistics, dictionary pages and encoding stats are exactly what the reader sees. Covers: a selective range
 * (estimate low), a non-selective range (estimate high), no filter / unknown predicate (1.0), a range that misses
 * the chunk entirely (0.0), equality on a dictionary-encoded column (fraction of matching ids), a conjunction
 * (product), and a null check. In package `org.apache.spark.sql.vecruntime` to reach the `private[vecruntime]`
 * estimator.
 */
@io.vecruntime.spark.test.tags.Scan
class LateMaterializationEstimatorSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      // One row group so the whole file's statistics describe the chunk we estimate against.
      "parquet.block.size" -> (16 * 1024 * 1024).toString,
      "parquet.enable.dictionary" -> "true"
    )

  /** Open the single Parquet file written under `path` and return (reader, its one BlockMetaData). */
  private def openSingleRowGroup(path: String)
      : (ParquetFileReader, org.apache.parquet.hadoop.metadata.BlockMetaData) = {
    val conf = new Configuration()
    val files = new java.io.File(new java.net.URI(path).getPath).listFiles()
      .filter(f => f.getName.endsWith(".parquet") && !f.getName.startsWith("."))
    assert(files.length == 1, s"expected one parquet file, got ${files.length}")
    val inputFile = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(files(0).toURI), conf)
    val reader = ParquetFileReader.open(inputFile, HadoopReadOptions.builder(conf, inputFile.getPath).build())
    val blocks = reader.getRowGroups
    assert(blocks.size() == 1, s"expected one row group, got ${blocks.size()}")
    (reader, blocks.get(0))
  }

  private def write(sql: String, rel: String): String = {
    val path = newTempPath(rel)
    withPlugin(enabled = false) {
      spark.sql(sql).repartition(1).write.mode("overwrite")
        .option("parquet.enable.dictionary", "true").parquet(path)
    }
    path
  }

  test("range selectivity from statistics: selective low, non-selective high, miss zero, no-op one") {
    val path = write(
      "SELECT CAST(id AS INT) AS id, CAST(id % 7 AS INT) AS k FROM range(0, 100000)",
      "est/range"
    )
    val (reader, block) = openSingleRowGroup(path)
    try {
      def est(fs: Seq[Filter]) =
        LateMaterializationEstimator.estimate(fs, block, reader, 0, dictionaries = true, blooms = false)

      // id in [0, 99999]. A 1000-wide range keeps ~1% -> low.
      val selective = est(Seq(And(GreaterThanOrEqual("id", 5000), LessThanOrEqual("id", 6000))))
      assert(selective < 0.1, s"selective range estimate too high: $selective")

      // Two thirds of the domain -> high.
      val nonSelective = est(Seq(LessThan("id", 66000)))
      assert(nonSelective > 0.5, s"non-selective range estimate too low: $nonSelective")

      // A range entirely above the max -> zero.
      assert(est(Seq(GreaterThan("id", 200000))) == 0.0)

      // No filter -> 1.0; an unknown (string-contains) predicate -> 1.0.
      assert(est(Seq.empty) == 1.0)
      assert(est(Seq(StringContains("k", "x"))) == 1.0)

      // A conjunction multiplies: id in a 1% band AND k < 4 (~4/7 of values) -> no larger than either.
      val conj = est(Seq(GreaterThanOrEqual("id", 5000), LessThanOrEqual("id", 6000), LessThan("k", 4)))
      assert(conj <= selective + 1e-9, s"conjunction $conj not <= single $selective")
    } finally reader.close()
  }

  test("equality on a dictionary-encoded column estimates from the dictionary") {
    // A low-cardinality dictionary column (7 distinct values). Equality on one value keeps ~1/7.
    val path = write(
      "SELECT CAST(id AS INT) AS id, CONCAT('v', CAST(id % 7 AS STRING)) AS d FROM range(0, 50000)",
      "est/dict"
    )
    val (reader, block) = openSingleRowGroup(path)
    try {
      val eq = LateMaterializationEstimator.estimate(
        Seq(EqualTo("d", "v3")),
        block,
        reader,
        0,
        dictionaries = true,
        blooms = false
      )
      assert(eq > 0.0 && eq < 0.3, s"dictionary equality estimate out of range: $eq (expected ~1/7)")
      // IN over two of the seven values keeps ~2/7.
      val in = LateMaterializationEstimator.estimate(
        Seq(In("d", Array[Any]("v1", "v5"))),
        block,
        reader,
        0,
        dictionaries = true,
        blooms = false
      )
      assert(in > eq, s"IN($in) should keep more than a single equality ($eq)")
      // An equality value absent from the dictionary keeps nothing.
      val absent = LateMaterializationEstimator.estimate(
        Seq(EqualTo("d", "nope")),
        block,
        reader,
        0,
        dictionaries = true,
        blooms = false
      )
      assert(absent == 0.0, s"absent dictionary value should estimate 0, got $absent")
    } finally reader.close()
  }

  test("null checks estimate from the null count") {
    val path = write(
      "SELECT CAST(id AS INT) AS id, CASE WHEN id % 4 = 0 THEN NULL ELSE CAST(id AS INT) END AS nv " +
        "FROM range(0, 40000)",
      "est/nulls"
    )
    val (reader, block) = openSingleRowGroup(path)
    try {
      val notNull = LateMaterializationEstimator.estimate(
        Seq(IsNotNull("nv")),
        block,
        reader,
        0,
        dictionaries = false,
        blooms = false
      )
      // ~3/4 non-null.
      assert(math.abs(notNull - 0.75) < 0.05, s"IsNotNull estimate $notNull not ~0.75")
      val isNull = LateMaterializationEstimator.estimate(
        Seq(IsNull("nv")),
        block,
        reader,
        0,
        dictionaries = false,
        blooms = false
      )
      assert(math.abs(isNull - 0.25) < 0.05, s"IsNull estimate $isNull not ~0.25")
    } finally reader.close()
  }
}
