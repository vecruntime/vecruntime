/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
import io.vecruntime.spark.arrow.{VectorAllocators, VectorDictionaryColumnVector}
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFilterExec, VectorHashAggregateExec, VectorParquetScanExec}

/**
 * #612: the native scan emits a dictionary-encoded string column as dictionary vectors (ids over the row
 * group's dictionary), as Spark's scan path hands them to our operators; a chunk that falls back from its
 * dictionary mid-way is decoded flat from that page on. Results must be Spark's either way.
 */
class DictionaryStringsSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (256 * 1024).toString,
      "parquet.page.size" -> (8 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1000"
    )

  private val node = classOf[VectorParquetScanExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      // Low-cardinality strings with nulls: every page dictionary-encoded.
      spark.sql(
        """SELECT CAST(id AS INT) AS i,
          |  CASE WHEN id % 11 = 0 THEN NULL ELSE CONCAT('cat-', CAST(id % 37 AS STRING)) END AS s,
          |  CONCAT('b', CAST(id % 5 AS STRING)) AS t
          |FROM range(0, 50000)""".stripMargin
      ).repartition(2).write.mode("overwrite").parquet(newTempPath("dict/low"))
      spark.read.parquet(newTempPath("dict/low")).createOrReplaceTempView("dict_low")
      // High-cardinality strings under a tiny dictionary page limit: parquet-java falls back to PLAIN mid-chunk.
      withConf("parquet.dictionary.page.size" -> "2048") {
        spark.sql(
          """SELECT CAST(id AS INT) AS i,
            |  CASE WHEN id % 13 = 0 THEN NULL ELSE CONCAT('key-', CAST(id * 7919 AS STRING)) END AS s
            |FROM range(0, 40000)""".stripMargin
        ).repartition(1).write.mode("overwrite").parquet(newTempPath("dict/fallback"))
      }
      spark.read.parquet(newTempPath("dict/fallback")).createOrReplaceTempView("dict_fallback")
    }
  }

  /** (dictionary-encoded string columns, all string columns) over the batches the native scan of `sql` emits. */
  private def scanColumnKinds(sql: String): (Long, Long) = withPlugin(enabled = true) {
    val df = spark.sql(sql)
    df.collect()
    val scan = PlanUtils.allNodes(finalPlan(df)).collectFirst { case s: VectorParquetScanExec => s }
      .getOrElse(fail("no native scan"))
    val stringOrdinals = scan.output.zipWithIndex.collect {
      case (a, i) if a.dataType == org.apache.spark.sql.types.StringType => i
    }
    scan.executeColumnar().mapPartitions { it =>
      var dict = 0L
      var all = 0L
      it.foreach { b =>
        stringOrdinals.foreach { i =>
          all += 1
          if (b.column(i).isInstanceOf[VectorDictionaryColumnVector]) dict += 1
        }
      }
      Iterator((dict, all))
    }.collect().foldLeft((0L, 0L)) { case ((d, a), (x, y)) => (d + x, a + y) }
  }

  test("dictionary-encoded strings come out of the native scan as dictionary vectors") {
    val (dict, all) = scanColumnKinds("SELECT i, s, t FROM dict_low")
    assert(all > 0 && dict === all, s"$dict of $all string columns were dictionary vectors")
    checkVectorized("SELECT i, s, t FROM dict_low", Seq(node))
  }

  test("filters, aggregates and string functions over dictionary strings return Spark's rows") {
    checkVectorized("SELECT i, s FROM dict_low WHERE s = 'cat-3' OR s IS NULL", Seq(node, classOf[VectorFilterExec]))
    checkVectorized("SELECT s, count(*), sum(i) FROM dict_low GROUP BY s", Seq(node, classOf[VectorHashAggregateExec]))
    checkVectorized("SELECT t, s, count(*) FROM dict_low WHERE i % 3 = 0 GROUP BY t, s", Seq(node))
    checkVectorized("SELECT upper(s), length(s), s LIKE 'cat-1%' FROM dict_low", Seq(node))
    checkVectorized("SELECT count(DISTINCT s) FROM dict_low", Seq(node))
  }

  test("a chunk that falls back from its dictionary mid-way decodes flat from there, with Spark's rows") {
    val (dict, all) = scanColumnKinds("SELECT i, s FROM dict_fallback")
    assert(all > 0, "no string batches")
    assert(dict < all, s"every batch was a dictionary vector ($dict of $all): the file did not fall back")
    checkVectorized("SELECT i, s FROM dict_fallback", Seq(node))
    checkVectorized("SELECT count(*), count(s), max(s), min(s) FROM dict_fallback", Seq(node))
    checkVectorized("SELECT i, s FROM dict_fallback WHERE i % 97 = 1", Seq(node))
  }

  test("with the switch off the column is flat, with the same rows") {
    withConf(VectorConf.ScanNativeParquetDictionaryStrings -> "false") {
      val (dict, all) = scanColumnKinds("SELECT i, s, t FROM dict_low")
      assert(all > 0 && dict === 0L, s"$dict dictionary vectors with the switch off")
      checkVectorized("SELECT s, count(*) FROM dict_low GROUP BY s", Seq(node))
    }
  }

  test("a LIMIT and many row groups leak neither ids nor dictionaries") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    checkVectorized("SELECT s, t FROM dict_low LIMIT 13", Seq(node))
    checkVectorized("SELECT s FROM dict_low WHERE i > 1000 LIMIT 3000", Seq(node))
    checkVectorized("SELECT s, count(*) FROM dict_low GROUP BY s", Seq(node))
    assert(root.getAllocatedMemory === before, s"allocated ${root.getAllocatedMemory} vs $before before")
  }
}
