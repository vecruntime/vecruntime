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
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * VARIANT (#787, #750): an unshredded variant group (`value` and `metadata` binaries) is read natively as the struct
 * of its two binaries, whose struct column Spark reads as the variant (`ColumnVector.getVariant` takes child 0 as the
 * value and child 1 as the metadata). A file with a shredded variant goes to Spark's reader. Compared with Spark's
 * reader row for row: flat, inside a list and inside a struct, dictionary and plain, v1 and v2 pages, with nulls.
 */
@io.vecruntime.spark.test.tags.Scan
@io.vecruntime.spark.test.tags.Nested
class NativeParquetVariantSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "spark.sql.variant.writeShredding.enabled" -> "false",
      // Extraction pushed into the scan reads a struct of paths, not the variant: covered on its own below.
      "spark.sql.variant.pushVariantIntoScan" -> "false",
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

  // Objects, arrays, scalars of every kind, a JSON null and a SQL null; a few repeated documents so the dictionary
  // has something to do.
  private val variantExpr =
    """CASE WHEN id % 9 = 0 THEN NULL
      |     WHEN id % 9 = 1 THEN parse_json('null')
      |     WHEN id % 9 = 2 THEN parse_json(CAST(id AS STRING))
      |     WHEN id % 9 = 3 THEN parse_json(CONCAT('"s', CAST(id % 13 AS STRING), '"'))
      |     WHEN id % 9 = 4 THEN parse_json(CONCAT('[1, 2.5, "x", ', CAST(id AS STRING), ', null, true]'))
      |     WHEN id % 9 = 5 THEN parse_json('{"a": 1, "b": {"c": [1, 2, 3]}}')
      |     ELSE parse_json(CONCAT('{"a": ', CAST(id AS STRING), ', "k', CAST(id % 7 AS STRING),
      |       '": "v", "d": ', CAST(id * 0.125 AS STRING), '}')) END""".stripMargin

  private def writeVariantTable(view: String, version: String, dict: String): Unit = {
    val path = newTempPath(view)
    withPlugin(enabled = false) {
      spark.sql(
        s"""SELECT CAST(id AS INT) AS i,
           |  $variantExpr AS v,
           |  CASE WHEN id % 11 = 0 THEN NULL ELSE array($variantExpr, parse_json('[]'), NULL) END AS vs,
           |  named_struct('n', CAST(id % 5 AS INT), 'w', $variantExpr) AS st
           |FROM range(0, 12000)""".stripMargin
      ).repartition(2).sortWithinPartitions("i")
        .write.option("parquet.writer.version", version).option("parquet.enable.dictionary", dict)
        .mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView(view)
    }
  }

  test("an unshredded VARIANT decodes natively, flat, in a list and in a struct (#787)") {
    for (
      (version, dict, view) <-
        Seq(("v1", "true", "t_var_v1d"), ("v1", "false", "t_var_v1p"), ("v2", "false", "t_var_v2"))
    ) {
      writeVariantTable(view, version, dict)
      for (batch <- Seq("1024", "100")) {
        withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
          checkVectorized(s"SELECT i, to_json(v), to_json(vs), to_json(st.w), st.n FROM $view", Seq(node))
          checkVectorized(
            s"SELECT i, variant_get(v, '$$.a', 'int'), schema_of_variant(v), is_variant_null(v) FROM $view " +
              "WHERE i % 4 = 1",
            Seq(node)
          )
          checkVectorized(s"SELECT i % 10, count(v), count(vs), count(st.w) FROM $view GROUP BY i % 10", Seq(node))
        }
      }
      assert(nativeRowGroupsOf(s"SELECT i, v, vs, st FROM $view") > 0, s"$view: not decoded natively")
    }
  }

  test("VARIANT values reach rows as Spark's own (#787)") {
    writeVariantTable("t_var_rows", "v1", "true")
    val expected = withPlugin(enabled = false)(spark.sql("SELECT i, v, vs, st FROM t_var_rows").collect())
    val actual = withPlugin(enabled = true)(spark.sql("SELECT i, v, vs, st FROM t_var_rows").collect())
    assert(expected.map(_.toString).sorted.toSeq == actual.map(_.toString).sorted.toSeq)
  }

  // #812: a shredded variant is read natively too: the file group decoded as its own struct (typed columns and
  // residual binary), each row rebuilt by Spark's SparkShreddingUtils.
  for ((version, dict) <- Seq("v1" -> "true", "v1" -> "false", "v2" -> "true")) {
    test(s"a shredded VARIANT decodes natively, Spark's values (version=$version, dictionary=$dict) (#812)") {
      val path = newTempPath(s"t_var_shredded_${version}_$dict")
      withPlugin(enabled = false) {
        withConf(
          "spark.sql.variant.writeShredding.enabled" -> "true",
          "spark.sql.variant.forceShreddingSchemaForTest" -> "a int, d decimal(18, 3), b struct<c: array<int>>"
        ) {
          spark.sql(s"SELECT CAST(id AS INT) AS i, $variantExpr AS v FROM range(0, 6000)")
            .coalesce(1).write.option("parquet.writer.version", version).option("parquet.enable.dictionary", dict)
            .mode("overwrite").parquet(path)
        }
        spark.read.parquet(path).createOrReplaceTempView("t_var_shredded")
      }
      checkVectorized("SELECT i, v FROM t_var_shredded", Seq(node))
      checkVectorized(
        "SELECT i, to_json(v), variant_get(v, '$.a', 'int'), variant_get(v, '$.b.c[1]', 'int'), " +
          "schema_of_variant(v), is_variant_null(v) FROM t_var_shredded",
        Seq(node)
      )
      checkVectorized(
        "SELECT count(*), sum(variant_get(v, '$.a', 'int')) FROM t_var_shredded WHERE i % 3 = 1",
        Seq(node)
      )
      assert(nativeRowGroupsOf("SELECT i, v FROM t_var_shredded") > 0, "the shredded file was not decoded natively")
    }
  }

  test("variant extraction pushed into the scan decodes natively, shredded or not (#812)") {
    writeVariantTable("t_var_push", "v1", "true")
    val shredded = newTempPath("t_var_push_shredded")
    withPlugin(enabled = false) {
      withConf(
        "spark.sql.variant.writeShredding.enabled" -> "true",
        "spark.sql.variant.forceShreddingSchemaForTest" -> "a int, d decimal(18, 3)"
      ) {
        spark.sql(s"SELECT CAST(id AS INT) AS i, $variantExpr AS v FROM range(0, 6000)")
          .coalesce(1).write.mode("overwrite").parquet(shredded)
      }
      spark.read.parquet(shredded).createOrReplaceTempView("t_var_push_shredded")
    }
    withConf("spark.sql.variant.pushVariantIntoScan" -> "true") {
      for (view <- Seq("t_var_push", "t_var_push_shredded")) {
        val sql = s"SELECT i, variant_get(v, '$$.a', 'int'), variant_get(v, '$$.d', 'string'), " +
          s"try_variant_get(v, '$$.a', 'date'), variant_get(v, '$$.b', 'variant') FROM $view"
        val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
        val actual = withPlugin(enabled = true)(spark.sql(sql).collect())
        assert(expected.map(_.toString).sorted.toSeq == actual.map(_.toString).sorted.toSeq, view)
        assert(withPlugin(enabled = true)(nativeRowGroupsOf(sql)) > 0, s"$view: the pushed struct was not native")
      }
    }
  }
}
