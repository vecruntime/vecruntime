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
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * The native Parquet scan over files from the Apache Parquet project's own conformance corpus,
 * `apache/parquet-testing` (Apache-2.0, vendored under `src/test/resources/parquet-testing/`; the commit is
 * in `SOURCE_SHA`). These are files other writers produced -- parquet-mr of several ages, parquet-cpp,
 * arrow-rs, Impala -- with the encodings, page versions, codecs and corner cases (empty v2 data pages, a
 * dictionary page at offset 0, pages of nulls, checksums) a reader must accept, plus corrupt ones from
 * `bad_data/` it must refuse.
 *
 * Each good file is read column by column:
 *  - a column the scan supports must be read by [[VectorParquetScanExec]] (its row groups, not a per-file
 *    fallback) and return exactly what Spark's row-based parquet-mr reader returns; the vectorized reader is
 *    not the reference because it rejects some of these encodings (BYTE_STREAM_SPLIT on INT32/INT64);
 *  - any other column must, with the plugin on, return what Spark returns with it off under the same
 *    configuration -- the same rows, or the same refusal.
 * A corrupt file must fail the query when Spark's reader fails it, never return rows.
 */
class ParquetTestingCorpusSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(VectorConf.ScanNativeParquet -> "true")

  private def resource(rel: String): String = {
    val url = getClass.getClassLoader.getResource(s"parquet-testing/$rel")
    assert(url != null, s"missing test resource parquet-testing/$rel")
    new java.io.File(url.toURI).getAbsolutePath
  }

  /**
   * Explicit read schemas, for files whose full schema Spark cannot infer (FLOAT16 is an illegal Parquet type
   * for Spark 4.1): every other column, read through the schema.
   */
  private val readSchemas: Map[String, String] = Map(
    "byte_stream_split_extended.gzip.parquet" ->
      ("float_plain FLOAT, float_byte_stream_split FLOAT, double_plain DOUBLE, double_byte_stream_split DOUBLE, " +
        "int32_plain INT, int32_byte_stream_split INT, int64_plain BIGINT, int64_byte_stream_split BIGINT, " +
        "flba5_plain BINARY, flba5_byte_stream_split BINARY, decimal_plain DECIMAL(7,3), " +
        "decimal_byte_stream_split DECIMAL(7,3)")
  )

  private def read(file: String, path: String): DataFrame = readSchemas.get(file) match {
    case Some(ddl) => spark.read.schema(ddl).parquet(path)
    case None => spark.read.parquet(path)
  }

  /** Binary values compare by content, not by array identity. */
  private def normalize(rows: Array[Row]): Array[Row] = rows.map(r =>
    Row.fromSeq(r.toSeq.map {
      case b: Array[Byte] => b.map("%02x".format(_)).mkString
      case v => v
    })
  )

  private def collectOrError(f: => Array[Row]): Either[Throwable, Array[Row]] =
    try Right(normalize(f))
    catch { case e: Exception => Left(e) }

  /** Every corpus file in `data/` and the columns the native scan must read itself. */
  private val goodFiles: Seq[(String, Set[String])] = Seq(
    // parquet-mr, PLAIN and dictionary, v1 pages, a mix of supported and unsupported types (INT96, float, binary)
    "alltypes_plain.parquet" -> Set("id", "bool_col", "int_col", "bigint_col", "double_col"),
    "alltypes_plain.snappy.parquet" -> Set("id", "bool_col", "int_col", "bigint_col", "double_col"),
    "alltypes_dictionary.parquet" -> Set("id", "bool_col", "int_col", "bigint_col", "double_col"),
    // the v2 encodings, from parquet-mr and arrow
    "delta_binary_packed.parquet" -> Set("bitwidth0", "bitwidth1", "bitwidth32", "bitwidth64", "int_value"),
    "delta_byte_array.parquet" -> Set("c_customer_id", "c_salutation", "c_first_name", "c_last_name"),
    "delta_length_byte_array.parquet" -> Set("FRUIT"),
    "delta_encoding_optional_column.parquet" -> Set("c_customer_sk", "c_current_cdemo_sk", "c_customer_id"),
    "delta_encoding_required_column.parquet" -> Set("c_customer_sk:", "c_current_cdemo_sk:", "c_customer_id:"),
    "byte_stream_split.zstd.parquet" -> Set("f64"),
    "byte_stream_split_extended.gzip.parquet" -> Set(
      "double_plain",
      "double_byte_stream_split",
      "int32_plain",
      "int32_byte_stream_split",
      "int64_plain",
      "int64_byte_stream_split"
    ),
    "rle_boolean_encoding.parquet" -> Set("datatype_boolean"),
    // decimals carried in INT32 / INT64; BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY decimals fall back per file
    "int32_decimal.parquet" -> Set("value"),
    "int64_decimal.parquet" -> Set("value"),
    "byte_array_decimal.parquet" -> Set(),
    "fixed_length_decimal.parquet" -> Set(),
    "fixed_length_decimal_legacy.parquet" -> Set(),
    // page-level corner cases
    "datapage_v2_empty_datapage.snappy.parquet" -> Set(),
    "page_v2_empty_compressed.parquet" -> Set("integer_column"),
    "int32_with_null_pages.parquet" -> Set("int32_field"),
    "dict-page-offset-zero.parquet" -> Set("l_partkey"),
    "nulls.snappy.parquet" -> Set(),
    "single_nan.parquet" -> Set("mycol"),
    "datapage_v1-snappy-compressed-checksum.parquet" -> Set("a", "b"),
    "datapage_v1-uncompressed-checksum.parquet" -> Set("a", "b"),
    "plain-dict-uncompressed-checksum.parquet" -> Set("long_field"),
    "rle-dict-snappy-checksum.parquet" -> Set("long_field"),
    // INT96 keeps the plan-level fallback
    "int96_from_spark.parquet" -> Set()
  )

  for ((file, native) <- goodFiles) {
    test(s"parquet-testing data/$file: every column returns Spark's rows; the supported ones are read natively") {
      val path = resource(s"data/$file")
      val columns = withPlugin(enabled = false)(read(file, path).schema.fields.toSeq)
      assert(columns.nonEmpty, s"$file has no columns")
      val unknown = native -- columns.map(_.name).toSet
      assert(
        unknown.isEmpty,
        s"$file has no columns $unknown; it has ${columns.map(c => s"${c.name}:${c.dataType.simpleString}")}"
      )
      for (c <- columns) {
        val what = s"$file column ${c.name}: ${c.dataType.simpleString}"
        def select(): DataFrame = read(file, path).select(s"`${c.name}`")
        if (native.contains(c.name)) {
          val expected = normalize(withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
            withPlugin(enabled = false)(select().collect())
          })
          val (actual, scans) = withPlugin(enabled = true) {
            val df = select()
            val rows = df.collect()
            (normalize(rows), PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s })
          }
          assertRowsEqual(expected, actual, 0.0, what)
          assert(scans.nonEmpty, s"$what was not read by the native scan")
          val rowGroups = scans.map(_.metrics("numRowGroups").value).sum
          assert(rowGroups > 0 || expected.isEmpty, s"$what: the native scan read no row group (the file fell over)")
        } else {
          val off = collectOrError(withPlugin(enabled = false)(select().collect()))
          val on = collectOrError(withPlugin(enabled = true)(select().collect()))
          (off, on) match {
            case (Right(e), Right(a)) => assertRowsEqual(e, a, 0.0, what)
            case (Left(_), Left(_)) =>
            case (Right(_), Left(err)) => fail(s"$what: Spark reads it, the plugin failed: $err")
            case (Left(err), Right(_)) => fail(s"$what: Spark refuses it ($err), the plugin returned rows")
          }
        }
      }
    }
  }

  // Corrupt files: when Spark's reader refuses one, the native scan must refuse it too, not return rows.
  for (
    file <- Seq("ARROW-RS-GH-6229-DICTHEADER.parquet", "ARROW-RS-GH-6229-LEVELS.parquet", "ARROW-GH-45185.parquet")
  ) {
    test(s"parquet-testing bad_data/$file is refused, as Spark's reader refuses it") {
      val path = resource(s"bad_data/$file")
      val off = collectOrError(withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
        withPlugin(enabled = false)(spark.read.parquet(path).collect())
      })
      val on = collectOrError(withPlugin(enabled = true)(spark.read.parquet(path).collect()))
      (off, on) match {
        case (Left(_), Left(_)) =>
        case (Left(err), Right(rows)) =>
          fail(s"$file: Spark refuses it ($err), the native scan returned ${rows.length} rows")
        case (Right(e), Right(a)) => assertRowsEqual(e, a, 0.0, file)
        case (Right(_), Left(err)) => fail(s"$file: Spark reads it, the native scan failed: $err")
      }
    }
  }
}
