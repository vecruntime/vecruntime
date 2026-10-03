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

import scala.jdk.CollectionConverters._

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.arrow.VectorAllocators
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorBroadcastBatches, VectorParquetScanExec}

/**
 * Compares our native Parquet scan ([[VectorParquetScanExec]]) with Spark's own reader row for row, and
 * pins the recorded fallback reasons. The plugin-off run uses Spark's reader (the flag does nothing without
 * the plugin), the plugin-on run uses our node -- so `checkVectorized` asserts identical results AND that
 * the node is in the plan. Files are written with small block/page sizes so the 4 flag-on tables span
 * several row groups and pages; each supported type is covered, with nulls and dictionary vs plain columns,
 * plus partition columns, a pushed filter that prunes row groups, and a dynamic-partition-pruning query.
 */
class VectorParquetScanSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      // Small blocks/pages so a table spans several row groups and pages.
      "parquet.block.size" -> (128 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024",
      // A local FileSystem that counts open() and getFileStatus() per path, under scheme `countfs`, for the
      // I/O-shape test below (one open, one getFileStatus per split).
      "spark.hadoop.fs.countfs.impl" -> classOf[CountingLocalFileSystem].getName,
      "spark.hadoop.fs.countfs.impl.disable.cache" -> "true"
    )

  private val node = classOf[VectorParquetScanExec]

  private def writeTable(name: String, sql: String): Unit = {
    val path = newTempPath(name)
    withPlugin(enabled = false) {
      spark.sql(sql).write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView(name)
    }
  }

  test("all supported types, dictionary and plain, with nulls, several row groups") {
    // i32 (dict), i64 (plain, high card), f64 (plain), dec7 (int32-physical), dec15 (int64-physical),
    // dt (date), s (dict string), each ~15% null; 20000 rows over small blocks => many row groups.
    writeTable(
      "t_types",
      """SELECT
        |  CAST(id AS INT) AS i32,
        |  CAST(id * 2654435761 AS BIGINT) AS i64,
        |  CAST(id * 1.5 AS DOUBLE) AS f64,
        |  CAST((id % 100000) / 100.0 AS DECIMAL(7,2)) AS dec7,
        |  CAST(id AS DECIMAL(15,2)) AS dec15,
        |  DATE_ADD(DATE'2000-01-01', CAST(id % 3000 AS INT)) AS dt,
        |  CASE WHEN id % 7 = 0 THEN NULL ELSE CONCAT('v', CAST(id % 50 AS STRING)) END AS s
        |FROM range(0, 20000)""".stripMargin
    )
    // Introduce nulls in the numeric columns via a second view (range() has no nulls).
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CASE WHEN i32 % 11 = 0 THEN NULL ELSE i32 END AS i32,
          |  CASE WHEN i32 % 13 = 0 THEN NULL ELSE i64 END AS i64,
          |  CASE WHEN i32 % 5 = 0 THEN NULL ELSE f64 END AS f64,
          |  CASE WHEN i32 % 9 = 0 THEN NULL ELSE dec7 END AS dec7,
          |  CASE WHEN i32 % 17 = 0 THEN NULL ELSE dec15 END AS dec15,
          |  dt, s
          |FROM t_types""".stripMargin
      ).write.mode("overwrite").parquet(newTempPath("t_types_nulls"))
      spark.read.parquet(newTempPath("t_types_nulls")).createOrReplaceTempView("t_types_nulls")
    }
    checkVectorized("SELECT * FROM t_types_nulls", Seq(node))
    checkVectorized("SELECT i32, s FROM t_types_nulls", Seq(node)) // projection prunes columns
    checkVectorized("SELECT dec7, dec15, dt FROM t_types_nulls", Seq(node))
  }

  test("page v2 (writer version PARQUET_2_0)") {
    val path = newTempPath("t_v2")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CONCAT('s', CAST(id % 40 AS STRING)) AS s FROM range(0, 8000)")
        .write.option("parquet.writer.version", "v2").mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_v2")
    }
    checkVectorized("SELECT * FROM t_v2", Seq(node))
  }

  test("DELTA_BINARY_PACKED (page v2, dictionary off) is decoded natively, not by Spark's reader (#559)") {
    // parquet.writer.version=v2 with the dictionary off writes INT32 / INT64 columns (ints, bigints, dates,
    // int-physical decimals) as DELTA_BINARY_PACKED. The footer check must admit those files, so the node
    // counts their row groups (a file that falls over to Spark's reader counts none), and the values must
    // match Spark's reader, including nulls, wrapping deltas at the type extremes, and small batches.
    val path = newTempPath("t_delta")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CASE WHEN id % 11 = 0 THEN NULL
          |       WHEN id % 5 = 0 THEN CAST(2147483647 AS INT) WHEN id % 7 = 0 THEN CAST(-2147483648 AS INT)
          |       ELSE CAST(id * 37 AS INT) END AS i32,
          |  CASE WHEN id % 13 = 0 THEN NULL
          |       WHEN id % 6 = 0 THEN 9223372036854775807L WHEN id % 9 = 0 THEN -9223372036854775808L
          |       ELSE CAST(id * 2654435761 AS BIGINT) END AS i64,
          |  CAST(id AS BIGINT) AS seq,
          |  DATE_ADD(DATE'2000-01-01', CAST(id % 3000 AS INT)) AS dt,
          |  CASE WHEN id % 4 = 0 THEN NULL ELSE CAST((id % 100000) / 100.0 AS DECIMAL(7,2)) END AS dec7,
          |  CAST(id AS DECIMAL(15,2)) AS dec15,
          |  CONCAT('s', CAST(id % 40 AS STRING)) AS s
          |FROM range(0, 30000)""".stripMargin
      ).repartition(3)
        .write
        .option("parquet.writer.version", "v2")
        .option("parquet.enable.dictionary", "false")
        .mode("overwrite")
        .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_delta")
    }
    // The files really carry DELTA_BINARY_PACKED for the integer columns.
    val files = new java.io.File(path).listFiles((_, nm) => nm.endsWith(".parquet"))
    assert(files.nonEmpty)
    val encodings = files.flatMap { f =>
      val in = org.apache.parquet.hadoop.util.HadoopInputFile
        .fromPath(new org.apache.hadoop.fs.Path(f.getAbsolutePath), new org.apache.hadoop.conf.Configuration())
      val r = org.apache.parquet.hadoop.ParquetFileReader.open(in)
      try {
        val cols = new scala.collection.mutable.ArrayBuffer[(String, String)]()
        r.getFooter.getBlocks.forEach(b =>
          b.getColumns.forEach(c => c.getEncodings.forEach(e => cols += ((c.getPath.toDotString, e.name()))))
        )
        cols
      } finally r.close()
    }.toSet
    for (c <- Seq("i32", "i64", "seq", "dt", "dec7", "dec15")) {
      assert(encodings.contains((c, "DELTA_BINARY_PACKED")), s"$c is not DELTA_BINARY_PACKED: $encodings")
    }
    for (batch <- Seq("1024", "100")) {
      withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
        checkVectorized("SELECT * FROM t_delta", Seq(node))
        checkVectorized("SELECT i32, dt, dec7 FROM t_delta WHERE i64 IS NOT NULL", Seq(node))
        withPlugin(enabled = true) {
          val df = spark.sql("SELECT i32, i64, seq, dt, dec7, dec15 FROM t_delta")
          df.collect()
          val scans = PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
          assert(scans.nonEmpty)
          val rowGroups = scans.map(_.metrics("numRowGroups").value).sum
          assert(rowGroups > 0, "no row group read natively: the DELTA_BINARY_PACKED files fell over to Spark's reader")
        }
      }
    }
  }

  test("DELTA_LENGTH_BYTE_ARRAY strings and BYTE_STREAM_SPLIT numbers are decoded natively (#559)") {
    // parquet-java does not choose these encodings on its own, so the files come from a test writer with a
    // per-column ValuesWriterFactory: DELTA_LENGTH_BYTE_ARRAY for the string, BYTE_STREAM_SPLIT for the
    // INT32 / INT64 / DOUBLE columns (int, date, bigint, decimal(15,2), double). One file with v1 pages and
    // one with v2, nulls in every column, the type extremes, NaN / -0.0 / infinities, empty and multi-byte
    // strings. The node must count their row groups (no per-file fallback) and match Spark's reader.
    import org.apache.parquet.example.data.simple.SimpleGroupFactory
    import org.apache.parquet.io.api.Binary
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      """message t {
        |  optional int32 i32;
        |  optional int32 dt (DATE);
        |  optional int64 i64;
        |  optional int64 dec (DECIMAL(15,2));
        |  optional double d;
        |  optional binary s (STRING);
        |}""".stripMargin
    )
    val dir = newTempPath("t_dlba_bss")
    new java.io.File(dir).mkdirs()
    val specials =
      Array(Double.NaN, -0.0, 0.0, Double.PositiveInfinity, Double.NegativeInfinity, Double.MinPositiveValue)
    for ((v2, f) <- Seq(false -> "v1.parquet", true -> "v2.parquet")) {
      val factory = new SimpleGroupFactory(schema)
      val rnd = new scala.util.Random(if (v2) 2 else 1)
      val rows = Iterator.tabulate(12000) { i =>
        val g = factory.newGroup()
        if (i % 11 != 0)
          g.append("i32", if (i % 5 == 0) Int.MaxValue else if (i % 7 == 0) Int.MinValue else rnd.nextInt())
        if (i % 13 != 0) g.append("dt", 10957 + (i % 3000))
        if (i % 17 != 0)
          g.append("i64", if (i % 6 == 0) Long.MaxValue else if (i % 9 == 0) Long.MinValue else rnd.nextLong())
        if (i % 4 != 0) g.append("dec", (i.toLong * 2654435761L) % 1000000000000000L)
        if (i % 19 != 0) g.append("d", if (i % 23 == 0) specials(i % specials.length) else rnd.nextGaussian() * 1e9)
        if (i % 3 != 0) {
          val s = i % 10 match {
            case 1 => ""
            case 2 => s"héllo wörld $i"
            case 3 => "x" * (100 + i % 400)
            case _ => s"v${rnd.nextInt(100000)}"
          }
          g.append("s", Binary.fromString(s))
        }
        g
      }
      org.apache.parquet.hadoop.V2EncodingWriter.write(new java.io.File(dir, f).getAbsolutePath, schema, rows, v2)
    }
    // The files really carry the encodings, and span several row groups.
    val encodings = new java.io.File(dir).listFiles((_, nm) => nm.endsWith(".parquet")).flatMap { f =>
      val in = org.apache.parquet.hadoop.util.HadoopInputFile
        .fromPath(new org.apache.hadoop.fs.Path(f.getAbsolutePath), new org.apache.hadoop.conf.Configuration())
      val r = org.apache.parquet.hadoop.ParquetFileReader.open(in)
      try {
        assert(r.getFooter.getBlocks.size() > 1, s"${f.getName}: expected several row groups")
        val cols = new scala.collection.mutable.ArrayBuffer[(String, String)]()
        r.getFooter.getBlocks.forEach(b =>
          b.getColumns.forEach(c => c.getEncodings.forEach(e => cols += ((c.getPath.toDotString, e.name()))))
        )
        cols
      } finally r.close()
    }.toSet
    assert(encodings.contains(("s", "DELTA_LENGTH_BYTE_ARRAY")), encodings.toString)
    for (c <- Seq("i32", "dt", "i64", "dec", "d")) {
      assert(encodings.contains((c, "BYTE_STREAM_SPLIT")), s"$c is not BYTE_STREAM_SPLIT: $encodings")
    }
    withPlugin(enabled = false) {
      spark.read.parquet(dir).createOrReplaceTempView("t_dlba_bss")
    }
    // Spark's vectorized reader rejects BYTE_STREAM_SPLIT on INT32/INT64 ("Unsupported encoding"), so the
    // reference is Spark's row-based parquet-mr reader.
    for (q <- Seq("SELECT * FROM t_dlba_bss", "SELECT s, d FROM t_dlba_bss WHERE i32 IS NOT NULL")) {
      val expected = withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
        withPlugin(enabled = false)(spark.sql(q).collect())
      }
      assert(expected.length > 0)
      for (batch <- Seq("1024", "100")) {
        withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
          withPlugin(enabled = true) {
            val df = spark.sql(q)
            val actual = df.collect()
            assertRowsEqual(expected, actual, 1e-9, s"$q (batch $batch)")
            val scans = PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
            assert(scans.nonEmpty, s"expected ${node.getSimpleName} in plan\n${finalPlan(df).treeString}")
            val rowGroups = scans.map(_.metrics("numRowGroups").value).sum
            assert(rowGroups > 2, s"$rowGroups row groups read natively: the files fell over to Spark's reader")
          }
        }
      }
    }
  }

  test("DELTA_BYTE_ARRAY strings (v2, no dictionary) are decoded natively (#559)") {
    // parquet-java writes DELTA_BYTE_ARRAY for v2 strings without a dictionary: per value, the length of
    // the prefix it shares with the previous value, then its suffix. Sorted keys share long prefixes;
    // nulls, empty strings, multi-byte UTF-8 and long values break the chain in every way, and small
    // pages restart it often. The node must count the row groups and return Spark's results.
    val path = newTempPath("t_dba")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CAST(id AS INT) AS i,
          |  CASE WHEN id % 13 = 0 THEN NULL
          |       WHEN id % 17 = 0 THEN ''
          |       WHEN id % 19 = 0 THEN CONCAT('héllo wörld ', CAST(id AS STRING))
          |       WHEN id % 23 = 0 THEN REPEAT('x', CAST(100 + id % 400 AS INT))
          |       ELSE CONCAT('https://example.com/item/', LPAD(CAST(id AS STRING), 9, '0')) END AS s,
          |  LPAD(CAST(id * 7 AS STRING), 12, '0') AS k
          |FROM range(0, 30000)""".stripMargin
      ).repartition(2)
        .sortWithinPartitions("i")
        .write
        .option("parquet.writer.version", "v2")
        .option("parquet.enable.dictionary", "false")
        .mode("overwrite")
        .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_dba")
    }
    val files = new java.io.File(path).listFiles((_, nm) => nm.endsWith(".parquet"))
    val encodings = files.flatMap { f =>
      val in = org.apache.parquet.hadoop.util.HadoopInputFile
        .fromPath(new org.apache.hadoop.fs.Path(f.getAbsolutePath), new org.apache.hadoop.conf.Configuration())
      val r = org.apache.parquet.hadoop.ParquetFileReader.open(in)
      try {
        val cols = new scala.collection.mutable.ArrayBuffer[(String, String)]()
        r.getFooter.getBlocks.forEach(b =>
          b.getColumns.forEach(c => c.getEncodings.forEach(e => cols += ((c.getPath.toDotString, e.name()))))
        )
        cols
      } finally r.close()
    }.toSet
    for (c <- Seq("s", "k")) {
      assert(encodings.contains((c, "DELTA_BYTE_ARRAY")), s"$c is not DELTA_BYTE_ARRAY: $encodings")
    }
    for (batch <- Seq("1024", "100")) {
      withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
        checkVectorized("SELECT * FROM t_dba", Seq(node))
        checkVectorized("SELECT s FROM t_dba WHERE s LIKE 'https://%5'", Seq(node))
        withPlugin(enabled = true) {
          val df = spark.sql("SELECT s, k FROM t_dba")
          df.collect()
          val scans = PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
          assert(scans.nonEmpty)
          val rowGroups = scans.map(_.metrics("numRowGroups").value).sum
          assert(rowGroups > 0, "no row group read natively: the DELTA_BYTE_ARRAY files fell over to Spark's reader")
        }
      }
    }
  }

  test("a decimal stored as FIXED_LEN_BYTE_ARRAY (legacy format) decodes natively into its INT64 lane (#559)") {
    // spark.sql.parquet.writeLegacyFormat=true (the format Hive and Impala read) stores every decimal as
    // FIXED_LEN_BYTE_ARRAY, also those with precision <= 18 that the planner admits as an INT64 lane. #592 made
    // such a file fall over (before it, the bytes were read as INT64 values: wrong results, no error); now its
    // big-endian bytes are converted into the INT64 lane. A legacy file and a standard one (INT32 / INT64
    // decimals) must both be decoded natively, with Spark's rows.
    val path = newTempPath("t_flba_dec")
    val query =
      """SELECT CAST(id AS INT) AS i,
        |  CASE WHEN id % 9 = 0 THEN NULL ELSE CAST((id * 37 % 1000000) / 100.0 AS DECIMAL(7,2)) END AS dec7,
        |  CASE WHEN id % 7 = 0 THEN NULL ELSE CAST((id * 2654435761) % 1000000000000 AS DECIMAL(15,2)) END AS dec15,
        |  CAST(-id * 1000003 AS DECIMAL(18,0)) AS dec18
        |FROM range(0, 6000)""".stripMargin
    withPlugin(enabled = false) {
      withConf("spark.sql.parquet.writeLegacyFormat" -> "true") {
        spark.sql(query).coalesce(1).write.mode("overwrite").parquet(path + "/legacy")
      }
      spark.sql(query).coalesce(1).write.mode("overwrite").parquet(path + "/standard")
    }
    def physical(dir: String): Set[String] =
      new java.io.File(dir).listFiles((_, nm) => nm.endsWith(".parquet")).flatMap { f =>
        val in = org.apache.parquet.hadoop.util.HadoopInputFile
          .fromPath(new org.apache.hadoop.fs.Path(f.getAbsolutePath), new org.apache.hadoop.conf.Configuration())
        val r = org.apache.parquet.hadoop.ParquetFileReader.open(in)
        try r.getFileMetaData.getSchema.getColumns.asScala.map(c =>
            s"${c.getPath.mkString(".")}:${c.getPrimitiveType.getPrimitiveTypeName}"
          )
        finally r.close()
      }.toSet
    assert(physical(path + "/legacy").contains("dec7:FIXED_LEN_BYTE_ARRAY"), physical(path + "/legacy").toString)
    assert(physical(path + "/legacy").contains("dec15:FIXED_LEN_BYTE_ARRAY"), physical(path + "/legacy").toString)
    assert(physical(path + "/standard").contains("dec7:INT32"), physical(path + "/standard").toString)
    assert(physical(path + "/standard").contains("dec15:INT64"), physical(path + "/standard").toString)
    def rowGroupsRead(dir: String): Long = withPlugin(enabled = true) {
      val df = spark.read.parquet(dir).select("dec7", "dec15", "dec18")
      df.collect()
      PlanUtils.allNodes(
        finalPlan(df)
      ).collect { case s: VectorParquetScanExec => s }.map(_.metrics("numRowGroups").value).sum
    }
    withPlugin(enabled = false) {
      spark.read.parquet(path + "/legacy").createOrReplaceTempView("t_flba_legacy")
      spark.read.parquet(path + "/standard").createOrReplaceTempView("t_flba_standard")
    }
    checkVectorized("SELECT * FROM t_flba_legacy", Seq(node))
    checkVectorized("SELECT sum(dec7), sum(dec15), min(dec18) FROM t_flba_legacy", Seq(node))
    checkVectorized("SELECT * FROM t_flba_standard", Seq(node))
    assert(rowGroupsRead(path + "/legacy") > 0, "the FIXED_LEN_BYTE_ARRAY decimal file was not decoded natively")
    assert(rowGroupsRead(path + "/standard") > 0, "the INT32 / INT64 decimal file was not decoded natively")
  }

  test("a pushed filter that skips pages through the column index keeps every column's rows aligned (#559)") {
    // parquet-java's readNextFilteredRowGroup also filters PAGES by the column index when the filter is
    // selective inside a row group, and returns only the matching row ranges of each column -- whose pages
    // start at different rows per column. The native decoder reads every column's pages back to back against
    // the row group's (filtered) row count, so a page-filtered row group misaligns the columns. The scan must
    // read whole row groups (row-group statistics and dictionaries still prune) and leave the row filter to
    // the Filter above it.
    val path = newTempPath("t_colidx")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id AS INT) AS i, CAST(id * 7 AS BIGINT) AS l,
          |  CONCAT('value-', CAST(id AS STRING), REPEAT('x', CAST(id % 37 AS INT))) AS s
          |FROM range(0, 60000)""".stripMargin
      ).coalesce(1)
        .sortWithinPartitions("i")
        .write
        .option("parquet.page.size", "4096")
        .option("parquet.page.row.count.limit", "1000")
        .option("parquet.enable.dictionary", "false")
        .mode("overwrite")
        .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_colidx")
    }
    checkVectorized("SELECT i, l, s FROM t_colidx WHERE i BETWEEN 12345 AND 12999", Seq(node))
    checkVectorized("SELECT count(*), sum(l), max(s) FROM t_colidx WHERE i < 1500 OR i > 58000", Seq(node))
    checkVectorized("SELECT s, i FROM t_colidx WHERE l = 7 * 31337", Seq(node))
  }

  test("wide decimals (p > 18) decode natively into the DECIMAL128 lane: FLBA, dictionary, DELTA_BYTE_ARRAY (#559)") {
    // Spark writes decimal(p > 18) as FIXED_LEN_BYTE_ARRAY: dictionary-encoded in v1, DELTA_BYTE_ARRAY in v2
    // without a dictionary, PLAIN when the dictionary is off. Values to +-(10^38 - 1), nulls, several row groups,
    // filters, aggregates and arithmetic on the lane, at two batch sizes; and a legacy-format file.
    val query =
      """SELECT CAST(id AS INT) AS i,
        |  CASE WHEN id % 7 = 0 THEN NULL
        |       ELSE CAST(CONCAT(CASE WHEN id % 2 = 0 THEN '-' ELSE '' END, CAST(id * 2654435761 AS STRING),
        |                        REPEAT(CAST(id % 10 AS STRING), CAST(id % 21 AS INT)), '.', CAST(id % 100 AS STRING))
        |                 AS DECIMAL(38, 2)) END AS d38,
        |  CAST(id % 97 AS DECIMAL(20, 0)) AS d20,
        |  CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id * 1000003 / 7 AS DECIMAL(25, 6)) END AS d25
        |FROM range(0, 30000)""".stripMargin
    for (
      (version, dict, legacy, view) <- Seq(
        ("v1", "true", "false", "t_wide_v1d"),
        ("v1", "false", "false", "t_wide_v1p"),
        ("v2", "false", "false", "t_wide_v2"),
        ("v1", "true", "true", "t_wide_legacy")
      )
    ) {
      val path = newTempPath(view)
      withPlugin(enabled = false) {
        withConf("spark.sql.parquet.writeLegacyFormat" -> legacy) {
          spark.sql(query).repartition(2).sortWithinPartitions("i")
            .write.option("parquet.writer.version", version).option("parquet.enable.dictionary", dict)
            .mode("overwrite").parquet(path)
        }
        spark.read.parquet(path).createOrReplaceTempView(view)
      }
      for (batch <- Seq("1024", "100")) {
        withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
          checkVectorized(s"SELECT * FROM $view", Seq(node))
          checkVectorized(s"SELECT i, d38 FROM $view WHERE d38 > 0 AND d20 < 50", Seq(node))
          checkVectorized(s"SELECT d20, count(*), sum(d25), min(d38), max(d38) FROM $view GROUP BY d20", Seq(node))
        }
      }
      assert(nativeRowGroupsOf(s"SELECT d38, d20, d25 FROM $view") > 0, s"$view: not decoded natively")
    }
  }

  test("a decimal whose file scale differs from the requested one falls over per file, with Spark's rescale (#559)") {
    // Spark reads a decimal(9,2) file as decimal(12,4) by rescaling the unscaled value; the native lanes carry the
    // file's unscaled values as they are, so such a file must go to Spark's reader. The same precision widening at
    // the same scale is read natively.
    val path = newTempPath("t_dec_scale")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CAST(id / 100.0 AS DECIMAL(9,2)) AS d, CAST(id AS DECIMAL(30,2)) AS w " +
        "FROM range(0, 5000)").coalesce(1).write.mode("overwrite").parquet(path)
    }
    def read(schema: String): Unit =
      withPlugin(enabled = false)(spark.read.schema(schema).parquet(path).createOrReplaceTempView("t_dec_scale"))
    read("i INT, d DECIMAL(12,4), w DECIMAL(32,4)")
    checkVectorized("SELECT * FROM t_dec_scale", Seq(node))
    assert(nativeRowGroupsOf("SELECT d, w FROM t_dec_scale") == 0, "a rescaled decimal was decoded natively")
    read("i INT, d DECIMAL(12,2), w DECIMAL(36,2)")
    checkVectorized("SELECT * FROM t_dec_scale", Seq(node))
    assert(nativeRowGroupsOf("SELECT d, w FROM t_dec_scale") > 0, "a widened same-scale decimal fell over")
  }

  private def nativeRowGroupsOf(sql: String): Long = withPlugin(enabled = true) {
    val df = spark.sql(sql)
    df.collect()
    PlanUtils.allNodes(
      finalPlan(df)
    ).collect { case s: VectorParquetScanExec => s }.map(_.metrics("numRowGroups").value).sum
  }

  test("partition columns are read as constant columns") {
    val path = newTempPath("t_part")
    withPlugin(enabled = false) {
      spark.sql(
        "SELECT CAST(id AS INT) AS v, CAST(id % 4 AS INT) AS p FROM range(0, 12000)"
      ).write.partitionBy("p").mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_part")
    }
    checkVectorized("SELECT v, p FROM t_part", Seq(node))
    checkVectorized("SELECT v FROM t_part WHERE p = 2", Seq(node)) // partition filter + partition column
  }

  test("a pushed data filter prunes row groups") {
    val path = newTempPath("t_filter")
    withPlugin(enabled = false) {
      // Sorted so row-group min/max stats are tight and the filter skips whole row groups.
      spark.sql("SELECT CAST(id AS INT) AS k, CAST(id AS BIGINT) AS v FROM range(0, 50000)")
        .sort("k").write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_filter")
    }
    checkVectorized("SELECT k, v FROM t_filter WHERE k > 49000", Seq(node))
    checkVectorized("SELECT count(*) AS c, sum(v) AS s FROM t_filter WHERE k BETWEEN 100 AND 200", Seq(node))
  }

  test("dynamic partition pruning query keeps the node on the pruned scan") {
    val factPath = newTempPath("dpp_fact")
    val dimPath = newTempPath("dpp_dim")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS v, CAST(id % 10 AS INT) AS pk FROM range(0, 40000)")
        .write.partitionBy("pk").mode("overwrite").parquet(factPath)
      spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact")
      spark.sql("SELECT CAST(id AS INT) AS pk, CONCAT('d', CAST(id AS STRING)) AS label FROM range(0, 10)")
        .write.mode("overwrite").parquet(dimPath)
      spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim")
    }
    withConf(
      "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> (10 * 1024 * 1024).toString
    ) {
      // The dim filter prunes the fact partitions at runtime; the fact scan is our node.
      checkVectorized(
        "SELECT f.v FROM dpp_fact f JOIN dpp_dim d ON f.pk = d.pk WHERE d.label = 'd3'",
        Seq(node)
      )
    }
  }

  test("empty result and all-null column still match Spark") {
    val path = newTempPath("t_edge")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS k, CAST(NULL AS STRING) AS allnull FROM range(0, 5000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_edge")
    }
    checkVectorized("SELECT k, allnull FROM t_edge WHERE k < 0", Seq(node)) // empty
    checkVectorized("SELECT allnull FROM t_edge", Seq(node)) // all-null column
  }

  test("tiny table with nulls (conditional-functions.sql regression): nanvl over c1 double, c2 int") {
    val path = newTempPath("t_tiny")
    withPlugin(enabled = false) {
      spark.sql(
        "SELECT c1, c2 FROM VALUES(1d, 0),(2d, 1),(CAST(NULL AS DOUBLE), 1),(CAST('NaN' AS DOUBLE), 0) AS t(c1, c2)"
      )
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_tiny")
    }
    checkVectorized("SELECT c1, c2 FROM t_tiny", Seq(node))
    checkVectorized("SELECT nanvl(c2, c1/c2 + c1/c2) FROM t_tiny", Seq(node))
  }

  test("BOOLEAN columns decode natively: PLAIN bit-packed (v1) and RLE (v2), with nulls (#559)") {
    // parquet-java writes booleans PLAIN (bit-packed) in v1 pages and RLE (a width-1 hybrid stream) in v2.
    // Runs and random stretches, nulls, several pages and row groups, a filter and an aggregate over the
    // column, at two batch sizes. The node must read the row groups itself and match Spark's reader.
    for ((version, view) <- Seq("v1" -> "t_bool_v1", "v2" -> "t_bool_v2")) {
      val path = newTempPath(view)
      withPlugin(enabled = false) {
        spark.sql(
          """SELECT CAST(id AS INT) AS i,
            |  CASE WHEN id % 7 = 0 THEN NULL WHEN (id DIV 300) % 3 = 0 THEN (id * 2654435761) % 5 < 2
            |       ELSE (id DIV 500) % 2 = 0 END AS b,
            |  id % 2 = 0 AS even
            |FROM range(0, 40000)""".stripMargin
        ).repartition(2)
          .sortWithinPartitions("i")
          .write
          .option("parquet.writer.version", version)
          .mode("overwrite")
          .parquet(path)
        spark.read.parquet(path).createOrReplaceTempView(view)
      }
      val encodings = new java.io.File(path).listFiles((_, nm) => nm.endsWith(".parquet")).flatMap { f =>
        val in = org.apache.parquet.hadoop.util.HadoopInputFile
          .fromPath(new org.apache.hadoop.fs.Path(f.getAbsolutePath), new org.apache.hadoop.conf.Configuration())
        val r = org.apache.parquet.hadoop.ParquetFileReader.open(in)
        try {
          val cols = new scala.collection.mutable.ArrayBuffer[(String, String)]()
          r.getFooter.getBlocks.forEach(b =>
            b.getColumns.forEach(c => c.getEncodings.forEach(e => cols += ((c.getPath.toDotString, e.name()))))
          )
          cols
        } finally r.close()
      }.toSet
      val valueEncoding = if (version == "v1") "PLAIN" else "RLE"
      assert(encodings.contains(("b", valueEncoding)), s"$version: b is not $valueEncoding: $encodings")
      for (batch <- Seq("1024", "100")) {
        withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
          checkVectorized(s"SELECT * FROM $view", Seq(node))
          checkVectorized(s"SELECT i FROM $view WHERE b AND NOT even", Seq(node))
          checkVectorized(s"SELECT b, count(*), sum(i) FROM $view GROUP BY b", Seq(node))
          withPlugin(enabled = true) {
            val df = spark.sql(s"SELECT b, even FROM $view")
            df.collect()
            val scans = PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
            assert(scans.nonEmpty, s"$version: expected ${node.getSimpleName}")
            assert(scans.map(_.metrics("numRowGroups").value).sum > 0, s"$version: no row group read natively")
          }
        }
      }
    }
  }

  test("TINYINT and SMALLINT columns decode natively: dictionary, PLAIN, DELTA_BINARY_PACKED, nulls (#559)") {
    // INT32 physical with INT(8) / INT(16) annotations, into the INT32 lane with the declared type on output
    // (VectorNarrowIntColumnVector). v1 pages are dictionary-encoded, v2 without a dictionary are
    // DELTA_BINARY_PACKED; nulls, the full range including the extremes, several row groups, filters, an aggregate
    // keyed by the narrow column, arithmetic and casts over it, at two batch sizes.
    for (
      (version, dict, view) <-
        Seq(("v1", "true", "t_narrow_v1"), ("v1", "false", "t_narrow_v1p"), ("v2", "false", "t_narrow_v2"))
    ) {
      val path = newTempPath(view)
      withPlugin(enabled = false) {
        spark.sql(
          """SELECT CAST(id AS INT) AS i,
            |  CASE WHEN id % 7 = 0 THEN NULL ELSE CAST((id * 37) % 256 - 128 AS TINYINT) END AS t,
            |  CASE WHEN id % 11 = 0 THEN NULL ELSE CAST((id * 2654435761) % 65536 - 32768 AS SMALLINT) END AS s,
            |  CAST(id % 5 AS TINYINT) AS lowcard
            |FROM range(0, 40000)""".stripMargin
        ).repartition(2)
          .sortWithinPartitions("i")
          .write
          .option("parquet.writer.version", version)
          .option("parquet.enable.dictionary", dict)
          .mode("overwrite")
          .parquet(path)
        spark.read.parquet(path).createOrReplaceTempView(view)
      }
      for (batch <- Seq("1024", "100")) {
        withConf("spark.sql.parquet.columnarReaderBatchSize" -> batch) {
          checkVectorized(s"SELECT * FROM $view", Seq(node))
          checkVectorized(s"SELECT i, t, s FROM $view WHERE t < 0 AND s > 100", Seq(node))
          checkVectorized(s"SELECT lowcard, count(*), sum(t), min(s), max(s) FROM $view GROUP BY lowcard", Seq(node))
          checkVectorized(s"SELECT t + 1, s * 2, CAST(t AS STRING), CAST(s AS BIGINT) FROM $view", Seq(node))
          withPlugin(enabled = true) {
            val df = spark.sql(s"SELECT t, s FROM $view")
            df.collect()
            val scans = PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
            assert(scans.nonEmpty, s"$view: expected ${node.getSimpleName}")
            assert(scans.map(_.metrics("numRowGroups").value).sum > 0, s"$view: no row group read natively")
          }
        }
      }
    }
  }

  test("an INT(8) / INT(16) value outside its declared range wraps as Spark's readers wrap it (#559)") {
    // A writer may store any INT32 under an INT(8) / INT(16) annotation. Spark's readers narrow with a
    // (byte) / (short) cast; the lane must carry the narrowed value too, or an operator over the int (a sum, a
    // comparison) would see 300 where Spark sees 44.
    val path = newTempPath("t_narrow_wrap")
    new java.io.File(path).mkdirs()
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message m { required int32 t (INTEGER(8,true)); optional int32 s (INTEGER(16,true)); }"
    )
    val conf = new org.apache.hadoop.conf.Configuration()
    val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
      .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
      .withConf(conf)
      .withType(schema)
      .build()
    val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
    val raw = Seq(0, 1, -1, 127, -128, 128, 300, -300, 65535, Int.MaxValue, Int.MinValue, 40000, -40000)
    try {
      for (k <- 0 until 3000) {
        val v = raw(k % raw.length) + (k / raw.length) * 256
        val g = factory.newGroup().append("t", v)
        if (k % 5 != 0) g.append("s", v)
        writer.write(g)
      }
    } finally writer.close()
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t_narrow_wrap"))
    checkVectorized("SELECT * FROM t_narrow_wrap", Seq(node))
    checkVectorized("SELECT sum(t), sum(s), min(t), max(s), count(*) FROM t_narrow_wrap WHERE t > 0", Seq(node))
    withPlugin(enabled = true) {
      val df = spark.sql("SELECT t, s FROM t_narrow_wrap")
      df.collect()
      val scans = PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s }
      assert(scans.map(_.metrics("numRowGroups").value).sum > 0, "no row group read natively")
    }
  }

  test("TIMESTAMP columns stored as INT64 MICROS and MILLIS decode natively, with nulls (#559)") {
    // MICROS is the lane as stored; MILLIS is scaled by 1000 (Spark's LongAsMicrosUpdater). Dictionary and
    // plain, v1 and v2 (DELTA_BINARY_PACKED), values before 1582 and after 2038, a filter and an aggregate.
    for (
      (unit, version, view) <- Seq(
        ("TIMESTAMP_MICROS", "v1", "t_ts_us1"),
        ("TIMESTAMP_MICROS", "v2", "t_ts_us2"),
        ("TIMESTAMP_MILLIS", "v1", "t_ts_ms1"),
        ("TIMESTAMP_MILLIS", "v2", "t_ts_ms2")
      )
    ) {
      val path = newTempPath(view)
      withPlugin(enabled = false) {
        withConf("spark.sql.parquet.outputTimestampType" -> unit) {
          spark.sql(
            """SELECT CAST(id AS INT) AS i,
              |  CASE WHEN id % 7 = 0 THEN NULL
              |       ELSE TIMESTAMP_MICROS((id * 2654435761 % 200000000000) * 1000000 - 30000000000000000) END AS ts,
              |  TIMESTAMP_MICROS((id % 13) * 86400000000) AS lowcard
              |FROM range(0, 30000)""".stripMargin
          ).repartition(2)
            .sortWithinPartitions("i")
            .write
            .option("parquet.writer.version", version)
            .mode("overwrite")
            .parquet(path)
        }
        spark.read.parquet(path).createOrReplaceTempView(view)
      }
      checkVectorized(s"SELECT * FROM $view", Seq(node))
      checkVectorized(s"SELECT i, ts FROM $view WHERE ts > TIMESTAMP'1990-01-01 00:00:00'", Seq(node))
      checkVectorized(s"SELECT lowcard, count(*), min(ts), max(ts) FROM $view GROUP BY lowcard", Seq(node))
      assert(nativeRowGroups(s"SELECT ts, lowcard FROM $view") > 0, s"$view: no row group read natively")
    }
  }

  test("INT96 timestamps and legacy-calendar files fall over per file with the file's own rebase modes (#559)") {
    // Spark's default output timestamp type is INT96: such a file is read by Spark's reader. A file written
    // with the LEGACY rebase mode (a hybrid-calendar date and timestamp before 1582) carries the legacy key, so
    // Spark rebases it on read -- the fallback reader must be built with that file's LEGACY mode, not the fixed
    // CORRECTED of its two-argument constructor. A CORRECTED file in the same table is still read natively.
    val path = newTempPath("t_ts_mixed")
    val query =
      """SELECT CAST(id AS INT) AS i,
        |  TIMESTAMP_MICROS(id * 86400000000 * 37 - 40000000000000000) AS ts,
        |  DATE_ADD(DATE'1000-01-01', CAST(id * 11 AS INT)) AS d
        |FROM range(0, 3000)""".stripMargin
    withPlugin(enabled = false) {
      withConf("spark.sql.parquet.outputTimestampType" -> "INT96") {
        spark.sql(query).coalesce(1).write.mode("overwrite").parquet(path + "/int96")
      }
      withConf(
        "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS",
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "LEGACY"
      ) {
        spark.sql(query).coalesce(1).write.mode("overwrite").parquet(path + "/legacy")
      }
      withConf(
        "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS",
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED"
      ) {
        spark.sql(query).coalesce(1).write.mode("overwrite").parquet(path + "/corrected")
      }
    }
    for (dir <- Seq("int96", "legacy", "corrected")) {
      withPlugin(enabled = false)(spark.read.parquet(s"$path/$dir").createOrReplaceTempView(s"t_ts_$dir"))
      checkVectorized(s"SELECT * FROM t_ts_$dir", Seq(node))
      checkVectorized(s"SELECT min(ts), max(ts), min(d), max(d) FROM t_ts_$dir", Seq(node))
    }
    assert(nativeRowGroups("SELECT ts, d FROM t_ts_int96") == 0, "an INT96 file was decoded natively")
    assert(nativeRowGroups("SELECT ts, d FROM t_ts_legacy") == 0, "a LEGACY-calendar file was decoded natively")
    assert(nativeRowGroups("SELECT ts, d FROM t_ts_corrected") > 0, "a CORRECTED file was not decoded natively")
    // Under EXCEPTION (resolved per file, not per plan), a CORRECTED Spark file is still read natively, and the
    // legacy one gets Spark's own rebase from its footer key.
    withConf("spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION") {
      checkVectorized("SELECT * FROM t_ts_corrected", Seq(node))
      checkVectorized("SELECT * FROM t_ts_legacy", Seq(node))
      assert(nativeRowGroups("SELECT ts, d FROM t_ts_corrected") > 0, "EXCEPTION: the CORRECTED file fell over")
    }
  }

  test("a TIMESTAMP(MILLIS) value whose micros overflow fails the read, as Spark's reader does (#559)") {
    val path = newTempPath("t_ts_overflow")
    new java.io.File(path).mkdirs()
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message m { optional int64 ts (TIMESTAMP(MILLIS,true)); }"
    )
    val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
      .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
      .withConf(new org.apache.hadoop.conf.Configuration())
      .withType(schema)
      .build()
    val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
    try {
      for (k <- 0 until 2000) {
        val g = factory.newGroup()
        // Nulls first: a null slot must not be scaled. Then one overflowing value.
        if (k % 3 != 0) g.append("ts", if (k == 1999) Long.MaxValue / 100 else 1700000000000L + k)
        writer.write(g)
      }
    } finally writer.close()
    def attempt(plugin: Boolean): Either[Throwable, Long] =
      try Right(withPlugin(enabled = plugin)(spark.read.parquet(path).collect().length.toLong))
      catch { case e: Exception => Left(e) }
    def overflow(t: Throwable): Boolean =
      Iterator.iterate(t)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[ArithmeticException])
    val off = attempt(plugin = false)
    val on = attempt(plugin = true)
    assert(off.isLeft && overflow(off.left.toOption.get), s"Spark's reader did not overflow: $off")
    assert(on.isLeft && overflow(on.left.toOption.get), s"the native scan did not overflow: $on")
  }

  private def nativeRowGroups(sql: String): Long = withPlugin(enabled = true) {
    val df = spark.sql(sql)
    df.collect()
    PlanUtils.allNodes(
      finalPlan(df)
    ).collect { case s: VectorParquetScanExec => s }.map(_.metrics("numRowGroups").value).sum
  }

  test("early termination (LIMIT) does not leak the reused vectors") {
    // A file large enough to span several row groups; a small LIMIT abandons the iterator mid-file, so the
    // task-completion listener must close the reused vectors and the allocator exactly once (no Arrow leak).
    val path = newTempPath("t_limit")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CONCAT('s', CAST(id % 100 AS STRING)) AS s FROM range(0, 60000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_limit")
    }
    checkVectorized("SELECT i, s FROM t_limit LIMIT 10", Seq(node))
    checkVectorized("SELECT i FROM t_limit WHERE i >= 0 LIMIT 5000", Seq(node))
  }

  test("scan -> filter(IsNotNull) -> project over nulls does NOT take the adapter copy path") {
    val path = newTempPath("t_nocopy")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CASE WHEN id % 4 = 0 THEN NULL ELSE CAST(id AS INT) END AS i,
          |  CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id * 3 AS BIGINT) END AS l,
          |  CASE WHEN id % 6 = 0 THEN NULL ELSE CAST(id * 1.5 AS DOUBLE) END AS d,
          |  CASE WHEN id % 7 = 0 THEN NULL ELSE CONCAT('s', CAST(id % 30 AS STRING)) END AS s
          |FROM range(0, 30000)""".stripMargin
      )
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_nocopy")
    }
    // The scan feeds our filter and project directly; the adapter must take every column zero-copy
    // (SparkColumnVectorBuffers.copy would bulk-read Arrow null slots and throw). Assert the copy counter
    // did not advance while our operators consumed the node's batches.
    val before = io.vecruntime.spark.adapter.ColumnVectorAdapters.copiedColumns()
    checkVectorized(
      "SELECT i, l, d, s FROM t_nocopy WHERE i IS NOT NULL AND s IS NOT NULL",
      Seq(node, classOf[org.apache.spark.sql.vecruntime.VectorFilterExec])
    )
    val after = io.vecruntime.spark.adapter.ColumnVectorAdapters.copiedColumns()
    assert(
      after == before,
      s"the adapter copy path was taken ${after - before} times; the scan's batches must adapt zero-copy"
    )
  }

  test("Spark consumer (ColumnarToRow) over the node's batches with nulls, per type") {
    val path = newTempPath("t_sparkcons")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT
          |  CASE WHEN id % 3 = 0 THEN NULL ELSE CAST(id AS INT) END AS i,
          |  CASE WHEN id % 4 = 0 THEN NULL ELSE CAST(id AS BIGINT) END AS l,
          |  CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS DOUBLE) END AS d,
          |  CASE WHEN id % 6 = 0 THEN NULL ELSE CAST((id % 100000)/100.0 AS DECIMAL(9,2)) END AS dec,
          |  CASE WHEN id % 7 = 0 THEN NULL ELSE DATE_ADD(DATE'2001-01-01', CAST(id % 500 AS INT)) END AS dt,
          |  CASE WHEN id % 8 = 0 THEN NULL ELSE CONCAT('v', CAST(id % 40 AS STRING)) END AS s
          |FROM range(0, 20000)""".stripMargin
      )
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_sparkcons")
    }
    // A bare SELECT * with no operator of ours above the scan: the node's batches go straight to
    // ColumnarToRowExec (a Spark consumer), which must read null slots without Arrow's get() throwing.
    checkVectorized("SELECT i, l, d, dec, dt, s FROM t_sparkcons", Seq(node))
  }

  test("a scalar-subquery partition filter is awaited (ReusedSubquery has-not-finished regression)") {
    // The scan's partition pruning uses a scalar subquery in the partition predicate. VectorParquetScanExec
    // wraps the scan and reads its selected partitions directly, so it must WAIT for the scan's subqueries
    // (not just DPP) -- else the subquery is "started but not finished" and reading the partitions throws an
    // IllegalArgumentException that awaitResult wraps in a null-condition SparkException (which broke a
    // SQL-scripting golden case). Two forms: an equality on a scalar subquery, and IN a scalar subquery.
    val factPath = newTempPath("sub_fact")
    val dimPath = newTempPath("sub_dim")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS v, CAST(id % 8 AS INT) AS pk FROM range(0, 40000)")
        .write.partitionBy("pk").mode("overwrite").parquet(factPath)
      spark.read.parquet(factPath).createOrReplaceTempView("sub_fact")
      spark.sql("SELECT CAST(id AS INT) AS k FROM range(3, 5)").write.mode("overwrite").parquet(dimPath)
      spark.read.parquet(dimPath).createOrReplaceTempView("sub_dim")
    }
    withConf("spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "false") {
      checkVectorized("SELECT v FROM sub_fact WHERE pk = (SELECT MIN(k) FROM sub_dim)", Seq(node))
      checkVectorized("SELECT v FROM sub_fact WHERE pk IN (SELECT k FROM sub_dim)", Seq(node))
    }
  }

  test("prefetch (#559/#566): many small files and multi-row-group files read identically at every depth") {
    // A split of many small files (the 1 TB store_sales shape: ~7 MB files, one row group each) exercises the
    // files opened ahead; one big file with many row groups exercises the chained row-group read-ahead.
    val small = newTempPath("t_pf_small")
    val big = newTempPath("t_pf_big")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id AS INT) AS i, CAST(id * 7 AS BIGINT) AS l,
          |  CASE WHEN id % 9 = 0 THEN NULL ELSE CONCAT('s', CAST(id % 40 AS STRING)) END AS s,
          |  CAST(id % 37 AS INT) AS p
          |FROM range(0, 40000)""".stripMargin
      ).repartition(37).write.mode("overwrite").partitionBy("p").parquet(small)
      spark.read.parquet(small).createOrReplaceTempView("t_pf_small")
    }
    withConf("parquet.block.size" -> (64 * 1024).toString) {
      withPlugin(enabled = false) {
        spark.sql("SELECT CAST(id AS INT) AS i, CAST(id * 3 AS BIGINT) AS l FROM range(0, 150000)")
          .coalesce(1).write.mode("overwrite").parquet(big)
        spark.read.parquet(big).createOrReplaceTempView("t_pf_big")
      }
    }
    // One partition over all the small files, so a single reader walks a long run of files.
    withConf(
      "spark.sql.files.maxPartitionBytes" -> (512L * 1024 * 1024).toString,
      "spark.sql.files.openCostInBytes" -> "0"
    ) {
      for ((files, rowGroups) <- Seq("0" -> "0", "1" -> "0", "0" -> "1", "2" -> "2", "8" -> "8")) {
        withConf(
          VectorConf.ScanNativeParquetPrefetchFiles -> files,
          VectorConf.ScanNativeParquetPrefetchRowGroups -> rowGroups
        ) {
          checkVectorized("SELECT p, count(*) AS c, sum(l) AS s, count(s) AS cs FROM t_pf_small GROUP BY p", Seq(node))
          checkVectorized("SELECT i, l, s, p FROM t_pf_small ORDER BY i", Seq(node))
          checkVectorized("SELECT i, l FROM t_pf_big ORDER BY i", Seq(node))
          checkVectorized("SELECT count(*) AS c, sum(l) AS s FROM t_pf_big WHERE i % 3 = 0", Seq(node))
          // Early termination with files and row groups still in flight: the task end drains and closes them.
          checkVectorized("SELECT i FROM t_pf_small LIMIT 7", Seq(node))
          checkVectorized("SELECT i FROM t_pf_big LIMIT 5", Seq(node))
        }
      }
    }
  }

  test("task input metrics count the scan's records and bytes, also those read on prefetch threads") {
    // Spark's FileScanRDD reports inputMetrics.recordsRead / bytesRead per task; the node did not, and the
    // read-ahead moves FileSystem reads onto prefetch threads that the task thread's per-thread FileSystem
    // byte count cannot see. Both must show up in the task metrics, close to what Spark's reader reports.
    val path = newTempPath("t_inmetrics")
    withPlugin(enabled = false) {
      spark.sql(
        "SELECT CAST(id AS INT) AS i, CAST(id * 7 AS BIGINT) AS l, CAST(id % 23 AS INT) AS p FROM range(0, 60000)"
      )
        .repartition(23).write.mode("overwrite").partitionBy("p").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_inmetrics")
    }
    def readMetrics(plugin: Boolean): (Long, Long) = {
      val records = new java.util.concurrent.atomic.AtomicLong()
      val bytes = new java.util.concurrent.atomic.AtomicLong()
      val tasks = new java.util.concurrent.atomic.AtomicLong()
      val listener = new org.apache.spark.scheduler.SparkListener {
        override def onTaskEnd(e: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit =
          if (e.taskMetrics != null) {
            records.addAndGet(e.taskMetrics.inputMetrics.recordsRead)
            bytes.addAndGet(e.taskMetrics.inputMetrics.bytesRead)
            tasks.incrementAndGet()
          }
      }
      spark.sparkContext.addSparkListener(listener)
      try {
        withPlugin(enabled = plugin) {
          spark.sql("SELECT i, l FROM t_inmetrics").collect()
        }
        // The listener bus is asynchronous: wait until the task count stops moving.
        var last = -1L
        var stable = 0
        while (stable < 5) {
          Thread.sleep(100)
          val now = tasks.get()
          if (now == last && now > 0) stable += 1 else stable = 0
          last = now
        }
      } finally spark.sparkContext.removeSparkListener(listener)
      (records.get(), bytes.get())
    }
    val (sparkRecords, sparkBytes) = readMetrics(plugin = false)
    assert(sparkRecords == 60000, s"Spark's reader reported $sparkRecords records")
    assert(sparkBytes > 0, "Spark's reader reported no bytes")
    for ((files, rowGroups) <- Seq("0" -> "0", "6" -> "2")) {
      withConf(
        VectorConf.ScanNativeParquetPrefetchFiles -> files,
        VectorConf.ScanNativeParquetPrefetchRowGroups -> rowGroups
      ) {
        val (records, bytes) = readMetrics(plugin = true)
        assert(records == 60000, s"prefetch $files/$rowGroups: $records records, expected 60000")
        // Same files, same columns: the byte counts agree up to the readers' different footer / page reads.
        assert(
          bytes > sparkBytes / 2 && bytes < sparkBytes * 2,
          s"prefetch $files/$rowGroups: $bytes bytes against Spark's $sparkBytes"
        )
      }
    }
  }

  test("a file split into several PartitionedFiles reads each row group once (no duplicates)") {
    // Write ONE parquet file with many small row groups, then force Spark to split it into >= 2
    // PartitionedFiles (small maxPartitionBytes). Each split must read only the row groups whose midpoint
    // falls in its byte range; without the range, every split reads the whole file -> duplicate rows.
    val path = newTempPath("t_split")
    withConf("parquet.block.size" -> (64 * 1024).toString, "parquet.page.size" -> (4 * 1024).toString) {
      withPlugin(enabled = false) {
        spark.sql("SELECT CAST(id AS INT) AS i, CAST(id * 7 AS BIGINT) AS l FROM range(0, 200000)")
          .coalesce(1).write.mode("overwrite").parquet(path)
        spark.read.parquet(path).createOrReplaceTempView("t_split")
      }
    }
    withConf("spark.sql.files.maxPartitionBytes" -> (256 * 1024).toString) {
      // Confirm the read actually splits (more than one partition), else the test proves nothing.
      val parts = withPlugin(enabled = true)(spark.sql("SELECT i FROM t_split").rdd.getNumPartitions)
      assert(parts >= 2, s"expected the file to split into >= 2 partitions, got $parts")
      checkVectorized("SELECT count(*) AS c FROM t_split", Seq(node))
      checkVectorized("SELECT count(*) AS c, sum(l) AS s, min(i) AS mn, max(i) AS mx FROM t_split", Seq(node))
      // Row-for-row (ordered) equality across splits, flag on vs off.
      checkVectorized("SELECT i, l FROM t_split ORDER BY i", Seq(node))
    }
  }

  test("a SlicedColumnVector over a large row-group vector adapts and serializes to only its slice rows") {
    // Regression for the 1 TB driver OOM: a batch is a SlicedColumnVector over a reused row-group vector of
    // (say) 1,000,000 rows; when adapted/serialized only the slice's rows must travel, not the whole vector.
    val allocator = VectorAllocators.newChild("slice-ser")
    try {
      val rgRows = 1000000
      val v = new org.apache.arrow.vector.IntVector("i", allocator)
      v.allocateNew(rgRows)
      var k = 0
      while (k < rgRows) { v.set(k, k); k += 1 }
      v.setValueCount(rgRows)
      val whole = new io.vecruntime.spark.arrow.VectorArrowColumnVector(v)
      val batchN = 4096
      val sliced = io.vecruntime.spark.arrow.SlicedColumnVector.of(whole, 512000, batchN, rgRows)
      val arena = java.lang.foreign.Arena.ofConfined()
      try {
        val vb = io.vecruntime.spark.adapter.ColumnVectorAdapters.adapt(sliced, batchN, arena)
        assert(vb.length() == batchN, s"adapted length ${vb.length()} should be the slice's $batchN, not $rgRows")
        assert(vb.getInt(0) == 512000 && vb.getInt(batchN - 1) == 512000 + batchN - 1, "slice reads the right rows")
        val batch = new org.apache.spark.sql.vectorized.ColumnarBatch(Array[ColumnVector](sliced), batchN)
        val (rows, bytes) = VectorBroadcastBatches.write(
          Iterator.single(batch),
          Array("i"),
          Array[org.apache.spark.sql.types.DataType](org.apache.spark.sql.types.IntegerType)
        )
        assert(rows == batchN, s"serialized $rows rows, expected $batchN")
        assert(
          bytes.length <= batchN.toLong * 8 + 8192,
          s"serialized ${bytes.length} bytes for $batchN rows: the row group leaked"
        )
      } finally arena.close()
      v.close()
    } finally {
      allocator.close()
    }
  }

  test("broadcast hash join with our multi-row-group scan as the build side: correct rows, bounded bytes") {
    // q24a shape in miniature: a large fact scanned by our node joined to a small dimension broadcast. The
    // build side (dimension) is our scan of a multi-row-group table; the broadcast relation must ship only
    // the dimension's rows, not a row group per emitted batch (the 1 TB driver OOM). Owned-buffer batches
    // guarantee that.
    val factPath = newTempPath("bj_fact")
    val dimPath = newTempPath("bj_dim")
    withConf("parquet.block.size" -> (1 << 20).toString, "parquet.page.size" -> (16 * 1024).toString) {
      withPlugin(enabled = false) {
        spark.sql(
          "SELECT CAST(id AS INT) AS f_id, CAST(id % 500 AS INT) AS d_id, CAST(id AS BIGINT) AS amt FROM range(0, 300000)"
        )
          .coalesce(1).write.mode("overwrite").parquet(factPath)
        spark.read.parquet(factPath).createOrReplaceTempView("bj_fact")
        // A dimension of 500 rows written into many small row groups (so a per-row-group broadcast would blow up).
        spark.sql("SELECT CAST(id AS INT) AS d_id, CONCAT('name', CAST(id AS STRING)) AS d_name FROM range(0, 500)")
          .coalesce(1).write.mode("overwrite").parquet(dimPath)
        spark.read.parquet(dimPath).createOrReplaceTempView("bj_dim")
      }
    }
    withConf(
      VectorConf.ScanNativeParquet -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> (10 * 1024 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "4096"
    ) {
      checkVectorized(
        "SELECT d.d_name, count(*) AS c, sum(f.amt) AS s FROM bj_fact f JOIN bj_dim d ON f.d_id = d.d_id " +
          "GROUP BY d.d_name HAVING sum(f.amt) > (SELECT min(amt) FROM bj_fact) ORDER BY d.d_name",
        Seq(node)
      )
    }
  }

  // ---------------------------------------------------------------- fallbacks

  test("fallback: a nested (struct) column keeps Spark's scan with a recorded reason") {
    val path = newTempPath("t_nested")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, named_struct('a', id, 'b', CAST(id AS STRING)) AS st FROM range(0, 1000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_nested")
    }
    checkFallback("SELECT i, st FROM t_nested", Seq(node), reasonContains = "nested or complex column")
  }

  test("fallback: a binary column is not a supported lane in slice 1") {
    val path = newTempPath("t_bin")
    withPlugin(enabled = false) {
      spark.sql("SELECT CAST(id AS INT) AS i, CAST(CONCAT('b', CAST(id AS STRING)) AS BINARY) AS b FROM range(0, 1000)")
        .write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t_bin")
    }
    checkFallback("SELECT i, b FROM t_bin", Seq(node), reasonContains = "unsupported column type")
  }

  test("the open path makes one open and one getFileStatus per split") {
    // The node must open each split's file ONCE (no reopen for footer-then-filter) and take the file's
    // status from the store exactly once -- on S3A that status carries the etag the Analytics Accelerator
    // stream needs; a status built from the PartitionedFile has none and the stream HEADs again itself.
    // A counting local FileSystem under scheme `countfs` wraps RawLocalFileSystem and tallies open() and
    // getFileStatus() against our one data file, counting ONLY calls made inside a Spark task (the node's
    // openFile runs in a task; Spark's driver-side planning I/O has no TaskContext and is excluded). We scan
    // with the flag on and assert the node's open path: one open and one getFileStatus per split.
    val dir = newTempPath("t_io_shape")
    withConf("parquet.block.size" -> (64 * 1024).toString, "parquet.page.size" -> (4 * 1024).toString) {
      withPlugin(enabled = false) {
        spark.sql("SELECT CAST(id AS INT) AS i, CAST(id * 7 AS BIGINT) AS l FROM range(0, 200000)")
          .coalesce(1).write.mode("overwrite").parquet(dir)
      }
    }
    // The single parquet part file Spark wrote, addressed through the counting FS (same bytes on disk, a
    // scheme that routes through CountingLocalFileSystem). A `file:`-scheme path would bypass the counter.
    val localDir = new java.io.File(new java.net.URI(dir).getPath)
    val part = localDir.listFiles((_, nm) => nm.endsWith(".parquet"))
      .sortBy(_.getName).head.getAbsolutePath
    val countPath = "countfs://" + part

    def scanCounting(maxPartitionBytes: Long): (Long, Int, Int, Int) = {
      CountingLocalFileSystem.reset(part)
      val (rows, splits) = withConf("spark.sql.files.maxPartitionBytes" -> maxPartitionBytes.toString) {
        withPlugin(enabled = true) {
          val df = spark.read.schema("i INT, l BIGINT").parquet(countPath)
          val n = df.count()
          assert(
            PlanUtils.allNodes(finalPlan(df)).exists(node.isInstance),
            s"expected ${node.getSimpleName} in plan\n${finalPlan(df).treeString}"
          )
          (n, df.rdd.getNumPartitions)
        }
      }
      (rows, splits, CountingLocalFileSystem.opens(part), CountingLocalFileSystem.getFileStatus(part))
    }

    // The counting FS tallies only calls made inside a Spark task (the node's openFile), excluding Spark's
    // driver-side planning I/O, and counts only STANDALONE getFileStatus (a HEAD), not the existence check
    // RawLocalFileSystem.open does internally. So the node's open path is measured directly: one open per
    // split, one standalone getFileStatus per split (HadoopInputFile.fromPath), never a second.
    val (rows1, splits1, opens1, stat1) = scanCounting(maxPartitionBytes = 256L * 1024 * 1024)
    assert(rows1 == 200000, s"expected 200000 rows, got $rows1")
    assert(splits1 == 1, s"expected a single split, got $splits1")
    assert(opens1 == 1, s"expected exactly 1 open for 1 split, got $opens1")
    assert(stat1 == 1, s"expected 1 standalone getFileStatus (HEAD) for 1 split, got $stat1")

    val (rowsN, splitsN, opensN, statN) = scanCounting(maxPartitionBytes = 256L * 1024)
    assert(rowsN == 200000, s"expected 200000 rows across splits, got $rowsN (duplicates?)")
    assert(splitsN >= 2, s"expected >= 2 splits, got $splitsN")
    assert(opensN == splitsN, s"expected exactly one open per split ($splitsN), got $opensN")
    assert(statN == splitsN, s"expected exactly one getFileStatus (HEAD) per split ($splitsN), got $statN")
  }
}

/**
 * A local [[org.apache.hadoop.fs.RawLocalFileSystem]] under the `countfs` scheme that tallies `open` and
 * `getFileStatus` per absolute local path, so a test can assert the native scan's open path makes one
 * `open` and one `getFileStatus` per split (the single-open shape of #559). It only counts; all
 * I/O delegates to the raw local FS. A `countfs:///<abs>` URI maps to the local file at `/<abs>`.
 */
object CountingLocalFileSystem {
  private val openCounts =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicInteger]()
  private val statCounts =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicInteger]()

  private def counter(
      m: java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicInteger],
      key: String
  ) =
    m.computeIfAbsent(key, _ => new java.util.concurrent.atomic.AtomicInteger())

  def reset(absPath: String): Unit = {
    counter(openCounts, absPath).set(0)
    counter(statCounts, absPath).set(0)
  }
  def opens(absPath: String): Int = counter(openCounts, absPath).get()
  def getFileStatus(absPath: String): Int = counter(statCounts, absPath).get()

  private[parquet] def countOpen(absPath: String): Unit = counter(openCounts, absPath).incrementAndGet()
  private[parquet] def countStat(absPath: String): Unit = counter(statCounts, absPath).incrementAndGet()
}

class CountingLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
  import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, Path => HPath}

  override def getScheme: String = "countfs"
  override def getUri: java.net.URI = java.net.URI.create("countfs:///")

  private def key(p: HPath): String = pathToFile(p).getAbsolutePath

  // Count only calls made from inside a Spark TASK (an executor), which is where the node's openFile runs.
  // Spark's own driver-side planning (file listing, stats) also touches this file but runs with no
  // TaskContext, so it is excluded -- the assertions then measure the NODE's open path, not Spark's.
  private def inTask: Boolean = org.apache.spark.TaskContext.get() != null

  // RawLocalFileSystem.open() itself calls getFileStatus() for an existence check; that is an artifact of
  // this FS, not a HEAD the node issues. We count only STANDALONE getFileStatus (the node's one HEAD per
  // split), by suppressing the count while inside our own open().
  private val insideOpen = new ThreadLocal[java.lang.Boolean] {
    override def initialValue(): java.lang.Boolean = java.lang.Boolean.FALSE
  }

  override def open(f: HPath, bufferSize: Int): FSDataInputStream = {
    if (inTask) CountingLocalFileSystem.countOpen(key(f))
    insideOpen.set(java.lang.Boolean.TRUE)
    try super.open(f, bufferSize)
    finally insideOpen.set(java.lang.Boolean.FALSE)
  }

  override def getFileStatus(f: HPath): FileStatus = {
    if (inTask && !insideOpen.get()) CountingLocalFileSystem.countStat(key(f))
    super.getFileStatus(f)
  }
}
