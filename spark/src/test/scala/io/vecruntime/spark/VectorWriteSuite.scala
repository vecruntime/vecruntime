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

import io.vecruntime.spark.test.{TestTables, VectorQuerySuite}
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFallback, VectorWriteFilesExec}

/**
 * #64: the columnar Parquet writer against Spark's. Each write runs twice, through our writer (the flag on) and
 * through Spark's (the flag off, the plugin still on), into two directories. Both are read back by Spark's reader
 * with the plugin off; the rows, the Spark schema and the footer keys must agree, and the plan of the write must
 * hold `VectorWriteFilesExec`, or Spark's `WriteFilesExec` with the expected reason.
 */
@io.vecruntime.spark.test.tags.Write
class VectorWriteSuite extends VectorQuerySuite {

  @volatile private var lastWritePlan: SparkPlan = _

  private val listener = new QueryExecutionListener {
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
      if (PlanUtils.allNodes(qe.executedPlan).exists(_.nodeName.contains("Write"))) lastWritePlan = qe.executedPlan
    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestTables.createMixed(spark, newTempPath("write/t"))
    spark.listenerManager.register(listener)
  }

  override protected def afterAll(): Unit = {
    spark.listenerManager.unregister(listener)
    super.afterAll()
  }

  /** Every Spark type the writer takes, from the mixed table: nulls in most columns. */
  private def wide(s: SparkSession): DataFrame =
    s.table("t").selectExpr(
      "i",
      "l",
      "d",
      "d2",
      "dt",
      "b",
      "s",
      "cast(i % 100 as tinyint) as by",
      "cast(i % 30000 as smallint) as sh",
      "cast(d2 as float) as f",
      "cast(l as decimal(7,2)) as dec7",
      "cast(l * 1000 as decimal(15,2)) as dec15",
      "cast(l as decimal(30,6)) as dec30",
      "timestamp_micros(l * 1000003) as ts"
    )

  private def write(
      df: DataFrame,
      path: String,
      columnar: Boolean,
      options: Map[String, String] = Map.empty,
      partitionBy: Seq[String] = Nil
  ): SparkPlan = {
    lastWritePlan = null
    withConf(VectorConf.WriteColumnar -> columnar.toString) {
      df.write.mode("overwrite").options(options).partitionBy(partitionBy: _*).parquet(path)
    }
    org.apache.spark.sql.vecruntime.TestListenerSync.drain(spark)
    assert(lastWritePlan != null, "no write plan captured")
    lastWritePlan
  }

  private def readSorted(path: String): Array[Row] =
    withPlugin(enabled = false)(spark.read.parquet(path).collect()).sortBy(_.toString)

  private def footers(path: String): Seq[ParquetMetadata] = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val files = fs.listFiles(p, true)
    Iterator
      .continually(files)
      .takeWhile(_.hasNext)
      .map(_.next())
      .filter(_.getPath.getName.endsWith(".parquet"))
      .toSeq
      .map { st =>
        val r = ParquetFileReader.open(HadoopInputFile.fromStatus(st, spark.sparkContext.hadoopConfiguration))
        try r.getFooter
        finally r.close()
      }
  }

  /** The partition directories under `path`, relative to it, and how many rows each holds. */
  private def partitionDirs(path: String): Map[String, Long] = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val root = fs.makeQualified(p).toString
    footers(path).zip(parquetFiles(path)).groupMapReduce { case (_, f) =>
      f.getParent.toString.stripPrefix(root)
    } { case (m, _) => m.getBlocks.asScala.map(_.getRowCount).sum }(_ + _)
  }

  private def parquetFiles(path: String): Seq[Path] = {
    val p = new Path(path)
    val files = p.getFileSystem(spark.sparkContext.hadoopConfiguration).listFiles(p, true)
    Iterator.continually(files).takeWhile(_.hasNext).map(_.next().getPath).filter(_.getName.endsWith(".parquet")).toSeq
  }

  private def hasVectorWrite(plan: SparkPlan): Boolean =
    PlanUtils.allNodes(plan).exists(_.isInstanceOf[VectorWriteFilesExec])

  private def fallbackReasons(plan: SparkPlan): Seq[String] =
    PlanUtils.allNodes(plan).flatMap(VectorFallback.reason)

  /** Writes `df` both ways and compares; returns our plan and our files' footers. */
  private def compareWrites(
      name: String,
      df: SparkSession => DataFrame,
      options: Map[String, String] = Map.empty,
      partitionBy: Seq[String] = Nil
  ): (SparkPlan, Seq[ParquetMetadata]) = {
    val ours = newTempPath(s"write/$name-ours")
    val theirs = newTempPath(s"write/$name-spark")
    val plan = write(df(spark), ours, columnar = true, options, partitionBy)
    write(df(spark), theirs, columnar = false, options, partitionBy)
    assert(partitionDirs(ours) === partitionDirs(theirs), s"$name: partition directories")
    val a = readSorted(ours)
    val b = readSorted(theirs)
    assert(a.length === b.length, s"$name: row counts")
    assertRowsEqual(b, a, 0.0, name)
    assert(spark.read.parquet(ours).schema === spark.read.parquet(theirs).schema, s"$name: schemas")
    // Our own reader reads our files too.
    assertRowsEqual(b, withPlugin(enabled = true)(spark.read.parquet(ours).collect()).sortBy(_.toString), 0.0, name)
    val ourFooters = footers(ours)
    val sparkFooter = footers(theirs).head
    ourFooters.foreach { f =>
      val kv = f.getFileMetaData.getKeyValueMetaData.asScala
      val sk = sparkFooter.getFileMetaData.getKeyValueMetaData.asScala
      assert(kv.get("org.apache.spark.version") === sk.get("org.apache.spark.version"), s"$name: version key")
      assert(
        kv.get("org.apache.spark.sql.parquet.row.metadata") === sk.get("org.apache.spark.sql.parquet.row.metadata"),
        s"$name: Spark schema key"
      )
      assert(f.getFileMetaData.getSchema === sparkFooter.getFileMetaData.getSchema, s"$name: Parquet schema")
    }
    (plan, ourFooters)
  }

  test("an unpartitioned write of every supported type is columnar, Parquet v2, and reads back as Spark's") {
    val (plan, footersOurs) = compareWrites("wide", wide)
    assert(hasVectorWrite(plan), s"expected VectorWriteFilesExec:\n${plan.treeString}\n${fallbackReasons(plan)}")
    val encodings =
      footersOurs.flatMap(_.getBlocks.asScala).flatMap(_.getColumns.asScala).flatMap(_.getEncodings.asScala).toSet
    assert(encodings.contains(Encoding.RLE_DICTIONARY), encodings)
    assert(!encodings.contains(Encoding.valueOf("PLAIN_DICTIONARY")), s"v1 dictionary pages in $encodings")
  }

  test("binary columns read from Parquet are written as BINARY and read back as Spark's") {
    val src = newTempPath("write/bin-src")
    withPlugin(enabled = false)(spark.table("t").selectExpr("i", "cast(s as binary) as bin").write.parquet(src))
    val (plan, _) = compareWrites("binary", s => s.read.parquet(src))
    assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
  }

  test("compression codecs: zstd, snappy and none read back as Spark's") {
    Seq("zstd", "snappy", "uncompressed").foreach { codec =>
      val (plan, _) = compareWrites(s"codec-$codec", s => s.table("t"), Map("compression" -> codec))
      assert(hasVectorWrite(plan), codec)
    }
  }

  test("a filtered and projected input: selection vectors and computed columns") {
    val (plan, _) =
      compareWrites(
        "filtered",
        s => s.table("t").where("i % 3 = 1 AND s IS NOT NULL").selectExpr("i * 2 AS j", "upper(s) AS u", "l")
      )
    assert(hasVectorWrite(plan))
  }

  test("maxRecordsPerFile splits batches between files with every row written once") {
    val ours = newTempPath("write/max-ours")
    val plan = write(spark.table("t"), ours, columnar = true, Map("maxRecordsPerFile" -> "1500"))
    assert(hasVectorWrite(plan))
    val files = footers(ours)
    assert(files.forall(_.getBlocks.asScala.map(_.getRowCount).sum <= 1500))
    assert(files.map(_.getBlocks.asScala.map(_.getRowCount).sum).sum === 20000)
    assertRowsEqual(
      withPlugin(enabled = false)(spark.table("t").collect()).sortBy(_.toString),
      readSorted(ours),
      0.0,
      "max"
    )
  }

  test("an empty input writes one file holding the schema") {
    val ours = newTempPath("write/empty-ours")
    write(spark.table("t").where("i < 0"), ours, columnar = true)
    val files = footers(ours)
    assert(files.size === 1)
    assert(readSorted(ours).isEmpty)
    assert(spark.read.parquet(ours).schema === spark.table("t").schema)
  }

  test("CREATE TABLE AS SELECT is columnar and the table reads back as Spark's") {
    withConf(VectorConf.WriteColumnar -> "true") {
      spark.sql("DROP TABLE IF EXISTS ctas_ours")
      lastWritePlan = null
      spark.sql("CREATE TABLE ctas_ours USING parquet AS SELECT i, s, l FROM t WHERE i % 2 = 0")
      org.apache.spark.sql.vecruntime.TestListenerSync.drain(spark)
    }
    assert(lastWritePlan != null && hasVectorWrite(lastWritePlan), Option(lastWritePlan).map(_.treeString))
    val expected = withPlugin(enabled = false)(spark.sql("SELECT i, s, l FROM t WHERE i % 2 = 0").collect())
    assertRowsEqual(
      expected.sortBy(_.toString),
      withPlugin(enabled = false)(spark.table("ctas_ours").collect()).sortBy(_.toString),
      0.0,
      "ctas"
    )
    spark.sql("DROP TABLE ctas_ours")
  }

  test("partitioned writes: the same directories, row counts and rows as Spark's") {
    // A boolean, then a date and a string with nulls (`__HIVE_DEFAULT_PARTITION__`), escaped characters.
    val (p1, _) = compareWrites("part-b", s => s.table("t"), partitionBy = Seq("b"))
    assert(hasVectorWrite(p1), s"${p1.treeString}\n${fallbackReasons(p1)}")
    val (p2, _) = compareWrites(
      "part-dt-s",
      s =>
        s.table("t").selectExpr(
          "i",
          "l",
          "d",
          "date_add(dt, i % 3) as day",
          "if(s is null, null, if(i % 7 = 0, '', concat(s, '/x=%'))) as k"
        ),
      partitionBy = Seq("day", "k")
    )
    assert(hasVectorWrite(p2), s"${p2.treeString}\n${fallbackReasons(p2)}")
  }

  test("partitioned writes roll files at maxRecordsPerFile within a partition") {
    val (plan, files) = compareWrites(
      "part-max",
      s => s.table("t"),
      options = Map("maxRecordsPerFile" -> "700"),
      partitionBy = Seq("b")
    )
    assert(hasVectorWrite(plan))
    assert(files.forall(_.getBlocks.asScala.map(_.getRowCount).sum <= 700))
  }

  test("bucketed tables: each bucket file holds the rows Spark's bucket file holds") {
    def bucketed(table: String, columnar: Boolean, partitioned: Boolean): SparkPlan = {
      lastWritePlan = null
      withConf(VectorConf.WriteColumnar -> columnar.toString) {
        spark.sql(s"DROP TABLE IF EXISTS $table")
        val w = spark.table("t").selectExpr("i", "l", "s", "d2", "b").write.bucketBy(5, "s", "i")
        (if (partitioned) w.partitionBy("b") else w).format("parquet").saveAsTable(table)
      }
      org.apache.spark.sql.vecruntime.TestListenerSync.drain(spark)
      lastWritePlan
    }
    def byBucket(table: String): Map[(String, Int), Seq[String]] = {
      val location =
        spark.sessionState.catalog.getTableMetadata(spark.sessionState.sqlParser.parseTableIdentifier(table)).location
      val root = location.toString
      parquetFiles(root).groupBy { f =>
        (
          f.getParent.toString.stripPrefix(new Path(root).toString),
          org.apache.spark.sql.execution.datasources.BucketingUtils.getBucketId(f.getName).get
        )
      }.map { case (k, fs) =>
        k -> withPlugin(enabled =
          false
        )(spark.read.parquet(fs.map(_.toString): _*).collect()).map(_.toString).toSeq.sorted
      }
    }
    Seq(false, true).foreach { partitioned =>
      val plan = bucketed("bk_ours", columnar = true, partitioned)
      assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
      bucketed("bk_spark", columnar = false, partitioned)
      val ours = byBucket("bk_ours")
      val theirs = byBucket("bk_spark")
      assert(ours.keySet === theirs.keySet, s"partitioned=$partitioned: bucket files")
      ours.foreach { case (k, rows) => assert(rows === theirs(k), s"partitioned=$partitioned: bucket $k") }
      // A bucketed join reads our table without a shuffle on its side, as Spark's.
      val joined = withPlugin(enabled =
        false
      )(spark.sql("SELECT count(*) FROM bk_ours a JOIN bk_spark b ON a.s = b.s AND a.i = b.i").collect())
      assert(joined.head.getLong(0) === withPlugin(enabled =
        false
      )(spark.sql("SELECT count(*) FROM t WHERE s IS NOT NULL").collect()).head.getLong(0))
    }
    spark.sql("DROP TABLE bk_ours")
    spark.sql("DROP TABLE bk_spark")
  }

  test("empty2null and collation_aware_hash run in our projection, as Spark computes them") {
    import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
    import org.apache.spark.sql.catalyst.expressions.{CollationAwareMurmur3Hash, Empty2Null}
    import org.apache.spark.sql.vecruntime.TestExprs.column
    // The collated column comes from a file, so the projection over the scan is ours.
    val src = newTempPath("write/exprs-src")
    withPlugin(enabled = false) {
      spark
        .table("t")
        .selectExpr(
          "i",
          "l",
          "dt",
          "if(i % 5 = 0, '', s) as e",
          "if(i % 2 = 0, upper(s), s) collate UTF8_LCASE as c",
          "s"
        )
        .write
        .parquet(src)
    }
    def query(s: SparkSession): DataFrame =
      s.read
        .parquet(src)
        .select(
          column(Empty2Null(UnresolvedAttribute("e"))).as("e2n"),
          column(CollationAwareMurmur3Hash(Seq(UnresolvedAttribute("s"), UnresolvedAttribute("i")), 42)).as("h1"),
          column(
            CollationAwareMurmur3Hash(
              Seq(UnresolvedAttribute("c"), UnresolvedAttribute("l"), UnresolvedAttribute("dt")),
              42
            )
          ).as("h2"),
          column(CollationAwareMurmur3Hash(Seq(UnresolvedAttribute("c")), 42)).as("h3")
        )
    val expected = withPlugin(enabled = false)(query(spark).collect())
    val df = withPlugin(enabled = true) { val d = query(spark); d.collect(); d }
    assertRowsEqual(expected, withPlugin(enabled = true)(df.collect()), 0.0, "empty2null / collation_aware_hash")
    assert(expected.exists(_.isNullAt(0)) && expected.exists(r => !r.isNullAt(0)))
    val project = nodesOf[org.apache.spark.sql.vecruntime.VectorProjectExec](df)
    assert(project.nonEmpty, s"${finalPlan(df).treeString}\n${VectorFallback.reasons(finalPlan(df)).map(_._2)}")
  }

  test("concurrent output writers: unsorted input, and the sorted rest past the writer limit, as Spark writes it") {
    // 4 writers for 7 partitions x 3 buckets (21 files per task): each task opens 4, then sorts the rest.
    // 64 writers: every task stays concurrent.
    Seq("4", "64").foreach { max =>
      withConf("spark.sql.maxConcurrentOutputFileWriters" -> max) {
        val (p1, _) = compareWrites(
          s"conc-part-$max",
          s => s.table("t").selectExpr("i", "l", "s", "i % 7 as p"),
          partitionBy = Seq("p")
        )
        assert(hasVectorWrite(p1), s"${p1.treeString}\n${fallbackReasons(p1)}")
        val (p2, _) = compareWrites(
          s"conc-max-$max",
          s => s.table("t").selectExpr("i", "l", "s", "i % 7 as p"),
          options = Map("maxRecordsPerFile" -> "300"),
          partitionBy = Seq("p")
        )
        assert(hasVectorWrite(p2))
        def bucketed(table: String, columnar: Boolean): SparkPlan = {
          lastWritePlan = null
          withConf(VectorConf.WriteColumnar -> columnar.toString) {
            spark.sql(s"DROP TABLE IF EXISTS $table")
            spark.table("t").selectExpr("i", "l", "s", "i % 7 as p").write.partitionBy("p").bucketBy(3, "i")
              .format("parquet").saveAsTable(table)
          }
          org.apache.spark.sql.vecruntime.TestListenerSync.drain(spark)
          lastWritePlan
        }
        val plan = bucketed("cbk_ours", columnar = true)
        assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
        bucketed("cbk_spark", columnar = false)
        assertRowsEqual(
          withPlugin(enabled = false)(spark.table("cbk_spark").collect()).sortBy(_.toString),
          withPlugin(enabled = false)(spark.table("cbk_ours").collect()).sortBy(_.toString),
          0.0,
          s"concurrent bucketed $max"
        )
        // Every file of ours holds one bucket's rows, as Spark's files do.
        val location = spark.sessionState.catalog.getTableMetadata(
          spark.sessionState.sqlParser.parseTableIdentifier("cbk_ours")
        ).location
        parquetFiles(location.toString).foreach { f =>
          val id = org.apache.spark.sql.execution.datasources.BucketingUtils.getBucketId(f.getName).get
          val ids = withPlugin(enabled =
            false
          )(spark.read.parquet(f.toString).selectExpr("pmod(hash(i), 3)").collect()).map(_.getInt(0)).toSet
          assert(ids.subsetOf(Set(id)), s"$f holds buckets $ids")
        }
        spark.sql("DROP TABLE cbk_ours")
        spark.sql("DROP TABLE cbk_spark")
      }
    }
  }

  test("structs, arrays and maps (#842): columnar, the same Parquet schema and rows as Spark's") {
    // Nulls at every level, empty arrays and maps, a list of structs holding a list, every leaf type the flat
    // writer takes (INT96 timestamps, the three decimal widths, float, binary), and a filter (a selection).
    val nested = (s: SparkSession) =>
      wide(s).selectExpr(
        "i",
        "IF(i % 9 = 0, NULL, named_struct('a', i, 'b', s, 'ts', ts, 'dec7', dec7, 'dec30', dec30)) AS st",
        "CASE WHEN i % 5 = 0 THEN NULL WHEN i % 5 = 1 THEN array() ELSE array(l, NULL, l + 1) END AS arr",
        "CASE WHEN i % 7 = 0 THEN NULL WHEN i % 7 = 1 THEN map() ELSE map(coalesce(s, 'x'), dec15, 'k', NULL) END AS m",
        "array(named_struct('x', by, 'ys', IF(i % 4 = 0, NULL, array(s, cast(b as string))), 'f', f), NULL) AS deep",
        "array(array(sh), array(), NULL) AS aa",
        "map(i, named_struct('d', dt, 'bin', cast(s as binary))) AS ms"
      )
    // Written once by Spark, then read by our scan (nested columns) and written by both writers.
    val src = newTempPath("write/nested-src")
    withPlugin(enabled = false)(nested(spark).write.mode("overwrite").parquet(src))
    val (plan, footers) = compareWrites("nested", s => s.read.parquet(src).where("i % 11 <> 3"))
    assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
    assert(footers.head.getFileMetaData.getSchema.getColumns.asScala.exists(_.getMaxRepetitionLevel == 2))
    // Partitioned: the runs of a batch, cut at partition boundaries, slice the nested lanes too.
    val (pp, _) = compareWrites(
      "nested-part",
      s => s.read.parquet(src).selectExpr("*", "i % 3 AS pk"),
      partitionBy = Seq("pk")
    )
    assert(hasVectorWrite(pp), s"${pp.treeString}\n${fallbackReasons(pp)}")
  }

  test("writeLegacyFormat nested: bag/array lists, key_value maps, structs of lists -- Spark's legacy schema") {
    // Legacy layout: lists as group (LIST){ repeated group bag { optional elem array } }, maps as
    // (MAP){ repeated group key_value ... }, with nulls at every level, empty collections and nesting at depth.
    val nested = (s: SparkSession) =>
      wide(s).selectExpr(
        "i",
        "IF(i % 9 = 0, NULL, named_struct('a', i, 'b', s, 'ts', ts)) AS st",
        "CASE WHEN i % 5 = 0 THEN NULL WHEN i % 5 = 1 THEN array() ELSE array(l, NULL, l + 1) END AS arr",
        "CASE WHEN i % 7 = 0 THEN NULL WHEN i % 7 = 1 THEN map() ELSE map(coalesce(s, 'x'), l, 'k', NULL) END AS m",
        // A struct holding a list, a list of structs holding a list (depth), and a list of lists.
        "named_struct('n', by, 'ys', IF(i % 4 = 0, NULL, array(s, cast(b as string)))) AS so",
        "array(named_struct('x', sh, 'ys', IF(i % 6 = 0, NULL, array(l))), NULL) AS deep",
        "array(array(sh), array(), NULL) AS aa",
        "map(i, named_struct('d', dt, 's', s)) AS ms"
      )
    // The source is written in legacy format too, so our scan reads a legacy-layout file and our writer re-emits it.
    val src = newTempPath("write/legacy-nested-src")
    withConf("spark.sql.parquet.writeLegacyFormat" -> "true") {
      withPlugin(enabled = false)(nested(spark).write.mode("overwrite").parquet(src))
      val (plan, footers) = compareWrites("legacy-nested", s => s.read.parquet(src).where("i % 11 <> 3"))
      assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
      // The legacy layout is present: a repeated group named `bag` (lists) and `key_value` (maps).
      val groups = footers.head.getFileMetaData.getSchema.getColumns.asScala.flatMap(_.getPath.toSeq).toSet
      assert(groups.contains("bag"), s"legacy list layout (bag) missing: $groups")
      assert(groups.contains("key_value"), s"legacy map layout missing: $groups")
      // Partitioned: runs cut at partition boundaries slice the legacy nested lanes too.
      val (pp, _) = compareWrites(
        "legacy-nested-part",
        s => s.read.parquet(src).selectExpr("*", "i % 3 AS pk"),
        partitionBy = Seq("pk")
      )
      assert(hasVectorWrite(pp), s"${pp.treeString}\n${fallbackReasons(pp)}")
    }
  }

  test("writeLegacyFormat decimals: every precision written as FIXED_LEN_BYTE_ARRAY, as Spark's legacy writer") {
    // Precisions 1, 9, 10, 18, 19 and 38: all FLBA in legacy format (p<=18 on the INT64 lane, p>18 DECIMAL128).
    val decs = (s: SparkSession) =>
      s.table("t").selectExpr(
        "i",
        "cast(i % 9 as decimal(1,0)) as d1",
        "cast(l as decimal(9,2)) as d9",
        "cast(l as decimal(10,3)) as d10",
        "cast(l * 1000 as decimal(18,4)) as d18",
        "cast(l * 1000 as decimal(19,4)) as d19",
        "cast(l as decimal(38,10)) as d38"
      )
    withConf("spark.sql.parquet.writeLegacyFormat" -> "true") {
      val (plan, footers) = compareWrites("legacy-dec", decs)
      assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
      import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
      val cols = footers.head.getFileMetaData.getSchema.getColumns.asScala
      cols.filter(_.getPath()(0).startsWith("d")).foreach(c =>
        assert(c.getPrimitiveType.getPrimitiveTypeName == FIXED_LEN_BYTE_ARRAY, s"${c.getPath()(0)} not FLBA")
      )
    }
  }

  test("write calendar rebase LEGACY: dates and timestamps rebased and the legacy metadata keys written") {
    // Ancient and modern dates and timestamps, INT96 and INT64 (MICROS and MILLIS) timestamps.
    val dates = (s: SparkSession) =>
      s.range(0, 4000).selectExpr(
        "cast(id as int) as i",
        "case when id % 13 = 0 then null else date_add(date'1000-01-01', cast(id * 211 as int)) end as d",
        "case when id % 11 = 0 then null else timestamp_micros(cast((id - 3000) * 86400000003 as bigint)) end as ts"
      )
    Seq("TIMESTAMP_MICROS", "TIMESTAMP_MILLIS", "INT96").foreach { tsType =>
      withConf(
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "LEGACY",
        "spark.sql.parquet.int96RebaseModeInWrite" -> "LEGACY",
        "spark.sql.parquet.outputTimestampType" -> tsType
      ) {
        val (plan, footers) = compareWrites(s"rebase-legacy-$tsType", dates)
        assert(hasVectorWrite(plan), s"$tsType: ${plan.treeString}\n${fallbackReasons(plan)}")
        val kv = footers.head.getFileMetaData.getKeyValueMetaData
        assert(kv.containsKey("org.apache.spark.legacyDateTime"), s"$tsType: missing legacyDateTime key")
        assert(kv.containsKey("org.apache.spark.timeZone"), s"$tsType: missing timeZone key")
        if (tsType == "INT96") {
          assert(kv.containsKey("org.apache.spark.legacyINT96"), "missing legacyINT96 key for INT96 output")
        }
      }
    }
  }

  test("write calendar rebase EXCEPTION: an ancient date or timestamp fails as Spark's write fails") {
    def ancient(s: SparkSession): DataFrame =
      s.range(0, 10).selectExpr("cast(id as int) as i", "date'1000-01-01' as d")
    withConf("spark.sql.parquet.datetimeRebaseModeInWrite" -> "EXCEPTION") {
      val p = newTempPath("write/rebase-exc")
      val ex = intercept[Exception](write(ancient(spark), p, columnar = true))
      def causes(t: Throwable): List[Throwable] = if (t == null) Nil else t :: causes(t.getCause)
      assert(
        causes(ex).exists(_.getClass.getSimpleName == "SparkUpgradeException"),
        s"expected a SparkUpgradeException in the cause chain, got ${causes(ex).map(_.getClass.getName)}"
      )
    }
    // A modern-only date under EXCEPTION writes fine and matches Spark.
    val modern = (s: SparkSession) =>
      s.range(0, 1000).selectExpr("cast(id as int) as i", "date_add(date'2020-01-01', cast(id % 365 as int)) as d")
    withConf("spark.sql.parquet.datetimeRebaseModeInWrite" -> "EXCEPTION") {
      val (plan, _) = compareWrites("rebase-exc-modern", modern)
      assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
    }
  }

  test("parquet.enable.dictionary#<column>: the column named is written without a dictionary, as Spark writes it") {
    val (plan, ours) =
      compareWrites(
        "dict-col",
        s => s.table("t").selectExpr("i % 10 AS k", "s", "l"),
        Map("parquet.enable.dictionary#s" -> "false")
      )
    assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
    // Spark's writer writes v1 by default, so the encodings differ; which columns use a dictionary does not.
    def dictionary(f: ParquetMetadata): Map[String, Boolean] =
      f.getBlocks.get(0).getColumns.asScala.map(c =>
        c.getPath.toDotString -> c.getEncodings.asScala.exists(_.usesDictionary())
      ).toMap
    val theirs = dictionary(footers(newTempPath("write/dict-col-spark")).head)
    assert(dictionary(ours.head) === theirs)
    assert(!theirs("s") && theirs("k"), theirs)
  }

  test("Bloom filters (parquet.bloom.filter.*#<column>): the same filter per chunk as Spark's writer") {
    val opts = Map(
      "parquet.bloom.filter.enabled#l" -> "true",
      "parquet.bloom.filter.expected.ndv#l" -> "5000",
      "parquet.bloom.filter.enabled#i" -> "true"
    )
    val (plan, ours) = compareWrites("bloom", s => s.table("t").select("i", "s", "l"), opts)
    assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
    def filters(dir: String): Seq[(String, org.apache.parquet.column.values.bloomfilter.BloomFilter)] =
      parquetFiles(dir).sortBy(_.getName).flatMap { p =>
        val r = org.apache.parquet.hadoop.ParquetFileReader
          .open(org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(p, spark.sparkContext.hadoopConfiguration))
        try r.getFooter.getBlocks.asScala.flatMap { b =>
            val bf = r.getBloomFilterDataReader(b)
            b.getColumns.asScala.map(c => c.getPath.toDotString -> bf.readBloomFilter(c))
          }.toSeq
        finally r.close()
      }
    val a = filters(newTempPath("write/bloom-ours"))
    val b = filters(newTempPath("write/bloom-spark"))
    assert(a.map(_._1) === b.map(_._1))
    a.zip(b).foreach { case ((c, x), (_, y)) => assert(x === y, c) }
    assert(a.exists { case (c, f) => c == "l" && f != null } && a.forall { case (c, f) => c != "s" || f == null })
    assert(ours.nonEmpty)
  }

  test("spark.vecruntime.write.columnar.fromRows: a write of rows goes through RowToColumnarExec to our writer") {

    val rows =
      (s: SparkSession) => s.createDataFrame((1 to 2000).map(i => (i, s"v${i % 7}", i.toLong * 3))).toDF("i", "s", "l")
    withConf(VectorConf.WriteColumnarFromRows -> "true") {
      val (plan, _) = compareWrites("from-rows", rows)
      assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
    }
    val (plain, _) = compareWrites("from-rows-off", rows)
    assert(!hasVectorWrite(plain) && fallbackReasons(plain).exists(_.contains("is not columnar")))
  }

  test("field ids (spark.sql.parquet.fieldId.write.enabled): the same Parquet schema, ids included, as Spark's") {
    def id(n: Int) = new org.apache.spark.sql.types.MetadataBuilder().putLong("parquet.field.id", n).build()
    withConf("spark.sql.parquet.fieldId.write.enabled" -> "true") {
      val (plan, footers) = compareWrites(
        "field-ids",
        s =>
          s.table("t").select(
            org.apache.spark.sql.functions.col("i").as("i", id(1)),
            org.apache.spark.sql.functions.col("s").as("s", id(7)),
            org.apache.spark.sql.functions.col("l")
          )
      )
      assert(hasVectorWrite(plan), s"${plan.treeString}\n${fallbackReasons(plan)}")
      val fields = footers.head.getFileMetaData.getSchema.getFields.asScala
      assert(fields.map(f => Option(f.getId).map(_.intValue())) === Seq(Some(1), Some(7), None))
    }
  }

  test("writes the prototype does not take keep Spark's writer, with the reason") {
    def reasonsOf(f: String => Unit): Seq[String] = {
      lastWritePlan = null
      withConf(VectorConf.WriteColumnar -> "true")(f(newTempPath(s"write/fb-${System.nanoTime()}")))
      org.apache.spark.sql.vecruntime.TestListenerSync.drain(spark)
      assert(!hasVectorWrite(lastWritePlan))
      fallbackReasons(lastWritePlan)
    }
    assert(
      reasonsOf(p => spark.table("t").write.option("parquet.writer.version", "v1").parquet(p))
        .exists(_.contains("writes Parquet v2"))
    )
    assert(reasonsOf(p => spark.table("t").write.orc(p)).exists(_.contains("is not Parquet")))
  }
}
