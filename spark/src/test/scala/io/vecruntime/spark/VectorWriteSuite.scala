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
          "if(s is null, null, concat(s, '/x=%')) as k"
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

  test("writes the prototype does not take keep Spark's writer, with the reason") {
    def reasonsOf(f: String => Unit): Seq[String] = {
      lastWritePlan = null
      withConf(VectorConf.WriteColumnar -> "true")(f(newTempPath(s"write/fb-${System.nanoTime()}")))
      org.apache.spark.sql.vecruntime.TestListenerSync.drain(spark)
      assert(!hasVectorWrite(lastWritePlan))
      fallbackReasons(lastWritePlan)
    }
    withConf("spark.sql.maxConcurrentOutputFileWriters" -> "4") {
      assert(
        reasonsOf(p => spark.table("t").write.partitionBy("b").parquet(p)).exists(_.contains(
          "maxConcurrentOutputFileWriters"
        ))
      )
    }
    assert(
      reasonsOf(p => spark.table("t").write.option("parquet.writer.version", "v1").parquet(p))
        .exists(_.contains("writes Parquet v2"))
    )
    assert(
      reasonsOf(p => spark.table("t").selectExpr("i", "named_struct('a', i, 'b', s) AS st").write.parquet(p))
        .exists(_.contains("not supported yet"))
    )
    assert(reasonsOf(p => spark.table("t").write.orc(p)).exists(_.contains("is not Parquet")))
  }
}
