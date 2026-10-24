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

/*
 * Ported from Apache Iceberg (Apache License 2.0):
 *   spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/source/TestFilteredScan.java
 *   at git tag `apache-iceberg-1.12.0`
 *   https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/spark/src/test/java/
 *     org/apache/iceberg/spark/source/TestFilteredScan.java
 * Apache Iceberg, Copyright The Apache Software Foundation; see the repository NOTICE.
 *
 * WHAT THIS PORTS. The upstream suite builds a 10-row table (id 0..9, ts, data) under several partition
 * specs and pushes id / timestamp / startsWith filters, asserting the surviving rows and, for the
 * partition specs, how many file splits the planner leaves. Here each scenario is a READ against
 * vecruntime's native Iceberg scan (`spark.vecruntime.scan.nativeIceberg.enabled=true`): we build the
 * SAME 10 rows, run the SAME filter as Spark SQL, and assert
 *   (1) the EXPLICIT expected ids the upstream asserts (pinned, not "ours == Iceberg's reader"), and
 *   (2) that `VectorIcebergScanExec` ran with NO task group handed to Iceberg's reader
 *       (`numIcebergGroups == 0`), i.e. the predicate pushdown happens on our scan, no silent fallback.
 * For the partitioned specs we also assert the number of partition splits the plan leaves, which is the
 * pruning upstream checks (identical under our scan, since Iceberg does the planning either way).
 *
 * Upstream cases ported (upstream name -> our test):
 *   testUnpartitionedIDFilters / ...CaseInsensitiveIDFilters -> "id equality filters over the unpartitioned ..."
 *   testUnpartitionedTimestampFilter                         -> "a timestamp < filter returns the later ids ..."
 *   testFilterByNonProjectedColumn                           -> "a filter on a column the query does not project"
 *   testBucketPartitionedIDFilters                           -> "id equality over a bucket(id) table prunes ..."
 *   testDayPartitionedTimestampFilters / ...Hour...          -> "a day/hour partitioned ts filter prunes ..."
 *   testPartitionedByDataStartsWith / NotStartsWith          -> "startsWith / not-startsWith over identity(data)"
 *   testUnpartitionedStartsWith / NotStartsWith              -> "LIKE 'jun%' / NOT LIKE over the unpartitioned ..."
 *
 * Runtime note: the repo's iceberg-spark-runtime reports "Apache Iceberg 1.12.0" at run time, matching
 * the ported tag. Data is loaded with SQL INSERT (one data file via coalesce(1) for the unpartitioned
 * table) rather than the low-level GenericFileWriterFactory the upstream uses.
 */
package io.vecruntime.iceberg.ported

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TestFilteredScanPortedSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("filtered-scan-port")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("filtered-scan-port")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.plugins", "io.vecruntime.spark.VectorPlugin")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.resolve("ice").toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def nodes(p: SparkPlan): Seq[SparkPlan] = {
    val nested = p.getClass.getSimpleName match {
      case "AdaptiveSparkPlanExec" =>
        p.getClass.getMethods.find(m => m.getName == "executedPlan" && m.getParameterCount == 0)
          .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
      case n if n.endsWith("QueryStageExec") =>
        p.getClass.getMethods.find(m => m.getName == "plan" && m.getParameterCount == 0)
          .map(_.invoke(p).asInstanceOf[SparkPlan]).toSeq
      case _ => Seq.empty
    }
    p +: (p.children ++ nested).flatMap(nodes)
  }

  /** Collect `q`, asserting a VectorIcebergScanExec ran and handed no group to Iceberg's reader. */
  private def nativeCollect(q: String): Seq[Row] = {
    val df: DataFrame = spark.sql(q)
    val got = df.collect().toSeq
    val scans = nodes(df.queryExecution.executedPlan).filter(_.getClass.getSimpleName == "VectorIcebergScanExec")
    assert(scans.nonEmpty, s"not the native scan for [$q]:\n${df.queryExecution.executedPlan}")
    assert(scans.map(_.metrics("numIcebergGroups").value).sum == 0, s"a group went to Iceberg's reader for [$q]")
    got
  }

  private def withConf[T](kv: (String, String))(f: => T): T = {
    val prev = spark.conf.getOption(kv._1)
    spark.conf.set(kv._1, kv._2)
    try f
    finally prev match { case Some(v) => spark.conf.set(kv._1, v); case None => spark.conf.unset(kv._1) }
  }

  private def icebergIds(q: String): Seq[Long] =
    withConf("spark.vecruntime.enabled" -> "false")(spark.sql(q).collect().toSeq).map(_.getLong(0)).sorted

  private def ids(rows: Seq[Row]): Seq[Long] = rows.map(_.getLong(0)).sorted

  /** The number of data files the plan actually reads (after partition pruning). */
  private def numFiles(q: String): Long = {
    val df = spark.sql(q)
    df.collect()
    nodes(df.queryExecution.executedPlan)
      .filter(_.getClass.getSimpleName == "VectorIcebergScanExec")
      .map(_.metrics("numFiles").value).sum
  }

  // id, ts (UTC), data -- exactly the upstream testRecords.
  private val ROWS = Seq(
    (0L, "2017-12-22 09:20:44.294658", "junction"),
    (1L, "2017-12-22 07:15:34.582910", "alligator"),
    (2L, "2017-12-22 06:02:09.243857", ""),
    (3L, "2017-12-22 03:10:11.134509", "clapping"),
    (4L, "2017-12-22 00:34:00.184671", "brush"),
    (5L, "2017-12-21 22:20:08.935889", "trap"),
    (6L, "2017-12-21 21:55:30.589712", "element"),
    (7L, "2017-12-21 17:31:14.532797", "limited"),
    (8L, "2017-12-21 15:21:51.237521", "global"),
    (9L, "2017-12-21 15:02:15.230570", "goldfish")
  )

  private def values: String =
    ROWS.map { case (i, ts, d) => s"($i, timestamp'$ts', '${d.replace("'", "''")}')" }.mkString(", ")

  /** Build the table with the given partitioning clause ("" = unpartitioned), all 10 rows. */
  private def build(name: String, partition: String): String = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(s"CREATE TABLE $name (id BIGINT, ts TIMESTAMP, data STRING) USING iceberg $partition")
    val df = spark.sql(s"SELECT * FROM VALUES $values AS t(id, ts, data)")
    (if (partition.isEmpty) df.coalesce(1) else df).writeTo(name).append()
    name
  }

  // -------------------------------------------------------------------------------------------
  // testUnpartitionedIDFilters + testUnpartitionedCaseInsensitiveIDFilters
  test("id equality filters over the unpartitioned table return exactly that id, native scan ran") {
    val t = build("ice.db.fs_unpart", "")
    for (i <- 0 until 10) {
      val got = nativeCollect(s"SELECT id, ts, data FROM $t WHERE id = $i")
      assert(ids(got) == Seq(i.toLong), s"id=$i returned ${ids(got)}")
    }
    // case-insensitive (Spark default is case-insensitive): ID resolves to id
    val got = nativeCollect(s"SELECT id FROM $t WHERE ID = 7")
    assert(ids(got) == Seq(7L))
  }

  // testUnpartitionedTimestampFilter
  test("a timestamp < filter returns the later ids on the native scan") {
    val t = build("ice.db.fs_ts", "")
    val got = nativeCollect(s"SELECT id FROM $t WHERE ts < timestamp'2017-12-22 00:00:00'")
    assert(ids(got) == Seq(5L, 6L, 7L, 8L, 9L), s"got ${ids(got)}")
    assert(ids(got) == icebergIds(s"SELECT id FROM $t WHERE ts < timestamp'2017-12-22 00:00:00'"))
  }

  // testFilterByNonProjectedColumn
  test("a filter on a column the query does not project still applies, native scan ran") {
    val t = build("ice.db.fs_nonproj", "")
    // project only id/ts, filter on data
    val got = nativeCollect(s"SELECT id, ts FROM $t WHERE data = 'junction'")
    assert(ids(got) == Seq(0L))
    val got2 = nativeCollect(s"SELECT id FROM $t WHERE data = 'brush' OR data = 'trap'")
    assert(ids(got2) == Seq(4L, 5L))
  }

  // testBucketPartitionedIDFilters
  test("id equality over a bucket(id) table returns that id and prunes to one split, native scan ran") {
    val t = build("ice.db.fs_bucket", "PARTITIONED BY (bucket(4, id))")
    for (i <- 0 until 10) {
      val got = nativeCollect(s"SELECT id FROM $t WHERE id = $i")
      assert(ids(got) == Seq(i.toLong), s"id=$i returned ${ids(got)}")
    }
    // an equality on a bucketed column prunes to the single bucket partition (one split).
    assert(numFiles(s"SELECT id FROM $t WHERE id = 5") == 1L, "id equality should prune to one bucket split")
  }

  // testDayPartitionedTimestampFilters
  test("a day-partitioned ts filter prunes to the matching days, native scan ran") {
    val t = build("ice.db.fs_day", "PARTITIONED BY (days(ts))")
    // the rows span two UTC days: 2017-12-21 (ids 5..9) and 2017-12-22 (ids 0..4)
    val d22 = nativeCollect(s"SELECT id FROM $t WHERE ts >= timestamp'2017-12-22 00:00:00'")
    assert(ids(d22) == Seq(0L, 1L, 2L, 3L, 4L), s"got ${ids(d22)}")
    assert(numFiles(s"SELECT id FROM $t WHERE ts >= timestamp'2017-12-22 00:00:00'") == 1L, "one day split")
    val d21 = nativeCollect(s"SELECT id FROM $t WHERE ts < timestamp'2017-12-22 00:00:00'")
    assert(ids(d21) == Seq(5L, 6L, 7L, 8L, 9L), s"got ${ids(d21)}")
  }

  // testHourPartitionedTimestampFilters
  test("an hour-partitioned ts filter returns the matching rows, native scan ran") {
    val t = build("ice.db.fs_hour", "PARTITIONED BY (hours(ts))")
    // ids 8 and 9 both fall in the 2017-12-21 15:00 UTC hour
    val h15 = nativeCollect(
      s"SELECT id FROM $t WHERE ts >= timestamp'2017-12-21 15:00:00' AND ts < timestamp'2017-12-21 16:00:00'"
    )
    assert(ids(h15) == Seq(8L, 9L), s"got ${ids(h15)}")
  }

  // testPartitionedByDataStartsWithFilter / testPartitionedByDataNotStartsWithFilter
  test("startsWith / not-startsWith over an identity(data) table prunes correctly, native scan ran") {
    val t = build("ice.db.fs_iddata", "PARTITIONED BY (data)")
    val yes = nativeCollect(s"SELECT id, data FROM $t WHERE data LIKE 'junc%'")
    assert(yes.map(_.getString(1)).toSeq == Seq("junction"), s"got ${yes.map(_.getString(1)).toSeq}")
    assert(numFiles(s"SELECT id FROM $t WHERE data LIKE 'junc%'") == 1L, "startsWith prunes to one data partition")
    val no = nativeCollect(s"SELECT id FROM $t WHERE data NOT LIKE 'junc%'")
    // 10 rows, 10 distinct data values (one empty), minus 'junction' = 9 rows, and (upstream) 9 splits.
    assert(ids(no) == Seq(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), s"got ${ids(no)}")
    assert(numFiles(s"SELECT id FROM $t WHERE data NOT LIKE 'junc%'") == 9L, "not-startsWith keeps nine splits")
  }

  // testUnpartitionedStartsWith / testUnpartitionedNotStartsWith
  test("LIKE 'jun%' / NOT LIKE over the unpartitioned table, native scan ran") {
    val t = build("ice.db.fs_like", "")
    val yes = nativeCollect(s"SELECT data FROM $t WHERE data LIKE 'jun%'")
    assert(yes.map(_.getString(0)).toSeq == Seq("junction"), s"got ${yes.map(_.getString(0)).toSeq}")
    val no = nativeCollect(s"SELECT id FROM $t WHERE data NOT LIKE 'jun%'")
    assert(ids(no) == Seq(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), s"got ${ids(no)}")
  }
}
