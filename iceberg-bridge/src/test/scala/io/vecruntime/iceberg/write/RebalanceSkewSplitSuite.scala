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
package io.vecruntime.iceberg.write

import java.nio.file.{Files, Path}

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression guard for Iceberg 1.12's TestCopyOnWrite/MergeOnRead {Delete,Update,Merge}#testSkew*:
 * a row-level command with a tiny Iceberg advisory partition size (100) and
 * `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=true` expects AQE's
 * OptimizeSkewInRebalancePartitions to split the skewed REBALANCE_PARTITIONS_BY_COL reduce partition
 * into PartialReducerPartitionSpec reads so the write produces 4 files (4 one-file scan tasks over 2
 * reducers). This exercises that our columnar [[org.apache.spark.sql.vecruntime.VectorShuffleExchangeExec]]
 * takes part in that split: it is a `ShuffleExchangeLike` that preserves the REBALANCE `shuffleOrigin`
 * (so `AQEShuffleReadRule.isSupported` matches it), honours the user's Iceberg advisory size rather than
 * scaling it, and its `ShuffledColumnarRDD` reads `PartialReducerPartitionSpec` through the shuffle
 * manager's multi-mapper reader.
 *
 * Each scenario runs with our write/scan/shuffle features OFF (oracle = Spark + Iceberg) and ON, and
 * asserts both produce 4 files (`added-data-files` for copy-on-write, `added-delete-files` for
 * merge-on-read), exactly as the upstream tests assert.
 *
 * Runs in the iceberg-bridge module (excluded from the gate's -pl list), so it is run manually.
 */
class RebalanceSkewSplitSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("skewrepro")

  private val FeatureKeys = Seq(
    "spark.vecruntime.iceberg.writer.enabled",
    "spark.vecruntime.iceberg.dvWriter.enabled",
    "spark.vecruntime.iceberg.positionDeletes.enabled",
    "spark.vecruntime.iceberg.copyUnchanged.enabled",
    "spark.vecruntime.scan.nativeIceberg.enabled",
    "spark.vecruntime.shuffle.enabled"
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("rebalance-skew-repro")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def withFeatures[T](on: Boolean)(f: => T): T = {
    val prev = FeatureKeys.map(k => k -> spark.conf.getOption(k))
    FeatureKeys.foreach(k => spark.conf.set(k, on.toString))
    try f
    finally prev.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  private def withSQLConf[T](kv: (String, String)*)(f: => T): T = {
    val prev = kv.map { case (k, _) => k -> spark.conf.getOption(k) }
    kv.foreach { case (k, v) => spark.conf.set(k, v) }
    try f
    finally prev.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  /** The DELETE/UPDATE distribution-mode property name; MERGE uses write.merge.distribution-mode. */
  private def seed(t: String, mode: String, cmd: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(s"CREATE TABLE $t (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)")
    (0 until 4).foreach(_ =>
      spark.sql(s"INSERT INTO $t SELECT cast(id as int) id, 'hr' dep FROM range(0, 100)")
    )
    val modeKey = s"write.$cmd.mode"
    val distKey = s"write.$cmd.distribution-mode"
    spark.sql(
      s"ALTER TABLE $t SET TBLPROPERTIES (" +
        "'read.split.open-file-cost'='" + Int.MaxValue + "', " +
        s"'$modeKey'='$mode', '$distKey'='hash')"
    )
  }

  private def skewConf: Seq[(String, String)] = Seq(
    "spark.sql.shuffle.partitions" -> "2",
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled" -> "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "256MB",
    "spark.sql.iceberg.advisory-partition-size" -> "100"
  )

  private def mergeSrc(): Unit =
    spark.sql("SELECT cast(id as int) id, 'hr' dep FROM range(0, 100)").createOrReplaceTempView("skew_src")

  /** Runs one skew command on `t` and returns (added-data-files, added-delete-files). */
  private def run(t: String, cmd: String): (String, String) = {
    withSQLConf(skewConf: _*) {
      val sql = cmd match {
        case "delete" => s"DELETE FROM $t WHERE mod(id, 2) = 0"
        case "update" => s"UPDATE $t SET id = -1 WHERE mod(id, 2) = 0"
        case "merge" =>
          mergeSrc()
          s"MERGE INTO $t t USING skew_src s ON t.id = s.id " +
            "WHEN MATCHED AND mod(t.id, 2) = 0 THEN UPDATE SET t.id = -1"
      }
      val df = spark.sql(sql)
      assert(
        df.queryExecution.executedPlan.toString().contains("REBALANCE_PARTITIONS_BY_COL"),
        s"$t: no REBALANCE_PARTITIONS_BY_COL in the plan for $cmd"
      )
      df.collect()
    }
    val table = Spark3Util.loadIcebergTable(spark, t)
    val s = table.currentSnapshot().summary()
    val adf = s.get("added-data-files")
    val adel = s.get("added-delete-files")
    // scalastyle:off println
    println(s"[SKEW] $t cmd=$cmd op=${table.currentSnapshot().operation()} " +
      s"added-data-files=$adf added-delete-files=$adel")
    // scalastyle:on println
    (adf, adel)
  }

  private def scenario(cmd: String, mode: String, cow: Boolean): Unit = {
    val tag = s"${cmd}_${if (cow) "cow" else "mor"}"
    val off = s"ice.db.skew_${tag}_off"
    val on = s"ice.db.skew_${tag}_on"
    val expectKey = if (cow) "added-data-files" else "added-delete-files"

    seed(off, mode, if (cmd == "merge") "merge" else cmd)
    val (offAdf, offAdel) = withFeatures(on = false)(run(off, cmd))
    val offVal = if (cow) offAdf else offAdel
    assert(offVal == "4", s"$tag baseline (features off) $expectKey=$offVal, expected 4")

    seed(on, mode, if (cmd == "merge") "merge" else cmd)
    val (onAdf, onAdel) = withFeatures(on = true)(run(on, cmd))
    val onVal = if (cow) onAdf else onAdel
    assert(onVal == "4", s"$tag features ON $expectKey=$onVal, expected 4 (skew split of the rebalance partition)")
  }

  // copy-on-write expects added-data-files=4 (clustered by _file, 1 block/task, 4 blocks over 2 reducers)
  test("skew DELETE copy-on-write -> 4 data files")(scenario("delete", "copy-on-write", cow = true))
  test("skew UPDATE copy-on-write -> 4 data files")(scenario("update", "copy-on-write", cow = true))
  test("skew MERGE copy-on-write -> 4 data files")(scenario("merge", "copy-on-write", cow = true))

  // merge-on-read expects added-delete-files=4 (clustered by _spec_id/_partition, 4 blocks to 1 reducer)
  test("skew DELETE merge-on-read -> 4 delete files")(scenario("delete", "merge-on-read", cow = false))
  test("skew UPDATE merge-on-read -> 4 delete files")(scenario("update", "merge-on-read", cow = false))
  test("skew MERGE merge-on-read -> 4 delete files")(scenario("merge", "merge-on-read", cow = false))
}
