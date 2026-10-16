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
package io.vecruntime.spark.iceberg

import io.vecruntime.spark.{VectorConf, VectorPlugin}
import io.vecruntime.spark.test.IcebergTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorIcebergScanExec}
import org.scalatest.Tag

/**
 * #735: Iceberg tables read by our native Parquet reader over Iceberg's planning (`VectorIcebergScanExec`). The
 * merge-on-read battery of [[IcebergMorSuiteBase]] runs through it: task groups with delete files are read by
 * Iceberg's reader inside the node, the others natively, and every result is checked against Spark. Needs only
 * the Iceberg runtime (`mvn -Piceberg`).
 */
class IcebergNativeScanSuite extends IcebergMorSuiteBase {

  override protected def suiteTags: Seq[Tag] = Seq(IcebergTest)

  override protected def expectedScanClass: String = "VectorIcebergScanExec"

  // With the plugin off Spark's own scan stays.
  override protected def pluginOffScanClass: String = "BatchScanExec"

  override protected def extraSparkConf: Map[String, String] =
    icebergConf ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "true",
      VectorConf.ScanNativeIceberg -> "true",
      s"spark.sql.catalog.${IcebergTables.Catalog}.read.parquet.vectorization.batch-size" -> "1024"
    )

  private def scans(df: DataFrame): Seq[VectorIcebergScanExec] =
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorIcebergScanExec => s }

  private def groups(df: DataFrame): (Long, Long) = {
    val s = scans(df)
    assert(s.nonEmpty, s"expected a VectorIcebergScanExec:\n${finalPlan(df).treeString}")
    (s.map(_.metrics("numNativeGroups").value).sum, s.map(_.metrics("numIcebergGroups").value).sum)
  }

  private val Db = IcebergTables.Db

  icebergTest("a table without deletes is read natively, every task group") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_native USING iceberg AS SELECT * FROM $Db.t_pos")
    useTable("t_native")
    val df =
      checkVectorized("SELECT s, count(*), sum(d2), min(i), max(l) FROM t WHERE i > 5 GROUP BY s", Seq(Filter, Agg))
    assertScanUnder(df, Filter)
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkMixedQueries()
  }

  icebergTest("position deletes are applied natively as a selection") {
    useTable("t_pos")
    val df = checkVectorized("SELECT count(*), sum(d2), min(i), max(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    assert(count("SELECT count(*) FROM t") === IcebergTables.MixedLiveRows)
    checkVectorized("SELECT s, count(*), sum(l) FROM t WHERE i % 3 = 1 GROUP BY s", Seq(Filter, Agg))
  }

  icebergTest("deletion vectors are applied natively as a selection") {
    useTable("t_dv")
    val df = checkVectorized("SELECT count(*), sum(d2), min(i), max(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkVectorized("SELECT s, count(*), sum(l) FROM t WHERE i > 7 GROUP BY s", Seq(Filter, Agg))
  }

  icebergTest("task groups with equality deletes are read by Iceberg's reader") {
    useTable("t_eq")
    val df = checkVectorized("SELECT count(*), sum(d2), min(i), max(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(iceberg > 0, s"native $native, Iceberg $iceberg")
  }

  icebergTest("a renamed column is matched by its field id") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_renamed USING iceberg AS SELECT i, l, s, d2 FROM $Db.t_pos")
    spark.sql(s"ALTER TABLE $Db.t_renamed RENAME COLUMN l TO l2")
    spark.sql(s"ALTER TABLE $Db.t_renamed RENAME COLUMN s TO s2")
    useTable("t_renamed")
    val df = checkVectorized("SELECT s2, count(*), sum(l2), max(d2) FROM t WHERE i > 3 GROUP BY s2", Seq(Filter, Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
  }

  icebergTest("a column added after a file was written reads as nulls from that file") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_added USING iceberg AS SELECT i, l, s FROM $Db.t_pos")
    spark.sql(s"ALTER TABLE $Db.t_added ADD COLUMN extra BIGINT")
    spark.sql(s"INSERT INTO $Db.t_added SELECT i + 100000, l, s, i * 7 FROM $Db.t_pos WHERE i < 500")
    useTable("t_added")
    val df = checkVectorized("SELECT count(*), count(extra), sum(extra), max(l) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkVectorized("SELECT s, count(extra), sum(l) FROM t WHERE extra IS NULL AND i > 2 GROUP BY s", Seq(Filter, Agg))
  }

  icebergTest("an identity-partitioned table reads its partition column from the files") {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_parted USING iceberg PARTITIONED BY (b) AS SELECT i, l, s, b, d2 FROM $Db.t_pos"
    )
    useTable("t_parted")
    val df = checkVectorized("SELECT b, count(*), sum(l), min(s) FROM t WHERE i > 1 GROUP BY b", Seq(Filter, Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    // Partition pruning by Iceberg's planning: only the b = true files.
    checkVectorized("SELECT count(*), sum(l) FROM t WHERE b AND i > 1", Seq(Filter, Agg))
  }

  icebergTest("switched off, Spark's BatchScanExec stays") {
    withConf(VectorConf.ScanNativeIceberg -> "false") {
      useTable("t_pos")
      val df = spark.sql("SELECT count(*) FROM t WHERE i > 1")
      df.collect()
      assert(scans(df).isEmpty, finalPlan(df).treeString)
    }
  }
}
