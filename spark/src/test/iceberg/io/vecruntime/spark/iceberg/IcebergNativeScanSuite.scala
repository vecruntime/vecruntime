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

  icebergTest("equality deletes are applied natively, the equality column read even when not projected") {
    useTable("t_eq")
    val df = checkVectorized("SELECT count(*), sum(d2), min(i), max(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    // `i`, the equality column, not projected: read for the deletes, then dropped.
    val noKey = checkVectorized("SELECT s, count(*), sum(l), max(d2) FROM t GROUP BY s", Seq(Agg))
    val (native2, iceberg2) = groups(noKey)
    assert(native2 > 0 && iceberg2 == 0, s"native $native2, Iceberg $iceberg2")
    assert(count("SELECT count(*) FROM t") === IcebergTables.EqLiveRows)
    assert(count("SELECT count(d2) FROM t") === IcebergTables.EqLiveRows)
    checkVectorized("SELECT s, count(*), sum(l) FROM t WHERE l % 5 = 2 GROUP BY s", Seq(Filter, Agg))
  }

  icebergTest("equality deletes on a string column are applied natively") {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_eqs USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT * FROM $Db.t_pos"
    )
    // A null in an equality delete matches the null rows, as in Iceberg.
    IcebergTables.addEqualityDeletesOn(spark, s"$Db.t_eqs", "s", Seq("s3", "s17", null))
    useTable("t_eqs")
    val df = checkVectorized("SELECT count(*), sum(l), min(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkVectorized("SELECT s, count(*) FROM t GROUP BY s", Seq(Agg))
  }

  icebergTest("metadata columns _file, _pos and _spec_id are read natively, with and without deletes") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_meta USING iceberg AS SELECT * FROM $Db.t_pos")
    Seq("t_meta", "t_pos", "t_eq").foreach { table =>
      // The table itself, not the view `t`: Iceberg's hidden metadata columns resolve on the relation.
      val t = s"$Db.$table"
      val df = checkVectorized(s"SELECT _file, _pos, _spec_id, i, s FROM $t WHERE i % 97 = 3", Seq(Filter))
      val (native, iceberg) = groups(df)
      assert(native > 0 && iceberg == 0, s"$table: native $native, Iceberg $iceberg")
      // Each one alone and out of order: _pos without the other constants, _file only, before the data columns.
      checkVectorized(s"SELECT l, _pos FROM $t WHERE i % 89 = 1", Seq(Filter))
      checkVectorized(s"SELECT _file, count(*), max(i) FROM $t GROUP BY _file", Seq(Agg))
      checkVectorized(s"SELECT _spec_id, count(*) FROM $t GROUP BY _spec_id", Seq(Agg))
    }
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

  private def rowGroups(df: DataFrame): Long = scans(df).map(_.metrics("numRowGroups").value).sum

  /** One file, ascending `a` and `b`, in many small row groups: row-group skipping is all a filter can do. */
  private def sortedTable(name: String): Unit = {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.$name (a INT, b BIGINT, s STRING) USING iceberg " +
        "TBLPROPERTIES ('write.parquet.row-group-size-bytes'='2048')"
    )
    spark.sql(
      s"INSERT INTO $Db.$name SELECT CAST(id AS INT), id * 3, concat('s', CAST(id % 40 AS STRING)) " +
        "FROM range(0, 20000, 1, 1)"
    )
  }

  icebergTest("the filter above the scan skips row groups, by field id") {
    sortedTable("t_push")
    useTable("t_push")
    val all = checkVectorized("SELECT count(*), sum(b) FROM t WHERE s IS NOT NULL", Seq(Filter, Agg))
    val df = checkVectorized("SELECT count(*), sum(b) FROM t WHERE a BETWEEN 1000 AND 1100", Seq(Filter, Agg))
    assert(scans(df).exists(_.pushedFilters.nonEmpty), finalPlan(df).treeString)
    val (read, total) = (rowGroups(df), rowGroups(all))
    assert(total > 10 && read > 0 && read * 5 < total, s"read $read of $total row groups")
    assert(count("SELECT count(*) FROM t WHERE a BETWEEN 1000 AND 1100") === 101)
    // Strings, IN lists, an OR and a NOT: the same rows as Spark's.
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a IN (3, 9000, 19999) OR b < 30", Seq(Filter, Agg))
    checkVectorized("SELECT count(*) FROM t WHERE s = 's7' AND NOT (a > 500)", Seq(Filter, Agg))
    // Spark's switch turns the push off.
    withConf("spark.sql.parquet.filterPushdown" -> "false") {
      val off = checkVectorized("SELECT count(*), sum(b) FROM t WHERE a BETWEEN 1000 AND 1100", Seq(Filter, Agg))
      assert(rowGroups(off) === total, s"read ${rowGroups(off)} of $total row groups")
    }
  }

  icebergTest("Iceberg's task groups are packed into tasks as Spark packs a file scan's splits") {
    // Small files, each its own task group (a 1 KB split target).
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_pack (a INT, b BIGINT, s STRING) USING iceberg " +
        "TBLPROPERTIES ('read.split.target-size'='1024')"
    )
    spark.sql(
      s"INSERT INTO $Db.t_pack SELECT CAST(id AS INT), id * 3, concat('s', CAST(id % 40 AS STRING)) " +
        "FROM range(0, 20000, 1, 16)"
    )
    useTable("t_pack")
    def tasks(df: DataFrame): Int = scans(df).map(_.executeColumnar().getNumPartitions).sum
    val sql = "SELECT s, count(*), sum(b) FROM t WHERE a % 7 <> 3 GROUP BY s"
    val off = withConf(VectorConf.ScanNativeIcebergPackSplits -> "false") { checkVectorized(sql, Seq(Filter, Agg)) }
    val on = checkVectorized(sql, Seq(Filter, Agg))
    assert(tasks(off) >= 16, s"${tasks(off)} tasks unpacked")
    assert(tasks(on) < tasks(off), s"${tasks(on)} tasks packed, ${tasks(off)} unpacked")
    // Packed to fill the cores: never fewer tasks than Spark's own packing of the same bytes would make.
    withConf("spark.sql.files.minPartitionNum" -> "4") {
      val four = checkVectorized(sql, Seq(Filter, Agg))
      assert(tasks(four) >= 4 && tasks(four) <= tasks(off), s"${tasks(four)} tasks with minPartitionNum 4")
    }
    // A table with deletes keeps its rows when its groups are packed.
    spark.sql(s"DELETE FROM $Db.t_pack WHERE a % 11 = 4")
    checkVectorized(sql, Seq(Filter, Agg))
    assert(count("SELECT count(*) FROM t") === 20000 - (0 until 20000).count(_ % 11 == 4))
  }

  icebergTest("a pushed filter on a renamed column skips row groups of the files written before the rename") {
    sortedTable("t_push_ren")
    spark.sql(s"ALTER TABLE $Db.t_push_ren RENAME COLUMN a TO a2")
    useTable("t_push_ren")
    val all = checkVectorized("SELECT count(*), sum(b) FROM t WHERE s IS NOT NULL", Seq(Filter, Agg))
    val df = checkVectorized("SELECT count(*), sum(b) FROM t WHERE a2 BETWEEN 1000 AND 1100", Seq(Filter, Agg))
    val (read, total) = (rowGroups(df), rowGroups(all))
    assert(read > 0 && read * 5 < total, s"read $read of $total row groups")
  }

  icebergTest("after a rename and a new column taking the old name, a filter on the new column reads the old files") {
    sortedTable("t_push_reuse")
    // The old files' `a` is now `a_old`; the new `a` (another field id) is null in them.
    spark.sql(s"ALTER TABLE $Db.t_push_reuse RENAME COLUMN a TO a_old")
    spark.sql(s"ALTER TABLE $Db.t_push_reuse ADD COLUMN a INT")
    spark.sql(s"INSERT INTO $Db.t_push_reuse VALUES (-1, -3, 'new', 5), (-2, -6, 'new', 6)")
    useTable("t_push_reuse")
    // By name, `a IS NULL` would be tested against the old files' a_old (no nulls) and skip them all.
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a IS NULL", Seq(Filter, Agg))
    assert(count("SELECT count(*) FROM t WHERE a IS NULL") === 20000)
    // The new column alone, beside the renamed one, and under OR / NOT: every one tested by field id.
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a = 5", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a IS NULL AND a_old BETWEEN 10 AND 20", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a = 6 OR a_old = 3", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE NOT (a = 6)", Seq(Filter, Agg))
    assert(count("SELECT count(*) FROM t WHERE a IS NULL AND a_old BETWEEN 10 AND 20") === 11)
  }

  icebergTest("pushed filters on a partitioned table with deletes return Spark's rows") {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_push_part USING iceberg PARTITIONED BY (b) TBLPROPERTIES " +
        s"('format-version'='2', 'write.parquet.row-group-size-bytes'='2048') AS SELECT i, l, s, b, d2 FROM $Db.t_pos"
    )
    spark.sql(s"DELETE FROM $Db.t_push_part WHERE i % 11 = 4")
    useTable("t_push_part")
    checkVectorized("SELECT b, count(*), sum(l) FROM t WHERE i BETWEEN 100 AND 400 GROUP BY b", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(l) FROM t WHERE b AND s >= 's3' AND d2 > 0.5", Seq(Filter, Agg))
    checkVectorized(s"SELECT _pos, i FROM $Db.t_push_part WHERE i < 50 AND _pos > 3", Seq(Filter))
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
