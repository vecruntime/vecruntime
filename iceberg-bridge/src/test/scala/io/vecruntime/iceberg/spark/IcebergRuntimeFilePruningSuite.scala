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
package io.vecruntime.iceberg.spark

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #iceberg-file-pruning: a broadcast hash join's RUNTIME filters (#610 -- min/max range and IN set on a data
 * column, derived from the build side) prune Iceberg data files BEFORE the native reader opens them, instead of
 * opening every file and only skipping its row groups. The scan evaluates each file's Iceberg metrics with
 * `InclusiveMetricsEvaluator` on the driver and drops the files that cannot match.
 *
 * Each test builds an UNPARTITIONED fact table of many files with disjoint key ranges (so Iceberg's own planning
 * and DPP cannot prune -- only column bounds can), joins it to a tiny broadcast dimension that selects one band,
 * and asserts: (1) the result equals the plugin-off oracle, (2) the pruned-file metric is positive and the
 * files-read count dropped versus the unpruned full scan, and (3) with the conf off, no file is pruned and the
 * result is still correct. Covers an IN-set key, a range key, a string key with truncated bounds, a file written
 * under metrics mode `none` (kept), v2 position deletes and a v3 deletion vector.
 */
class IcebergRuntimeFilePruningSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("ice-rf-prune")
  private val Prune = "spark.vecruntime.scan.nativeIceberg.runtimeFilePruning.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("iceberg-runtime-file-pruning")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.session.timeZone", "UTC")
      // Force a broadcast hash join for the small dimension so #610 attaches runtime filters.
      .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
      // Keep DPP from pruning too (we test COLUMN-bound pruning on an unpartitioned table; DPP prunes partitions).
      .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
      .config("spark.plugins", "io.vecruntime.spark.VectorPlugin")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.resolve("ice").toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      // One file per INSERT, one small row group, so per-file bounds are tight and distinct.
      .config("spark.sql.iceberg.write.distribution-mode", "none")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def withConf[T](kv: (String, String)*)(f: => T): T = {
    val prev = kv.map { case (k, _) => k -> spark.conf.getOption(k) }
    kv.foreach { case (k, v) => spark.conf.set(k, v) }
    try f
    finally prev.foreach { case (k, v) =>
        v match { case Some(x) => spark.conf.set(k, x); case None => spark.conf.unset(k) }
      }
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

  private def icebergScans(df: DataFrame): Seq[SparkPlan] =
    nodes(df.queryExecution.executedPlan).filter(_.getClass.getSimpleName == "VectorIcebergScanExec")

  /** Run `q`; return (rows, files read, files pruned by the runtime filter), asserting the native scan ran. */
  private def run(q: String): (Seq[Row], Long, Long) = {
    val df = spark.sql(q)
    val got = df.collect().toSeq
    val scans = icebergScans(df)
    assert(scans.nonEmpty, s"not the native scan for [$q]:\n${df.queryExecution.executedPlan}")
    val files = scans.map(_.metrics("numFiles").value).sum
    val pruned = scans.map(_.metrics("numFilesPrunedByRuntimeFilter").value).sum
    (got, files, pruned)
  }

  private def ids(rows: Seq[Row]): Seq[Long] = rows.map(_.getLong(0)).sorted

  private def oracle(q: String): Seq[Row] =
    withConf("spark.vecruntime.enabled" -> "false")(spark.sql(q).collect().toSeq)

  /** The full-table file count (no join), the unpruned baseline. */
  private def fullFiles(table: String): Long = run(s"SELECT id FROM $table")._2

  /**
   * A fact table of `bands` files, each file holding `perBand` rows whose key `k` falls in one disjoint band
   * `[band*1000, band*1000+perBand)`; `id` is globally unique. Written one INSERT per band so each is its own
   * data file with tight, disjoint `k` bounds.
   */
  private def buildFact(name: String, bands: Int, perBand: Int): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(s"CREATE TABLE $name (id BIGINT, k INT, s STRING, payload STRING) USING iceberg")
    (0 until bands).foreach { b =>
      spark.sql(
        s"""INSERT INTO $name SELECT
           |  CAST($b * 1000 + id AS BIGINT) AS id,
           |  CAST($b * 1000 + id AS INT) AS k,
           |  concat('key', lpad(CAST($b * 1000 + id AS STRING), 7, '0')) AS s,
           |  repeat('x', 20) AS payload
           |FROM range(0, $perBand)""".stripMargin
      )
    }
    spark.sql(s"REFRESH TABLE $name")
  }

  test("an IN-set runtime filter prunes files outside the build key set; result matches the oracle") {
    val t = "ice.db.rf_in"
    buildFact(t, bands = 8, perBand = 50)
    val full = fullFiles(t)
    assert(full >= 8, s"expected at least 8 files, got $full")
    // A dim with keys only in band 2 -> a small IN set; files of other bands cannot match.
    spark.sql("DROP TABLE IF EXISTS dimin")
    spark.sql("CREATE TABLE dimin (k INT) USING parquet")
    spark.sql("INSERT INTO dimin VALUES (2000), (2005), (2010), (2049)")
    val q = s"SELECT f.id FROM $t f JOIN dimin d ON f.k = d.k"
    val (got, files, pruned) = run(q)
    assert(ids(got) == ids(oracle(q)), s"IN join != oracle: ${ids(got).take(5)} vs ${ids(oracle(q)).take(5)}")
    assert(pruned > 0, s"no file pruned by the IN runtime filter (read $files of $full)")
    assert(files < full, s"IN runtime filter did not reduce files: read $files of $full")
    // Conf off: no pruning, same result.
    withConf(Prune -> "false") {
      val (got2, files2, pruned2) = run(q)
      assert(pruned2 == 0, s"pruning happened with the conf off ($pruned2)")
      assert(files2 == full, s"conf off read $files2 files, expected the full $full")
      assert(ids(got2) == ids(oracle(q)), "conf off result != oracle")
    }
  }

  test("a range runtime filter prunes files whose bounds fall outside the build min/max") {
    val t = "ice.db.rf_range"
    buildFact(t, bands = 10, perBand = 60)
    val full = fullFiles(t)
    // A dim whose keys span only bands 4..5 -> the #610 range filter is [4000.., 5059]; bands 0..3, 6..9 prune.
    spark.sql("DROP TABLE IF EXISTS dimrange")
    spark.sql("CREATE TABLE dimrange (k INT) USING parquet")
    spark.sql("INSERT INTO dimrange SELECT CAST(4000 + id AS INT) FROM range(0, 1060)")
    val q = s"SELECT f.id FROM $t f JOIN dimrange d ON f.k = d.k"
    val (got, files, pruned) = run(q)
    assert(ids(got) == ids(oracle(q)), s"range join != oracle")
    assert(pruned > 0, s"no file pruned by the range runtime filter (read $files of $full)")
    assert(files < full, s"range runtime filter did not reduce files: read $files of $full")
  }

  test("a string key with truncated bounds prunes correctly (InclusiveMetricsEvaluator handles truncation)") {
    val t = "ice.db.rf_str"
    // Truncate string column metrics hard (2 chars) so the stored bounds are PREFIXES, exercising the truncated
    // lower/upper path in the evaluator; pruning must stay correct.
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"""CREATE TABLE $t (id BIGINT, s STRING, payload STRING) USING iceberg
         |  TBLPROPERTIES ('write.metadata.metrics.column.s' = 'truncate(2)')""".stripMargin
    )
    // Four files, each a disjoint prefix band: 'aa*', 'bb*', 'cc*', 'dd*'.
    Seq("aa" -> 0L, "bb" -> 1000L, "cc" -> 2000L, "dd" -> 3000L).foreach { case (p, base) =>
      spark.sql(
        s"""INSERT INTO $t SELECT CAST($base + id AS BIGINT),
           |  concat('$p', lpad(CAST(id AS STRING), 4, '0')), repeat('x', 20) FROM range(0, 40)""".stripMargin
      )
    }
    spark.sql(s"REFRESH TABLE $t")
    val full = fullFiles(t)
    spark.sql("DROP TABLE IF EXISTS dimstr")
    spark.sql("CREATE TABLE dimstr (s STRING) USING parquet")
    spark.sql("INSERT INTO dimstr VALUES ('cc0001'), ('cc0007'), ('cc0039')")
    val q = s"SELECT f.id FROM $t f JOIN dimstr d ON f.s = d.s"
    val (got, files, pruned) = run(q)
    assert(ids(got) == ids(oracle(q)), s"string join != oracle")
    assert(pruned > 0, s"no file pruned by the truncated-string runtime filter (read $files of $full)")
    assert(files < full, s"string runtime filter did not reduce files: read $files of $full")
  }

  test("a file written under metrics mode none is KEPT (no bounds), result still correct") {
    val t = "ice.db.rf_none"
    spark.sql(s"DROP TABLE IF EXISTS $t")
    // One table where column k keeps no bounds (metrics mode none): every file must be kept by the evaluator.
    spark.sql(
      s"""CREATE TABLE $t (id BIGINT, k INT, payload STRING) USING iceberg
         |  TBLPROPERTIES ('write.metadata.metrics.default' = 'none')""".stripMargin
    )
    (0 until 6).foreach { b =>
      spark.sql(
        s"""INSERT INTO $t SELECT CAST($b * 1000 + id AS BIGINT), CAST($b * 1000 + id AS INT), repeat('x', 20)
           |FROM range(0, 40)""".stripMargin
      )
    }
    spark.sql(s"REFRESH TABLE $t")
    val full = fullFiles(t)
    spark.sql("DROP TABLE IF EXISTS dimnone")
    spark.sql("CREATE TABLE dimnone (k INT) USING parquet")
    spark.sql("INSERT INTO dimnone VALUES (2000), (2005)")
    val q = s"SELECT f.id FROM $t f JOIN dimnone d ON f.k = d.k"
    val (got, files, pruned) = run(q)
    assert(ids(got) == ids(oracle(q)), s"none-mode join != oracle")
    // No bounds -> the evaluator keeps every file -> nothing pruned, every file opened.
    assert(pruned == 0, s"a metrics-mode-none table must not be pruned, but pruned $pruned")
    assert(files == full, s"none-mode read $files files, expected the full $full")
  }

  test("pruning is correct for a v2 position-delete table and a v3 deletion-vector table") {
    // v2 MOR with position deletes.
    val t2 = "ice.db.rf_v2del"
    spark.sql(s"DROP TABLE IF EXISTS $t2")
    spark.sql(
      s"""CREATE TABLE $t2 (id BIGINT, k INT, payload STRING) USING iceberg
         |  TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')""".stripMargin
    )
    (0 until 6).foreach { b =>
      spark.sql(
        s"""INSERT INTO $t2 SELECT CAST($b * 1000 + id AS BIGINT), CAST($b * 1000 + id AS INT), repeat('x', 20)
           |FROM range(0, 50)""".stripMargin
      )
    }
    // Delete some rows in band 2 (the band the join keeps), producing position deletes on a kept file.
    spark.sql(s"DELETE FROM $t2 WHERE k % 2 = 0 AND k >= 2000 AND k < 3000")
    spark.sql(s"REFRESH TABLE $t2")
    val full2 = fullFiles(t2)
    spark.sql("DROP TABLE IF EXISTS dimv2")
    spark.sql("CREATE TABLE dimv2 (k INT) USING parquet")
    spark.sql("INSERT INTO dimv2 VALUES (2001), (2003), (2005), (2049)")
    val q2 = s"SELECT f.id FROM $t2 f JOIN dimv2 d ON f.k = d.k"
    val (got2, files2, pruned2) = run(q2)
    assert(ids(got2) == ids(oracle(q2)), s"v2-delete join != oracle: ${ids(got2)} vs ${ids(oracle(q2))}")
    assert(pruned2 > 0, s"no file pruned on the v2-delete table (read $files2 of $full2)")

    // v3 with deletion vectors.
    val t3 = "ice.db.rf_v3dv"
    spark.sql(s"DROP TABLE IF EXISTS $t3")
    spark.sql(
      s"""CREATE TABLE $t3 (id BIGINT, k INT, payload STRING) USING iceberg
         |  TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')""".stripMargin
    )
    (0 until 6).foreach { b =>
      spark.sql(
        s"""INSERT INTO $t3 SELECT CAST($b * 1000 + id AS BIGINT), CAST($b * 1000 + id AS INT), repeat('x', 20)
           |FROM range(0, 50)""".stripMargin
      )
    }
    spark.sql(s"DELETE FROM $t3 WHERE k % 3 = 0 AND k >= 2000 AND k < 3000")
    spark.sql(s"REFRESH TABLE $t3")
    val full3 = fullFiles(t3)
    spark.sql("DROP TABLE IF EXISTS dimv3")
    spark.sql("CREATE TABLE dimv3 (k INT) USING parquet")
    spark.sql("INSERT INTO dimv3 VALUES (2002), (2004), (2006), (2048)")
    val q3 = s"SELECT f.id FROM $t3 f JOIN dimv3 d ON f.k = d.k"
    val (got3, files3, pruned3) = run(q3)
    assert(ids(got3) == ids(oracle(q3)), s"v3-DV join != oracle: ${ids(got3)} vs ${ids(oracle(q3))}")
    assert(pruned3 > 0, s"no file pruned on the v3-DV table (read $files3 of $full3)")
  }
}
