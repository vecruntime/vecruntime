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
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFallback, VectorParquetScanExec}

/**
 * Plan-level tests for the native Parquet scan: the node is planned only with the flag on and a supported
 * scan; a recorded reason otherwise; and Spark's own `FileSourceScanExec` stays when the flag is off.
 */
class VectorParquetScanPlanSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map("parquet.block.size" -> (128 * 1024).toString)

  private def write(name: String, sql: String): Unit = withPlugin(enabled = false) {
    val path = newTempPath(name)
    spark.sql(sql).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(name)
  }

  test("flag off: Spark's FileSourceScanExec stays, no node") {
    write("p_off", "SELECT CAST(id AS INT) AS i FROM range(0, 2000)")
    withConf(VectorConf.ScanNativeParquet -> "false") {
      val df = withPlugin(enabled = true)(spark.sql("SELECT i FROM p_off"))
      df.collect()
      val nodes = PlanUtils.allNodes(finalPlan(df))
      assert(nodes.exists(_.isInstanceOf[FileSourceScanExec]), "Spark's scan must remain with the flag off")
      assert(!nodes.exists(_.isInstanceOf[VectorParquetScanExec]), "no native node with the flag off")
    }
  }

  test("default (key unset): the native scan is planned") {
    write("p_default", "SELECT CAST(id AS INT) AS i FROM range(0, 2000)")
    spark.conf.unset(VectorConf.ScanNativeParquet)
    val df = withPlugin(enabled = true)(spark.sql("SELECT i FROM p_default"))
    df.collect()
    val nodes = PlanUtils.allNodes(finalPlan(df))
    assert(nodes.exists(_.isInstanceOf[VectorParquetScanExec]), "the native scan is on by default")
  }

  test("default with Comet's scan active: off unless set explicitly") {
    def conf(kv: (String, String)*) = {
      val c = new org.apache.spark.sql.internal.SQLConf
      kv.foreach { case (k, v) => c.setConfString(k, v) }
      c
    }
    val comet = "spark.plugins" -> "org.apache.spark.CometPlugin,io.vecruntime.spark.VectorPlugin"
    assert(VectorConf.scanNativeParquet(conf()))
    assert(!VectorConf.scanNativeParquet(conf(comet)))
    assert(
      !VectorConf.scanNativeParquet(conf("spark.sql.extensions" -> "org.apache.comet.CometSparkSessionExtensions"))
    )
    assert(VectorConf.scanNativeParquet(conf(comet, "spark.comet.scan.enabled" -> "false")))
    assert(VectorConf.scanNativeParquet(conf(comet, "spark.comet.enabled" -> "false")))
    assert(VectorConf.scanNativeParquet(conf(comet, VectorConf.ScanNativeParquet -> "true")))
    assert(!VectorConf.scanNativeParquet(conf(VectorConf.ScanNativeParquet -> "false")))
  }

  test("flag on, supported scan: the node is planned") {
    write("p_on", "SELECT CAST(id AS INT) AS i, CAST(id AS BIGINT) AS l FROM range(0, 2000)")
    withConf(VectorConf.ScanNativeParquet -> "true") {
      val df = withPlugin(enabled = true)(spark.sql("SELECT i, l FROM p_on"))
      df.collect()
      val nodes = PlanUtils.allNodes(finalPlan(df))
      assert(nodes.exists(_.isInstanceOf[VectorParquetScanExec]), "native node expected with the flag on")
    }
  }

  test("the native scan's tasks prefer the hosts FileScanRDD prefers, localhost dropped (#559)") {
    write("p_loc", "SELECT CAST(id AS INT) AS i FROM range(0, 2000)")
    withConf(VectorConf.ScanNativeParquet -> "true") {
      val df = withPlugin(enabled = true)(spark.sql("SELECT i FROM p_loc"))
      df.collect()
      val node = PlanUtils.allNodes(finalPlan(df)).collectFirst { case n: VectorParquetScanExec => n }
        .getOrElse(fail("native node expected"))
      val rdd = node.executeColumnar()
      def file(len: Long, hosts: String*) = org.apache.spark.sql.execution.datasources.PartitionedFile(
        org.apache.spark.sql.catalyst.InternalRow.empty,
        org.apache.spark.paths.SparkPath.fromUrlString("file:/x"),
        0L,
        len,
        hosts.toArray
      )
      // HDFS-like: real hosts, the three with the most bytes, most first.
      val hdfs = org.apache.spark.sql.execution.datasources.FilePartition(
        0,
        Array(file(100, "h1", "h2"), file(50, "h2", "h3"), file(10, "h4"), file(5, "h5"))
      )
      assert(rdd.preferredLocations(hdfs) === Seq("h2", "h1", "h3"))
      // S3A reports localhost for every block: no preference, as Spark's scan.
      val s3a = org.apache.spark.sql.execution.datasources.FilePartition(0, Array(file(100, "localhost")))
      assert(rdd.preferredLocations(s3a).isEmpty)
    }
  }

  test("flag on, unsupported column: Spark's scan stays with a recorded reason") {
    write("p_nested", "SELECT CAST(id AS INT) AS i, named_struct('a', id) AS st FROM range(0, 1000)")
    withConf(VectorConf.ScanNativeParquet -> "true") {
      val df = withPlugin(enabled = true)(spark.sql("SELECT i, st FROM p_nested"))
      df.collect()
      val plan = finalPlan(df)
      assert(!PlanUtils.allNodes(plan).exists(_.isInstanceOf[VectorParquetScanExec]))
      val reasons = VectorFallback.reasons(plan).map(_._2).mkString("; ")
      assert(reasons.contains("nested or complex column"), s"expected a recorded reason, got: $reasons")
    }
  }
}
