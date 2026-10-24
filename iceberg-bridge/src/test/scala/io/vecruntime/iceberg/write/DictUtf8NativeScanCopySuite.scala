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
 * Minimal regression for the #901 M4b/M4a dictionary-UTF8 copy-path crash. A copy-on-write UPDATE/DELETE
 * that re-encodes a DICTIONARY-encoded string column read from the NATIVE Iceberg scan buffers the gathered
 * columns across many batches and re-encodes them at close(). The native scan reuses/releases its vectors
 * between batches, so a gathered dictionary-encoded column that retained the producer's dictionary BY
 * REFERENCE later pointed at overwritten bytes -- BinaryChunkWriter.idOf then read a negative per-value
 * length and threw {@code IndexOutOfBoundsException: The provided size is negative}. The fix copies the
 * dictionary into the writer's own arena ({@code Gather.column}).
 *
 * All three conditions are required to reproduce: a changed string column, {@code copyUnchanged} on
 * (default), and the native Iceberg scan on. The table uses a low-cardinality string so the scan emits it
 * dictionary-encoded, and enough rows that the rewrite buffers several batches before close().
 */
class DictUtf8NativeScanCopySuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("dictutf8")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("dict-utf8-native-copy")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16384")
      .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1024")
      // All three conditions ON: our writer, copy-unchanged, and the native Iceberg scan.
      .config("spark.vecruntime.iceberg.writer.enabled", "true")
      .config("spark.vecruntime.iceberg.copyUnchanged.enabled", "true")
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def table(name: String) = Spark3Util.loadIcebergTable(spark, name)

  /** A low-cardinality string column (so the native scan dictionary-encodes it), plus an untouched string. */
  private def create(t: String, version: Int): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"""CREATE TABLE $t (id BIGINT, s STRING, keep STRING, n INT) USING iceberg
         |TBLPROPERTIES ('format-version'='$version', 'write.update.mode'='copy-on-write',
         |  'write.delete.mode'='copy-on-write', 'write.parquet.row-group-size-bytes'='16384',
         |  'write.target-file-size-bytes'='262144')""".stripMargin
    )
    // 8000 rows, s and keep both low-cardinality (dictionary-friendly), several row groups and batches.
    spark.sql(
      s"""INSERT INTO $t
         |SELECT id, concat('v', id % 40) as s, concat('k', id % 25) as keep, cast(id % 1000 as int) as n
         |FROM range(8000)""".stripMargin
    )
  }

  private def dataById(t: String): Map[Long, String] =
    spark.sql(s"SELECT * FROM $t ORDER BY id").collect().map(r => r.getLong(0) -> r.toString).toMap

  for (version <- Seq(2, 3)) {
    test(s"v$version COW UPDATE of a dictionary string column via native scan") {
      val t = s"ice.db.dictupd_v$version"
      create(t, version)
      val before = dataById(t)
      // Change the dictionary string column on a sparse subset -> most column chunks are copied, s re-encoded.
      spark.sql(s"UPDATE $t SET s = concat(s, 'x') WHERE id % 11 = 2").collect()
      val after = dataById(t)
      assert(after.keySet == before.keySet, s"$t: id set changed")
      after.foreach { case (id, _) =>
        val expectedS = if (id % 11 == 2) s"v${id % 40}x" else s"v${id % 40}"
        val row = spark.sql(s"SELECT s, keep, n FROM $t WHERE id = $id").collect()(0)
        assert(row.getString(0) == expectedS, s"$t: s for id=$id")
        assert(row.getString(1) == s"k${id % 25}", s"$t: keep for id=$id untouched")
        assert(row.getInt(2) == (id % 1000).toInt, s"$t: n for id=$id untouched")
      }
    }

    test(s"v$version COW DELETE copying groups with a dictionary string column via native scan") {
      val t = s"ice.db.dictdel_v$version"
      create(t, version)
      val before = dataById(t)
      // Sparse delete -> most groups survive and are copied; lineage (v3) re-encoded via the M4a path.
      spark.sql(s"DELETE FROM $t WHERE id % 13 = 3").collect()
      val survivors = spark.sql(s"SELECT id, s, keep, n FROM $t").collect()
      assert(survivors.length == before.size - before.keys.count(_ % 13 == 3))
      survivors.foreach { r =>
        val id = r.getLong(0)
        assert(id % 13 != 3, s"$t: deleted id=$id still present")
        assert(r.getString(1) == s"v${id % 40}", s"$t: s for id=$id")
        assert(r.getString(2) == s"k${id % 25}", s"$t: keep for id=$id")
        assert(r.getInt(3) == (id % 1000).toInt, s"$t: n for id=$id")
      }
    }
  }
}
