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

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.vecruntime.ParquetFileResolver
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #857: the native Iceberg scan caches loaded delete files per executor, as Iceberg's own Spark reader does
 * (`spark.sql.iceberg.executor-cache.*`). A partition-granularity position-delete file is referenced by every
 * data file of its partition; without the cache the scan's plain `BaseDeleteLoader` (`canCache()`=false) re-reads
 * and re-parses it once per data file, with the cache it is read once per executor and served from memory
 * afterwards.
 *
 * <p>The table is a single partition holding several data files (a tiny target file size forces the split) with
 * one partition-granularity delete file deleting rows from all of them. `ParquetFileResolver.deleteFileOpens`
 * counts every actual open of a delete file by the scan's loader, so:
 *   - cache ON (the default): the shared delete file is opened ONCE for the whole scan (a single executor-cache
 *     miss), fewer than the data-file count;
 *   - cache OFF: it is opened once per data file.
 * Both read the same rows as Iceberg's own reader.
 */
class IcebergDeleteFileCacheSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("icedelcache")
  private val Cache = "spark.sql.iceberg.executor-cache.enabled"
  private val T = "ice.db.mordel"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      // One executor thread, so "opened once per executor" is unambiguous and not spread over racing tasks.
      .master("local[1]")
      .appName("iceberg-delete-file-cache")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "1")
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

  private def withConf[T0](kv: (String, String))(f: => T0): T0 = {
    val prev = spark.conf.getOption(kv._1)
    spark.conf.set(kv._1, kv._2)
    try f
    finally prev match { case Some(v) => spark.conf.set(kv._1, v); case None => spark.conf.unset(kv._1) }
  }

  private def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)

  /** One single-partition v2 MoR table split into several data files, then one partition-granularity delete. */
  private def create(): Int = {
    spark.sql(s"DROP TABLE IF EXISTS $T")
    spark.sql(
      s"""CREATE TABLE $T (id BIGINT, p INT, v STRING) USING iceberg PARTITIONED BY (p)
         |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
         |  'write.delete.granularity'='partition', 'write.target-file-size-bytes'='4096')""".stripMargin
    )
    // All rows in ONE partition (p=0); several separate appends each write a data file, so the partition holds
    // several data files that one partition-granularity delete file will span.
    for (base <- 0 until 4) {
      spark.sql(
        s"""INSERT INTO $T SELECT /*+ REPARTITION(1) */ id, 0 AS p, concat('v', cast(id AS STRING)) AS v
           |FROM range(${base * 1500}, ${(base + 1) * 1500})""".stripMargin
      )
    }
    val dataFiles = dataFileCount()
    assert(dataFiles >= 2, s"expected several data files in the partition, got $dataFiles")
    // A DELETE hitting rows spread across every data file of the partition -> ONE partition-granularity delete
    // file referenced by all of them (merge-on-read, so no data file is rewritten).
    spark.sql(s"DELETE FROM $T WHERE id % 3 = 0")
    assert(deleteFileCount() == 1, s"expected a single partition-granularity delete file, got ${deleteFileCount()}")
    spark.sql(s"REFRESH TABLE $T")
    dataFiles
  }

  private def dataFileCount(): Int =
    spark.sql(s"SELECT count(*) FROM $T.data_files").collect()(0).getLong(0).toInt

  private def deleteFileCount(): Int =
    spark.sql(s"SELECT count(*) FROM $T.delete_files").collect()(0).getLong(0).toInt

  /** The scan's result and the number of delete-file opens it did, with the given cache setting. */
  private def scan(cacheOn: Boolean): (Seq[Row], Long) =
    withConf(Cache -> cacheOn.toString) {
      spark.sql(s"REFRESH TABLE $T")
      val before = ParquetFileResolver.deleteFileOpens.get()
      val got = sorted(spark.sql(s"SELECT id, v FROM $T").collect().toSeq)
      (got, ParquetFileResolver.deleteFileOpens.get() - before)
    }

  test("a shared partition-granularity delete file is loaded once per executor with the cache on, per data file off") {
    val dataFiles = create()

    // Iceberg's own reader (our scan off) is the oracle.
    val expected = withConf("spark.vecruntime.enabled" -> "false")(
      sorted(spark.sql(s"SELECT id, v FROM $T").collect().toSeq)
    )

    // Cache OFF: the plain loader re-opens the shared delete file once per data file.
    val (gotOff, opensOff) = scan(cacheOn = false)
    assert(gotOff == expected, "native scan with the delete cache OFF read different rows than Iceberg's reader")
    assert(
      opensOff == dataFiles.toLong,
      s"cache off: expected one delete-file open per data file ($dataFiles), got $opensOff"
    )

    // Cache ON (default): the shared delete file is opened once for the whole scan.
    val (gotOn, opensOn) = scan(cacheOn = true)
    assert(gotOn == expected, "native scan with the delete cache ON read different rows than Iceberg's reader")
    assert(opensOn == 1L, s"cache on: expected the shared delete file to be opened once, got $opensOn")
    assert(opensOn < opensOff, s"cache on ($opensOn) should open the delete file fewer times than off ($opensOff)")
  }
}
