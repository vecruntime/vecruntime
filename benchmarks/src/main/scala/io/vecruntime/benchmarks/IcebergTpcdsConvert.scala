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
package io.vecruntime.benchmarks

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.spark.sql.SparkSession

/**
 * #735: copies the 24 TPC-DS Parquet tables under `--data` into Iceberg tables `<catalog>.<namespace>.<table>` of
 * a Hadoop catalog under `--warehouse` (the catalog of [[IcebergMorGenerator.catalogConf]]: S3FileIO on an s3a
 * warehouse). A table Hive-partitioned on disk (`col=value` directories, the fact tables' `*_date_sk`) becomes an
 * identity-partitioned Iceberg table on that column, so Iceberg's planning prunes the same files the Parquet scan
 * prunes. Format v2, zstd, written with hash distribution (one writer per partition). An existing table is
 * skipped unless `--replace`.
 *
 * {{{
 * IcebergTpcdsConvert --data s3a://bucket/tpcds/sf3000-doeks --warehouse s3a://bucket/iceberg/tpcds \
 *   --namespace sf3000 [--tables store_sales,item] [--replace]
 * }}}
 * Run it with the catalog through `TpcdsRunner --data catalog:ice.sf3000` afterwards.
 *
 * `--stats` converts nothing: it runs Iceberg's `compute_table_stats` on each table instead, which writes the
 * per-column distinct-count sketches (Puffin `apache-datasketches-theta-v1`) of the current snapshot. Iceberg's scan
 * reports them to Spark as column statistics when `spark.sql.cbo.enabled` is on.
 */
object IcebergTpcdsConvert {

  final case class Args(
      data: String = "",
      warehouse: String = "",
      namespace: String = "tpcds",
      tables: Seq[String] = TpcdsRunner.Tables,
      replace: Boolean = false,
      stats: Boolean = false
  )

  def parse(args: List[String], a: Args = Args()): Args = args match {
    case Nil => a
    case "--data" :: v :: t => parse(t, a.copy(data = v))
    case "--warehouse" :: v :: t => parse(t, a.copy(warehouse = v))
    case "--namespace" :: v :: t => parse(t, a.copy(namespace = v))
    case "--tables" :: v :: t => parse(t, a.copy(tables = v.split(",").map(_.trim).filter(_.nonEmpty).toSeq))
    case "--replace" :: t => parse(t, a.copy(replace = true))
    case "--stats" :: t => parse(t, a.copy(stats = true))
    case other => throw new IllegalArgumentException(s"unknown arguments: ${other.mkString(" ")}")
  }

  /** The Hive partition column of a Parquet table directory (its first `col=value` child), if any. */
  def partitionColumn(fs: FileSystem, dir: HPath): Option[String] =
    fs.listStatus(
      dir
    ).iterator.filter(_.isDirectory).map(_.getPath.getName).find(_.contains("=")).map(_.takeWhile(_ != '='))

  def main(argv: Array[String]): Unit = {
    val a = parse(argv.toList)
    require(a.data.nonEmpty && a.warehouse.nonEmpty, "--data and --warehouse are required")
    val builder = SparkSession.builder().appName(s"iceberg-tpcds-convert-${a.namespace}")
    IcebergMorGenerator.catalogConf(a.warehouse).foreach { case (k, v) => builder.config(k, v) }
    val spark = builder.getOrCreate()
    val cat = IcebergMorGenerator.Catalog
    val base = a.data.stripSuffix("/")
    val fs = FileSystem.get(new URI(base), spark.sparkContext.hadoopConfiguration)
    if (a.stats) {
      a.tables.foreach { t =>
        val t0 = System.nanoTime()
        val out = spark.sql(s"CALL $cat.system.compute_table_stats(table => '${a.namespace}.$t')").collect()
        println(f"[stats] $t: ${out.map(_.mkString(" ")).mkString("; ")}, ${(System.nanoTime() - t0) / 1e9}%.0f s")
      }
      spark.stop()
      return
    }
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $cat.${a.namespace}")
    a.tables.foreach { t =>
      val dir = new HPath(s"$base/$t")
      val target = s"$cat.${a.namespace}.$t"
      if (!fs.exists(dir)) println(s"[convert] $t: no Parquet at $dir, skipped")
      else if (!a.replace && spark.catalog.tableExists(target)) println(s"[convert] $t: exists, skipped")
      else {
        val t0 = System.nanoTime()
        spark.read.parquet(dir.toString).createOrReplaceTempView(s"src_$t")
        val part = partitionColumn(fs, dir)
        val partitioned = part.map(c => s"PARTITIONED BY ($c)").getOrElse("")
        spark.sql(s"DROP TABLE IF EXISTS $target")
        spark.sql(
          s"""CREATE TABLE $target USING iceberg $partitioned
             |TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd',
             |  'write.distribution-mode' = 'hash')
             |AS SELECT * FROM src_$t""".stripMargin
        )
        val rows = spark.table(target).count()
        val src = spark.table(s"src_$t").count()
        require(rows == src, s"$t: $rows rows in Iceberg, $src in Parquet")
        println(
          f"[convert] $t: $rows%,d rows, partitioned by ${part.getOrElse("-")}, ${(System.nanoTime() - t0) / 1e9}%.0f s"
        )
      }
    }
    spark.stop()
  }
}
