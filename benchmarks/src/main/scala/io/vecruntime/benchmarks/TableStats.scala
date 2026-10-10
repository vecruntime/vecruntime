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

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.{Path => HPath}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics}

/**
 * Table statistics for a file-based benchmark dataset (#650), so planner rules that use row counts and
 * distinct counts can be measured with them. Off by default: the tables are plain temp views, which carry no
 * statistics but their size.
 *
 * `spark.vecruntime.bench.tableStats=<path>` registers each table directory as an external session-catalog
 * table (`CREATE TABLE t USING parquet LOCATION ...`) and gives it statistics:
 * - if `<path>` exists, the statistics recorded there, a JSON object per table, as `ANALYZE` wrote them;
 * - if it does not, `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` on each table (one full scan each),
 *   then writes them to `<path>` for the next run.
 * Either way the plans see the same `CatalogStatistics` an `ANALYZE` would leave.
 */
object TableStats {

  val Conf = "spark.vecruntime.bench.tableStats"

  sealed trait Mode
  final case class Load(path: String) extends Mode
  final case class Analyze(path: String) extends Mode

  def mode(spark: SparkSession): Option[Mode] =
    spark.conf.getOption(Conf).map(_.trim).filter(_.nonEmpty).map { p =>
      val path = new HPath(p)
      if (path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)) Load(p) else Analyze(p)
    }

  def register(spark: SparkSession, base: String, tables: Seq[String], mode: Mode): Unit = {
    val catalog = spark.sessionState.catalog
    tables.foreach { t =>
      if (!catalog.tableExists(TableIdentifier(t))) {
        spark.sql(s"CREATE TABLE `$t` USING parquet LOCATION '$base/$t'")
        // A partitioned directory (the fact tables, `ss_sold_date_sk=...`) has no partition in the catalog
        // until they are recovered; without this the table reads as empty.
        if (catalog.getTableMetadata(TableIdentifier(t)).partitionColumnNames.nonEmpty)
          spark.sql(s"ALTER TABLE `$t` RECOVER PARTITIONS")
      }
    }
    mode match {
      case Load(path) =>
        val recorded = read(spark, path)
        tables.foreach(t =>
          recorded.get(t).foreach(s => catalog.alterTableStats(TableIdentifier(t), Some(s(spark)(t))))
        )
        println(
          s"[tpcds] table statistics loaded from $path for ${tables.count(recorded.contains)} of ${tables.size} tables"
        )
      case Analyze(path) =>
        tables.foreach { t =>
          val t0 = System.nanoTime()
          spark.sql(s"ANALYZE TABLE `$t` COMPUTE STATISTICS FOR ALL COLUMNS")
          val rows = catalog.getTableMetadata(TableIdentifier(t)).stats.flatMap(_.rowCount).getOrElse(BigInt(-1))
          println(f"[tpcds] analyzed $t ($rows rows) in ${(System.nanoTime() - t0) / 1e9}%.1f s")
        }
        write(spark, path, tables.flatMap(t => catalog.getTableMetadata(TableIdentifier(t)).stats.map(t -> _)).toMap)
        println(s"[tpcds] table statistics written to $path")
    }
  }

  // ----------------------------------------------------------------- JSON (CatalogColumnStat's own string map)

  private type Recorded = SparkSession => String => CatalogStatistics

  private def write(spark: SparkSession, path: String, stats: Map[String, CatalogStatistics]): Unit = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods.{compact, render}
    val json = stats.map { case (t, s) =>
      t -> (("sizeInBytes" -> s.sizeInBytes.toString) ~
        ("rowCount" -> s.rowCount.map(_.toString)) ~
        ("colStats" -> s.colStats.map { case (c, cs) =>
          c -> cs.toMap(c).map { case (k, v) => k.stripPrefix(s"$c.") -> v }
        }.toMap))
    }
    val out = new HPath(path)
    val os = out.getFileSystem(spark.sparkContext.hadoopConfiguration).create(out, true)
    try os.write(compact(render(json)).getBytes(StandardCharsets.UTF_8))
    finally os.close()
  }

  private def read(spark: SparkSession, path: String): Map[String, Recorded] = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods.parse
    implicit val formats: Formats = DefaultFormats
    val in = new HPath(path)
    val is = in.getFileSystem(spark.sparkContext.hadoopConfiguration).open(in)
    val text =
      try new String(is.readAllBytes(), StandardCharsets.UTF_8)
      finally is.close()
    parse(text).extract[Map[String, JValue]].map { case (t, v) =>
      val size = BigInt((v \ "sizeInBytes").extract[String])
      val rows = (v \ "rowCount").extractOpt[String].map(BigInt(_))
      val cols = (v \ "colStats").extract[Map[String, Map[String, String]]]
      t -> ((s: SparkSession) =>
        (table: String) => {
          val schema = s.sessionState.catalog.getTableMetadata(TableIdentifier(table)).schema
          val colStats = cols.flatMap { case (c, m) =>
            schema.find(_.name == c).flatMap { f =>
              CatalogColumnStat.fromMap(table, c, m.map { case (k, x) => s"$c.$k" -> x })
            }.map(c -> _)
          }
          CatalogStatistics(size, rows, colStats)
        }
      )
    }
  }
}
