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
package io.vecruntime.benchmarks

import org.apache.spark.sql.SparkSession

/**
 * A synthetic two-table benchmark built for the cases TPC-DS does not exercise (#610, #611):
 *
 *   - `fact` is written in `key` order (500 rows per key), so every row group covers a narrow key range. A join
 *     against a selective slice of `dim` hands the scan a key range or IN list (runtime filters, #610) that rules
 *     out most row groups from their statistics alone. TPC-DS fact tables are not clustered on their join keys,
 *     which is why the same switch skipped nothing there.
 *   - `status` is `rare` on a scattered 1% of rows, next to two wide string columns. A filter on it keeps about
 *     1% of every batch, the regime where late materialization (#611) decodes the payload for the survivors only.
 *
 * Generate with [[ShowcaseGenRunner]]; run with `ShowcaseRunner --data <dir>` (or `--tables <uri>` on a cluster),
 * the same arguments as [[TpchRunner]].
 */
object ShowcaseRunner {

  val Queries: Seq[(String, String)] = Seq(
    // #610: a contiguous 1% of the keys (one region) -> a key range on fact.
    "rf1" ->
      """SELECT d.name, sum(f.amount) AS s, count(*) AS c
        |FROM fact f JOIN dim d ON f.key = d.key
        |WHERE d.region = 42
        |GROUP BY d.name ORDER BY s DESC, d.name LIMIT 20""".stripMargin,
    // #610: 100 keys spread over the whole key space -> an IN list (a range would cover everything).
    "rf2" ->
      """SELECT sum(f.score) AS s, count(*) AS c
        |FROM fact f JOIN dim d ON f.key = d.key
        |WHERE d.key % 2500 = 7""".stripMargin,
    // #611: ~1% scattered survivors of every batch; the wide payload is decoded for those rows only.
    "lm1" ->
      """SELECT count(*) AS c, sum(length(p1) + length(p2)) AS l, max(p1) AS m, sum(amount) AS s
        |FROM fact WHERE status = 'rare'""".stripMargin,
    // #611: ~0.1% survivors.
    "lm2" ->
      """SELECT count(*) AS c, max(p2) AS m, avg(score) AS a
        |FROM fact WHERE status = 'rare' AND amount < 1000.0""".stripMargin
  )

  lazy val Showcase: TpchRunner.Suite =
    TpchRunner.Suite(
      "showcase",
      "Showcase (runtime filters, late materialization)",
      Seq("fact", "dim"),
      "fact",
      Queries
    )

  def main(argv: Array[String]): Unit = TpchRunner.mainWith(Showcase, argv)
}

/**
 * Writes the [[ShowcaseRunner]] tables: `--out <base URI>` (each table a directory under it), `--rows <fact rows>`
 * (default 125,000,000, about 10 GB of snappy Parquet), `--files <fact files>` (default 64).
 */
object ShowcaseGenRunner {

  def main(args: Array[String]): Unit = {
    val opts = args.sliding(2, 2).collect { case Array(k, v) if k.startsWith("--") => k.stripPrefix("--") -> v }.toMap
    val out = opts.getOrElse("out", sys.error("--out <base URI> is required")).stripSuffix("/")
    val rows = opts.get("rows").map(_.toLong).getOrElse(125000000L)
    val files = opts.get("files").map(_.toInt).getOrElse(64)
    val keys = rows / 500
    val spark = SparkSession.builder().appName("showcase-gen").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    println(s"[showcase-gen] $rows fact rows in $files files, $keys keys, out $out")
    // range() splits [0, rows) into contiguous slices, one per partition, so every file (and every row group in it)
    // holds a narrow, ascending key range without a sort.
    spark
      .range(0, rows, 1, files)
      .selectExpr(
        "id DIV 500 AS key",
        "CASE WHEN pmod(hash(id), 100) = 0 THEN 'rare' ELSE 'common' END AS status",
        "md5(CAST(id AS STRING)) AS p1",
        "md5(CAST(id * 7919 AS STRING)) AS p2",
        "pmod(hash(id * 31), 1000000) / 100.0 AS amount",
        "CAST(pmod(hash(id * 17), 100000) AS DOUBLE) AS score"
      )
      .write
      .mode("overwrite")
      .parquet(s"$out/fact")
    spark
      .range(0, keys, 1, 1)
      .selectExpr("id AS key", s"id DIV ${keys / 100} AS region", "concat('name-', CAST(id AS STRING)) AS name")
      .write
      .mode("overwrite")
      .parquet(s"$out/dim")
    Seq("fact", "dim").foreach { t =>
      val df = spark.read.parquet(s"$out/$t")
      println(f"[showcase-gen] $t: ${df.count()}%,d rows, ${df.inputFiles.length}%,d files")
    }
    spark.stop()
  }
}
