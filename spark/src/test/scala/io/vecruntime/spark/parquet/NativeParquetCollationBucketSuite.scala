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
package io.vecruntime.spark.parquet

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * Collated strings and bucketed scans in the native Parquet scan (#568). Every query is compared with Spark's
 * own scan, and the native cases must decode row groups themselves:
 *   - a collated string (`UTF8_LCASE`, `UNICODE_CI`) read from a file, at the top level, in a struct and in a
 *     list, v1 and v2 pages with and without dictionaries; equality, grouping and ordering by collation above
 *     the scan; a partitioned table with a collated column;
 *   - a bucketed table: Spark's bucket partitioning reported (a join of two tables bucketed alike needs no
 *     shuffle), bucket pruning, buckets coalesced for a join, and a bucketed and sorted table.
 */
@io.vecruntime.spark.test.tags.Scan
class NativeParquetCollationBucketSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1"
    )

  private def show(v: Any): String = v match {
    case null => "null"
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  private def scans(plan: SparkPlan): Seq[VectorParquetScanExec] =
    PlanUtils.allNodes(plan).collect { case s: VectorParquetScanExec => s }

  /** Runs `query` with and without the plugin; returns the native plan. */
  private def check(query: String, native: Boolean = true): SparkPlan = {
    val expected = withPlugin(enabled = false)(rows(spark.sql(query)))
    val (actual, plan) = withPlugin(enabled = true) {
      val df = spark.sql(query)
      val r = rows(df)
      (r, finalPlan(df))
    }
    assert(actual == expected, s"$query\nexpected ${expected.take(5)}\nactual   ${actual.take(5)}")
    if (native) {
      val s = scans(plan)
      assert(s.nonEmpty, s"$query was not read by the native scan:\n$plan")
      assert(s.map(_.metrics("numRowGroups").value).sum > 0, s"$query: no row group decoded natively")
    }
    plan
  }

  private def withTables(names: String*)(body: => Unit): Unit =
    try body
    finally withPlugin(enabled = false)(names.foreach(n => spark.sql(s"DROP TABLE IF EXISTS $n")))

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"collated strings read natively, compared by collation above the scan (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"collated-$v2-$dictionary")
      withConf(
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 3000).selectExpr(
            "id",
            "case when id % 7 = 0 then null else concat(case when id % 2 = 0 then 'Key' else 'KEY' end, id % 13) end as s",
            "named_struct('c', concat('Ä', id % 5), 'n', id) as st",
            "array(concat('x', id % 3), upper(concat('x', id % 3))) as l"
          ).repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false) {
        spark.read
          .schema(
            "id bigint, s string collate UTF8_LCASE, st struct<c: string collate UNICODE_CI, n: bigint>, " +
              "l array<string collate UTF8_LCASE>"
          )
          .parquet(path)
          .createOrReplaceTempView("t")
      }
      check("SELECT * FROM t")
      check("SELECT id, s FROM t WHERE s = 'key3'")
      check("SELECT s, count(*) FROM t GROUP BY s")
      check("SELECT st.c, count(*), max(id) FROM t GROUP BY st.c")
      check("SELECT DISTINCT l[1] FROM t")
      check("SELECT id FROM t WHERE array_contains(l, 'X1') AND id < 50")
    }
  }

  test("a table with a collated column, partitioned") {
    withTables("coll_t") {
      withPlugin(enabled = false) {
        // Spark refuses a collated partition column (INVALID_PARTITION_COLUMN_DATA_TYPE): the partition is plain.
        spark.sql("CREATE TABLE coll_t (id int, s string collate UTF8_LCASE, p string) " +
          "USING parquet PARTITIONED BY (p)")
        spark.sql("INSERT INTO coll_t SELECT cast(id as int), concat('Ab', id % 9), " +
          "case when id % 2 = 0 then 'P' else 'q' end FROM range(0, 2000)")
      }
      check("SELECT * FROM coll_t")
      check("SELECT p, s, count(*) FROM coll_t WHERE s IN ('ab1', 'AB2') GROUP BY p, s")
      check("SELECT count(*), max(s) FROM coll_t WHERE p = 'P'")
    }
  }

  private def bucketed(name: String, buckets: Int, sorted: Boolean = false, offset: Long = 0): Unit =
    withPlugin(enabled = false) {
      val w = spark.range(offset, offset + 4000).selectExpr(
        "cast(id % 997 as int) as k",
        "id",
        "concat('v', id % 31) as v"
      ).write.mode("overwrite").bucketBy(buckets, "k")
      (if (sorted) w.sortBy("k") else w).format("parquet").saveAsTable(name)
    }

  private def shuffles(plan: SparkPlan): Int =
    PlanUtils.allNodes(plan).count(_.isInstanceOf[ShuffleExchangeLike])

  test("a bucketed scan reads natively with Spark's bucket partitioning: a join on the bucket key needs no shuffle") {
    withTables("bk_a", "bk_b") {
      bucketed("bk_a", 8)
      bucketed("bk_b", 8, offset = 500)
      val join = "SELECT a.k, a.id, b.id, b.v FROM bk_a a JOIN bk_b b ON a.k = b.k"
      val sparkShuffles = withPlugin(enabled = false)(shuffles(finalPlan(spark.sql(join))))
      val plan = check(join)
      assert(sparkShuffles == 0, "Spark's plan shuffles: the test tables are not bucketed alike")
      assert(shuffles(plan) == 0, s"the native plan shuffles a bucketed join:\n$plan")
      assert(scans(plan).size == 2, s"both sides should be native scans:\n$plan")
      check("SELECT k, count(*), sum(id) FROM bk_a GROUP BY k")
      // Bucket pruning: only k's bucket is read.
      check("SELECT * FROM bk_a WHERE k = 17")
      check("SELECT * FROM bk_a WHERE k IN (3, 600)")
    }
  }

  test("buckets coalesced for a join, and a bucketed and sorted table") {
    withTables("bk_c8", "bk_c4", "bk_s") {
      bucketed("bk_c8", 8)
      bucketed("bk_c4", 4, offset = 100)
      withConf("spark.sql.bucketing.coalesceBucketsInJoin.enabled" -> "true") {
        val join = "SELECT a.k, a.v, b.id FROM bk_c8 a JOIN bk_c4 b ON a.k = b.k"
        val plan = check(join)
        val sparkShuffles = withPlugin(enabled = false)(shuffles(finalPlan(spark.sql(join))))
        assert(shuffles(plan) == sparkShuffles, s"native ${shuffles(plan)} shuffles, Spark $sparkShuffles:\n$plan")
      }
      bucketed("bk_s", 4, sorted = true)
      check("SELECT * FROM bk_s")
      check("SELECT k, max(v) FROM bk_s GROUP BY k")
    }
  }
}
