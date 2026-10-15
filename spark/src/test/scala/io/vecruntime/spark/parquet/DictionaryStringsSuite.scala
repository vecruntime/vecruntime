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
import io.vecruntime.spark.arrow.{VectorAllocators, VectorDictionaryColumnVector}
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFilterExec, VectorHashAggregateExec, VectorParquetScanExec}

/**
 * #612: the native scan emits a dictionary-encoded string column as dictionary vectors (ids over the row
 * group's dictionary), as Spark's scan path hands them to our operators; a chunk that falls back from its
 * dictionary mid-way is decoded flat from that page on. Results must be Spark's either way.
 */
class DictionaryStringsSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (256 * 1024).toString,
      "parquet.page.size" -> (8 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1000"
    )

  private val node = classOf[VectorParquetScanExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withPlugin(enabled = false) {
      // Low-cardinality strings with nulls: every page dictionary-encoded.
      spark.sql(
        """SELECT CAST(id AS INT) AS i,
          |  CASE WHEN id % 11 = 0 THEN NULL ELSE CONCAT('cat-', CAST(id % 37 AS STRING)) END AS s,
          |  CONCAT('b', CAST(id % 5 AS STRING)) AS t
          |FROM range(0, 50000)""".stripMargin
      ).repartition(2).write.mode("overwrite").parquet(newTempPath("dict/low"))
      spark.read.parquet(newTempPath("dict/low")).createOrReplaceTempView("dict_low")
      // High-cardinality strings under a tiny dictionary page limit: parquet-java falls back to PLAIN mid-chunk.
      withConf("parquet.dictionary.page.size" -> "2048") {
        spark.sql(
          """SELECT CAST(id AS INT) AS i,
            |  CASE WHEN id % 13 = 0 THEN NULL ELSE CONCAT('key-', CAST(id * 7919 AS STRING)) END AS s
            |FROM range(0, 40000)""".stripMargin
        ).repartition(1).write.mode("overwrite").parquet(newTempPath("dict/fallback"))
      }
      spark.read.parquet(newTempPath("dict/fallback")).createOrReplaceTempView("dict_fallback")
    }
  }

  /** (dictionary-encoded string columns, all string columns) over the batches the native scan of `sql` emits. */
  private def scanColumnKinds(sql: String): (Long, Long) = withPlugin(enabled = true) {
    val df = spark.sql(sql)
    df.collect()
    val scan = PlanUtils.allNodes(finalPlan(df)).collectFirst { case s: VectorParquetScanExec => s }
      .getOrElse(fail("no native scan"))
    val stringOrdinals = scan.output.zipWithIndex.collect {
      case (a, i) if a.dataType == org.apache.spark.sql.types.StringType => i
    }
    scan.executeColumnar().mapPartitions { it =>
      var dict = 0L
      var all = 0L
      it.foreach { b =>
        stringOrdinals.foreach { i =>
          all += 1
          if (b.column(i).isInstanceOf[VectorDictionaryColumnVector]) dict += 1
        }
      }
      Iterator((dict, all))
    }.collect().foldLeft((0L, 0L)) { case ((d, a), (x, y)) => (d + x, a + y) }
  }

  test("dictionary-encoded strings come out of the native scan as dictionary vectors") {
    val (dict, all) = scanColumnKinds("SELECT i, s, t FROM dict_low")
    assert(all > 0 && dict === all, s"$dict of $all string columns were dictionary vectors")
    checkVectorized("SELECT i, s, t FROM dict_low", Seq(node))
  }

  test("filters, aggregates and string functions over dictionary strings return Spark's rows") {
    checkVectorized("SELECT i, s FROM dict_low WHERE s = 'cat-3' OR s IS NULL", Seq(node, classOf[VectorFilterExec]))
    checkVectorized("SELECT s, count(*), sum(i) FROM dict_low GROUP BY s", Seq(node, classOf[VectorHashAggregateExec]))
    checkVectorized("SELECT t, s, count(*) FROM dict_low WHERE i % 3 = 0 GROUP BY t, s", Seq(node))
    checkVectorized("SELECT upper(s), length(s), s LIKE 'cat-1%' FROM dict_low", Seq(node))
    checkVectorized("SELECT count(DISTINCT s) FROM dict_low", Seq(node))
  }

  test("a chunk that falls back from its dictionary mid-way decodes flat from there, with Spark's rows") {
    val (dict, all) = scanColumnKinds("SELECT i, s FROM dict_fallback")
    assert(all > 0, "no string batches")
    assert(dict < all, s"every batch was a dictionary vector ($dict of $all): the file did not fall back")
    checkVectorized("SELECT i, s FROM dict_fallback", Seq(node))
    checkVectorized("SELECT count(*), count(s), max(s), min(s) FROM dict_fallback", Seq(node))
    checkVectorized("SELECT i, s FROM dict_fallback WHERE i % 97 = 1", Seq(node))
  }

  test("a struct's string fields come out as dictionary vectors (#568), with Spark's rows through every consumer") {
    val path = newTempPath("dict/struct")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id AS INT) AS i,
          |  CASE WHEN id % 9 = 0 THEN NULL ELSE named_struct(
          |    'a', CASE WHEN id % 11 = 0 THEN NULL ELSE CONCAT('cat-', CAST(id % 37 AS STRING)) END,
          |    'n', CAST(id AS INT),
          |    'in', named_struct('b', CONCAT('b', CAST(id % 5 AS STRING)), 'm', id)) END AS st,
          |  array(CONCAT('e', CAST(id % 3 AS STRING))) AS l
          |FROM range(0, 50000)""".stripMargin
      ).repartition(2).write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("dict_st")
    }
    // The struct's fields, as the scan emits them: dictionary vectors for its strings (the inner struct's too).
    val kinds = withPlugin(enabled = true) {
      val df = spark.sql("SELECT i, st FROM dict_st")
      df.collect()
      val scan = PlanUtils.allNodes(finalPlan(df)).collectFirst { case s: VectorParquetScanExec => s }
        .getOrElse(fail("no native scan"))
      val o = scan.output.indexWhere(_.name == "st")
      scan.executeColumnar().mapPartitions { it =>
        var dict = 0L
        var all = 0L
        it.foreach { b =>
          val st = b.column(o)
          Seq(st.getChild(0), st.getChild(2).getChild(0)).foreach { f =>
            all += 1
            if (
              io.vecruntime.spark.arrow.VectorNestedColumnVector.unwrapLeaf(
                f
              ).isInstanceOf[VectorDictionaryColumnVector]
            ) dict += 1
          }
        }
        Iterator((dict, all))
      }.collect().foldLeft((0L, 0L)) { case ((d, a), (x, y)) => (d + x, a + y) }
    }
    assert(
      kinds._1 > 0 && kinds._1 == kinds._2,
      s"${kinds._1} of ${kinds._2} struct string fields were dictionary vectors"
    )
    checkVectorized("SELECT i, st FROM dict_st", Seq(node))
    checkVectorized("SELECT st.a, count(*), sum(st.n) FROM dict_st GROUP BY st.a", Seq(node))
    checkVectorized(
      "SELECT st.in.b, st.a, count(*) FROM dict_st WHERE st.a = 'cat-3' OR st.a IS NULL GROUP BY 1, 2",
      Seq(node)
    )
    checkVectorized("SELECT upper(st.a), length(st.in.b), st.a LIKE 'cat-1%' FROM dict_st", Seq(node))
    checkVectorized("SELECT count(DISTINCT st.a), max(st.in.b) FROM dict_st", Seq(node))
    checkVectorized("SELECT i, st, l FROM dict_st WHERE i % 97 = 1 ORDER BY i", Seq(node))
    checkVectorized(
      "SELECT x.st.a, y.st.in.b FROM dict_st x JOIN dict_st y ON x.st.a = y.st.a AND x.i = y.i + 1 WHERE x.i < 2000",
      Seq(node)
    )
    checkVectorized("SELECT st.a, collect_list(st.in.b)[0] FROM dict_st WHERE i < 500 GROUP BY st.a", Seq(node))
  }

  test(
    "the strings of lists and maps come out as dictionary vectors (#568), with Spark's rows through every consumer"
  ) {
    val path = newTempPath("dict/lists")
    withPlugin(enabled = false) {
      spark.sql(
        """SELECT CAST(id AS INT) AS i,
          |  CASE WHEN id % 13 = 0 THEN NULL ELSE array(
          |    CONCAT('e', CAST(id % 7 AS STRING)),
          |    CASE WHEN id % 5 = 0 THEN NULL ELSE CONCAT('f', CAST(id % 3 AS STRING)) END) END AS l,
          |  CASE WHEN id % 17 = 0 THEN NULL ELSE map(
          |    CONCAT('k', CAST(id % 4 AS STRING)),
          |    CASE WHEN id % 6 = 0 THEN NULL ELSE CONCAT('v', CAST(id % 9 AS STRING)) END) END AS m,
          |  array(array(CONCAT('x', CAST(id % 2 AS STRING))), array()) AS ll,
          |  array(named_struct('a', CONCAT('s', CAST(id % 8 AS STRING)), 'n', CAST(id AS INT))) AS ls
          |FROM range(0, 50000)""".stripMargin
      ).repartition(2).write.mode("overwrite").parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("dict_lists")
    }
    // The innermost string vectors, as the scan emits them.
    val kinds = withPlugin(enabled = true) {
      val df = spark.sql("SELECT i, l, m, ll, ls FROM dict_lists")
      df.collect()
      val scan = PlanUtils.allNodes(finalPlan(df)).collectFirst { case s: VectorParquetScanExec => s }
        .getOrElse(fail("no native scan"))
      val cols = Seq("l", "m", "ll", "ls").map(n => scan.output.indexWhere(_.name == n))
      scan.executeColumnar().mapPartitions { it =>
        var dict = 0L
        var all = 0L
        it.foreach { b =>
          val Seq(l, m, ll, ls) = cols.map(b.column)
          Seq(l.getChild(0), m.getChild(0), m.getChild(1), ll.getChild(0).getChild(0), ls.getChild(0).getChild(0))
            .foreach { f =>
              all += 1
              if (
                io.vecruntime.spark.arrow.VectorNestedColumnVector.unwrapLeaf(
                  f
                ).isInstanceOf[VectorDictionaryColumnVector]
              ) dict += 1
            }
        }
        Iterator((dict, all))
      }.collect().foldLeft((0L, 0L)) { case ((d, a), (x, y)) => (d + x, a + y) }
    }
    assert(
      kinds._1 > 0 && kinds._1 == kinds._2,
      s"${kinds._1} of ${kinds._2} list and map string vectors were dictionary vectors"
    )
    checkVectorized("SELECT i, l, m, ll, ls FROM dict_lists", Seq(node))
    checkVectorized("SELECT l[0], l[1], m['k1'], ll[0][0], ls[0].a FROM dict_lists", Seq(node))
    checkVectorized("SELECT e, count(*) FROM dict_lists LATERAL VIEW explode(l) t AS e GROUP BY e", Seq(node))
    checkVectorized("SELECT k, v, count(*) FROM dict_lists LATERAL VIEW explode(m) t AS k, v GROUP BY k, v", Seq(node))
    checkVectorized(
      "SELECT array_contains(l, 'e3'), size(l), concat_ws('|', l), map_keys(m), flatten(ll), " +
        "transform(l, x -> upper(x)), exists(ls, s -> s.a = 's2') FROM dict_lists WHERE i % 89 = 1",
      Seq(node)
    )
    checkVectorized("SELECT l, count(*) FROM dict_lists GROUP BY l", Seq(node))
    checkVectorized("SELECT i, l, m FROM dict_lists WHERE i % 97 = 1 ORDER BY i", Seq(node))
  }

  test("with the switch off the column is flat, with the same rows") {
    withConf(VectorConf.ScanNativeParquetDictionaryStrings -> "false") {
      val (dict, all) = scanColumnKinds("SELECT i, s, t FROM dict_low")
      assert(all > 0 && dict === 0L, s"$dict dictionary vectors with the switch off")
      checkVectorized("SELECT s, count(*) FROM dict_low GROUP BY s", Seq(node))
    }
  }

  test("a LIMIT and many row groups leak neither ids nor dictionaries") {
    val root = VectorAllocators.root()
    val before = root.getAllocatedMemory
    checkVectorized("SELECT s, t FROM dict_low LIMIT 13", Seq(node))
    checkVectorized("SELECT s FROM dict_low WHERE i > 1000 LIMIT 3000", Seq(node))
    checkVectorized("SELECT s, count(*) FROM dict_low GROUP BY s", Seq(node))
    assert(root.getAllocatedMemory === before, s"allocated ${root.getAllocatedMemory} vs $before before")
  }
}
