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
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * Field-id matching (`spark.sql.parquet.fieldId.read.enabled`) and struct fields missing inside lists and maps,
 * in the native Parquet scan (#568 schema evolution). Every query is compared with Spark's vectorized reader,
 * and the native cases must decode row groups themselves:
 *   - columns and struct fields matched by id under other names, in another order, at the top level, in a struct,
 *     in a list of structs and in a map's struct values; an id the file lacks is null;
 *   - names swapped between ids (a requested field named like a sibling's file field): Spark's reader pairs the
 *     fields by name after matching ids, so the native scan leaves such a file to it and gives its outcome;
 *   - a file with no ids: Spark's error, or nulls under `fieldId.read.ignoreMissing`;
 *   - a filter on id-matched columns (row-group pushdown is by name over the file's schema, as Spark's);
 *   - a struct field missing inside a list or a map of structs is null.
 */
class NativeParquetFieldIdSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (4 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "1024"
    )

  private def show(v: Any): String = v match {
    case null => "null"
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case m: scala.collection.Map[_, _] =>
      m.toSeq.map { case (k, x) => s"${show(k)}->${show(x)}" }.sorted.mkString("{", ",", "}")
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  private def check(query: String, native: Boolean = true): Unit = {
    val expected = withPlugin(enabled = false)(rows(spark.sql(query)))
    val (actual, scans) = withPlugin(enabled = true) {
      val df = spark.sql(query)
      val r = rows(df)
      (r, PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s })
    }
    assert(actual == expected, s"$query\nexpected ${expected.take(5)}\nactual   ${actual.take(5)}")
    if (native) {
      assert(scans.nonEmpty, s"$query was not read by the native scan")
      assert(scans.map(_.metrics("numRowGroups").value).sum > 0, s"$query: no row group decoded natively")
    }
  }

  /** The innermost Spark error condition a query fails with, or its rows. */
  private def outcome(query: String, plugin: Boolean): Either[String, Seq[String]] =
    withPlugin(enabled = plugin) {
      try Right(rows(spark.sql(query)))
      catch {
        case e: Throwable =>
          Left(Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collect {
            case s: SparkThrowable if s.getCondition != null => s.getCondition
            case r: RuntimeException if r.getMessage != null && r.getMessage.contains("field Id") => "FIELD_ID"
          }.toSeq.lastOption.getOrElse(e.getClass.getName))
      }
    }

  private def id(n: Long): Metadata = new MetadataBuilder().putLong("parquet.field.id", n).build()

  private def f(name: String, dt: DataType, n: Long): StructField = StructField(name, dt, nullable = true, id(n))

  /** The file's schema, with ids on every field at every depth. */
  private val written = StructType(Seq(
    f("id", LongType, 1),
    f("a", StringType, 2),
    f("s", StructType(Seq(f("x", IntegerType, 4), f("y", StringType, 5))), 3),
    f("l", ArrayType(StructType(Seq(f("p", IntegerType, 7), f("q", StringType, 8)))), 6),
    f("m", MapType(StringType, StructType(Seq(f("u", IntegerType, 10), f("v", DoubleType, 11)))), 9)
  ))

  private def writeWithIds(path: String, v2: Boolean, dictionary: Boolean): Unit = {
    val df = spark.range(0, 3000).selectExpr(
      "id",
      "case when id % 5 = 0 then null else concat('a', id % 31) end",
      "case when id % 7 = 0 then null else named_struct('x', cast(id as int), 'y', concat('y', id % 11)) end",
      "case when id % 9 = 0 then null else array(named_struct('p', cast(id as int), 'q', concat('q', id)), " +
        "cast(null as struct<p: int, q: string>)) end",
      "map(concat('k', id % 3), named_struct('u', cast(id as int), 'v', id / 4.0d))"
    )
    withConf(
      "parquet.writer.version" -> (if (v2) "v2" else "v1"),
      "parquet.enable.dictionary" -> dictionary.toString
    ) {
      withPlugin(enabled = false) {
        spark.createDataFrame(df.rdd, written).repartition(2).write.mode("overwrite").parquet(path)
      }
    }
  }

  private def view(path: String, schema: StructType, name: String = "t"): Unit =
    withPlugin(enabled = false)(spark.read.schema(schema).parquet(path).createOrReplaceTempView(name))

  private def byId[T](body: => T): T = withConf("spark.sql.parquet.fieldId.read.enabled" -> "true")(body)

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"columns and fields matched by id under other names, in another order (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"fid-$v2-$dictionary")
      writeWithIds(path, v2, dictionary)
      byId {
        // Renamed at every depth, reordered, and with ids the file lacks (99, 98, 97): those are null.
        view(
          path,
          StructType(Seq(
            f(
              "mm",
              MapType(
                StringType,
                StructType(Seq(f("vv", DoubleType, 11), f("uu", IntegerType, 10), f("ww", StringType, 97)))
              ),
              9
            ),
            f("ss", StructType(Seq(f("yy", StringType, 5), f("zz", LongType, 98), f("xx", IntegerType, 4))), 3),
            f("key", LongType, 1),
            f("ll", ArrayType(StructType(Seq(f("qq", StringType, 8), f("pp", IntegerType, 7)))), 6),
            f("aa", StringType, 2),
            f("missing", StringType, 99)
          ))
        )
        check("SELECT * FROM t")
        check("SELECT key, aa, ss.xx, ll[0].pp, mm['k1'].vv FROM t WHERE aa = 'a3' OR ss.xx > 2900")
        check("SELECT count(aa), count(missing), count(ss.zz), max(ss.yy), sum(ll[0].pp), sum(mm['k2'].uu) FROM t")
      }
    }
  }

  test("names swapped between ids: Spark's outcome (its reader pairs fields by name after the id match)") {
    val path = newTempPath("fid-swap")
    writeWithIds(path, v2 = false, dictionary = true)
    byId {
      // `a` with id 1 (the file's `id`), `id` with id 2 (the file's `a`), and s's two fields swapped the same way.
      // Spark's reader clips by id but then pairs the file's fields with the read schema's by NAME, so it reads
      // the file's `id` as the requested `id` (a string): a type mismatch. The native scan gives the same outcome
      // (such a file goes to Spark's reader); a different answer would be a different query result.
      view(
        path,
        StructType(Seq(
          f("a", LongType, 1),
          f("id", StringType, 2),
          f("s", StructType(Seq(f("y", IntegerType, 4), f("x", StringType, 5))), 3)
        ))
      )
      val off = outcome("SELECT * FROM t", plugin = false)
      val on = outcome("SELECT * FROM t", plugin = true)
      assert(on == off, s"native $on, Spark $off")
      // Renamed to names the file does not use, the same ids read natively.
      view(path, StructType(Seq(f("k", LongType, 1), f("v", StringType, 2))), "r")
      check("SELECT * FROM r")
      check("SELECT k, v FROM r WHERE v = 'a3'")
    }
  }

  test("a file with no field ids: Spark's error, or nulls under ignoreMissing") {
    val path = newTempPath("fid-none")
    withPlugin(enabled = false) {
      spark.range(0, 1000).selectExpr("id", "concat('a', id) as a").write.mode("overwrite").parquet(path)
    }
    byId {
      view(path, StructType(Seq(f("id", LongType, 1), f("a", StringType, 2))))
      val off = outcome("SELECT * FROM t", plugin = false)
      val on = outcome("SELECT * FROM t", plugin = true)
      assert(off.isLeft, s"Spark's reader should refuse a file with no ids: $off")
      assert(on == off, s"native $on, Spark $off")
      withConf("spark.sql.parquet.fieldId.read.ignoreMissing" -> "true") {
        check("SELECT * FROM t")
      }
    }
  }

  test("a struct field missing inside a list or a map of structs is null") {
    val path = newTempPath("missing-in-list")
    withPlugin(enabled = false) {
      spark.range(0, 3000).selectExpr(
        "id",
        "case when id % 9 = 0 then null else array(named_struct('p', cast(id as int)), " +
          "cast(null as struct<p: int>)) end as l",
        "map(concat('k', id % 3), named_struct('u', cast(id as int))) as m",
        "array(array(named_struct('p', id))) as ll"
      ).repartition(2).write.mode("overwrite").parquet(path)
    }
    view(
      path,
      StructType.fromDDL(
        "id bigint, l array<struct<p: int, q: string, r: struct<s: int>>>, m map<string, struct<u: int, w: double>>, " +
          "ll array<array<struct<p: bigint, n: array<int>>>>"
      )
    )
    check("SELECT * FROM t")
    check("SELECT id, l[0].q, l[0].r, m['k1'].u, m['k1'].w, ll[0][0].p, ll[0][0].n FROM t WHERE l[0].p > 10")
    // Pruned to the missing fields alone, the file holds none of the struct's fields: Spark 4.1 reads one more
    // file field for the struct's nulls, which our readers do not, so the file stays on Spark's reader.
    check("SELECT id, m['k1'].w FROM t", native = false)
    check("SELECT count(l[0].q), count(m['k2'].w), sum(m['k2'].u), sum(l[0].p) FROM t")
  }
}
