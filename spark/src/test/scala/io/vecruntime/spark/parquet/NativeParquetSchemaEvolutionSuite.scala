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
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorParquetScanExec}

/**
 * Schema evolution in the native Parquet scan (#568): a table read with a schema newer than some of its files,
 * every query compared with Spark's vectorized reader and the native cases asserted to decode row groups:
 *   - columns the file does not hold (flat, struct, list, map, wide decimal) are null in every row;
 *   - struct fields the file does not hold (flat, a struct, a list), at any struct depth, are null, with the
 *     struct's own nulls taken from a field the file holds;
 *   - type widening as Spark's ParquetVectorUpdaterFactory does it: INT32 and FLOAT as double, DATE as
 *     timestamp_ntz (with the file's date rebase), decimals at a larger scale and precision, signed integers
 *     as decimals; flat, in a struct and in a list; Spark's and parquet-java's files;
 *   - what stays on Spark's reader: a column with an existence default,
 *     an integer read as a decimal wider than 18 digits. Spark's rows either way.
 */
class NativeParquetSchemaEvolutionSuite extends VectorQuerySuite {

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

  /** Row groups the native scan decoded for a query. */
  private def nativeRowGroups(query: String): Long = withPlugin(enabled = true) {
    val df = spark.sql(query)
    df.collect()
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorParquetScanExec => s.metrics("numRowGroups").value }.sum
  }

  private def write(df: DataFrame, path: String, v2: Boolean, dictionary: Boolean): Unit =
    withConf(
      "parquet.writer.version" -> (if (v2) "v2" else "v1"),
      "parquet.enable.dictionary" -> dictionary.toString,
      "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
    ) {
      withPlugin(enabled = false)(df.repartition(2).write.mode("overwrite").parquet(path))
    }

  /** A view over `path` read with the schema `ddl` (Spark's reader settles the plan; the query is ours). */
  private def view(path: String, ddl: String, name: String = "t"): Unit =
    withPlugin(enabled = false)(spark.read.schema(ddl).parquet(path).createOrReplaceTempView(name))

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"columns the file does not hold are null in every row (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"missing-$v2-$dictionary")
      // The old file: id and a. The new file: every column.
      write(
        spark.range(0, 3000).selectExpr("id", "case when id % 5 = 0 then null else concat('a', id % 31) end as a"),
        path + "/old",
        v2,
        dictionary
      )
      write(
        spark.range(3000, 6000).selectExpr(
          "id",
          "concat('a', id % 31) as a",
          "case when id % 3 = 0 then null else concat('b', id) end as b",
          "named_struct('x', cast(id as int), 'y', concat('y', id % 7)) as c",
          "array(cast(id as int), cast(null as int)) as d",
          "map(concat('k', id % 3), cast(id as int)) as e",
          "cast(id * 7 / 3 as decimal(20,2)) as f",
          "cast(id as double) as g"
        ),
        path + "/new",
        v2,
        dictionary
      )
      val ddl = "id bigint, a string, b string, c struct<x: int, y: string>, d array<int>, e map<string, int>, " +
        "f decimal(20,2), g double"
      view(path + "/{old,new}", ddl)
      check("SELECT * FROM t")
      check("SELECT id, b, g FROM t WHERE b IS NULL AND id % 4 = 0")
      check("SELECT count(b), count(c), count(c.y), count(d), count(e), count(f), sum(g), max(a) FROM t")
      check("SELECT id FROM t WHERE c.x > 4000 OR g IS NULL AND id % 9 = 0")
      // The old file alone: every new column null, decoded natively.
      view(path + "/old", ddl, "o")
      check("SELECT * FROM o")
      assert(nativeRowGroups("SELECT b, c, d, e, f FROM o") > 0, "a file missing columns fell over")
    }

    test(s"struct fields the file does not hold are null, at any depth (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"missing-fields-$v2-$dictionary")
      write(
        spark.range(0, 3000).selectExpr(
          "id",
          "case when id % 6 = 0 then null else named_struct('x', cast(id as int), " +
            "'z', case when id % 4 = 0 then null else named_struct('p', id) end) end as s"
        ),
        path + "/old",
        v2,
        dictionary
      )
      write(
        spark.range(3000, 6000).selectExpr(
          "id",
          "named_struct('x', cast(id as int), 'y', concat('y', id), " +
            "'z', named_struct('p', id, 'q', cast(id as double) / 2), 'w', array(id)) as s"
        ),
        path + "/new",
        v2,
        dictionary
      )
      val ddl = "id bigint, s struct<x: int, y: string, z: struct<p: bigint, q: double>, w: array<bigint>>"
      view(path + "/{old,new}", ddl)
      check("SELECT * FROM t")
      check("SELECT id, s.y, s.z.q, s.w FROM t WHERE s IS NULL OR s.z IS NULL OR s.y IS NOT NULL")
      check("SELECT count(s), count(s.y), count(s.z), count(s.z.q), count(s.w), max(s.x) FROM t")
      view(path + "/old", ddl, "o")
      assert(nativeRowGroups("SELECT s FROM o") > 0, "a file missing struct fields fell over")
    }
  }

  test("a struct's nulls come from a field the file holds when its first field is missing") {
    val path = newTempPath("missing-first-field")
    write(
      spark.range(0, 2000).selectExpr(
        "id",
        "case when id % 3 = 0 then null else named_struct('b', concat('b', id)) end as s"
      ),
      path,
      v2 = false,
      dictionary = true
    )
    view(path, "id bigint, s struct<a: int, b: string, c: struct<d: int>>")
    check("SELECT * FROM t")
    check("SELECT id, s IS NULL, s.a, s.b FROM t")
    // Pruned to s.a and s.c, the file holds none of the struct's fields: Spark 4.1 reads one more of the file's
    // fields for the struct's nulls, which our readers do not, so this file stays on Spark's reader.
    check("SELECT id, s IS NULL, s.a, s.c FROM t", native = false)
  }

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"type widening as Spark widens: double, timestamp_ntz, decimals (v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"widen-$v2-$dictionary")
      write(
        spark.range(0, 3000).selectExpr(
          "id",
          "case when id % 7 = 0 then null else cast(id * 1000003 - 1500000000 as int) end as i",
          "case when id % 5 = 0 then null else cast(id / 7.0 as float) end as f",
          "case when id % 11 = 0 then null else date_add(date'1900-01-01', cast(id * 13 as int)) end as d",
          "cast((id - 1500) * 3 / 100 as decimal(5,2)) as d32",
          "cast(id * 1000003 / 100 as decimal(12,2)) as d64",
          "cast(id * 1000003 / 100 as decimal(22,2)) as d128",
          "cast(id % 100 as int) as small",
          "named_struct('i', cast(id as int), 'f', cast(id / 3.0 as float)) as st",
          "array(cast(id / 9.0 as float), cast(null as float)) as fl"
        ),
        path,
        v2,
        dictionary
      )
      view(
        path,
        "id bigint, i double, f double, d timestamp_ntz, d32 decimal(8,5), d64 decimal(15,4), d128 decimal(30,6), " +
          "small decimal(12,2), st struct<i: double, f: double>, fl array<double>"
      )
      check("SELECT * FROM t")
      check("SELECT id, i, f, d FROM t WHERE i > 0 AND f < 300.0 OR d < timestamp_ntz'1901-01-01 00:00:00'")
      check("SELECT sum(i), sum(f), min(d), max(d), sum(d32), sum(d64), sum(d128), sum(small), sum(st.f), " +
        "sum(fl[0]) FROM t")
    }
  }

  test("legacy decimals (FIXED_LEN_BYTE_ARRAY) read at a larger scale") {
    val path = newTempPath("widen-legacy-decimal")
    withConf("spark.sql.parquet.writeLegacyFormat" -> "true") {
      write(
        spark.range(0, 3000).selectExpr(
          "id",
          "case when id % 7 = 0 then null else cast((id - 1500) * 3 / 100 as decimal(5,2)) end as a",
          "cast(id * 1000003 / 100 as decimal(12,2)) as b",
          "cast(-id * 1000003 / 100 as decimal(22,2)) as c"
        ),
        path,
        v2 = false,
        dictionary = true
      )
    }
    view(path, "id bigint, a decimal(9,4), b decimal(30,9), c decimal(38,10)")
    check("SELECT * FROM t")
    check("SELECT sum(a), sum(b), sum(c) FROM t WHERE a IS NOT NULL")
  }

  test("a DATE read as timestamp_ntz from a LEGACY-calendar file is rebased first") {
    val path = newTempPath("widen-legacy-date")
    withConf("spark.sql.parquet.datetimeRebaseModeInWrite" -> "LEGACY") {
      write(
        spark.range(0, 2000).selectExpr(
          "id",
          "case when id % 9 = 0 then null else date_add(date'1000-01-01', cast(id * 211 as int)) end as d"
        ),
        path,
        v2 = false,
        dictionary = false
      )
    }
    view(path, "id bigint, d timestamp_ntz")
    check("SELECT * FROM t")
    check("SELECT min(d), max(d) FROM t")
  }

  test("parquet-java integers widened: INT32 as double and as decimal, INT(16) as decimal") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message m { required int32 id; optional int32 a; optional int32 b (INTEGER(16,true)); optional float c; }"
    )
    for (version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0)) {
      val path = newTempPath(s"pj-widen-$version")
      new java.io.File(path).mkdirs()
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
        .withConf(new org.apache.hadoop.conf.Configuration())
        .withType(schema)
        .withWriterVersion(version)
        .withPageSize(1024)
        .withRowGroupSize(16 * 1024L)
        .build()
      val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
      try {
        for (k <- 0 until 3000) {
          val g = factory.newGroup().append("id", k)
          if (k % 7 != 0) g.append("a", k * 700001 - 1000000000)
          if (k % 5 != 0) g.append("b", (k % 30000) - 15000)
          if (k % 3 != 0) g.append("c", k / 7.0f)
          writer.write(g)
        }
      } finally writer.close()
      for (
        (ddl, name) <- Seq(
          "id int, a double, b double, c double" -> "dbl",
          "id int, a decimal(12,2), b decimal(18,6), c double" -> "dec"
        )
      ) {
        view(path, ddl, name)
        check(s"SELECT * FROM $name")
        check(s"SELECT sum(a), sum(b), sum(c) FROM $name")
      }
    }
  }

  test("evolution the native readers leave to Spark's reader: Spark's rows") {
    val path = newTempPath("evolve-fallback")
    write(
      spark.range(0, 2000).selectExpr(
        "id",
        "cast(id as int) as i",
        "array(named_struct('x', cast(id as int))) as l"
      ),
      path,
      v2 = false,
      dictionary = true
    )
    // A field missing inside a list of structs: null in each element, read natively by the tree reader since the
    // field-id slice (NativeParquetFieldIdSuite has the full cases).
    view(path, "id bigint, l array<struct<x: int, y: string>>")
    check("SELECT * FROM t")
    // An INT32 read as a decimal of more than 18 digits (the DECIMAL128 lane takes binary decimals only).
    view(path, "id bigint, i decimal(25,3)", "w")
    check("SELECT * FROM w", native = false)
    // A column added with a default: Spark fills the default in the files without it.
    try {
      withPlugin(enabled = false) {
        spark.sql("DROP TABLE IF EXISTS evolve_default")
        spark.sql("CREATE TABLE evolve_default (id int) USING parquet")
        spark.sql("INSERT INTO evolve_default SELECT cast(id as int) FROM range(0, 500)")
        spark.sql("ALTER TABLE evolve_default ADD COLUMN v int DEFAULT 42")
        spark.sql("INSERT INTO evolve_default SELECT cast(id as int), cast(id as int) FROM range(500, 600)")
      }
      check("SELECT * FROM evolve_default", native = false)
      check("SELECT count(*), sum(v) FROM evolve_default", native = false)
    } finally withPlugin(enabled = false)(spark.sql("DROP TABLE IF EXISTS evolve_default"))
  }
}
