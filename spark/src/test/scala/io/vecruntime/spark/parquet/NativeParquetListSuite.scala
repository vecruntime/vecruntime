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
 * Lists and maps in the native Parquet scan (#568, work item 8): top-level `array<leaf>` and
 * `map<leaf, leaf>` columns. Every query is compared with Spark's row-based parquet-java reader (vectorized
 * reader off, plugin off), and a query over such columns must be read by [[VectorParquetScanExec]] with row
 * groups decoded natively.
 *
 * Writers: Spark's (three-level lists, and the legacy form with `writeLegacyFormat`; v1 and v2 pages,
 * dictionary on and off; lists longer than a page), parquet-java's `ExampleParquetWriter` (the two-level
 * list, a bare repeated leaf, required and optional elements and values), Hardwood's dictionary-encoded list
 * spanning batches, and the `apache/parquet-testing` list and map files.
 */
class NativeParquetListSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(
      VectorConf.ScanNativeParquet -> "true",
      "parquet.block.size" -> (64 * 1024).toString,
      "parquet.page.size" -> (2 * 1024).toString,
      "spark.sql.parquet.columnarReaderBatchSize" -> "777"
    )

  private def resource(rel: String): String = {
    val url = getClass.getClassLoader.getResource(s"parquet-testing/$rel")
    assert(url != null, s"missing test resource parquet-testing/$rel")
    new java.io.File(url.toURI).getAbsolutePath
  }

  private def show(v: Any): String = v match {
    case null => "null"
    case b: Array[Byte] => b.map("%02x".format(_)).mkString("0x", "", "")
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case m: scala.collection.Map[_, _] =>
      m.map { case (k, x) => show(k) + "->" + show(x) }.toSeq.sorted.mkString("{", ",", "}")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case d: java.lang.Double => if (d.isNaN) "NaN" else d.toString
    case f: java.lang.Float => if (f.isNaN) "NaN" else f.toString
    case x => x.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().map(show).toSeq.sorted

  private def reference(query: String): Seq[String] =
    withConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
      withPlugin(enabled = false)(rows(spark.sql(query)))
    }

  /** `query` returns Spark's rows; with `native`, the native scan read it and decoded row groups itself. */
  private def check(query: String, native: Boolean = true): Unit = {
    val expected = reference(query)
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

  /** Lists of every lane type: null lists, empty lists, null elements, and lists longer than a page. */
  private def lists: DataFrame =
    spark.range(0, 5000).selectExpr(
      "cast(id as int) as id",
      "case when id % 9 = 0 then null when id % 9 = 1 then array() else " +
        "transform(sequence(0, cast(id % 7 as int)), k -> case when k = 3 then null else cast(id + k as int) end) end as ai",
      "case when id % 11 = 0 then null else transform(sequence(0, cast(id % 4 as int)), k -> concat('s', (id + k) % 13)) end as astr",
      "transform(sequence(0, cast(id % 3 as int)), k -> (id * k) * 1000003) as al",
      "case when id % 5 = 0 then array() else transform(sequence(0, cast(id % 2 as int)), k -> id / (k + 3.0)) end as ad",
      "transform(sequence(0, cast(id % 3 as int)), k -> case when k = 1 then null else (id + k) % 2 = 0 end) as ab",
      "transform(sequence(0, cast(id % 2 as int)), k -> cast((id + k) * 3 / 100 as decimal(9,2))) as adec",
      "transform(sequence(0, cast(id % 2 as int)), k -> cast((id + k) * 123456789012 as decimal(25,3))) as awdec",
      "transform(sequence(0, cast(id % 3 as int)), k -> date_add(date'2021-01-01', cast(id % 300 + k as int))) as adt",
      "transform(sequence(0, cast(id % 2 as int)), k -> timestamp_micros(1600000000000000 + (id + k) * 1000)) as ats",
      "transform(sequence(0, cast(id % 2 as int)), k -> cast((id + k) / 7.0 as float)) as af",
      "transform(sequence(0, cast(id % 2 as int)), k -> cast((id + k) % 100 as tinyint)) as ati",
      "case when id % 100 = 7 then sequence(0, 2000) else array(cast(id as int)) end as along", // past a page
      "case when id % 13 = 0 then null when id % 13 = 1 then map() else " +
        "map_from_arrays(transform(sequence(0, cast(id % 3 as int)), k -> concat('k', k)), " +
        "transform(sequence(0, cast(id % 3 as int)), k -> case when k = 2 then null else id + k end)) end as m",
      "map_from_arrays(array(cast(id as int), cast(id + 1 as int)), array(concat('v', id), cast(null as string))) as mi"
    )

  for (legacy <- Seq(false, true); v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(s"Spark-written lists and maps of every lane type (legacy=$legacy, v2=$v2, dictionary=$dictionary)") {
      val path = newTempPath(s"lists-$legacy-$v2-$dictionary")
      withConf(
        "spark.sql.parquet.writeLegacyFormat" -> legacy.toString,
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString,
        "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
      ) {
        lists.repartition(2).write.mode("overwrite").parquet(path)
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, ai, m FROM t WHERE size(ai) > 2")
      check("SELECT id, size(ai) n, get(ai, 1) e1, element_at(astr, -1) last, exists(ai, x -> x IS NULL) en, " +
        "filter(ai, x -> x > id) f, transform(al, x -> x + 1) t, map_keys(m) k, map_values(m) v FROM t")
      check("SELECT count(*), sum(size(along)), sum(size(m)), max(array_max(adt)) FROM t")
      check("SELECT id, x FROM t LATERAL VIEW explode(ai) e AS x")
      check("SELECT id, k, v FROM t LATERAL VIEW explode(m) e AS k, v")
      check("SELECT * FROM t DISTRIBUTE BY id % 5")
    }
  }

  test("parquet-java's writer: two-level and three-level lists, a bare repeated leaf, maps, v1 and v2") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      """message m {
        |  required int32 id;
        |  optional group two (LIST) { repeated int32 element; }
        |  required group three (LIST) { repeated group list { required binary element (STRING); } }
        |  optional group threeopt (LIST) { repeated group list { optional int64 element; } }
        |  repeated double bare;
        |  optional group mp (MAP) { repeated group key_value { required int32 key; optional binary value (STRING); } }
        |}""".stripMargin
    )
    for (version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0); dict <- Seq(true, false)) {
      val path = newTempPath(s"pj-list-$version-$dict")
      new java.io.File(path).mkdirs()
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(new org.apache.hadoop.fs.Path(path + "/part-0.parquet"))
        .withConf(new org.apache.hadoop.conf.Configuration())
        .withType(schema)
        .withWriterVersion(version)
        .withDictionaryEncoding(dict)
        .withPageSize(1024)
        .withRowGroupSize(32 * 1024L)
        .build()
      val factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
      try {
        for (k <- 0 until 4000) {
          val g = factory.newGroup().append("id", k)
          if (k % 7 != 0) {
            val two = g.addGroup("two")
            for (j <- 0 until k % 4) two.append("element", k * 10 + j)
          }
          val three = g.addGroup("three")
          for (j <- 0 until (k % 3)) three.addGroup("list").append("element", s"e${(k + j) % 17}")
          if (k % 5 != 1) {
            val t = g.addGroup("threeopt")
            for (j <- 0 until k % 3) {
              val l = t.addGroup("list")
              if ((k + j) % 4 != 0) l.append("element", k.toLong * j)
            }
          }
          for (j <- 0 until k % 3) g.append("bare", k / (j + 2.0))
          if (k % 6 != 0) {
            val mp = g.addGroup("mp")
            for (j <- 0 until k % 3) {
              val kv = mp.addGroup("key_value").append("key", j)
              if (j != 1) kv.append("value", s"v${k % 23}")
            }
          }
          writer.write(g)
        }
      } finally writer.close()
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      withClue(s"parquet-java $version dictionary=$dict") {
        check("SELECT * FROM t")
        check("SELECT id, size(two), size(three), size(threeopt), size(bare), size(mp) FROM t")
        check("SELECT id, x FROM t LATERAL VIEW explode(three) e AS x")
        check("SELECT id, mp[1], map_keys(mp) FROM t WHERE size(threeopt) > 0")
      }
    }
  }

  test("Hardwood's dictionary-encoded list column across batches, read natively") {
    val path = newTempPath("dict-list")
    spark.range(0, 20000).selectExpr(
      "cast(id as int) as i",
      "case when id % 13 = 0 then null else transform(sequence(0, cast(id % 6 as int)), k -> " +
        "case when k = 2 then null else concat('v', (id + k) % 5) end) end as tags"
    ).repartition(1).write.option("parquet.block.size", 1 << 20).mode("overwrite").parquet(path)
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
    check("SELECT * FROM t")
    check(
      "SELECT i, size(tags) n, get(tags, 1) t1, array_contains(tags, 'v3') c3, filter(tags, s -> s <> 'v1') f FROM t"
    )
    check("SELECT i FROM t WHERE array_contains(tags, 'v2') AND size(tags) > 3")
  }

  test("parquet-testing list and map files answer as Spark does; the lists of leaves are read natively") {
    for (file <- Seq("list_columns.parquet", "null_list.parquet", "repeated_primitive_no_list.parquet")) {
      withPlugin(enabled = false)(spark.read.parquet(resource(s"data/$file")).createOrReplaceTempView("t"))
      withClue(file)(check("SELECT * FROM t", native = file != "repeated_primitive_no_list.parquet"))
    }
    // A top-level repeated primitive (no LIST annotation) is read natively; its repeated group is a struct of lists.
    withPlugin(enabled = false) {
      spark.read.parquet(resource("data/repeated_primitive_no_list.parquet")).createOrReplaceTempView("t")
    }
    val tops = withPlugin(enabled = false)(spark.table("t").schema.fields.filterNot(_.dataType.isInstanceOf[
      org.apache.spark.sql.types.StructType
    ]).map(f => s"`${f.name}`"))
    check(s"SELECT ${tops.mkString(", ")} FROM t")
    for (
      (file, ddl) <- Seq(
        "nested_maps.snappy.parquet" -> None, // a map of maps: Spark's scan
        "nested_lists.snappy.parquet" -> None, // a list of lists of lists: native (its own test below)
        "old_list_structure.parquet" -> None, // the legacy LIST<LIST<int32>>: native (its own test below)
        "incorrect_map_schema.parquet" -> None,
        "map_no_value.parquet" -> Some("my_map map<int, int>, my_list array<int>")
      )
    ) {
      withPlugin(enabled = false) {
        ddl.fold(spark.read.parquet(resource(s"data/$file")))(d =>
          spark.read.schema(d).parquet(resource(s"data/$file"))
        )
          .createOrReplaceTempView("t")
      }
      withClue(file)(check("SELECT * FROM t", native = false))
    }
  }

  for (legacy <- Seq(false, true); v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(
      s"Spark-written lists of structs: nulls at the list, the struct and the field (legacy=$legacy, v2=$v2, dictionary=$dictionary)"
    ) {
      val path = newTempPath(s"ls-$legacy-$v2-$dictionary")
      withConf(
        "spark.sql.parquet.writeLegacyFormat" -> legacy.toString,
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString,
        "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 5000).selectExpr(
            "id",
            "case when id % 9 = 0 then null when id % 9 = 1 then array() else " +
              "transform(sequence(0, cast(id % 5 as int)), k -> case when k = 2 then null else named_struct(" +
              "'a', case when k = 3 then null else cast(id + k as int) end, " +
              "'s', concat('s', (id + k) % 11), 'd', (id + k) / 4.0, " +
              "'ts', timestamp_micros(id * 1000 + k), 'b', k % 2 = 0) end) end as ls"
          ).repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, size(ls), transform(ls, x -> x.a) a, filter(ls, x -> x.s = 's3') f FROM t")
      check("SELECT id, e.a, e.s FROM t LATERAL VIEW explode(ls) x AS e")
      check("SELECT id, ls FROM t WHERE exists(ls, x -> x.b) AND id % 3 = 1")
      check("SELECT id % 7 k, count(*), sum(size(ls)) FROM t GROUP BY 1")
    }
  }

  test("parquet-java lists of structs: the three-level LIST and the legacy two-level repeated group") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val shapes = Seq(
      "three" -> """message m { required int32 id;
        |  optional group ls (LIST) { repeated group list { optional group element {
        |    optional int32 a; optional binary s (STRING); } } } }""".stripMargin,
      "two" -> """message m { required int32 id;
        |  optional group ls (LIST) { repeated group array { required int32 a; optional binary s (STRING); } } }""".stripMargin
    )
    for ((shape, text) <- shapes; version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0)) {
      val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(text)
      val path = newTempPath(s"pj-ls-$shape-$version")
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
        for (k <- 0 until 4000) {
          val g = factory.newGroup().append("id", k)
          if (k % 7 != 0) {
            val ls = g.addGroup("ls")
            for (j <- 0 until k % 4) {
              if (shape == "three") {
                val e = ls.addGroup("list")
                if ((k + j) % 5 != 0) {
                  val el = e.addGroup("element")
                  if ((k + j) % 3 != 0) el.append("a", k * 10 + j)
                  if ((k + j) % 4 != 1) el.append("s", s"s${(k + j) % 9}")
                }
              } else {
                val el = ls.addGroup("array").append("a", k * 10 + j)
                if ((k + j) % 4 != 1) el.append("s", s"s${(k + j) % 9}")
              }
            }
          }
          writer.write(g)
        }
      } finally writer.close()
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, transform(ls, x -> x.a) a, size(ls) n FROM t WHERE id % 5 = 2")
    }
  }

  for (legacy <- Seq(false, true); v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(
      s"Spark-written lists of lists, two and three deep, nulls at every depth (legacy=$legacy, v2=$v2, dictionary=$dictionary)"
    ) {
      val path = newTempPath(s"ll-$legacy-$v2-$dictionary")
      withConf(
        "spark.sql.parquet.writeLegacyFormat" -> legacy.toString,
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 5000).selectExpr(
            "id",
            // array<array<int>>: a null outer list, an empty one, a null inner list, an empty inner list, null leaves.
            "case when id % 9 = 0 then null when id % 9 = 1 then array() else " +
              "transform(sequence(0, cast(id % 4 as int)), k -> case when k = 1 then null when k = 2 then array() " +
              "else transform(sequence(0, cast((id + k) % 3 as int)), j -> case when j = 1 then null " +
              "else cast(id * 10 + k + j as int) end) end) end as ll",
            // array<array<array<string>>>
            "case when id % 7 = 0 then null else transform(sequence(0, cast(id % 3 as int)), a -> " +
              "case when a = 2 then null else transform(sequence(0, cast((id + a) % 2 as int)), b -> " +
              "transform(sequence(0, cast((id + a + b) % 3 as int)), c -> concat('s', id % 13, a, b, c))) end) end as lll"
          ).repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, size(ll), transform(ll, x -> size(x)) s, flatten(ll) f FROM t")
      check("SELECT id, x FROM t LATERAL VIEW explode(ll) e AS x WHERE id % 5 = 1")
      check("SELECT id, ll FROM t WHERE exists(ll, x -> exists(x, y -> y % 7 = 0))")
      check("SELECT id % 7 k, count(*), sum(size(lll)) FROM t GROUP BY 1")
    }
  }

  test("parquet-java lists of lists: the three-level LIST of LIST and the legacy two-level form") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val shapes = Seq(
      "three" -> """message m { required int32 id;
        |  optional group ll (LIST) { repeated group list { optional group element (LIST) {
        |    repeated group list { optional int64 element; } } } } }""".stripMargin,
      "two" -> """message m { required int32 id;
        |  optional group ll (LIST) { repeated group array (LIST) { repeated int64 array; } } }""".stripMargin
    )
    for ((shape, text) <- shapes; version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0)) {
      val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(text)
      val path = newTempPath(s"pj-ll-$shape-$version")
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
        for (k <- 0 until 4000) {
          val g = factory.newGroup().append("id", k)
          if (k % 6 != 0) {
            val ll = g.addGroup("ll")
            for (i <- 0 until k % 4) {
              if (shape == "three") {
                val outer = ll.addGroup("list")
                if ((k + i) % 5 != 0) {
                  val inner = outer.addGroup("element")
                  for (j <- 0 until (k + i) % 3) {
                    val e = inner.addGroup("list")
                    if ((k + i + j) % 4 != 1) e.append("element", (k * 100 + i * 10 + j).toLong)
                  }
                }
              } else {
                val inner = ll.addGroup("array")
                for (j <- 0 until (k + i) % 3) inner.append("array", (k * 100 + i * 10 + j).toLong)
              }
            }
          }
          writer.write(g)
        }
      } finally writer.close()
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, transform(ll, x -> size(x)) s, flatten(ll) f FROM t WHERE id % 5 = 2")
    }
  }

  test("parquet-testing lists of lists: nested_lists and the legacy old_list_structure, read natively") {
    for (file <- Seq("nested_lists.snappy.parquet", "old_list_structure.parquet")) {
      withPlugin(enabled = false)(spark.read.parquet(resource(s"data/$file")).createOrReplaceTempView("t"))
      val lists = withPlugin(enabled = false)(spark.table("t").schema.fields.filter(f =>
        NativeParquetSupport.isReadable(f.dataType)
      ).map(f => s"`${f.name}`"))
      withClue(file)(check(s"SELECT ${lists.mkString(", ")} FROM t"))
    }
  }

  for (legacy <- Seq(false, true); v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(
      s"Spark-written lists and maps inside structs, nulls at the struct and inside (legacy=$legacy, v2=$v2, dictionary=$dictionary)"
    ) {
      val path = newTempPath(s"sl-$legacy-$v2-$dictionary")
      withConf(
        "spark.sql.parquet.writeLegacyFormat" -> legacy.toString,
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 5000).selectExpr(
            "id",
            // A struct whose FIRST field is a list (its nulls come from the list's levels), then a leaf and a map.
            "case when id % 11 = 0 then null else named_struct(" +
              "'tags', case when id % 4 = 0 then null else transform(sequence(0, cast(id % 3 as int)), " +
              "k -> case when k = 1 then null else concat('t', (id + k) % 7) end) end, " +
              "'n', cast(id as int), " +
              "'attrs', case when id % 5 = 0 then null else map_from_arrays(" +
              "transform(sequence(0, cast(id % 2 as int)), k -> concat('a', k)), " +
              "transform(sequence(0, cast(id % 2 as int)), k -> case when k = 1 then null else id + k end)) end) end as s",
            // A struct holding a struct holding a list of lists.
            "named_struct('inner', case when id % 6 = 0 then null else named_struct(" +
              "'ll', transform(sequence(0, cast(id % 2 as int)), k -> array(cast(id + k as int), null)))  end, " +
              "'x', id % 9) as d"
          ).repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, s.tags, size(s.tags), s.n, s.attrs['a0'] a0, map_keys(s.attrs) k FROM t")
      check("SELECT id, d.inner.ll, flatten(d.inner.ll) f, d.x FROM t WHERE s IS NULL OR s.n % 3 = 1")
      check("SELECT id, tag FROM t LATERAL VIEW explode(s.tags) e AS tag")
      check("SELECT d.x, count(*), sum(size(s.tags)) FROM t GROUP BY d.x")
    }
  }

  test("parquet-java structs holding a three-level LIST, a legacy two-level list and a MAP") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      """message m { required int32 id;
        |  optional group s {
        |    optional group l (LIST) { repeated group list { optional int32 element; } }
        |    required int32 n;
        |    optional group legacy (LIST) { repeated binary array (STRING); }
        |    optional group m (MAP) { repeated group key_value { required binary key (STRING); optional int64 value; } }
        |  } }""".stripMargin
    )
    for (version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0)) {
      val path = newTempPath(s"pj-sl-$version")
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
        for (k <- 0 until 4000) {
          val g = factory.newGroup().append("id", k)
          if (k % 7 != 0) {
            val s = g.addGroup("s")
            if (k % 3 != 0) {
              val l = s.addGroup("l")
              for (j <- 0 until k % 4) {
                val e = l.addGroup("list")
                if ((k + j) % 5 != 0) e.append("element", k * 10 + j)
              }
            }
            s.append("n", k)
            if (k % 4 != 1) {
              val lg = s.addGroup("legacy")
              for (j <- 0 until k % 3) lg.append("array", s"v${(k + j) % 9}")
            }
            if (k % 5 != 2) {
              val m = s.addGroup("m")
              for (j <- 0 until k % 3) {
                val kv = m.addGroup("key_value").append("key", s"k$j")
                if ((k + j) % 4 != 0) kv.append("value", (k * 100 + j).toLong)
              }
            }
          }
          writer.write(g)
        }
      } finally writer.close()
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, s.l, s.legacy, s.m['k1'] FROM t WHERE id % 5 = 3")
    }
  }

  for (v2 <- Seq(false, true); dictionary <- Seq(true, false)) {
    test(
      s"Spark-written maps of structs: nulls at the map, the value struct and its fields (v2=$v2, dictionary=$dictionary)"
    ) {
      val path = newTempPath(s"ms-$v2-$dictionary")
      withConf(
        "parquet.writer.version" -> (if (v2) "v2" else "v1"),
        "parquet.enable.dictionary" -> dictionary.toString,
        "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS"
      ) {
        withPlugin(enabled = false) {
          spark.range(0, 5000).selectExpr(
            "id",
            "case when id % 9 = 0 then null when id % 9 = 1 then map() else map_from_arrays(" +
              "transform(sequence(0, cast(id % 4 as int)), k -> concat('k', k)), " +
              "transform(sequence(0, cast(id % 4 as int)), k -> case when k = 2 then null else named_struct(" +
              "'a', case when k = 3 then null else cast(id + k as int) end, 's', concat('s', (id + k) % 11), " +
              "'d', (id + k) / 4.0, 'ts', timestamp_micros(id * 1000 + k)) end)) end as ms",
            // A map of structs inside a struct.
            "named_struct('m', map(cast(id % 5 as int), named_struct('x', id, 'y', concat('y', id % 3))), " +
              "'n', cast(id as int)) as sm"
          ).repartition(2).write.mode("overwrite").parquet(path)
        }
      }
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, ms['k1'] v, ms['k1'].a a, map_keys(ms) k, size(ms) n FROM t")
      check("SELECT id, k, v.s FROM t LATERAL VIEW explode(ms) e AS k, v WHERE id % 3 = 1")
      check("SELECT id, sm.m[cast(id % 5 as int)].y, sm.n FROM t")
      check("SELECT id % 7 g, count(*), sum(size(ms)) FROM t GROUP BY 1")
    }
  }

  test("parquet-java maps of structs: a MAP whose value is an optional group, v1 and v2") {
    import org.apache.parquet.column.ParquetProperties.WriterVersion
    val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      """message m { required int32 id;
        |  optional group ms (MAP) { repeated group key_value { required binary key (STRING);
        |    optional group value { optional int32 a; optional binary s (STRING); } } } }""".stripMargin
    )
    for (version <- Seq(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0)) {
      val path = newTempPath(s"pj-ms-$version")
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
        for (k <- 0 until 4000) {
          val g = factory.newGroup().append("id", k)
          if (k % 7 != 0) {
            val m = g.addGroup("ms")
            for (j <- 0 until k % 4) {
              val kv = m.addGroup("key_value").append("key", s"k$j")
              if ((k + j) % 5 != 0) {
                val v = kv.addGroup("value")
                if ((k + j) % 3 != 0) v.append("a", k * 10 + j)
                if ((k + j) % 4 != 1) v.append("s", s"s${(k + j) % 9}")
              }
            }
          }
          writer.write(g)
        }
      } finally writer.close()
      withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
      check("SELECT * FROM t")
      check("SELECT id, ms['k1'].a, ms['k2'].s FROM t WHERE id % 5 = 2")
    }
  }

  test(
    "a list of structs holding a list, a list of lists of structs and a map of structs holding a list keep Spark's scan"
  ) {
    val path = newTempPath("deep-lists")
    spark.range(0, 400).selectExpr(
      "cast(id as int) as id",
      "array(array(cast(id as int)), array()) as ll",
      "array(named_struct('a', cast(id as int))) as ls",
      "named_struct('l', array(id)) as sl",
      "array(named_struct('l', array(id))) as lsl",
      "array(array(named_struct('a', id))) as lls",
      "map(cast(id as int), named_struct('a', id)) as ms",
      "map(cast(id as int), named_struct('l', array(id))) as msl",
      "array(cast(id as int), 7) as flat"
    ).write.mode("overwrite").parquet(path)
    withPlugin(enabled = false)(spark.read.parquet(path).createOrReplaceTempView("t"))
    check("SELECT * FROM t", native = false)
    check("SELECT id, lsl FROM t", native = false)
    check("SELECT id, lls FROM t", native = false)
    check("SELECT id, msl FROM t", native = false)
    check("SELECT id, flat, ls, ll, sl, ms FROM t") // each of these is read natively
  }
}
