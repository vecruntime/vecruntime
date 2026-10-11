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

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.vecruntime.{VectorFilterExec, VectorProjectExec}

/**
 * Nested types (#705) over the nested-type cases of two Java Parquet projects:
 *
 *  - Apache parquet-java and its conformance corpus `apache/parquet-testing` (vendored under
 *    `src/test/resources/parquet-testing/`, commit in `SOURCE_SHA`): files other writers produced --
 *    modern three-level lists, the legacy two-level `LIST<LIST<int32>>`, repeated groups and repeated
 *    primitives with no LIST annotation, nested maps, a map whose key is optional (Presto/Trino), a map
 *    beside a key-only map, null and empty lists, a deep struct from arrow-rs. Plus the Dremel paper's
 *    `Document` records (parquet-java `TestColumnIO` / `Paper`).
 *  - Hardwood (hardwood-hq/hardwood): its `NestedSchemaTest` shapes (a list of structs, a struct of
 *    structs), the modern list of lists, and a dictionary-encoded nested column that spans batches.
 *
 * Every nested column of every file goes through the nested lane's consumers -- the column itself,
 * `size`, element access, `exists`, `filter`, `transform`, `map_keys` / `map_values`, and a repartition
 * through the shuffle -- and must answer exactly what Spark answers with the plugin off (the same rows,
 * or the same refusal). The recreated tables are written both in Spark's three-level form and in the
 * legacy form (`spark.sql.parquet.writeLegacyFormat`), and the queries our lanes compute are asserted to
 * run in our operators.
 */
class NestedParquetCompatSuite extends VectorQuerySuite {
  private val Project = classOf[VectorProjectExec]
  private val Filter = classOf[VectorFilterExec]

  private def resource(rel: String): String = {
    val url = getClass.getClassLoader.getResource(s"parquet-testing/$rel")
    assert(url != null, s"missing test resource parquet-testing/$rel")
    new java.io.File(url.toURI).getAbsolutePath
  }

  /** A value as text: binary by content, through rows, arrays and maps (maps by sorted entries). */
  private def show(v: Any): String = v match {
    case null => "null"
    case b: Array[Byte] => b.map("%02x".format(_)).mkString("0x", "", "")
    case r: Row => r.toSeq.map(show).mkString("[", ",", "]")
    case m: scala.collection.Map[_, _] =>
      m.map { case (k, x) => show(k) + "->" + show(x) }.toSeq.sorted.mkString("{", ",", "}")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case d: java.lang.Double => if (d.isNaN) "NaN" else d.toString
    case x => x.toString
  }

  private def rowsOf(f: => DataFrame): Either[String, Seq[String]] =
    try Right(f.collect().map(show).toSeq.sorted)
    catch { case e: Exception => Left(Option(e.getMessage).getOrElse(e.toString).linesIterator.next()) }

  /** `query` (over the view `t`) answers what Spark answers: the same rows, or a refusal both ways. */
  private def sameAsSpark(query: String, what: String): Unit = {
    val off = withPlugin(enabled = false)(rowsOf(spark.sql(query)))
    val on = withPlugin(enabled = true)(rowsOf(spark.sql(query)))
    (off, on) match {
      case (Right(e), Right(a)) => assert(a == e, s"$what: $query")
      case (Left(_), Left(_)) =>
      case (Right(_), Left(err)) => fail(s"$what: Spark answers $query, the plugin failed: $err")
      case (Left(err), Right(_)) => fail(s"$what: Spark refuses $query ($err), the plugin returned rows")
    }
  }

  /** The nested-lane consumers over one column of type `dt` (a top-level column name `c`). */
  private def queriesFor(c: String, dt: DataType): Seq[String] = {
    val q = s"`$c`"
    val common = Seq(s"SELECT $q FROM t", s"SELECT * FROM t DISTRIBUTE BY rand(7)")
    dt match {
      case _: ArrayType =>
        common ++ Seq(
          s"SELECT size($q) n, $q[0] e0, element_at($q, -1) el FROM t",
          s"SELECT exists($q, x -> x IS NULL) en, forall($q, x -> x IS NOT NULL) fn FROM t",
          s"SELECT filter($q, x -> x IS NOT NULL) f, transform($q, (x, i) -> i) ti FROM t",
          s"SELECT count(*) FROM t WHERE size($q) > 1"
        )
      case _: MapType =>
        common ++ Seq(
          s"SELECT size($q) n, map_keys($q) k, map_values($q) v FROM t",
          s"SELECT exists(map_values($q), x -> x IS NULL) en FROM t"
        )
      case s: StructType =>
        common ++ s.fields.flatMap(f => queriesFor(s"$c`.`${f.name}", f.dataType).filterNot(_.contains("DISTRIBUTE")))
      case _ => Nil
    }
  }

  private def isNested(dt: DataType): Boolean =
    dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType] || dt.isInstanceOf[StructType]

  /**
   * The parquet-testing files with nested columns, and what Spark makes of them. A file Spark refuses to
   * read must be refused with the plugin on too; `map_no_value.parquet`'s key-only map has no Spark type,
   * so the file is read through the columns Spark can represent.
   */
  private val corpus: Seq[(String, Option[String])] = Seq(
    "list_columns.parquet" -> None, // null list, empty list, null element, values: int64 and utf8
    "nested_lists.snappy.parquet" -> None, // array<array<array<string>>>, modern three-level
    "nested_maps.snappy.parquet" -> None, // map<string, map<int, boolean>>
    "nested_structs.rust.parquet" -> None, // a deep struct (ARROW-11452)
    "old_list_structure.parquet" -> None, // legacy two-level LIST<LIST<int32>>: [[1, 2], [3, 4]]
    "repeated_no_annotation.parquet" -> None, // a repeated group with no LIST annotation
    "repeated_primitive_no_list.parquet" -> None, // repeated primitives, top level and in a group
    "null_list.parquet" -> None, // a null list
    "incorrect_map_schema.parquet" -> None, // a map with an optional key (Presto/Trino)
    "map_no_value.parquet" -> Some("my_map map<int, int>, my_list array<int>") // beside a key-only map
  )

  for ((file, schema) <- corpus) {
    test(s"parquet-testing data/$file: every nested column answers as Spark does") {
      val path = resource(s"data/$file")
      def read(): DataFrame = schema.fold(spark.read.parquet(path))(ddl => spark.read.schema(ddl).parquet(path))
      val columns = withPlugin(enabled = false) {
        try read().schema.fields.toSeq
        catch { case _: Exception => Nil }
      }
      if (columns.isEmpty) {
        // Spark cannot read the file: neither may we.
        val on = withPlugin(enabled = true)(rowsOf(read()))
        assert(on.isLeft, s"$file: Spark refuses it, the plugin returned rows")
      } else {
        val nested = columns.filter(f => isNested(f.dataType))
        assert(nested.nonEmpty, s"$file has no nested column: ${columns.map(_.dataType.simpleString)}")
        withPlugin(enabled = false)(read().createOrReplaceTempView("t"))
        for (f <- nested; q <- queriesFor(f.name, f.dataType)) sameAsSpark(q, s"$file ${f.name}")
      }
    }
  }

  test("parquet-testing list_columns: our lanes compute over another writer's null, empty and null-element lists") {
    spark.read.parquet(resource("data/list_columns.parquet")).createOrReplaceTempView("t")
    checkVectorized(
      "SELECT size(int64_list) n, get(int64_list, 0) e0, element_at(utf8_list, -1) last, " +
        "array_contains(int64_list, 2L) c2, array_max(utf8_list) mx, exists(int64_list, x -> x IS NULL) en, " +
        "filter(utf8_list, s -> s IS NOT NULL) f, transform(int64_list, x -> x + 1) t FROM t",
      Seq(Project)
    )
  }

  test("parquet-java's Dremel Document records, written three-level and legacy two-level") {
    Seq(false, true).foreach { legacy =>
      val path = newTempPath(s"document-$legacy")
      withConf("spark.sql.parquet.writeLegacyFormat" -> legacy.toString) {
        spark.sql(
          """SELECT * FROM VALUES
            |  (10L, named_struct('Backward', CAST(NULL AS array<bigint>), 'Forward', array(20L, 40L, 60L)),
            |   array(named_struct('Language', array(named_struct('Code', 'en-us', 'Country', 'us'),
            |                                        named_struct('Code', 'en', 'Country', CAST(NULL AS string))),
            |                      'Url', 'http://A'),
            |         named_struct('Language', CAST(array() AS array<struct<Code: string, Country: string>>), 'Url', 'http://B'),
            |         named_struct('Language', array(named_struct('Code', 'en-gb', 'Country', 'gb')),
            |                      'Url', CAST(NULL AS string)))),
            |  (20L, named_struct('Backward', array(10L, 30L), 'Forward', array(80L)),
            |   array(named_struct('Language', CAST(array() AS array<struct<Code: string, Country: string>>),
            |                      'Url', 'http://C')))
            |  AS document(DocId, Links, Name)""".stripMargin
        ).write.mode("overwrite").parquet(path)
      }
      spark.read.parquet(path).createOrReplaceTempView("t")
      val what = s"Document, legacy=$legacy"
      // The repeated primitives under Links: our lanes compute these.
      checkVectorized(
        "SELECT DocId, size(Links.Backward) b, Links.Forward[0] f0, element_at(Links.Forward, -1) fl, " +
          "exists(Links.Forward, x -> x > 50) big, transform(Links.Forward, x -> x * 2) f2, " +
          "filter(Links.Forward, x -> x > DocId) after FROM t",
        Seq(Project)
      )
      // Repeated within repeated (Name.Language): the column, the counts and a shuffle.
      Seq(
        "SELECT * FROM t",
        "SELECT DocId, size(Name) n, transform(Name, n -> n.Url) urls FROM t",
        "SELECT DocId, Name.Language.Country FROM t", // parquet-java's projection schema2
        "SELECT * FROM t DISTRIBUTE BY DocId"
      ).foreach(q => sameAsSpark(q, what))
      val urls = withPlugin(enabled = true)(spark.sql("SELECT DocId, Name.Url FROM t ORDER BY DocId").collect())
      assert(urls.map(show).toSeq == Seq("[10,(http://A,http://B,null)]", "[20,(http://C)]"), what)
    }
  }

  test("Hardwood's NestedSchemaTest shapes and its list of lists, both list encodings") {
    Seq(false, true).foreach { legacy =>
      val path = newTempPath(s"hardwood-$legacy")
      withConf("spark.sql.parquet.writeLegacyFormat" -> legacy.toString) {
        spark.range(0, 400).selectExpr(
          "cast(id as int) as id",
          "case when id % 9 = 0 then null else array(named_struct('name', concat('widget', id % 7), 'quantity', " +
            "cast(id % 4 as int)), named_struct('name', 'gadget', 'quantity', cast(null as int))) end as items",
          "case when id % 11 = 0 then null else named_struct('street', concat(id, ' Main St'), 'city', 'Townsville', " +
            "'zip', case when id % 3 = 0 then null else '12345' end) end as address",
          "case when id % 5 = 0 then null else array(array(cast(id as int), 2), array(3), array()) end as mylist"
        ).write.mode("overwrite").parquet(path)
      }
      spark.read.parquet(path).createOrReplaceTempView("t")
      val what = s"Hardwood shapes, legacy=$legacy"
      Seq(
        "SELECT * FROM t",
        "SELECT id, size(items) n, transform(items, i -> i.name) names, address.city, address.zip FROM t",
        "SELECT id, size(mylist) n, mylist[0] first, transform(mylist, l -> size(l)) sizes FROM t",
        "SELECT * FROM t DISTRIBUTE BY id % 7"
      ).foreach(q => sameAsSpark(q, what))
      val first = withPlugin(enabled = true)(spark.sql("SELECT mylist FROM t WHERE id = 1").collect())
      assert(first.map(show).toSeq == Seq("[((1,2),(3),())]"), what)
    }
  }

  test("Hardwood's dictionary-encoded nested column across batches: lanes compute it") {
    val path = newTempPath("dict-nested")
    spark.range(0, 20000).selectExpr(
      "cast(id as int) as i",
      "case when id % 13 = 0 then null else transform(sequence(0, cast(id % 6 as int)), k -> " +
        "case when k = 2 then null else concat('v', (id + k) % 5) end) end as tags"
    ).repartition(1).write.option("parquet.block.size", 1 << 20).mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("t")
    checkVectorized(
      "SELECT i, size(tags) n, get(tags, 1) t1, array_contains(tags, 'v3') c3, array_position(tags, 'v0') p0, " +
        "array_max(tags) mx, exists(tags, s -> s = 'v4') e4, filter(tags, s -> s <> 'v1') f, " +
        "transform(tags, s -> upper(s)) u FROM t",
      Seq(Project)
    )
    checkVectorized("SELECT i FROM t WHERE array_contains(tags, 'v2') AND size(tags) > 3", Seq(Filter))
    sameAsSpark("SELECT * FROM t DISTRIBUTE BY i % 11", "dictionary nested column through the shuffle")
  }
}
