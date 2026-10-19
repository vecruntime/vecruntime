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
package io.vecruntime.spark.iceberg

import io.vecruntime.spark.{VectorConf, VectorPlugin}
import io.vecruntime.spark.test.IcebergTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorIcebergScanExec}
import org.scalatest.Tag

/**
 * #735: Iceberg tables read by our native Parquet reader over Iceberg's planning (`VectorIcebergScanExec`). The
 * merge-on-read battery of [[IcebergMorSuiteBase]] runs through it: task groups with delete files are read by
 * Iceberg's reader inside the node, the others natively, and every result is checked against Spark. Needs only
 * the Iceberg runtime (`mvn -Piceberg`).
 */
class IcebergNativeScanSuite extends IcebergMorSuiteBase {

  override protected def suiteTags: Seq[Tag] = Seq(IcebergTest)

  override protected def expectedScanClass: String = "VectorIcebergScanExec"

  // With the plugin off Spark's own scan stays.
  override protected def pluginOffScanClass: String = "BatchScanExec"

  override protected def extraSparkConf: Map[String, String] =
    icebergConf ++ Map(
      "spark.plugins" -> classOf[VectorPlugin].getName,
      "spark.sql.adaptive.enabled" -> "true",
      VectorConf.ScanNativeIceberg -> "true",
      s"spark.sql.catalog.${IcebergTables.Catalog}.read.parquet.vectorization.batch-size" -> "1024"
    )

  private def scans(df: DataFrame): Seq[VectorIcebergScanExec] =
    PlanUtils.allNodes(finalPlan(df)).collect { case s: VectorIcebergScanExec => s }

  private def groups(df: DataFrame): (Long, Long) = {
    val s = scans(df)
    assert(s.nonEmpty, s"expected a VectorIcebergScanExec:\n${finalPlan(df).treeString}")
    (s.map(_.metrics("numNativeGroups").value).sum, s.map(_.metrics("numIcebergGroups").value).sum)
  }

  private val Db = IcebergTables.Db

  icebergTest("a table without deletes is read natively, every task group") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_native USING iceberg AS SELECT * FROM $Db.t_pos")
    useTable("t_native")
    val df =
      checkVectorized("SELECT s, count(*), sum(d2), min(i), max(l) FROM t WHERE i > 5 GROUP BY s", Seq(Filter, Agg))
    assertScanUnder(df, Filter)
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkMixedQueries()
  }

  icebergTest("position deletes are applied natively as a selection") {
    useTable("t_pos")
    val df = checkVectorized("SELECT count(*), sum(d2), min(i), max(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    assert(count("SELECT count(*) FROM t") === IcebergTables.MixedLiveRows)
    checkVectorized("SELECT s, count(*), sum(l) FROM t WHERE i % 3 = 1 GROUP BY s", Seq(Filter, Agg))
  }

  icebergTest("deleted rows stay deleted when a Spark row operator reads the scan directly") {
    // A Scala UDF is not one of our expressions: Spark's filter and aggregate stay, over a ColumnarToRow that
    // reads every physical row of a batch, so the scan must hand it only the rows its deletes keep.
    spark.udf.register("andr_keep", (i: Int) => i % 3 != 1)
    Seq("t_pos", "t_dv", "t_eq").foreach { name =>
      useTable(name)
      Seq(
        "SELECT count(*), sum(l), min(i), max(i) FROM t WHERE andr_keep(i)",
        "SELECT s, count(*), sum(l) FROM t WHERE andr_keep(i) GROUP BY s",
        "SELECT i, l, s FROM t WHERE andr_keep(i) AND i % 101 = 5"
      ).foreach { q =>
        val df = checkVectorized(q, Nil)
        assert(df.collect().nonEmpty, s"the query should return rows: $q")
        val (native, iceberg) = groups(df)
        assert(native > 0 && iceberg == 0, s"$name native $native, Iceberg $iceberg: $q")
      }
    }
  }

  icebergTest("deletion vectors are applied natively as a selection") {
    useTable("t_dv")
    val df = checkVectorized("SELECT count(*), sum(d2), min(i), max(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkVectorized("SELECT s, count(*), sum(l) FROM t WHERE i > 7 GROUP BY s", Seq(Filter, Agg))
  }

  icebergTest("equality deletes are applied natively, the equality column read even when not projected") {
    useTable("t_eq")
    val df = checkVectorized("SELECT count(*), sum(d2), min(i), max(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    // `i`, the equality column, not projected: read for the deletes, then dropped.
    val noKey = checkVectorized("SELECT s, count(*), sum(l), max(d2) FROM t GROUP BY s", Seq(Agg))
    val (native2, iceberg2) = groups(noKey)
    assert(native2 > 0 && iceberg2 == 0, s"native $native2, Iceberg $iceberg2")
    assert(count("SELECT count(*) FROM t") === IcebergTables.EqLiveRows)
    assert(count("SELECT count(d2) FROM t") === IcebergTables.EqLiveRows)
    checkVectorized("SELECT s, count(*), sum(l) FROM t WHERE l % 5 = 2 GROUP BY s", Seq(Filter, Agg))
  }

  icebergTest("equality deletes on a string column are applied natively") {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_eqs USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT * FROM $Db.t_pos"
    )
    // A null in an equality delete matches the null rows, as in Iceberg.
    IcebergTables.addEqualityDeletesOn(spark, s"$Db.t_eqs", "s", Seq("s3", "s17", null))
    useTable("t_eqs")
    val df = checkVectorized("SELECT count(*), sum(l), min(i) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkVectorized("SELECT s, count(*) FROM t GROUP BY s", Seq(Agg))
  }

  icebergTest("metadata columns _file, _pos and _spec_id are read natively, with and without deletes") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_meta USING iceberg AS SELECT * FROM $Db.t_pos")
    Seq("t_meta", "t_pos", "t_eq").foreach { table =>
      // The table itself, not the view `t`: Iceberg's hidden metadata columns resolve on the relation.
      val t = s"$Db.$table"
      val df = checkVectorized(s"SELECT _file, _pos, _spec_id, i, s FROM $t WHERE i % 97 = 3", Seq(Filter))
      val (native, iceberg) = groups(df)
      assert(native > 0 && iceberg == 0, s"$table: native $native, Iceberg $iceberg")
      // Each one alone and out of order: _pos without the other constants, _file only, before the data columns.
      checkVectorized(s"SELECT l, _pos FROM $t WHERE i % 89 = 1", Seq(Filter))
      checkVectorized(s"SELECT _file, count(*), max(i) FROM $t GROUP BY _file", Seq(Agg))
      checkVectorized(s"SELECT _spec_id, count(*) FROM $t GROUP BY _spec_id", Seq(Agg))
    }
  }

  icebergTest("a renamed column is matched by its field id") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_renamed USING iceberg AS SELECT i, l, s, d2 FROM $Db.t_pos")
    spark.sql(s"ALTER TABLE $Db.t_renamed RENAME COLUMN l TO l2")
    spark.sql(s"ALTER TABLE $Db.t_renamed RENAME COLUMN s TO s2")
    useTable("t_renamed")
    val df = checkVectorized("SELECT s2, count(*), sum(l2), max(d2) FROM t WHERE i > 3 GROUP BY s2", Seq(Filter, Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
  }

  icebergTest("a column added after a file was written reads as nulls from that file") {
    spark.sql(s"CREATE OR REPLACE TABLE $Db.t_added USING iceberg AS SELECT i, l, s FROM $Db.t_pos")
    spark.sql(s"ALTER TABLE $Db.t_added ADD COLUMN extra BIGINT")
    spark.sql(s"INSERT INTO $Db.t_added SELECT i + 100000, l, s, i * 7 FROM $Db.t_pos WHERE i < 500")
    useTable("t_added")
    val df = checkVectorized("SELECT count(*), count(extra), sum(extra), max(l) FROM t", Seq(Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    checkVectorized("SELECT s, count(extra), sum(l) FROM t WHERE extra IS NULL AND i > 2 GROUP BY s", Seq(Filter, Agg))
  }

  /**
   * A table whose columns were promoted after files were written: int to bigint, float to double, decimal(9,2) to
   * decimal(18,2) (both 64-bit unscaled) and decimal(9,2) to decimal(30,2) (a 128-bit lane). The old files keep
   * the narrow physical types; the new ones carry values only the wide types hold.
   */
  private def createPromoted(name: String, props: String): Unit = {
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.$name (k INT, i INT, f FLOAT, dm DECIMAL(9, 2), dw DECIMAL(9, 2), s STRING)
         |USING iceberg TBLPROPERTIES ($props)""".stripMargin
    )
    spark.sql(
      s"""INSERT INTO $Db.$name SELECT CAST(id AS INT), CAST(id * 7 - 9000 AS INT), CAST(id / 8.0 AS FLOAT),
         |  CAST(id * 13 % 99999 / 10.0 AS DECIMAL(9, 2)), CAST(id * 11 % 99999 / 10.0 AS DECIMAL(9, 2)),
         |  IF(id % 13 = 0, NULL, concat('s', id % 7))
         |FROM range(0, 3000)""".stripMargin
    )
    spark.sql(s"ALTER TABLE $Db.$name ALTER COLUMN i TYPE BIGINT")
    spark.sql(s"ALTER TABLE $Db.$name ALTER COLUMN f TYPE DOUBLE")
    spark.sql(s"ALTER TABLE $Db.$name ALTER COLUMN dm TYPE DECIMAL(18, 2)")
    spark.sql(s"ALTER TABLE $Db.$name ALTER COLUMN dw TYPE DECIMAL(30, 2)")
    spark.sql(
      s"""INSERT INTO $Db.$name SELECT CAST(id AS INT), id * 3000000000, id / 3.0,
         |  CAST(id * 100000000 AS DECIMAL(18, 2)), CAST(CAST(id AS DECIMAL(30, 2)) * 100000000000000000 AS DECIMAL(30, 2)),
         |  concat('n', id % 5)
         |FROM range(3000, 3600)""".stripMargin
    )
  }

  private val promotedQueries = Seq(
    "SELECT count(*), sum(i), min(i), max(i), sum(f), max(f), sum(dm), max(dm), sum(dw), max(dw) FROM t",
    "SELECT s, count(*), sum(i), sum(dm), sum(dw) FROM t WHERE i > 100 GROUP BY s",
    // Old files' statistics are on the narrow types: a pushed filter compares them against wide literals.
    "SELECT k, i, f, dm, dw FROM t WHERE i > 2000000000 OR i < -8000",
    "SELECT k, f, dm FROM t WHERE f >= 300.5 AND dm < 5000.00",
    "SELECT k, dw FROM t WHERE dw > 100000000000000.00 OR dw < 1.00",
    "SELECT k, i FROM t WHERE i = -8993 OR i = 9000000000000"
  )

  icebergTest("promoted columns read their old files widened: int to bigint, float to double, decimal precision") {
    createPromoted("t_promoted", "'format-version' = '2'")
    useTable("t_promoted")
    promotedQueries.foreach { q =>
      val df = checkVectorized(q, Nil)
      assert(df.collect().nonEmpty, s"the query should return rows: $q")
      val (native, iceberg) = groups(df)
      assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg: $q")
    }
  }

  icebergTest("promoted columns under position deletes and deletion vectors read their old files widened") {
    Seq(2, 3).foreach { v =>
      val name = s"t_promoted_v$v"
      createPromoted(name, s"'format-version' = '$v', 'write.delete.mode' = 'merge-on-read'")
      spark.sql(s"DELETE FROM $Db.$name WHERE k % 5 = 0")
      useTable(name)
      assert(count("SELECT count(*) FROM t") === 3600 - 720)
      promotedQueries.foreach { q =>
        val df = checkVectorized(q, Nil)
        assert(df.collect().nonEmpty, s"the query should return rows: $q")
        val (native, iceberg) = groups(df)
        assert(native > 0 && iceberg == 0, s"v$v native $native, Iceberg $iceberg: $q")
      }
    }
  }

  /**
   * #nested: a table with struct, list and map columns, nested in one another, some values null. The old files are
   * written before a nested field is renamed and another added inside the struct.
   */
  private def createNested(name: String, props: String): Unit = {
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.$name (
         |  k INT, st STRUCT<a: INT, b: STRING, inner: STRUCT<x: DOUBLE, ys: ARRAY<BIGINT>>>,
         |  arr ARRAY<BIGINT>, m MAP<STRING, STRUCT<p: INT, q: STRING>>, lst ARRAY<STRUCT<u: INT, v: ARRAY<STRING>>>)
         |USING iceberg TBLPROPERTIES ($props)""".stripMargin
    )
    def rows(from: Int, to: Int, withC: Boolean): String = {
      val b = if (withC) "b2" else "b"
      val c = if (withC) ", 'c', CAST(id % 3 AS INT)" else ""
      val inner = "named_struct('x', id / 4.0, 'ys', IF(id % 5 = 0, NULL, array(id, id + 1, id * 2)))"
      val st = s"named_struct('a', CAST(id % 97 AS INT), '$b', IF(id % 7 = 0, NULL, concat('b', id % 13)), " +
        s"'inner', $inner$c)"
      s"""SELECT CAST(id AS INT), IF(id % 11 = 0, NULL, $st),
         |  IF(id % 9 = 0, NULL, sequence(0L, id % 6)),
         |  IF(id % 8 = 0, NULL, map(concat('k', id % 4), named_struct('p', CAST(id AS INT), 'q', concat('q', id % 3)))),
         |  array(named_struct('u', CAST(id % 10 AS INT), 'v', array(concat('v', id % 2), NULL)),
         |        named_struct('u', CAST(id % 7 AS INT), 'v', IF(id % 4 = 0, NULL, array('w'))))
         |FROM range($from, $to)""".stripMargin
    }
    spark.sql(s"INSERT INTO $Db.$name ${rows(0, 3000, withC = false)}")
    spark.sql(s"ALTER TABLE $Db.$name RENAME COLUMN st.b TO b2")
    spark.sql(s"ALTER TABLE $Db.$name ADD COLUMN st.c INT")
    spark.sql(s"INSERT INTO $Db.$name ${rows(3000, 3600, withC = true)}")
  }

  private val nestedQueries = Seq(
    "SELECT k, st, arr, m, lst FROM t WHERE k % 17 = 3",
    "SELECT st.a, count(*), count(st.b2), sum(st.c), sum(st.inner.x), sum(size(st.inner.ys)) FROM t GROUP BY st.a",
    "SELECT k, st.b2, st.c, st.inner.ys, arr FROM t WHERE st.a > 90",
    "SELECT k, m['k1'].p, m['k2'].q, lst[0].u, lst[1].v FROM t WHERE k > 2900",
    "SELECT sum(size(arr)), sum(size(m)), sum(size(lst)), count(st.inner) FROM t"
  )

  private def checkNestedNatively(label: String): Unit = nestedQueries.foreach { q =>
    val df = checkVectorized(q, Nil)
    assert(df.collect().nonEmpty, s"the query should return rows: $q")
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"$label native $native, Iceberg $iceberg: $q")
  }

  icebergTest("struct, list and map columns are read natively, a renamed and an added nested field by field id") {
    createNested("t_nested", "'format-version' = '2'")
    useTable("t_nested")
    checkNestedNatively("v2")
  }

  icebergTest("nested columns under position deletes and deletion vectors are read natively") {
    Seq(2, 3).foreach { v =>
      val name = s"t_nested_v$v"
      createNested(name, s"'format-version' = '$v', 'write.delete.mode' = 'merge-on-read'")
      spark.sql(s"DELETE FROM $Db.$name WHERE k % 5 = 0")
      useTable(name)
      assert(count("SELECT count(*) FROM t") === 3600 - 720)
      checkNestedNatively(s"v$v")
    }
  }

  icebergTest("nested columns with equality deletes on a top-level key are read natively") {
    createNested("t_nested_eq", "'format-version' = '2', 'write.delete.mode' = 'merge-on-read'")
    IcebergTables.addEqualityDeletesOn(spark, s"$Db.t_nested_eq", "k", (0 until 3600 by 7).map(Int.box))
    useTable("t_nested_eq")
    assert(count("SELECT count(*) FROM t") === 3600 - (0 until 3600 by 7).size)
    checkNestedNatively("equality")
  }

  icebergTest("v3 initial defaults fill the files written before their columns were added, read natively") {
    import org.apache.iceberg.expressions.Literal
    import org.apache.iceberg.types.Types
    spark.sql(
      s"""CREATE OR REPLACE TABLE $Db.t_defaults (k INT, s STRING) USING iceberg
         |TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')""".stripMargin
    )
    spark.sql(s"INSERT INTO $Db.t_defaults SELECT CAST(id AS INT), concat('s', id % 5) FROM range(0, 3000)")
    val table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, s"$Db.t_defaults")
    table.updateSchema()
      .addColumn("ci", Types.IntegerType.get(), "", Literal.of(7))
      .addColumn("cl", Types.LongType.get(), "", Literal.of(9000000000L))
      .addColumn("cb", Types.BooleanType.get(), "", Literal.of(true))
      .addColumn("cf", Types.DoubleType.get(), "", Literal.of(2.5d))
      .addColumn("cd", Types.DecimalType.of(9, 2), "", Literal.of(new java.math.BigDecimal("12.34")))
      .addColumn("cs", Types.StringType.get(), "", Literal.of("it's"))
      .addColumn("cdt", Types.DateType.get(), "", Literal.of(19000).to(Types.DateType.get()))
      .addColumn(
        "cts",
        Types.TimestampType.withZone(),
        "",
        Literal.of(1704067200123456L).to(Types.TimestampType.withZone())
      )
      .addColumn(
        "cnt",
        Types.TimestampType.withoutZone(),
        "",
        Literal.of(1704067200123456L).to(Types.TimestampType.withoutZone())
      )
      .commit()
    spark.sql(
      s"""INSERT INTO $Db.t_defaults SELECT CAST(id AS INT), concat('n', id % 3), CAST(id AS INT), id, id % 2 = 0,
         |  id / 3.0, CAST(id / 100.0 AS DECIMAL(9, 2)), concat('v', id), date_add(DATE '2020-01-01', CAST(id AS INT)),
         |  timestamp_seconds(id), CAST(timestamp_seconds(id) AS TIMESTAMP_NTZ)
         |FROM range(3000, 3500)""".stripMargin
    )
    // One position delete, so a deleted file is read with its defaults too.
    spark.sql(s"DELETE FROM $Db.t_defaults WHERE k % 10 = 1")
    useTable("t_defaults")
    Seq(
      "SELECT k, s, ci, cl, cb, cf, cd, cs, cdt, cts, cnt FROM t WHERE k % 97 = 3",
      "SELECT ci, cb, count(*), sum(cl), sum(cf), sum(cd), min(cs), max(cdt), max(cts), min(cnt) FROM t GROUP BY ci, cb",
      "SELECT count(*) FROM t WHERE ci = 7 AND cs = 'it''s' AND cd = 12.34"
    ).foreach { q =>
      val df = checkVectorized(q, Nil)
      assert(df.collect().nonEmpty, s"the query should return rows: $q")
      val (native, iceberg) = groups(df)
      assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg: $q")
    }
    assert(count("SELECT count(*) FROM t WHERE ci = 7 AND cs = 'it''s'") === 2700)
  }

  icebergTest("v3 variant columns, shredded or not, are read natively, also under deletion vectors") {
    Seq(false, true).foreach { shred =>
      val name = s"t_variant_${if (shred) "shredded" else "plain"}"
      // Iceberg's vectorized reader cannot read a shredded variant ("Unsupported variant: shredded typed_value
      // primitive"), so the shredded table is read row-wise by Iceberg: Spark's reference results, and the DELETE.
      // Our scan then takes the non-vectorized path, every task group read natively.
      spark.sql(
        s"""CREATE OR REPLACE TABLE $Db.$name (k INT, v VARIANT, s STRUCT<w: VARIANT, n: INT>) USING iceberg
           |TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read',
           |  'write.parquet.shred-variants' = '$shred', 'read.parquet.vectorization.enabled' = '${!shred}')""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO $Db.$name SELECT CAST(id AS INT),
           |  IF(id % 13 = 0, NULL, parse_json(concat('{"a": ', id, ', "b": "x', id % 7, '", "c": [', id % 3, ', 2]}'))),
           |  named_struct('w', parse_json(concat('[', id % 5, ', {"z": ', id, '}]')), 'n', CAST(id % 4 AS INT))
           |FROM range(0, 3000)""".stripMargin
      )
      spark.sql(s"DELETE FROM $Db.$name WHERE k % 6 = 0")
      useTable(name)
      Seq(
        "SELECT k, v, s FROM t WHERE k % 31 = 2",
        "SELECT variant_get(v, '$.b', 'string') b, count(*), sum(variant_get(v, '$.a', 'int')) FROM t GROUP BY b",
        "SELECT k, variant_get(v, '$.c[0]', 'int'), variant_get(s.w, '$[1].z', 'bigint'), s.n FROM t WHERE k > 2950",
        "SELECT count(v), count(*) FROM t WHERE is_variant_null(v) = false"
      ).foreach { q =>
        val df = checkVectorized(q, Nil)
        assert(df.collect().nonEmpty, s"the query should return rows: $q")
        val (native, iceberg) = groups(df)
        assert(native > 0 && iceberg == 0, s"shredded=$shred native $native, Iceberg $iceberg: $q")
      }
    }
  }

  icebergTest("an identity-partitioned table reads its partition column from the files") {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_parted USING iceberg PARTITIONED BY (b) AS SELECT i, l, s, b, d2 FROM $Db.t_pos"
    )
    useTable("t_parted")
    val df = checkVectorized("SELECT b, count(*), sum(l), min(s) FROM t WHERE i > 1 GROUP BY b", Seq(Filter, Agg))
    val (native, iceberg) = groups(df)
    assert(native > 0 && iceberg == 0, s"native $native, Iceberg $iceberg")
    // Partition pruning by Iceberg's planning: only the b = true files.
    checkVectorized("SELECT count(*), sum(l) FROM t WHERE b AND i > 1", Seq(Filter, Agg))
  }

  private def rowGroups(df: DataFrame): Long = scans(df).map(_.metrics("numRowGroups").value).sum

  /** One file, ascending `a` and `b`, in many small row groups: row-group skipping is all a filter can do. */
  private def sortedTable(name: String): Unit = {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.$name (a INT, b BIGINT, s STRING) USING iceberg " +
        "TBLPROPERTIES ('write.parquet.row-group-size-bytes'='2048')"
    )
    spark.sql(
      s"INSERT INTO $Db.$name SELECT CAST(id AS INT), id * 3, concat('s', CAST(id % 40 AS STRING)) " +
        "FROM range(0, 20000, 1, 1)"
    )
  }

  icebergTest("the filter above the scan skips row groups, by field id") {
    sortedTable("t_push")
    useTable("t_push")
    val all = checkVectorized("SELECT count(*), sum(b) FROM t WHERE s IS NOT NULL", Seq(Filter, Agg))
    val df = checkVectorized("SELECT count(*), sum(b) FROM t WHERE a BETWEEN 1000 AND 1100", Seq(Filter, Agg))
    assert(scans(df).exists(_.pushedFilters.nonEmpty), finalPlan(df).treeString)
    val (read, total) = (rowGroups(df), rowGroups(all))
    assert(total > 10 && read > 0 && read * 5 < total, s"read $read of $total row groups")
    assert(count("SELECT count(*) FROM t WHERE a BETWEEN 1000 AND 1100") === 101)
    // Strings, IN lists, an OR and a NOT: the same rows as Spark's.
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a IN (3, 9000, 19999) OR b < 30", Seq(Filter, Agg))
    checkVectorized("SELECT count(*) FROM t WHERE s = 's7' AND NOT (a > 500)", Seq(Filter, Agg))
    // Spark's switch turns the push off.
    withConf("spark.sql.parquet.filterPushdown" -> "false") {
      val off = checkVectorized("SELECT count(*), sum(b) FROM t WHERE a BETWEEN 1000 AND 1100", Seq(Filter, Agg))
      assert(rowGroups(off) === total, s"read ${rowGroups(off)} of $total row groups")
    }
  }

  icebergTest("Iceberg's task groups are packed into tasks as Spark packs a file scan's splits") {
    // Small files, each its own task group (a 1 KB split target).
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_pack (a INT, b BIGINT, s STRING) USING iceberg " +
        "TBLPROPERTIES ('read.split.target-size'='1024')"
    )
    spark.sql(
      s"INSERT INTO $Db.t_pack SELECT CAST(id AS INT), id * 3, concat('s', CAST(id % 40 AS STRING)) " +
        "FROM range(0, 20000, 1, 16)"
    )
    useTable("t_pack")
    def tasks(df: DataFrame): Int = scans(df).map(_.executeColumnar().getNumPartitions).sum
    val sql = "SELECT s, count(*), sum(b) FROM t WHERE a % 7 <> 3 GROUP BY s"
    val off = withConf(VectorConf.ScanNativeIcebergPackSplits -> "false") { checkVectorized(sql, Seq(Filter, Agg)) }
    val on = checkVectorized(sql, Seq(Filter, Agg))
    assert(tasks(off) >= 16, s"${tasks(off)} tasks unpacked")
    assert(tasks(on) < tasks(off), s"${tasks(on)} tasks packed, ${tasks(off)} unpacked")
    // Packed to fill the cores: never fewer tasks than Spark's own packing of the same bytes would make.
    withConf("spark.sql.files.minPartitionNum" -> "4") {
      val four = checkVectorized(sql, Seq(Filter, Agg))
      assert(tasks(four) >= 4 && tasks(four) <= tasks(off), s"${tasks(four)} tasks with minPartitionNum 4")
    }
    // A table with deletes keeps its rows when its groups are packed.
    spark.sql(s"DELETE FROM $Db.t_pack WHERE a % 11 = 4")
    checkVectorized(sql, Seq(Filter, Agg))
    assert(count("SELECT count(*) FROM t") === 20000 - (0 until 20000).count(_ % 11 == 4))
  }

  icebergTest("a broadcast join's build keys skip the fact's row groups, by field id (#610 on Iceberg)") {
    sortedTable("t_rf")
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_rf_dim USING iceberg AS " +
        "SELECT CAST(id AS INT) AS k, id * 3 AS lk, concat('d', CAST(id AS STRING)) AS name FROM range(0, 20000)"
    )
    // The dimension is filtered on its name, not its key: Spark infers nothing for the fact side.
    for (
      (sql, inMax) <- Seq(
        s"SELECT count(*), sum(f.b) FROM $Db.t_rf f JOIN $Db.t_rf_dim d ON f.a = d.k WHERE d.name LIKE 'd12__'" -> "1024",
        s"SELECT count(*), sum(f.b) FROM $Db.t_rf f JOIN $Db.t_rf_dim d ON f.a = d.k WHERE d.name LIKE 'd12__'" -> "10",
        s"SELECT count(*), sum(f.a) FROM $Db.t_rf f JOIN $Db.t_rf_dim d ON f.b = d.lk " +
          "WHERE d.name LIKE 'd31__' AND f.s <> 's1'" -> "1024"
      )
    ) withConf(VectorConf.JoinRuntimeFiltersInMax -> inMax) {
      val off = withConf(VectorConf.JoinRuntimeFilters -> "false") { checkVectorized(sql, Seq(Agg)) }
      val on = withConf(VectorConf.JoinRuntimeFilters -> "true") { checkVectorized(sql, Seq(Agg)) }
      def rf(df: DataFrame) = scans(df).map(_.metrics("numRuntimeFilters").value).sum
      assert(rf(off) === 0)
      assert(rf(on) > 0, s"$sql\n${finalPlan(on).treeString}")
      assert(rowGroups(on) * 5 < rowGroups(off), s"read ${rowGroups(on)} of ${rowGroups(off)} row groups: $sql")
    }
    // A left join keeps every fact row: no filter.
    val left = withConf(VectorConf.JoinRuntimeFilters -> "true") {
      checkVectorized(
        s"SELECT count(*), count(d.name) FROM $Db.t_rf f LEFT JOIN " +
          s"(SELECT * FROM $Db.t_rf_dim WHERE name LIKE 'd12__') d ON f.a = d.k",
        Seq(Agg)
      )
    }
    assert(scans(left).map(_.metrics("numRuntimeFilters").value).sum === 0)
  }

  icebergTest("a pushed filter on a renamed column skips row groups of the files written before the rename") {
    sortedTable("t_push_ren")
    spark.sql(s"ALTER TABLE $Db.t_push_ren RENAME COLUMN a TO a2")
    useTable("t_push_ren")
    val all = checkVectorized("SELECT count(*), sum(b) FROM t WHERE s IS NOT NULL", Seq(Filter, Agg))
    val df = checkVectorized("SELECT count(*), sum(b) FROM t WHERE a2 BETWEEN 1000 AND 1100", Seq(Filter, Agg))
    val (read, total) = (rowGroups(df), rowGroups(all))
    assert(read > 0 && read * 5 < total, s"read $read of $total row groups")
  }

  icebergTest("after a rename and a new column taking the old name, a filter on the new column reads the old files") {
    sortedTable("t_push_reuse")
    // The old files' `a` is now `a_old`; the new `a` (another field id) is null in them.
    spark.sql(s"ALTER TABLE $Db.t_push_reuse RENAME COLUMN a TO a_old")
    spark.sql(s"ALTER TABLE $Db.t_push_reuse ADD COLUMN a INT")
    spark.sql(s"INSERT INTO $Db.t_push_reuse VALUES (-1, -3, 'new', 5), (-2, -6, 'new', 6)")
    useTable("t_push_reuse")
    // By name, `a IS NULL` would be tested against the old files' a_old (no nulls) and skip them all.
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a IS NULL", Seq(Filter, Agg))
    assert(count("SELECT count(*) FROM t WHERE a IS NULL") === 20000)
    // The new column alone, beside the renamed one, and under OR / NOT: every one tested by field id.
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a = 5", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a IS NULL AND a_old BETWEEN 10 AND 20", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE a = 6 OR a_old = 3", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(b) FROM t WHERE NOT (a = 6)", Seq(Filter, Agg))
    assert(count("SELECT count(*) FROM t WHERE a IS NULL AND a_old BETWEEN 10 AND 20") === 11)
  }

  icebergTest("pushed filters on a partitioned table with deletes return Spark's rows") {
    spark.sql(
      s"CREATE OR REPLACE TABLE $Db.t_push_part USING iceberg PARTITIONED BY (b) TBLPROPERTIES " +
        s"('format-version'='2', 'write.parquet.row-group-size-bytes'='2048') AS SELECT i, l, s, b, d2 FROM $Db.t_pos"
    )
    spark.sql(s"DELETE FROM $Db.t_push_part WHERE i % 11 = 4")
    useTable("t_push_part")
    checkVectorized("SELECT b, count(*), sum(l) FROM t WHERE i BETWEEN 100 AND 400 GROUP BY b", Seq(Filter, Agg))
    checkVectorized("SELECT count(*), sum(l) FROM t WHERE b AND s >= 's3' AND d2 > 0.5", Seq(Filter, Agg))
    checkVectorized(s"SELECT _pos, i FROM $Db.t_push_part WHERE i < 50 AND _pos > 3", Seq(Filter))
  }

  icebergTest("switched off, Spark's BatchScanExec stays") {
    withConf(VectorConf.ScanNativeIceberg -> "false") {
      useTable("t_pos")
      val df = spark.sql("SELECT count(*) FROM t WHERE i > 1")
      df.collect()
      assert(scans(df).isEmpty, finalPlan(df).treeString)
    }
  }
}
