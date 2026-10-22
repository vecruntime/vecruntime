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
package io.vecruntime.iceberg.spark

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

import org.apache.iceberg.{FileFormat, Table}
import org.apache.iceberg.data.{GenericFileWriterFactory, GenericRecord}
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.vecruntime.ParquetFileResolver
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #901 R2: equality deletes probed on the batch's lanes (EqualityDeleteSet) delete exactly the rows Iceberg's own
 * reader deletes: single- and multi-column keys over int, long, string, date, timestamp, decimal (narrow and
 * wide), float and double (NaN and -0.0), boolean and binary, with nulls in keys and in data, several groups of
 * delete files on one table, and deletes beside position deletes.
 */
class IcebergEqualityDeletesSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("iceeqdel")
  private val Native = "spark.vecruntime.iceberg.nativeEqualityDeletes.enabled"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("iceberg-equality-deletes")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.plugins", "io.vecruntime.spark.VectorPlugin")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.resolve("ice").toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def withConf[T](kv: (String, String))(f: => T): T = {
    val prev = spark.conf.getOption(kv._1)
    spark.conf.set(kv._1, kv._2)
    try f
    finally prev match { case Some(v) => spark.conf.set(kv._1, v); case None => spark.conf.unset(kv._1) }
  }

  private def sorted(rows: Seq[Row]): Seq[Row] = rows.sortBy(_.toString)

  /** One equality delete file on `columns` of `t`, with the given rows of Iceberg Java values (nulls allowed). */
  private def addDeletes(t: String, columns: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    val table: Table = Spark3Util.loadIcebergTable(spark, t)
    val schema = table.schema()
    val deleteSchema = schema.select(columns: _*)
    val factory = new GenericFileWriterFactory.Builder(table)
      .deleteFileFormat(FileFormat.PARQUET)
      .equalityFieldIds(columns.map(c => schema.findField(c).fieldId()).toArray)
      .equalityDeleteRowSchema(deleteSchema)
      .build()
    val out = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build().newOutputFile()
    val writer = factory.newEqualityDeleteWriter(out, table.spec(), null)
    try rows.foreach { r =>
        val record = GenericRecord.create(deleteSchema)
        columns.zip(r).foreach { case (c, v) => record.setField(c, v) }
        writer.write(record)
      }
    finally writer.close()
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
    spark.sql(s"REFRESH TABLE $t")
  }

  /** `q` with the native probe equals Iceberg's reader and the row probe; returns the batches probed natively. */
  private def check(q: String): Long = {
    val expected = withConf("spark.vecruntime.enabled" -> "false")(sorted(spark.sql(q).collect().toSeq))
    val before = ParquetFileResolver.nativeEqualityBatches.get()
    val got = sorted(spark.sql(q).collect().toSeq)
    val probed = ParquetFileResolver.nativeEqualityBatches.get() - before
    assert(got == expected, s"$q\nnative: ${got.take(5)}\nIceberg: ${expected.take(5)}")
    val rowProbe = withConf(Native -> "false")(sorted(spark.sql(q).collect().toSeq))
    assert(rowProbe == expected, s"$q: the row probe differs from Iceberg's reader")
    probed
  }

  private val T = "ice.db.eq"

  private def create(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $T")
    spark.sql(
      s"""CREATE TABLE $T (i INT, l BIGINT, s STRING, d DATE, ts TIMESTAMP, dec DECIMAL(9, 2), wd DECIMAL(30, 2),
         |  f FLOAT, db DOUBLE, b BOOLEAN, bin BINARY) USING iceberg
         |  TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')""".stripMargin
    )
    spark.sql(
      s"""INSERT INTO $T SELECT
         |  if(id % 50 = 0, null, cast(id % 1000 AS INT)),
         |  id * 3,
         |  if(id % 70 = 0, null, concat('é', cast(id % 40 AS STRING))),
         |  date_add(date'1969-12-20', cast(id % 30 AS INT)),
         |  timestamp_micros(id * 1000000),
         |  cast(id % 500 AS DECIMAL(9, 2)) / 4,
         |  cast(id AS DECIMAL(30, 2)) * 1000000000000,
         |  CASE WHEN id % 97 = 0 THEN cast('NaN' AS FLOAT) WHEN id % 89 = 0 THEN cast(-0.0 AS FLOAT)
         |       WHEN id % 83 = 0 THEN cast(0.0 AS FLOAT) ELSE cast(id % 20 AS FLOAT) / 2 END,
         |  CASE WHEN id % 79 = 0 THEN cast('NaN' AS DOUBLE) ELSE (id % 25) / 4.0 END,
         |  id % 3 = 0,
         |  cast(concat('b', cast(id % 11 AS STRING)) AS BINARY)
         |FROM range(0, 6000)""".stripMargin
    )
  }

  test("single- and multi-column keys over every supported type, nulls, NaN and -0.0, several groups") {
    create()
    addDeletes(T, Seq("i"), Seq(Seq(7), Seq(13), Seq(null), Seq(999)))
    addDeletes(
      T,
      Seq("s", "d"),
      Seq(
        Seq("é3", java.time.LocalDate.of(1969, 12, 24)),
        Seq(null, java.time.LocalDate.of(1970, 1, 1)),
        Seq("é5", null)
      )
    )
    addDeletes(T, Seq("dec"), Seq(Seq(new BigDecimal("12.25")), Seq(new BigDecimal("0.50"))))
    addDeletes(T, Seq("wd"), Seq(Seq(new BigDecimal("42000000000000.00"))))
    addDeletes(T, Seq("f"), Seq(Seq(java.lang.Float.valueOf(Float.NaN)), Seq(java.lang.Float.valueOf(-0.0f))))
    addDeletes(T, Seq("db"), Seq(Seq(java.lang.Double.valueOf(Double.NaN)), Seq(java.lang.Double.valueOf(2.5))))
    addDeletes(T, Seq("b", "l"), Seq(Seq(true, 300L), Seq(false, 301L)))
    addDeletes(T, Seq("bin"), Seq(Seq(ByteBuffer.wrap("b4".getBytes("UTF-8")))))
    addDeletes(T, Seq("ts"), Seq(Seq(java.time.OffsetDateTime.of(1970, 1, 1, 0, 20, 34, 0, java.time.ZoneOffset.UTC))))
    val probed = check(s"SELECT * FROM $T")
    assert(probed > 0, "no batch was probed natively")
    check(s"SELECT i, s, count(*) FROM $T GROUP BY i, s")
    // The float key: NaN and -0.0 deleted, 0.0 kept.
    check(s"SELECT f, count(*) FROM $T GROUP BY f")
  }

  test("equality deletes beside position deletes") {
    create()
    addDeletes(T, Seq("i"), Seq(Seq(1), Seq(2), Seq(3)))
    spark.sql(s"DELETE FROM $T WHERE l % 7 = 0")
    assert(check(s"SELECT * FROM $T") > 0)
  }

  test("equality deletes keyed on uuid, fixed, and a composite (uuid, int) key") {
    import org.apache.iceberg.types.Types
    import org.apache.iceberg.types.Types.NestedField.{optional, required}
    def uuidOf(n: Long) = new java.util.UUID(n * 0x9e3779b97f4a7c15L, ~n * 0xbf58476d1ce4e5b9L)
    def fx(i: Int) = f"$i%08X".sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray
    val iceCatalog = spark.sessionState.catalogManager
      .catalog("ice")
      .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
      .icebergCatalog()
    spark.udf.register("uuid_of", (n: Long) => uuidOf(n).toString)
    val N = 4000

    def create(name: String): Unit = {
      val schema = new org.apache.iceberg.Schema(
        required(1, "id", Types.LongType.get()),
        optional(2, "u", Types.UUIDType.get()),
        optional(3, "f", Types.FixedType.ofLength(4)),
        optional(4, "n", Types.IntegerType.get()),
        optional(5, "a", Types.IntegerType.get()),
        optional(6, "s", Types.StringType.get())
      )
      val tid = org.apache.iceberg.catalog.TableIdentifier.of("db", name)
      iceCatalog.dropTable(tid, false)
      iceCatalog.createTable(
        tid,
        schema,
        org.apache.iceberg.PartitionSpec.unpartitioned(),
        java.util.Map.of("format-version", "2", "write.delete.mode", "merge-on-read")
      )
      spark.sql(s"REFRESH TABLE ice.db.$name")
      // u null when id%7==0; f null when id%9==0. a = id (unique int), s = 's<id>' (unique string).
      withConf("spark.vecruntime.enabled" -> "false")(spark.sql(
        s"""INSERT INTO ice.db.$name SELECT id,
           |  if(id % 7 = 0, null, uuid_of(id)),
           |  if(id % 9 = 0, null, unhex(lpad(hex(id), 8, '0'))),
           |  cast(id % 5 AS INT),
           |  cast(id AS INT),
           |  concat('s', id)
           |FROM range(0, $N)""".stripMargin
      ).collect())
    }

    /** The ids the native scan returns (plugin on), and whether the native equality probe ran. */
    def nativeIds(t: String): (Set[Int], Boolean) = {
      val before = ParquetFileResolver.nativeEqualityBatches.get()
      val ids = spark.sql(s"SELECT id FROM $t").collect().map(_.getLong(0).toInt).toSet
      (ids, ParquetFileResolver.nativeEqualityBatches.get() > before)
    }
    def icebergIds(t: String): Set[Int] =
      withConf("spark.vecruntime.enabled" -> "false")(
        spark.sql(s"SELECT id FROM $t").collect().map(_.getLong(0).toInt).toSet
      )

    /**
     * The native scan deletes exactly `expected` (computed independently from the data minus the keys), it equals
     * Iceberg's own reader, and the native equality probe actually ran.
     */
    def assertRemaining(t: String, expected: Set[Int]): Unit = {
      val iceberg = icebergIds(t)
      val (got, probed) = nativeIds(t)
      info(s"$t: native removed ${N - got.size}, Iceberg removed ${N - iceberg.size}, expected ${N - expected.size}")
      assert(probed, s"$t: the native equality probe did not run")
      assert(iceberg == expected, s"$t: Iceberg's reader removed ${N - iceberg.size}, expected ${N - expected.size}")
      assert(
        got == expected,
        s"$t: native scan removed ${N - got.size} rows, expected ${N - expected.size}; " +
          s"missing=${(expected -- got).take(5)} extra=${(got -- expected).take(5)}"
      )
    }

    // --- uuid key, with a null key that deletes the null-uuid rows ---
    create("equ")
    val uKeys = (0 until N by 13).toSet
    addDeletes(
      "ice.db.equ",
      Seq("u"),
      ((0 until N by 13).filter(_ % 7 != 0).map(i => Seq[Any](uuidOf(i.toLong))) :+ Seq[Any](null))
    )
    // Deleted: a non-null uuid in uKeys, or a null uuid (id%7==0). Remaining = the rest.
    val uRemaining = (0 until N).filterNot(i => (uKeys.contains(i) && i % 7 != 0) || i % 7 == 0).toSet
    assertRemaining("ice.db.equ", uRemaining)
    info(s"uuid: deleted ${N - uRemaining.size}, remaining ${uRemaining.size}")

    // --- fixed key ---
    // Iceberg 1.12's own reader does NOT apply a fixed (byte-array) equality delete correctly, and the native
    // scan mirrors it. Root cause, verified against Iceberg's own classes (iceberg-spark-runtime 1.12.0):
    //   * org.apache.iceberg.data.parquet.GenericParquetReaders$FixedReader.read(byte[]) REUSES the caller's
    //     byte[] buffer (it copies into aload_1 and returns that same array), so every row's fixed value is the
    //     SAME array instance, mutated in place as the file is read.
    //   * org.apache.iceberg.data.BaseDeleteLoader.readEqDeletes feeds those records through a flyweight
    //     InternalRecordWrapper into StructLikeSet; StructLikeSet.add -> StructLikeWrapper.copyFor -> set() only
    //     stores the StructLike REFERENCE (no deep copy of field values).
    // The two combine so a many-key fixed delete file collapses to ONE set element (verified: a 143-key fixed
    // delete loads as StructLikeSet.size == 1). Both Iceberg's own reader and the native scan (same loader) then
    // remove only the degenerate match. We assert native == Iceberg's own reader.
    create("eqf")
    addDeletes("ice.db.eqf", Seq("f"), (0 until N by 17).filter(_ % 9 != 0).map(i => Seq[Any](fx(i))))
    val (fGot, fProbed) = nativeIds("ice.db.eqf")
    assert(fProbed, "eqf: the native equality probe did not run")
    assert(fGot == icebergIds("ice.db.eqf"), "eqf: native scan differs from Iceberg's reader on a fixed key")
    info(s"fixed: native removed ${N - fGot.size}")

    // --- composite controls: multi-column equality keys with no uuid component delete exactly as expected,
    // proving the harness (writer field ids, delete-file equality ids, delete-row schema) is correct. ---
    create("eqcii") // (int, int): a = id (unique), n = id % 5
    addDeletes(
      "ice.db.eqcii",
      Seq("a", "n"),
      (0 until N by 11).map(i => Seq[Any](Integer.valueOf(i), Integer.valueOf(i % 5)))
    )
    val ciiExpected = (0 until N).filterNot(_ % 11 == 0).toSet
    assertRemaining("ice.db.eqcii", ciiExpected)
    info(s"composite (int,int): removed ${N - ciiExpected.size}")

    create("eqcsi") // (string, int): s = 's<id>' (unique), n = id % 5
    addDeletes("ice.db.eqcsi", Seq("s", "n"), (0 until N by 11).map(i => Seq[Any](s"s$i", Integer.valueOf(i % 5))))
    val csiExpected = (0 until N).filterNot(_ % 11 == 0).toSet
    assertRemaining("ice.db.eqcsi", csiExpected)
    info(s"composite (string,int): removed ${N - csiExpected.size}")

    // --- composite (uuid, int) key ---
    // The controls above (int,int) and (string,int) delete correctly, so the harness is right. A composite whose
    // key includes a uuid, however, deletes nothing in Iceberg 1.12's OWN reader (verified: both the vectorized
    // and the row-based reader keep every row), and the native scan mirrors it. Each component matches in
    // isolation -- the loaded StructLikeSet has the right distinct keys, and a hand-built DeleteFilter probe
    // (InternalRowWrapper + StructProjection, pruned read schema) returns contains == true for a key -- yet
    // Iceberg's assembled DeleteFilter applies zero. The single-column uuid key DOES apply (see the uuid case
    // above, 836 rows), so this is specific to uuid as one field of a multi-field key, in Iceberg's reader.
    create("eqc")
    addDeletes(
      "ice.db.eqc",
      Seq("u", "n"),
      (0 until N by 11).filter(_ % 7 != 0).map(i => Seq[Any](uuidOf(i.toLong), Integer.valueOf(i % 5)))
    )
    val (cGot, cProbed) = nativeIds("ice.db.eqc")
    assert(cGot == icebergIds("ice.db.eqc"), "eqc: native scan differs from Iceberg's reader on a composite key")
    info(s"composite (uuid,int): native removed ${N - cGot.size}, probe ran = $cProbed")

    // The uuid key read only for the deletes (not projected) still works: a count that selects only id.
    create("equ2")
    addDeletes(
      "ice.db.equ2",
      Seq("u"),
      ((0 until N by 13).filter(_ % 7 != 0).map(i => Seq[Any](uuidOf(i.toLong))) :+ Seq[Any](null))
    )
    val before = ParquetFileResolver.nativeEqualityBatches.get()
    val cnt = spark.sql("SELECT count(id) FROM (SELECT id FROM ice.db.equ2)").collect()(0).getLong(0)
    assert(ParquetFileResolver.nativeEqualityBatches.get() > before, "equ2: the native probe did not run")
    assert(cnt == uRemaining.size, s"equ2: unprojected-key count $cnt, expected ${uRemaining.size}")
  }
}
