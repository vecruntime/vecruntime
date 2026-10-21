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
package io.vecruntime.iceberg.write

import java.lang.foreign.Arena
import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.file.{Files => NioFiles, Path}
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}

import scala.jdk.CollectionConverters._
import scala.util.Random

import io.vecruntime.kernels.{ArrowLayout, NestedVectorBuffers, VectorBuffers}
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{Files, Metrics, MetricsConfig, Schema}
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.data.parquet.{GenericParquetReaders, GenericParquetWriter}
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.parquet.{Parquet, ParquetSchemaUtil}
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField.optional
import org.scalatest.funsuite.AnyFunSuite

/**
 * [[IcebergParquetAppender]] against Iceberg's own Parquet writer and reader (#842): a file written
 * from the engine's columns must read back through Iceberg's generic reader value for value, carry
 * Iceberg's field ids and schema key, and report the same metrics Iceberg's own appender reports for
 * the same rows.
 */
class IcebergParquetAppenderSuite extends AnyFunSuite {
  import IcebergParquetAppenderSuite.Row

  private val schema = new Schema(
    optional(1, "i", Types.IntegerType.get()),
    optional(2, "l", Types.LongType.get()),
    optional(3, "d", Types.DoubleType.get()),
    optional(4, "f", Types.FloatType.get()),
    optional(5, "b", Types.BooleanType.get()),
    optional(6, "s", Types.StringType.get()),
    optional(7, "day", Types.DateType.get()),
    optional(8, "dec", Types.DecimalType.of(15, 2)),
    optional(9, "wide", Types.DecimalType.of(38, 6)),
    optional(10, "ts", Types.TimestampType.withZone())
  )

  private def rows(n: Int, seed: Long): IndexedSeq[Row] = {
    val r = new Random(seed)
    def maybe[T](odds: Int)(v: => T): Option[T] = if (r.nextInt(odds) == 0) None else Some(v)
    (0 until n).map { _ =>
      Row(
        maybe(10)(r.nextInt()),
        maybe(10)(r.nextLong()),
        maybe(17)(r.nextGaussian()),
        maybe(19)(r.nextGaussian().toFloat),
        maybe(13)(r.nextBoolean()),
        maybe(11)("v" + r.nextInt(500)),
        maybe(9)(10000 + r.nextInt(5000)),
        maybe(7)(r.nextInt(1000000000).toLong * (if (r.nextBoolean()) 1 else -1)),
        maybe(9)(new BigInteger(100, r.self).subtract(BigInteger.ONE.shiftLeft(99))),
        maybe(8)(1700000000000000L + r.nextInt(Int.MaxValue).toLong * 1000L)
      )
    }
  }

  private def columns(a: Arena, rs: IndexedSeq[Row]): java.util.List[VectorBuffers] = {
    def nulls(f: Row => Option[_]) = rs.map(f(_).isEmpty).toArray
    java.util.List.of(
      ArrowLayout.ofInts(a, rs.map(_.i.getOrElse(0)).toArray, nulls(_.i)),
      ArrowLayout.ofLongs(a, rs.map(_.l.getOrElse(0L)).toArray, nulls(_.l)),
      ArrowLayout.ofDoubles(a, rs.map(_.d.getOrElse(0.0)).toArray, nulls(_.d)),
      // A float column arrives in a FLOAT64 lane, as the scan and the operators hand it over.
      ArrowLayout.ofDoubles(a, rs.map(_.f.map(_.toDouble).getOrElse(0.0)).toArray, nulls(_.f)),
      ArrowLayout.ofBooleans(a, rs.map(_.b.getOrElse(false)).toArray, nulls(_.b)),
      ArrowLayout.ofStrings(a, rs.map(_.s.orNull).toArray),
      ArrowLayout.ofInts(a, rs.map(_.day.getOrElse(0)).toArray, nulls(_.day)),
      ArrowLayout.ofLongs(a, rs.map(_.dec.getOrElse(0L)).toArray, nulls(_.dec)),
      ArrowLayout.ofDecimal128(a, rs.map(_.wide.orNull).toArray, nulls(_.wide)),
      ArrowLayout.ofLongs(a, rs.map(_.ts.getOrElse(0L)).toArray, nulls(_.ts))
    )
  }

  private def record(row: Row): Record = {
    val rec = GenericRecord.create(schema)
    rec.setField("i", row.i.map(Int.box).orNull)
    rec.setField("l", row.l.map(Long.box).orNull)
    rec.setField("d", row.d.map(Double.box).orNull)
    rec.setField("f", row.f.map(Float.box).orNull)
    rec.setField("b", row.b.map(Boolean.box).orNull)
    rec.setField("s", row.s.orNull)
    rec.setField("day", row.day.map(d => LocalDate.ofEpochDay(d.toLong)).orNull)
    rec.setField("dec", row.dec.map(v => JBigDecimal.valueOf(v, 2)).orNull)
    rec.setField("wide", row.wide.map(v => new JBigDecimal(v, 6)).orNull)
    rec.setField(
      "ts",
      row.ts.map { us =>
        OffsetDateTime.ofInstant(
          Instant.ofEpochSecond(Math.floorDiv(us, 1000000L), Math.floorMod(us, 1000000L) * 1000L),
          ZoneOffset.UTC
        )
      }.orNull
    )
    rec
  }

  private def withDir[T](f: Path => T): T = {
    val dir = NioFiles.createTempDirectory("iceberg-appender")
    try f(dir)
    finally {
      dir.toFile.listFiles().foreach(_.delete())
      dir.toFile.delete()
    }
  }

  /** Writes `rs` with the appender in batches of `batch` rows. */
  private def writeNative(path: Path, rs: IndexedSeq[Row], batch: Int, props: Map[String, String] = Map.empty)
      : IcebergParquetAppender = {
    val appender = IcebergParquetAppender.create(
      Files.localOutput(path.toFile),
      schema,
      props.asJava,
      MetricsConfig.getDefault,
      null,
      new Configuration()
    )
    val arena = Arena.ofConfined()
    try {
      rs.grouped(batch).foreach { part =>
        appender.add(new IcebergParquetAppender.Batch(columns(arena, part), part.length))
      }
      appender.close()
    } finally arena.close()
    appender
  }

  private def writeIceberg(path: Path, rs: IndexedSeq[Row]): FileAppender[Record] = {
    val appender: FileAppender[Record] = Parquet
      .write(Files.localOutput(path.toFile))
      .schema(schema)
      .createWriterFunc((s: Schema, t: org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType) =>
        GenericParquetWriter.create(s, t)
      )
      .build()
    rs.foreach(r => appender.add(record(r)))
    appender.close()
    appender
  }

  private def readIceberg(path: Path): IndexedSeq[Record] = {
    val in = Parquet
      .read(Files.localInput(path.toFile))
      .project(schema)
      .createReaderFunc(t => GenericParquetReaders.buildReader(schema, t))
      .build[Record]()
    try in.asScala.map(_.copy()).toIndexedSeq
    finally in.close()
  }

  test("a file from the engine's columns reads back through Iceberg's reader") {
    withDir { dir =>
      val rs = rows(5000, 1L)
      val path = dir.resolve("native.parquet")
      val appender = writeNative(path, rs, 1024)
      val back = readIceberg(path)
      assert(back.length == rs.length)
      rs.zip(back).zipWithIndex.foreach { case ((want, got), k) =>
        val expected = record(want)
        schema.columns().asScala.foreach { f =>
          assert(got.getField(f.name()) == expected.getField(f.name()), s"row $k column ${f.name()}")
        }
      }
      assert(appender.metrics().recordCount() == rs.length)
      assert(appender.length() == NioFiles.size(path))
      assert(!appender.splitOffsets().isEmpty)
    }
  }

  test("the file carries Iceberg's field ids and schema key") {
    withDir { dir =>
      val path = dir.resolve("native.parquet")
      writeNative(path, rows(100, 2L), 100)
      val reader = org.apache.parquet.hadoop.ParquetFileReader.open(
        org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
          new org.apache.hadoop.fs.Path(path.toUri),
          new Configuration()
        )
      )
      try {
        val footer = reader.getFooter
        val fileSchema = footer.getFileMetaData.getSchema
        assert(fileSchema.toString == ParquetSchemaUtil.convert(schema, "table").toString)
        assert(fileSchema.getFields.asScala.map(_.getId.intValue) == (1 to 10))
        assert(footer.getFileMetaData.getKeyValueMetaData.get("iceberg.schema") ==
          org.apache.iceberg.SchemaParser.toJson(schema))
      } finally reader.close()
    }
  }

  test("metrics equal those of Iceberg's own appender for the same rows") {
    withDir { dir =>
      assertSameMetrics(dir, rows(20000, 3L))
    }
  }

  test("metrics with NaNs equal Iceberg's: NaN counts, and bounds that skip NaN") {
    withDir { dir =>
      val r = new Random(5L)
      val rs = rows(20000, 5L).map { row =>
        row.copy(
          d = if (r.nextInt(50) == 0) Some(Double.NaN) else row.d,
          f = if (r.nextInt(40) == 0) Some(Float.NaN) else row.f
        )
      }
      assertSameMetrics(dir, rs)
    }
  }

  test("metrics of a column of NaNs only equal Iceberg's") {
    withDir { dir =>
      assertSameMetrics(dir, rows(3000, 6L).map(_.copy(d = Some(Double.NaN), f = Some(Float.NaN))))
    }
  }

  private def assertSameMetrics(dir: Path, rs: IndexedSeq[Row]): Unit = {
    val native = writeNative(dir.resolve("native.parquet"), rs, 4096).metrics()
    val iceberg = writeIceberg(dir.resolve("iceberg.parquet"), rs).metrics()
    def bounds(m: Metrics, upper: Boolean) =
      (if (upper) m.upperBounds() else m.lowerBounds()).asScala.toMap
    assert(native.recordCount() == iceberg.recordCount())
    assert(native.valueCounts() == iceberg.valueCounts())
    assert(native.nullValueCounts() == iceberg.nullValueCounts())
    assert(native.nanValueCounts() == iceberg.nanValueCounts())
    Seq(false, true).foreach { upper =>
      val (n, i) = (bounds(native, upper), bounds(iceberg, upper))
      assert(n.keySet == i.keySet, s"bound columns (upper=$upper)")
      n.foreach { case (id, v) => assert(v == i(id), s"bound of field $id (upper=$upper)") }
    }
  }

  test("structs, lists and maps read back and report Iceberg's metrics") {
    val nested = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(
        2,
        "s",
        Types.StructType.of(
          optional(3, "x", Types.DoubleType.get()),
          optional(4, "y", Types.StringType.get())
        )
      ),
      optional(5, "tags", Types.ListType.ofOptional(6, Types.IntegerType.get())),
      optional(7, "m", Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.LongType.get()))
    )
    val r = new Random(7L)
    val n = 3000
    // Per row: id, s (null, or x / y), tags (null, or elements, some null), m (null, or entries).
    val ids = Array.tabulate(n)(k => if (r.nextInt(10) == 0) None else Some(k))
    val ss = Array.fill(n) {
      if (r.nextInt(8) == 0) None
      else Some((
        if (r.nextInt(6) == 0) None else Some(if (r.nextInt(30) == 0) Double.NaN else r.nextGaussian()),
        if (r.nextInt(7) == 0) None else Some("y" + r.nextInt(100))
      ))
    }
    val tags = Array.fill(n) {
      if (r.nextInt(9) == 0) None
      else Some(Seq.fill(r.nextInt(4))(if (r.nextInt(5) == 0) None else Some(r.nextInt(1000))))
    }
    val maps = Array.fill(n) {
      if (r.nextInt(9) == 0) None
      else Some((0 until r.nextInt(3)).map(e => ("k" + e, if (r.nextInt(4) == 0) None else Some(r.nextLong()))))
    }
    def offsets(a: Arena, sizes: Array[Int]) = {
      val seg = ArrowLayout.allocateOffsets(a, sizes.length)
      var at = 0
      sizes.indices.foreach { k =>
        seg.set(VectorBuffers.LE_INT, k.toLong << 2, at)
        at += sizes(k)
      }
      seg.set(VectorBuffers.LE_INT, sizes.length.toLong << 2, at)
      seg
    }
    def vectors(a: Arena): java.util.List[VectorBuffers] = {
      val x = ss.map(_.flatMap(_._1))
      val struct = NestedVectorBuffers.struct(
        n,
        ArrowLayout.validityFrom(a, ss.map(_.isEmpty), n),
        java.util.List.of(
          ArrowLayout.ofDoubles(a, x.map(_.getOrElse(0.0)), x.map(_.isEmpty)),
          ArrowLayout.ofStrings(a, ss.map(_.flatMap(_._2).orNull))
        )
      )
      val elems = tags.flatMap(_.getOrElse(Nil))
      val list = NestedVectorBuffers.list(
        n,
        ArrowLayout.validityFrom(a, tags.map(_.isEmpty), n),
        offsets(a, tags.map(_.map(_.length).getOrElse(0))),
        ArrowLayout.ofInts(a, elems.map(_.getOrElse(0)), elems.map(_.isEmpty))
      )
      val entries = maps.flatMap(_.getOrElse(Nil))
      val map = NestedVectorBuffers.map(
        n,
        ArrowLayout.validityFrom(a, maps.map(_.isEmpty), n),
        offsets(a, maps.map(_.map(_.length).getOrElse(0))),
        ArrowLayout.ofStrings(a, entries.map(_._1)),
        ArrowLayout.ofLongs(a, entries.map(_._2.getOrElse(0L)), entries.map(_._2.isEmpty))
      )
      java.util.List.of(ArrowLayout.ofInts(a, ids.map(_.getOrElse(0)), ids.map(_.isEmpty)), struct, list, map)
    }
    def record(k: Int): Record = {
      val rec = GenericRecord.create(nested)
      rec.setField("id", ids(k).map(Int.box).orNull)
      rec.setField(
        "s",
        ss(k).map { case (x, y) =>
          val s = GenericRecord.create(nested.findType("s").asStructType())
          s.setField("x", x.map(Double.box).orNull)
          s.setField("y", y.orNull)
          s
        }.orNull
      )
      rec.setField("tags", tags(k).map(_.map(_.map(Int.box).orNull).asJava).orNull)
      rec.setField(
        "m",
        maps(k).map { es =>
          val m = new java.util.LinkedHashMap[String, java.lang.Long]()
          es.foreach { case (key, v) => m.put(key, v.map(Long.box).orNull) }
          m
        }.orNull
      )
      rec
    }
    withDir { dir =>
      val path = dir.resolve("native.parquet")
      val appender = IcebergParquetAppender.create(
        Files.localOutput(path.toFile),
        nested,
        java.util.Map.of(),
        MetricsConfig.getDefault,
        null,
        new Configuration()
      )
      val arena = Arena.ofConfined()
      try {
        appender.add(new IcebergParquetAppender.Batch(vectors(arena), n))
        appender.close()
      } finally arena.close()
      val in = Parquet.read(Files.localInput(path.toFile)).project(nested)
        .createReaderFunc(t => GenericParquetReaders.buildReader(nested, t)).build[Record]()
      val back =
        try in.asScala.map(_.copy()).toIndexedSeq
        finally in.close()
      assert(back.length == n)
      back.zipWithIndex.foreach { case (got, k) =>
        val want = record(k)
        // NaN != NaN, so the struct (whose x may be NaN) is compared by its text.
        assert(got.getField("id") == want.getField("id"), s"row $k id")
        assert(String.valueOf(got.getField("s")) == String.valueOf(want.getField("s")), s"row $k s")
        assert(got.getField("tags") == want.getField("tags"), s"row $k tags")
        assert(got.getField("m") == want.getField("m"), s"row $k m")
      }
      val iceberg: FileAppender[Record] = Parquet.write(Files.localOutput(dir.resolve("iceberg.parquet").toFile))
        .schema(nested)
        .createWriterFunc((s: Schema, t: org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType) =>
          GenericParquetWriter.create(s, t)
        )
        .build()
      (0 until n).foreach(k => iceberg.add(record(k)))
      iceberg.close()
      val (a, b) = (appender.metrics(), iceberg.metrics())
      assert(a.valueCounts() == b.valueCounts())
      assert(a.nullValueCounts() == b.nullValueCounts())
      assert(a.nanValueCounts() == b.nanValueCounts())
      assert(a.lowerBounds() == b.lowerBounds())
      assert(a.upperBounds() == b.upperBounds())
    }
  }

  test("a float or double inside a list or a map is refused") {
    val listOfDoubles = new Schema(optional(1, "xs", Types.ListType.ofOptional(2, Types.DoubleType.get())))
    withDir { dir =>
      intercept[UnsupportedOperationException] {
        IcebergParquetAppender.create(
          Files.localOutput(dir.resolve("x.parquet").toFile),
          listOfDoubles,
          java.util.Map.of(),
          MetricsConfig.getDefault,
          null,
          new Configuration()
        )
      }
    }
  }

  test("table properties pick the codec and the row-group size") {
    withDir { dir =>
      val path = dir.resolve("native.parquet")
      val appender = writeNative(
        path,
        rows(20000, 4L),
        2048,
        Map("write.parquet.compression-codec" -> "snappy", "write.parquet.row-group-size-bytes" -> "65536")
      )
      val reader = org.apache.parquet.hadoop.ParquetFileReader.open(
        org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
          new org.apache.hadoop.fs.Path(path.toUri),
          new Configuration()
        )
      )
      try {
        val blocks = reader.getFooter.getBlocks.asScala
        assert(blocks.length > 1)
        assert(blocks.flatMap(_.getColumns.asScala).forall(_.getCodec.name() == "SNAPPY"))
        assert(appender.splitOffsets().asScala.map(_.longValue) == blocks.map(_.getStartingPos))
      } finally reader.close()
      assert(readIceberg(path).length == 20000)
    }
  }
}

object IcebergParquetAppenderSuite {
  final case class Row(
      i: Option[Int],
      l: Option[Long],
      d: Option[Double],
      f: Option[Float],
      b: Option[Boolean],
      s: Option[String],
      day: Option[Int],
      dec: Option[Long],
      wide: Option[BigInteger],
      ts: Option[Long]
  )
}
