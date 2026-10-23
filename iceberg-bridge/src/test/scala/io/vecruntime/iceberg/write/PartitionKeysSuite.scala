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
import java.nio.ByteBuffer

import scala.util.Random

import io.vecruntime.kernels.{ArrowLayout, NestedVectorBuffers, VectorBuffers}
import org.apache.iceberg.{PartitionKey, PartitionSpec, Schema}
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField.optional
import org.scalatest.funsuite.AnyFunSuite

/**
 * [[PartitionKeys]] against Iceberg's own partitioning (#842, W3): for every transform over every source type it
 * takes, the partition of each row computed from the engine's columns must equal Iceberg's `PartitionKey` of the
 * same row as a record, nulls, negative values, dates before 1970 and timestamps before the epoch included.
 */
class PartitionKeysSuite extends AnyFunSuite {

  private val schema = new Schema(
    optional(1, "i", Types.IntegerType.get()),
    optional(2, "l", Types.LongType.get()),
    optional(3, "day", Types.DateType.get()),
    optional(4, "ts", Types.TimestampType.withZone()),
    optional(5, "s", Types.StringType.get()),
    optional(6, "dec", Types.DecimalType.of(15, 2)),
    optional(7, "wide", Types.DecimalType.of(38, 6)),
    optional(8, "b", Types.BooleanType.get()),
    optional(9, "bin", Types.BinaryType.get())
  )

  private val n = 5000

  private final class Data(seed: Long) {
    private val r = new Random(seed)
    private def maybe[T](v: => T): Option[T] = if (r.nextInt(12) == 0) None else Some(v)
    val i: Array[Option[Int]] = Array.fill(n)(maybe(r.nextInt(2000000) - 1000000))
    val l: Array[Option[Long]] = Array.fill(n)(maybe(r.nextLong() >> r.nextInt(40)))
    // Days around the epoch, before 1970 included.
    val day: Array[Option[Int]] = Array.fill(n)(maybe(r.nextInt(40000) - 20000))
    // Micros around the epoch, negative included, on hour boundaries now and then.
    val ts: Array[Option[Long]] = Array.fill(n)(maybe {
      val v = (r.nextLong() % 3000000000000000L)
      if (r.nextInt(10) == 0) v - Math.floorMod(v, 3600000000L) else v
    })
    val s: Array[Option[String]] =
      Array.fill(n)(maybe {
        // A mix of ASCII, 2-byte (é), 3-byte (CJK), 4-byte (emoji, a surrogate pair) code points and the
        // empty string, so truncate-by-code-point and identity are exercised on every UTF-8 width.
        val parts = Seq(
          "é" * r.nextInt(3),
          "日" * r.nextInt(3),
          "\uD83D\uDE00" * r.nextInt(3), // U+1F600
          "s" + r.nextInt(1000),
          "x" * r.nextInt(6)
        )
        if (r.nextInt(20) == 0) "" else parts.mkString
      })
    val dec: Array[Option[Long]] = Array.fill(n)(maybe(r.nextInt(2000000000).toLong - 1000000000L))
    val wide: Array[Option[BigInteger]] =
      Array.fill(n)(maybe(new BigInteger(90, r.self).subtract(BigInteger.ONE.shiftLeft(89))))
    val b: Array[Option[Boolean]] = Array.fill(n)(maybe(r.nextBoolean()))
    val bin: Array[Option[Array[Byte]]] = Array.fill(n)(maybe(Array.fill(1 + r.nextInt(8))(r.nextInt(256).toByte)))

    def columns(a: Arena): java.util.List[VectorBuffers] = {
      java.util.List.of(
        ArrowLayout.ofInts(a, i.map(_.getOrElse(0)), nulls(i)),
        ArrowLayout.ofLongs(a, l.map(_.getOrElse(0L)), nulls(l)),
        ArrowLayout.ofInts(a, day.map(_.getOrElse(0)), nulls(day)),
        ArrowLayout.ofLongs(a, ts.map(_.getOrElse(0L)), nulls(ts)),
        ArrowLayout.ofStrings(a, s.map(_.orNull)),
        ArrowLayout.ofLongs(a, dec.map(_.getOrElse(0L)), nulls(dec)),
        ArrowLayout.ofDecimal128(a, wide.map(_.orNull), nulls(wide)),
        ArrowLayout.ofBooleans(a, b.map(_.getOrElse(false)), nulls(b)),
        binary(a)
      )
    }

    private def binary(a: Arena): VectorBuffers = {
      val bytes = bin.map(_.getOrElse(Array.emptyByteArray))
      val offsets = bytes.scanLeft(0)(_ + _.length)
      ArrowLayout.ofUtf8(a, bytes.flatten, offsets, n, nulls(bin))
    }

    private def nulls(xs: Array[_ <: Option[_]]) = xs.map(_.isEmpty)

    /** Row `k` as Iceberg's internal-representation record (what `PartitionKey.partition` takes). */
    def record(k: Int): GenericRecord = {
      val rec = GenericRecord.create(schema)
      rec.set(0, i(k).map(Int.box).orNull)
      rec.set(1, l(k).map(Long.box).orNull)
      rec.set(2, day(k).map(Int.box).orNull)
      rec.set(3, ts(k).map(Long.box).orNull)
      rec.set(4, s(k).orNull)
      rec.set(5, dec(k).map(v => JBigDecimal.valueOf(v, 2)).orNull)
      rec.set(6, wide(k).map(v => new JBigDecimal(v, 6)).orNull)
      rec.set(7, b(k).map(Boolean.box).orNull)
      rec.set(8, bin(k).map(ByteBuffer.wrap).orNull)
      rec
    }
  }

  private def check(name: String, spec: PartitionSpec): Unit = test(s"$name equals Iceberg's partition key") {
    val data = new Data(name.hashCode.toLong)
    val arena = Arena.ofConfined()
    try {
      val columns = data.columns(arena)
      val keys = new PartitionKeys(spec, schema)
      // Several batches: the writer calls compute on slices, so compute must give the same keys per slice.
      for ((from, until) <- Seq((0, n), (0, 1), (1, 1000), (1000, n))) {
        val values = keys.compute(columns, from, until)
        val expected = new PartitionKey(spec, schema)
        (from until until).foreach { k =>
          expected.partition(data.record(k))
          val got = values.key(k - from)
          assert(got == expected, s"row $k: ${got.toPath} vs ${expected.toPath}")
        }
      }
      // Grouping: rows the engine calls same-partition must have equal Iceberg keys, and vice versa.
      val values = keys.compute(columns, 0, n)
      val expected = Array.tabulate(n) { k =>
        val e = new PartitionKey(spec, schema)
        e.partition(data.record(k))
        e.copy()
      }
      for (a <- Seq(0, 1, 2, 17, 500, 2500, n - 1); b <- Seq(0, 3, 42, 777, 2500, n - 1)) {
        assert(values.samePartition(a, b) == (expected(a) == expected(b)), s"samePartition($a,$b)")
      }
    } finally arena.close()
  }

  private def spec(f: PartitionSpec.Builder => PartitionSpec.Builder): PartitionSpec =
    f(PartitionSpec.builderFor(schema)).build()

  check(
    "identity over every type",
    spec(_.identity("i").identity("l").identity("day").identity("ts").identity("s")
      .identity("dec").identity("wide").identity("b").identity("bin"))
  )
  check("year of a date", spec(_.year("day")))
  check("month of a date", spec(_.month("day")))
  check("day of a date", spec(_.day("day")))
  check("year of a timestamp", spec(_.year("ts")))
  check("month of a timestamp", spec(_.month("ts")))
  check("day of a timestamp", spec(_.day("ts")))
  check("hour of a timestamp", spec(_.hour("ts")))
  check("bucket of int, long, date, timestamp", spec(_.bucket("i", 16).bucket("l", 7).bucket("day", 5).bucket("ts", 3)))
  check(
    "bucket of string, decimals, binary",
    spec(_.bucket("s", 11).bucket("dec", 4).bucket("wide", 9).bucket("bin", 6))
  )
  check(
    "truncate of int, long, string, decimals",
    spec(_.truncate("i", 100).truncate("l", 1000).truncate("s", 2)
      .truncate("dec", 50).truncate("wide", 1000))
  )
  // The object-valued transforms now group on lanes (#901 follow-up); each must still equal Iceberg's key.
  check("identity of a string (grouped on the lane)", spec(_.identity("s")))
  check("identity of binary (grouped on the lane)", spec(_.identity("bin")))
  check("identity of a decimal and a wide decimal", spec(_.identity("dec").identity("wide")))
  check("truncate of a string to 1 code point", spec(_.truncate("s", 1)))
  check("truncate of a string to 3 code points", spec(_.truncate("s", 3)))
  check("truncate of binary", spec(_.truncate("bin", 3)))
  check("truncate of a narrow and a wide decimal", spec(_.truncate("dec", 100).truncate("wide", 100000)))
  check("void", spec(_.alwaysNull("i").identity("s")))

  // ---- Nested partition sources (#901 item 7): a source inside (possibly nested) structs.

  // schema: id, st STRUCT<a INT, s STRING, inner STRUCT<d DECIMAL(15,2), day DATE>>
  private val nestedSchema = new Schema(
    optional(20, "id", Types.LongType.get()),
    optional(
      21,
      "st",
      Types.StructType.of(
        optional(22, "a", Types.IntegerType.get()),
        optional(23, "s", Types.StringType.get()),
        optional(
          24,
          "inner",
          Types.StructType.of(
            optional(25, "d", Types.DecimalType.of(15, 2)),
            optional(26, "day", Types.DateType.get())
          )
        )
      )
    )
  )

  private final class Nested(seed: Long) {
    private val r = new Random(seed)
    private def maybe[T](v: => T): Option[T] = if (r.nextInt(7) == 0) None else Some(v)
    val id: Array[Long] = Array.tabulate(n)(_.toLong)
    // A null struct, a null inner struct, and null leaves all occur.
    val stNull: Array[Boolean] = Array.fill(n)(r.nextInt(6) == 0)
    val a: Array[Option[Int]] = Array.fill(n)(maybe(r.nextInt(400) - 200))
    val s: Array[Option[String]] = Array.fill(n)(maybe("p" + r.nextInt(50)))
    val innerNull: Array[Boolean] = Array.fill(n)(r.nextInt(5) == 0)
    val d: Array[Option[Long]] = Array.fill(n)(maybe(r.nextInt(1000000).toLong - 500000L))
    val day: Array[Option[Int]] = Array.fill(n)(maybe(r.nextInt(30000) - 15000))

    def columns(arena: Arena): java.util.List[VectorBuffers] = {
      val idCol = ArrowLayout.ofLongs(arena, id, null)
      val aCol = ArrowLayout.ofInts(arena, a.map(_.getOrElse(0)), a.map(_.isEmpty))
      val sCol = ArrowLayout.ofStrings(arena, s.map(_.orNull))
      val dCol = ArrowLayout.ofLongs(arena, d.map(_.getOrElse(0L)), d.map(_.isEmpty))
      val dayCol = ArrowLayout.ofInts(arena, day.map(_.getOrElse(0)), day.map(_.isEmpty))
      val inner = NestedVectorBuffers.struct(
        n,
        ArrowLayout.validityFrom(arena, innerNull, n),
        java.util.List.of[VectorBuffers](dCol, dayCol)
      )
      val st = NestedVectorBuffers.struct(
        n,
        ArrowLayout.validityFrom(arena, stNull, n),
        java.util.List.of[VectorBuffers](aCol, sCol, inner)
      )
      java.util.List.of[VectorBuffers](idCol, st)
    }

    def record(k: Int): GenericRecord = {
      val rec = GenericRecord.create(nestedSchema)
      rec.set(0, Long.box(id(k)))
      if (stNull(k)) {
        rec.set(1, null)
      } else {
        val stType = nestedSchema.findField(21).`type`().asStructType()
        val st = GenericRecord.create(stType)
        st.set(0, a(k).map(Int.box).orNull)
        st.set(1, s(k).orNull)
        if (innerNull(k)) {
          st.set(2, null)
        } else {
          val innerType = stType.fields().get(2).`type`().asStructType()
          val inner = GenericRecord.create(innerType)
          inner.set(0, d(k).map(v => JBigDecimal.valueOf(v, 2)).orNull)
          inner.set(1, day(k).map(Int.box).orNull)
          st.set(2, inner)
        }
        rec.set(1, st)
      }
      rec
    }
  }

  private def nestedSpec(f: PartitionSpec.Builder => PartitionSpec.Builder): PartitionSpec =
    f(PartitionSpec.builderFor(nestedSchema)).build()

  private def checkNested(name: String, spec: PartitionSpec): Unit =
    test(s"nested: $name equals Iceberg's partition key") {
      val data = new Nested(name.hashCode.toLong)
      val arena = Arena.ofConfined()
      try {
        val columns = data.columns(arena)
        val keys = new PartitionKeys(spec, nestedSchema)
        val values = keys.compute(columns, 0, n)
        val expected = new PartitionKey(spec, nestedSchema)
        (0 until n).foreach { k =>
          expected.partition(data.record(k))
          val got = values.key(k)
          assert(got == expected, s"row $k: ${got.toPath} vs ${expected.toPath}")
        }
      } finally arena.close()
    }

  checkNested("identity of a nested int", nestedSpec(_.identity("st.a")))
  checkNested("bucket of a nested string", nestedSpec(_.bucket("st.s", 8)))
  checkNested("truncate of a nested string", nestedSpec(_.truncate("st.s", 2)))
  checkNested("bucket of a doubly-nested decimal", nestedSpec(_.bucket("st.inner.d", 5)))
  checkNested("truncate of a doubly-nested decimal", nestedSpec(_.truncate("st.inner.d", 100)))
  checkNested("day of a doubly-nested date", nestedSpec(_.day("st.inner.day")))
  checkNested("month of a doubly-nested date", nestedSpec(_.month("st.inner.day")))
  checkNested(
    "several nested sources at once",
    nestedSpec(_.identity("st.a").bucket("st.s", 4).truncate("st.inner.d", 50).year("st.inner.day"))
  )
}
