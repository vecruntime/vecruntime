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
package io.vecruntime.spark.expr

import java.security.MessageDigest
import java.util.zip.CRC32

import io.vecruntime.kernels.{ArrowLayout, Bitmap, SegmentVectorBuffers, StringConcatKernels, VecType, VectorBuffers}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * `hash(...)`: Spark's Murmur3 chain over the children in order, calling Spark's own
 * `Murmur3_x86_32` steps -- a null child leaves the running value alone, `-0.0` is normalised, ints
 * and dates hash as ints, longs, timestamps and short decimals as longs, booleans as 1/0, strings as
 * their bytes. The same per-type rules `XxHash64Expr` uses, so the two agree on what they accept.
 */
final case class Murmur3HashExpr(children: Seq[VectorExpr], types: Seq[DataType], seed: Int) extends VectorExpr {
  override def dataType: DataType = IntegerType
  private val kinds: Array[Int] = types.map(XxHash64Expr.kindOf).toArray

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val cols = children.map(CaseWhenExpr.materialise(_, ctx)).toArray
    val out = ArrowLayout.allocateData(ctx.arena, VecType.INT32, n)
    var i = 0
    while (i < n) {
      var h = seed
      var k = 0
      while (k < cols.length) {
        val v = cols(k)
        if (v.validity() == null || Bitmap.isSet(v.validity(), i)) h = Murmur3HashExpr.step(kinds(k), v, i, h)
        k += 1
      }
      out.setAtIndex(VectorBuffers.LE_INT, i, h)
      i += 1
    }
    SegmentVectorBuffers.fixedWidth(VecType.INT32, n, null, out)
  }
}

object Murmur3HashExpr {
  private def step(kind: Int, v: VectorBuffers, i: Int, h: Int): Int = kind match {
    case XxHash64Expr.INT => Murmur3_x86_32.hashInt(v.data().getAtIndex(VectorBuffers.LE_INT, i), h)
    case XxHash64Expr.LONG => Murmur3_x86_32.hashLong(v.data().getAtIndex(VectorBuffers.LE_LONG, i), h)
    case XxHash64Expr.DOUBLE =>
      val d = v.data().getAtIndex(VectorBuffers.LE_DOUBLE, i)
      Murmur3_x86_32.hashLong(java.lang.Double.doubleToLongBits(if (d == -0.0d) 0.0d else d), h)
    case XxHash64Expr.BOOL => Murmur3_x86_32.hashInt(if (Bitmap.isSet(v.data(), i)) 1 else 0, h)
    case _ =>
      val bytes = v.getUtf8Bytes(i)
      Murmur3_x86_32.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, h)
  }
}

/**
 * `hive-hash(...)`: Hive's hash (Spark's `HiveHash`), which a Hive-compatible bucketed write sorts on (#842).
 * Hive's own `HiveHasher` steps per child so the result is byte-identical to Spark's, and the children combine
 * the way `HiveHash` combines them: `result = 31 * result + childHash` with `result` seeded at 0 and each
 * child's hash computed with its own seed of 0 (a null child contributes 0). The per-type rules match
 * `HiveHashFunction` for the primitive types a bucket key can be: int/byte/short/date as an int, long/timestamp
 * / day-time interval as a long, a short decimal as its unscaled long, boolean as 1/0, float as its int bits,
 * double as its long bits, and a string or binary as its bytes.
 */
final case class HiveHashExpr(children: Seq[VectorExpr], types: Seq[DataType]) extends VectorExpr {
  override def dataType: DataType = IntegerType
  private val kinds: Array[Int] = types.map(HiveHashExpr.kindOf).toArray

  override def eval(ctx: EvalContext): VectorBuffers = {
    val n = ctx.numRows
    val cols = children.map(CaseWhenExpr.materialise(_, ctx)).toArray
    val out = ArrowLayout.allocateData(ctx.arena, VecType.INT32, n)
    var i = 0
    while (i < n) {
      var h = 0
      var k = 0
      while (k < cols.length) {
        val v = cols(k)
        val child = if (v.validity() == null || Bitmap.isSet(v.validity(), i)) HiveHashExpr.step(kinds(k), v, i) else 0
        h = 31 * h + child
        k += 1
      }
      out.setAtIndex(VectorBuffers.LE_INT, i, h)
      i += 1
    }
    SegmentVectorBuffers.fixedWidth(VecType.INT32, n, null, out)
  }
}

object HiveHashExpr {
  import org.apache.spark.sql.catalyst.expressions.HiveHasher
  import org.apache.spark.sql.types.{
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    ShortType,
    TimestampNTZType,
    TimestampType
  }

  val INT = 0
  val LONG = 1
  val DOUBLE = 2
  val FLOAT = 3
  val BOOL = 4
  val BYTES = 5
  val DECIMAL_LONG = 6

  /** The per-type step kind; -1 for a type Hive bucketing does not reach (the compiler rejects it first). */
  def kindOf(dt: DataType): Int = dt match {
    case IntegerType | ByteType | ShortType | DateType => INT
    case LongType | TimestampType | TimestampNTZType | _: org.apache.spark.sql.types.DayTimeIntervalType => LONG
    case _: org.apache.spark.sql.types.YearMonthIntervalType => INT
    case d: DecimalType if d.precision <= 18 => DECIMAL_LONG
    case FloatType => FLOAT
    case DoubleType => DOUBLE
    case BooleanType => BOOL
    case _: StringType => BYTES
    case org.apache.spark.sql.types.BinaryType => BYTES
    case _ => -1
  }

  /** Whether a bucket-key child type is supported by the Hive-hash kernel. */
  def supports(dt: DataType): Boolean = kindOf(dt) >= 0

  private def step(kind: Int, v: VectorBuffers, i: Int): Int = kind match {
    case INT => HiveHasher.hashInt(v.data().getAtIndex(VectorBuffers.LE_INT, i))
    case LONG => HiveHasher.hashLong(v.data().getAtIndex(VectorBuffers.LE_LONG, i))
    case DECIMAL_LONG => HiveHasher.hashLong(v.data().getAtIndex(VectorBuffers.LE_LONG, i))
    case FLOAT =>
      val f = java.lang.Float.intBitsToFloat(v.data().getAtIndex(VectorBuffers.LE_INT, i))
      HiveHasher.hashInt(if (f == -0.0f) 0 else java.lang.Float.floatToIntBits(f))
    case DOUBLE =>
      val d = v.data().getAtIndex(VectorBuffers.LE_DOUBLE, i)
      HiveHasher.hashLong(if (d == -0.0d) 0L else java.lang.Double.doubleToLongBits(d))
    case BOOL => HiveHasher.hashInt(if (Bitmap.isSet(v.data(), i)) 1 else 0)
    case _ =>
      val bytes = v.getUtf8Bytes(i)
      HiveHasher.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length)
  }
}

/**
 * `md5`, `sha1`, `sha2` and `crc32` over the bytes of a UTF8 lane (Spark's `string -> binary` cast is
 * a reinterpretation): a per-row digest loop -- not vectorisable, present so a query containing one
 * keeps the operator. The hex strings go out through the row writer.
 */
final case class DigestExpr(kind: DigestExpr.Kind, child: VectorExpr) extends VectorExpr {
  override def dataType: DataType = if (kind == DigestExpr.Crc32) LongType else StringType
  override def children: Seq[VectorExpr] = Seq(child)

  override def eval(ctx: EvalContext): VectorBuffers = {
    val v = CaseWhenExpr.materialise(child, ctx)
    val n = ctx.numRows
    kind match {
      case DigestExpr.Crc32 =>
        val out = ArrowLayout.allocateData(ctx.arena, VecType.INT64, n)
        val crc = new CRC32
        var i = 0
        while (i < n) {
          if (v.validity() == null || Bitmap.isSet(v.validity(), i)) {
            crc.reset()
            crc.update(v.getUtf8Bytes(i))
            out.setAtIndex(VectorBuffers.LE_LONG, i, crc.getValue)
          }
          i += 1
        }
        SegmentVectorBuffers.fixedWidth(VecType.INT64, n, v.validity(), out)
      case DigestExpr.Sha2(bits) if !DigestExpr.sha2Algorithm(bits).isDefined =>
        // Spark returns null for a bit length it does not know.
        SegmentVectorBuffers.utf8(
          n,
          ArrowLayout.allocateBitmap(ctx.arena, n),
          ArrowLayout.allocateOffsets(ctx.arena, n),
          ArrowLayout.allocateBytes(ctx.arena, 0)
        )
      case _ =>
        val md = MessageDigest.getInstance(kind match {
          case DigestExpr.Md5 => "MD5"
          case DigestExpr.Sha1 => "SHA-1"
          case DigestExpr.Sha2(bits) => DigestExpr.sha2Algorithm(bits).get
          case DigestExpr.Crc32 => throw new IllegalStateException("unreachable")
        })
        val rows = new Array[Array[Byte]](n)
        var i = 0
        while (i < n) {
          if (v.validity() == null || Bitmap.isSet(v.validity(), i)) {
            md.reset()
            rows(i) = DigestExpr.hex(md.digest(v.getUtf8Bytes(i)))
          }
          i += 1
        }
        StringConcatKernels.fromRows(rows, null, ctx.arena)
    }
  }
}

object DigestExpr {
  sealed trait Kind
  case object Md5 extends Kind
  case object Sha1 extends Kind
  final case class Sha2(bits: Int) extends Kind
  case object Crc32 extends Kind

  /** Spark's `sha2` bit lengths: 0 means 256; anything else is a null result. */
  def sha2Algorithm(bits: Int): Option[String] = bits match {
    case 224 => Some("SHA-224")
    case 0 | 256 => Some("SHA-256")
    case 384 => Some("SHA-384")
    case 512 => Some("SHA-512")
    case _ => None
  }

  private val Digits = "0123456789abcdef".getBytes

  /** Lowercase hex, as Spark's `DigestUtils.*Hex`. */
  def hex(bytes: Array[Byte]): Array[Byte] = {
    val out = new Array[Byte](bytes.length * 2)
    var i = 0
    while (i < bytes.length) {
      out(2 * i) = Digits((bytes(i) >> 4) & 0xf)
      out(2 * i + 1) = Digits(bytes(i) & 0xf)
      i += 1
    }
    out
  }
}
