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

import io.vecruntime.kernels.{ArrowLayout, IcebergTransformKernels, SegmentVectorBuffers, VecType, VectorBuffers}
import io.vecruntime.spark.arrow.ArrowOutput
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Substring}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{
  BinaryType,
  DataType,
  DateType,
  DecimalType,
  IntegerType,
  LongType,
  StringType,
  TimestampNTZType,
  TimestampType
}

/** An Iceberg time transform (years / months / days / hours) over a date or timestamp lane; INT32 out. */
final case class IcebergTimeExpr(transform: IcebergTransformKernels.Time, child: VectorExpr, dataType: DataType)
    extends VectorExpr {
  override def children: Seq[VectorExpr] = Seq(child)
  override def eval(ctx: EvalContext): VectorBuffers = {
    val a = IcebergTransformExprs.fixed(child, ctx)
    val out = ArrowLayout.allocateData(ctx.arena, VecType.INT32, ctx.numRows)
    IcebergTransformKernels.time(transform, a, out)
    SegmentVectorBuffers.fixedWidth(VecType.INT32, ctx.numRows, a.validity(), out)
  }
}

/** Iceberg's `bucket(n, v)` over an int, long, date, timestamp, decimal (up to 18 digits), string or binary lane. */
final case class IcebergBucketExpr(numBuckets: Int, child: VectorExpr, sourceType: DataType) extends VectorExpr {
  override def dataType: DataType = IntegerType
  override def children: Seq[VectorExpr] = Seq(child)
  override def eval(ctx: EvalContext): VectorBuffers = {
    val out = ArrowLayout.allocateData(ctx.arena, VecType.INT32, ctx.numRows)
    val a = sourceType match {
      case StringType | BinaryType =>
        val v = CaseWhenExpr.materialise(child, ctx)
        IcebergTransformKernels.bucketBytes(v, numBuckets, out)
        v
      case _: DecimalType =>
        val v = IcebergTransformExprs.fixed(child, ctx)
        IcebergTransformKernels.bucketDecimal(v, numBuckets, out)
        v
      case _ =>
        val v = IcebergTransformExprs.fixed(child, ctx)
        IcebergTransformKernels.bucketIntegral(v, numBuckets, out)
        v
    }
    SegmentVectorBuffers.fixedWidth(VecType.INT32, ctx.numRows, a.validity(), out)
  }
}

/** Iceberg's `truncate(w, v)` over an int, long or decimal (up to 18 digits: its unscaled value) lane. */
final case class IcebergTruncateExpr(width: Int, child: VectorExpr, dataType: DataType) extends VectorExpr {
  override def children: Seq[VectorExpr] = Seq(child)
  override def eval(ctx: EvalContext): VectorBuffers = {
    val a = IcebergTransformExprs.fixed(child, ctx)
    val lane = if (dataType == IntegerType) VecType.INT32 else VecType.INT64
    val out = ArrowLayout.allocateData(ctx.arena, lane, ctx.numRows)
    if (lane == VecType.INT32) IcebergTransformKernels.truncateInt(a, width, out)
    else IcebergTransformKernels.truncateLong(a, width.toLong, out)
    SegmentVectorBuffers.fixedWidth(lane, ctx.numRows, a.validity(), out)
  }
}

/**
 * Iceberg's Spark functions (`org.apache.iceberg.spark.functions`), as Spark binds them: a `StaticInvoke` of
 * the function's magic `invoke` method. They are what Spark's `V2Writes` puts in the exchange and the sort
 * a hash- or range-distributed Iceberg write requires (`bucket(16, id)`, `days(ts)`, ...), so compiling them
 * keeps those operators on batches. Matched by class name: the engine does not depend on Iceberg.
 */
object IcebergTransformExprs {

  private val Prefix = "org.apache.iceberg.spark.functions."

  /** A fixed-width lane of `e`, a dictionary-encoded one decoded. */
  private[expr] def fixed(e: VectorExpr, ctx: EvalContext): VectorBuffers = {
    val v = CaseWhenExpr.materialise(e, ctx)
    if (v.isDictionaryEncoded()) ArrowOutput.decodeDictionary(v, ctx.arena) else v
  }

  /** Whether `s` is one of Iceberg's Spark functions (compiled or not). */
  def isIcebergFunction(s: StaticInvoke): Boolean = s.staticObject.getName.startsWith(Prefix)

  def compile(
      s: StaticInvoke,
      input: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
      compileChild: (Expression, Seq[org.apache.spark.sql.catalyst.expressions.Attribute]) => ExpressionCompiler.Result
  ): ExpressionCompiler.Result = {
    val fn = s.staticObject.getName.stripPrefix(Prefix)
    def column(e: Expression) = compileChild(e, input).flatMap {
      case _: LiteralExpr => Left(s"Iceberg function $fn of a literal")
      case c => Right(c)
    }
    def time(t: IcebergTransformKernels.Time, arg: Expression) = column(arg).map(IcebergTimeExpr(t, _, s.dataType))
    (fn, s.arguments) match {
      case ("YearsFunction$DateToYearsFunction", Seq(a)) if a.dataType == DateType =>
        time(IcebergTransformKernels.Time.DATE_YEARS, a)
      case ("MonthsFunction$DateToMonthsFunction", Seq(a)) if a.dataType == DateType =>
        time(IcebergTransformKernels.Time.DATE_MONTHS, a)
      case ("DaysFunction$DateToDaysFunction", Seq(a)) if a.dataType == DateType =>
        time(IcebergTransformKernels.Time.DATE_DAYS, a)
      case (f, Seq(a)) if a.dataType == TimestampType || a.dataType == TimestampNTZType =>
        f match {
          case "YearsFunction$TimestampToYearsFunction" | "YearsFunction$TimestampNtzToYearsFunction" =>
            time(IcebergTransformKernels.Time.TIMESTAMP_YEARS, a)
          case "MonthsFunction$TimestampToMonthsFunction" | "MonthsFunction$TimestampNtzToMonthsFunction" =>
            time(IcebergTransformKernels.Time.TIMESTAMP_MONTHS, a)
          case "DaysFunction$TimestampToDaysFunction" | "DaysFunction$TimestampNtzToDaysFunction" =>
            time(IcebergTransformKernels.Time.TIMESTAMP_DAYS, a)
          case "HoursFunction$TimestampToHoursFunction" | "HoursFunction$TimestampNtzToHoursFunction" =>
            time(IcebergTransformKernels.Time.TIMESTAMP_HOURS, a)
          case other => Left(s"Iceberg function $other not compiled")
        }
      case (f, Seq(Literal(n: Int, IntegerType), a)) if f.startsWith("BucketFunction$") && n > 0 =>
        a.dataType match {
          case IntegerType | LongType | DateType | TimestampType | TimestampNTZType | StringType | BinaryType =>
            column(a).map(IcebergBucketExpr(n, _, a.dataType))
          case d: DecimalType if d.precision <= io.vecruntime.spark.adapter.TypeMapping.MAX_DECIMAL_PRECISION =>
            column(a).map(IcebergBucketExpr(n, _, a.dataType))
          case other => Left(s"Iceberg bucket over ${other.simpleString} not compiled")
        }
      case ("TruncateFunction$TruncateString", Seq(Literal(w: Int, IntegerType), a))
          if w > 0 && a.dataType == StringType =>
        // The first `w` code points: Spark's substring from 1, which the compiler already has.
        compileChild(Substring(a, Literal(1), Literal(w)), input)
      case ("TruncateFunction$TruncateInt", Seq(Literal(w: Int, IntegerType), a))
          if w > 0 && a.dataType == IntegerType =>
        column(a).map(IcebergTruncateExpr(w, _, IntegerType))
      case ("TruncateFunction$TruncateBigInt", Seq(Literal(w: Int, IntegerType), a))
          if w > 0 && a.dataType == LongType =>
        column(a).map(IcebergTruncateExpr(w, _, LongType))
      case ("TruncateFunction$TruncateDecimal", Seq(Literal(w: Int, IntegerType), a)) if w > 0 =>
        a.dataType match {
          case d: DecimalType if d.precision <= io.vecruntime.spark.adapter.TypeMapping.MAX_DECIMAL_PRECISION =>
            column(a).map(IcebergTruncateExpr(w, _, d))
          case other => Left(s"Iceberg truncate over ${other.simpleString} not compiled")
        }
      case (other, _) => Left(
          s"Iceberg function $other${s.arguments.map(_.dataType.simpleString).mkString("(", ", ", ")")} not compiled"
        )
    }
  }
}
