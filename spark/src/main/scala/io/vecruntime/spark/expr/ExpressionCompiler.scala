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

import io.vecruntime.kernels.{
  ArithOp,
  BitKernels,
  CastKernels,
  CompareOp,
  DateKernels,
  MathKernels,
  PredicateKernels,
  RoundKernels,
  StringCaseKernels,
  StringLengthKernels,
  StringMatchKernels,
  VecType
}
import io.vecruntime.spark.adapter.TypeMapping
import io.vecruntime.kernels.TranscendentalKernels
import org.apache.spark.sql.catalyst.expressions.{
  Abs,
  Acos,
  Acosh,
  Add,
  AddMonths,
  Alias,
  And,
  Ascii,
  Asin,
  Asinh,
  Atan,
  Atan2,
  Atanh,
  Attribute,
  AttributeReference,
  BinaryArithmetic,
  BitLength,
  BitwiseAnd,
  BitwiseCount,
  BitwiseGet,
  BitwiseNot,
  BitwiseOr,
  BitwiseXor,
  BloomFilterMightContain,
  BoundReference,
  BRound,
  CaseWhen,
  Cast,
  Cbrt,
  CheckOverflow,
  CheckOverflowInTableInsert,
  Ceil,
  Chr,
  Coalesce,
  CollationKey,
  CreateArray,
  CreateMap,
  CreateNamedStruct,
  Concat,
  ConcatWs,
  Contains,
  Crc32,
  Cos,
  Cosh,
  Cot,
  Csc,
  DateAdd,
  DateDiff,
  DateFormatClass,
  DateFromUnixDate,
  DateSub,
  DayOfMonth,
  DayOfWeek,
  DayOfYear,
  Divide,
  ElementAt,
  ArrayContains,
  ArrayAggregate,
  ArrayFilter,
  MapFilter,
  TransformValues,
  ZipWith,
  Slice,
  ArrayTransform,
  MapValues,
  ArrayExists,
  ArrayForAll,
  LambdaFunction,
  NamedExpression,
  NamedLambdaVariable,
  ArrayMax,
  ArrayMin,
  ArrayPosition,
  MapKeys,
  Elt,
  EndsWith,
  EqualNullSafe,
  EqualTo,
  EvalMode,
  Exp,
  Expm1,
  Expression,
  FindInSet,
  Floor,
  FromUnixTime,
  GetArrayItem,
  GetArrayStructFields,
  GetMapValue,
  GetStructField,
  GreaterThan,
  Size,
  Greatest,
  GreaterThanOrEqual,
  Hour,
  Hypot,
  If,
  In,
  InitCap,
  InSet,
  IntegralDivide,
  IsNaN,
  IsNotNull,
  IsNull,
  KnownFloatingPointNormalized,
  LastDay,
  Least,
  Length,
  LessThan,
  Like,
  LessThanOrEqual,
  Literal,
  Log,
  Log10,
  Log1p,
  Log2,
  Logarithm,
  Lower,
  Md5,
  Murmur3Hash,
  MakeDate,
  MakeDecimal,
  MicrosToTimestamp,
  MillisToTimestamp,
  Minute,
  MonotonicallyIncreasingID,
  Month,
  MonthsBetween,
  Multiply,
  NaNvl,
  NextDay,
  Not,
  OctetLength,
  Or,
  Pmod,
  Pow,
  Quarter,
  Remainder,
  Rint,
  Round,
  RoundCeil,
  RoundFloor,
  Overlay,
  Sec,
  Second,
  SecondsToTimestamp,
  Sha1,
  Sha2,
  ShiftLeft,
  ShiftRight,
  ShiftRightUnsigned,
  Signum,
  Sin,
  Sinh,
  SparkPartitionID,
  Sqrt,
  StartsWith,
  StringInstr,
  StringLocate,
  StringLPad,
  StringRepeat,
  StringReplace,
  StringRPad,
  StringSpace,
  StringSplitSQL,
  StringTranslate,
  StringTrim,
  StringTrimLeft,
  StringTrimRight,
  Substring,
  SubstringIndex,
  Subtract,
  Tan,
  Tanh,
  ToDegrees,
  ToRadians,
  ToUnixTimestamp,
  TruncDate,
  TruncTimestamp,
  UnaryMathExpression,
  UnaryMinus,
  UnaryPositive,
  UnixDate,
  UnixTimestamp,
  UnixMicros,
  UnixMillis,
  UnixSeconds,
  UnscaledValue,
  Upper,
  WeekDay,
  WeekOfYear,
  XxHash64,
  Year
}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  ShortType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructType,
  TimestampNTZType,
  TimestampType
}

/**
 * Translates Catalyst expressions into [[VectorExpr]] trees. Returns a human-readable reason on
 * failure so the planner rule can tag the Spark operator and fall back.
 */
object ExpressionCompiler {

  type Result = Either[String, VectorExpr]

  /** Whether the expression itself or one of its direct children is BINARY (#705). */
  private def binaryOperand(e: Expression): Boolean =
    e.dataType == BinaryType || e.children.exists(_.dataType == BinaryType)

  /** #812: an expression producing or reading a time or ANSI interval (TypeMapping.isComputeRestricted). */
  private def restrictedOperand(e: Expression): Boolean =
    TypeMapping.isComputeRestricted(e.dataType) || e.children.exists(c => TypeMapping.isComputeRestricted(c.dataType))

  /**
   * #812: the expressions whose result over a time or an ANSI interval is the long's (int's) on its lane:
   * references, comparisons, null tests and the conditionals that pick a value. Arithmetic, casts and functions
   * read the value as a time or an interval and stay Spark's.
   */
  private def restrictedNeutral(e: Expression): Boolean = e match {
    case _: AttributeReference | _: BoundReference | _: Alias => true
    case _: EqualTo | _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual => true
    case Not(_: EqualTo) | _: EqualNullSafe => true
    case _: IsNull | _: IsNotNull => true
    case _: Coalesce | _: If | _: CaseWhen => true
    case Literal(null, _) => true
    case _ => false
  }

  /** An expression producing or reading a collated string, anywhere inside its type (#705). */
  private def collatedOperand(e: Expression): Boolean =
    TypeMapping.isCollated(e.dataType) || e.children.exists(c => TypeMapping.isCollated(c.dataType))

  /**
   * Expressions whose result over BINARY is the same as over a binary-collation string of the same
   * bytes: references, comparisons (unsigned byte order in both), null tests and the conditionals that
   * pick a value. Everything else over binary is refused (#705).
   */
  private def binaryNeutral(e: Expression): Boolean = e match {
    case _: AttributeReference | _: BoundReference | _: Alias => true
    case _: EqualTo | _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual => true
    case Not(_: EqualTo) | _: EqualNullSafe => true
    case _: IsNull | _: IsNotNull => true
    case _: Coalesce | _: If | _: CaseWhen => true
    case Literal(null, _) => true
    // Handled below with their own binary rules: a cast reaches the cast matrix (no binary target or
    // source is in it), a digest takes only `cast(string as binary)`, and a bloom probe reads its binary
    // filter as a subquery result, never as a lane.
    case _: Cast | _: Md5 | _: Sha1 | _: Sha2 | _: Crc32 => true
    case _: BloomFilterMightContain | _: org.apache.spark.sql.vecruntime.PartitionedBloomMightContain => true
    // A collated string's collation key (#705): binary output, compiled by its own rule below.
    case _: CollationKey => true
    case _ => false
  }

  private val comparableTypes: Set[VecType] =
    Set(VecType.INT32, VecType.INT64, VecType.FLOAT64, VecType.UTF8, VecType.DECIMAL128)

  /** Types whose literals can be used as compare / IN operands. */
  private def isLiteralType(dt: DataType): Boolean = dt match {
    case IntegerType | LongType | DoubleType | DateType | TimestampType | TimestampNTZType | StringType | ByteType |
        ShortType | FloatType =>
      true // byte and short literals ride the int lane (#327), float ones the double lane (#712)
    case d: DecimalType => TypeMapping.isSupported(d)
    case _ => false
  }

  /** A decimal wider than the INT64 lane: carried as DECIMAL128 (#257), compared on its limbs (#258). */
  private def isWideDecimal(dt: DataType): Boolean = dt match {
    case d: DecimalType => TypeMapping.hasLane(d) && !TypeMapping.isSupported(d)
    case _ => false
  }

  /**
   * An operand of a wide decimal type (a comparison, an IN, an arithmetic): the bare column, a
   * non-null literal carrying its unscaled value as two limbs, or an expression that compiles onto the
   * lane itself (a wide arithmetic); anything else -- a cast, a function -- keeps its own reason.
   */
  private def wideOperand(e: Expression, input: Seq[Attribute]): Result = e match {
    case Literal(null, dt) => Left(s"null literal of ${dt.simpleString}")
    case Literal(v, dt) => Right(LiteralExpr(v, dt))
    case a: AttributeReference => compileLaneColumn(a, input)
    case other => compile(other, input)
  }

  /** An arithmetic operand: the wide path for a wide type, [[compile]] for a narrow one. */
  private def wideArithmeticOperand(e: Expression, input: Seq[Attribute]): Result = operand(e, input)

  /** Compiles a comparison / IN operand: the wide-decimal path for wide types, [[compile]] otherwise. */
  private def operand(e: Expression, input: Seq[Attribute]): Result =
    if (CollationKeyExpr.collationOf(e.dataType) >= 0) collatedOperandKey(e, input)
    else if (isWideDecimal(e.dataType)) wideOperand(e, input)
    else compile(e, input)

  /**
   * A comparison / IN operand of a collated string type (#705): its collation key -- the column's
   * [[CollationKeyExpr]], a literal's key bytes folded now. Keys are equal exactly when the collation says equal
   * and compare, unsigned, in its order, so the comparison kernels decide as Spark's collation-aware comparator.
   */
  private def collatedOperandKey(e: Expression, input: Seq[Attribute]): Result = {
    val id = CollationKeyExpr.collationOf(e.dataType)
    e match {
      case a: AttributeReference => compileMovedColumn(a, input).map(CollationKeyExpr(_, id))
      case Literal(null, _) => Left("comparison against NULL")
      case Literal(v: UTF8String, _) =>
        Right(LiteralExpr(
          UTF8String.fromBytes(org.apache.spark.sql.catalyst.util.CollationFactory.getCollationKeyBytes(v, id)),
          BinaryType
        ))
      case other => Left(s"unsupported ${other.prettyName} over a collated string")
    }
  }

  /** A comparison, IN or <=> whose operands are collated columns or literals: compiled on their collation keys. */
  private def collatedComparison(e: Expression): Boolean = {
    def plain(x: Expression) = x.isInstanceOf[AttributeReference] || x.isInstanceOf[Literal]
    e match {
      case _: EqualTo | _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual | _: EqualNullSafe =>
        e.children.forall(plain)
      case Not(EqualTo(l, r)) => plain(l) && plain(r)
      case In(v, list) => plain(v) && list.forall(_.isInstanceOf[Literal])
      case InSet(v, _) => plain(v)
      case _ => false
    }
  }

  /**
   * A bare column of any lane type -- including a DECIMAL128 column no kernel computes on yet (#257)
   * -- or any compiled expression. For operators that only move a column (sort keys, gathered
   * payloads); everything that reads the column goes through [[compile]], which refuses it.
   */
  def compileLaneColumn(expr: Expression, input: Seq[Attribute]): Result = expr match {
    // A collated string (#705) has the UTF8 lane but bytes are not its order or equality: never a key,
    // never an operand of the aggregate functions that read their input through here.
    case e if TypeMapping.isCollated(e.dataType) =>
      Left(s"${e.dataType.simpleString} not supported as a key or operand (a collated string)")
    case a: AttributeReference if TypeMapping.hasLane(a.dataType) && !TypeMapping.isSupported(a.dataType) =>
      val ordinal = input.indexWhere(_.exprId == a.exprId)
      if (ordinal < 0) Left(s"unbound attribute ${a.name}") else Right(ColumnRef(ordinal, a.dataType))
    case e => compile(e, input)
  }

  /**
   * A sort or window key: a collated string column orders and compares as its collation key (#705) --
   * equal keys exactly where the collation says equal, unsigned key order its order -- and anything else as
   * [[compileLaneColumn]]. Not for grouping keys, whose output is the column's own value.
   */
  def compileOrderKey(expr: Expression, input: Seq[Attribute]): Result = expr match {
    case a: AttributeReference if CollationKeyExpr.collationOf(a.dataType) >= 0 =>
      compileMovedColumn(a, input).map(CollationKeyExpr(_, CollationKeyExpr.collationOf(a.dataType)))
    case e => compileLaneColumn(e, input)
  }

  /**
   * [[compileLaneColumn]] for operators that only copy a column into their output (an expand's or a
   * rollup's projections, a merge's row values): a collated string column is carried as its bytes (#705).
   */
  def compileMovedColumn(expr: Expression, input: Seq[Attribute]): Result = expr match {
    case a: AttributeReference if TypeMapping.isCollated(a.dataType) && TypeMapping.hasLane(a.dataType) =>
      val ordinal = input.indexWhere(_.exprId == a.exprId)
      if (ordinal < 0) Left(s"unbound attribute ${a.name}") else Right(ColumnRef(ordinal, a.dataType))
    case e => compileLaneColumn(e, input)
  }

  def compile(expr: Expression, input: Seq[Attribute]): Result = expr match {
    // BINARY rides the UTF8 lane (#705), and the string kernels read characters, so only expressions
    // that compare, test or carry a value may touch a binary operand: anything else over binary (length,
    // substring, lpad, casts, hashes, ...) would count UTF-8 characters where Spark counts bytes.
    case e if binaryOperand(e) && !binaryNeutral(e) =>
      Left(s"unsupported ${e.prettyName} over binary")

    // #812: a time or an ANSI interval rides its long (int) lane to be carried, compared, sorted, grouped and
    // joined; anything that reads it as a time or an interval stays Spark's.
    case e if restrictedOperand(e) && !restrictedNeutral(e) =>
      Left(s"unsupported ${e.prettyName} over ${e.dataType.simpleString}")

    // A collated string (#705) rides the UTF8 lane only to be carried: whatever compares, hashes, orders,
    // matches or maps its characters follows the collation, not the bytes, and stays Spark's. A null test
    // reads only validity.
    // Spark's collation key (the join keys RewriteCollationJoin puts on a collated equality, and their exchanges'
    // hash keys): the bytes CollationFactory gives the value, a binary key the kernels compare as any (#705).
    case k @ CollationKey(a: AttributeReference) if CollationKeyExpr.collationOf(a.dataType) >= 0 =>
      compileMovedColumn(a, input).map(CollationKeyExpr(_, k.collationId))
    // Spark 4's bucket hash, which V1Writes sorts a bucketed write on (#842): Murmur3 per key as `Murmur3Hash`,
    // except that a collated string key is hashed as its collation key's bytes, as Spark hashes it.
    case org.apache.spark.sql.catalyst.expressions.CollationAwareMurmur3Hash(children, seed) if children.nonEmpty =>
      children.foldLeft[Either[String, Vector[(VectorExpr, DataType)]]](Right(Vector.empty)) { (acc, c) =>
        acc.flatMap { done =>
          val id = CollationKeyExpr.collationOf(c.dataType)
          if (id >= 0) c match {
            case a: AttributeReference =>
              compileMovedColumn(a, input).map(v => done :+ ((CollationKeyExpr(v, id): VectorExpr, StringType)))
            case other => Left(s"unsupported collation_aware_hash over a collated ")
          }
          else if (!XxHash64Expr.supports(c.dataType))
            Left(s"unsupported collation_aware_hash key type ${c.dataType.simpleString}")
          else compile(c, input).map(v => done :+ ((v, c.dataType)))
        }
      }.map(ks => Murmur3HashExpr(ks.map(_._1), ks.map(_._2), seed))
    // V1Writes puts it on every string partition column: an empty string writes the default partition (#842).
    case org.apache.spark.sql.catalyst.expressions.Empty2Null(child) =>
      compile(child, input).map(Empty2NullExpr(_, child.dataType))
    case e
        if collatedOperand(e) && !(e.isInstanceOf[IsNull] || e.isInstanceOf[IsNotNull] || e.isInstanceOf[Alias]) &&
          !collatedComparison(e) =>
      Left(s"unsupported ${e.prettyName} over a collated string")

    // A scalar subquery result in any reference-free shape (the ScalarSubquery, a struct field of a
    // merged one, a CASE over such fields): a literal by the time the operator runs, read then.
    case e if SubqueryLiteralExpr.isDeferred(e) =>
      if (SubqueryLiteralExpr.supports(e.dataType)) Right(SubqueryLiteralExpr(e))
      else Left(s"subquery result of type ${e.dataType.simpleString} not supported")

    case a: AttributeReference =>
      val ordinal = input.indexWhere(_.exprId == a.exprId)
      if (ordinal < 0) Left(s"unbound attribute ${a.name}")
      else if (!TypeMapping.isSupported(a.dataType)) Left(s"unsupported type ${a.dataType.simpleString} for ${a.name}")
      else Right(ColumnRef(ordinal, a.dataType))

    case b: BoundReference =>
      if (!TypeMapping.isSupported(b.dataType)) Left(s"unsupported type ${b.dataType.simpleString}")
      else Right(ColumnRef(b.ordinal, b.dataType))

    // A struct field, possibly through a chain of structs, read from the struct vector's child (#50).
    case g: GetStructField =>
      structPath(g, input).flatMap { case (ordinal, path) =>
        if (TypeMapping.isSupported(g.dataType)) Right(StructFieldExpr(ordinal, path, g.dataType))
        else Left(
          s"struct field ${g.sql} of type ${g.dataType.simpleString} not supported as a value (#50: nested results are not planned)"
        )
      }
    // size(arr) / size(map): the element count read from Spark's vector (Spark adds size(arr) > 0 below a non-outer explode).
    case s: Size =>
      val isMap = s.child.dataType.isInstanceOf[MapType]
      if (!isMap && !s.child.dataType.isInstanceOf[ArrayType])
        Left(s"size over ${s.child.dataType.simpleString} not supported")
      else nestedColumnPath(s.child, input).map { case (ordinal, path) =>
        SizeExpr(ordinal, path, isMap, s.legacySizeOfNull)
      }
    // One element of an array / one value of a map per row, over the column's nested lane (#705).
    case g @ GetArrayItem(child, ordinal, failOnError) =>
      elementAccess(child, ordinal, ElementAccessExpr.ZeroBased, failOnError, g, input)
    case g @ GetMapValue(child, key) =>
      elementAccess(child, key, ElementAccessExpr.MapKey, failOnError = false, g, input)
    case e @ ElementAt(child, arg, None, failOnError) if child.dataType.isInstanceOf[ArrayType] =>
      elementAccess(child, arg, ElementAccessExpr.OneBased, failOnError, e, input)
    case e @ ElementAt(child, key, None, _) if child.dataType.isInstanceOf[MapType] =>
      elementAccess(child, key, ElementAccessExpr.MapKey, failOnError = false, e, input)
    // Scans of each row's elements over the nested lane (#705): `map_contains_key` is array_contains over map_keys.
    case e @ ArrayContains(MapKeys(child), v) => arrayScan(child, ArrayScanExpr.Contains, v, e, input)
    case e @ ArrayContains(child, v) => arrayScan(child, ArrayScanExpr.Contains, v, e, input)
    case e @ ArrayPosition(child, v) => arrayScan(child, ArrayScanExpr.Position, v, e, input)
    case e @ ArrayMin(child) => arrayScan(child, ArrayScanExpr.Min, null, e, input)
    case e @ ArrayMax(child) => arrayScan(child, ArrayScanExpr.Max, null, e, input)
    case e: ArrayAggregate => arrayAggregate(e, input)
    // Higher-order predicates: the lambda compiled over the element lane (#705).
    case ArrayExists(child, LambdaFunction(body, Seq(v: NamedLambdaVariable), _), followThreeValuedLogic) =>
      arrayPredicate(child, v, body, forAll = false, followThreeValuedLogic, "exists", input)
    case ArrayForAll(child, LambdaFunction(body, Seq(v: NamedLambdaVariable), _)) =>
      arrayPredicate(child, v, body, forAll = true, followThreeValuedLogic = true, "forall", input)
    case g: GetArrayStructFields =>
      Left(s"array of struct fields ${g.sql} not supported (#50: only struct fields are read)")

    // A typed null is an all-invalid column; the operators that need a *value* literal (comparison
    // operands, IN lists, string arguments) match the Spark node before compiling and keep their reasons.
    case Literal(null, dt) if TypeMapping.isSupported(dt) => Right(NullLiteralExpr(dt))
    case Literal(null, dt) => Left(s"null literal of ${dt.simpleString}")
    // A non-null boolean literal is a real constant BOOL column (a `WHERE true`, a join `ON true` /
    // `ON false`, a projected constant); a NULL boolean fell to the NullLiteralExpr case above.
    case Literal(v: Boolean, BooleanType) => Right(ConstBoolExpr(v))
    case Literal(v, dt) if isLiteralType(dt) => Right(LiteralExpr(v, dt))
    case Literal(_, dt) => Left(s"unsupported literal type ${dt.simpleString}")

    case Alias(child, _) => compile(child, input)

    // Spark wraps doubles used as grouping, join and window keys in NormalizeNaNAndZero so that all
    // NaNs and both zeros compare equal. The key tables compare bits, so the normalisation is a real
    // pass over doubles (the marker around it is an identity); floats are not a supported lane type.
    case KnownFloatingPointNormalized(child) => compile(child, input)
    // A float is normalised on its widened double: the canonical NaN and +0.0 narrow back to float's own (#712).
    case NormalizeNaNAndZero(child) if child.dataType == DoubleType || child.dataType == FloatType =>
      compile(child, input).map(NormalizeDoubleExpr(_))
    case NormalizeNaNAndZero(child) => Left(s"NormalizeNaNAndZero over ${child.dataType.simpleString} not supported")

    case EqualTo(l, r) => comparison(CompareOp.EQ, l, r, input)
    case LessThan(l, r) => comparison(CompareOp.LT, l, r, input)
    case LessThanOrEqual(l, r) => comparison(CompareOp.LE, l, r, input)
    case GreaterThan(l, r) => comparison(CompareOp.GT, l, r, input)
    case GreaterThanOrEqual(l, r) => comparison(CompareOp.GE, l, r, input)
    case Not(EqualTo(l, r)) => comparison(CompareOp.NE, l, r, input)

    case In(value, list) => inList(value, list, input)
    case InSet(value, hset) => inSet(value, hset, input)
    case EqualNullSafe(l, r) => nullSafeEquality(l, r, input)
    case IsNaN(child) =>
      if (child.dataType != DoubleType && child.dataType != FloatType)
        Left(s"isnan over ${child.dataType.simpleString}")
      else numericChild(child, input, "isnan").map(IsNaNExpr(_))

    case StartsWith(l, r) => stringMatch(StringMatchKernels.Kind.PREFIX, l, r, input)

    // Hashes: Spark's own Murmur3 steps per child (the xxhash64 chain's per-type rules); digests per row.
    case Murmur3Hash(children, seed) if children.nonEmpty && children.forall(c => XxHash64Expr.supports(c.dataType)) =>
      children.foldLeft[Either[String, Vector[VectorExpr]]](Right(Vector.empty)) { (acc, c) =>
        for (done <- acc; v <- compile(c, input)) yield done :+ v
      }.map(vs => Murmur3HashExpr(vs, children.map(_.dataType), seed))
    case Md5(BinaryFromString(str)) => stringSubject(str, input, "md5").map(DigestExpr(DigestExpr.Md5, _))
    case Sha1(BinaryFromString(str)) => stringSubject(str, input, "sha1").map(DigestExpr(DigestExpr.Sha1, _))
    case Sha2(BinaryFromString(str), Literal(bits: Int, IntegerType)) =>
      stringSubject(str, input, "sha2").map(DigestExpr(DigestExpr.Sha2(bits), _))
    case Sha2(_, _) => Left("sha2 with a non-literal bit length not supported")
    case Crc32(BinaryFromString(str)) => stringSubject(str, input, "crc32").map(DigestExpr(DigestExpr.Crc32, _))

    // The search family: one byte-search primitive under every function; string children only.
    case StringInstr(str, sub) if str.dataType == StringType =>
      for (h <- stringArg(str, input, "instr"); nd <- stringArg(sub, input, "instr"))
        yield LocateExpr(nd, h, LiteralExpr(1, IntegerType))
    case StringLocate(sub, str, start) if str.dataType == StringType =>
      for (
        nd <- stringArg(sub, input, "locate"); h <- stringArg(str, input, "locate");
        st <- intArg(start, input, "locate")
      ) yield LocateExpr(nd, h, st)
    case StringReplace(str, search, replacement) if str.dataType == StringType =>
      for (
        h <- stringSubject(str, input, "replace"); se <- stringArg(search, input, "replace");
        r <- stringArg(replacement, input, "replace")
      ) yield ReplaceExpr(h, se, r)
    case StringTranslate(str, Literal(m: UTF8String, StringType), Literal(r: UTF8String, StringType))
        if str.dataType == StringType =>
      stringSubject(str, input, "translate").map { h =>
        val (from, to) = TranslateExpr.dictionary(m.toString, r.toString)
        TranslateExpr(h, from, to)
      }
    case StringTranslate(_, _, _) => Left("translate with a non-literal from/to string not supported")
    case SubstringIndex(str, delim, count) if str.dataType == StringType =>
      for (
        h <- stringSubject(str, input, "substring_index"); d <- stringArg(delim, input, "substring_index");
        c <- intArg(count, input, "substring_index")
      ) yield SubstringIndexExpr(h, d, c)
    case e @ ElementAt(StringSplitSQL(str, delim), part, Some(Literal(dflt: UTF8String, StringType)), _)
        if str.dataType == StringType && dflt.numBytes == 0 =>
      for (
        h <- stringSubject(str, input, "split_part"); d <- stringArg(delim, input, "split_part");
        k <- intArg(part, input, "split_part")
      ) yield SplitPartExpr(h, d, k, e.origin.context)
    case FindInSet(word, set) if set.dataType == StringType =>
      for (w <- stringArg(word, input, "find_in_set"); st <- stringArg(set, input, "find_in_set"))
        yield FindInSetExpr(w, st)

    // Case mapping: the ASCII rows in the kernel, the rest through Spark's own CollationSupport.
    // A collated column follows ICU rules and is not byte-mapped -- the line Comet draws too.
    case Upper(child) => caseMap(StringCaseKernels.Kind.UPPER, child, input, "upper")
    case Lower(child) => caseMap(StringCaseKernels.Kind.LOWER, child, input, "lower")
    case InitCap(child) => caseMap(StringCaseKernels.Kind.INITCAP, child, input, "initcap")
    case StringTrim(src, trimStr) => trim(StringCaseKernels.Side.BOTH, src, trimStr, input, "trim")
    case StringTrimLeft(src, trimStr) => trim(StringCaseKernels.Side.LEFT, src, trimStr, input, "ltrim")
    case StringTrimRight(src, trimStr) => trim(StringCaseKernels.Side.RIGHT, src, trimStr, input, "rtrim")

    // Several string inputs: per-row lengths summed across them, one buffer, a per-input copy loop.
    // Array and binary forms are declined by type; an all-literal call folds in Spark.
    case Concat(children) if children.nonEmpty && children.forall(_.dataType == StringType) =>
      stringArgs(children, input, "concat").map(ConcatExpr)
    case ConcatWs(children) if children.nonEmpty && children.forall(_.dataType == StringType) =>
      stringArgs(children, input, "concat_ws").map(ps => ConcatWsExpr(ps.head, ps.tail))
    case e @ Elt(children, failOnError) if children.length > 1 && children.tail.forall(_.dataType == StringType) =>
      for (i <- intArg(children.head, input, "elt"); ps <- stringArgs(children.tail, input, "elt"))
        yield EltExpr(i, ps, failOnError, e.origin.context)

    // The measuring family: an INT32 per string, once per dictionary entry on dictionary input.
    case Length(child) if child.dataType == StringType =>
      stringSubject(child, input, "length").map(StringMeasureExpr(StringLengthKernels.Measure.CHARS, _))
    case OctetLength(child) if child.dataType == StringType =>
      stringSubject(child, input, "octet_length").map(StringMeasureExpr(StringLengthKernels.Measure.BYTES, _))
    case BitLength(child) if child.dataType == StringType =>
      stringSubject(child, input, "bit_length").map(StringMeasureExpr(StringLengthKernels.Measure.BITS, _))
    case Ascii(child) =>
      stringSubject(child, input, "ascii").map(StringMeasureExpr(StringLengthKernels.Measure.ASCII, _))
    case Chr(child) =>
      compile(child, input).flatMap {
        case _: LiteralExpr => Left("chr of a literal")
        case c if c.vecType == VecType.INT32 || c.vecType == VecType.INT64 => Right(ChrExpr(c))
        case _ => Left(s"chr over ${child.dataType.simpleString} not supported")
      }

    // The slicing family writes new UTF8 data: Spark's UTF8String semantics per lane (left/right are
    // Spark's own rewrites onto substring; binary inputs are not lanes and fall back).
    case Substring(str, pos, len) if str.dataType == StringType =>
      for (
        s <- stringSubject(str, input, "substring"); p <- intArg(pos, input, "substring");
        l <- intArg(len, input, "substring")
      ) yield SubstringExpr(s, p, l)
    case StringLPad(str, len, pad) =>
      for (
        s <- stringSubject(str, input, "lpad"); l <- intArg(len, input, "lpad", StringSlices.MaxLiteralCount);
        pd <- stringArg(pad, input, "lpad")
      ) yield PadExpr(s, l, pd, left = true)
    case StringRPad(str, len, pad) =>
      for (
        s <- stringSubject(str, input, "rpad"); l <- intArg(len, input, "rpad", StringSlices.MaxLiteralCount);
        pd <- stringArg(pad, input, "rpad")
      ) yield PadExpr(s, l, pd, left = false)
    case StringRepeat(str, times) =>
      for (s <- stringSubject(str, input, "repeat"); t <- intArg(times, input, "repeat", StringSlices.MaxLiteralCount))
        yield RepeatExpr(s, t)
    case StringSpace(n) =>
      intArg(n, input, "space", StringSlices.MaxLiteralCount).flatMap {
        case _: LiteralExpr => Left("space of a literal") // folds in Spark; a constant column has no producer here
        case c => Right(SpaceExpr(c))
      }
    case Overlay(in, replace, pos, len) if in.dataType == StringType =>
      for {
        s <- stringSubject(in, input, "overlay"); r <- stringArg(replace, input, "overlay")
        p <- intArg(pos, input, "overlay"); l <- intArg(len, input, "overlay")
      } yield OverlayExpr(s, r, p, l)
    case EndsWith(l, r) => stringMatch(StringMatchKernels.Kind.SUFFIX, l, r, input)
    case Contains(l, r) => stringMatch(StringMatchKernels.Kind.CONTAINS, l, r, input)
    case like: Like => likeTokens(like, input)

    case And(l, r) => binaryBoolean(l, r, input)(AndExpr.apply)
    case Or(l, r) => binaryBoolean(l, r, input)(OrExpr.apply)
    case Not(child) => booleanChild(child, input).map(NotExpr.apply)

    case IsNull(child) => nullTestChild(child, input).map(IsNullExpr.apply)
    case IsNotNull(child) => nullTestChild(child, input).map(IsNotNullExpr.apply)

    // Conditionals blend same-typed branches by per-branch masks (CaseWhenExpr). IF is the
    // one-branch case; COALESCE picks the first non-null operand, i.e. IS NOT NULL conditions over
    // the operands themselves (an operand is evaluated once for its test and once for its value).
    case CaseWhen(branches, elseValue) => conditional(branches, elseValue, expr, input)
    case If(predicate, trueValue, falseValue) => conditional(Seq((predicate, trueValue)), Some(falseValue), expr, input)
    // A boolean operand with a non-null boolean literal fallback: evaluate the operand once.
    case Coalesce(Seq(c, Literal(fill: Boolean, BooleanType))) if c.dataType == BooleanType =>
      compile(c, input).map(BoolNullFillExpr(_, fill))
    case Coalesce(children) if children.length >= 2 =>
      conditional(children.init.map(c => (IsNotNull(c), c)), Some(children.last), expr, input)

    case e: Add => arithmetic(ArithOp.ADD, e.left, e.right, e.evalMode, e, input)
    case e: Subtract => arithmetic(ArithOp.SUB, e.left, e.right, e.evalMode, e, input)
    case e: Multiply => arithmetic(ArithOp.MUL, e.left, e.right, e.evalMode, e, input)
    case e: Divide => arithmetic(ArithOp.DIV, e.left, e.right, e.evalMode, e, input)

    case e @ UnaryMinus(child, _) if isWideDecimal(child.dataType) =>
      operand(child, input).flatMap {
        case _: LiteralExpr => Left("negation of a literal")
        case c => Right(WideDecimalUnaryExpr(c, abs = false, e.dataType.asInstanceOf[DecimalType]))
      }
    case e @ UnaryMinus(child, failOnError) =>
      compile(child, input).flatMap {
        case _: LiteralExpr => Left("negation of a literal")
        case c if !arithmeticTypes.contains(c.vecType) => Left(s"negation not supported for ${c.dataType.simpleString}")
        // Doubles and decimals of at most 18 digits cannot overflow here; integers raise in ANSI mode.
        case c => Right(NegateExpr(
            c,
            ansi = failOnError && c.vecType != VecType.FLOAT64 && !TypeMapping.isDecimal(c.dataType),
            e.origin.context
          ))
      }

    // The optimizer's DecimalAggregates rewrite: sum(decimal) becomes MakeDecimal(sum(UnscaledValue(x))).
    case UnscaledValue(child) =>
      compile(child, input).flatMap {
        case _: LiteralExpr => Left("unscaled value of a literal")
        case c if !TypeMapping.isDecimal(c.dataType) => Left(s"unscaled value of ${c.dataType.simpleString}")
        case c => Right(UnscaledValueExpr(c))
      }
    case m: MakeDecimal =>
      compile(m.child, input).flatMap {
        case _: LiteralExpr => Left("make_decimal of a literal")
        case c if c.vecType != VecType.INT64 => Left(s"make_decimal of ${c.dataType.simpleString}")
        case c if !TypeMapping.isSupported(m.dataType) =>
          Left(s"make_decimal into ${m.dataType.simpleString} exceeds ${TypeMapping.MAX_DECIMAL_PRECISION} digits")
        case c => Right(MakeDecimalExpr(c, m.dataType.asInstanceOf[DecimalType], m.nullOnOverflow))
      }

    // CheckOverflow(child, decimal(p, s), nullOnOverflow) is Spark's `toPrecision(p, s, HALF_UP)`:
    // a rescale plus a range check, null or NUMERIC_VALUE_OUT_OF_RANGE past the precision. Spark
    // 4.1 no longer leaves it in batch plans (the decimal operators check internally, as
    // DecimalArithExpr does), so this is the identity when the child already has the declared type
    // and the decimal cast's own rescale-and-check otherwise.
    case c @ CheckOverflow(child, dt, nullOnOverflow) =>
      child.dataType match {
        case _ if !TypeMapping.isSupported(dt) =>
          Left(s"check_overflow into ${dt.simpleString} exceeds ${TypeMapping.MAX_DECIMAL_PRECISION} digits")
        case f: DecimalType if !TypeMapping.isSupported(f) =>
          Left(s"check_overflow over ${f.simpleString} exceeds ${TypeMapping.MAX_DECIMAL_PRECISION} digits")
        case f: DecimalType =>
          compile(child, input).flatMap {
            case _: LiteralExpr => Left("check_overflow of a literal")
            case ce if f == dt => Right(ce)
            case ce => Right(DecimalCastExpr(ce, f, dt, ansi = !nullOnOverflow, c.origin.context))
          }
        case other => Left(s"check_overflow over ${other.simpleString}")
      }

    // Casts to the operand's own type (Spark's Average emits `sum.cast(double)` on a double sum).
    case c: Cast if c.child.dataType == c.dataType => compile(c.child, input)

    // A table write's per-column ANSI cast (MERGE/UPDATE/INSERT into a narrower column): the cast
    // compiles as usual and only its overflow error is renamed to the table-insert one.
    case CheckOverflowInTableInsert(cast: Cast, columnName) =>
      compile(cast, input).map(ce => TableInsertOverflowExpr(ce, cast.child.dataType, cast.dataType, columnName))

    // FLOAT (#712): to double is the widened lane as it is; to float from an int lane or a double rounds to
    // single precision once, as Spark's `toFloat`. Every other cast from or to float stays with Spark: a long
    // rounds once in Spark and twice through a double, and a float prints shorter than its double.
    case c: Cast if c.child.dataType == FloatType || c.dataType == FloatType =>
      compile(c.child, input).flatMap {
        case _: LiteralExpr => Left("cast of a literal")
        case ce => (c.child.dataType, c.dataType) match {
            case (FloatType, DoubleType) => Right(RetypeExpr(ce, DoubleType))
            case (DoubleType, FloatType) => Right(RoundFloatExpr(ce))
            case (IntegerType | ShortType | ByteType, FloatType) => Right(RoundFloatExpr(CastExpr(ce, DoubleType)))
            case (f, t) => Left(s"unsupported cast ${f.simpleString} -> ${t.simpleString} (#712)")
          }
      }

    case c: Cast if TypeMapping.isDecimal(c.child.dataType) || TypeMapping.isDecimal(c.dataType) =>
      decimalCast(c, input)

    // timestamp -> date is zone dependent: compiled only under a UTC or fixed-offset session zone.
    case c @ Cast(child, DateType, _, _) if child.dataType == TimestampType =>
      DateExprs.fixedOffsetMicros(c.timeZoneId) match {
        case None =>
          Left(s"cast timestamp -> date needs a fixed-offset session zone, not ${c.timeZoneId.getOrElse("none")}")
        case Some(offset) =>
          compile(child, input).flatMap {
            case _: LiteralExpr => Left("cast of a literal")
            case ce => Right(TimestampToDateExpr(ce, offset))
          }
      }

    // try_cast: the same nodes with null instead of raise -- the non-numeric nodes already null in legacy mode.
    case c: Cast if sliceOneCast(c) =>
      val from = c.child.dataType
      val to = c.dataType
      val ansi = c.evalMode == EvalMode.ANSI
      val tryMode = c.evalMode == EvalMode.TRY
      compile(c.child, input).flatMap {
        case _: LiteralExpr => Left("cast of a literal")
        case ce =>
          (from, to) match {
            // Narrow integers (#327): into tinyint / smallint from any int lane or double; out of them the lane is already an int.
            case (IntegerType | LongType | DoubleType | ShortType | ByteType, ByteType | ShortType) =>
              Right(NarrowIntExpr(ce, to, ansi, c.origin.context, nullOnOverflow = tryMode))
            case (ByteType | ShortType, IntegerType) => Right(RetypeExpr(ce, IntegerType))
            case (LongType, IntegerType) | (DoubleType, IntegerType) | (DoubleType, LongType) =>
              Right(NarrowCastExpr(ce, to, ansi, c.origin.context, nullOnOverflow = tryMode))
            case (IntegerType | LongType | DoubleType, BooleanType) => Right(ToBooleanExpr(ce))
            case (BooleanType, IntegerType | LongType | DoubleType) => Right(FromBooleanExpr(ce, to))
            case (StringType, BooleanType) => Right(StringToBooleanExpr(ce, ansi, c.origin.context))
            case (DateType, TimestampType) =>
              DateExprs.fixedOffsetMicros(c.timeZoneId) match {
                case None => Left(
                    s"cast date -> timestamp needs a fixed-offset session zone, not ${c.timeZoneId.getOrElse("none")}"
                  )
                case Some(offset) => Right(DateToTimestampExpr(ce, offset))
              }
            case (StringType, IntegerType | LongType | DoubleType) =>
              Right(StringToNumberExpr(ce, to, ansi, c.origin.context))
            case (DateType | TimestampType, StringType) =>
              c.timeZoneId match {
                case None => Left("cast to string without a session zone")
                case Some(zone) => Right(DateTimeToStringExpr(ce, from == DateType, zone))
              }
            case (StringType, DateType | TimestampType) =>
              c.timeZoneId match {
                case None => Left("cast from string without a session zone")
                case Some(zone) => Right(StringToDateTimeExpr(ce, to == TimestampType, zone, ansi, c.origin.context))
              }
            case (_, StringType) => Right(ToStringExpr(ce))
            case _ => Left(s"unsupported cast ${from.simpleString} -> ${to.simpleString}")
          }
      }

    case c: Cast =>
      val child = c.child
      val dt = c.dataType
      compile(child, input).flatMap {
        case _: LiteralExpr => Left("cast of a literal")
        case c if !TypeMapping.isSupported(dt) => Left(s"unsupported cast target ${dt.simpleString}")
        case c if !CastKernels.isSupported(c.vecType, TypeMapping.vecTypeOf(dt)) =>
          Left(s"unsupported cast ${child.dataType.simpleString} -> ${dt.simpleString}")
        case c => Right(CastExpr(c, dt))
      }

    case Year(child) => dateField(DateKernels.Field.YEAR, child, input)
    case Month(child) => dateField(DateKernels.Field.MONTH, child, input)
    case DayOfMonth(child) => dateField(DateKernels.Field.DAY, child, input)
    case DayOfYear(child) => dateField(DateKernels.Field.DAY_OF_YEAR, child, input)
    case Quarter(child) => dateField(DateKernels.Field.QUARTER, child, input)
    case DayOfWeek(child) => dateField(DateKernels.Field.DAY_OF_WEEK, child, input)
    case WeekDay(child) => dateField(DateKernels.Field.WEEKDAY, child, input)

    case TruncDate(date, format) =>
      format match {
        case Literal(f: UTF8String, StringType) =>
          DateExprs.truncUnit(f.toString) match {
            case None => Left(s"trunc unit '$f' not supported")
            case Some(unit) => dateChild(date, input).map(DateTruncExpr(unit, _))
          }
        case _ => Left("trunc unit is not a string literal")
      }

    // date +/- days and date - date are INT32 lane arithmetic; Spark does not overflow-check them.
    case e @ DateAdd(start, days) => dateArith(ArithOp.ADD, start, days, e.dataType, input)
    case e @ DateSub(start, days) => dateArith(ArithOp.SUB, start, days, e.dataType, input)
    case e @ DateDiff(end, start) if end.dataType == DateType && start.dataType == DateType =>
      dateArith(ArithOp.SUB, end, start, e.dataType, input)

    // Pattern functions: Spark's own formatter per row (a literal pattern), and date_trunc on timestamps.
    case DateFormatClass(ts, Literal(fmt: UTF8String, StringType), tz) =>
      formatInstant(ts, fmt.toString, tz, input, "date_format")
    case DateFormatClass(_, _, _) => Left("date_format with a non-literal pattern not supported")
    case FromUnixTime(sec, Literal(fmt: UTF8String, StringType), Some(zone)) if sec.dataType == LongType =>
      compile(sec, input).flatMap {
        case _: LiteralExpr => Left("from_unixtime of a literal")
        case c => Right(FormatInstantExpr(c, childIsDate = false, secondsIn = true, fmt.toString, zone, 0L))
      }
    case FromUnixTime(_, _, _) => Left("from_unixtime with a non-literal pattern not supported")
    case UnixTimestamp(t, _, tz, _) => unixTimestamp(t, tz, input, "unix_timestamp")
    case ToUnixTimestamp(t, _, tz, _) => unixTimestamp(t, tz, input, "to_unix_timestamp")
    case TruncTimestamp(Literal(fmt: UTF8String, StringType), ts, tz) =>
      TruncTimestampExpr.of(fmt.toString) match {
        case None => Left(s"date_trunc unit '$fmt' not supported")
        case Some(unit) =>
          DateExprs.fixedOffsetMicros(tz) match {
            case None => Left(s"date_trunc needs a fixed-offset session zone, not ${tz.getOrElse("none")}")
            case Some(offset) => instantLane(ts, input, "date_trunc").map { case (c, isDate) =>
                TruncTimestampExpr(unit, c, isDate, offset)
              }
          }
      }
    case TruncTimestamp(_, _, _) => Left("date_trunc with a non-literal unit not supported")

    // Day-number relabels and epoch scaling: the lane is the same, only Spark's type changes.
    case UnixDate(child) => dateChild(child, input).map(RelabelExpr(_, IntegerType))
    case DateFromUnixDate(child) => intLane(child, input, "date_from_unix_date").map(RelabelExpr(_, DateType))
    case MicrosToTimestamp(child) => integralLane(child, input, "timestamp_micros").map(RelabelExpr(_, TimestampType))
    case UnixMicros(child) => timestampLane(child, input, "unix_micros").map(RelabelExpr(_, LongType))
    case SecondsToTimestamp(child) if child.dataType == IntegerType || child.dataType == LongType =>
      integralLane(child, input, "timestamp_seconds").map(EpochScaleExpr(_, 1000000L, toMicros = true, TimestampType))
    case SecondsToTimestamp(child) => Left(s"timestamp_seconds over ${child.dataType.simpleString} not supported")
    case MillisToTimestamp(child) =>
      integralLane(child, input, "timestamp_millis").map(EpochScaleExpr(_, 1000L, toMicros = true, TimestampType))
    case UnixSeconds(child) =>
      timestampLane(child, input, "unix_seconds").map(EpochScaleExpr(_, 1000000L, toMicros = false, LongType))
    case UnixMillis(child) =>
      timestampLane(child, input, "unix_millis").map(EpochScaleExpr(_, 1000L, toMicros = false, LongType))

    // Month and week arithmetic on the civil-date conversion.
    case LastDay(child) => dateChild(child, input).map(DateScalarExpr(DateScalarExpr.LastDay, _, None, DateType))
    case WeekOfYear(child) =>
      dateChild(child, input).map(DateScalarExpr(DateScalarExpr.WeekOfYear, _, None, IntegerType))
    case AddMonths(start, months) =>
      for (d <- dateChild(start, input); m <- intArg(months, input, "add_months"))
        yield DateScalarExpr(DateScalarExpr.AddMonths, d, Some(m), DateType)
    case NextDay(start, Literal(day: UTF8String, StringType), _) =>
      val code = DateKernels.dayOfWeekCode(day.toString)
      if (code < 0) Left(s"next_day with an unknown day name '$day'")
      else dateChild(start, input).map(DateScalarExpr(DateScalarExpr.NextDay(code), _, None, DateType))
    case NextDay(_, _, _) => Left("next_day with a non-literal day name not supported")
    case e @ MonthsBetween(a, b, Literal(roundOff: Boolean, BooleanType), tz) =>
      DateExprs.fixedOffsetMicros(tz) match {
        case None => Left(s"months_between needs a fixed-offset session zone, not ${tz.getOrElse("none")}")
        case Some(offset) =>
          for (ca <- instantLane(a, input, "months_between"); cb <- instantLane(b, input, "months_between"))
            yield MonthsBetweenExpr(ca._1, cb._1, ca._2, cb._2, roundOff, offset)
      }
    case e @ MakeDate(y, m, d, failOnError) =>
      for (
        cy <- intLane(y, input, "make_date"); cm <- intLane(m, input, "make_date"); cd <- intLane(d, input, "make_date")
      )
        yield MakeDateExpr(cy, cm, cd, failOnError, e.origin.context)

    // Per-partition prefix plus a running row number; state lives in the node, per task.
    case _: MonotonicallyIncreasingID => Right(MonotonicIdExpr())
    // The task's partition index, constant per task.
    case _: SparkPartitionID => Right(SparkPartitionIdExpr())

    case e @ Abs(child, _) if isWideDecimal(child.dataType) =>
      operand(child, input).flatMap {
        case _: LiteralExpr => Left("abs of a literal")
        case c => Right(WideDecimalUnaryExpr(c, abs = true, e.dataType.asInstanceOf[DecimalType]))
      }
    case e @ Abs(child, failOnError) =>
      numericChild(child, input, "abs").map(c =>
        AbsExpr(c, ansi = failOnError && c.vecType != VecType.FLOAT64, e.origin.context)
      )
    case UnaryPositive(child) => compile(child, input)
    case Signum(child) =>
      if (child.dataType != DoubleType) Left(s"signum over ${child.dataType.simpleString}")
      else numericChild(child, input, "signum").map(SignumExpr(_))
    case Sqrt(child) =>
      if (child.dataType != DoubleType) Left(s"sqrt over ${child.dataType.simpleString}")
      else numericChild(child, input, "sqrt").map(SqrtExpr(_))

    // The transcendental family: one scalar call per lane, exactly Spark's (Math or StrictMath per
    // function); the analyzer has cast the argument to double. pi() and e() fold to literals.
    case e: UnaryMathExpression if transcendental.contains(e.getClass) =>
      unaryMath(transcendental(e.getClass), e.child, e.prettyName, input)
    case x: XxHash64 =>
      x.children.find(c => !XxHash64Expr.supports(c.dataType)) match {
        case Some(c) => Left(s"xxhash64 over ${c.dataType.simpleString} not supported")
        case None =>
          x.children.foldRight[Either[String, List[VectorExpr]]](Right(Nil)) { (c, acc) =>
            for (rest <- acc; e <- compile(c, input)) yield e :: rest
          }.map(cs => XxHash64Expr(cs, x.children.map(_.dataType), x.seed))
      }
    case b: BloomFilterMightContain =>
      for {
        filter <- {
          val f = b.bloomFilterExpression
          if (SubqueryLiteralExpr.isDeferred(f)) Right(SubqueryLiteralExpr(f)) // binary: read as bytes, never a lane
          else Left("bloom filter is not a subquery result")
        }
        hash <- compile(b.valueExpression, input).flatMap {
          case h if h.vecType == VecType.INT64 && !h.isInstanceOf[LiteralExpr] => Right(h)
          case _ => Left("bloom filter probe value is not a long lane")
        }
      } yield BloomProbeExpr(filter, hash)

    case b: org.apache.spark.sql.vecruntime.PartitionedBloomMightContain =>
      // The partitioned filter of FactBloomFilter (#653): the same probe, its sub-filter picked by the hash.
      for {
        filter <- {
          val f = b.filter
          if (SubqueryLiteralExpr.isDeferred(f)) Right(SubqueryLiteralExpr(f))
          else Left("partitioned bloom filter is not a subquery result")
        }
        hash <- compile(b.value, input).flatMap {
          case h if h.vecType == VecType.INT64 && !h.isInstanceOf[LiteralExpr] => Right(h)
          case _ => Left("bloom filter probe value is not a long lane")
        }
      } yield BloomProbeExpr(filter, hash, partitioned = true)

    case Pow(l, r) => binaryMath(TranscendentalKernels.Fn2.POW, l, r, "pow", input)
    case Atan2(l, r) => binaryMath(TranscendentalKernels.Fn2.ATAN2, l, r, "atan2", input)
    case Hypot(l, r) => binaryMath(TranscendentalKernels.Fn2.HYPOT, l, r, "hypot", input)
    case Logarithm(l, r) => binaryMath(TranscendentalKernels.Fn2.LOG_BASE, l, r, "log", input)

    case e: Remainder => divideLike(DivideLikeExpr.Rem, e.left, e.right, e.dataType, e.evalMode, e, input)
    case e: Pmod => divideLike(DivideLikeExpr.Pmod, e.left, e.right, e.dataType, e.evalMode, e, input)
    case e: IntegralDivide => divideLike(DivideLikeExpr.Div, e.left, e.right, e.dataType, e.evalMode, e, input)

    case e @ Greatest(children) => pick(MathKernels.Pick.GREATEST, children, e.dataType, input)
    case e @ Least(children) => pick(MathKernels.Pick.LEAST, children, e.dataType, input)

    case NaNvl(l, r) =>
      if (l.dataType != DoubleType || r.dataType != DoubleType) Left(s"nanvl over ${l.dataType.simpleString}")
      else
        for {
          le <- compile(l, input)
          re <- compile(r, input)
          _ <- if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("nanvl of two literals")
          else Right(())
        } yield NanvlExpr(le, re)

    case e @ Ceil(child) => ceilFloor(child, ceil = true, e.dataType, input)
    case e @ Floor(child) => ceilFloor(child, ceil = false, e.dataType, input)
    case Rint(child) =>
      if (child.dataType != DoubleType) Left(s"rint over ${child.dataType.simpleString}")
      else numericChild(child, input, "rint").map(RintExpr(_))
    case r: Round => rounding(RoundKernels.Mode.HALF_UP, "round", r.child, r.scale, r.dataType, r.ansiEnabled, r, input)
    case r: BRound =>
      rounding(RoundKernels.Mode.HALF_EVEN, "bround", r.child, r.scale, r.dataType, r.ansiEnabled, r, input)
    case r: RoundCeil =>
      rounding(RoundKernels.Mode.CEILING, "ceil", r.child, r.scale, r.dataType, ansi = false, r, input)
    case r: RoundFloor =>
      rounding(RoundKernels.Mode.FLOOR, "floor", r.child, r.scale, r.dataType, ansi = false, r, input)

    case e @ BitwiseAnd(l, r) => bitwise(BitKernels.BitOp.AND, "&", l, r, e.dataType, input)
    case e @ BitwiseOr(l, r) => bitwise(BitKernels.BitOp.OR, "|", l, r, e.dataType, input)
    case e @ BitwiseXor(l, r) => bitwise(BitKernels.BitOp.XOR, "^", l, r, e.dataType, input)
    case e @ ShiftLeft(l, r) => bitwise(BitKernels.BitOp.SHL, "shiftleft", l, r, e.dataType, input)
    case e @ ShiftRight(l, r) => bitwise(BitKernels.BitOp.SHR, "shiftright", l, r, e.dataType, input)
    case e @ ShiftRightUnsigned(l, r) => bitwise(BitKernels.BitOp.USHR, "shiftrightunsigned", l, r, e.dataType, input)
    case BitwiseNot(child) => integralChild(child, input, "~").map(BitNotExpr(_))
    case BitwiseCount(child) => integralChild(child, input, "bit_count").map(BitCountExpr(_))
    case _: BitwiseGet => Left("bit_get returns tinyint, which has no lane")

    case h @ Hour(child, _) => timeField(DateKernels.TimeField.HOUR, child, h.timeZoneId, input)
    case m @ Minute(child, _) => timeField(DateKernels.TimeField.MINUTE, child, m.timeZoneId, input)
    case s @ Second(child, _) => timeField(DateKernels.TimeField.SECOND, child, s.timeZoneId, input)

    // Iceberg's partition transforms, as Spark binds Iceberg's functions: the keys of a hash- or
    // range-distributed Iceberg write's exchange and sort (#842).
    case s: org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
        if IcebergTransformExprs.isIcebergFunction(s) =>
      IcebergTransformExprs.compile(s, input, compile)

    case other => Left(s"unsupported expression ${other.getClass.getSimpleName}: ${other.sql}")
  }

  /** The input column and the chain of field ordinals a struct-field access reads; anything but a column or a field of one is refused. */
  private def structPath(e: Expression, input: Seq[Attribute]): Either[String, (Int, Seq[Int])] =
    nestedColumnPath(e, input)

  /**
   * A nested column as (input ordinal, chain of struct-field ordinals): a bare column, or a struct field
   * of one at any depth. Shared with the operators that read Spark's vectors directly (the generate).
   */
  /**
   * `arr[i]`, `element_at(arr, i)`, `map[k]`, `element_at(map, k)` (#705): the container must be a
   * column or a struct field of one with a nested lane, the result a lane (a nested result -- an
   * element that is itself an array -- is a later step), and a map key one the lookup compares.
   */
  private def elementAccess(
      container: Expression,
      arg: Expression,
      mode: ElementAccessExpr.Mode,
      failOnError: Boolean,
      e: Expression,
      input: Seq[Attribute],
      nestedResult: Boolean = false
  ): Result = {
    val what = e.prettyName
    if (TypeMapping.nestedTypeOf(container.dataType) == null)
      Left(s"$what over ${container.dataType.simpleString} not supported (a leaf without a lane)")
    else if (!TypeMapping.hasLane(e.dataType) && !(nestedResult && TypeMapping.nestedTypeOf(e.dataType) != null))
      // A wide decimal element is gathered onto a DECIMAL128 lane like any other (#705): the consumers that read
      // it (comparisons, wide arithmetic, the projection's output) take that lane; the others refuse it by type.
      Left(s"$what with a ${e.dataType.simpleString} result not supported (a leaf without a lane)")
    else {
      val argOk = mode match {
        case ElementAccessExpr.MapKey =>
          val kt = container.dataType.asInstanceOf[MapType].keyType
          val lane = TypeMapping.vecTypeOf(kt)
          if (kt.isInstanceOf[StringType] && kt != StringType)
            Left(s"$what on a collated ${kt.simpleString} key not supported")
          else if (lane == null || !ElementAccessExpr.keyLane(lane) || !TypeMapping.isSupported(kt))
            Left(s"$what on a ${kt.simpleString} key not supported")
          else Right(())
        case _ =>
          arg.dataType match {
            case IntegerType | LongType | ShortType | ByteType => Right(())
            case other => Left(s"$what with a ${other.simpleString} index not supported")
          }
      }
      for (_ <- argOk; p <- nestedColumnPath(container, input); a <- compile(arg, input))
        yield ElementAccessExpr(p._1, p._2, container.dataType, mode, a, failOnError, e.origin.context)
    }
  }

  /**
   * `array_contains`, `array_position`, `array_min`, `array_max` -- and `map_contains_key`, a contains
   * over `map_keys` (#705): the container a column or a struct field of one with a nested lane, the
   * elements on a lane they compare on, the value of the element type.
   */
  private def arrayScan(
      container: Expression,
      kind: ArrayScanExpr.Kind,
      arg: Expression,
      e: Expression,
      input: Seq[Attribute]
  ): Result = {
    val what = e.prettyName
    val elementType = container.dataType match {
      case a: ArrayType => a.elementType
      case m: MapType => m.keyType
      case other => other
    }
    val lane = TypeMapping.vecTypeOf(elementType)
    if (TypeMapping.nestedTypeOf(container.dataType) == null)
      Left(s"$what over ${container.dataType.simpleString} not supported (a leaf without a lane)")
    else if (elementType.isInstanceOf[StringType] && elementType != StringType)
      Left(s"$what over collated ${elementType.simpleString} elements not supported")
    else if (lane == null || !ElementAccessExpr.keyLane(lane) || !TypeMapping.hasLane(elementType))
      Left(s"$what over ${elementType.simpleString} elements not supported")
    // array_min / array_max return an element, gathered at the winning position: a wide decimal one onto a
    // DECIMAL128 lane, compared on its two limbs (#705).
    else if (arg != null && arg.dataType != elementType)
      Left(s"$what of a ${arg.dataType.simpleString} in ${elementType.simpleString} elements not supported")
    else
      for (
        p <- nestedColumnPath(container, input);
        a <- if (arg == null) Right(null)
        else if (isWideDecimal(arg.dataType)) wideOperand(arg, input)
        else compile(arg, input)
      ) yield ArrayScanExpr(p._1, p._2, container.dataType, kind, a)
  }

  /**
   * `exists` / `forall` (#705): the array a column or a struct field of one with a nested lane, its
   * elements on a lane, and the lambda body an expression we compile with the lambda variable as
   * input 0 and the outer lane columns it reads after it. A nested lambda, or a body reading an outer
   * column without a lane, stays with Spark.
   */
  private def arrayPredicate(
      container: Expression,
      variable: NamedLambdaVariable,
      body: Expression,
      forAll: Boolean,
      followThreeValuedLogic: Boolean,
      what: String,
      input: Seq[Attribute]
  ): Result = {
    val elementType = container.dataType match {
      case a: ArrayType => a.elementType
      case other => other
    }
    if (!container.dataType.isInstanceOf[ArrayType] || TypeMapping.nestedTypeOf(container.dataType) == null)
      Left(s"$what over ${container.dataType.simpleString} not supported")
    else if (!laneOrNested(elementType))
      Left(s"$what over ${elementType.simpleString} elements not supported")
    else
      for (
        p <- nestedColumnPath(container, input);
        l <- lambdaOverElements(Seq(variable), body, what, input)
      )
        yield
          if (l._1.dataType != BooleanType) throw new IllegalStateException(s"$what lambda of ${l._1.dataType}")
          else ArrayPredicateExpr(p._1, p._2, container.dataType, forAll, l._1, l._2, followThreeValuedLogic)
  }

  /**
   * A value a lambda can read: a leaf with a lane, or an array / map / struct with a nested lane (#705). A wide
   * decimal (DECIMAL128) counts: the lambda moves it, and what reads it -- a comparison, wide arithmetic -- takes
   * the wide operand path or refuses it with its own reason.
   */
  private def laneOrNested(dt: DataType): Boolean =
    TypeMapping.hasLane(dt) || TypeMapping.nestedTypeOf(dt) != null

  /**
   * Every lambda variable `body` reads is one of `ids` or bound by a lambda inside `body` (#705, nested
   * lambdas): an inner lambda is compiled by its own higher-order function, which binds its variables
   * and reads ours as outer columns.
   */
  private def lambdaScoped(body: Expression, ids: Set[org.apache.spark.sql.catalyst.expressions.ExprId]): Boolean = {
    val bound = ids ++ body.collect { case l: LambdaFunction =>
      l.arguments.collect { case v: NamedLambdaVariable => v.exprId }
    }.flatten
    body.find { case v: NamedLambdaVariable => !bound.contains(v.exprId); case _ => false }.isEmpty
  }

  /**
   * A lambda over array elements (#705) compiled as an ordinary expression: its variables -- the
   * element, and the element's int index when it takes two -- become inputs 0 (and 1), the outer
   * lane columns the body reads follow. Returns the body and the outer columns' input ordinals. A
   * nested lambda, or an outer column without a lane, is refused.
   */
  private def lambdaOverElements(
      variables: Seq[NamedExpression],
      body: Expression,
      what: String,
      input: Seq[Attribute],
      secondIsIndex: Boolean = true
  ): Either[String, (VectorExpr, Seq[Int])] = {
    val vars = variables.collect { case v: NamedLambdaVariable => v }
    val ids = vars.map(_.exprId).toSet
    if (vars.length != variables.length || vars.isEmpty || vars.length > 2)
      Left(s"$what with a lambda of ${variables.length} arguments not supported")
    else if (vars.exists(v => !laneOrNested(v.dataType)))
      Left(s"$what over ${vars.map(_.dataType.simpleString).mkString(", ")} variables not supported")
    else if (secondIsIndex && vars.length == 2 && vars(1).dataType != IntegerType)
      Left(s"$what with an index of ${vars(1).dataType.simpleString} not supported")
    else if (!lambdaScoped(body, ids)) Left(s"$what reading a lambda variable it does not bind")
    else {
      val outerAttrs = body.collect { case a: AttributeReference => a }.distinct
      outerAttrs.find(a => !laneOrNested(a.dataType)) match {
        case Some(a) =>
          Left(s"$what reading the outer column ${a.name} of type ${a.dataType.simpleString} not supported")
        case None =>
          val lambdaAttrs = vars.map(v => AttributeReference(v.name, v.dataType, v.nullable)(exprId = v.exprId))
          val byId = vars.map(_.exprId).zip(lambdaAttrs).toMap
          val replaced = body.transform { case v: NamedLambdaVariable if byId.contains(v.exprId) => byId(v.exprId) }
          val outerOrdinals = outerAttrs.map(a => input.indexWhere(_.exprId == a.exprId))
          if (outerOrdinals.exists(_ < 0)) Left(s"$what over an unbound outer column")
          // A nested body (#705: a lambda returning an array, a map or a struct) compiles as a projection.
          else compileProjection(replaced, lambdaAttrs ++ outerAttrs).map(c => (c, outerOrdinals))
      }
    }
  }

  /**
   * A projection that may have an array result (#705, nested results): `map_keys`, `map_values`,
   * `transform` and `filter` build an array lane; everything else is [[compile]].
   */
  def compileProjection(expr: Expression, input: Seq[Attribute]): Result =
    if (TypeMapping.nestedTypeOf(expr.dataType) != null) compileNested(expr, input)
    else if (isWideDecimal(expr.dataType)) compileWideResult(expr, input)
    else compile(expr, input)

  /** A wide decimal result: a column (a lambda variable included) moved as its DECIMAL128 lane, else [[compile]]. */
  private def compileWideResult(expr: Expression, input: Seq[Attribute]): Result = expr match {
    case Alias(child, _) => compileWideResult(child, input)
    case a: AttributeReference => compileLaneColumn(a, input)
    case other => compile(other, input)
  }

  private def compileNested(expr: Expression, input: Seq[Attribute]): Result = expr match {
    case Alias(child, _) => compileNested(child, input)
    // The optimizer's marker over a nested grouping or join key whose float leaves it normalised (#705): the
    // normalisation itself is the child (a transform, a struct of normalised fields), compiled as it is.
    case KnownFloatingPointNormalized(child) => compileNested(child, input)
    // A nested column, or a nested struct field of one, as its value (#705): a lambda returning its element.
    case a: AttributeReference if TypeMapping.nestedTypeOf(a.dataType) != null =>
      nestedColumnPath(a, input).map { case (o, path) => NestedValueExpr(o, path, a.dataType) }
    case g: GetStructField if TypeMapping.nestedTypeOf(g.dataType) != null =>
      nestedColumnPath(g, input).map { case (o, path) => NestedValueExpr(o, path, g.dataType) }
    case e @ MapKeys(child) => mapPart(child, values = false, e, input)
    case e @ MapValues(child) => mapPart(child, values = true, e, input)
    case e @ ArrayTransform(child, LambdaFunction(body, vars, _)) =>
      lambdaArray(child, vars, body, keep = false, e, input)
    case e @ ArrayFilter(child, LambdaFunction(body, vars, _)) => lambdaArray(child, vars, body, keep = true, e, input)
    // Element access whose element is itself an array, map or struct: the element gathered as a nested lane.
    case g @ GetArrayItem(child, ordinal, failOnError) =>
      elementAccess(child, ordinal, ElementAccessExpr.ZeroBased, failOnError, g, input, nestedResult = true)
    case g @ GetMapValue(child, key) =>
      elementAccess(child, key, ElementAccessExpr.MapKey, failOnError = false, g, input, nestedResult = true)
    case e @ ElementAt(child, arg, None, failOnError) if child.dataType.isInstanceOf[ArrayType] =>
      elementAccess(child, arg, ElementAccessExpr.OneBased, failOnError, e, input, nestedResult = true)
    case e @ ElementAt(child, key, None, _) if child.dataType.isInstanceOf[MapType] =>
      elementAccess(child, key, ElementAccessExpr.MapKey, failOnError = false, e, input, nestedResult = true)
    case e @ Slice(child, start, length) => slice(child, start, length, e, input)
    case e @ ZipWith(l, r, LambdaFunction(body, vars, _)) => zipWith(l, r, vars, body, e, input)
    case e @ MapFilter(child, LambdaFunction(body, vars, _)) => mapLambda(child, vars, body, filter = true, e, input)
    case e @ TransformValues(child, LambdaFunction(body, vars, _)) =>
      mapLambda(child, vars, body, filter = false, e, input)
    // A struct constructor (#705), alone or under the null guard NormalizeFloatingNumbers puts around a
    // struct key it rebuilds with normalised fields: the struct is null exactly where the guarded value is.
    case s: CreateNamedStruct => namedStruct(s, None, input)
    case If(IsNull(guard), Literal(null, _), s: CreateNamedStruct)
        if TypeMapping.nestedTypeOf(guard.dataType) != null =>
      compileNested(guard, input).flatMap(g => namedStruct(s, Some(g), input))
    // `if(p, null, x)` / `if(p, x, null)` over a nested `x` (#842): `x` with its validity narrowed.
    case If(p, Literal(null, _), x) => nullWhen(p, x, nullWhenTrue = true, expr.dataType, input)
    case If(p, x, Literal(null, _)) => nullWhen(p, x, nullWhenTrue = false, expr.dataType, input)
    // A struct, array or map literal (all-literal children Spark folded): lifted to its constructor and
    // compiled through the same path, so a folded `named_struct`/`array`/`map` is built like an unfolded one.
    case Literal(value, dt) if TypeMapping.nestedTypeOf(dt) != null =>
      if (value == null) Left(s"null literal of ${dt.simpleString}")
      else compileNested(NestedLiterals.lift(value, dt), input)
    // `array(...)` and `map(...)` constructors (#842, #901 R2): their children may be scalar lanes,
    // moved nested columns, null literals or other constructors, to any depth.
    case c: CreateArray if c.children.nonEmpty =>
      constructorLanes(c.children, "array", input).map(CreateArrayExpr(_, c.dataType))
    case m: CreateMap if m.keys.nonEmpty =>
      // Keys may repeat: resolved at runtime by Spark's spark.sql.mapKeyDedupPolicy, per row
      // (CreateMapExpr). A null key fails as Spark's does. Keys and values may be nested to any depth.
      for (k <- constructorLanes(m.keys, "map", input); v <- constructorLanes(m.values, "map", input))
        yield CreateMapExpr(k, v, m.dataType)
    // Anything else keeps the reason the ordinary compiler gives it (element access with a nested
    // element, ...).
    case other => compile(other, input)
  }

  /** `if(p, null, x)` (`nullWhenTrue`) or `if(p, x, null)` over a nested `x` (#842). */
  private def nullWhen(
      p: Expression,
      x: Expression,
      nullWhenTrue: Boolean,
      dataType: DataType,
      input: Seq[Attribute]
  ): Result =
    if (TypeMapping.nestedTypeOf(x.dataType) == null) Left(s"if over ${x.dataType.simpleString}")
    else for (pe <- compilePredicate(p, input); xe <- compileNested(x, input))
      yield NestedNullWhenExpr(pe, xe, nullWhenTrue, dataType)

  /**
   * The children of an `array(...)` / `map(...)` constructor, each of one shared element type: a scalar
   * lane, or -- to any depth (#901 R2) -- a moved nested column, a null literal or another constructor,
   * compiled as a projection. The constructor's own analysis has given them all one type.
   */
  private def constructorLanes(
      es: Seq[Expression],
      what: String,
      input: Seq[Attribute]
  ): Either[String, Seq[VectorExpr]] =
    // Spark's analysis has coerced the children to one element type; they can still differ only in
    // nullability (a `named_struct` of non-null fields beside one of nullable fields), which is not a
    // mixed type and does not change the lane. Normalising nullability folds that difference away.
    if (es.map(e => asNullable(e.dataType)).distinct.length != 1) Left(s"$what(...) of mixed element types")
    else if (TypeMapping.nestedTypeOf(es.head.dataType) != null)
      // Nested elements: each compiled as a projection (a moved column, a null, or a constructor).
      es.foldRight[Either[String, List[VectorExpr]]](Right(Nil)) { (e, acc) =>
        for (done <- acc; v <- compileProjection(e, input)) yield v :: done
      }
    else if (es.exists(e => !TypeMapping.isSupported(e.dataType)))
      Left(s"$what(...) of ${es.map(_.dataType.simpleString).distinct.mkString(", ")}")
    else es.foldRight[Either[String, List[VectorExpr]]](Right(Nil)) { (e, acc) =>
      for (done <- acc; v <- compile(e, input)) yield v :: done
    }

  /** A type with every field, element and value made nullable, so two children differ only where their shape does. */
  private def asNullable(dt: DataType): DataType = dt match {
    case a: ArrayType => ArrayType(asNullable(a.elementType), containsNull = true)
    case m: MapType => MapType(asNullable(m.keyType), asNullable(m.valueType), valueContainsNull = true)
    case s: StructType =>
      StructType(s.fields.map(f => f.copy(dataType = asNullable(f.dataType), nullable = true)))
    case other => other
  }

  /** `named_struct(...)`'s fields, each a lane or a nested value, as one struct lane (#705). */
  private def namedStruct(s: CreateNamedStruct, nullWhen: Option[VectorExpr], input: Seq[Attribute]): Result =
    s.valExprs.foldRight[Either[String, List[VectorExpr]]](Right(Nil)) { (f, acc) =>
      for (rest <- acc; v <- compileProjection(f, input)) yield v :: rest
    }.map(fields => NamedStructExpr(fields, nullWhen, s.dataType))

  /**
   * A folded nested literal (a `named_struct` / `array` / `map` of only literals Spark constant-folded
   * to one `Literal(InternalRow | ArrayData | MapData, type)`, #901 R2) lifted back to its constructor,
   * so the compiler builds it exactly as it builds an unfolded one. A non-null value only; the caller
   * handles a null literal.
   */
  private object NestedLiterals {
    def lift(value: Any, dt: DataType): Expression = dt match {
      case s: StructType =>
        val row = value.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow]
        val args = s.fields.zipWithIndex.flatMap { case (f, i) =>
          Seq(
            Literal(org.apache.spark.unsafe.types.UTF8String.fromString(f.name), StringType),
            fieldLiteral(row, i, f.dataType)
          )
        }
        CreateNamedStruct(args.toSeq)
      case a: ArrayType =>
        val data = value.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
        CreateArray((0 until data.numElements()).map(i => elementLiteral(data, i, a.elementType)))
      case m: MapType =>
        val data = value.asInstanceOf[org.apache.spark.sql.catalyst.util.MapData]
        val keys = data.keyArray()
        val vals = data.valueArray()
        val entries = (0 until data.numElements()).flatMap { i =>
          Seq(elementLiteral(keys, i, m.keyType), elementLiteral(vals, i, m.valueType))
        }
        CreateMap(entries)
      case other => throw new IllegalStateException(s"not a nested literal type: ${other.simpleString}")
    }

    private def fieldLiteral(row: org.apache.spark.sql.catalyst.InternalRow, i: Int, dt: DataType): Expression =
      if (row.isNullAt(i)) Literal(null, dt)
      else lower(row.get(i, dt), dt)

    private def elementLiteral(
        data: org.apache.spark.sql.catalyst.util.ArrayData,
        i: Int,
        dt: DataType
    ): Expression =
      if (data.isNullAt(i)) Literal(null, dt) else lower(data.get(i, dt), dt)

    /** A scalar field/element as a `Literal`, a nested one lifted recursively. */
    private def lower(v: Any, dt: DataType): Expression =
      if (TypeMapping.nestedTypeOf(dt) != null) lift(v, dt) else Literal(v, dt)
  }

  /** `zip_with(a, b, (x, y) -> f)` (#705): two arrays with nested lanes, a lambda with a lane result. */
  private def zipWith(
      l: Expression,
      r: Expression,
      vars: Seq[NamedExpression],
      body: Expression,
      e: Expression,
      input: Seq[Attribute]
  ): Result =
    if (TypeMapping.nestedTypeOf(l.dataType) == null || TypeMapping.nestedTypeOf(r.dataType) == null)
      Left(s"zip_with over ${l.dataType.simpleString} and ${r.dataType.simpleString} not supported")
    else if (vars.length != 2) Left("zip_with with a lambda of other than two arguments")
    else if (!laneOrNested(body.dataType)) Left(s"zip_with to ${body.dataType.simpleString} not supported")
    else
      for (
        lp <- nestedColumnPath(l, input); rp <- nestedColumnPath(r, input);
        lam <- lambdaOverElements(vars, body, "zip_with", input, secondIsIndex = false)
      ) yield ZipWithExpr(lp, l.dataType, rp, r.dataType, lam._1, lam._2, e.dataType)

  /** `map_filter` / `transform_values` (#705): a map with a nested lane, the lambda over key and value. */
  private def mapLambda(
      container: Expression,
      vars: Seq[NamedExpression],
      body: Expression,
      filter: Boolean,
      e: Expression,
      input: Seq[Attribute]
  ): Result = {
    val what = e.prettyName
    if (!container.dataType.isInstanceOf[MapType] || TypeMapping.nestedTypeOf(container.dataType) == null)
      Left(s"$what over ${container.dataType.simpleString} not supported")
    else if (vars.length != 2) Left(s"$what with a lambda of other than two arguments")
    else if (!filter && !laneOrNested(body.dataType))
      Left(s"$what to ${body.dataType.simpleString} values not supported")
    else
      for (
        p <- nestedColumnPath(container, input);
        lam <- lambdaOverElements(vars, body, what, input, secondIsIndex = false)
      ) yield MapLambdaExpr(p._1, p._2, container.dataType, lam._1, lam._2, filter, e.dataType)
  }

  /**
   * `aggregate(arr, zero, (acc, x) -> merge, acc -> finish)` (#705): the array a column or a struct
   * field of one with a nested lane, the accumulator and the result on lanes. The merge is compiled as
   * `if(live, merge, acc)` over the accumulator, the element and a live flag (ArrayAggregateExpr folds
   * position by position across the batch); the finish over the accumulator, or nothing for identity.
   */
  private def arrayAggregate(e: ArrayAggregate, input: Seq[Attribute]): Result = e match {
    case ArrayAggregate(
          container,
          zero,
          LambdaFunction(mb, Seq(acc: NamedLambdaVariable, x: NamedLambdaVariable), _),
          LambdaFunction(fb, Seq(acc2: NamedLambdaVariable), _)
        ) =>
      def nested(b: Expression, ids: Set[org.apache.spark.sql.catalyst.expressions.ExprId]) = !lambdaScoped(b, ids)
      def outerOf(b: Expression): Either[String, (Seq[AttributeReference], Seq[Int])] = {
        val attrs = b.collect { case a: AttributeReference => a }.distinct
        attrs.find(a => !laneOrNested(a.dataType)) match {
          case Some(a) => Left(s"aggregate reading the outer column ${a.name} of type ${a.dataType.simpleString}")
          case None =>
            val ords = attrs.map(a => input.indexWhere(_.exprId == a.exprId))
            if (ords.exists(_ < 0)) Left("aggregate over an unbound outer column") else Right((attrs, ords))
        }
      }
      val elementType = container.dataType match { case a: ArrayType => a.elementType; case o => o }
      if (!container.dataType.isInstanceOf[ArrayType] || TypeMapping.nestedTypeOf(container.dataType) == null)
        Left(s"aggregate over ${container.dataType.simpleString} not supported")
      else if (
        !laneOrNested(elementType) || !TypeMapping.isSupported(acc.dataType) ||
        !TypeMapping.isSupported(e.dataType)
      )
        Left(s"aggregate of ${elementType.simpleString} into ${acc.dataType.simpleString} not supported")
      else if (nested(mb, Set(acc.exprId, x.exprId)) || nested(fb, Set(acc2.exprId)))
        Left("aggregate reading a lambda variable it does not bind")
      else {
        val accAttr = AttributeReference(acc.name, acc.dataType, acc.nullable)(exprId = acc.exprId)
        val xAttr = AttributeReference(x.name, x.dataType, x.nullable)(exprId = x.exprId)
        val liveAttr = AttributeReference("__live", BooleanType, nullable = false)()
        val merged = mb.transform {
          case v: NamedLambdaVariable if v.exprId == acc.exprId => accAttr
          case v: NamedLambdaVariable if v.exprId == x.exprId => xAttr
        }
        for {
          mo <- outerOf(mb)
          mergeC <- compile(If(liveAttr, merged, accAttr), Seq(accAttr, xAttr, liveAttr) ++ mo._1)
          zeroC <- compile(zero, input)
          p <- nestedColumnPath(container, input)
          fin <- fb match {
            case v: NamedLambdaVariable if v.exprId == acc2.exprId => Right(None)
            case _ =>
              val acc2Attr = AttributeReference(acc2.name, acc2.dataType, acc2.nullable)(exprId = acc2.exprId)
              val f = fb.transform { case v: NamedLambdaVariable if v.exprId == acc2.exprId => acc2Attr }
              for (fo <- outerOf(fb); fc <- compile(f, acc2Attr +: fo._1)) yield Some((fc, fo._2))
          }
        } yield ArrayAggregateExpr(p._1, p._2, container.dataType, zeroC, mergeC, mo._2, fin, e.dataType)
      }
    case other => Left(s"aggregate of shape ${other.sql} not supported")
  }

  /** `slice(arr, start, length)` (#705): a column or a struct field of one with a nested lane, int bounds. */
  private def slice(container: Expression, start: Expression, length: Expression, e: Expression, input: Seq[Attribute])
      : Result =
    if (TypeMapping.nestedTypeOf(container.dataType) == null)
      Left(s"slice over ${container.dataType.simpleString} not supported")
    else if (start.dataType != IntegerType || length.dataType != IntegerType)
      Left(s"slice with ${start.dataType.simpleString} / ${length.dataType.simpleString} bounds not supported")
    else
      for (p <- nestedColumnPath(container, input); s <- compile(start, input); l <- compile(length, input))
        yield SliceExpr(p._1, p._2, container.dataType, s, l, e.dataType)

  private def mapPart(container: Expression, values: Boolean, e: Expression, input: Seq[Attribute]): Result =
    if (TypeMapping.nestedTypeOf(container.dataType) == null)
      Left(s"${e.prettyName} over ${container.dataType.simpleString} not supported")
    else nestedColumnPath(container, input).map { case (o, path) =>
      MapPartExpr(o, path, container.dataType, values, e.dataType)
    }

  private def lambdaArray(
      container: Expression,
      vars: Seq[NamedExpression],
      body: Expression,
      keep: Boolean,
      e: Expression,
      input: Seq[Attribute]
  ): Result = {
    val what = e.prettyName
    val elementType = container.dataType match { case a: ArrayType => a.elementType; case other => other }
    if (!container.dataType.isInstanceOf[ArrayType] || TypeMapping.nestedTypeOf(container.dataType) == null)
      Left(s"$what over ${container.dataType.simpleString} not supported")
    else if (!laneOrNested(elementType))
      Left(s"$what over ${elementType.simpleString} elements not supported")
    else if (!keep && !laneOrNested(body.dataType))
      Left(s"$what to ${body.dataType.simpleString} elements not supported")
    else
      for (p <- nestedColumnPath(container, input); l <- lambdaOverElements(vars, body, what, input))
        yield
          if (keep) ArrayFilterExpr(p._1, p._2, container.dataType, l._1, l._2, vars.length == 2, e.dataType)
          else TransformExpr(p._1, p._2, container.dataType, l._1, l._2, vars.length == 2, e.dataType)
  }

  def nestedColumnPath(e: Expression, input: Seq[Attribute]): Either[String, (Int, Seq[Int])] = e match {
    case a: AttributeReference =>
      val ordinal = input.indexWhere(_.exprId == a.exprId)
      if (ordinal < 0) Left(s"unbound attribute ${a.name}") else Right((ordinal, Nil))
    case GetStructField(child, ordinal, _) => nestedColumnPath(child, input).map { case (o, path) =>
        (o, path :+ ordinal)
      }
    case other => Left(s"nested access over ${other.sql} not supported (only a column or a struct field of one)")
  }

  /** A compiled non-literal operand on an INT32 / INT64 / FLOAT64 lane (decimals are #49). */
  private val transcendental: Map[Class[_], TranscendentalKernels.Fn] = {
    import TranscendentalKernels.Fn._
    Map(
      classOf[Exp] -> EXP,
      classOf[Expm1] -> EXPM1,
      classOf[Log] -> LOG,
      classOf[Log2] -> LOG2,
      classOf[Log10] -> LOG10,
      classOf[Log1p] -> LOG1P,
      classOf[Cbrt] -> CBRT,
      classOf[Sin] -> SIN,
      classOf[Cos] -> COS,
      classOf[Tan] -> TAN,
      classOf[Asin] -> ASIN,
      classOf[Acos] -> ACOS,
      classOf[Atan] -> ATAN,
      classOf[Sinh] -> SINH,
      classOf[Cosh] -> COSH,
      classOf[Tanh] -> TANH,
      classOf[Asinh] -> org.apache.spark.sql.vecruntime.shims.SparkShims.asinh,
      classOf[Acosh] -> org.apache.spark.sql.vecruntime.shims.SparkShims.acosh,
      classOf[Atanh] -> ATANH,
      classOf[Cot] -> COT,
      classOf[Sec] -> SEC,
      classOf[Csc] -> CSC,
      classOf[ToDegrees] -> DEGREES,
      classOf[ToRadians] -> RADIANS
    )
  }

  private def doubleChild(e: Expression, input: Seq[Attribute], what: String): Result =
    if (e.dataType != DoubleType) Left(s"$what over ${e.dataType.simpleString}")
    else numericChild(e, input, what)

  private def unaryMath(fn: TranscendentalKernels.Fn, child: Expression, what: String, input: Seq[Attribute]): Result =
    doubleChild(child, input, what).map(UnaryMathExpr(fn, _))

  /** A binary math function: both sides double, a literal on at most one side (both would have folded). */
  private def binaryMath(
      fn: TranscendentalKernels.Fn2,
      l: Expression,
      r: Expression,
      what: String,
      input: Seq[Attribute]
  ): Result =
    if (l.dataType != DoubleType || r.dataType != DoubleType)
      Left(s"$what over ${l.dataType.simpleString}, ${r.dataType.simpleString}")
    else for (a <- compile(l, input); b <- compile(r, input)) yield {
      (a, b) match {
        case (_: LiteralExpr, _: LiteralExpr) => return Left(s"$what of two literals")
        case _ =>
      }
      if (!a.isInstanceOf[LiteralExpr] && !arithmeticTypes.contains(a.vecType))
        return Left(s"$what over ${l.dataType.simpleString} not supported")
      if (!b.isInstanceOf[LiteralExpr] && !arithmeticTypes.contains(b.vecType))
        return Left(s"$what over ${r.dataType.simpleString} not supported")
      BinaryMathExpr(fn, a, b)
    }

  private def numericChild(e: Expression, input: Seq[Attribute], what: String): Result =
    compile(e, input).flatMap {
      case _: LiteralExpr => Left(s"$what of a literal")
      case c if TypeMapping.isDecimal(c.dataType) => Left(s"$what over ${c.dataType.simpleString} not supported")
      case c if !arithmeticTypes.contains(c.vecType) => Left(s"$what over ${c.dataType.simpleString} not supported")
      case c => Right(c)
    }

  /**
   * `%`, `pmod` and `div`: same Spark type on both sides (Spark's coercion has cast them), numeric
   * lanes only, `try_*` mode falls back. The kernel handles a zero divisor; ANSI raises it.
   */
  private def divideLike(
      kind: DivideLikeExpr.Kind,
      l: Expression,
      r: Expression,
      resultType: DataType,
      mode: EvalMode.Value,
      e: Expression,
      input: Seq[Attribute]
  ): Result = {
    val what = kind match {
      case DivideLikeExpr.Rem => "%"; case DivideLikeExpr.Pmod => "pmod"; case DivideLikeExpr.Div => "div"
    }
    // try_mod is Remainder in TRY mode: a zero divisor is null, exactly the legacy path.
    if (mode == EvalMode.TRY && kind != DivideLikeExpr.Rem) Left(s"try_* $what not supported")
    else if (l.dataType != r.dataType)
      Left(s"$what operands differ: ${l.dataType.simpleString} vs ${r.dataType.simpleString}")
    else if (TypeMapping.isDecimal(l.dataType)) Left(s"$what over ${l.dataType.simpleString} not supported")
    else if (kind == DivideLikeExpr.Div && TypeMapping.vecTypeOf(l.dataType) == VecType.FLOAT64)
      Left("div over double not supported")
    // A float remainder is exact on the widened doubles; pmod adds the divisor in float arithmetic first (#712).
    else if (kind == DivideLikeExpr.Pmod && l.dataType == FloatType) Left("pmod over float not supported")
    else
      for {
        le <- compile(l, input)
        re <- compile(r, input)
        _ <- if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("arithmetic on two literals")
        else Right(())
        _ <- if (!arithmeticTypes.contains(le.vecType)) Left(s"$what over ${l.dataType.simpleString} not supported")
        else Right(())
      } yield DivideLikeExpr(kind, le, re, resultType, mode == EvalMode.ANSI, e.origin.context)
  }

  /** `greatest` / `least`: every child the same Spark type on a numeric lane, at least one non-literal. */
  private def pick(
      pick: MathKernels.Pick,
      children: Seq[Expression],
      resultType: DataType,
      input: Seq[Attribute]
  ): Result = {
    val what = if (pick == MathKernels.Pick.GREATEST) "greatest" else "least"
    if (children.size < 2) Left(s"$what with fewer than two arguments")
    else if (children.exists(_.dataType != resultType))
      Left(s"$what operands differ: ${children.map(_.dataType.simpleString).distinct.mkString("/")}")
    else if (
      TypeMapping.isDecimal(resultType) || !TypeMapping.isSupported(resultType) || !arithmeticTypes.contains(
        TypeMapping.vecTypeOf(resultType)
      )
    ) Left(s"$what over ${resultType.simpleString} not supported")
    else {
      val compiled = children.map {
        case Literal(null, _) => Left("null literal")
        case c => compile(c, input)
      }
      compiled.collectFirst { case Left(reason) => reason } match {
        case Some(reason) => Left(reason)
        case None =>
          val exprs = compiled.collect { case Right(c) => c }
          if (exprs.forall(_.isInstanceOf[LiteralExpr])) Left(s"$what of literals only")
          else Right(PickExpr(pick, exprs, resultType))
      }
    }
  }

  /** `ceil` / `floor`: long is the identity, double goes to long, decimal to Spark's `bounded(p - s + 1, 0)`. */
  private def ceilFloor(child: Expression, ceil: Boolean, resultType: DataType, input: Seq[Attribute]): Result = {
    val what = if (ceil) "ceil" else "floor"
    child.dataType match {
      case LongType => compile(child, input)
      case DoubleType => numericChild(child, input, what).map(CeilFloorExpr(_, ceil, resultType))
      case d: DecimalType if TypeMapping.isSupported(d) && TypeMapping.isSupported(resultType) =>
        compile(child, input).flatMap {
          case _: LiteralExpr => Left(s"$what of a literal")
          case c => Right(CeilFloorExpr(c, ceil, resultType))
        }
      case d: DecimalType => Left(s"$what over ${d.simpleString} -> ${resultType.simpleString} not supported")
      case t => Left(s"$what over ${t.simpleString} not supported")
    }
  }

  /**
   * `round` / `bround` / two-argument `ceil` / `floor`: the scale must be an int literal (Spark
   * requires it foldable), the child a numeric or decimal lane, and the result -- which for decimals
   * Spark widens by a digit -- must still fit 18 digits.
   */
  private def rounding(
      mode: RoundKernels.Mode,
      what: String,
      child: Expression,
      scale: Expression,
      resultType: DataType,
      ansi: Boolean,
      e: Expression,
      input: Seq[Attribute]
  ): Result =
    scale match {
      // round (half up) of a wide decimal to a non-negative scale is Spark's `toPrecision(p, k, HALF_UP)`:
      // the decimal-to-decimal cast's rescale, which never overflows since the result type keeps every
      // integer digit (#326). bround (half even) and negative scales stay refused.
      case Literal(k: Int, IntegerType)
          if isWideDecimal(child.dataType) && mode == RoundKernels.Mode.HALF_UP && k >= 0 && TypeMapping.hasLane(
            resultType
          ) =>
        wideOperand(child, input).flatMap {
          case _: LiteralExpr => Left(s"$what of a literal")
          case c => Right(WideDecimalCastExpr(c, child.dataType, resultType, ansi = false, e.origin.context))
        }
      case Literal(k: Int, IntegerType) =>
        if (!RoundExpr.supports(child.dataType, resultType))
          Left(s"$what over ${child.dataType.simpleString} -> ${resultType.simpleString} not supported")
        else
          compile(child, input).flatMap {
            case _: LiteralExpr => Left(s"$what of a literal")
            // An integer rounded to a non-negative scale is itself.
            case c if k >= 0 && !TypeMapping.isDecimal(c.dataType) && c.vecType != VecType.FLOAT64 => Right(c)
            case c => Right(RoundExpr(c, resultType, mode, k, ansi, e.origin.context))
          }
      case Literal(null, _) => Left(s"$what with a null scale")
      case other => Left(s"$what with a non-literal scale ${other.sql}")
    }

  /** A compiled non-literal INT32 / INT64 operand (bytes, shorts and booleans have no lane here). */
  private def integralChild(e: Expression, input: Seq[Attribute], what: String): Result =
    if (e.dataType != IntegerType && e.dataType != LongType)
      Left(s"$what over ${e.dataType.simpleString} not supported")
    else
      compile(e, input).flatMap {
        case _: LiteralExpr => Left(s"$what of a literal")
        case c => Right(c)
      }

  /**
   * `& | ^` (same integral type on both sides after Spark's coercion) and the shifts (an INT32 amount
   * on the right); a literal may sit on either side but not both.
   */
  private def bitwise(
      op: BitKernels.BitOp,
      what: String,
      l: Expression,
      r: Expression,
      resultType: DataType,
      input: Seq[Attribute]
  ): Result = {
    val leftOk = l.dataType == IntegerType || l.dataType == LongType
    val rightOk = if (op.isShift) r.dataType == IntegerType else r.dataType == l.dataType
    if (!leftOk || !rightOk) Left(s"$what over ${l.dataType.simpleString}, ${r.dataType.simpleString} not supported")
    else
      for {
        le <- compile(l, input)
        re <- compile(r, input)
        _ <- if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left(s"$what of two literals")
        else Right(())
      } yield BitBinaryExpr(op, le, re, resultType)
  }

  /** A compiled non-literal date operand. */
  /** The casts of #43 (the widening kernel and the decimal path keep their own cases). */
  private def sliceOneCast(c: Cast): Boolean = (c.child.dataType, c.dataType) match {
    case (IntegerType | LongType | DoubleType | ShortType | ByteType, ByteType | ShortType) => true // #327
    case (ByteType | ShortType, IntegerType) => true
    case (LongType, IntegerType) | (DoubleType, IntegerType) | (DoubleType, LongType) => true
    case (IntegerType | LongType | DoubleType, BooleanType) => true
    case (BooleanType, IntegerType | LongType | DoubleType) => true
    case (StringType, BooleanType) => true
    case (DateType, TimestampType) => true
    case (from, StringType) if CastExprs.stringable(from) => true
    case (StringType, IntegerType | LongType | DoubleType) => true
    case (DateType | TimestampType, StringType) => true
    case (StringType, DateType | TimestampType) => true
    case _ => false
  }

  private def dateChild(e: Expression, input: Seq[Attribute]): Result =
    if (e.dataType != DateType) Left(s"date function over ${e.dataType.simpleString}")
    else compile(e, input).flatMap {
      case _: LiteralExpr => Left("date function on a literal")
      case c => Right(c)
    }

  private def intLane(e: Expression, input: Seq[Attribute], what: String): Result =
    if (e.dataType != IntegerType) Left(s"$what argument ${e.dataType.simpleString} is not an int")
    else compile(e, input)

  private def integralLane(e: Expression, input: Seq[Attribute], what: String): Result =
    if (e.dataType != IntegerType && e.dataType != LongType)
      Left(s"$what over ${e.dataType.simpleString} not supported")
    else compile(e, input).flatMap { case _: LiteralExpr => Left(s"$what of a literal"); case c => Right(c) }

  private def timestampLane(e: Expression, input: Seq[Attribute], what: String): Result =
    if (e.dataType != TimestampType) Left(s"$what over ${e.dataType.simpleString} not supported")
    else compile(e, input).flatMap { case _: LiteralExpr => Left(s"$what of a literal"); case c => Right(c) }

  /** `date_format` over a timestamp lane (any zone -- Spark's formatter does the work) or a date lane (fixed offset). */
  private def formatInstant(
      e: Expression,
      pattern: String,
      tz: Option[String],
      input: Seq[Attribute],
      what: String
  ): Result = tz match {
    case None => Left(s"$what without a session zone")
    case Some(zone) =>
      instantLane(e, input, what).flatMap {
        case (_: LiteralExpr, _) => Left(s"$what of a literal")
        case (c, false) => Right(FormatInstantExpr(c, childIsDate = false, secondsIn = false, pattern, zone, 0L))
        case (c, true) =>
          DateExprs.fixedOffsetMicros(tz) match {
            case None => Left(s"$what over a date needs a fixed-offset session zone, not $zone")
            case Some(offset) =>
              Right(FormatInstantExpr(c, childIsDate = true, secondsIn = false, pattern, zone, offset))
          }
      }
  }

  /** `unix_timestamp` / `to_unix_timestamp` over a timestamp or date lane; parsing a string is not ours. */
  private def unixTimestamp(e: Expression, tz: Option[String], input: Seq[Attribute], what: String): Result =
    if (e.dataType.isInstanceOf[StringType]) Left(s"$what parsing a string not supported")
    else
      instantLane(e, input, what).flatMap {
        case (_: LiteralExpr, _) => Left(s"$what of a literal")
        case (c, false) => Right(UnixTimestampExpr(c, childIsDate = false, 0L))
        case (c, true) =>
          DateExprs.fixedOffsetMicros(tz) match {
            case None => Left(s"$what over a date needs a fixed-offset session zone, not ${tz.getOrElse("none")}")
            case Some(offset) => Right(UnixTimestampExpr(c, childIsDate = true, offset))
          }
      }

  /** A timestamp lane, or a date lane behind Spark's date -> timestamp cast (the flag says which). */
  private def instantLane(e: Expression, input: Seq[Attribute], what: String): Either[String, (VectorExpr, Boolean)] =
    e match {
      case Cast(child, TimestampType, _, _) if child.dataType == DateType => compile(child, input).map(c => (c, true))
      case _ if e.dataType == TimestampType => compile(e, input).map(c => (c, false))
      case _ if e.dataType == DateType => compile(e, input).map(c => (c, true))
      case _ => Left(s"$what over ${e.dataType.simpleString} not supported")
    }

  private def dateField(field: DateKernels.Field, child: Expression, input: Seq[Attribute]): Result =
    dateChild(child, input).map(DateFieldExpr(field, _))

  /** `date_add` / `date_sub` take a date and an int; `datediff` two dates. Both are INT32 lanes. */
  private def dateArith(
      op: ArithOp,
      l: Expression,
      r: Expression,
      resultType: DataType,
      input: Seq[Attribute]
  ): Result =
    if (l.dataType != DateType) Left(s"date arithmetic over ${l.dataType.simpleString}")
    else if (r.dataType != IntegerType && r.dataType != DateType)
      Left(s"date arithmetic with ${r.dataType.simpleString} days")
    else
      for {
        le <- compile(l, input)
        re <- compile(r, input)
        _ <- if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("arithmetic on two literals")
        else Right(())
      } yield ArithExpr(op, le, re, resultType, ansiDivideByZero = false, queryContext = null)

  private def timeField(
      field: DateKernels.TimeField,
      child: Expression,
      zoneId: Option[String],
      input: Seq[Attribute]
  ): Result =
    if (child.dataType != TimestampType) Left(s"time field over ${child.dataType.simpleString}")
    else DateExprs.fixedOffsetMicros(zoneId) match {
      case None => Left(s"time field needs a fixed-offset session zone, not ${zoneId.getOrElse("none")}")
      case Some(offset) =>
        compile(child, input).flatMap {
          case _: LiteralExpr => Left("time field on a literal")
          case c => Right(TimeFieldExpr(field, c, offset))
        }
    }

  private val arithmeticTypes: Set[VecType] = Set(VecType.INT32, VecType.INT64, VecType.FLOAT64)

  /**
   * Spark 4 defaults to ANSI mode. Double arithmetic is identical in both modes except that
   * division by zero raises instead of yielding null, which the kernel wrapper handles. Integer
   * arithmetic in ANSI mode is computed wrapping and checked afterwards with an overflow lane mask
   * (`OverflowKernels`); the error is raised only if an active row overflowed.
   */
  private def arithmetic(
      op: ArithOp,
      l: Expression,
      r: Expression,
      mode: EvalMode.Value,
      e: Expression,
      input: Seq[Attribute]
  ): Result =
    if (TypeMapping.isDecimal(l.dataType) || TypeMapping.isDecimal(r.dataType))
      decimalArithmetic(op, l, r, mode, e, input)
    else for {
      le <- compile(l, input)
      re <- compile(r, input)
      _ <- checkArithmetic(op, le, re, l, r, mode)
    } yield e.dataType match {
      // float arithmetic (#712): computed on the widened doubles, then rounded to single precision -- for
      // + - * a double holds more than 2p + 2 bits, so one rounding of the exact double result is IEEE's.
      case FloatType =>
        RoundFloatExpr(ArithExpr(op, le, re, DoubleType, ansiDivideByZero = false, e.origin.context))
      // tinyint / smallint arithmetic (#327): exact on the int lane (two bytes or shorts never overflow an
      // int), then narrowed into the declared type -- Spark wraps in legacy mode, raises in ANSI, nulls in try.
      case ByteType | ShortType =>
        NarrowIntExpr(
          ArithExpr(op, le, re, IntegerType, ansiDivideByZero = false, e.origin.context),
          e.dataType,
          mode == EvalMode.ANSI,
          e.origin.context,
          nullOnOverflow = mode == EvalMode.TRY,
          arithmetic = true
        )
      case _ => ArithExpr(
          op,
          le,
          re,
          e.dataType,
          mode == EvalMode.ANSI,
          e.origin.context,
          nullOnOverflow = mode == EvalMode.TRY
        )
    }

  private def checkArithmetic(
      op: ArithOp,
      le: VectorExpr,
      re: VectorExpr,
      l: Expression,
      r: Expression,
      mode: EvalMode.Value
  ): Either[String, Unit] = {
    if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("arithmetic on two literals")
    else if (l.dataType != r.dataType)
      Left(s"arithmetic operands differ: ${l.dataType.simpleString} vs ${r.dataType.simpleString}")
    else if (!arithmeticTypes.contains(le.vecType)) Left(s"arithmetic not supported for ${l.dataType.simpleString}")
    else if (op == ArithOp.DIV && le.vecType != VecType.FLOAT64)
      Left(s"division not supported for ${l.dataType.simpleString}")
    else Right(())
  }

  /**
   * The speculative narrow form of decimal arithmetic whose declared result is wider than 18 digits
   * (#26): a `Multiply` with both operands at most 18 digits (or speculative themselves) and the
   * declared scale exactly `s1 + s2` (so the declared precision is at most 38 and Spark never rounds
   * the product), or an `Add` / `Subtract` whose declared scale is the common scale `max(s1, s2)` (so
   * Spark only range-checks the exact sum); not `try_*`. `None` when the expression is not that
   * shape; only the wide decimal sum and average ask, because only they can take the escalated rows
   * exactly.
   */
  def speculativeDecimalArithmetic(e: Expression, input: Seq[Attribute]): Option[Result] = {
    // An operand is a lane (at most 18 digits) or, recursively, a speculative expression itself.
    def operand(o: Expression): Result =
      if (TypeMapping.isSupported(o.dataType)) compile(o, input)
      else speculativeDecimalArithmetic(o, input).getOrElse(Left(
        s"decimal operand ${o.dataType.simpleString} of ${o.sql} is neither a lane nor a speculative product or sum"
      ))
    def operands(l: Expression, r: Expression): Either[String, (VectorExpr, VectorExpr)] =
      for {
        le <- operand(l)
        re <- operand(r)
        _ <- if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("arithmetic on two literals")
        else Right(())
      } yield (le, re)
    // A wide operand that is not itself speculative arithmetic (a wide column, a cast to a wide decimal --
    // CountBelowJoin's `cast(count as decimal(20,0))`, #762) leaves the expression to the 128-bit kernels
    // (#258): None, not a refusal.
    def speculable(o: Expression): Boolean =
      TypeMapping.isSupported(o.dataType) || speculativeDecimalArithmetic(o, input).isDefined
    e match {
      case m @ Multiply(l, r, _) if !speculable(l) || !speculable(r) => None
      case a: BinaryArithmetic
          if (a.isInstanceOf[Add] || a.isInstanceOf[Subtract]) &&
            (!speculable(a.left) || !speculable(a.right)) => None
      case m @ Multiply(l, r, _) =>
        // Spark 4.1's Multiply carries a NumericEvalContext; the mode lives inside it.
        val mode = m.evalContext.evalMode
        (l.dataType, r.dataType, e.dataType) match {
          case (lt: DecimalType, rt: DecimalType, dt: DecimalType)
              if !TypeMapping.isSupported(
                dt
              ) && dt.scale == lt.scale + rt.scale && dt.precision <= DecimalType.MAX_PRECISION && mode != EvalMode.TRY =>
            Some(operands(l, r).map { case (le, re) =>
              SpeculativeDecimalMulExpr(le, re, lt, rt, dt, mode == EvalMode.ANSI, e.origin.context)
            })
          case _ => None
        }
      case a: BinaryArithmetic if a.isInstanceOf[Add] || a.isInstanceOf[Subtract] =>
        val mode = a.evalContext.evalMode
        (a.left.dataType, a.right.dataType, e.dataType) match {
          case (lt: DecimalType, rt: DecimalType, dt: DecimalType)
              if !TypeMapping.isSupported(dt) && dt.scale == math.max(lt.scale, rt.scale) && mode != EvalMode.TRY =>
            Some(operands(a.left, a.right).map { case (le, re) =>
              SpeculativeDecimalAddExpr(
                le,
                re,
                a.isInstanceOf[Subtract],
                lt,
                rt,
                dt,
                mode == EvalMode.ANSI,
                e.origin.context
              )
            })
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Decimal arithmetic on unscaled long lanes. Spark computes the result type from the operand
   * types (`max(p1-s1, p2-s2) + max(s1, s2) + 1` digits for `+`/`-`, `p1+p2+1` for `*`), which
   * leaves room for every result: only division needs an overflow check. Results wider than 18
   * digits have no lane representation and fall back.
   */
  private def decimalArithmetic(
      op: ArithOp,
      l: Expression,
      r: Expression,
      mode: EvalMode.Value,
      e: Expression,
      input: Seq[Attribute]
  ): Result = (l.dataType, r.dataType, e.dataType) match {
    case (lt: DecimalType, rt: DecimalType, dt: DecimalType)
        if isWideDecimal(lt) || isWideDecimal(rt) || isWideDecimal(dt) =>
      // A wide operand or result (#258): the DECIMAL128 kernels, exact with Spark's toPrecision. The
      // operands are lanes (wide or narrow) or literals; a wide operand that is itself an expression
      // compiles recursively through this same path.
      if (mode == EvalMode.TRY) Left("try_* arithmetic not supported")
      else if (!TypeMapping.hasLane(lt) || !TypeMapping.hasLane(rt) || !TypeMapping.hasLane(dt))
        Left(s"decimal wider than ${DecimalType.MAX_PRECISION} digits in ${e.sql}")
      else for {
        le <- wideArithmeticOperand(l, input)
        re <- wideArithmeticOperand(r, input)
        _ <- if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("arithmetic on two literals")
        else Right(())
      } yield WideDecimalArithExpr(op, le, re, lt, rt, dt, mode == EvalMode.ANSI, e.origin.context)
    case (lt: DecimalType, rt: DecimalType, dt: DecimalType) =>
      if (!TypeMapping.isSupported(dt))
        Left(s"decimal result ${dt.simpleString} exceeds ${TypeMapping.MAX_DECIMAL_PRECISION} digits")
      else if (!TypeMapping.isSupported(lt) || !TypeMapping.isSupported(rt))
        Left(s"decimal operand wider than ${TypeMapping.MAX_DECIMAL_PRECISION} digits")
      else if (mode == EvalMode.TRY) Left("try_* arithmetic not supported")
      else {
        val shapeOk = op match {
          case ArithOp.ADD | ArithOp.SUB => dt.scale == math.max(lt.scale, rt.scale)
          case ArithOp.MUL => dt.scale == lt.scale + rt.scale
          case ArithOp.DIV => rt.scale + dt.scale - lt.scale >= 0
        }
        if (!shapeOk) Left(s"unexpected decimal result scale for ${e.sql}")
        else for {
          le <- compile(l, input)
          re <- compile(r, input)
          _ <- if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("arithmetic on two literals")
          else Right(())
        } yield DecimalArithExpr(op, le, re, lt, rt, dt, mode == EvalMode.ANSI, e.origin.context)
      }
    case _ => Left(s"mixed decimal and non-decimal arithmetic: ${e.sql}")
  }

  private def decimalCast(c: Cast, input: Seq[Attribute]): Result = {
    val from = c.child.dataType
    val to = c.dataType
    if (isWideDecimal(from) || isWideDecimal(to)) wideDecimalCast(c, input)
    else {
      val supportedPair = (from, to) match {
        case (_: DecimalType, _: DecimalType) => true
        case (IntegerType | LongType | DoubleType, _: DecimalType) => true
        case (_: DecimalType, DoubleType | LongType | IntegerType) => true
        case _ => false
      }
      if (!TypeMapping.isSupported(from) || !TypeMapping.isSupported(to))
        Left(s"unsupported cast ${from.simpleString} -> ${to.simpleString}")
      else if (!supportedPair) Left(s"unsupported cast ${from.simpleString} -> ${to.simpleString}")
      else if (c.evalMode == EvalMode.TRY) Left("try_cast not supported")
      else compile(c.child, input).flatMap {
        case _: LiteralExpr => Left("cast of a literal")
        case child => Right(DecimalCastExpr(child, from, to, c.evalMode == EvalMode.ANSI, c.origin.context))
      }
    }
  }

  /**
   * A cast with a wide decimal on either side (#258): decimal to decimal at any width, int / long /
   * date / double to a wide decimal, a wide decimal to double / long / int / string. A string source
   * is refused (Spark's `Decimal.fromString` rules are the next step), as is `try_cast`.
   */
  private def wideDecimalCast(c: Cast, input: Seq[Attribute]): Result = {
    val from = c.child.dataType
    val to = c.dataType
    val supportedPair = (from, to) match {
      case (_: DecimalType, _: DecimalType) => TypeMapping.hasLane(from) && TypeMapping.hasLane(to)
      case (IntegerType | LongType | DateType | DoubleType, t: DecimalType) => TypeMapping.hasLane(t)
      case (f: DecimalType, DoubleType | LongType | IntegerType | StringType) => TypeMapping.hasLane(f)
      case _ => false
    }
    if (!supportedPair) Left(s"unsupported cast ${from.simpleString} -> ${to.simpleString}")
    else if (c.evalMode == EvalMode.TRY) Left("try_cast not supported")
    else operand(c.child, input).flatMap {
      case _: LiteralExpr => Left("cast of a literal")
      case child => Right(WideDecimalCastExpr(child, from, to, c.evalMode == EvalMode.ANSI, c.origin.context))
    }
  }

  private def conditional(
      branches: Seq[(Expression, Expression)],
      elseValue: Option[Expression],
      e: Expression,
      input: Seq[Attribute]
  ): Result = {
    // A wide decimal result (#326) is the DECIMAL128 lane: its branches are wide columns, wide
    // arithmetic or wide literals, blended limb by limb; anything else the wide path refuses itself.
    if (!TypeMapping.isSupported(e.dataType) && !isWideDecimal(e.dataType))
      Left(s"unsupported result type ${e.dataType.simpleString} for ${e.sql}")
    else {
      def value(v: Expression): Either[String, Option[VectorExpr]] = v match {
        case Literal(null, _) => Right(None)
        case Literal(x, dt) if dt == e.dataType && CaseWhenExpr.isBranchLiteralType(dt) =>
          Right(Some(LiteralExpr(x, dt)))
        case other if other.dataType != e.dataType =>
          Left(s"branch type ${other.dataType.simpleString} differs from ${e.dataType.simpleString}")
        case other => operand(other, input).map(Some(_))
      }
      val compiled = branches.foldLeft[Either[String, Vector[(VectorExpr, Option[VectorExpr])]]](Right(Vector.empty)) {
        case (acc, (cond, v)) =>
          for {
            done <- acc
            c <- booleanChild(cond, input)
            bv <- value(v)
          } yield done :+ ((c, bv))
      }
      for {
        bs <- compiled
        rest <- elseValue match {
          case Some(x) => value(x).map(Some(_))
          case None => Right(None)
        }
      } yield CaseWhenExpr(bs, rest, e.dataType)
    }
  }

  /** A filter condition must produce a non-literal boolean column. */
  def compilePredicate(expr: Expression, input: Seq[Attribute]): Result =
    compile(expr, input).flatMap {
      case _: LiteralExpr => Left("literal predicate")
      case e if e.dataType != BooleanType => Left(s"predicate is not boolean: ${e.dataType.simpleString}")
      case e => Right(e)
    }

  private def comparison(op: CompareOp, l: Expression, r: Expression, input: Seq[Attribute]): Result =
    if (l.dataType == BooleanType && r.dataType == BooleanType) booleanComparison(op, l, r, input)
    else
      // Spark casts both sides of a decimal comparison to one type, so a wide comparison is "same
      // lane, compare": bare wide columns or wide literals, checked like any other pair.
      for {
        le <- operand(l, input)
        re <- operand(r, input)
        _ <- check(le, re, l, r)
      } yield CompareExpr(op, le, re)

  /** Comparisons between BOOL columns, or a BOOL column and a boolean literal, on the packed words. */
  private def booleanComparison(op: CompareOp, l: Expression, r: Expression, input: Seq[Attribute]): Result =
    (l, r) match {
      case (Literal(null, _), _) | (_, Literal(null, _)) => Left("comparison against NULL")
      case (_: Literal, _: Literal) => Left("comparison of two literals")
      case (Literal(v: Boolean, _), c) => booleanChild(c, input).map(BoolCompareScalarExpr(op.flip(), _, v))
      case (c, Literal(v: Boolean, _)) => booleanChild(c, input).map(BoolCompareScalarExpr(op, _, v))
      case (a, b) => binaryBoolean(a, b, input)(BoolCompareExpr(op, _, _))
    }

  /** `a <=> b`: same type on both sides, never null; a `NULL` literal side is `IS NULL` of the other. */
  private def nullSafeEquality(l: Expression, r: Expression, input: Seq[Attribute]): Result =
    (l, r) match {
      case (Literal(null, _), Literal(null, _)) => Left("<=> of two literals")
      case (Literal(null, _), c) => nullTestChild(c, input).map(IsNullExpr.apply)
      case (c, Literal(null, _)) => nullTestChild(c, input).map(IsNullExpr.apply)
      case _ if l.dataType == BooleanType && r.dataType == BooleanType =>
        if (l.isInstanceOf[Literal] || r.isInstanceOf[Literal]) Left("<=> of a boolean literal")
        else binaryBoolean(l, r, input)(NullSafeEqExpr(_, _))
      case _ =>
        for {
          le <- if (TypeMapping.isCollated(l.dataType)) operand(l, input) else compile(l, input)
          re <- if (TypeMapping.isCollated(r.dataType)) operand(r, input) else compile(r, input)
          _ <- check(le, re, l, r)
        } yield NullSafeEqExpr(le, re)
    }

  /**
   * `value IN set` for the optimizer's `InSet`: numeric lanes take one binary search per row over
   * the sorted keys; strings go through the same per-literal path as `IN`. A set holding `NULL`
   * falls back (Spark's result is then null for non-members).
   */
  private def inSet(value: Expression, hset: Set[Any], input: Seq[Attribute]): Result = {
    if (hset.isEmpty) Left("empty IN set")
    else if (hset.contains(null)) Left("NULL in IN set")
    else if (CollationKeyExpr.collationOf(value.dataType) >= 0) {
      // A collated value (#705): the set's elements as their collation keys.
      if (!hset.forall(_.isInstanceOf[UTF8String])) Left("IN set elements are not strings")
      else for {
        c <- collatedOperandKey(value, input)
        lits <- hset.toSeq.foldRight[Either[String, List[LiteralExpr]]](Right(Nil)) { (v, acc) =>
          for (rest <- acc; k <- collatedOperandKey(Literal(v, value.dataType), input))
            yield k.asInstanceOf[LiteralExpr] :: rest
        }
      } yield stringIn(c, lits)
    } else compile(value, input).flatMap {
      case _: LiteralExpr => Left("IN over a literal")
      case c if c.vecType == VecType.UTF8 =>
        if (!hset.forall(_.isInstanceOf[UTF8String])) Left("IN set elements are not strings")
        else Right(stringIn(c, hset.toSeq.map(v => LiteralExpr(v, value.dataType))))
      case c if c.vecType == VecType.FLOAT64 =>
        if (!hset.forall(_.isInstanceOf[Double])) Left("IN set elements are not doubles")
        else Right(InSetExpr(c, PredicateKernels.doubleKeys(hset.toArray.map(_.asInstanceOf[Double]))))
      case c if c.vecType == VecType.INT32 || c.vecType == VecType.INT64 =>
        val keys = hset.toArray.map(LiteralExpr(_, value.dataType).number.longValue())
        java.util.Arrays.sort(keys)
        Right(InSetExpr(c, keys))
      case _ => Left(s"IN not supported for ${value.dataType.simpleString}")
    }
  }

  /**
   * `value IN (list)` where every element is a non-null literal of the value's type (Spark's own
   * type coercion has already folded any casts into the literals). A list with a `NULL`, a
   * non-literal element or a mismatched type falls back; so does `InSet`, the optimizer's rewrite
   * above `spark.sql.optimizer.inSetConversionThreshold` literals (#48).
   */
  private def inList(value: Expression, list: Seq[Expression], input: Seq[Attribute]): Result = {
    if (list.isEmpty) Left("empty IN list")
    else if (list.exists { case Literal(null, _) => true; case _ => false }) Left("NULL in IN list")
    else if (!list.forall(_.isInstanceOf[Literal])) Left("IN list is not all literals")
    else if (list.exists(_.dataType != value.dataType)) Left(
      s"IN operands differ: ${value.dataType.simpleString} vs ${list.map(_.dataType.simpleString).distinct.mkString("/")}"
    )
    else operand(value, input).flatMap {
      case _: LiteralExpr => Left("IN over a literal")
      case c if !comparableTypes.contains(c.vecType) => Left(s"IN not supported for ${value.dataType.simpleString}")
      case c =>
        val lits = list.map(operand(_, input))
        lits.collectFirst { case Left(reason) => reason } match {
          case Some(reason) => Left(reason)
          case None => Right(stringIn(c, lits.collect { case Right(lit: LiteralExpr) => lit }))
        }
    }
  }

  /**
   * `startswith` / `endswith` / `contains` of a string column against a non-null string literal.
   * A column pattern, a non-string operand or a `NULL` pattern falls back; `LIKE` with inner
   * wildcards never reaches here (the optimizer leaves it as `Like`, #3 follow-up).
   */
  /**
   * `IN` over literals: a string column with more than a few literals goes through the hash set (#371,
   * one pass whatever the list's size); short lists and the other lanes compare literal by literal.
   */
  private def stringIn(c: VectorExpr, lits: Seq[LiteralExpr]): VectorExpr =
    if (c.vecType == VecType.UTF8 && lits.size > 3) StringInSetExpr(c, lits) else InExpr(c, lits)

  /** The string a slicing function works on: a UTF8 lane (a literal subject folds in Spark). */
  private def stringSubject(e: Expression, input: Seq[Attribute], what: String): Result =
    compile(e, input).flatMap {
      case _: LiteralExpr => Left(s"$what of a literal")
      case c if c.vecType != VecType.UTF8 => Left(s"$what over ${e.dataType.simpleString} not supported")
      case c => Right(c)
    }

  /** A string argument: a UTF8 lane or a non-null string literal. */
  private def stringArg(e: Expression, input: Seq[Attribute], what: String): Result = e match {
    case Literal(null, _) => Left(s"$what with a null argument")
    case _ =>
      compile(e, input).flatMap {
        case lit: LiteralExpr => Right(lit)
        case c if c.vecType != VecType.UTF8 => Left(s"$what argument ${e.dataType.simpleString} not supported")
        case c => Right(c)
      }
  }

  private def caseMap(kind: StringCaseKernels.Kind, child: Expression, input: Seq[Attribute], what: String): Result =
    if (child.dataType != StringType)
      Left(s"$what over ${child.dataType.sql.toLowerCase} not supported (a collated string follows ICU rules)")
    else stringSubject(
      child,
      input,
      what
    ).map(CaseMapExpr(kind, _, SQLConf.get.getConf(SQLConf.ICU_CASE_MAPPINGS_ENABLED)))

  /** A trim with no trim string, or a literal one taken as a set of code points; a column trim set falls back. */
  private def trim(
      side: StringCaseKernels.Side,
      src: Expression,
      trimStr: Option[Expression],
      input: Seq[Attribute],
      what: String
  ): Result =
    if (src.dataType != StringType) Left(s"$what over ${src.dataType.sql.toLowerCase} not supported")
    else trimStr match {
      case None => stringSubject(src, input, what).map(TrimExpr(side, _, None))
      case Some(Literal(t: UTF8String, StringType)) =>
        stringSubject(src, input, what).map(TrimExpr(side, _, Some(t.toString.codePoints().toArray)))
      case Some(Literal(null, _)) => Left(s"$what with a null trim string")
      case Some(_) => Left(s"$what with a non-literal trim string not supported")
    }

  /** Spark's `string -> binary` cast, a reinterpretation of the bytes: the UTF8 lane underneath serves as the binary. */
  private object BinaryFromString {
    def unapply(e: Expression): Option[Expression] = e match {
      case Cast(child, BinaryType, _, _) if child.dataType == StringType => Some(child)
      case _ => None
    }
  }

  /** Several string arguments, each a UTF8 lane or a non-null literal; at least one must be a lane. */
  private def stringArgs(es: Seq[Expression], input: Seq[Attribute], what: String): Either[String, Seq[VectorExpr]] = {
    val compiled = es.foldLeft[Either[String, Vector[VectorExpr]]](Right(Vector.empty)) { (acc, e) =>
      for (done <- acc; c <- stringArg(e, input, what)) yield done :+ c
    }
    compiled.flatMap { ps =>
      if (ps.forall(_.isInstanceOf[LiteralExpr])) Left(s"$what of literals only") else Right(ps)
    }
  }

  /** An int argument: an INT32 lane or an int literal, the literal bounded when it sizes the output. */
  private def intArg(e: Expression, input: Seq[Attribute], what: String, maxLiteral: Int = Int.MaxValue): Result =
    e match {
      case Literal(null, _) => Left(s"$what with a null argument")
      case Literal(v: Int, IntegerType) if v > maxLiteral => Left(s"$what count $v exceeds the batch output cap")
      case _ if e.dataType != IntegerType => Left(s"$what argument ${e.dataType.simpleString} is not an int")
      case _ => compile(e, input)
    }

  /**
   * `LIKE` with several `%` wildcards -- the shape `LikeSimplification` leaves alone -- as a
   * multi-token matcher (#264): the pattern is split on `%` into a prefix, tokens found in order,
   * and a suffix. `_`, escape characters and collated strings stay with Spark.
   */
  private def likeTokens(like: Like, input: Seq[Attribute]): Result = like.right match {
    case Literal(null, _) => Left("null LIKE pattern")
    case Literal(p: UTF8String, StringType) =>
      val pattern = p.toString
      if (pattern.indexOf('_') >= 0) Left("LIKE pattern with a `_` wildcard not supported")
      else if (pattern.indexOf(like.escapeChar) >= 0) Left("LIKE pattern with an escape character not supported")
      else if (pattern.indexOf('%') < 0) Left("LIKE pattern without a wildcard (Spark folds it to an equality)")
      else compile(like.left, input).flatMap {
        case _: LiteralExpr => Left("LIKE on a literal")
        case c if c.vecType != VecType.UTF8 => Left(s"LIKE over ${like.left.dataType.simpleString} not supported")
        case c =>
          val parts = pattern.split("%", -1)
          val prefix = parts.head.getBytes(java.nio.charset.StandardCharsets.UTF_8)
          val suffix = parts.last.getBytes(java.nio.charset.StandardCharsets.UTF_8)
          val tokens =
            parts.slice(1, parts.length - 1).filter(_.nonEmpty).map(_.getBytes(java.nio.charset.StandardCharsets.UTF_8))
          Right(LikeTokensExpr(c, prefix, tokens, suffix))
      }
    case _: Literal => Left(s"LIKE pattern of type ${like.right.dataType.simpleString}")
    case _ => Left("LIKE pattern is not a literal")
  }

  private def stringMatch(kind: StringMatchKernels.Kind, l: Expression, r: Expression, input: Seq[Attribute]): Result =
    r match {
      case Literal(null, _) => Left("null pattern")
      case Literal(_, StringType) =>
        for {
          c <- compile(l, input)
          _ <- c match {
            case _: LiteralExpr => Left("string match on a literal")
            case c if c.vecType != VecType.UTF8 => Left(s"string match not supported for ${l.dataType.simpleString}")
            case _ => Right(())
          }
          p <- compile(r, input)
        } yield StringMatchExpr(kind, c, p.asInstanceOf[LiteralExpr])
      case _: Literal => Left(s"string pattern of type ${r.dataType.simpleString}")
      case _ => Left("string pattern is not a literal")
    }

  private def check(le: VectorExpr, re: VectorExpr, l: Expression, r: Expression): Either[String, Unit] = {
    if (le.isInstanceOf[LiteralExpr] && re.isInstanceOf[LiteralExpr]) Left("comparison of two literals")
    else if (l.dataType != r.dataType)
      Left(s"comparison operands differ: ${l.dataType.simpleString} vs ${r.dataType.simpleString}")
    else if (!comparableTypes.contains(le.vecType)) Left(s"comparison not supported for ${l.dataType.simpleString}")
    else Right(())
  }

  private def binaryBoolean(l: Expression, r: Expression, input: Seq[Attribute])(
      make: (VectorExpr, VectorExpr) => VectorExpr
  ): Result =
    for {
      le <- booleanChild(l, input)
      re <- booleanChild(r, input)
    } yield make(le, re)

  private def booleanChild(e: Expression, input: Seq[Attribute]): Result =
    compile(e, input).flatMap {
      case _: LiteralExpr => Left("boolean literal operand")
      case v if v.dataType != BooleanType => Left(s"expected boolean, got ${v.dataType.simpleString}")
      case v => Right(v)
    }

  private def nullTestChild(e: Expression, input: Seq[Attribute]): Result =
    if (e.isInstanceOf[AttributeReference] && TypeMapping.isCollated(e.dataType) && TypeMapping.hasLane(e.dataType))
      compileMovedColumn(e, input) // a collated string column (#705): its validity is its lane's
    else if (!TypeMapping.isSupported(e.dataType) && nestedColumnPath(e, input).isRight)
      nestedColumnPath(e, input).map { case (ordinal, path) => NestedValidityExpr(ordinal, path) }
    else compile(e, input).flatMap {
      case _: LiteralExpr => Left("null test on literal")
      case v => Right(v)
    }
}
