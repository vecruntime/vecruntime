/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
package org.apache.spark.sql.vecruntime

import io.vecruntime.kernels.{Bitmap, GroupAssignment, VectorBuffers}
import io.vecruntime.spark.adapter.TypeMapping
import io.vecruntime.spark.agg.{GroupedAggState, Rows, VectorAggFunction, VectorAggregates}
import io.vecruntime.spark.arrow.{ArrowOutput, BorrowedColumnVector, SelectedColumnarBatch, VectorAllocators}
import io.vecruntime.spark.expr.{ExpressionCompiler, LiteralExpr, VectorExpr}
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Ascending,
  Attribute,
  DenseRank,
  Expression,
  NamedExpression,
  Rank,
  RowNumber,
  SortOrder,
  SpecifiedWindowFrame,
  UnboundedFollowing,
  UnboundedPreceding,
  WindowExpression,
  WindowSpecDefinition
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  Average,
  Complete,
  Count,
  First,
  Last,
  Max,
  Min,
  Sum
}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  CumeDist,
  EmptyRow,
  FrameLessOffsetWindowFunction,
  Literal,
  NTile,
  NthValue,
  PercentRank,
  SpecialFrameBoundary,
  UnboundedFollowing => UnboundedFollowingBound
}
import org.apache.spark.sql.types.{
  ByteType,
  ShortType,
  DateType,
  IntegerType,
  StringType,
  TimestampType,
  BooleanType,
  DecimalType => SparkDecimalType
}
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, EvalMode, RangeFrame, RowFrame}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.{Final, Partial, WindowExec, WindowGroupLimitExec, WindowGroupLimitMode}
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, LongType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.mutable

/**
 * Columnar replacement for WindowExec in two shapes (#58). Both walk the child's rows in the order
 * Spark already established -- the child arrives sorted by the partition keys and then the order keys,
 * because this operator requires exactly what `WindowExec` does -- and detect a partition boundary
 * where a partition key differs from the previous row's (null-safe equality, as Spark's window
 * functions compare with `<=>`).
 *
 * Ranking (layer 1): `row_number`, `rank` and `dense_rank`. A row starts a new peer group when an
 * order key differs from the previous row's; `row_number` counts rows since the partition began,
 * `rank` is the row number at which the current peer group began, `dense_rank` counts peer groups.
 * The previous row's keys and the counters carry across batches, so a partition longer than a batch
 * is one partition. Streaming: every input batch is one output batch, the input columns borrowed,
 * the ranks new INT32 columns.
 *
 * Whole-partition aggregates (layer 2): `sum`, `avg`, `count`, `min`, `max` and the rest of the
 * grouped aggregate family over the frame `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` -- Spark's
 * default frame for a spec without `ORDER BY`, the shape of TPC-DS q12, q20, q47, q53, q57, q63, q89,
 * q98. Each partition is a group: the aggregate functions of the grouped hash aggregate run over the
 * batch with the partition ordinal as the group id, and the partition's value is the function's
 * `evaluateExpression` over its buffers, exactly as the Final aggregate computes it. Rows are held
 * (copied, in memory, no spill -- like the sort) until their partition ends, since the value is
 * known only then, and are emitted batch by batch with the values gathered per row.
 *
 * Running frames (layer 2b): `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` and `RANGE BETWEEN
 * UNBOUNDED PRECEDING AND CURRENT ROW` -- the latter Spark's default for an aggregate over a spec WITH
 * `ORDER BY`, where every row of a peer group gets the value at the end of its group. Each row (ROWS)
 * or peer group (RANGE) is a group; the same grouped states accumulate them, and at emission a group's
 * value is its partition's prefix: the previous group's running buffers combined with its own, slot by
 * slot -- sums and counts add (a bigint sum with Spark's ANSI overflow), `min`/`max` compare -- so the
 * function's `evaluateExpression` over the prefixed buffers is the running value. `sum`, `avg`,
 * `count`, `min` and `max` have that prefix form; the other functions do not and are refused.
 *
 * The child may be a row operator: Spark plans `Window` above `Sort` above an exchange, and without
 * Comet's shuffle that sort stays Spark's, so `RowToColumnarExec` is inserted below us; from here up
 * the chain (a filter on the rank, a projection, a limit) is columnar again.
 *
 * Offset functions (layer 3): `lag`, `lead`, `first_value`, `last_value` and `nth_value` are one row
 * of the partition each -- a row shifted by a literal offset (the default outside the partition), the
 * partition's first row, the frame's last row (the partition; the current row for `ROWS ... CURRENT
 * ROW`; the end of the peer group for the `RANGE` default), the frame's n-th row -- read from the held
 * copies of the partition's rows, across batch boundaries; `IGNORE NULLS` is refused.
 *
 * The ranking functions that need the partition size -- `percent_rank`, `cume_dist`, `ntile` -- go through
 * the same held-partition path (layer 1b), as do `row_number` / `rank` / `dense_rank` when they share an
 * operator with an offset function.
 *
 * Sliding frames (layer 2c): `sum`, `avg`, `count`, `min`, `max` over `ROWS BETWEEN a AND b` with any
 * literal bounds (`n PRECEDING`/`FOLLOWING`, `CURRENT ROW`, unbounded either side) are re-aggregated per
 * row over the frame's rows in order, as Spark's own sliding frames are -- so double sums are bit-identical
 * -- and a frame starting at the partition is advanced incrementally as its end moves, as Spark's
 * unbounded-preceding frame is. When such an aggregate shares an operator with a running frame, an
 * offset or a ranking function, all of them take this path.
 *
 * `RANGE` frames with value offsets (layer 2d, `RANGE BETWEEN 5 PRECEDING AND CURRENT ROW`, `1 FOLLOWING
 * AND 3 FOLLOWING`, an unbounded side with an offset on the other): the same five aggregates over one
 * integral or date order key, computed per partition by `WindowFrameKernels` once the partition is held --
 * the keys and the input gathered into primitive arrays reused across partitions, the frame of every row
 * found by Spark's two-pointer walk (`rangeBounds`: the bound is the key plus the offset in the key's own
 * arithmetic, compared in the `SortOrder`'s direction and null ordering, so a null key's frame is the null
 * peer group), and the aggregate re-run in row order per frame as Spark's `SlidingWindowFunctionFrame`
 * does (a frame whose lower bound has not moved continues the previous accumulation; counts and unchecked
 * bigint sums slide). The peer-bounded `RANGE` frames without an offset (`CURRENT ROW AND UNBOUNDED
 * FOLLOWING`, `CURRENT ROW AND CURRENT ROW`) take the row path over any key type.
 *
 * `IGNORE NULLS`, decimal aggregates (#28), the other aggregate functions over sliding frames, `RANGE`
 * offsets over decimal, timestamp (interval) and string keys, and `min`/`max` over strings in such a frame
 * are refused with a reason naming the function and frame.
 */
case class VectorWindowExec(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan
) extends VectorExec {

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  /** Same contracts as WindowExec, so the exchange and sort Spark planned below are the right ones. */
  override def requiredChildDistribution: Seq[Distribution] =
    if (partitionSpec.isEmpty) AllTuples :: Nil else ClusteredDistribution(partitionSpec) :: Nil
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def compileKey(e: Expression): VectorExpr = ExpressionCompiler.compileLaneColumn(e, child.output) match {
    case Right(v) => v
    case Left(reason) => throw new IllegalStateException(s"cannot vectorize window key ${e.sql}: $reason")
  }

  // A constant key never separates two rows (`ORDER BY length('abc')` folds to a literal: every row is a
  // peer), so literals are dropped rather than evaluated as columns.
  @transient private lazy val partitionKeys: Array[VectorExpr] =
    partitionSpec.map(compileKey).filterNot(_.isInstanceOf[LiteralExpr]).toArray
  @transient private lazy val orderKeys: Array[VectorExpr] =
    orderSpec.map(o => compileKey(o.child)).filterNot(_.isInstanceOf[LiteralExpr]).toArray

  private def aggregateMode: Boolean = VectorWindowPlanner.aggregateWindows(windowExpression).isDefined
  private def offsetMode: Boolean =
    !aggregateMode && VectorWindowPlanner.offsetWindows(windowExpression, child.output, orderSpec).isRight
  @transient private lazy val offsets: Array[VectorWindowPlanner.OffsetFunction] =
    VectorWindowPlanner.offsetWindows(
      windowExpression,
      child.output,
      orderSpec
    ).getOrElse(throw new IllegalStateException("window expressions are not offset functions")).toArray

  @transient private lazy val kinds: Array[Int] = windowExpression.map(e =>
    VectorWindowPlanner.rankKind(e).getOrElse(
      throw new IllegalStateException(s"cannot vectorize window function ${e.sql}")
    )
  ).toArray

  @transient private lazy val (aggregates: Seq[AggregateExpression], frame: Int) =
    VectorWindowPlanner.aggregateWindows(windowExpression).getOrElse(
      throw new IllegalStateException("window expressions are not aggregates over one supported frame")
    )
  @transient private lazy val combiners: Array[Array[VectorWindowPlanner.Combine]] =
    if (frame == VectorWindowPlanner.WholePartition) null
    else aggregates.map(a =>
      VectorWindowPlanner.prefixCombiners(
        a
      ).getOrElse(throw new IllegalStateException(s"no running frame for ${a.sql}"))
    ).toArray
  @transient private lazy val finalizers: Array[Array[Any] => Array[Any]] =
    if (combiners == null) null
    else aggregates.map(a => VectorWindowPlanner.prefixFinalizer(a).getOrElse(identity[Array[Any]] _)).toArray
  @transient private lazy val aggFunctions: Array[VectorAggFunction] = aggregates.map { a =>
    // A running decimal sum or average runs in its partial form; the prefix pass finalises per row (#259).
    val compiled = if (combiners != null && VectorWindowPlanner.runsPartial(a))
      a.copy(mode = org.apache.spark.sql.catalyst.expressions.aggregate.Partial)
    else a
    VectorAggregates.compile(compiled, child.output) match {
      case Right(f) => f
      case Left(reason) => throw new IllegalStateException(s"cannot vectorize window aggregate ${a.sql}: $reason")
    }
  }.toArray
  @transient private lazy val aggResults: Array[VectorExpr] = {
    val attrs = aggregates.map(_.resultAttribute)
    // Each output column is the window alias's value: the aggregate's own result, wrapped in any
    // scalar expressions over it (Spark's decimal-avg `cast(avg(UnscaledValue(d)) OVER (...) / scale
    // as decimal(p,s))`). For a bare window alias the wrapper is the identity, so this is the result
    // attribute as before.
    val resultExprs = VectorWindowPlanner.resultExpressions(windowExpression, aggregates)
    VectorAggregatePlanner.compileFinalResults(Nil, aggregates, attrs, resultExprs) match {
      case Right(exprs) => exprs.toArray
      case Left(reason) => throw new IllegalStateException(s"cannot vectorize window aggregate results: $reason")
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val pk = partitionKeys
    val childAttrs = child.output.map(a => (a.name, a.dataType)).toArray
    val windowAttrs = windowExpression.map(e => (e.name, e.dataType)).toArray
    val m = vectorMetrics
    if (aggregateMode) {
      val aggs = aggFunctions
      val results = aggResults
      val layout = VectorAggregatePlanner.bufferLayout(Nil, aggregates).toArray
      val ok = orderKeys
      val fr = frame
      val cb = combiners
      val fz = finalizers
      // The buffer columns carry the functions' emitted types: a Complete-mode decimal average emits its result type in slot 0 (#259).
      val bufferAttrs = aggregates.zip(aggs).flatMap { case (a, f) =>
        val declared = a.aggregateFunction.aggBufferAttributes
        val types = f.emittedTypes(declared.map(_.dataType))
        // A running decimal sum or average is finalised per row into slot 0, in the result type.
        val emitted = if (cb != null && VectorWindowPlanner.runsPartial(a)) a.dataType +: types.tail else types
        declared.map(_.name).zip(emitted)
      }.toArray
      child.executeColumnar().mapPartitionsInternal { iter =>
        new VectorWindowAggregateIterator(
          iter,
          pk,
          ok,
          fr,
          cb,
          fz,
          aggs,
          layout,
          bufferAttrs,
          results,
          childAttrs,
          windowAttrs,
          m
        )
      }
    } else if (offsetMode) {
      val ok = orderKeys
      val fs = offsets
      child.executeColumnar().mapPartitionsInternal { iter =>
        new VectorWindowOffsetIterator(iter, pk, ok, fs, childAttrs, windowAttrs, m)
      }
    } else {
      val ok = orderKeys
      val ks = kinds
      child.executeColumnar().mapPartitionsInternal { iter =>
        new VectorWindowIterator(iter, pk, ok, ks, childAttrs, windowAttrs, m)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  override def verboseStringWithOperatorId(): String = {
    s"""$formattedNodeName
       |Window: ${windowExpression.map(_.sql).mkString(", ")}
       |Partition: ${partitionSpec.map(_.sql).mkString(", ")}
       |Order: ${orderSpec.map(_.sql).mkString(", ")}
       |Output: ${output.map(_.name).mkString(", ")}
       |""".stripMargin
  }
}

object VectorWindowPlanner {
  val RowNumberKind = 0
  val RankKind = 1
  val DenseRankKind = 2

  /** Which ranking function a window expression is, or why it is not one this layer computes. */
  def rankKind(e: NamedExpression): Either[String, Int] = e match {
    case Alias(WindowExpression(f, _), _) => f match {
        case _: RowNumber => Right(RowNumberKind)
        case _: Rank => Right(RankKind)
        case _: DenseRank => Right(DenseRankKind)
        case _: PercentRank | _: CumeDist | _: NTile =>
          Left(s"window function ${f.prettyName} needs the partition size (held-partition path)")
        // Only reached beside a ranking function: the aggregate frames themselves are judged first.
        case a: AggregateExpression => Left(
            s"window aggregate ${a.aggregateFunction.prettyName} beside a ranking function in one operator not supported"
          )
        case other => Left(s"window function ${other.prettyName} not supported (row_number, rank and dense_rank are)")
      }
    case other => Left(s"window expression ${other.sql} not supported")
  }

  /** The ranking kind of a bare ranking function (WindowGroupLimitExec's `rankLikeFunction`). */
  def rankLikeKind(f: Expression): Either[String, Int] = f match {
    case _: RowNumber => Right(RowNumberKind)
    case _: Rank => Right(RankKind)
    case _: DenseRank => Right(DenseRankKind)
    case other => Left(s"window group limit function ${other.prettyName} not supported")
  }

  /** Aggregate frames this operator computes. */
  val WholePartition = 0

  /** `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`: a prefix over rows. */
  val RunningRows = 1

  /** `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (Spark's default with `ORDER BY`): a prefix over peer groups. */
  val RunningRange = 2

  /** Combines the running value so far with the next group's buffer slot; either side may be null. */
  type Combine = (Any, Any) => Any

  private def frameKind(f: Expression): Option[Int] = f match {
    case SpecifiedWindowFrame(_, UnboundedPreceding, UnboundedFollowing) => Some(WholePartition)
    case SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow) => Some(RunningRows)
    case SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow) => Some(RunningRange)
    case _ => None
  }

  /** The aggregate and frame kind of `e`, if it is a Complete aggregate without FILTER over a frame we compute. */
  private def aggregateWindow(e: NamedExpression): Option[(AggregateExpression, Int)] = e match {
    // first_value / last_value are positions in the frame, not prefixes: the offset family computes them.
    case Alias(WindowExpression(a: AggregateExpression, _), _)
        if a.aggregateFunction.isInstanceOf[First] || a.aggregateFunction.isInstanceOf[Last] => None
    case Alias(WindowExpression(a: AggregateExpression, WindowSpecDefinition(_, _, f)), _)
        if a.mode == Complete && a.filter.isEmpty =>
      frameKind(f).map(k => (a, k))
    // A window aggregate wrapped in scalar expressions over its result: Spark rewrites a decimal avg
    // to `cast(avg(UnscaledValue(d)) OVER (...) / scale as decimal(p,s))`, nesting the
    // WindowExpression inside `Cast(Divide(...))`. Peel the wrapper to the single WindowExpression and
    // classify by it; the exec projects the wrapper over the computed window column (see aggResults).
    case Alias(body, _) if !body.isInstanceOf[WindowExpression] =>
      wrappedAggregateWindow(body).flatMap { case (a, f) =>
        if (
          a.mode == Complete && a.filter.isEmpty &&
          !a.aggregateFunction.isInstanceOf[First] && !a.aggregateFunction.isInstanceOf[Last]
        )
          frameKind(f).map(k => (a, k))
        else None
      }
    case _ => None
  }

  /**
   * The single [[WindowExpression]] nested inside a scalar wrapper (`Cast`, `Divide`, arithmetic)
   * over its result, with its aggregate function and frame -- or None when there is not exactly one
   * WindowExpression, or its function is not an aggregate. The wrapper itself (whether the scalar
   * expressions around the result are ones we can compile) is checked when the result is compiled.
   */
  private def wrappedAggregateWindow(body: Expression): Option[(AggregateExpression, Expression)] = {
    val windows = body.collect { case w: WindowExpression => w }
    windows match {
      case Seq(WindowExpression(a: AggregateExpression, WindowSpecDefinition(_, _, f))) => Some((a, f))
      case _ => None
    }
  }

  /**
   * The window alias's output value as an expression over the aggregate's `resultAttribute`: the
   * scalar wrapper with the nested [[WindowExpression]] replaced by the aggregate's result attribute
   * (`compileFinalResults` then inlines the aggregate's own evaluate expression and compiles the
   * wrapper around it). For a bare `Alias(WindowExpression(...), name)` this is just the result
   * attribute aliased -- the previous behaviour.
   */
  private def windowResultExpr(e: NamedExpression, agg: AggregateExpression): NamedExpression = e match {
    case Alias(WindowExpression(_, _), name) => Alias(agg.resultAttribute, name)()
    case Alias(body, name) =>
      val replaced = body.transform { case _: WindowExpression => agg.resultAttribute }
      Alias(replaced, name)()
    case other => other
  }

  /** The offset-family / ranking functions only support a bare window alias; a wrapped result is aggregate-only. */
  def resultExpressions(es: Seq[NamedExpression], aggs: Seq[AggregateExpression]): Seq[NamedExpression] =
    es.zip(aggs).map { case (e, a) => windowResultExpr(e, a) }

  /**
   * Offset-family kinds: a shifted row, the frame's first row, the frame's last row, the frame's n-th row;
   * and the position-based ranking functions that need the partition size (`percent_rank`, `cume_dist`,
   * `ntile`) or that sit beside offset functions in one operator (`row_number`, `rank`, `dense_rank`).
   */
  val Shift = 0
  val FirstValue = 1
  val LastValue = 2
  val NthValueKind = 3
  val RowNumberAt = 4
  val RankAt = 5
  val DenseRankAt = 6
  val PercentRankAt = 7
  val CumeDistAt = 8
  val NTileAt = 9

  /**
   * A window function whose value is one row of the partition, read from the held rows: `kind` says
   * which row, `frame` (an aggregate frame kind) where the frame ends for the position-based ones,
   * `default` (Spark's internal representation) what a shifted row outside the partition yields.
   * A sliding aggregate over a `RANGE` frame with value offsets carries its [[RangeSpec]] in `range`.
   */
  final case class OffsetFunction(
      kind: Int,
      inputOrdinal: Int,
      dataType: DataType,
      offset: Int,
      default: Any,
      frame: Int,
      frameLo: Int = 0,
      frameHi: Int = 0,
      checked: Boolean = false,
      countAll: Boolean = false,
      context: org.apache.spark.QueryContext = null,
      inputType: DataType = null,
      range: RangeSpec = null
  )

  /**
   * A `RANGE` frame with value offsets over the single order key, as Spark's `createBoundOrdering`
   * evaluates it: the frame of a row with key `k` holds the rows whose key compares `>= k + lo` and
   * `<= k + hi` in the `SortOrder`'s comparison (`descending`, `nullsFirst`), `CURRENT ROW` being offset
   * 0 and the unbounded sides flagged. The offsets carry Spark's sign (`n PRECEDING` is `-n`, negated
   * again under `DESC`); the bound is `Add` in the key's width (`keyBits`), wrapped, or the ANSI
   * overflow error when `checked` -- `DateAdd` over a date key wraps and is never checked.
   */
  final case class RangeSpec(
      loUnbounded: Boolean,
      lo: Long,
      hiUnbounded: Boolean,
      hi: Long,
      keyBits: Int,
      descending: Boolean,
      nullsFirst: Boolean,
      checked: Boolean
  )

  /** Sliding-frame aggregates (`ROWS BETWEEN a AND b`, any bounds): re-aggregated per row over the frame's rows, in order, as Spark does. */
  val SlidingSum = 10
  val SlidingCount = 11
  val SlidingAvg = 12
  val SlidingMin = 13
  val SlidingMax = 14

  /** Frame bound sentinels in `frameLo` / `frameHi` (relative row offsets otherwise). */
  val UnboundedLo = Int.MinValue
  val UnboundedHi = Int.MaxValue

  /** `frameHi` for the `RANGE ... CURRENT ROW` upper bound: the end of the current peer group. */
  val PeerEndHi = Int.MaxValue - 1

  /** `frameLo` for the `RANGE CURRENT ROW ...` lower bound: the start of the current peer group. */
  val PeerStartLo = Int.MinValue + 1

  private def frameBound(b: Expression, upper: Boolean): Option[Int] = b match {
    case UnboundedPreceding => Some(UnboundedLo)
    case UnboundedFollowingBound => Some(UnboundedHi)
    case CurrentRow => Some(0)
    case other => literalInt(other)
  }

  /** Bits of the integral lane a `RANGE` order key of this type compares in, or None when value offsets over it are not computed. */
  private def rangeKeyBits(dt: DataType): Option[Int] = dt match {
    case ByteType => Some(8)
    case ShortType => Some(16)
    case IntegerType | DateType => Some(32)
    case LongType => Some(64)
    case _ => None
  }

  /**
   * The [[RangeSpec]] of a `RANGE` frame with at least one value offset, or why it is not computed: the
   * order key must be one integral or date column (Spark itself rejects several order keys here) and
   * each offset a foldable value of the key's type -- an int for a date key, as `DateAdd` takes;
   * interval offsets over date and timestamp keys and decimal keys are refused.
   */
  private def rangeSpec(
      name: String,
      frame: SpecifiedWindowFrame,
      orderSpec: Seq[SortOrder]
  ): Either[String, RangeSpec] = {
    if (orderSpec.length != 1)
      return Left(s"window aggregate $name over frame ${frame.sql}: RANGE offsets need exactly one order key")
    val order = orderSpec.head
    val keyType = order.child.dataType
    if (order.child.foldable)
      return Left(
        s"window aggregate $name over frame ${frame.sql}: RANGE offsets over a constant order key not supported"
      )
    val bits = rangeKeyBits(keyType) match {
      case Some(b) => b
      case None => return Left(
          s"window aggregate $name over frame ${frame.sql}: RANGE offsets over a ${keyType.simpleString} order key not supported (integral and date keys are)"
        )
    }
    val descending = order.direction == org.apache.spark.sql.catalyst.expressions.Descending
    // Spark's WindowFrameCoercion cast the offsets to the key's type; a date key keeps an int offset (DateAdd).
    def offset(b: Expression): Either[String, Option[Long]] = b match {
      case UnboundedPreceding | UnboundedFollowingBound => Right(None)
      case CurrentRow => Right(Some(0L))
      case other
          if other.foldable && (other.dataType == keyType || (keyType == DateType && other.dataType == IntegerType)) =>
        other.eval(EmptyRow) match {
          case null => Left(s"window aggregate $name over frame ${frame.sql}: a null RANGE offset")
          case v: java.lang.Number =>
            val signed = if (descending) -v.longValue() else v.longValue() // Spark's UnaryMinus under DESC
            Right(Some(signed))
          case _ => Left(s"window aggregate $name over frame ${frame.sql}: RANGE offset ${other.sql} is not integral")
        }
      case other => Left(
          s"window aggregate $name over frame ${frame.sql}: RANGE offset ${other.sql} of type ${other.dataType.simpleString} over a ${keyType.simpleString} key not supported"
        )
    }
    for (lo <- offset(frame.lower); hi <- offset(frame.upper)) yield RangeSpec(
      loUnbounded = lo.isEmpty,
      lo = lo.getOrElse(0L),
      hiUnbounded = hi.isEmpty,
      hi = hi.getOrElse(0L),
      keyBits = bits,
      descending = descending,
      nullsFirst = order.nullOrdering == org.apache.spark.sql.catalyst.expressions.NullsFirst,
      // Add in the key's type raises under ANSI; DateAdd never does.
      checked = keyType != DateType && org.apache.spark.sql.internal.SQLConf.get.ansiEnabled
    )
  }

  /** Whether a lane the RANGE kernels read: an integral (int32/int64) or double lane, not a string, boolean or decimal. */
  private def rangeLane(dt: DataType): Boolean = dt match {
    case ByteType | ShortType | IntegerType | DateType | LongType | TimestampType | DoubleType => true
    case _ => false
  }

  /**
   * A sum/count/avg/min/max over a `ROWS` frame with literal bounds, a `RANGE` frame whose bounds are
   * both special (unbounded / the peer group), or a `RANGE` frame with value offsets (a [[RangeSpec]]).
   */
  private def slidingAggregate(
      a: AggregateExpression,
      frame: Expression,
      output: Seq[Attribute],
      orderSpec: Seq[SortOrder]
  ): Either[String, OffsetFunction] = {
    val name = a.aggregateFunction.prettyName
    if (a.filter.nonEmpty) return Left(s"window aggregate $name with FILTER not supported")
    if (a.mode != Complete) return Left(s"window aggregate $name in mode ${a.mode} not supported")
    if (
      a.dataType.isInstanceOf[DecimalType] || a.aggregateFunction.children.exists(_.dataType.isInstanceOf[DecimalType])
    )
      return Left(
        s"window aggregate $name over decimals in a sliding frame not supported (the sliding kernels are long and double)"
      )
    val bounds: Either[String, (Int, Int, RangeSpec)] = frame match {
      case SpecifiedWindowFrame(RowFrame, lower, upper) =>
        (frameBound(lower, upper = false), frameBound(upper, upper = true)) match {
          case (Some(lo), Some(hi)) => Right((lo, hi, null))
          case _ => Left(s"window aggregate $name over frame ${frame.sql}: bounds must be literals")
        }
      // No value offset: the bounds are the partition's ends or the current peer group's, for any key type.
      case SpecifiedWindowFrame(RangeFrame, lower: SpecialFrameBoundary, upper: SpecialFrameBoundary) =>
        ((lower, upper) match {
          case (UnboundedPreceding, UnboundedFollowingBound) => Some((UnboundedLo, UnboundedHi))
          case (UnboundedPreceding, CurrentRow) => Some((UnboundedLo, PeerEndHi))
          case (CurrentRow, UnboundedFollowingBound) => Some((PeerStartLo, UnboundedHi))
          case (CurrentRow, CurrentRow) => Some((PeerStartLo, PeerEndHi))
          case _ => None // an analysis error in Spark
        }).map { case (lo, hi) => (lo, hi, null) }
          .toRight(s"window aggregate $name over frame ${frame.sql} not supported")
      case f @ SpecifiedWindowFrame(RangeFrame, _, _) => rangeSpec(name, f, orderSpec).map(spec => (0, 0, spec))
      case other => Left(s"window aggregate $name over frame ${other.sql} not supported")
    }
    bounds.flatMap { case (lo, hi, range) =>
      // The RANGE kernels read integral and double lanes; min/max over another lane stay on Spark.
      def laneChecked(child: Expression, fn: OffsetFunction): Either[String, OffsetFunction] =
        if (range != null && !rangeLane(child.dataType))
          Left(
            s"window aggregate $name over ${child.dataType.simpleString} in frame ${frame.sql} not supported (integral, date and double inputs are)"
          )
        else Right(fn)
      a.aggregateFunction match {
        case sum: Sum if sum.evalContext.evalMode == EvalMode.TRY =>
          Left(s"window aggregate try_sum over a sliding frame not supported")
        case sum: Sum if sum.dataType == LongType || sum.dataType == DoubleType =>
          inputOrdinal(sum.child, output).map(ord =>
            OffsetFunction(
              SlidingSum,
              ord,
              sum.dataType,
              0,
              null,
              WholePartition,
              lo,
              hi,
              checked = sum.evalContext.evalMode == EvalMode.ANSI,
              context = sum.origin.context,
              inputType = sum.child.dataType,
              range = range
            )
          )
        case c: Count => c.children match {
            case Seq(l: Literal) if l.value != null =>
              Right(OffsetFunction(
                SlidingCount,
                -1,
                LongType,
                0,
                null,
                WholePartition,
                lo,
                hi,
                countAll = true,
                range = range
              ))
            case Seq(child) => inputOrdinal(child, output).map(ord =>
                OffsetFunction(
                  SlidingCount,
                  ord,
                  LongType,
                  0,
                  null,
                  WholePartition,
                  lo,
                  hi,
                  inputType = child.dataType,
                  range = range
                )
              )
            case _ => Left("window count over several columns in a sliding frame not supported")
          }
        case av: Average if av.dataType == DoubleType =>
          inputOrdinal(av.child, output).map(ord =>
            OffsetFunction(
              SlidingAvg,
              ord,
              DoubleType,
              0,
              null,
              WholePartition,
              lo,
              hi,
              inputType = av.child.dataType,
              range = range
            )
          )
        case m: Min => inputOrdinal(m.child, output).flatMap(ord =>
            laneChecked(
              m.child,
              OffsetFunction(SlidingMin, ord, m.dataType, 0, null, WholePartition, lo, hi, range = range)
            )
          )
        case m: Max => inputOrdinal(m.child, output).flatMap(ord =>
            laneChecked(
              m.child,
              OffsetFunction(SlidingMax, ord, m.dataType, 0, null, WholePartition, lo, hi, range = range)
            )
          )
        case other => Left(
            s"window aggregate ${other.prettyName} over a sliding frame not supported (sum, avg, count, min and max are)"
          )
      }
    }
  }

  private def literalInt(e: Expression): Option[Int] = e match {
    case l: Literal if l.foldable && l.value != null && (l.dataType == IntegerType) => Some(l.value.asInstanceOf[Int])
    case other if other.foldable && other.dataType == IntegerType =>
      Option(other.eval(EmptyRow)).map(_.asInstanceOf[Int])
    case _ => None
  }

  /** A foldable default in Spark's internal representation, with decimals as their unscaled long (the INT64 lane). */
  private def literalDefault(e: Expression, dt: DataType): Either[String, Any] =
    if (!e.foldable) Left("default value must be a literal")
    else e.eval(EmptyRow) match {
      case null => Right(null)
      case d: org.apache.spark.sql.types.Decimal => Right(java.lang.Long.valueOf(d.toUnscaledLong))
      case v => Right(v)
    }

  private def inputOrdinal(input: Expression, output: Seq[Attribute]): Either[String, Int] = input match {
    case a: AttributeReference =>
      val i = output.indexWhere(_.exprId == a.exprId)
      if (i < 0) Left(s"input ${a.sql} is not a column of the child")
      else if (!TypeMapping.hasLane(a.dataType))
        Left(s"unsupported column type ${a.dataType.simpleString} for ${a.name}")
      else Right(i)
    case other => Left(s"input ${other.sql} is not a column (Spark projects complex inputs below the window)")
  }

  /** `e` as an offset-family function, or why it is not one. */
  private def offsetWindow(
      e: NamedExpression,
      output: Seq[Attribute],
      orderSpec: Seq[SortOrder]
  ): Either[String, OffsetFunction] = e match {
    case Alias(WindowExpression(f: FrameLessOffsetWindowFunction, _), _) =>
      if (f.ignoreNulls) Left(s"${f.prettyName} IGNORE NULLS not supported")
      else literalInt(f.offset) match {
        case None => Left(s"${f.prettyName} offset must be an integer literal")
        case Some(off) =>
          for (
            ord <- inputOrdinal(f.input, output);
            dflt <- literalDefault(f.default, f.dataType).left.map(r => s"${f.prettyName}: $r")
          )
            yield OffsetFunction(Shift, ord, f.dataType, off, dflt, WholePartition)
      }
    case Alias(WindowExpression(n: NthValue, WindowSpecDefinition(_, _, frame)), _) =>
      if (n.ignoreNulls) Left("nth_value IGNORE NULLS not supported")
      else (literalInt(n.offset), frameKind(frame)) match {
        case (None, _) => Left("nth_value offset must be an integer literal")
        case (_, None) => Left(s"nth_value over frame ${frame.sql} not supported")
        case (Some(k), Some(fk)) =>
          inputOrdinal(n.input, output).map(ord => OffsetFunction(NthValueKind, ord, n.dataType, k, null, fk))
      }
    case Alias(WindowExpression(a: AggregateExpression, WindowSpecDefinition(_, _, frame)), _)
        if a.aggregateFunction.isInstanceOf[First] || a.aggregateFunction.isInstanceOf[Last] =>
      val (child, ignoreNulls, isFirst) = a.aggregateFunction match {
        case fst: First => (fst.child, fst.ignoreNulls, true)
        case lst: Last => (lst.child, lst.ignoreNulls, false)
      }
      val name = a.aggregateFunction.prettyName
      if (ignoreNulls) Left(s"$name IGNORE NULLS not supported")
      else if (a.filter.nonEmpty) Left(s"$name with FILTER not supported")
      else frameKind(frame) match {
        case None => Left(s"$name over frame ${frame.sql} not supported")
        case Some(fk) => inputOrdinal(child, output).map(ord =>
            OffsetFunction(if (isFirst) FirstValue else LastValue, ord, a.dataType, 0, null, fk)
          )
      }
    case Alias(WindowExpression(a: AggregateExpression, WindowSpecDefinition(_, _, frame)), _) =>
      slidingAggregate(a, frame, output, orderSpec)
    case Alias(WindowExpression(_: RowNumber, _), _) =>
      Right(OffsetFunction(RowNumberAt, -1, IntegerType, 0, null, WholePartition))
    case Alias(WindowExpression(_: Rank, _), _) =>
      Right(OffsetFunction(RankAt, -1, IntegerType, 0, null, WholePartition))
    case Alias(WindowExpression(_: DenseRank, _), _) =>
      Right(OffsetFunction(DenseRankAt, -1, IntegerType, 0, null, WholePartition))
    case Alias(WindowExpression(_: PercentRank, _), _) =>
      Right(OffsetFunction(PercentRankAt, -1, DoubleType, 0, null, WholePartition))
    case Alias(WindowExpression(_: CumeDist, _), _) =>
      Right(OffsetFunction(CumeDistAt, -1, DoubleType, 0, null, WholePartition))
    case Alias(WindowExpression(n: NTile, _), _) =>
      literalInt(n.buckets) match {
        case Some(b) if b > 0 => Right(OffsetFunction(NTileAt, -1, IntegerType, b, null, WholePartition))
        case _ => Left("ntile buckets must be a positive integer literal")
      }
    case Alias(WindowExpression(f, _), _) => Left(s"window function ${f.prettyName} is not an offset function")
    case other => Left(s"window expression ${other.sql} not supported")
  }

  /** All of the operator's expressions as offset-family functions, or the first reason one is not. */
  def offsetWindows(
      es: Seq[NamedExpression],
      output: Seq[Attribute],
      orderSpec: Seq[SortOrder]
  ): Either[String, Seq[OffsetFunction]] = {
    val all = es.map(offsetWindow(_, output, orderSpec))
    all.collectFirst { case Left(r) => r }.toLeft(all.collect { case Right(f) => f })
  }

  /** All of the operator's expressions as aggregates over ONE frame kind, or None when any is something else. */
  def aggregateWindows(es: Seq[NamedExpression]): Option[(Seq[AggregateExpression], Int)] = {
    val aggs = es.map(aggregateWindow)
    if (es.nonEmpty && aggs.forall(_.isDefined) && aggs.flatten.map(_._2).distinct.length == 1)
      Some((aggs.flatten.map(_._1), aggs.flatten.head._2))
    else None
  }

  private def nullSafe(op: (Any, Any) => Any): Combine = (p, c) => if (p == null) c else if (c == null) p else op(p, c)
  private def addLong(checked: Boolean, context: org.apache.spark.QueryContext): Combine = nullSafe { (p, c) =>
    val a = p.asInstanceOf[java.lang.Long].longValue(); val b = c.asInstanceOf[java.lang.Long].longValue()
    if (checked) {
      try java.lang.Long.valueOf(Math.addExact(a, b))
      catch {
        case _: ArithmeticException => throw VectorErrors.arithmeticOverflow("long overflow", "try_sum", context)
      }
    } else java.lang.Long.valueOf(a + b)
  }
  private val addDouble: Combine = nullSafe((p, c) =>
    java.lang.Double.valueOf(
      p.asInstanceOf[java.lang.Double].doubleValue() + c.asInstanceOf[java.lang.Double].doubleValue()
    )
  )
  private def extreme(isMin: Boolean): Combine = nullSafe { (p, c) =>
    // Boxed Integer / Long / Double / Boolean / UTF8String: Comparable with Spark's ordering (doubles total, strings binary).
    val cmp = p.asInstanceOf[Comparable[Any]].compareTo(c)
    if ((cmp <= 0) == isMin) p else c
  }

  /** How a running frame carries each buffer slot forward, or why this function has no running form here. */
  def prefixCombiners(a: AggregateExpression): Either[String, Array[Combine]] = a.aggregateFunction match {
    case s: Sum if s.evalContext.evalMode == EvalMode.TRY => Left("running try_sum not supported")
    case s: Sum if s.dataType == LongType =>
      Right(Array(addLong(s.evalContext.evalMode == EvalMode.ANSI, s.origin.context)))
    case s: Sum if s.dataType == DoubleType => Right(Array(addDouble))
    // A decimal sum's partial buffer is (exact total or null past the buffer precision, isEmpty): the total is
    // added exactly and poisons once it overflows, as Spark's buffer does, the emptiness is an AND (#259).
    case s: Sum if s.dataType.isInstanceOf[SparkDecimalType] =>
      Right(Array(addDecimal(s.dataType.asInstanceOf[SparkDecimalType]), andEmpty))
    case _: Count => Right(Array(addLong(checked = false, null)))
    case av: Average if av.dataType == DoubleType => Right(Array(addDouble, addLong(checked = false, null)))
    case av: Average if av.child.dataType.isInstanceOf[SparkDecimalType] =>
      Right(Array(
        addDecimal(av.aggBufferAttributes.head.dataType.asInstanceOf[SparkDecimalType]),
        addLong(checked = false, null)
      ))
    case _: Min => Right(Array(extreme(isMin = true)))
    case _: Max => Right(Array(extreme(isMin = false)))
    case other => Left(s"running frame for ${other.prettyName} not supported (sum, avg, count, min and max are)")
  }

  /** `(total, total) -> total`: exact `BigDecimal` addition, null (overflowed) once the total leaves the buffer precision or either side is null. */
  private def addDecimal(bufferType: SparkDecimalType): Combine = {
    val limit = java.math.BigInteger.TEN.pow(bufferType.precision)
    (p, c) =>
      if (p == null || c == null) null
      else {
        val r = p.asInstanceOf[java.math.BigDecimal].add(c.asInstanceOf[java.math.BigDecimal])
        if (r.unscaledValue().abs().compareTo(limit) >= 0) null else r
      }
  }
  private val andEmpty: Combine =
    (p, c) => java.lang.Boolean.valueOf(p.asInstanceOf[java.lang.Boolean] && c.asInstanceOf[java.lang.Boolean])

  /**
   * The decimal aggregates run a running frame in their *partial* form (so the prefix can add exact
   * totals); this turns the combined slots of one row into the result the merge would emit --
   * Spark's `If(isEmpty, null, CheckOverflowInSum(sum))` for a sum (an overflowed total is null in
   * legacy mode, `ARITHMETIC_OVERFLOW` under ANSI), `DecimalAvgResult` for an average -- in slot 0,
   * which the result projection forwards.
   */
  def prefixFinalizer(a: AggregateExpression): Option[Array[Any] => Array[Any]] = a.aggregateFunction match {
    case s: Sum if s.dataType.isInstanceOf[SparkDecimalType] =>
      val ansi = s.evalContext.evalMode == EvalMode.ANSI
      val context = s.origin.context
      Some { slots =>
        val out = slots.clone()
        out(0) =
          if (slots(1).asInstanceOf[java.lang.Boolean]) null
          else if (slots(0) == null) { if (ansi) throw VectorErrors.overflowInSumOfDecimal(context) else null }
          else slots(0)
        out
      }
    case av: Average if av.child.dataType.isInstanceOf[SparkDecimalType] =>
      val result = io.vecruntime.spark.agg.DecimalAvgResult(av)
      Some { slots =>
        val out = slots.clone()
        out(0) =
          result.value(slots(0).asInstanceOf[java.math.BigDecimal], slots(1).asInstanceOf[java.lang.Long].longValue())
        out
      }
    case _ => None
  }

  /** Whether the running frame needs the aggregate's partial buffer rather than its finalised form. */
  def runsPartial(a: AggregateExpression): Boolean = prefixFinalizer(a).isDefined

  /** Why a window aggregate is not computed: the frame, the function itself, or its scalar wrapper. */
  private def aggregateReason(e: NamedExpression, input: Seq[Attribute], orderSpec: Seq[SortOrder]): Option[String] = {
    // The aggregate and frame, from a bare `Alias(WindowExpression(...), _)` or one wrapped in scalar
    // expressions over the result (the decimal-avg `Cast(Divide(...))` rewrite). first_value /
    // last_value are the offset family's, not this path's.
    val extracted: Option[(AggregateExpression, Expression)] = e match {
      case Alias(WindowExpression(a: AggregateExpression, _), _)
          if a.aggregateFunction.isInstanceOf[First] || a.aggregateFunction.isInstanceOf[Last] => None
      case Alias(WindowExpression(a: AggregateExpression, WindowSpecDefinition(_, _, frame)), _) => Some((a, frame))
      case Alias(body, _) if !body.isInstanceOf[WindowExpression] =>
        wrappedAggregateWindow(body).filterNot { case (a, _) =>
          a.aggregateFunction.isInstanceOf[First] || a.aggregateFunction.isInstanceOf[Last]
        }
      case _ => None
    }
    extracted.flatMap { case (a, frame) =>
      val funcReason = frameKind(frame) match {
        case Some(kind) =>
          // Decimal aggregates run on the aggregate machinery (the 128-bit accumulators, #259): a running frame
          // takes the partial buffer and finalises per row (prefixFinalizer), a whole partition the final form.
          VectorAggregates.compile(
            if (kind != WholePartition && runsPartial(a))
              a.copy(mode = org.apache.spark.sql.catalyst.expressions.aggregate.Partial)
            else a,
            input
          )
            .left.toOption.map(r => s"window aggregate ${a.aggregateFunction.prettyName}: $r")
            .orElse(if (kind == WholePartition) None
            else prefixCombiners(a).left.toOption.map(r => s"window aggregate ${a.aggregateFunction.prettyName}: $r"))
        case None => frame match {
            case SpecifiedWindowFrame(RowFrame, lower, upper)
                if frameBound(lower, upper = false).isDefined && frameBound(upper, upper = true).isDefined =>
              slidingAggregate(a, frame, input, orderSpec).left.toOption
            // A RANGE frame with value offsets or peer bounds: the sliding path's own reason.
            case SpecifiedWindowFrame(RangeFrame, _, _) => slidingAggregate(a, frame, input, orderSpec).left.toOption
            case _ => Some(s"window aggregate ${a.aggregateFunction.prettyName} over frame ${frame.sql} not supported")
          }
      }
      // A scalar wrapper (the decimal-avg cast/divide) must itself compile over the aggregate's result.
      funcReason.orElse(
        VectorAggregatePlanner
          .compileFinalResults(Nil, Seq(a), Seq(a.resultAttribute), Seq(windowResultExpr(e, a)))
          .left.toOption
          .map(r => s"window result ${e.name}: $r")
      )
    }
  }

  def plan(w: WindowExec): Either[String, VectorWindowExec] = {
    val keys = w.partitionSpec ++ w.orderSpec.map(_.child)
    val keyFailures = keys.flatMap { k =>
      if (k.dataType == DoubleType)
        Some(s"window key ${k.sql}: double keys not supported (Spark compares them after NaN and zero normalisation)")
      else if (!TypeMapping.hasLane(k.dataType)) Some(s"window key type ${k.dataType.simpleString} not supported")
      else ExpressionCompiler.compileLaneColumn(k, w.child.output).left.toOption.map(r => s"window key ${k.sql}: $r")
    }
    aggregateWindows(w.windowExpression) match {
      case Some((aggs, _)) =>
        val reasons = w.windowExpression.flatMap(aggregateReason(_, w.child.output, w.orderSpec))
        val attrs = aggs.map(_.resultAttribute)
        // The result columns are the window aliases' values (the aggregate result, wrapped in any
        // scalar expressions over it -- Spark's decimal-avg cast/divide). Compile the wrappers, not
        // the bare attributes, so a wrapper we cannot compile is a reason rather than a runtime throw.
        val resultExprs = resultExpressions(w.windowExpression, aggs)
        val resultReason = if (reasons.nonEmpty) None
        else VectorAggregatePlanner.compileFinalResults(Nil, aggs, attrs, resultExprs).left.toOption.map(r =>
          s"window aggregate result: $r"
        )
        (reasons ++ resultReason ++ keyFailures).headOption.toLeft(VectorWindowExec(
          w.windowExpression,
          w.partitionSpec,
          w.orderSpec,
          w.child
        ))
      case None =>
        val kinds = w.windowExpression.map(rankKind)
        kinds.collectFirst { case Left(reason) => reason } match {
          case Some(reason) =>
            offsetWindows(w.windowExpression, w.child.output, w.orderSpec) match {
              case Right(_) => keyFailures.headOption.toLeft(VectorWindowExec(
                  w.windowExpression,
                  w.partitionSpec,
                  w.orderSpec,
                  w.child
                ))
              case Left(offsetReason) =>
                // A ranking function beside an aggregate in one spec, an aggregate over another frame, two
                // frame kinds in one operator, or an offset function we cannot read: say which.
                val mixed = w.windowExpression.map(aggregateWindow).flatten.map(_._2).distinct.length > 1
                val offsetLike = w.windowExpression.exists {
                  case Alias(WindowExpression(f, _), _) =>
                    f.isInstanceOf[FrameLessOffsetWindowFunction] || f.isInstanceOf[NthValue] ||
                    f.isInstanceOf[PercentRank] || f.isInstanceOf[CumeDist] || f.isInstanceOf[NTile] || f.isInstanceOf[
                      AggregateExpression
                    ] ||
                    (f match {
                      case a: AggregateExpression =>
                        a.aggregateFunction.isInstanceOf[First] || a.aggregateFunction.isInstanceOf[Last];
                      case _ => false
                    })
                  case _ => false
                }
                Left(w.windowExpression.flatMap(aggregateReason(_, w.child.output, w.orderSpec)).headOption.getOrElse(
                  if (mixed) "window aggregates over different frames in one operator not supported"
                  else if (offsetLike && !offsetReason.contains("is not an offset function")) offsetReason
                  else if (offsetLike) "offset functions beside other window functions in one operator not supported"
                  else reason
                ))
            }
          case None if w.orderSpec.isEmpty => Left("ranking window without an ORDER BY")
          case None =>
            keyFailures.headOption.toLeft(VectorWindowExec(w.windowExpression, w.partitionSpec, w.orderSpec, w.child))
        }
    }
  }
}

/** Detects partition (or peer) boundaries: null-safe equality of a row's keys with the previous row's. */
private[vecruntime] final class KeyTracker(keys: Array[VectorExpr]) {
  private val previous = new Array[Any](keys.length)
  private var lanes: Array[VectorBuffers] = _
  var hasPrevious = false

  def startBatch(ctx: io.vecruntime.spark.expr.EvalContext): Unit = lanes = keys.map(_.eval(ctx))

  /** Key `k`'s lane over the current batch. */
  def lane(k: Int): VectorBuffers = lanes(k)

  /** True when row `i` differs from the remembered row (or none is remembered). */
  def changed(i: Int): Boolean = {
    if (!hasPrevious) return true
    var k = 0
    while (k < lanes.length) {
      val current = if (Rows.valid(lanes(k), i)) Rows.box(lanes(k), i) else null
      val before = previous(k)
      if (!(if (before == null) current == null else before.equals(current))) return true
      k += 1
    }
    false
  }

  def remember(i: Int): Unit = {
    var k = 0
    while (k < lanes.length) {
      previous(k) = if (Rows.valid(lanes(k), i)) Rows.box(lanes(k), i) else null
      k += 1
    }
    hasPrevious = true
  }
}

/** The ranking walk over sorted batches; the previous row's keys and the counters carry across batches. */
private[vecruntime] class VectorWindowIterator(
    input: Iterator[ColumnarBatch],
    partitionKeys: Array[VectorExpr],
    orderKeys: Array[VectorExpr],
    kinds: Array[Int],
    childAttrs: Array[(String, DataType)],
    windowAttrs: Array[(String, DataType)],
    metrics: VectorMetrics
) extends VectorBatchIterator(input, "VectorWindowExec") {

  private val partition = new KeyTracker(partitionKeys)
  private val order = new KeyTracker(orderKeys)
  private var rowNumber = 0L
  private var rank = 0L
  private var denseRank = 0L

  override protected def process(batch: ColumnarBatch): ColumnarBatch = metrics.timed {
    metrics.numInputBatches += 1
    withEvalContext(batch) { ctx =>
      val n = ctx.numRows
      val live = ctx.selection
      val outRows = if (live == null) n else ctx.selectedCount
      partition.startBatch(ctx)
      order.startBatch(ctx)
      val results = new Array[Array[Int]](kinds.length)
      var f = 0
      while (f < kinds.length) { results(f) = new Array[Int](outRows); f += 1 }
      var out = 0
      var i = 0
      while (i < n) {
        if (live == null || Bitmap.isSet(live, i)) {
          val newPartition = partition.changed(i)
          if (newPartition) {
            rowNumber = 0L
            rank = 0L
            denseRank = 0L
          }
          rowNumber += 1
          val peer = !newPartition && !order.changed(i)
          if (!peer) {
            rank = rowNumber
            denseRank += 1
          }
          partition.remember(i)
          order.remember(i)
          f = 0
          while (f < kinds.length) {
            results(f)(out) = kinds(f) match {
              case VectorWindowPlanner.RowNumberKind => rowNumber.toInt
              case VectorWindowPlanner.RankKind => rank.toInt
              case _ => denseRank.toInt
            }
            f += 1
          }
          out += 1
        }
        i += 1
      }
      val columns = new Array[ColumnVector](childAttrs.length + kinds.length)
      var c = 0
      while (c < childAttrs.length) {
        val (name, dt) = childAttrs(c)
        // Forwarded columns are never copied: the child keeps them alive until its next batch.
        columns(c) = if (live == null) BorrowedColumnVector.of(batch.column(c))
        else ArrowOutput.compact(name, dt, ctx.input(c), live, outRows, allocator)
        c += 1
      }
      f = 0
      while (f < kinds.length) {
        val (name, dt) = windowAttrs(f)
        val buffers = ArrowOutput.allocateFixed(name, dt, outRows, allocator)
        val data = buffers.data()
        var r = 0
        while (r < outRows) { data.setAtIndex(VectorBuffers.LE_INT, r, results(f)(r)); r += 1 }
        columns(childAttrs.length + f) = ArrowOutput.finish(buffers, outRows, true)
        f += 1
      }
      metrics.numOutputBatches += 1
      metrics.numOutputRows += outRows
      new ColumnarBatch(columns, outRows)
    }
  }
}

/**
 * Whole-partition aggregates over sorted batches. Partitions are groups numbered in arrival order;
 * the grouped aggregate states accumulate every batch with the partition ordinal as the group id.
 * A batch is held (its live rows copied) until the partition its last row belongs to has ended --
 * the next batch starts a new one, or the input ends -- and is then emitted with the values of its
 * partitions gathered per row.
 */
private[vecruntime] class VectorWindowAggregateIterator(
    input: Iterator[ColumnarBatch],
    partitionKeys: Array[VectorExpr],
    orderKeys: Array[VectorExpr],
    frame: Int,
    combiners: Array[Array[VectorWindowPlanner.Combine]],
    /** Per aggregate, the combined slots of a row turned into the emitted form (a decimal sum or average, #259); identity otherwise. */
    finalizers: Array[Array[Any] => Array[Any]],
    aggs: Array[VectorAggFunction],
    layout: Array[OutputSlot],
    bufferAttrs: Array[(String, DataType)],
    results: Array[VectorExpr],
    childAttrs: Array[(String, DataType)],
    windowAttrs: Array[(String, DataType)],
    metrics: VectorMetrics
) extends Iterator[ColumnarBatch] with AutoCloseable {

  /** A held batch: owned copies of the live rows and each row's partition ordinal. */
  private final class Held(val columns: Array[ColumnVector], val numRows: Int, val groups: Array[Int])

  private val allocator: BufferAllocator = VectorAllocators.newChild("VectorWindowExec")
  private val states: Array[GroupedAggState] = aggs.map(_.newGroupedState())
  private val partition = new KeyTracker(partitionKeys)
  private val order = new KeyTracker(orderKeys)
  private var numGroups = 0
  // Running frames: groups are rows or peer groups, and a group's value is the prefix of its partition's
  // buffers up to it. `partitionStarts` marks the groups that begin a partition; `lastPrefix` is the
  // prefix of group `prefixDone - 1`, the only earlier group a later batch can still reference.
  private val partitionStarts = mutable.BitSet.empty
  private var prefixDone = 0
  private var lastPrefix: Array[Array[Any]] = _
  private var lastEmitted: Array[Array[Any]] = _
  private val held = mutable.Queue.empty[Held]
  private val ready = mutable.Queue.empty[ColumnarBatch]
  private var idScratch = new Array[Int](0)
  private var inputDone = false
  private var current: ColumnarBatch = _
  private var closed = false

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  private def consume(batch: ColumnarBatch): Unit = metrics.timed {
    metrics.numInputBatches += 1
    EvalContexts.withBatch(batch) { ctx =>
      val n = ctx.numRows
      val live = ctx.selection
      val outRows = if (live == null) n else ctx.selectedCount
      if (idScratch.length < n) idScratch = new Array[Int](n)
      partition.startBatch(ctx)
      order.startBatch(ctx)
      val groups = new Array[Int](outRows)
      var out = 0
      var i = 0
      while (i < n) {
        if (live == null || Bitmap.isSet(live, i)) {
          val newPartition = partition.changed(i)
          val newGroup = frame match {
            case VectorWindowPlanner.RunningRows => true
            case VectorWindowPlanner.RunningRange => newPartition || order.changed(i)
            case _ => newPartition
          }
          if (newGroup) {
            numGroups += 1
            if (newPartition) partitionStarts += numGroups - 1
          }
          partition.remember(i)
          order.remember(i)
          idScratch(i) = numGroups - 1
          groups(out) = numGroups - 1
          out += 1
        } else idScratch(i) = -1
        i += 1
      }
      val assignment = GroupAssignment.of(idScratch, n, numGroups, ctx.arena, live)
      var s = 0
      while (s < states.length) { states(s).update(ctx, assignment); s += 1 }
      val columns = new Array[ColumnVector](childAttrs.length)
      var c = 0
      while (c < childAttrs.length) {
        val (name, dt) = childAttrs(c)
        // The child reuses or releases its batch once we pull the next one: the held rows are copies.
        columns(c) = if (live == null) ArrowOutput.copy(name, dt, ctx.input(c), allocator)
        else ArrowOutput.compact(name, dt, ctx.input(c), live, outRows, allocator)
        c += 1
      }
      held.enqueue(new Held(columns, outRows, groups))
    }
  }

  /** Emits every held batch whose partitions have all ended (all of them once the input is done). */
  private def release(): Unit = metrics.timed {
    while (held.nonEmpty && (inputDone || held.head.groups(held.head.numRows - 1) < numGroups - 1)) {
      val h = held.dequeue()
      val from = h.groups(0)
      val to = h.groups(h.numRows - 1) + 1
      val idx = new Array[Int](h.numRows)
      var r = 0
      while (r < h.numRows) { idx(r) = h.groups(r) - from; r += 1 }
      // The groups' buffers as a batch of `to - from` rows -- for a running frame the prefix within the
      // partition -- the results evaluated over it and gathered per row.
      val prefixes = if (combiners == null) null else prefixOf(from, to)
      val bufferColumns = new Array[ColumnVector](layout.length)
      var c = 0
      while (c < layout.length) {
        val (name, dt) = bufferAttrs(c)
        bufferColumns(c) = layout(c) match {
          case BufferSlot(aggIdx, slot) if prefixes == null =>
            AggBufferColumns.column(name, dt, states(aggIdx), slot, from, to, allocator)
          case BufferSlot(aggIdx, slot) =>
            AggBufferColumns.values(name, dt, to - from, o => prefixes(o)(aggIdx)(slot), allocator)
          case KeySlot(_) => throw new IllegalStateException("key slot in a window aggregate")
        }
        c += 1
      }
      val buffers = new ColumnarBatch(bufferColumns, to - from)
      val columns = new Array[ColumnVector](childAttrs.length + results.length)
      try {
        EvalContexts.withBatch(buffers) { ctx =>
          var f = 0
          while (f < results.length) {
            val (name, dt) = windowAttrs(f)
            columns(childAttrs.length + f) = results(f) match {
              case lit: LiteralExpr => ArrowOutput.constant(name, dt, lit.value, h.numRows, allocator)
              case e => ArrowOutput.gather(name, dt, e.eval(ctx), idx, 0, h.numRows, allocator)
            }
            f += 1
          }
        }
      } finally buffers.close()
      System.arraycopy(h.columns, 0, columns, 0, childAttrs.length)
      metrics.numOutputBatches += 1
      metrics.numOutputRows += h.numRows
      ready.enqueue(new ColumnarBatch(columns, h.numRows))
    }
  }

  /** The running values of groups `[from, to)`: each group's buffers combined with the prefix before it in its partition. */
  private def prefixOf(from: Int, to: Int): Array[Array[Array[Any]]] = {
    val out = new Array[Array[Array[Any]]](to - from)
    var g = from
    while (g < to) {
      if (g == prefixDone - 1) {
        out(g - from) = lastEmitted
      } else {
        require(g == prefixDone, s"running frame groups released out of order: $g after $prefixDone")
        val value = new Array[Array[Any]](aggs.length)
        var a = 0
        while (a < aggs.length) {
          val slots = new Array[Any](combiners(a).length)
          var k = 0
          while (k < slots.length) {
            val current = states(a).bufferValue(g, k)
            slots(k) = if (partitionStarts.contains(g)) current else combiners(a)(k)(lastPrefix(a)(k), current)
            k += 1
          }
          value(a) = slots
          a += 1
        }
        lastPrefix = value
        val emitted = if (finalizers == null) value
        else { val e = value.clone(); var i = 0; while (i < e.length) { e(i) = finalizers(i)(value(i)); i += 1 }; e }
        lastEmitted = emitted
        prefixDone = g + 1
        out(g - from) = emitted
      }
      g += 1
    }
    out
  }

  override def hasNext: Boolean = {
    while (ready.isEmpty && !inputDone) {
      if (input.hasNext) {
        val batch = input.next()
        if (batch.numRows() > 0) consume(batch)
      } else inputDone = true
      release()
    }
    ready.nonEmpty
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    releaseCurrent()
    current = ready.dequeue()
    current
  }

  private def releaseCurrent(): Unit = if (current != null) { current.close(); current = null }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      releaseCurrent()
      ready.foreach(_.close())
      ready.clear()
      held.foreach(_.columns.foreach(_.close()))
      held.clear()
      allocator.close()
    }
  }
}

/**
 * Columnar replacement for WindowGroupLimitExec (#58, layer 4): Spark's per-partition top-k below a
 * ranking window, inserted for a filter `rank <= k` (k up to `spark.sql.optimizer.windowGroupLimitThreshold`)
 * -- in Partial mode below the exchange, over whatever produced the rows, and in Final mode above the
 * sort. Both modes require the same ordering as the window (partition keys, then order keys) and the
 * Final mode the window's distribution, so the walk is the ranking walk of [[VectorWindowIterator]]
 * with a filter: a row is kept while the ranking function's value at that row is at most `limit`.
 * Spark's own operator also lets the first row of the peer group past the limit through (it checks the
 * previous row's rank); both are pre-filters under the window that computes the real ranks, so the
 * tighter set is exact for the query and smaller for the shuffle.
 *
 * Output rows are compacted (a top-k keeps a few rows per partition); a batch with every row kept is
 * forwarded, one with none is dropped.
 */
case class VectorWindowGroupLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan
) extends VectorExec {

  override def output: Seq[Attribute] = child.output

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final => if (partitionSpec.isEmpty) AllTuples :: Nil else ClusteredDistribution(partitionSpec) :: Nil
  }
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def compileKey(e: Expression): VectorExpr = ExpressionCompiler.compileLaneColumn(e, child.output) match {
    case Right(v) => v
    case Left(reason) => throw new IllegalStateException(s"cannot vectorize window group limit key ${e.sql}: $reason")
  }
  @transient private lazy val partitionKeys: Array[VectorExpr] =
    partitionSpec.map(compileKey).filterNot(_.isInstanceOf[LiteralExpr]).toArray
  @transient private lazy val orderKeys: Array[VectorExpr] =
    orderSpec.map(o => compileKey(o.child)).filterNot(_.isInstanceOf[LiteralExpr]).toArray
  @transient private lazy val kind: Int = VectorWindowPlanner.rankLikeKind(rankLikeFunction).getOrElse(
    throw new IllegalStateException(s"cannot vectorize window group limit function ${rankLikeFunction.sql}")
  )

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val pk = partitionKeys
    val ok = orderKeys
    val k = kind
    val lim = limit
    val childAttrs = child.output.map(a => (a.name, a.dataType)).toArray
    val m = vectorMetrics
    child.executeColumnar().mapPartitionsInternal { iter =>
      new VectorWindowGroupLimitIterator(iter, pk, ok, k, lim, childAttrs, m)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  override def verboseStringWithOperatorId(): String = {
    s"""$formattedNodeName
       |Function: ${rankLikeFunction.sql} <= $limit ($mode)
       |Partition: ${partitionSpec.map(_.sql).mkString(", ")}
       |Order: ${orderSpec.map(_.sql).mkString(", ")}
       |Output: ${output.map(_.name).mkString(", ")}
       |""".stripMargin
  }
}

object VectorWindowGroupLimitPlanner {
  def plan(g: WindowGroupLimitExec): Either[String, VectorWindowGroupLimitExec] = {
    VectorWindowPlanner.rankLikeKind(g.rankLikeFunction) match {
      case Left(reason) => Left(reason)
      case Right(_) =>
        val keys = g.partitionSpec ++ g.orderSpec.map(_.child)
        val keyFailures = keys.flatMap { k =>
          if (k.dataType == DoubleType) Some(
            s"window key ${k.sql}: double keys not supported (Spark compares them after NaN and zero normalisation)"
          )
          else if (!TypeMapping.hasLane(k.dataType)) Some(s"window key type ${k.dataType.simpleString} not supported")
          else
            ExpressionCompiler.compileLaneColumn(k, g.child.output).left.toOption.map(r => s"window key ${k.sql}: $r")
        }
        keyFailures.headOption.toLeft(VectorWindowGroupLimitExec(
          g.partitionSpec,
          g.orderSpec,
          g.rankLikeFunction,
          g.limit,
          g.mode,
          g.child
        ))
    }
  }
}

/** The ranking walk with a filter: rows whose ranking value is at most `limit` survive. */
private[vecruntime] class VectorWindowGroupLimitIterator(
    input: Iterator[ColumnarBatch],
    partitionKeys: Array[VectorExpr],
    orderKeys: Array[VectorExpr],
    kind: Int,
    limit: Int,
    childAttrs: Array[(String, DataType)],
    metrics: VectorMetrics
) extends VectorBatchIterator(input, "VectorWindowGroupLimitExec") {

  private val partition = new KeyTracker(partitionKeys)
  private val order = new KeyTracker(orderKeys)
  private var rowNumber = 0L
  private var rank = 0L
  private var denseRank = 0L

  override protected def process(batch: ColumnarBatch): ColumnarBatch = metrics.timed {
    metrics.numInputBatches += 1
    withEvalContext(batch) { ctx =>
      val n = ctx.numRows
      val live = ctx.selection
      partition.startBatch(ctx)
      order.startBatch(ctx)
      val kept = Bitmap.allocate(ctx.arena, n)
      var keptCount = 0
      var i = 0
      while (i < n) {
        if (live == null || Bitmap.isSet(live, i)) {
          val newPartition = partition.changed(i)
          if (newPartition) {
            rowNumber = 0L
            rank = 0L
            denseRank = 0L
          }
          rowNumber += 1
          val peer = !newPartition && !order.changed(i)
          if (!peer) {
            rank = rowNumber
            denseRank += 1
          }
          partition.remember(i)
          order.remember(i)
          val value = kind match {
            case VectorWindowPlanner.RowNumberKind => rowNumber
            case VectorWindowPlanner.RankKind => rank
            case _ => denseRank
          }
          if (value <= limit) { Bitmap.set(kept, i); keptCount += 1 }
        }
        i += 1
      }
      if (keptCount == 0) {
        null
      } else if (live == null && keptCount == n) {
        metrics.numOutputBatches += 1
        metrics.numOutputRows += n
        batch
      } else {
        val columns = new Array[ColumnVector](childAttrs.length)
        var c = 0
        while (c < childAttrs.length) {
          val (name, dt) = childAttrs(c)
          columns(c) = ArrowOutput.compact(name, dt, ctx.input(c), kept, keptCount, allocator)
          c += 1
        }
        metrics.numOutputBatches += 1
        metrics.numOutputRows += keptCount
        new ColumnarBatch(columns, keptCount)
      }
    }
  }
}

/**
 * Offset functions over the held partition: `lag` / `lead` (a row shifted by a literal offset, the default
 * outside the partition), `first_value` (the partition's first row), `last_value` (the last row of the
 * frame: the partition, the current row for `ROWS ... CURRENT ROW`, the end of the peer group for the
 * `RANGE` default) and `nth_value` (the frame's n-th row, null when the frame is shorter). Rows are held
 * as copies until their partition has ended; a released batch stays readable (its columns are forwarded
 * borrowed) until no later batch can still address a row of one of its partitions, since `lag` reads
 * backwards across batch boundaries.
 */
private[vecruntime] class VectorWindowOffsetIterator(
    input: Iterator[ColumnarBatch],
    partitionKeys: Array[VectorExpr],
    orderKeys: Array[VectorExpr],
    functions: Array[VectorWindowPlanner.OffsetFunction],
    childAttrs: Array[(String, DataType)],
    windowAttrs: Array[(String, DataType)],
    metrics: VectorMetrics
) extends Iterator[ColumnarBatch] with AutoCloseable {

  /**
   * A held batch: owned copies of the live rows, each row's partition ordinal, position in it, and peer
   * ordinal; under a `RANGE` frame with value offsets also each row's order key as a long (`keys`, null
   * rows flagged in the `keyValidity` bitmap words, null when every key is valid).
   */
  private final class Held(
      val columns: Array[ColumnVector],
      val numRows: Int,
      val firstGlobal: Long,
      val partOf: Array[Int],
      val posOf: Array[Int],
      val peerOf: Array[Int],
      val keys: Array[Long],
      val keyValidity: Array[Long]
  ) {

    /** The consumer has moved past this batch's output (whose columns are borrowed from here). */
    var consumed = false
    private val adapted = new Array[VectorBuffers](columns.length)

    /** Column `c` as a lane (zero-copy over the held Arrow vector). */
    def buffers(c: Int): VectorBuffers = {
      if (adapted(c) == null)
        adapted(c) = io.vecruntime.spark.adapter.ColumnVectorAdapters.adapt(columns(c), numRows, scratchArena)
      adapted(c)
    }
    def close(): Unit = columns.foreach(_.close())
  }

  private val allocator: BufferAllocator = VectorAllocators.newChild("VectorWindowExec")
  // Only the adapter's fallback copy would use it, and the held columns are our own Arrow vectors.
  private val scratchArena = java.lang.foreign.Arena.ofConfined()
  private val partition = new KeyTracker(partitionKeys)
  private val order = new KeyTracker(orderKeys)
  // A `RANGE` frame with value offsets: the single order key is captured per row for the frame kernels.
  private val rangeMode = functions.exists(_.range != null)
  private var numPartitions = 0
  private var numPeers = 0
  private var posInPartition = 0
  private var globalRows = 0L
  // Filled when a partition / peer group ends: its length / its last position. Filled when they open:
  // the peer group's first position and the partition's first peer ordinal (for dense_rank), and the
  // global index of the partition's first row.
  private val partitionLength = mutable.ArrayBuffer.empty[Int]
  private val partitionFirstGlobal = mutable.ArrayBuffer.empty[Long]
  private val peerEnd = mutable.ArrayBuffer.empty[Int]
  private val peerStart = mutable.ArrayBuffer.empty[Int]
  private val partitionFirstPeer = mutable.ArrayBuffer.empty[Int]
  private val held = mutable.Queue.empty[Held]
  private val released = mutable.ArrayBuffer.empty[Held]
  // No longer addressable by any unreleased row, closed once the consumer is past their output too.
  private val retired = mutable.ArrayBuffer.empty[Held]
  private val ready = mutable.Queue.empty[(ColumnarBatch, Held)]
  private var inputDone = false
  private var current: ColumnarBatch = _
  private var currentHeld: Held = _
  private var closed = false

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  private def pruneRetired(): Unit = {
    val (done, waiting) = retired.partition(_.consumed)
    done.foreach(_.close())
    retired.clear(); retired ++= waiting
  }

  private def endPeer(): Unit = if (numPeers > 0) peerEnd += posInPartition - 1
  private def endPartition(): Unit = if (numPartitions > 0) partitionLength += posInPartition

  private def consume(batch: ColumnarBatch): Unit = metrics.timed {
    metrics.numInputBatches += 1
    EvalContexts.withBatch(batch) { ctx =>
      val n = ctx.numRows
      val live = ctx.selection
      val outRows = if (live == null) n else ctx.selectedCount
      partition.startBatch(ctx)
      order.startBatch(ctx)
      val partOf = new Array[Int](outRows)
      val posOf = new Array[Int](outRows)
      val peerOf = new Array[Int](outRows)
      val keys = if (rangeMode) new Array[Long](outRows) else null
      val keyValidity = if (rangeMode) new Array[Long]((outRows + 63) >>> 6) else null
      val keyLane = if (rangeMode) order.lane(0) else null
      val keyIsInt = rangeMode && keyLane.`type`() == io.vecruntime.kernels.VecType.INT32
      var anyNullKey = false
      var out = 0
      var i = 0
      while (i < n) {
        if (live == null || Bitmap.isSet(live, i)) {
          val newPartition = partition.changed(i)
          if (newPartition) {
            endPeer(); endPartition()
            numPartitions += 1; numPeers += 1; posInPartition = 0
            partitionFirstPeer += numPeers - 1
            partitionFirstGlobal += globalRows + out
            peerStart += 0
          } else if (order.changed(i)) {
            endPeer(); numPeers += 1
            peerStart += posInPartition
          }
          partition.remember(i)
          order.remember(i)
          partOf(out) = numPartitions - 1
          posOf(out) = posInPartition
          peerOf(out) = numPeers - 1
          if (rangeMode) {
            if (Rows.valid(keyLane, i)) {
              keys(out) = if (keyIsInt) keyLane.getInt(i).toLong else keyLane.getLong(i)
              keyValidity(out >>> 6) |= 1L << (out & 63)
            } else anyNullKey = true
          }
          posInPartition += 1
          out += 1
        }
        i += 1
      }
      val columns = new Array[ColumnVector](childAttrs.length)
      var c = 0
      while (c < childAttrs.length) {
        val (name, dt) = childAttrs(c)
        columns(c) = if (live == null) ArrowOutput.copy(name, dt, ctx.input(c), allocator)
        else ArrowOutput.compact(name, dt, ctx.input(c), live, outRows, allocator)
        c += 1
      }
      held.enqueue(new Held(
        columns,
        outRows,
        globalRows,
        partOf,
        posOf,
        peerOf,
        keys,
        if (anyNullKey) keyValidity else null
      ))
      globalRows += outRows
    }
  }

  /** The held batch (released or not) holding global row `g`. */
  private def batchOf(g: Long): Held = {
    var i = released.length - 1
    while (i >= 0) { val h = released(i); if (g >= h.firstGlobal && g < h.firstGlobal + h.numRows) return h; i -= 1 }
    val it = held.iterator
    while (it.hasNext) { val h = it.next(); if (g >= h.firstGlobal && g < h.firstGlobal + h.numRows) return h }
    throw new IllegalStateException(s"row $g is no longer held")
  }

  /** Spark's internal representation of column `c` at row `r` of `h`, or null; decimals as their unscaled long. */
  private def valueAt(h: Held, c: Int, r: Int, dt: DataType): Any = {
    val col = h.columns(c)
    if (col.isNullAt(r)) null
    else dt match {
      case IntegerType | DateType => java.lang.Integer.valueOf(col.getInt(r))
      case ByteType => java.lang.Byte.valueOf(col.getByte(r)) // #327: the int lane read back as the declared type
      case ShortType => java.lang.Short.valueOf(col.getShort(r))
      case LongType | TimestampType => java.lang.Long.valueOf(col.getLong(r))
      case DoubleType => java.lang.Double.valueOf(col.getDouble(r))
      case BooleanType => java.lang.Boolean.valueOf(col.getBoolean(r))
      case StringType => col.getUTF8String(r).clone()
      case d: SparkDecimalType if d.precision > TypeMapping.MAX_DECIMAL_PRECISION =>
        col.getDecimal(
          r,
          d.precision,
          d.scale
        ).toJavaBigDecimal // a wide value, boxed as the DECIMAL128 column builder reads it (#259)
      case d: SparkDecimalType => java.lang.Long.valueOf(col.getDecimal(r, d.precision, d.scale).toUnscaledLong)
      case other => throw new IllegalStateException(s"offset window over $other")
    }
  }

  /**
   * The position-based ranking functions of row `r` (Spark's definitions): `rank` is the peer group's first
   * position + 1, `dense_rank` the peer ordinal within the partition + 1, `percent_rank` (rank - 1) / (n - 1)
   * (0 for a single row), `cume_dist` rows up to the peer group's end over n, `ntile` Spark's bucket walk --
   * the first `n % buckets` buckets hold one row more than `n / buckets`.
   */
  private def rankingValue(fn: VectorWindowPlanner.OffsetFunction, h: Held, r: Int, pos: Int, len: Int): Any = {
    val peer = h.peerOf(r)
    fn.kind match {
      case VectorWindowPlanner.RowNumberAt => java.lang.Integer.valueOf(pos + 1)
      case VectorWindowPlanner.RankAt => java.lang.Integer.valueOf(peerStart(peer) + 1)
      case VectorWindowPlanner.DenseRankAt => java.lang.Integer.valueOf(peer - partitionFirstPeer(h.partOf(r)) + 1)
      case VectorWindowPlanner.PercentRankAt =>
        java.lang.Double.valueOf(if (len > 1) peerStart(peer).toDouble / (len - 1).toDouble else 0.0)
      case VectorWindowPlanner.CumeDistAt => java.lang.Double.valueOf((peerEnd(peer) + 1).toDouble / len.toDouble)
      case _ =>
        val buckets = fn.offset
        val size = len / buckets
        val padding = len % buckets
        val padded = padding * (size + 1)
        java.lang.Integer.valueOf(if (pos < padded) pos / (size + 1) + 1 else (pos - padded) / size + padding + 1)
    }
  }

  /** Running aggregate over the rows of one frame, fed in row order (Spark re-aggregates a frame the same way). */
  private abstract class SlidingState { def reset(): Unit; def add(v: Any): Unit; def result: Any }
  private def newState(fn: VectorWindowPlanner.OffsetFunction): SlidingState = fn.kind match {
    case VectorWindowPlanner.SlidingSum if fn.dataType == LongType =>
      new SlidingState {
        var sum = 0L; var count = 0L
        def reset(): Unit = { sum = 0L; count = 0L }
        def add(v: Any): Unit = if (v != null) {
          val x = v match { case l: java.lang.Long => l.longValue(); case i: java.lang.Integer => i.longValue() }
          if (fn.checked) {
            try sum = Math.addExact(sum, x)
            catch {
              case _: ArithmeticException =>
                throw VectorErrors.arithmeticOverflow("long overflow", "try_sum", fn.context)
            }
          } else sum += x
          count += 1
        }
        def result: Any = if (count == 0) null else java.lang.Long.valueOf(sum)
      }
    case VectorWindowPlanner.SlidingSum => new SlidingState {
        var sum = 0.0; var count = 0L
        def reset(): Unit = { sum = 0.0; count = 0L }
        def add(v: Any): Unit = if (v != null) { sum += v.asInstanceOf[java.lang.Double].doubleValue(); count += 1 }
        def result: Any = if (count == 0) null else java.lang.Double.valueOf(sum)
      }
    case VectorWindowPlanner.SlidingCount => new SlidingState {
        var n = 0L
        def reset(): Unit = n = 0L
        def add(v: Any): Unit = if (fn.countAll || v != null) n += 1
        def result: Any = java.lang.Long.valueOf(n)
      }
    case VectorWindowPlanner.SlidingAvg => new SlidingState {
        var sum = 0.0; var count = 0L
        def reset(): Unit = { sum = 0.0; count = 0L }
        def add(v: Any): Unit = if (v != null) { sum += v.asInstanceOf[Number].doubleValue(); count += 1 }
        def result: Any = if (count == 0) null else java.lang.Double.valueOf(sum / count)
      }
    case k => new SlidingState {
        val isMin = k == VectorWindowPlanner.SlidingMin
        var current: Any = null
        def reset(): Unit = current = null
        def add(v: Any): Unit = if (v != null) {
          if (current == null) current = v
          else { val cmp = current.asInstanceOf[Comparable[Any]].compareTo(v); if ((cmp > 0) == isMin) current = v }
        }
        def result: Any = current
      }
  }

  /** Reads the function's input at partition-relative position `target` of the partition row `r` of `h` belongs to. */
  private def inputAt(fn: VectorWindowPlanner.OffsetFunction, h: Held, r: Int, pos: Int, target: Int): Any = {
    if (fn.inputOrdinal < 0) java.lang.Boolean.TRUE
    else {
      val g = h.firstGlobal + r + (target - pos)
      val src = if (g >= h.firstGlobal && g < h.firstGlobal + h.numRows) h else batchOf(g)
      valueAt(
        src,
        fn.inputOrdinal,
        (g - src.firstGlobal).toInt,
        if (fn.inputType != null) fn.inputType else fn.dataType
      )
    }
  }

  // Per function: the running state of an UNBOUNDED PRECEDING frame for the partition being released
  // (rows are added as the upper bound advances, in order, exactly Spark's UnboundedPrecedingWindowFunctionFrame).
  private val runningStates = new Array[SlidingState](functions.length)
  private val runningPart = Array.fill(functions.length)(-1)
  private val runningEnd = new Array[Int](functions.length)

  /** The sliding aggregate of row `r`: its frame clamped to the partition, re-aggregated in order (or advanced, when the frame starts at the partition). */
  private def slidingValue(f: Int, fn: VectorWindowPlanner.OffsetFunction, h: Held, r: Int, pos: Int, len: Int): Any = {
    val part = h.partOf(r)
    val hi = fn.frameHi match {
      case VectorWindowPlanner.UnboundedHi => len - 1
      case VectorWindowPlanner.PeerEndHi => peerEnd(h.peerOf(r))
      case d => math.min(len - 1, pos + d)
    }
    if (fn.frameLo == VectorWindowPlanner.UnboundedLo) {
      if (runningStates(f) == null) runningStates(f) = newState(fn)
      val st = runningStates(f)
      if (runningPart(f) != part) { st.reset(); runningPart(f) = part; runningEnd(f) = -1 }
      while (runningEnd(f) < hi) { runningEnd(f) += 1; st.add(inputAt(fn, h, r, pos, runningEnd(f))) }
      st.result
    } else {
      val lo = fn.frameLo match {
        case VectorWindowPlanner.PeerStartLo => peerStart(h.peerOf(r))
        case d => math.max(0, pos + d)
      }
      val st = newState(fn)
      var t = lo
      while (t <= hi) { st.add(inputAt(fn, h, r, pos, t)); t += 1 }
      st.result
    }
  }

  /**
   * A `RANGE` frame with value offsets (the kernels of `WindowFrameKernels`): the partition's order keys
   * and the function's input are gathered from the held batches into primitive arrays reused across
   * partitions, the two-pointer walk yields every row's frame, and the aggregate is computed for the
   * whole partition at once; the results are cached until the partition's last batch has been written.
   * No per-row objects: the output column is written straight into its Arrow buffers.
   */
  private final class RangeFrames(fn: VectorWindowPlanner.OffsetFunction) {
    private val spec = fn.range
    private val inputLane: io.vecruntime.kernels.VecType =
      if (fn.inputOrdinal < 0) null
      else TypeMapping.vecTypeOf(if (fn.inputType != null) fn.inputType else fn.dataType)
    // The aggregate's lane: doubles for a double sum / min / max and every average, longs otherwise.
    private val doubles = fn.kind == VectorWindowPlanner.SlidingAvg ||
      (fn.kind != VectorWindowPlanner.SlidingCount && inputLane == io.vecruntime.kernels.VecType.FLOAT64)
    private var capacity = 0
    private var keys: Array[Long] = _
    private var keyValidity: Array[Long] = _
    private var lo: Array[Int] = _
    private var hi: Array[Int] = _
    private var longs: Array[Long] = _
    private var dbls: Array[Double] = _
    private var validity: Array[Long] = _
    private var outLongs: Array[Long] = _
    private var outDoubles: Array[Double] = _
    private var outValidity: Array[Long] = _
    private var computedPart = -1

    private def ensure(len: Int): Unit = if (len > capacity) {
      capacity = math.max(len, math.max(1024, capacity * 2))
      val words = (capacity + 63) >>> 6
      keys = new Array[Long](capacity)
      keyValidity = new Array[Long](words)
      lo = new Array[Int](capacity)
      hi = new Array[Int](capacity)
      validity = new Array[Long](words)
      outValidity = new Array[Long](words)
      if (fn.inputOrdinal >= 0) {
        if (doubles) dbls = new Array[Double](capacity) else longs = new Array[Long](capacity)
      }
      if (doubles) outDoubles = new Array[Double](capacity) else outLongs = new Array[Long](capacity)
    }

    /** Computes the partition's values into the `out*` arrays (by position in the partition). */
    private def compute(part: Int): Unit = {
      val len = partitionLength(part)
      val start = partitionFirstGlobal(part)
      ensure(len)
      java.util.Arrays.fill(keyValidity, 0, (len + 63) >>> 6, 0L)
      java.util.Arrays.fill(validity, 0, (len + 63) >>> 6, 0L)
      var anyNullKey = false
      var anyNullValue = false
      // The partition's rows, batch by batch, in global order (released batches first, then the held ones).
      val it = released.iterator ++ held.iterator
      while (it.hasNext) {
        val h = it.next()
        val from = math.max(start, h.firstGlobal)
        val to = math.min(start + len, h.firstGlobal + h.numRows)
        if (from < to) {
          val o = (from - start).toInt
          val r0 = (from - h.firstGlobal).toInt
          val count = (to - from).toInt
          System.arraycopy(h.keys, r0, keys, o, count)
          if (h.keyValidity != null) {
            anyNullKey = true
            var j = 0
            while (j < count) {
              if (((h.keyValidity((r0 + j) >>> 6) >>> ((r0 + j) & 63)) & 1L) != 0L)
                keyValidity((o + j) >>> 6) |= 1L << ((o + j) & 63)
              j += 1
            }
          } else {
            var j = 0
            while (j < count) { keyValidity((o + j) >>> 6) |= 1L << ((o + j) & 63); j += 1 }
          }
          if (fn.inputOrdinal >= 0) {
            val v = h.buffers(fn.inputOrdinal)
            var j = 0
            while (j < count) {
              val r = r0 + j
              if (Rows.valid(v, r)) {
                validity((o + j) >>> 6) |= 1L << ((o + j) & 63)
                if (fn.kind != VectorWindowPlanner.SlidingCount) {
                  inputLane match {
                    case io.vecruntime.kernels.VecType.INT32 =>
                      if (doubles) dbls(o + j) = v.getInt(r).toDouble else longs(o + j) = v.getInt(r).toLong
                    case io.vecruntime.kernels.VecType.INT64 =>
                      if (doubles) dbls(o + j) = v.getLong(r).toDouble else longs(o + j) = v.getLong(r)
                    case _ => dbls(o + j) = v.getDouble(r)
                  }
                }
              } else anyNullValue = true
              j += 1
            }
          }
        }
      }
      val kv = if (anyNullKey) keyValidity else null
      val vv = if (anyNullValue) validity else null
      try {
        io.vecruntime.kernels.WindowFrameKernels.rangeBounds(
          keys,
          kv,
          len,
          spec.descending,
          spec.nullsFirst,
          spec.loUnbounded,
          spec.lo,
          spec.hiUnbounded,
          spec.hi,
          spec.keyBits,
          spec.checked,
          lo,
          hi
        )
      } catch {
        case _: ArithmeticException =>
          throw VectorErrors.arithmeticOverflow(
            if (spec.keyBits == 64) "long overflow" else "integer overflow",
            "try_add",
            fn.context
          )
      }
      fn.kind match {
        case VectorWindowPlanner.SlidingCount =>
          io.vecruntime.kernels.WindowFrameKernels.frameCount(vv, lo, hi, len, fn.countAll, outLongs)
          java.util.Arrays.fill(outValidity, 0, (len + 63) >>> 6, -1L)
        case VectorWindowPlanner.SlidingSum if !doubles =>
          try io.vecruntime.kernels.WindowFrameKernels.frameSumLong(
              longs,
              vv,
              lo,
              hi,
              len,
              fn.checked,
              outLongs,
              outValidity
            )
          catch {
            case _: ArithmeticException => throw VectorErrors.arithmeticOverflow("long overflow", "try_sum", fn.context)
          }
        case VectorWindowPlanner.SlidingSum | VectorWindowPlanner.SlidingAvg =>
          io.vecruntime.kernels.WindowFrameKernels.frameSumDouble(
            dbls,
            vv,
            lo,
            hi,
            len,
            fn.kind == VectorWindowPlanner.SlidingAvg,
            outDoubles,
            outValidity
          )
        case k =>
          val isMin = k == VectorWindowPlanner.SlidingMin
          if (doubles) io.vecruntime.kernels.WindowFrameKernels.frameMinMaxDouble(
            dbls,
            vv,
            lo,
            hi,
            len,
            isMin,
            outDoubles,
            outValidity
          )
          else io.vecruntime.kernels.WindowFrameKernels.frameMinMaxLong(
            longs,
            vv,
            lo,
            hi,
            len,
            isMin,
            outLongs,
            outValidity
          )
      }
      computedPart = part
    }

    /** The function's column over held batch `h`, computing each of its partitions once. */
    def column(h: Held, name: String, dt: DataType): ColumnVector = {
      val out = ArrowOutput.allocateFixed(name, dt, h.numRows, allocator)
      val data = out.data()
      val validityOut = out.validity()
      val outLane = TypeMapping.vecTypeOf(dt)
      var allValid = true
      var r = 0
      while (r < h.numRows) {
        val part = h.partOf(r)
        if (computedPart != part) compute(part)
        val pos = h.posOf(r)
        val valid = ((outValidity(pos >>> 6) >>> (pos & 63)) & 1L) != 0L
        Bitmap.setTo(validityOut, r, valid)
        if (!valid) allValid = false
        outLane match {
          case io.vecruntime.kernels.VecType.INT32 =>
            data.setAtIndex(VectorBuffers.LE_INT, r, if (valid) outLongs(pos).toInt else 0)
          case io.vecruntime.kernels.VecType.INT64 =>
            data.setAtIndex(VectorBuffers.LE_LONG, r, if (valid) outLongs(pos) else 0L)
          case _ => data.setAtIndex(VectorBuffers.LE_DOUBLE, r, if (valid) outDoubles(pos) else 0.0)
        }
        r += 1
      }
      ArrowOutput.finish(out, h.numRows, allValid)
    }
  }

  private val rangeFrames: Array[RangeFrames] =
    functions.map(fn => if (fn.range != null) new RangeFrames(fn) else null)

  /** Emits every held batch whose partitions have all ended; keeps released batches readable while needed. */
  private def release(): Unit = metrics.timed {
    while (held.nonEmpty && (inputDone || held.head.partOf(held.head.numRows - 1) < numPartitions - 1)) {
      val h = held.dequeue()
      // Rows of partitions before this batch's first one can no longer be addressed by anything unreleased.
      val firstPart = h.partOf(0)
      val (dead, alive) = released.partition(e => e.partOf(e.numRows - 1) < firstPart)
      retired ++= dead
      released.clear(); released ++= alive; released += h
      pruneRetired()
      val columns = new Array[ColumnVector](childAttrs.length + functions.length)
      var c = 0
      while (c < childAttrs.length) { columns(c) = BorrowedColumnVector.of(h.columns(c)); c += 1 }
      var f = 0
      while (f < functions.length) {
        val fn = functions(f)
        val (name, dt) = windowAttrs(f)
        columns(childAttrs.length + f) = if (rangeFrames(f) != null) rangeFrames(f).column(h, name, dt)
        else AggBufferColumns.values(
          name,
          dt,
          h.numRows,
          { r =>
            val part = h.partOf(r); val pos = h.posOf(r); val len = partitionLength(part)
            val frameEnd = fn.frame match {
              case VectorWindowPlanner.WholePartition => len - 1
              case VectorWindowPlanner.RunningRows => pos
              case _ => peerEnd(h.peerOf(r))
            }
            if (fn.kind >= VectorWindowPlanner.SlidingSum) slidingValue(f, fn, h, r, pos, len)
            else if (fn.kind >= VectorWindowPlanner.RowNumberAt) rankingValue(fn, h, r, pos, len)
            else {
              val target: Int = fn.kind match {
                case VectorWindowPlanner.Shift => pos + fn.offset
                case VectorWindowPlanner.FirstValue => 0
                case VectorWindowPlanner.LastValue => frameEnd
                case _ => if (fn.offset - 1 <= frameEnd) fn.offset - 1 else -1
              }
              if (target < 0 || target >= len) { if (fn.kind == VectorWindowPlanner.Shift) fn.default else null }
              else {
                val g = h.firstGlobal + r + (target - pos)
                val src = if (g >= h.firstGlobal && g < h.firstGlobal + h.numRows) h else batchOf(g)
                valueAt(src, fn.inputOrdinal, (g - src.firstGlobal).toInt, fn.dataType)
              }
            }
          },
          allocator
        )
        f += 1
      }
      metrics.numOutputBatches += 1
      metrics.numOutputRows += h.numRows
      ready.enqueue((new ColumnarBatch(columns, h.numRows), h))
    }
  }

  override def hasNext: Boolean = {
    while (ready.isEmpty && !inputDone) {
      if (input.hasNext) {
        val batch = input.next()
        if (batch.numRows() > 0) consume(batch)
      } else {
        inputDone = true
        endPeer(); endPartition()
      }
      release()
    }
    ready.nonEmpty
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    releaseCurrent()
    val (batch, h) = ready.dequeue()
    current = batch
    currentHeld = h
    current
  }

  private def releaseCurrent(): Unit = if (current != null) {
    current.close()
    current = null
    currentHeld.consumed = true
    currentHeld = null
    pruneRetired()
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      if (current != null) { current.close(); current = null }
      ready.foreach(_._1.close()); ready.clear()
      held.foreach(_.close()); held.clear()
      released.foreach(_.close()); released.clear()
      retired.foreach(_.close()); retired.clear()
      scratchArena.close()
      allocator.close()
    }
  }
}
