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
package org.apache.spark.sql.vecruntime

import io.vecruntime.spark.VectorConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  Attribute,
  AttributeSet,
  Cast,
  Coalesce,
  EqualTo,
  Expression,
  If,
  IsNull,
  Literal,
  Multiply,
  NamedExpression,
  Or,
  PredicateHelper
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  BROADCAST,
  Filter,
  HintInfo,
  Join,
  LogicalPlan,
  Project,
  RepartitionByExpression
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.{DecimalType, DoubleType, LongType}

/**
 * Collapses a join input that only supplies grouping columns into its distinct rows and their count (#692), so a
 * join whose output explodes on a few keys does not.
 *
 * Shape: `Aggregate(G, aggs, path)` where `path` runs through deterministic projections, filters and inner
 * equi-joins, every aggregate function is a non-distinct, unfiltered `sum`, `count`, `min` or `max`, and one input
 * `S` of a join on the path
 * - is a subtree without joins or aggregates (a table as it is read, filtered and projected),
 * - holds no aggregate input: the plan above uses its columns only as values -- grouping columns, filter and join
 *   columns -- never as something summed or counted,
 * - and is too large to be broadcast (a broadcast side is replicated whole anyway; the rule would add a shuffle).
 *
 * Rewrite: `S` becomes `Aggregate(K, K ++ count(*), RepartitionByExpression(joinKeys, Project(K, S)))`, with `K`
 * the columns of `S` the plan above needs plus its join keys (a key expression such as `upper(ca_country)` becomes
 * a column of the projection), and the top aggregate weighs each row by the count: `sum(x)` becomes
 * `sum(x * count)`, `count(...)` the sum of the counts of the rows it would count, `min` and `max` are unchanged.
 * TPC-DS q24a and q24b join `customer_address` on `(upper(ca_country), ca_zip)` alone and use only `ca_state`
 * above: at 3 TB one task of the join produced 367 M rows from 0.9 M input rows for the aggregate above to fold
 * back to 120 k (40.7 s; the median task 0.44 s). Each sales row met every address of its zip.
 *
 * Why the result is the same: an inner join pairs a row of the other side with each row of `S` whose keys match;
 * rows of `S` equal on `K` give pairs that agree on every column the plan above reads, so the plan above sees each
 * such pair `count` times either way, and `sum`, `count`, `min` and `max` over repeated rows are what the weighted
 * forms compute. A row with a null key forms its own group, which the inner join drops as it drops each of its
 * rows. A floating-point sum may differ in the last digits, as any reordering of a sum does.
 *
 * Why it costs no exchange: the join needs `S` partitioned by its join keys, which are columns of `K`, so the
 * repartition below the aggregate is the exchange the join would have added and every group is whole within one
 * task: the aggregate is a local one ([[LocalPreAggregate]]), planned with no exchange of its own, and given up
 * once a task's first rows show its keys do not repeat (a side joined on its unique key).
 *
 * With statistics (`ANALYZE ... FOR COLUMNS`, or a DSv2 source's column statistics) the rule is cheaper. They bound
 * the groups by the product of the distinct counts of the columns `K` is computed from:
 * - a side whose bound exceeds `1 / minReduction` of its rows (default 2: half) is left alone, so the rows a task
 *   would spend finding that out are not spent;
 * - a side whose bound proves the reduction is aggregated without that judgement, which a task whose first rows
 *   happen not to repeat would otherwise fail;
 * - a side small enough once collapsed to be broadcast (bound x row size within `autoBroadcastJoinThreshold`) is
 *   aggregated as usual, by its own keys, and broadcast to the join, so the other, large side is joined where it
 *   is read with no exchange at all.
 *
 * Runs in the session's last optimizer batch ([[RegisterLateOptimizerRules]]), once per aggregate ([[Tag]]). Off
 * with `spark.vecruntime.optimizer.countBelowJoin.enabled=false` and with the plugin.
 */
case class CountBelowJoin(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!VectorConf.countBelowJoinEnabled(session.sessionState.conf)) return plan
    plan.transformDown {
      case a: Aggregate if a.getTagValue(CountBelowJoin.Tag).isEmpty && !LocalPreAggregate.isLocal(a) =>
        val out = rewrite(a).getOrElse(a)
        out.setTagValue(CountBelowJoin.Tag, ())
        out
    }
  }

  private def supported(ae: AggregateExpression): Boolean =
    !ae.isDistinct && ae.filter.isEmpty && ae.mode == Complete &&
      (ae.aggregateFunction match {
        case s: Sum => weightable(s)
        case _: Count | _: Min | _: Max => true
        case _ => false
      })

  /**
   * A sum that can be weighted in its own result type. A long or a double: x * count in that type. A decimal
   * of up to 8 digits already reaches here as a long sum of the unscaled values (Spark's `DecimalAggregates`), so
   * q24's `sum(ss_net_paid)` is a long. A wider decimal (#762) is weighted as `x * cast(count as decimal(20,0))`,
   * a product our aggregate multiplies on the 128-bit lane, summed and cast back to the sum's own type -- but only
   * when that product cannot overflow: `x`'s precision plus the count's 20 digits and the product's carry digit
   * must fit 38. Past that, Spark would lower the product's scale (rounding every weighted row) or, without ANSI,
   * null an overflowing product the sum would then skip, where the unweighted sum overflows to null. q23's
   * `decimal(17,2)` fits exactly. A wider decimal sum is left alone.
   */
  private def weightable(s: Sum): Boolean = s.dataType match {
    case LongType | DoubleType => true
    case _: DecimalType => s.child.dataType match {
        case d: DecimalType => d.precision + CountDecimal.precision + 1 <= DecimalType.MAX_PRECISION
        case _ => false
      }
    case _ => false
  }

  /** The count as a decimal: a long's 19 digits and its sign fit decimal(20,0). */
  private val CountDecimal = DecimalType(20, 0)

  private def rewrite(a: Aggregate): Option[LogicalPlan] = {
    val aggExprs = a.aggregateExpressions.flatMap(_.collect { case ae: AggregateExpression => ae })
    if (aggExprs.isEmpty || !aggExprs.forall(supported)) return None
    if (!a.groupingExpressions.forall(_.deterministic)) return None
    val inputs = AttributeSet(aggExprs.flatMap(_.references))
    val needed = AttributeSet(a.groupingExpressions.flatMap(_.references)) ++
      AttributeSet(a.aggregateExpressions.flatMap(_.references))
    val (newChild, counts) = descend(a.child, inputs, needed)
    if (counts.isEmpty) return None
    val weight: Expression = counts.map(c => c: Expression).reduce(Multiply(_, _))
    val newAggs = a.aggregateExpressions.map(_.transformDown {
      case ae: AggregateExpression if aggExprs.exists(_ eq ae) => weighted(ae, weight)
    }.asInstanceOf[NamedExpression])
    Some(a.copy(aggregateExpressions = newAggs, child = newChild))
  }

  /** The aggregate over rows that each stand for `weight` rows of the original input. */
  private def weighted(ae: AggregateExpression, weight: Expression): Expression = ae.aggregateFunction match {
    case s: Sum if s.dataType.isInstanceOf[DecimalType] =>
      // A wide decimal (see `weightable`): x * cast(count as decimal(20,0)) at x's scale, summed in 38 digits and
      // cast back to the sum's own type, which raises (ANSI) or nulls past its precision as the sum itself would.
      val product = Multiply(s.child, Cast(weight, CountDecimal))
      Cast(s.copy(child = product).toAggregateExpression(), s.dataType)
    case s: Sum =>
      // A long or a double (see `weightable`): x * count in the sum's own type.
      val t = s.dataType
      s.copy(child = Multiply(Cast(s.child, t), Cast(weight, t))).toAggregateExpression()
    case c: Count =>
      // count(*) is count(1); count(x, y) counts the rows where none is null.
      val nullable = c.children.filter(_.nullable)
      val w: Expression =
        if (nullable.isEmpty) weight
        else If(nullable.map(e => IsNull(e): Expression).reduce(Or(_, _)), Literal(0L), weight)
      Coalesce(Seq(Sum(w).toAggregateExpression(), Literal(0L))) // a global aggregate of no row counts 0
    case _: Min | _: Max => ae
  }

  /**
   * Walks down `p`; returns it with every collapsible join input collapsed, and the count columns it added (each
   * carried up to the top aggregate). `needed` is what the plan above reads from `p`; `inputs` the aggregate inputs.
   */
  private def descend(p: LogicalPlan, inputs: AttributeSet, needed: AttributeSet): (LogicalPlan, Seq[Attribute]) =
    p match {
      case Project(list, child) if list.forall(_.deterministic) =>
        // An aggregate input computed here is computed from the columns it references.
        val childInputs = AttributeSet(list.flatMap {
          case al: Alias if inputs.contains(al.toAttribute) => al.references.toSeq
          case e if inputs.contains(e.toAttribute) => e.references.toSeq
          case _ => Nil
        })
        val (c, counts) = descend(child, childInputs, AttributeSet(list.flatMap(_.references)))
        if (counts.isEmpty) (p, Nil) else (Project(list ++ counts, c), counts)
      case Filter(cond, child) if cond.deterministic =>
        val (c, counts) = descend(child, inputs, needed ++ cond.references)
        if (counts.isEmpty) (p, Nil) else (Filter(cond, c), counts)
      case j @ Join(l, r, Inner, Some(cond), _) if cond.deterministic =>
        val conjuncts = splitConjunctivePredicates(cond)
        val above = needed ++ cond.references
        val (nl, cl, condL, bl) = side(l, r, conjuncts, inputs, needed, above)
        val (nr, cr, condR, br) = side(r, nl, condL, inputs, needed, above)
        if (cl.isEmpty && cr.isEmpty) (p, Nil)
        else {
          // One side at most: a join broadcasts one of its inputs.
          val broadcast = Some(HintInfo(strategy = Some(BROADCAST)))
          val hint =
            if (bl) j.hint.copy(leftHint = broadcast) else if (br) j.hint.copy(rightHint = broadcast) else j.hint
          (j.copy(left = nl, right = nr, condition = Some(condR.reduce(And)), hint = hint), cl ++ cr)
        }
      case _ => (p, Nil)
    }

  /**
   * One input `s` of a join whose other input is `o`: collapsed when it qualifies, else descended into. Returns the
   * new input, its count columns, the join's conjuncts with `s`'s key expressions replaced by their columns, and
   * whether the collapsed input is to be broadcast.
   */
  private def side(
      s: LogicalPlan,
      o: LogicalPlan,
      conjuncts: Seq[Expression],
      inputs: AttributeSet,
      neededAboveJoin: AttributeSet,
      above: AttributeSet
  ): (LogicalPlan, Seq[Attribute], Seq[Expression], Boolean) = {
    // The equalities pairing an expression of `s` with one of the other side, in the join's order.
    val keys = conjuncts.collect {
      case EqualTo(x, y) if refsOnly(x, s) && refsOnly(y, o) => x
      case EqualTo(x, y) if refsOnly(y, s) && refsOnly(x, o) => y
    }
    // The other side broadcast, `s` is streamed with no exchange: a repartition would add one.
    if (collapsible(s, inputs, keys) && !broadcastable(o)) collapse(s, conjuncts, keys, neededAboveJoin)
    else {
      val (c, counts) = descend(s, inputs, above)
      (c, counts, conjuncts, false)
    }
  }

  private def refsOnly(e: Expression, p: LogicalPlan): Boolean =
    e.references.nonEmpty && e.references.subsetOf(p.outputSet) && e.deterministic

  private def collapsible(s: LogicalPlan, inputs: AttributeSet, keys: Seq[Expression]): Boolean =
    keys.nonEmpty && s.outputSet.intersect(inputs).isEmpty &&
      !s.exists(n => n.isInstanceOf[Join] || n.isInstanceOf[Aggregate]) &&
      s.expressions.forall(_.deterministic) && s.subqueries.isEmpty && !broadcastable(s)

  /** Small enough that Spark would broadcast it: replicated whole, so a repartition would only add a shuffle. */
  private def broadcastable(s: LogicalPlan): Boolean = {
    val threshold = session.sessionState.conf.autoBroadcastJoinThreshold
    threshold >= 0 && s.stats.sizeInBytes <= BigInt(threshold)
  }

  private def collapse(
      s: LogicalPlan,
      conjuncts: Seq[Expression],
      keys: Seq[Expression],
      neededAboveJoin: AttributeSet
  ): (LogicalPlan, Seq[Attribute], Seq[Expression], Boolean) = {
    // Key expressions that are not columns become columns of the projection the join compares.
    val aliases = keys.distinct.collect { case k if !k.isInstanceOf[Attribute] => k -> Alias(k, "_cbj_key")() }
    def keyColumn(k: Expression): Expression =
      aliases.collectFirst { case (e, al) if e.semanticEquals(k) => al.toAttribute }.getOrElse(k)
    val newConjuncts = conjuncts.map(_.transformUp {
      case e if aliases.exists(_._1.semanticEquals(e)) => keyColumn(e)
    })
    val keyCols = keys.map(k => keyColumn(k).asInstanceOf[Attribute]).distinct
    // What the plan above the join reads from `s`, and the join's own references to it after the rewrite.
    val reads = neededAboveJoin ++ AttributeSet(newConjuncts.flatMap(_.references))
    val kept = s.output.filter(reads.contains)
    val projected = Project(kept ++ aliases.map(_._2), s)
    val group = projected.output
    val estimate = groups(s, kept ++ aliases.map(_._2))
    val minReduction = VectorConf.countBelowJoinMinReduction(session.sessionState.conf)
    estimate match {
      // Statistics show the keys cannot repeat enough (a side joined on its unique key): nothing to collapse, and
      // no rows spent finding that out at run time.
      case Some((rows, g)) if BigDecimal(g) * BigDecimal(minReduction) > BigDecimal(rows) =>
        return (s, Nil, conjuncts, false)
      case _ =>
    }
    val count = Alias(Count(Literal(1)).toAggregateExpression(), "_cbj_count")()
    val small = estimate.exists { case (_, g) =>
      val threshold = session.sessionState.conf.autoBroadcastJoinThreshold
      // Spark's row size estimate: 8 bytes of row overhead plus each column's default size, the count included.
      val rowBytes = 8 + group.map(_.dataType.defaultSize).sum + 8
      threshold >= 0 && BigInt(rowBytes) * g <= BigInt(threshold)
    }
    val agg =
      if (small) {
        // Provably small once collapsed: an ordinary aggregate (exchange on its own keys, small by then), broadcast
        // to the join, so the other side -- the large one -- is joined where it is read, with no exchange.
        Aggregate(group, group ++ Seq(count), projected)
      } else {
        val a = Aggregate(group, group ++ Seq(count), RepartitionByExpression(keyCols, projected, None))
        // One per task with no exchange of its own: the repartition already made every group whole. Statistics that
        // prove the reduction turn off the judgement on the first rows.
        a.setTagValue(LocalPreAggregate.Tag, LocalPreAggregate.Mark(provenReduction = estimate.isDefined))
        a
      }
    agg.setTagValue(CountBelowJoin.Tag, ())
    (agg, Seq(count.toAttribute), newConjuncts, small)
  }

  /**
   * `(rows, groups)` from statistics: the base relation's row count, and an upper bound of the groups -- the product
   * of the distinct counts of the columns each grouping column is computed from, at most the rows.
   */
  private def groups(s: LogicalPlan, cols: Seq[NamedExpression]): Option[(BigInt, BigInt)] = {
    val rows = KeyStats.baseRowCount(s)
    val sources = cols.flatMap(_.references).distinct
    val ndvs = sources.map(a => KeyStats.distinctCount(s, a))
    rows.filter(_ > 0).filter(_ => ndvs.forall(_.isDefined)).map(r => (r, ndvs.flatten.product.min(r)))
  }
}

object CountBelowJoin {

  /** On an aggregate this rule has looked at, and on the collapsing aggregates it adds. */
  val Tag: TreeNodeTag[Unit] = TreeNodeTag[Unit]("vecruntime.countBelowJoin")
}
