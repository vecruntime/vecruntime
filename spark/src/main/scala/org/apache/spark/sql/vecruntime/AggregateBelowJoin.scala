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
  Attribute,
  AttributeSet,
  Cast,
  Coalesce,
  DynamicPruningSubquery,
  EqualNullSafe,
  EqualTo,
  Expression,
  IsNotNull,
  Literal,
  NamedExpression,
  PredicateHelper
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Aggregates the fact side of a star join before the joins (#657), the eager aggregation EMR Serverless plans.
 *
 * Shape: `Aggregate(G, aggs, path)` where `path` runs through projections, filters and inner (or left semi,
 * the fact on the left) equi-joins down to a subtree `F`, and
 * - every aggregate function is a non-distinct, unfiltered `sum`, `count`, `min` or `max` whose inputs are
 *   columns of `F` passed up unchanged;
 * - the columns of `F` needed above `F` other than those inputs -- `P`, the keys of the pre-aggregate -- are all
 *   join keys on the path (a star aggregation: `G` groups by the other sides' columns), with at least one join.
 *
 * Rewrite: `F` becomes `Aggregate(P, P ++ partials, F)` and the top aggregate combines the partials -- `sum` of
 * the partial sums (cast to the original result type), `sum` of the partial counts, `min`/`max` of the partial
 * minima/maxima. TPC-DS q4, q11 and q74 aggregate `store_sales` (and the catalog and web facts) after joining
 * `customer` and `date_dim`, grouped by seven customer strings: at 1 TB each fact stage shuffled 6.1 GB against
 * 0.7 GB on EMR, which aggregates `(ss_customer_sk, ss_sold_date_sk)` above the scan.
 *
 * Why the result is the same: an inner join keeps or repeats an `F` row by its join keys only, and every row of
 * a pre-aggregated group has the same keys, so the group meets exactly the rows each of its rows would.
 * `sum`, `count`, `min` and `max` distribute over that regrouping (a decimal sum is exact; a floating-point sum
 * may differ in the last digits, as any reordering of a sum does). A null key forms its own group, which the
 * inner join drops as it drops each of the group's rows.
 *
 * The pre-aggregate is local (#693, [[LocalPreAggregate]]): one per task, no exchange of the fact on its keys,
 * and given up on in a task whose first rows show it does not reduce them.
 *
 * Runs in the session's last optimizer batch ([[RegisterLateOptimizerRules]]). A pre-aggregate is never
 * pre-aggregated again (its subtree is an `Aggregate`). Off with
 * `spark.vecruntime.optimizer.aggregateBelowJoin.enabled=false` and with the plugin.
 */
case class AggregateBelowJoin(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!VectorConf.aggregateBelowJoinEnabled(session.sessionState.conf)) return plan
    plan.transformDown { case a: Aggregate => rewrite(a).getOrElse(a) }
  }

  private def supported(ae: AggregateExpression): Boolean =
    !ae.isDistinct && ae.filter.isEmpty && ae.mode == Complete &&
      (ae.aggregateFunction match {
        case _: Sum | _: Count | _: Min | _: Max => true
        case _ => false
      })

  private def rewrite(a: Aggregate): Option[LogicalPlan] = {
    val aggExprs = a.aggregateExpressions.flatMap(_.collect { case ae: AggregateExpression => ae }).distinct
    if (aggExprs.isEmpty || !aggExprs.forall(supported)) return None
    val inputs = AttributeSet(aggExprs.flatMap(_.references))
    if (inputs.isEmpty) return None // only count(*): no side to choose by its inputs
    // What the aggregate needs from below other than the aggregate inputs.
    val outside = AttributeSet(a.groupingExpressions.flatMap(_.references)) ++
      AttributeSet(a.aggregateExpressions.flatMap(e => stripAggs(e).references))
    descend(a.child, aggExprs, inputs, outside, AttributeSet.empty, 0).map { case (newChild, combine, _) =>
      val newAggs = a.aggregateExpressions.map(_.transformDown {
        case ae: AggregateExpression if combine.contains(ae) => combine(ae)
      }.asInstanceOf[NamedExpression])
      a.copy(aggregateExpressions = newAggs, child = newChild)
    }
  }

  /** The expression with its aggregate expressions removed (their inputs do not count as needed above). */
  private def stripAggs(e: Expression): Expression = e.transformDown {
    case ae: AggregateExpression => Literal.create(null, ae.dataType)
  }

  /**
   * Walks down `p` to the subtree holding every aggregate input; returns the rewritten `p` and, for each top
   * aggregate expression, its combining replacement. `needed` is what the plan above needs from `p` apart from
   * the aggregate inputs; `joinKeys` the join keys met on the way.
   */
  private def descend(
      p: LogicalPlan,
      aggExprs: Seq[AggregateExpression],
      inputs: AttributeSet,
      needed: AttributeSet,
      joinKeys: AttributeSet,
      joins: Int
  ): Option[(LogicalPlan, Map[AggregateExpression, Expression], Seq[Attribute])] = p match {
    // The fact's own subtree -- its scan with its filters and projections, no join or aggregate -- is aggregated
    // as a whole, its filters inside. Descending past them put the pre-aggregate below the fact's dynamic
    // partition pruning filter: the pruning then filtered the aggregate's output instead of the scan, the copies
    // of a CTE read per year became one scan of every partition (q4 at 1 TB: 5.05 G input rows against 1.97 G,
    // #675).
    case f if joins > 0 && inputs.subsetOf(f.outputSet) && factSubtree(f) =>
      leaf(f, aggExprs, needed, joinKeys)
    case Project(list, child) if list.forall(passThrough(_, inputs)) =>
      val computed = AttributeSet(list.collect { case al: Alias => al }.flatMap(_.references))
      // Above the pre-aggregate the inputs are gone; the partials take their place in the projection.
      descend(child, aggExprs, inputs, needed ++ computed, joinKeys, joins).map { case (c, m, parts) =>
        (
          Project(
            list.filterNot(e =>
              e.isInstanceOf[Attribute] && inputs.contains(e.toAttribute) && !needed.contains(e.toAttribute)
            ) ++ parts,
            c
          ),
          m,
          parts
        )
      }
    case Filter(cond, child) =>
      descend(child, aggExprs, inputs, needed ++ cond.references, joinKeys, joins).map { case (c, m, parts) =>
        (Filter(cond, c), m, parts)
      }
    case j @ Join(l, r, jt, Some(cond), _) if jt == Inner || jt == LeftSemi =>
      // Only columns compared for equality are join keys; a column in any other conjunct (q72's
      // `inv_quantity_on_hand < cs_quantity`) is needed above but is not one, so the fact is not pre-aggregated.
      val keys = AttributeSet(splitConjunctivePredicates(cond).flatMap {
        case EqualTo(a: Attribute, b: Attribute) => Seq(a, b)
        case EqualNullSafe(a: Attribute, b: Attribute) => Seq(a, b)
        case _ => Nil
      })
      val refs = cond.references
      // A selectively filtered other side prunes the fact at run time -- the runtime filters that skip row groups
      // need the join directly over the fact's scan, and an aggregate in between would cost them -- unless the
      // fact is already pruned on this join's keys by dynamic partition pruning, which sits in the fact's own
      // subtree (q4's `date_dim` filtered by year, `ss_sold_date_sk` pruned by it).
      val (factSide, other) = if (inputs.subsetOf(l.outputSet)) (l, r) else (r, l)
      if (selective(other) && !partitionPruned(factSide, keys)) None
      else if (inputs.subsetOf(l.outputSet))
        descend(l, aggExprs, inputs, needed ++ refs, joinKeys ++ keys, joins + 1).map { case (c, m, parts) =>
          (j.copy(left = c), m, parts)
        }
      else if (jt == Inner && inputs.subsetOf(r.outputSet))
        descend(r, aggExprs, inputs, needed ++ refs, joinKeys ++ keys, joins + 1).map { case (c, m, parts) =>
          (j.copy(right = c), m, parts)
        }
      else None
    case _: Aggregate => None // already aggregated: a pre-aggregate of this rule, or the plan's own
    case f if joins > 0 && inputs.subsetOf(f.outputSet) => leaf(f, aggExprs, needed, joinKeys)
    case _ => None
  }

  /** A subtree without joins or aggregates: the fact as it is read, scanned, filtered and projected. */
  private def factSubtree(p: LogicalPlan): Boolean =
    !p.exists(n => n.isInstanceOf[Join] || n.isInstanceOf[Aggregate])

  /** The pre-aggregate over the fact subtree `f`, when its kept columns are join keys and it reduces. */
  private def leaf(
      f: LogicalPlan,
      aggExprs: Seq[AggregateExpression],
      needed: AttributeSet,
      joinKeys: AttributeSet
  ): Option[(LogicalPlan, Map[AggregateExpression, Expression], Seq[Attribute])] = {
    val keys = f.output.filter(needed.contains)
    // A star aggregation only: every fact column kept above is a join key, none is grouped by itself.
    if (keys.isEmpty || !keys.forall(joinKeys.contains) || !reduces(f, keys)) None
    else Some(preAggregate(f, keys, aggExprs, provenReduction(f, keys)))
  }

  /** Whether `p` has a filter other than IS NOT NULL checks (a selective dimension). */
  private def selective(p: LogicalPlan): Boolean = p.exists {
    case Filter(cond, _) => splitConjunctivePredicates(cond).exists(!_.isInstanceOf[IsNotNull])
    case _ => false
  }

  /**
   * Optional evidence that grouping `f` by `keys` removes rows: with `minReduction` > 0, statistics must put the
   * number of groups -- the product of the keys' distinct counts, an upper bound -- at most `1 / minReduction`
   * of the base relation's rows; with `requireStatistics`, a fact without statistics is declined. Both are off by
   * default (#675): at 1 TB they declined only queries the pre-aggregate speeds up (q4, q11, q74: 35-52 %
   * faster), since the product of distinct counts ignores how the keys correlate. The 34-104 % slowdown that
   * once justified them was lost partition pruning, which `factSubtree` fixes.
   */
  private def reduces(f: LogicalPlan, keys: Seq[Attribute]): Boolean = {
    val minReduction = VectorConf.aggregateBelowJoinMinReduction(session.sessionState.conf)
    val ndvs = keys.map(k => KeyStats.distinctCount(f, k))
    (KeyStats.baseRowCount(f), ndvs.forall(_.isDefined)) match {
      case (Some(rows), true) if rows > 0 =>
        BigDecimal(ndvs.flatten.product) * BigDecimal(minReduction) <= BigDecimal(rows)
      // No statistics to decide on (#675): decline unless the switch says the other guards are enough.
      case _ => !VectorConf.aggregateBelowJoinRequireStatistics(session.sessionState.conf)
    }
  }

  /**
   * Whether statistics prove the pre-aggregate reduces (#693): the product of the keys' distinct counts -- an
   * upper bound on the groups -- times the pass-through ratio at most the base relation's rows. Statistics can
   * prove a reduction but never disprove one (the product ignores how the keys correlate: q4's
   * `(customer, date)` is bounded at 7x its fact's rows and reduces it 60x), so an unproven pre-aggregate is
   * planned too, and judged on its first rows at run time; a proven one is only judged at its memory budget, so
   * a task whose first rows happen not to repeat a key does not give it up.
   */
  private def provenReduction(f: LogicalPlan, keys: Seq[Attribute]): Boolean = {
    val ratio = math.max(
      1.0,
      session.sessionState.conf.getConfString(
        AggSpillPolicy.PassThroughKey,
        AggSpillPolicy.DefaultPassThroughRatio.toString
      ).toDouble
    )
    val ndvs = keys.map(k => KeyStats.distinctCount(f, k))
    (KeyStats.baseRowCount(f), ndvs.forall(_.isDefined)) match {
      case (Some(rows), true) if rows > 0 => BigDecimal(ndvs.flatten.product) * BigDecimal(ratio) <= BigDecimal(rows)
      case _ => false
    }
  }

  /** Whether `p` has a dynamic partition pruning filter on one of `keys`. */
  private def partitionPruned(p: LogicalPlan, keys: AttributeSet): Boolean = p.exists {
    case Filter(cond, _) =>
      splitConjunctivePredicates(cond).exists {
        case d: DynamicPruningSubquery => d.pruningKey.references.intersect(keys).nonEmpty
        case _ => false
      }
    case _ => false
  }

  private def passThrough(e: NamedExpression, inputs: AttributeSet): Boolean = e match {
    case _: Attribute => true
    case al: Alias => al.references.intersect(inputs).isEmpty // computed, but not from an aggregate input
    case _ => false
  }

  /** `Aggregate(keys, keys ++ partials, f)` and, per top aggregate expression, how its partials combine. */
  private def preAggregate(
      f: LogicalPlan,
      keys: Seq[Attribute],
      aggExprs: Seq[AggregateExpression],
      proven: Boolean
  ): (LogicalPlan, Map[AggregateExpression, Expression], Seq[Attribute]) = {
    val parts = aggExprs.zipWithIndex.map { case (ae, i) => ae -> Alias(ae, s"_pre_agg_$i")() }
    val combine: Map[AggregateExpression, Expression] = parts.map { case (ae, al) =>
      val partial = al.toAttribute
      val combined: Expression = ae.aggregateFunction match {
        case s: Sum => Cast(s.copy(child = partial).toAggregateExpression(), s.dataType)
        case _: Count =>
          Coalesce(Seq(Sum(partial).toAggregateExpression(), Literal(0L))) // a global aggregate of no row counts 0
        case _: Min => Min(partial).toAggregateExpression()
        case _: Max => Max(partial).toAggregateExpression()
      }
      ae -> combined
    }.toMap
    val pre = Aggregate(keys, keys ++ parts.map(_._2), f)
    // Planned one per task, no exchange (#693): see LocalPreAggregate.
    pre.setTagValue(LocalPreAggregate.Tag, LocalPreAggregate.Mark(provenReduction = proven))
    (pre, combine, parts.map(_._2.toAttribute))
  }
}
