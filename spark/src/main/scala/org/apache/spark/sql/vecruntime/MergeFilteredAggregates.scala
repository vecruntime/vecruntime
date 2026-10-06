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

import io.vecruntime.spark.VectorConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  Attribute,
  AttributeMap,
  AttributeSet,
  Expression,
  If,
  Literal,
  NamedExpression,
  Or,
  PredicateHelper,
  ScalarSubquery
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Global aggregates over the same data with different filters, computed in one pass.
 *
 * TPC-DS q88 cross-joins eight `count(*)` over `store_sales JOIN household_demographics JOIN
 * time_dim JOIN store` that differ only in their `time_dim` filter; q90 two, q28 six over
 * `store_sales` alone with different `WHERE`s, and q9 is fifteen scalar subqueries over `store_sales`
 * with five different `WHERE`s (Spark's `MergeScalarSubqueries` merges only identical ones). Each is a
 * full read of the fact table: q88 reads `store_sales` eight times (23.0 G rows at 1 TB), where EMR
 * Serverless, which merges them, reads it once (2.9 G).
 *
 * Two global aggregates (`Aggregate` without grouping keys) merge when their inputs are the same plan
 * apart from `Filter`s: at each pair of differing filters the merged plan keeps the rows of either,
 * `Filter(c1 OR c2)`, and every aggregate keeps only its own side's rows through an aggregate `FILTER
 * (WHERE ...)` -- a `DISTINCT` aggregate takes the predicate into its argument instead, `IF(p, x,
 * NULL)` (as EMR's plan does), which `count`, `sum`, `avg`, `min` and `max` ignore the same way.
 * The predicates' columns are carried up through the projections. Only projections, filters and
 * inner joins with equal conditions are walked; the leaves must be the same plan (`sameResult`).
 *
 * Applied to (a) an inner join without a condition of two global aggregates (each yields exactly
 * one row, so the join is the concatenation and becomes one aggregate), folded over q88's chain,
 * and (b) the uncorrelated scalar subqueries of one operator whose plans are global aggregates:
 * each mergeable group is replaced by one merged aggregate under a projection of its own column,
 * which Spark's `MergeScalarSubqueries` then computes once. Off with
 * `spark.vecruntime.optimizer.mergeFilteredAggregates.enabled=false`, and with the plugin.
 */
case class MergeFilteredAggregates(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (!VectorConf.mergeFilteredAggregatesEnabled(session.sessionState.conf)) plan
    else
      plan.transformUp {
        case j @ Join(a1: Aggregate, a2: Aggregate, Inner, None, _) if global(a1) && global(a2) =>
          merge(a1, a2).getOrElse(j)
        case p if p.expressions.exists(_.exists(scalar)) => mergeScalarSubqueries(p)
      }

  private def global(a: Aggregate): Boolean =
    a.groupingExpressions.isEmpty && a.aggregateExpressions.forall(_.deterministic)

  private def scalar(e: Expression): Boolean = e match {
    case s: ScalarSubquery => s.outerAttrs.isEmpty && s.joinCond.isEmpty && (s.plan match {
        case a: Aggregate => global(a) && a.aggregateExpressions.size == 1
        case _ => false
      })
    case _ => false
  }

  /** One aggregate computing both sides' columns, or None when the inputs do not line up. */
  private def merge(a1: Aggregate, a2: Aggregate): Option[Aggregate] = {
    val m = mergePlans(a1.child, a2.child).getOrElse(return None)
    val aggs1 = a1.aggregateExpressions.map(e => restrict(e, m.p1).getOrElse(return None))
    val aggs2 = a2.aggregateExpressions.map(e => restrict(rename(e, m.map), m.p2).getOrElse(return None))
    Some(Aggregate(Nil, aggs1 ++ aggs2, m.plan))
  }

  /**
   * The merged plan, how the second input's attributes are named in it, and the predicate that
   * selects each input's rows from it (None: all of them), over the merged plan's output.
   */
  private case class Merged(
      plan: LogicalPlan,
      map: AttributeMap[Attribute],
      p1: Option[Expression],
      p2: Option[Expression]
  )

  private def mergePlans(x: LogicalPlan, y: LogicalPlan): Option[Merged] =
    if (x.output.size == y.output.size && x.sameResult(y))
      Some(Merged(x, AttributeMap(y.output.zip(x.output)), None, None))
    else
      (x, y) match {
        case (Filter(cx, xc), Filter(cy, yc)) if cx.deterministic && cy.deterministic =>
          mergePlans(xc, yc).map { m =>
            val cy2 = rename(cy, m.map)
            if (cx.semanticEquals(cy2)) m.copy(plan = Filter(cx, m.plan))
            else Merged(Filter(Or(cx, cy2), m.plan), m.map, and(m.p1, cx), and(m.p2, cy2))
          }
        case (Filter(cx, xc), _) if cx.deterministic =>
          mergePlans(xc, y).map(m => m.copy(p1 = and(m.p1, cx)))
        case (_, Filter(cy, yc)) if cy.deterministic =>
          mergePlans(x, yc).map(m => m.copy(p2 = and(m.p2, rename(cy, m.map))))
        case (Project(lx, xc), Project(ly, yc)) if (lx ++ ly).forall(_.deterministic) =>
          mergePlans(xc, yc).map { m =>
            val list = scala.collection.mutable.ArrayBuffer.from[NamedExpression](lx)
            val map = scala.collection.mutable.Map.from(m.map.toSeq)
            ly.foreach { ne =>
              val renamed = rename(ne, m.map).asInstanceOf[NamedExpression]
              list.find(_.semanticEquals(renamed)).orElse(list.find(sameValue(_, renamed))) match {
                case Some(existing) => map(ne.toAttribute) = existing.toAttribute
                case None =>
                  list += renamed
                  map(ne.toAttribute) = renamed.toAttribute
              }
            }
            // The predicates still to be applied read columns the projections may have dropped.
            val have = AttributeSet(list.map(_.toAttribute))
            (m.p1.toSeq ++ m.p2).flatMap(_.references).distinct.foreach { a =>
              if (!have.contains(a)) list += a
            }
            Merged(Project(list.toSeq, m.plan), AttributeMap(map.toSeq), m.p1, m.p2)
          }
        case (Join(lx, rx, Inner, cx, hx), Join(ly, ry, Inner, cy, _)) =>
          for {
            l <- mergePlans(lx, ly)
            r <- mergePlans(rx, ry)
            map = AttributeMap(l.map.toSeq ++ r.map.toSeq)
            if cx.map(_.canonicalized) == cy.map(c => rename(c, map).canonicalized)
          } yield Merged(Join(l.plan, r.plan, Inner, cx, hx), map, and(l.p1, r.p1), and(l.p2, r.p2))
        case _ => None
      }

  /** Two projection entries that compute the same value under different names. */
  private def sameValue(a: NamedExpression, b: NamedExpression): Boolean = (a, b) match {
    case (x: Alias, y: Alias) => x.child.semanticEquals(y.child)
    case (x: Attribute, y: Alias) => y.child.semanticEquals(x)
    case (x: Alias, y: Attribute) => x.child.semanticEquals(y)
    case _ => false
  }

  /** `e` counting only the rows `p` selects; None for a distinct aggregate we cannot restrict. */
  private def restrict(e: NamedExpression, p: Option[Expression]): Option[NamedExpression] = p match {
    case None => Some(e)
    case Some(pred) =>
      var ok = true
      val out = e.transformDown {
        case ae: AggregateExpression if ae.isDistinct =>
          ae.aggregateFunction match {
            case _: Count | _: Sum | _: Average | _: Min | _: Max =>
              val fn = ae.aggregateFunction.mapChildren(c => If(pred, c, Literal(null, c.dataType)))
              ae.copy(aggregateFunction =
                fn.asInstanceOf[org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction]
              )
            case _ => ok = false; ae
          }
        case ae: AggregateExpression =>
          ae.copy(filter = Some(ae.filter.map(f => And(f, pred)).getOrElse(pred)))
      }
      if (ok) Some(out.asInstanceOf[NamedExpression]) else None
  }

  private def rename[E <: Expression](e: E, map: AttributeMap[Attribute]): E =
    e.transform { case a: Attribute => map.getOrElse(a, a) }.asInstanceOf[E]

  private def and(p: Option[Expression], c: Expression): Option[Expression] = Some(p.map(And(_, c)).getOrElse(c))
  private def and(a: Option[Expression], b: Option[Expression]): Option[Expression] = (a, b) match {
    case (Some(x), Some(y)) => Some(And(x, y))
    case _ => a.orElse(b)
  }

  /** The operator's uncorrelated global-aggregate scalar subqueries, mergeable ones computed as one. */
  private def mergeScalarSubqueries(p: LogicalPlan): LogicalPlan = {
    val subqueries = p.expressions.flatMap(_.collect { case s: ScalarSubquery if scalar(s) => s }).distinct
    if (subqueries.size < 2) return p
    // Greedy groups: each subquery joins the first group whose merged aggregate it merges with.
    val groups =
      scala.collection.mutable.ArrayBuffer.empty[(Aggregate, scala.collection.mutable.ArrayBuffer[ScalarSubquery])]
    subqueries.foreach { s =>
      val a = s.plan.asInstanceOf[Aggregate]
      val i = groups.indexWhere { case (g, _) => merge(g, a).isDefined }
      if (i < 0) groups += (a -> scala.collection.mutable.ArrayBuffer(s))
      else {
        groups(i) = (merge(groups(i)._1, a).get, groups(i)._2 += s)
      }
    }
    val replacement: Map[ScalarSubquery, ScalarSubquery] = groups.iterator.filter(_._2.size > 1).flatMap {
      case (merged, members) =>
        members.map { s =>
          val col = s.plan.output.head
          s -> s.withNewPlan(Project(Seq(col), merged))
        }
    }.toMap
    if (replacement.isEmpty) p
    else
      p.transformExpressions {
        case s: ScalarSubquery if replacement.contains(s) => replacement(s)
      }
  }
}
