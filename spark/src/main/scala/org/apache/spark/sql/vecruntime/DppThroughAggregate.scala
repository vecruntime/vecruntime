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
  DynamicPruningSubquery,
  EqualTo,
  Expression,
  PredicateHelper
}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Dynamic partition pruning through an aggregate (#633).
 *
 * Shape: `Join(Inner | LeftSemi, A, F, A.k = F.k')` where `F` has a selective filter and `A` reaches,
 * through projections, filters and inner / semi joins, an `Aggregate` grouping by `k`. Below the
 * aggregate, `k` comes from a dimension `D` that is inner-joined to a fact on `fact.p = D.d`, `p` a
 * partition column of a file scan (through projections, filters and unions of fact branches).
 * TPC-DS q59 (`store_sales` grouped by `d_week_seq`, joined to a `d_month_seq` range of `date_dim`) and
 * q2 (`web_sales` union `catalog_sales`, the same way) are this shape.
 *
 * Spark's `PartitionPruning` follows the join key down to `D.k`, which is not a partition column, and
 * stops; the inner fact-to-dimension join gets no pruning either, since `D` has no selective filter. This
 * rule adds `p IN (SELECT D.d FROM D LEFT SEMI JOIN F ON D.k = F.k')` to the fact scan, as a
 * `DynamicPruningSubquery` that Spark plans like its own (a broadcast reuse, else a one-off subquery over
 * the small dimension). It is exact: a fact row whose dimension row has a `k` outside `F` only feeds
 * groups the inner (or semi) join above drops, and every dimension row with a `k` in `F` keeps its `d`.
 *
 * Declined: a join key that is not a grouping key of the aggregate, an outer join that preserves the
 * aggregate's side, a non-selective `F`, a fact scan that already has a pruning filter on `p`, a key
 * computed by an expression rather than passed through. Runs once before Spark's DPP rules
 * (`injectPreCBORule`). Off with `spark.vecruntime.optimizer.dppThroughAggregate.enabled=false`, with the
 * plugin, and with `spark.sql.optimizer.dynamicPartitionPruning.enabled=false`.
 */
case class DppThroughAggregate(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (!VectorConf.dppThroughAggregateEnabled(conf) || !conf.dynamicPartitionPruningEnabled) return plan
    plan.transformDown { case j: Join => rewriteJoin(j) }
  }

  private def rewriteJoin(j: Join): Join = j match {
    case ExtractEquiJoinKeys(jt, leftKeys, rightKeys, _, _, left, right, _) =>
      var newLeft = left
      var newRight = right
      leftKeys.zip(rightKeys).foreach {
        case (lk: Attribute, rk: Attribute) =>
          if (prunesLeft(jt) && selective(right)) {
            newLeft = throughAbove(newLeft, lk, right, rk).getOrElse(newLeft)
          }
          if (jt == Inner && selective(left)) {
            newRight = throughAbove(newRight, rk, left, lk).getOrElse(newRight)
          }
        case _ =>
      }
      if ((newLeft eq left) && (newRight eq right)) j else j.copy(left = newLeft, right = newRight)
    case _ => j
  }

  private def prunesLeft(jt: JoinType): Boolean = jt == Inner || jt == LeftSemi

  private def selective(p: LogicalPlan): Boolean =
    p.exists { case f: Filter => isLikelySelective(f.condition); case _ => false }

  /** Above the aggregate: follow `k` (an output attribute of `p`) down to an `Aggregate` grouping by it. */
  private def throughAbove(p: LogicalPlan, k: Attribute, f: LogicalPlan, fk: Attribute): Option[LogicalPlan] =
    p match {
      case Project(list, child) =>
        passThrough(list, k).flatMap(ck => throughAbove(child, ck, f, fk).map(c => Project(list, c)))
      case Filter(cond, child) if cond.deterministic =>
        throughAbove(child, k, f, fk).map(c => Filter(cond, c))
      case jn @ Join(l, r, jt, _, _) if jt == Inner || jt == LeftSemi =>
        if (l.outputSet.contains(k)) throughAbove(l, k, f, fk).map(c => jn.copy(left = c))
        else if (jt == Inner && r.outputSet.contains(k)) throughAbove(r, k, f, fk).map(c => jn.copy(right = c))
        else None
      case a: Aggregate
          if a.groupingExpressions.exists(_.semanticEquals(k)) && a.aggregateExpressions.forall(_.deterministic) =>
        // `k` is a grouping column passed through unchanged, so it is also an attribute of the child.
        throughBelow(a.child, k, f, fk).map(c => a.copy(child = c))
      case _ => None
    }

  /** Below the aggregate: find the inner join of the dimension producing `k` with a partitioned fact. */
  private def throughBelow(p: LogicalPlan, k: Attribute, f: LogicalPlan, fk: Attribute): Option[LogicalPlan] =
    p match {
      case Project(list, child) =>
        passThrough(list, k).flatMap(ck => throughBelow(child, ck, f, fk).map(c => Project(list, c)))
      case Filter(cond, child) if cond.deterministic =>
        throughBelow(child, k, f, fk).map(c => Filter(cond, c))
      case jn @ ExtractEquiJoinKeys(Inner, lks, rks, _, _, l, r, _) =>
        val (dim, fact, dimKeys, factKeys, dimIsLeft) =
          if (l.outputSet.contains(k)) (l, r, lks, rks, true)
          else if (r.outputSet.contains(k)) (r, l, rks, lks, false)
          else return None
        // Deeper first: the dimension may itself sit above another such join.
        throughBelow(dim, k, f, fk) match {
          case Some(d) =>
            val j = jn.asInstanceOf[Join]
            return Some(if (dimIsLeft) j.copy(left = d) else j.copy(right = d))
          case None =>
        }
        val pruned = dimKeys.zip(factKeys).iterator.flatMap {
          case (dd: Attribute, fp) if dim.outputSet.contains(dd) && fp.references.subsetOf(fact.outputSet) =>
            val build = Join(dim, f, LeftSemi, Some(EqualTo(k, fk)), JoinHint.NONE)
            onScans(fact, fp, build, dd)
          case _ => None
        }
        if (!pruned.hasNext) None
        else {
          val newFact = pruned.next()
          val j = jn.asInstanceOf[Join]
          Some(if (dimIsLeft) j.copy(right = newFact) else j.copy(left = newFact))
        }
      case _ => None
    }

  /** `k` through a projection: the child attribute it is (or aliases), if passed through unchanged. */
  private def passThrough(list: Seq[Expression], k: Attribute): Option[Attribute] =
    list.collectFirst {
      case a: Attribute if a.exprId == k.exprId => a
      case al @ Alias(c: Attribute, _) if al.exprId == k.exprId => c
    }

  /**
   * Adds the pruning filter on `value` (an expression over `p`'s output) to every partitioned file scan it
   * traces to, through projections, filters and unions. `None` when no scan can take it.
   */
  private def onScans(p: LogicalPlan, value: Expression, build: LogicalPlan, buildKey: Attribute): Option[LogicalPlan] =
    p match {
      case Project(list, child) =>
        val aliases = getAliasMap(Project(list, child))
        onScans(child, replaceAlias(value, aliases), build, buildKey).map(c => Project(list, c))
      case Filter(cond, child) =>
        if (hasPruningOn(cond, value)) None
        else onScans(child, value, build, buildKey).map(c => Filter(cond, c))
      case u: Union if !u.byName && !u.allowMissingCol && value.references.size == 1 =>
        val a = value.references.head
        val idx = u.output.indexWhere(_.exprId == a.exprId)
        if (idx < 0) None
        else {
          // The same expression over each branch's column at that position (a cast, typically).
          val kids = u.children.map { c =>
            val ca = c.output(idx)
            onScans(c, value.transform { case r: Attribute if r.exprId == a.exprId => ca }, build, buildKey)
          }
          if (kids.forall(_.isEmpty)) None
          else Some(u.withNewChildren(kids.zip(u.children).map { case (n, o) => n.getOrElse(o) }))
        }
      case l: LogicalRelation =>
        l.relation match {
          case fs: HadoopFsRelation if fs.partitionSchema.nonEmpty =>
            val partCols = AttributeSet(l.resolve(fs.partitionSchema, session.sessionState.analyzer.resolver))
            if (value.references.nonEmpty && value.references.subsetOf(partCols) && value.deterministic) {
              Some(Filter(DynamicPruningSubquery(value, build, Seq(buildKey), Seq(0), onlyInBroadcast = false), l))
            } else None
          case _ => None
        }
      case _ => None
    }

  private def hasPruningOn(cond: Expression, value: Expression): Boolean =
    splitConjunctivePredicates(cond).exists {
      case d: DynamicPruningSubquery => d.pruningKey.semanticEquals(value)
      case _ => false
    }
}
