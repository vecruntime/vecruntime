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
  Attribute,
  AttributeSet,
  DynamicPruningSubquery,
  EqualTo,
  Expression,
  PredicateHelper
}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Transitive dynamic partition pruning through a second join key (#634).
 *
 * Shape: an inner `Join(X, D)` on `X.p = D.d AND X.x = D.y`, where:
 * - `p` traces, through projections, filters, unions and inner or semi joins, to a partition column of a
 *   file scan inside `X`;
 * - `D` has no selective filter, so Spark's `PartitionPruning` adds nothing for `p`;
 * - `x` traces, through projections, filters and inner joins, to a subtree `S` of `X` (no join in it) that
 *   has a selective filter.
 *
 * TPC-DS q72 is this shape: `inventory` joins `date_dim d2` on `inv_date_sk = d2.d_date_sk AND
 * d1.d_week_seq = d2.d_week_seq`, with `d1` restricted to one year. EMR Serverless prunes `inventory` there.
 *
 * The scan gets `p IN (SELECT D.d FROM D LEFT SEMI JOIN S ON D.y = S.x)` as a `DynamicPruningSubquery`
 * (`onlyInBroadcast = false`), planned like Spark's own DPP. Exact: every row the join keeps has `p = D.d`
 * for a `D` row whose `y` equals an `x` produced by a row of `S`, which passed `S`'s filter, so its `d` is in
 * the set. Only inner joins are crossed between `S` and the join, so `x` always comes from a row of `S`.
 *
 * Declined: a selective `D` (Spark's own DPP covers it), a non-selective `S`, a `D` or `S` larger than the
 * broadcast threshold (the subquery scans both once more), a non-inner join at the top, a source key computed
 * by an expression, a scan that already has a pruning filter on `p`. Runs once before Spark's
 * DPP rules (`injectPreCBORule`). Off with `spark.vecruntime.optimizer.transitiveDpp.enabled=false`, with
 * the plugin, and with `spark.sql.optimizer.dynamicPartitionPruning.enabled=false`.
 */
case class TransitiveDpp(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (!VectorConf.transitiveDppEnabled(conf) || !conf.dynamicPartitionPruningEnabled) return plan
    plan.transformDown { case j: Join => rewrite(j) }
  }

  private def rewrite(j: Join): Join = j match {
    case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, _, _, left, right, _) =>
      val pairs = leftKeys.zip(rightKeys)
      // Try X = left, D = right, then the other way round.
      tryPrune(left, right, pairs).map(l => j.copy(left = l))
        .orElse(tryPrune(right, left, pairs.map(_.swap)).map(r => j.copy(right = r)))
        .getOrElse(j)
    case _ => j
  }

  /** `x` with one of its scans pruned through a `d` key, using another key pair to reach a filtered `S`. */
  private def tryPrune(x: LogicalPlan, d: LogicalPlan, pairs: Seq[(Expression, Expression)]): Option[LogicalPlan] = {
    if (selective(d)) return None
    // Pruning pairs: any expression over `x` (a cast of the partition column, typically) against `d`.
    val prunePairs = pairs.filter { case (xe, de) =>
      xe.references.nonEmpty && xe.references.subsetOf(x.outputSet) && de.references.nonEmpty &&
      de.references.subsetOf(d.outputSet) && xe.deterministic && de.deterministic
    }
    // Source pairs: an attribute of `x` that traces to a filtered subtree.
    val sourcePairs = prunePairs.collect { case (xa: Attribute, de) => (xa, de) }
    if (prunePairs.size < 2 || sourcePairs.isEmpty) return None
    prunePairs.iterator.flatMap { case (xp, dd) =>
      sourcePairs.iterator.filter { case (xx, dy) => !(xx eq xp) || !(dy eq dd) }.flatMap { case (xx, dy) =>
        sourceOf(x, xx).filter(s => selective(s) && small(s) && small(d)).flatMap { s =>
          val build = Join(d, s, LeftSemi, Some(EqualTo(dy, xx)), JoinHint.NONE)
          onScans(x, xp, build, dd)
        }
      }
    }.nextOption()
  }

  private def selective(p: LogicalPlan): Boolean =
    p.exists { case f: Filter => isLikelySelective(f.condition); case _ => false }

  /**
   * The pruning subquery scans `D` and `S` once more, so both must be small: at most the broadcast
   * threshold, as for a dimension Spark would broadcast. This also keeps a filtered fact from ever being
   * the source.
   */
  private def small(p: LogicalPlan): Boolean = {
    val threshold = session.sessionState.conf.autoBroadcastJoinThreshold
    threshold > 0 && p.stats.sizeInBytes <= BigInt(threshold)
  }

  /** The join-free subtree of `p` that produces `a`, reached through projections, filters and inner joins. */
  private def sourceOf(p: LogicalPlan, a: Attribute): Option[LogicalPlan] =
    if (!p.exists(_.isInstanceOf[Join])) Some(p)
    else
      p match {
        case Project(list, child) =>
          list.collectFirst {
            case at: Attribute if at.exprId == a.exprId => at
            case al @ org.apache.spark.sql.catalyst.expressions.Alias(c: Attribute, _) if al.exprId == a.exprId => c
          }.flatMap(sourceOf(child, _))
        case Filter(cond, child) if cond.deterministic => sourceOf(child, a)
        case Join(l, r, Inner, _, _) =>
          if (l.outputSet.contains(a)) sourceOf(l, a) else if (r.outputSet.contains(a)) sourceOf(r, a) else None
        case _ => None
      }

  /**
   * Adds the pruning filter on `value` to the partitioned file scan it traces to, through projections, filters,
   * unions and inner or semi joins. `None` when no scan can take it.
   */
  private def onScans(
      p: LogicalPlan,
      value: Expression,
      build: LogicalPlan,
      buildKey: Expression
  ): Option[LogicalPlan] =
    p match {
      case pr @ Project(list, child) =>
        onScans(child, replaceAlias(value, getAliasMap(pr)), build, buildKey).map(c => Project(list, c))
      case Filter(cond, child) =>
        if (hasPruningOn(cond, value)) None else onScans(child, value, build, buildKey).map(c => Filter(cond, c))
      case jn @ Join(l, r, jt, _, _) if jt == Inner || jt == LeftSemi =>
        if (value.references.subsetOf(l.outputSet)) onScans(l, value, build, buildKey).map(c => jn.copy(left = c))
        else if (jt == Inner && value.references.subsetOf(r.outputSet))
          onScans(r, value, build, buildKey).map(c => jn.copy(right = c))
        else None
      case u: Union if !u.byName && !u.allowMissingCol && value.references.size == 1 =>
        val a = value.references.head
        val idx = u.output.indexWhere(_.exprId == a.exprId)
        if (idx < 0) None
        else {
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
