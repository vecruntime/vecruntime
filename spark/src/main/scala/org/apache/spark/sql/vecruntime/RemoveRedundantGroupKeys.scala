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
  BoundReference,
  Cast,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, First}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode

/**
 * Drops grouping keys made redundant by a broadcast join whose build keys turned out unique (#635; EMR's
 * AQERemoveRedundantGroupKeys does the same). An adaptive (runtime) optimizer rule: it runs when AQE
 * re-optimizes after a query stage finished, so a broadcast stage below the aggregate has been built and
 * its keys can be checked on the data it actually holds.
 *
 * When a broadcast build side's join key `b` holds every value at most once, each output row of the join
 * carries exactly one build row, so every column of the build side is a function of `b` -- and, for an
 * inner join, of the stream key equal to `b`. An aggregate above that groups by `b` (or the equal stream
 * key) and also by build-side columns therefore forms the same groups without those columns: they are
 * dropped from the grouping keys, and any output that still needs one reads it with `first()`, which sees
 * one value per group. TPC-DS q23a/b group by `substr(i_item_desc, 1, 30), i_item_sk, d_date` over a join
 * with `item`, broadcast on `i_item_sk`: the string key goes, and with it most of the aggregate's cost.
 *
 * Nothing is assumed about the data: no keys are dropped unless the materialised broadcast says its keys
 * are unique (our broadcast relation sorts its key column once, on the driver, see
 * `VectorBroadcastBatches.keysUnique`; a broadcast left to Spark is not looked at), so a build
 * side with a repeated key keeps the plan as it was. The functional dependency is carried through
 * projections, filters and joins (outer joins null both sides of a dependency together), not through
 * aggregates, unions or windows. Off with `spark.vecruntime.optimizer.removeRedundantGroupKeys.enabled=false`.
 */
case class RemoveRedundantGroupKeys(session: SparkSession) extends Rule[LogicalPlan] {

  /** Determinants and the attributes they determine, valid on a plan's output rows. */
  private case class Dependency(determinants: AttributeSet, dependents: AttributeSet)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!VectorConf.removeRedundantGroupKeysEnabled(conf)) return plan
    plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case a: Aggregate if a.groupingExpressions.size > 1 => rewrite(a).getOrElse(a)
    }
  }

  private def rewrite(a: Aggregate): Option[Aggregate] = {
    val groupAttrs = a.groupingExpressions.collect { case g: Attribute => g }
    if (groupAttrs.size < 2) return None
    val deps = dependencies(a.child)
    if (deps.isEmpty) return None
    val removable = deps.flatMap { d =>
      if (groupAttrs.exists(d.determinants.contains)) {
        groupAttrs.filter(g => d.dependents.contains(g) && !d.determinants.contains(g))
      } else Nil
    }
    val drop = AttributeSet(removable)
    if (drop.isEmpty) return None
    val grouping = a.groupingExpressions.filterNot {
      case g: Attribute => drop.contains(g)
      case _ => false
    }
    if (grouping.isEmpty) return None
    val outputs = a.aggregateExpressions.map {
      case at: Attribute if drop.contains(at) =>
        Alias(first(at), at.name)(exprId = at.exprId, qualifier = at.qualifier)
      case ne => readFirst(ne, drop).asInstanceOf[NamedExpression]
    }
    Some(a.copy(groupingExpressions = grouping, aggregateExpressions = outputs))
  }

  private def first(at: Attribute): Expression = First(at, ignoreNulls = false).toAggregateExpression()

  /** A dropped key used outside an aggregate function becomes `first(key)`. */
  private def readFirst(e: Expression, drop: AttributeSet): Expression = e match {
    case ae: AggregateExpression => ae
    case at: Attribute if drop.contains(at) => first(at)
    case other => other.withNewChildren(other.children.map(readFirst(_, drop)))
  }

  /** The functional dependencies that hold on `p`'s output rows. */
  private def dependencies(p: LogicalPlan): Seq[Dependency] = p match {
    case Project(list, child) =>
      dependencies(child).flatMap { d =>
        val dets = list.collect {
          case at: Attribute if d.determinants.contains(at) => at
          case al @ Alias(at: Attribute, _) if d.determinants.contains(at) => al.toAttribute
        }
        val dependents = list.collect {
          case ne
              if ne.deterministic && ne.references.nonEmpty && ne.references.subsetOf(d.dependents ++ d.determinants) =>
            ne.toAttribute
        }
        if (dets.isEmpty) None else Some(Dependency(AttributeSet(dets), AttributeSet(dependents)))
      }
    case Filter(_, child) => dependencies(child)
    case j: Join =>
      val below = j.joinType match {
        case LeftSemi | LeftAnti => dependencies(j.left)
        case _ => dependencies(j.left) ++ dependencies(j.right)
      }
      below ++ fromUniqueBroadcast(j)
    case _ => Nil
  }

  /** The dependency a join adds when one of its sides is a broadcast stage with unique keys. */
  private def fromUniqueBroadcast(j: Join): Seq[Dependency] = j match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, _) =>
      def side(stage: LogicalPlan, ownKeys: Seq[Expression], otherKeys: Seq[Expression]): Option[Dependency] =
        stage match {
          case s: LogicalQueryStage if inOutput(joinType, stage eq right) =>
            uniqueKey(s).flatMap { b =>
              val at = ownKeys.indexWhere(_.semanticEquals(b))
              if (at < 0) None
              else {
                val stream = otherKeys(at) match {
                  case k: Attribute if joinType == Inner => Seq(k)
                  case _ => Nil
                }
                Some(Dependency(AttributeSet(b +: stream), s.outputSet))
              }
            }
          case _ => None
        }
      side(right, rightKeys, leftKeys).toSeq ++ side(left, leftKeys, rightKeys).toSeq
    case _ => Nil
  }

  /** Whether the stage's side reaches the join's output (not the right side of a semi or anti join). */
  private def inOutput(joinType: JoinType, isRight: Boolean): Boolean =
    !(isRight && (joinType == LeftSemi || joinType == LeftAnti))

  /** The stage's single hashed key attribute, when the stage is a built broadcast with unique keys. */
  private def uniqueKey(s: LogicalQueryStage): Option[Attribute] = s.physicalPlan match {
    case b: BroadcastQueryStageExec if b.isMaterialized =>
      exchangeOf(b.plan).flatMap { e =>
        val ordinal = e.mode match {
          case HashedRelationBroadcastMode(Seq(key), _) => boundOrdinal(key)
          case _ => None
        }
        ordinal.filter(_ < s.output.size).filter(o => isUnique(e, o)).map(s.output(_))
      }
    case _ => None
  }

  private def exchangeOf(p: SparkPlan): Option[VectorBroadcastExchangeExec] = p match {
    case e: VectorBroadcastExchangeExec => Some(e)
    case ReusedExchangeExec(_, e: VectorBroadcastExchangeExec) => Some(e)
    case _ => None
  }

  private def boundOrdinal(e: Expression): Option[Int] = e match {
    case BoundReference(o, _, _) => Some(o)
    case Cast(child, _, _, _) => boundOrdinal(child)
    case _ => None
  }

  private def isUnique(e: VectorBroadcastExchangeExec, ordinal: Int): Boolean =
    // Read the broadcast from `completionFuture`, the promise AQE waits on to mark the stage materialised
    // (#697). `relationFuture` completes a moment later -- its body returns after fulfilling the promise --
    // so a re-optimisation in between saw `relationFuture.isDone == false` and kept every grouping key.
    try {
      e.completionFuture.value match {
        case Some(scala.util.Success(b)) =>
          b.value match {
            case v: VectorBroadcastBatches => v.keysUnique(ordinal)
            case _ => false
          }
        case _ => false
      }
    } catch {
      case scala.util.control.NonFatal(_) => false
    }
}
