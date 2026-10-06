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
  AttributeSet,
  EqualTo,
  Exists,
  Expression,
  IsNotNull,
  ListQuery,
  NamedExpression,
  Not,
  PredicateHelper
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{
  DataType,
  DateType,
  DecimalType,
  IntegralType,
  StringType,
  TimestampNTZType,
  TimestampType
}

/**
 * An existence-only self-join becomes one aggregate.
 *
 * TPC-DS q95's `ws_wh` is `web_sales ws1 JOIN web_sales ws2 ON ws1.ws_order_number =
 * ws2.ws_order_number AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk`, used only through `IN`: it
 * asks which orders ship from more than one warehouse. Spark runs it as a many-to-many join of the
 * fact table with itself (q95 at 1 TB on 2026-10-04: 96 s for us; EMR Serverless, which rewrites it, 18 s).
 * The same set of keys is `SELECT k FROM t WHERE k IS NOT NULL GROUP BY k HAVING min(v) <> max(v)`:
 * two rows of a key with different non-null `v` exist exactly when the key's smallest and largest
 * non-null `v` differ, and a null `v` (or key) never satisfies the join, as it never moves a min or
 * a max. The rewrite drops duplicates, so it is applied only where nothing above can count rows:
 * on the build side of a left semi / anti join, or inside an `IN` / `EXISTS` subquery, reached
 * through projections, filters and inner joins only, and when nothing above reads a column of the
 * self-join other than its join keys.
 *
 * The two sides must be the same plan (`sameResult`), the condition exactly equalities between the
 * same column of each side plus one `<>` on one more column, of a type whose `=` agrees with
 * `min` / `max` ordering (integers, decimals, dates, timestamps, binary-collated strings). Anything
 * else keeps Spark's plan. Off with `spark.vecruntime.optimizer.selfJoinToAggregate.enabled=false`,
 * and with the plugin.
 */
case class SelfJoinToAggregate(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (!VectorConf.selfJoinToAggregateEnabled(session.sessionState.conf)) plan
    else
      plan.transformUpWithSubqueries {
        case j @ Join(_, right, LeftSemi | LeftAnti, cond, _) =>
          val required = AttributeSet(cond.toSeq.flatMap(_.references)).intersect(right.outputSet)
          val rewritten = rewrite(right, required)
          if (rewritten eq right) j else j.copy(right = rewritten)
        case p =>
          p.transformExpressionsUp {
            case l: ListQuery =>
              val rewritten = rewrite(l.plan, l.plan.outputSet)
              if (rewritten eq l.plan) l else l.withNewPlan(rewritten)
            case e: Exists =>
              val rewritten = rewrite(e.plan, AttributeSet.empty)
              if (rewritten eq e.plan) e else e.withNewPlan(rewritten)
          }
      }

  /** `plan` with every applicable self-join below it rewritten; `required` is what the parent reads. */
  private def rewrite(plan: LogicalPlan, required: AttributeSet): LogicalPlan = plan match {
    case p @ Project(list, child) =>
      val c = rewrite(child, AttributeSet(list.flatMap(_.references)))
      if (c eq child) p else p.copy(child = c)
    case f @ Filter(cond, child) =>
      val c = rewrite(child, required ++ cond.references)
      if (c eq child) f else f.copy(child = c)
    case j @ Join(left, right, Inner, cond, _) =>
      selfJoin(j, required).getOrElse {
        val below = required ++ AttributeSet(cond.toSeq.flatMap(_.references))
        val l = rewrite(left, below.intersect(left.outputSet))
        val r = rewrite(right, below.intersect(right.outputSet))
        if ((l eq left) && (r eq right)) j else j.copy(left = l, right = r)
      }
    case other => other
  }

  private def selfJoin(j: Join, required: AttributeSet): Option[LogicalPlan] = {
    val (left, right) = (j.left, j.right)
    if (j.condition.isEmpty || !left.deterministic || !left.sameResult(right)) return None
    if (left.output.size != right.output.size) return None
    val leftIdx = left.output.map(_.exprId).zipWithIndex.toMap
    val rightIdx = right.output.map(_.exprId).zipWithIndex.toMap

    // Each conjunct names the same column position on both sides: Some(index) or None.
    def samePosition(a: Expression, b: Expression): Option[Int] = (a, b) match {
      case (x: Attribute, y: Attribute) =>
        (leftIdx.get(x.exprId), rightIdx.get(y.exprId)) match {
          case (Some(i), Some(k)) if i == k => Some(i)
          case _ =>
            (leftIdx.get(y.exprId), rightIdx.get(x.exprId)) match {
              case (Some(i), Some(k)) if i == k => Some(i)
              case _ => None
            }
        }
      case _ => None
    }

    val keys = scala.collection.mutable.ArrayBuffer.empty[Int]
    val differs = scala.collection.mutable.ArrayBuffer.empty[Int]
    val notNull = scala.collection.mutable.ArrayBuffer.empty[Int] // implied by the join; checked below
    splitConjunctivePredicates(j.condition.get).foreach {
      case EqualTo(a, b) => keys += samePosition(a, b).getOrElse(return None)
      case Not(EqualTo(a, b)) => differs += samePosition(a, b).getOrElse(return None)
      case IsNotNull(a: Attribute) =>
        notNull += leftIdx.get(a.exprId).orElse(rightIdx.get(a.exprId)).getOrElse(return None)
      case _ => return None
    }
    if (keys.isEmpty || differs.size != 1 || keys.contains(differs.head)) return None
    if (!notNull.forall(i => keys.contains(i) || i == differs.head)) return None
    val keyIdx = keys.distinct.toSeq
    val leftKeys = keyIdx.map(left.output)
    val rightKeys = keyIdx.map(right.output)
    if (!required.subsetOf(AttributeSet(leftKeys ++ rightKeys))) return None
    val v = left.output(differs.head)
    if (!orderAgreesWithEquality(v.dataType) || !leftKeys.forall(k => orderAgreesWithEquality(k.dataType))) return None

    val notNullKeys = leftKeys.map(k => IsNotNull(k): Expression).reduce(And)
    val lo = Alias(Min(v).toAggregateExpression(), "gen_min_" + v.name)()
    val hi = Alias(Max(v).toAggregateExpression(), "gen_max_" + v.name)()
    val agg = Aggregate(leftKeys, leftKeys ++ Seq(lo, hi), Filter(notNullKeys, left))
    val distinct = Filter(Not(EqualTo(lo.toAttribute, hi.toAttribute)), agg)
    // Both sides' key attributes stay visible: the right side's as aliases of the left side's.
    val output: Seq[NamedExpression] =
      leftKeys ++ rightKeys.zip(leftKeys).map { case (r, l) => Alias(l, r.name)(exprId = r.exprId) }
    Some(Project(output, distinct))
  }

  /** Types whose `=` holds exactly when `min` / `max` cannot tell two values apart. */
  private def orderAgreesWithEquality(t: DataType): Boolean = t match {
    case _: IntegralType | _: DecimalType | DateType | TimestampType | TimestampNTZType => true
    case s: StringType => s.supportsBinaryEquality
    case _ => false
  }
}
