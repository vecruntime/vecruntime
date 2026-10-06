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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, IsNotNull, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Repeated aggregate subplans that differ only by an inferred `IsNotNull` on a grouping key share
 * one input again (#632).
 *
 * A CTE or view used twice is inlined twice and each copy optimized on its own;
 * `InferFiltersFromConstraints` then adds `IsNotNull(k)` to the copy that sits under a join on `k`
 * and pushes it down to the scan. The copies no longer match, `ReuseExchange` cannot share them and
 * the input -- typically a fact scan, a join and a shuffle -- runs twice (TPC-DS q65: `store_sales`
 * scanned twice at 1 TB where EMR Serverless scans it once; q1 the same over `store_returns`).
 *
 * Below an `Aggregate` grouping by `k`, `IsNotNull(k)` only removes the rows of the null group, so it
 * is the same as `IsNotNull(k)` above the aggregate. When removing such conjuncts from the inputs of
 * two aggregates with the same grouping makes the inputs the same plan, this rule moves them above
 * each aggregate (adding the key to the aggregate's output when the parent had pruned it), and the
 * shared exchange is planned once. Only `IsNotNull` of a grouping attribute is moved, and only
 * through projections that pass the attribute through, filters and inner joins; anything else, and
 * aggregates whose inputs still differ, are left as they are.
 *
 * Runs in the session's last optimizer batch (registered by [[RegisterLateOptimizerRules]]): any
 * earlier, and Spark's final filter push-down moves the predicates back below the aggregates. A
 * rewritten pair has the same input, so the rule does nothing on the next pass of the batch. Off with
 * `spark.vecruntime.optimizer.sharedAggregateInputs.enabled=false`, and with the plugin.
 */
case class SharedAggregateInputs(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!VectorConf.sharedAggregateInputsEnabled(session.sessionState.conf)) return plan
    val aggs = plan.collect { case a: Aggregate if keys(a).nonEmpty => a }
    if (aggs.size < 2) return plan
    // Each aggregate's input with the movable conjuncts stripped, and the keys they were on.
    val stripped: Map[Aggregate, (LogicalPlan, Seq[Attribute])] = aggs.map { a =>
      val removed = scala.collection.mutable.LinkedHashSet.empty[Attribute]
      val child = strip(a.child, AttributeSet(keys(a)), removed)
      a -> (child, removed.toSeq)
    }.toMap
    val rewrite = scala.collection.mutable.Map.empty[Aggregate, LogicalPlan]
    for (i <- aggs.indices; j <- aggs.indices if i < j) {
      val (a, b) = (aggs(i), aggs(j))
      val (ca, ra) = stripped(a)
      val (cb, rb) = stripped(b)
      if ((ra.nonEmpty || rb.nonEmpty) && !a.child.sameResult(b.child) && ca.sameResult(cb) && sameGrouping(a, b)) {
        rewrite.getOrElseUpdate(a, pullUp(a, ca, ra))
        rewrite.getOrElseUpdate(b, pullUp(b, cb, rb))
      }
    }
    if (rewrite.isEmpty) plan
    else plan.transformDown { case a: Aggregate if rewrite.contains(a) => rewrite(a) }
  }

  private def keys(a: Aggregate): Seq[Attribute] = a.groupingExpressions.collect { case k: Attribute => k }

  private def sameGrouping(a: Aggregate, b: Aggregate): Boolean = {
    // The inputs are the same plan up to attribute names; compare the groupings by position in it.
    val ia = a.child.output.map(_.exprId).zipWithIndex.toMap
    val ib = b.child.output.map(_.exprId).zipWithIndex.toMap
    a.groupingExpressions.size == b.groupingExpressions.size &&
    a.groupingExpressions.zip(b.groupingExpressions).forall {
      case (x: Attribute, y: Attribute) => ia.get(x.exprId).isDefined && ia.get(x.exprId) == ib.get(y.exprId)
      case _ => false
    }
  }

  /** `p` without `IsNotNull(k)` conjuncts on `keys` that reach the aggregate unchanged. */
  private def strip(
      p: LogicalPlan,
      keys: AttributeSet,
      removed: scala.collection.mutable.Set[Attribute]
  ): LogicalPlan =
    if (keys.isEmpty) p
    else
      p match {
        case f @ Filter(cond, child) if f.deterministic =>
          val (drop, keep) = splitConjunctivePredicates(cond).partition {
            case IsNotNull(a: Attribute) => keys.contains(a)
            case _ => false
          }
          drop.foreach { case IsNotNull(a: Attribute) => removed += a; case _ => }
          val c = strip(child, keys, removed)
          if (keep.isEmpty) c else Filter(keep.reduce(And), c)
        case Project(list, child) =>
          // A key passes through when the projection keeps the child's attribute as it is.
          val through = AttributeSet(list.collect { case a: Attribute if keys.contains(a) => a })
          Project(list, strip(child, through, removed))
        case Join(l, r, Inner, cond, hint) =>
          Join(
            strip(l, keys.intersect(l.outputSet), removed),
            strip(r, keys.intersect(r.outputSet), removed),
            Inner,
            cond,
            hint
          )
        case other => other
      }

  /** `a` over its stripped input, the removed predicates re-applied to its output. */
  private def pullUp(a: Aggregate, child: LogicalPlan, removed: Seq[Attribute]): LogicalPlan =
    if (removed.isEmpty) a.copy(child = child)
    else {
      val have = AttributeSet(a.output)
      val missing = removed.filterNot(have.contains)
      val agg = a.copy(aggregateExpressions = a.aggregateExpressions ++ missing, child = child)
      val filtered = Filter(removed.map(k => IsNotNull(k): Expression).reduce(And), agg)
      if (missing.isEmpty) filtered else Project(a.output, filtered)
    }
}
