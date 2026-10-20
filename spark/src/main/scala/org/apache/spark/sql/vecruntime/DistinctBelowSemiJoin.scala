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
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * The right side of a left semi join reduced to its distinct join columns, one aggregate per task (#840).
 *
 * A left semi join only asks whether a left row has a match, so duplicate right rows change nothing: the right
 * side can be grouped by the columns the join condition reads without changing the result. Spark rewrites
 * `INTERSECT` as `Distinct(LeftSemi(left, right))` with nothing on the right: TPC-DS q14's `cross_items` joins
 * `item` (400 K rows) to `catalog_sales x item` (8.6 G rows at 10 TB) and the distinct result to
 * `web_sales x item` (4.3 G rows) on `(brand, class, category)`, which has a few thousand values. The two
 * sort-merge joins and their sorts took 5,900 of q14a's 17,300 task seconds and spilled ~230 GiB.
 *
 * The aggregate is local ([[LocalPreAggregate]]): per task, no exchange of its own -- the join's exchange of the
 * right side follows it and carries the reduced rows, and adaptive execution then sees a small side. A task
 * whose first rows do not repeat (a high-cardinality key) gives it up and passes rows through. A right side
 * already small enough to broadcast, already an aggregate, or not deterministic is left alone.
 *
 * Runs in the session's last optimizer batch ([[RegisterLateOptimizerRules]]). Off with
 * `spark.vecruntime.optimizer.distinctBelowSemiJoin.enabled=false` and with the plugin.
 */
case class DistinctBelowSemiJoin(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (!VectorConf.distinctBelowSemiJoinEnabled(conf)) return plan
    plan.transformUp {
      case j @ Join(_, r, LeftSemi, Some(cond), _) if eligible(r) =>
        // What the condition reads from the right side, in the side's own column order.
        val keys = r.output.filter(cond.references.contains)
        if (keys.isEmpty || r.stats.sizeInBytes <= BigInt(math.max(conf.autoBroadcastJoinThreshold, 0L))) j
        else {
          val distinct = Aggregate(keys, keys, r)
          distinct.setTagValue(LocalPreAggregate.Tag, LocalPreAggregate.Mark(provenReduction = false))
          j.copy(right = distinct)
        }
    }
  }

  private def eligible(r: LogicalPlan): Boolean = !r.isInstanceOf[Aggregate] && r.deterministic
}
