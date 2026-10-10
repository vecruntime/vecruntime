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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}

import io.vecruntime.spark.VectorConf

/**
 * The local pre-aggregate of [[AggregateBelowJoin]] (#693): one aggregate per task, no exchange.
 *
 * The pre-aggregate need not be global. The top aggregate recombines its partials (sum of sums, sum of counts,
 * min of minima, max of maxima), and the joins between them keep or repeat a row by its join keys only, so ANY
 * split of the fact's rows into groups sharing those keys gives the same result -- a group per task, a group per
 * table the task flushed, a group per row. Planned by Spark's own strategy it was a Partial, an exchange of the
 * fact on the keys and a Final: on TPC-DS q47 at 1 TB grouping `store_sales` by `(item, store, date)` removes
 * about 1 % of its 650 M rows, and the exchange of all of them tripled the query (6.9 -> 22.8 s). Local, the
 * pre-aggregate costs at most one pass over each task's rows; and [[VectorHashAggregateExec]] gives up on it
 * once a task's first rows show they do not reduce, sending the rest through one batch at a time
 * ([[AggSpillPolicy.EmitAndReset]] with `probeRows`).
 *
 * The logical pre-aggregate carries [[Tag]]; [[LocalPreAggregateStrategy]] plans a tagged one as a single
 * Complete `HashAggregateExec` with no required distribution, which [[VectorColumnarRule]] replaces with ours
 * marked `localPreAggregate`. A node that lost the tag is planned as a normal aggregate: slower, still correct.
 * Statistics that prove the keys reduce the rows ([[Mark.provenReduction]]) turn the early judgement off.
 */
object LocalPreAggregate {

  /**
   * `provenReduction`: statistics show the keys reduce the fact's rows (see `AggregateBelowJoin.provenReduction`),
   * so the aggregate is not judged on its first rows, only at its memory budget.
   */
  final case class Mark(provenReduction: Boolean)

  /** On the logical pre-aggregate of [[AggregateBelowJoin]], and on the physical aggregate planned from it. */
  val Tag: TreeNodeTag[Mark] = TreeNodeTag[Mark]("vecruntime.localPreAggregate")

  def mark(node: TreeNode[_]): Option[Mark] = node.getTagValue(Tag)

  def isLocal(node: TreeNode[_]): Boolean = mark(node).isDefined
}

/** Plans a tagged pre-aggregate as one Complete aggregate per task (see [[LocalPreAggregate]]). */
case class LocalPreAggregateStrategy(session: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case a: Aggregate
        if LocalPreAggregate.isLocal(a) && VectorConf.aggregateBelowJoinLocal(session.sessionState.conf) =>
      val mark = LocalPreAggregate.mark(a).get
      a match {
        case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
            if aggExpressions.forall(_.isInstanceOf[AggregateExpression]) =>
          val aggregates = aggExpressions.map(_.asInstanceOf[AggregateExpression].copy(mode = Complete))
          val attributes = aggregates.map(_.resultAttribute)
          // The operator Spark itself would pick for these buffers (a string min/max is not mutable in an
          // UnsafeRow: a sort aggregate, which ours replaces with the same hash operator); only the stage
          // structure differs -- one Complete stage with no required distribution.
          val exec: SparkPlan =
            if (
              Aggregate.supportsHashAggregate(
                aggregates.flatMap(_.aggregateFunction.aggBufferAttributes),
                groupingExpressions
              )
            )
              HashAggregateExec(
                requiredChildDistributionExpressions = None,
                isStreaming = false,
                numShufflePartitions = None,
                groupingExpressions = groupingExpressions,
                aggregateExpressions = aggregates,
                aggregateAttributes = attributes,
                initialInputBufferOffset = 0,
                resultExpressions = resultExpressions,
                child = planLater(child)
              )
            else
              SortAggregateExec(
                requiredChildDistributionExpressions = None,
                isStreaming = false,
                numShufflePartitions = None,
                groupingExpressions = groupingExpressions,
                aggregateExpressions = aggregates,
                aggregateAttributes = attributes,
                initialInputBufferOffset = 0,
                resultExpressions = resultExpressions,
                child = planLater(child)
              )
          exec.setTagValue(LocalPreAggregate.Tag, mark)
          exec :: Nil
        case _ => Nil
      }
    case _ => Nil
  }
}
