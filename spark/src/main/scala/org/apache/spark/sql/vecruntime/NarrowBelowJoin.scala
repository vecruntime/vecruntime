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
  Expression,
  Length,
  NamedExpression,
  OctetLength,
  Substring
}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BinaryType, StringType}

/**
 * Computes a narrowing projection of one join side's string column below the join (#635), on the smaller
 * side, instead of above it.
 *
 * Shape: `Project(..., f(x) AS a, ..., Join(left, right))` where
 * - `f` is a substring (`substr`, `left`, `right` once analysed) or a length of a string or binary column,
 *   built only from that side's columns and literals;
 * - that side is the join's smaller one by estimated size (a dimension, typically broadcast).
 *
 * The side becomes `Project(columns still needed above, f(x) AS a, side)`, and the top projection reads `a`. The
 * join then carries the narrow result instead of the whole string column, and `f` runs once per row of the
 * smaller side rather than once per joined row. TPC-DS q23a computes `substr(i_item_desc, 1, 30)` above
 * `store_sales JOIN item`: at 1 TB every one of ~1.66G joined rows gathered the full description from the
 * build side to cut 30 characters of it.
 *
 * Why the result is the same. `f` is deterministic and never fails, and a join only repeats or drops the
 * rows of each side; it does not change their values. On the null-supplying side of an outer join an
 * unmatched row has the column null, and `f(null)` is null, which is what the padded row gets anyway.
 *
 * Runs in the session's last optimizer batch ([[RegisterLateOptimizerRules]]), after Spark's column pruning
 * and filter push-down. Off with `spark.vecruntime.optimizer.narrowBelowJoin.enabled=false` and with the
 * plugin.
 */
case class NarrowBelowJoin(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!VectorConf.narrowBelowJoinEnabled(session.sessionState.conf)) return plan
    plan.transformUp { case p @ Project(list, j: Join) => rewrite(p, list, j).getOrElse(p) }
  }

  private def narrowing(e: Expression): Boolean = e match {
    case Substring(str, pos, len) =>
      isVarWidth(str) && pos.foldable && len.foldable && str.references.nonEmpty
    case Length(c) => isVarWidth(c) && c.references.nonEmpty
    case OctetLength(c) => isVarWidth(c) && c.references.nonEmpty
    case _ => false
  }

  private def isVarWidth(e: Expression): Boolean = e.dataType match {
    case _: StringType | BinaryType => e.isInstanceOf[Attribute]
    case _ => false
  }

  private def rewrite(p: Project, list: Seq[NamedExpression], j: Join): Option[LogicalPlan] = {
    val smallerIsLeft = j.left.stats.sizeInBytes <= j.right.stats.sizeInBytes
    val side = if (smallerIsLeft) j.left else j.right
    if (!j.output.exists(a => side.outputSet.contains(a))) return None // a semi/anti join's right side
    val pushed = list.collect {
      case a @ Alias(e, _) if narrowing(e) && e.references.subsetOf(side.outputSet) && e.deterministic => a
    }
    if (pushed.isEmpty) return None
    val pushedIds = pushed.map(_.exprId).toSet
    val above = list.filterNot(e => pushedIds.contains(e.exprId))
    // What the rest of the plan still needs from the side: the remaining projections and the join condition.
    val needed =
      AttributeSet(above.flatMap(_.references)) ++ j.condition.map(_.references).getOrElse(AttributeSet.empty)
    val keep = side.output.filter(needed.contains)
    val newSide = Project(keep ++ pushed, side)
    val newJoin = if (smallerIsLeft) j.copy(left = newSide) else j.copy(right = newSide)
    val newList = list.map(e => if (pushedIds.contains(e.exprId)) e.toAttribute else e)
    Some(Project(newList, newJoin))
  }
}
