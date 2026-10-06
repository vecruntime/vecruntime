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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, IsNotNull, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

/**
 * Column statistics a planner rule can use when they exist (#650), whatever produced them:
 * - Spark `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS`: the catalog table's `CatalogStatistics`, read
 *   directly so they count without `spark.sql.cbo.enabled`;
 * - a DSv2 source that reports column statistics (`SupportsReportStatistics.columnStats`), as Iceberg does
 *   from Puffin theta sketches: the relation's `Statistics.attributeStats`.
 * A key is traced through projections (an alias of a plain column), filters and joins to the relation that
 * produces it; anything else (an expression over several columns, an aggregate) has no statistics here.
 */
private[vecruntime] object KeyStats extends PredicateHelper {

  /** The base relation's distinct count of the column `key` traces to, if known. */
  def distinctCount(plan: LogicalPlan, key: Expression): Option[BigInt] = key match {
    case a: Attribute => trace(plan, a)
    case _ => None
  }

  private def trace(plan: LogicalPlan, a: Attribute): Option[BigInt] = plan match {
    case Project(list, child) =>
      list.collectFirst {
        case x: Attribute if x.exprId == a.exprId => trace(child, x)
        case al @ Alias(src: Attribute, _) if al.exprId == a.exprId => trace(child, src)
      }.flatten
    case Filter(_, child) => trace(child, a)
    case SubqueryAlias(_, child) => trace(child, a)
    case j: Join =>
      if (j.left.outputSet.contains(a)) trace(j.left, a)
      else if (j.right.outputSet.contains(a)) trace(j.right, a)
      else None
    case rel: LogicalRelation =>
      rel.catalogTable.flatMap(_.stats).flatMap(_.colStats.get(a.name)).flatMap(_.distinctCount)
    case rel: DataSourceV2ScanRelation => fromStats(rel, a)
    case rel: DataSourceV2Relation => fromStats(rel, a)
    case _ => None
  }

  private def fromStats(rel: LeafNode, a: Attribute): Option[BigInt] =
    rel.stats.attributeStats.get(a).flatMap(_.distinctCount)

  /**
   * Whether `plan` is reduced below its base relations by a filter: any conjunct but `IS NOT NULL` of a plain
   * column, which is what the optimizer infers from join keys and removes no row a join would keep.
   */
  def reduced(plan: LogicalPlan): Boolean = plan.exists {
    case Filter(cond, _) =>
      splitConjunctivePredicates(cond).exists {
        case IsNotNull(_: Attribute) => false
        case _ => true
      }
    case _ => false
  }
}
