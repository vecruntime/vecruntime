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

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2ScanRelationPushDown}

/**
 * #735: DSv2 (Iceberg) tables in our dynamic partition pruning rules (`TransitiveDpp`, `DppThroughAggregate`), which
 * run before Spark scans a DSv2 relation, so they see the `DataSourceV2Relation` itself.
 */
private[vecruntime] object V2PruningColumns {

  /**
   * The columns a dynamic pruning filter prunes `r` by: its table's identity partition columns (top level), the
   * counterpart of a file table's partition columns -- an Iceberg scan reports them as its runtime filter attributes
   * and prunes files by them. Empty for a table without identity partitions.
   */
  def of(r: DataSourceV2Relation): AttributeSet =
    scala.util.Try {
      val names = r.table.partitioning().toSeq.collect {
        case t: IdentityTransform if t.ref.fieldNames().length == 1 => t.ref.fieldNames()(0)
      }
      val resolver = org.apache.spark.sql.internal.SQLConf.get.resolver
      AttributeSet(r.output.filter(a => names.exists(n => resolver(n, a.name))))
    }.getOrElse(AttributeSet.empty)

  /**
   * A pruning subquery's dimension plan, its DSv2 relations scanned: the optimizer has already pushed DSv2 scans
   * down (and does not again for a plan that only lives inside a `DynamicPruningSubquery`), and Spark plans no
   * unscanned `DataSourceV2Relation`. A plan without DSv2 relations is returned as it is.
   */
  def scanned(plan: LogicalPlan): LogicalPlan =
    if (plan.exists(_.isInstanceOf[DataSourceV2Relation])) V2ScanRelationPushDown(plan) else plan
}
