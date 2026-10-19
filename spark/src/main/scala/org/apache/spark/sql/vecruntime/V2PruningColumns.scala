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

import scala.jdk.CollectionConverters._

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
   * Iceberg transforms (their `toString`) whose inclusive projection turns a filter on the source column into a
   * filter on the partition value: identity, bucket[n], truncate[w], year, month, day, hour. Not `void` (always
   * null, projects to true) and not a transform Iceberg does not know.
   */
  private val PruningTransform = """(identity|bucket\[\d+\]|truncate\[\d+\]|year|month|day|hour)""".r

  /**
   * The columns a dynamic pruning filter prunes `r` by, top level only. For an Iceberg table (#806): the source
   * column of every partition field that prunes, in **every** partition spec the table has had, the columns an
   * Iceberg scan reports as its runtime filter attributes (`SparkRuntimeFilterableScan.filterAttributes`). The scan
   * projects the runtime filter through each data file's own spec (`Projections.inclusive`), so a file written
   * under an older spec is pruned by that spec's fields: `days(ts)` keeps the matching days, `bucket(n, k)` the
   * matching buckets, and a column an older spec partitioned by still prunes that spec's files after it was dropped
   * from the current one. Other DSv2 tables: their identity partition columns.
   */
  def of(r: DataSourceV2Relation): AttributeSet =
    scala.util.Try {
      val names = icebergSourceColumns(r.table).getOrElse(identityNames(r))
      matching(r, names)
    }.getOrElse(AttributeSet.empty)

  /**
   * `r`'s identity partition columns in its current spec (top level): the columns it is pruned by value, not by a
   * coarser partition value.
   */
  def identityColumns(r: DataSourceV2Relation): AttributeSet =
    scala.util.Try(matching(r, identityNames(r))).getOrElse(AttributeSet.empty)

  private def identityNames(r: DataSourceV2Relation): Seq[String] =
    r.table.partitioning().toSeq.collect {
      case t: IdentityTransform if t.ref.fieldNames().length == 1 => t.ref.fieldNames()(0)
    }

  private def matching(r: DataSourceV2Relation, names: Seq[String]): AttributeSet = {
    val resolver = org.apache.spark.sql.internal.SQLConf.get.resolver
    AttributeSet(r.output.filter(a => names.exists(n => resolver(n, a.name))))
  }

  /**
   * An Iceberg `SparkTable`'s pruning source columns over all its specs, reached reflectively (the plugin has no
   * compile-time Iceberg dependency); `None` for another table. A source column the current schema no longer has,
   * or a nested one, is left out: a pruning key is a top-level output column.
   */
  private def icebergSourceColumns(table: AnyRef): Option[Seq[String]] = {
    val isSparkTable = Iterator.iterate[Class[_]](table.getClass)(_.getSuperclass).takeWhile(_ != null)
      .exists(_.getName == "org.apache.iceberg.spark.source.SparkTable")
    if (!isSparkTable) return None
    Option(table.getClass.getMethod("table").invoke(table)).map { t =>
      def call(target: AnyRef, name: String): AnyRef = target.getClass.getMethod(name).invoke(target)
      val schema = call(t, "schema")
      val findColumnName = schema.getClass.getMethod("findColumnName", classOf[Int])
      val specs = call(t, "specs").asInstanceOf[java.util.Map[_, AnyRef]].values().asScala.toSeq
      specs.flatMap { spec =>
        call(spec, "fields").asInstanceOf[java.util.List[AnyRef]].asScala.flatMap { field =>
          val transform = call(field, "transform").toString
          val sourceId = call(field, "sourceId").asInstanceOf[Integer]
          val name = findColumnName.invoke(schema, sourceId).asInstanceOf[String]
          if (PruningTransform.pattern.matcher(transform).matches() && name != null && !name.contains('.')) Some(name)
          else None
        }
      }.distinct
    }
  }

  /**
   * A pruning subquery's dimension plan, its DSv2 relations scanned: the optimizer has already pushed DSv2 scans
   * down (and does not again for a plan that only lives inside a `DynamicPruningSubquery`), and Spark plans no
   * unscanned `DataSourceV2Relation`. A plan without DSv2 relations is returned as it is.
   */
  def scanned(plan: LogicalPlan): LogicalPlan =
    if (plan.exists(_.isInstanceOf[DataSourceV2Relation])) V2ScanRelationPushDown(plan) else plan
}
