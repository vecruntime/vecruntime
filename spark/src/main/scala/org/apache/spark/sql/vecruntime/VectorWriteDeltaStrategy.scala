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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, WriteDelta}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/**
 * Planner strategy for the columnar v3 deletion-vector writer (#20, slice 4-live). It intercepts the
 * logical [[WriteDelta]] -- the merge-on-read DELETE/UPDATE/MERGE write -- and, when the target writes
 * deletion vectors (format version 3) and the write is delete-only, plans [[VectorWriteDeltaExec]] so
 * the DELETE half runs columnar. For anything else -- the flag off, the bridge/Iceberg absent, a v2
 * table, or a write that also inserts rows (UPDATE/MERGE with a data half, out of scope until slice 5)
 * -- it returns `Nil`, so Spark's own `DataSourceV2Strategy` plans the ordinary `WriteDeltaExec` and
 * the fallback is Spark's real writer, unchanged. The reason is logged when explain-fallback is on.
 *
 * Injected through `SparkSessionExtensions.injectPlannerStrategy`; strategies Spark tries before its
 * own, so returning a non-empty plan wins and returning `Nil` cedes to Spark.
 */
case class VectorWriteDeltaStrategy(session: SparkSession) extends SparkStrategy with Logging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case wd: WriteDelta =>
      val conf = session.sessionState.conf
      decline(wd, conf) match {
        case Some(reason) =>
          if (VectorConf.explainFallback(conf)) logInfo(s"spark-vector: v3 DV writer declined -- $reason")
          Nil
        case None =>
          val icebergTable = VectorWriteDeltaStrategy.icebergTableOf(wd.table)
          Seq(
            VectorWriteDeltaExec(
              icebergTable.get,
              wd.write.get,
              wd.projections,
              planLater(wd.query),
              refreshCache(wd.originalTable)
            )
          )
      }
    case _ => Nil
  }

  /**
   * Recaches the target after the commit, as Spark's `DataSourceV2Strategy` does for its own
   * `WriteDeltaExec`: by catalog name for a catalog table (without time travel), else by plan.
   */
  private def refreshCache(original: LogicalPlan): () => Unit = { () =>
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    // The cache manager takes the classic (non-Connect) session, which is what a planner strategy
    // injected into a running session always receives.
    val classic = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    val cacheManager = classic.sharedState.cacheManager
    original match {
      case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation =>
        (r.catalog, r.identifier) match {
          case (Some(catalog), Some(ident)) =>
            cacheManager.recacheTableOrView(classic, ident.toQualifiedNameParts(catalog), includeTimeTravel = false)
          case _ => cacheManager.recacheByPlan(classic, r)
        }
      case other => cacheManager.recacheByPlan(classic, other)
    }
  }

  /** Some(reason) to leave the write to Spark; None to take it columnar. */
  private def decline(wd: WriteDelta, conf: org.apache.spark.sql.internal.SQLConf): Option[String] = {
    if (!VectorConf.icebergDvWriterEnabled(conf)) return Some("spark.vecruntime.iceberg.dvWriter.enabled is off")
    if (!IcebergDvBridge.isAvailable) return Some("iceberg-bridge module is not on the classpath")
    if (wd.write.isEmpty) return Some("the DeltaWrite is not resolved")
    // DELETE, UPDATE and MERGE: an insert half (row projection) is written by Iceberg's own writer.
    if (wd.projections.metadataProjection.isEmpty)
      return Some("write has no metadata projection (spec id / partition)")
    val table = VectorWriteDeltaStrategy.icebergTableOf(wd.table)
    if (table.isEmpty) return Some("target is not an Iceberg table")
    if (!IcebergDvBridge.isDvEligible(table.get))
      return Some("target is not a format-version-3 (deletion-vector) table")
    // Partitioned tables and tables that already carry deletes are both supported: the spec id and
    // partition tuple come from the metadata projection, and each data file's previously committed
    // DV is merged into its new one and replaced in the commit (Iceberg's rewritableDeletes), exactly
    // as Iceberg's own writer does.
    None
  }
}

object VectorWriteDeltaStrategy {

  /**
   * The underlying Iceberg `Table` behind a `WriteDelta`'s target relation, or None if it is not an
   * Iceberg v2-catalog table. Resolved reflectively (the core has no Iceberg compile dep): a
   * `DataSourceV2Relation.table` that is Iceberg's `SparkTable` exposes `table()` returning the
   * `org.apache.iceberg.Table`.
   */
  def icebergTableOf(relation: AnyRef): Option[AnyRef] = {
    try {
      // Walk .table() accessors down to the org.apache.iceberg.Table: a DELETE's target is a
      // DataSourceV2Relation whose table() is Spark's RowLevelOperationTable, whose table() is
      // Iceberg's SparkTable (a connector Table), whose table() is the org.apache.iceberg.Table.
      var cur: AnyRef = invokeNoArg(relation, "table").orNull
      var hops = 0
      while (cur != null && hops < 5) {
        if (isIcebergTable(cur.getClass)) return Some(cur)
        cur = invokeNoArg(cur, "table").orNull
        hops += 1
      }
      None
    } catch { case _: Throwable => None }
  }

  private def invokeNoArg(target: AnyRef, name: String): Option[AnyRef] =
    target.getClass.getMethods
      .find(m => m.getName == name && m.getParameterCount == 0)
      .flatMap(m => Option(m.invoke(target)))

  private def isIcebergTable(c: Class[_]): Boolean = {
    var cur: Class[_] = c
    while (cur != null) {
      if (cur.getName == "org.apache.iceberg.Table") return true
      if (cur.getInterfaces.exists(i => isIcebergTable(i))) return true
      cur = cur.getSuperclass
    }
    false
  }
}
