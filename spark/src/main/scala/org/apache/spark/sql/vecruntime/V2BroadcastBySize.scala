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
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Filter, HintInfo, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * #735: a small Iceberg join input broadcast as its Parquet copy would be.
 *
 * Spark broadcasts a join input whose estimated size is at most `spark.sql.autoBroadcastJoinThreshold`. A file
 * table's estimate is its files' bytes (compressed); an Iceberg scan reports its rows times Spark's default column
 * widths instead, several times larger: at 3 TB TPC-DS `customer_demographics` is 7.8 MB of Parquet and estimated
 * above the 10 MB threshold, so q7 and q26 plan shuffle joins and AQE turns them into broadcast joins only after
 * both sides have been shuffled -- two extra shuffles of the fact, q7 23 s against 12 s with a broadcast.
 *
 * A join input that is one DSv2 scan under projections and filters, too large to broadcast by its estimate, is
 * estimated again by the bytes of the files its scan planned (Iceberg's task groups), scaled by the share of a row
 * the scan reads and by what the input keeps of the scan's estimate (`byFiles`); at most the threshold, it gets a
 * broadcast hint, which Spark's join selection takes when the join type allows that side to be broadcast. The
 * uncompressed estimate must stay within `spark.vecruntime.optimizer.v2BroadcastBySize.maxEstimatedBytes`, so a
 * table that compresses unusually well is not broadcast whole. A join that already carries a hint is left alone.
 * Off with `spark.vecruntime.optimizer.v2BroadcastBySize.enabled=false`, and with the plugin.
 */
case class V2BroadcastBySize(session: SparkSession) extends Rule[LogicalPlan] with JoinSelectionHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    val threshold = conf.autoBroadcastJoinThreshold
    if (!VectorConf.v2BroadcastBySizeEnabled(conf) || threshold <= 0) return plan
    val maxEstimated = VectorConf.v2BroadcastBySizeMaxEstimatedBytes(conf)
    def small(side: LogicalPlan): Boolean = {
      val estimate = side.stats.sizeInBytes
      estimate > threshold && V2BroadcastBySize.smallByFiles(side, threshold, maxEstimated)
    }
    plan.transformUp {
      case j @ Join(left, right, jt, _, hint) if hint.leftHint.isEmpty && hint.rightHint.isEmpty =>
        val broadcast = Some(HintInfo(strategy = Some(BROADCAST)))
        val r = canBuildBroadcastRight(jt) && small(right)
        val l = canBuildBroadcastLeft(jt) && small(left)
        // The smaller side when both qualify, as Spark picks the build side by size.
        if (r && (!l || right.stats.sizeInBytes <= left.stats.sizeInBytes))
          j.copy(hint = hint.copy(rightHint = broadcast))
        else if (l) j.copy(hint = hint.copy(leftHint = broadcast))
        else j
    }
  }
}

object V2BroadcastBySize {

  /** The DSv2 scan `p` reads when `p` is that scan under projections and filters only. */
  def scanOf(p: LogicalPlan): Option[DataSourceV2ScanRelation] = p match {
    case Project(_, child) => scanOf(child)
    case Filter(_, child) => scanOf(child)
    case s: DataSourceV2ScanRelation => Some(s)
    case _ => None
  }

  /**
   * Whether `side` -- one DSv2 scan under projections and filters -- is small enough to broadcast by the bytes of
   * the files its scan planned, as its Parquet copy would be judged: its own (uncompressed) estimate within
   * `maxEstimated` so a table that compresses unusually well is not broadcast whole, and the file-byte estimate
   * (`byFiles`) at most `threshold`. Shared by the broadcast-hint rule and [[CountBelowJoin]], which must not
   * collapse-and-shuffle a dimension its Parquet copy keeps a broadcast (#735, q89).
   */
  def smallByFiles(side: LogicalPlan, threshold: Long, maxEstimated: Long): Boolean =
    side.stats.sizeInBytes <= maxEstimated &&
      scanOf(side).flatMap(byFiles(side, _)).exists(_ <= threshold)

  /**
   * `side`'s size by the files its scan planned, as a file table's input is estimated: the files' bytes times the
   * share of a row the scan reads (Spark's row-size estimate of the scan's columns against the table's, as Spark
   * scales a projection of a file scan), times what `side` keeps of the scan's own estimate. The scan's estimate
   * already counts only the columns it reads, the files every column, so without the column share a narrow read of
   * a wide table (q82's `item`, 5 of 22 columns) would look several times larger than its Parquet copy.
   */
  def byFiles(side: LogicalPlan, scan: DataSourceV2ScanRelation): Option[BigInt] = {
    val whole = scan.stats.sizeInBytes
    if (whole <= 0) None
    else
      plannedBytes(scan).map { files =>
        val read = EstimationUtils.getSizePerRow(scan.output)
        val table = EstimationUtils.getSizePerRow(scan.relation.output)
        val columns = if (table > 0) BigInt(files) * read / table else BigInt(files)
        columns.min(BigInt(files)) * side.stats.sizeInBytes / whole
      }
  }

  /**
   * The bytes of the files an Iceberg scan planned (its task groups' `sizeBytes`), or None for a scan of another
   * source or when they cannot be read. Iceberg plans them at most once per scan and keeps them for execution.
   */
  def plannedBytes(scan: DataSourceV2ScanRelation): Option[Long] =
    if (!scan.scan.getClass.getName.startsWith("org.apache.iceberg.spark.source.")) None
    else
      scala.util.Try {
        var c: Class[_] = scan.scan.getClass
        var m: java.lang.reflect.Method = null
        while (m == null && c != null) {
          m = c.getDeclaredMethods.find(x => x.getName == "taskGroups" && x.getParameterCount == 0).orNull
          c = c.getSuperclass
        }
        m.setAccessible(true)
        val groups = m.invoke(scan.scan).asInstanceOf[java.util.List[AnyRef]]
        // Through the public interface: a task group's own class may not be public.
        val sizeBytes = Class.forName("org.apache.iceberg.ScanTaskGroup", false, scan.scan.getClass.getClassLoader)
          .getMethod("sizeBytes")
        var bytes = 0L
        groups.forEach(g => bytes += sizeBytes.invoke(g).asInstanceOf[java.lang.Long].longValue)
        bytes
      }.toOption
}
