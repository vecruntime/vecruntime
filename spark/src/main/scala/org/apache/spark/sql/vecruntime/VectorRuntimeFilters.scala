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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.{InnerLike, LeftSemi}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

/**
 * Runtime filters from broadcast build keys (#610). A broadcast hash join whose streamed side reaches a
 * native Parquet scan through filters, projections and the streamed sides of other inner joins hands the
 * scan, per join key that is a plain data column there, the build side's key domain: an IN list when the
 * build has few distinct values, otherwise the [min, max] range of an integer key. The scan ANDs them with
 * its static pushed filters, so row groups and pages that hold no key the build side has are skipped
 * through the existing statistics, dictionary and column-index paths (Parquet's IN conversion turns a long
 * list into its range, `spark.sql.parquet.pushdown.inFilterThreshold`).
 *
 * Only for joins that drop a streamed row without a match: inner joins and left semi joins with the build
 * on the right. A null key never matches, and neither filter keeps one.
 */
object VectorRuntimeFilters {

  /** The supported key types: integer and string columns. */
  private def supported(dt: DataType): Boolean = dt match {
    case IntegerType | LongType | StringType => true
    case _ => false
  }

  /** Whether `join` drops every streamed row without a match. */
  def eligible(join: VectorBroadcastHashJoinExec): Boolean = !join.isNullAwareAntiJoin && (join.joinType match {
    case _: InnerLike => true
    case LeftSemi => join.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildRight
    case _ => false
  })

  /**
   * The native scan under `plan` producing `a` unchanged, and its attribute there: through filters,
   * projections (a column, or an alias of one) and the streamed side of an eligible join, which passes its
   * streamed rows' columns through.
   */
  def scanFor(plan: SparkPlan, a: Attribute): Option[(VectorParquetScanExec, Attribute)] = plan match {
    case s: VectorParquetScanExec => s.output.find(_.exprId == a.exprId).map(x => (s, x))
    case f: VectorFilterExec => scanFor(f.child, a)
    case p: VectorProjectExec =>
      p.projectList.find(_.exprId == a.exprId).collect {
        case r: AttributeReference => r
        case Alias(r: AttributeReference, _) => r
      }.flatMap(scanFor(p.child, _))
    case j: VectorBroadcastHashJoinExec if eligible(j) && j.streamedPlan.output.exists(_.exprId == a.exprId) =>
      scanFor(j.streamedPlan, a)
    case _ => None
  }

  /**
   * Attaches the filters of `join`'s keys, read from its build table (built on the driver from the
   * broadcast, the same shared table its tasks use when they run in the driver's JVM), to the scans its
   * streamed keys reach. `inMax`: the most distinct values sent as an IN list.
   */
  def attach(join: VectorBroadcastHashJoinExec, build: BuildTable, inMax: Int): Unit = {
    if (!eligible(join) || build.numRows == 0) return
    val table = build.table
    val groups = table.size()
    join.streamedKeys.zipWithIndex.foreach {
      case (a: Attribute, k) if supported(a.dataType) =>
        scanFor(join.streamedPlan, a).foreach { case (scan, col) =>
          if (scan.isDataColumn(col.name))
            filterOf(col.name, col.dataType, table, k, groups, inMax).foreach(scan.addRuntimeFilter)
        }
      case _ =>
    }
  }

  /** The filter of key column `k` over its `groups` keys, or None when it would keep everything. */
  private[vecruntime] def filterOf(
      name: String,
      dt: DataType,
      table: io.vecruntime.kernels.GroupKeyTable,
      k: Int,
      groups: Int,
      inMax: Int
  ): Option[sources.Filter] = {
    val values = new java.util.HashSet[Any]()
    var g = 0
    while (g < groups && values.size() <= inMax) {
      if (!table.isNull(k, g)) values.add(value(dt, table, k, g))
      g += 1
    }
    if (values.isEmpty) Some(sources.In(name, Array.empty[Any])) // no key can match
    else if (values.size() <= inMax) Some(sources.In(name, values.toArray))
    else dt match {
      case IntegerType | LongType =>
        var lo = Long.MaxValue
        var hi = Long.MinValue
        g = 0
        while (g < groups) {
          if (!table.isNull(k, g)) {
            val v = if (dt == IntegerType) table.getInt(k, g).toLong else table.getLong(k, g)
            if (v < lo) lo = v
            if (v > hi) hi = v
          }
          g += 1
        }
        def lit(v: Long): Any = if (dt == IntegerType) v.toInt else v
        Some(sources.And(sources.GreaterThanOrEqual(name, lit(lo)), sources.LessThanOrEqual(name, lit(hi))))
      case _ => None
    }
  }

  private def value(dt: DataType, table: io.vecruntime.kernels.GroupKeyTable, k: Int, g: Int): Any = dt match {
    case IntegerType => table.getInt(k, g)
    case LongType => table.getLong(k, g)
    case StringType => table.getString(k, g)
    case other => throw new IllegalArgumentException(s"no runtime filter for $other")
  }
}
