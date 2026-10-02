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
package org.apache.spark.sql.execution.vector

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.sources.Filter

/**
 * `FileSourceScanLike.pushedDownFilters` is `protected` in the trait, so Scala refuses a direct call even
 * from inside the `execution` package tree; the compiled class nonetheless carries a public accessor. This
 * bridge calls it reflectively so [[org.apache.spark.sql.vecruntime.VectorParquetScanExec]] can reuse the
 * data filters Spark already pushed for row-group / page skipping. Reflection failure yields no filters --
 * row-group skipping is a correctness-preserving optimization, so an empty result only forgoes the skip.
 */
object FileScanAccess {

  def pushedDownFilters(scan: FileSourceScanExec): Seq[Filter] = {
    try {
      val m = scan.getClass.getMethod("pushedDownFilters")
      m.setAccessible(true)
      m.invoke(scan) match {
        case s: scala.collection.immutable.Seq[_] => s.asInstanceOf[Seq[Filter]]
        case s: java.util.List[_] => s.asScala.toSeq.asInstanceOf[Seq[Filter]]
        case _ => Seq.empty
      }
    } catch {
      case _: Throwable => Seq.empty
    }
  }

  /**
   * Start and WAIT for all of the scan's subqueries (dynamic-partition-pruning, scalar, and REUSED
   * subqueries in its partition filters). `SparkPlan.prepareSubqueries` / `waitForSubqueries` are
   * `protected`, so a wrapping node cannot call them directly; invoked reflectively here. Without waiting,
   * reading the scan's selected partitions throws "... has not finished" for a subquery that was started but
   * never awaited. Idempotent.
   */
  def prepareAndWaitForSubqueries(scan: FileSourceScanExec): Unit = {
    val cls = classOf[org.apache.spark.sql.execution.SparkPlan]
    val prepare = cls.getDeclaredMethod("prepareSubqueries")
    prepare.setAccessible(true)
    prepare.invoke(scan)
    val wait = cls.getDeclaredMethod("waitForSubqueries")
    wait.setAccessible(true)
    wait.invoke(scan)
  }
}
