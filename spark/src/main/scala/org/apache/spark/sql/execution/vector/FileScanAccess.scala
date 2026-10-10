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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.{FileSourceScanExec, ListingPartition, PartitionedFileUtil, ScanFileListing}
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition}
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
   * The scan's file partitions for a non-bucketed read, planned exactly as `FileSourceScanExec.createReadRDD`
   * plans them (Spark 4.1 / 4.2: the same code in both), but WITHOUT materialising `scan.inputRDD` (#672).
   * Building `inputRDD` calls `ParquetFileFormat.buildReaderWithPartitionValues`, which broadcasts the whole
   * Hadoop conf for a reader we never run; `Configuration.write` gzips every key and value with a fresh
   * zlib stream, about 2,000 short-lived 45 KB mallocs per broadcast, on the driver, per planned scan.
   *
   * `dynamicallySelectedPartitions` is `protected` in the trait (public in the compiled class) and is read
   * reflectively, like [[pushedDownFilters]]. None for a bucketed scan, or when the accessor is missing: the
   * caller then falls back to `scan.inputRDD.partitions`.
   */
  def filePartitions(scan: FileSourceScanExec): Option[Seq[FilePartition]] =
    if (scan.bucketedScan) None
    else dynamicallySelectedPartitions(scan).map(planReadPartitions(scan, _))

  private def dynamicallySelectedPartitions(scan: FileSourceScanExec): Option[ScanFileListing] =
    try {
      val m = scan.getClass.getMethod("dynamicallySelectedPartitions")
      m.setAccessible(true)
      Option(m.invoke(scan)).collect { case l: ScanFileListing => l }
    } catch {
      // NoSuchMethodException / IllegalAccessException only: an exception the listing itself throws (a
      // subquery that has not finished, a file-listing failure) must surface, as it would from inputRDD.
      case _: NoSuchMethodException | _: IllegalAccessException => None
      case e: java.lang.reflect.InvocationTargetException if e.getCause != null => throw e.getCause
    }

  /** `FileSourceScanExec.createReadRDD`'s partition planning, without its reader. */
  private def planReadPartitions(scan: FileSourceScanExec, selected: ScanFileListing): Seq[FilePartition] = {
    val relation = scan.relation
    val session = relation.sparkSession
    val maxSplitBytes = FilePartition.maxSplitBytes(session, selected)
    val shouldProcess: Path => Boolean = scan.optionalBucketSet match {
      case Some(bucketSet) if session.sessionState.conf.bucketingEnabled =>
        // Do not prune the file if bucket file name is invalid (as Spark).
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ => _ => true
    }
    val splitFiles = selected.filePartitionIterator.flatMap { partition =>
      val ListingPartition(partitionVals, _, fileStatusIterator) = partition
      fileStatusIterator.flatMap { file =>
        val filePath = file.getPath
        if (shouldProcess(filePath)) {
          val isSplitable = relation.fileFormat.isSplitable(session, relation.options, filePath)
          PartitionedFileUtil.splitFiles(
            file = file,
            filePath = filePath,
            isSplitable = isSplitable,
            maxSplitBytes = maxSplitBytes,
            partitionValues = partitionVals
          )
        } else Seq.empty
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    FilePartition.getFilePartitions(session, splitFiles.toSeq, maxSplitBytes)
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
