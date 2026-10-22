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

import java.lang.foreign.Arena

import scala.collection.mutable

import io.vecruntime.spark.VectorConf
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{
  AppendData,
  LogicalPlan,
  OverwriteByExpression,
  OverwritePartitionsDynamic,
  ReplaceData
}
import org.apache.spark.sql.connector.write.{MergeSummaryImpl, Write, WriteSummary, WriterCommitMessage}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, RowToColumnarExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2CommandExec}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * Planner strategy for the columnar Iceberg data-file writer (#842, W2). It takes the logical writes whose data
 * files Iceberg writes row by row -- `AppendData` (`INSERT INTO`), `OverwriteByExpression` and
 * `OverwritePartitionsDynamic` (`INSERT OVERWRITE`), and `ReplaceData` (copy-on-write `DELETE` / `UPDATE` /
 * `MERGE INTO`) -- and, when the target is an Iceberg table the columnar writer covers, plans
 * [[VectorIcebergWriteExec]]. Anything else returns `Nil`, so Spark's `DataSourceV2Strategy` plans its own
 * exec and Iceberg's writer, unchanged; the reason is logged when explain-fallback is on.
 */
case class VectorIcebergWriteStrategy(session: SparkSession) extends SparkStrategy with Logging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case a: AppendData => take(plan, a.table, a.query, a.write, None)
    case o: OverwriteByExpression => take(plan, o.table, o.query, o.write, None)
    case o: OverwritePartitionsDynamic => take(plan, o.table, o.query, o.write, None)
    case r: ReplaceData =>
      val conf = session.sessionState.conf
      // The write's columns, in the order of Iceberg's write schema: the row's data columns from the row
      // projection and, on a format v3 table, the row lineage the copied rows keep (_row_id and
      // _last_updated_sequence_number, which Iceberg appends to the write schema) from the metadata projection.
      val row = r.projections.rowProjection
      val meta = r.projections.metadataProjection
      def ordinal(name: String): Option[Int] = {
        val inRow = row.schema.fieldNames.indexOf(name)
        if (inRow >= 0) Some(row.colOrdinals(inRow).asInstanceOf[Int])
        else meta.flatMap { m =>
          val k = m.schema.fieldNames.indexOf(name)
          if (k >= 0) Some(m.colOrdinals(k).asInstanceOf[Int]) else None
        }
      }
      r.write match {
        case Some(w) if IcebergWriteBridge.isAvailable && IcebergWriteBridge.isSparkWrite(w) =>
          val names = IcebergWriteBridge.writeSchemaNames(w)
          val ordinals = names.map(ordinal)
          if (ordinals.exists(_.isEmpty)) {
            declined(s"write column ${names(ordinals.indexWhere(_.isEmpty))} is in neither projection")
          } else {
            // #901 M4a: a copy-on-write DELETE (no row data changes, just rows dropped) can copy the row
            // groups it does not touch, when the flag is on, the target is unpartitioned, and the scan
            // projects _file / _pos (Iceberg's copy-on-write scan does). Anything else writes every group.
            val copy =
              if (
                VectorConf.icebergCopyUnchangedEnabled(conf) &&
                VectorIcebergWriteStrategy.isDelete(r) &&
                IcebergWriteBridge.isUnpartitioned(w)
              ) {
                (ordinal("_file"), ordinal("_pos")) match {
                  case (Some(f), Some(p)) => Some((f, p))
                  case _ => None
                }
              } else None
            take(plan, r.table, r.query, r.write, Some(ordinals.map(_.get)), r.originalTable, copy)
          }
        case _ => take(plan, r.table, r.query, r.write, None, r.originalTable)
      }
    case _ => Nil
  }

  private def declined(reason: String): Seq[SparkPlan] = {
    if (VectorConf.explainFallback(session.sessionState.conf)) {
      logInfo(s"spark-vector: columnar Iceberg writer declined -- $reason")
    }
    Nil
  }

  private def take(
      plan: LogicalPlan,
      table: LogicalPlan,
      query: LogicalPlan,
      write: Option[Write],
      ordinals: Option[Array[Int]],
      original: LogicalPlan = null,
      copy: Option[(Int, Int)] = None
  ): Seq[SparkPlan] = {
    val conf = session.sessionState.conf
    val reason =
      if (!VectorConf.icebergWriterEnabled(conf)) Some(s"${VectorConf.IcebergWriterEnabled} is off")
      else if (!IcebergWriteBridge.isAvailable) Some("the iceberg-bridge module is not on the classpath")
      else if (write.isEmpty) Some("the write is not resolved")
      else if (!IcebergWriteBridge.isSparkWrite(write.get)) Some("the target is not an Iceberg table")
      else IcebergWriteBridge.declineReason(write.get)
    reason match {
      case Some(r) => declined(r)
      case None =>
        val rowOrdinals = ordinals.getOrElse(query.output.indices.toArray)
        Seq(
          VectorIcebergWriteExec(
            write.get,
            IcebergWriteBridge.specOf(write.get),
            rowOrdinals,
            planLater(query),
            VectorIcebergWriteStrategy.refreshCache(session, Option(original).getOrElse(table)),
            plan.nodeName,
            copy.map(_._1).getOrElse(-1),
            copy.map(_._2).getOrElse(-1)
          )
        )
    }
  }
}

object VectorIcebergWriteStrategy {

  /**
   * Recaches the target after the commit, as Spark's `DataSourceV2Strategy` does for its own write execs: by
   * catalog name for a catalog table (without time travel), else by plan.
   */
  def refreshCache(session: SparkSession, target: LogicalPlan): () => Unit = { () =>
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val classic = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    val cacheManager = classic.sharedState.cacheManager
    target match {
      case r: DataSourceV2Relation =>
        (r.catalog, r.identifier) match {
          case (Some(catalog), Some(ident)) =>
            cacheManager.recacheTableOrView(classic, ident.toQualifiedNameParts(catalog), includeTimeTravel = false)
          case _ => cacheManager.recacheByPlan(classic, r)
        }
      case other => cacheManager.recacheByPlan(classic, other)
    }
  }

  /**
   * Whether a `ReplaceData` is a copy-on-write DELETE (as opposed to UPDATE or MERGE): every output row is an
   * unchanged copy of a surviving source row, so a delete-free row group is byte-identical and can be copied.
   * An UPDATE or MERGE rewrites column values, so a "delete-free" group can still hold changed rows and must not
   * be copied. Read from the operation's command (`RowLevelOperation.Command.DELETE`).
   */
  def isDelete(r: ReplaceData): Boolean =
    try r.operation.command() == org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
    catch { case _: Throwable => false }
}

/**
 * The columnar Iceberg data-file write (#842, W2). Each task turns its batches into the engine's columns (the
 * write's row columns, at `rowOrdinals` of the child's output, compacted to any selection, as
 * [[VectorWriteFilesExec]] does) and writes them through the bridge's `IcebergDataTaskWriter` --
 * `IcebergParquetAppender` over the columnar Parquet writer, files rolled at the write's target size and named by
 * Iceberg's own `OutputFileFactory` -- then returns Iceberg's own task commit message. The driver commits through
 * the `BatchWrite` of the same Iceberg `Write` (`write.toBatch`): an append, an overwrite or a copy-on-write
 * rewrite, with Iceberg's snapshot properties, isolation checks and metrics. On failure it aborts through Iceberg.
 *
 * The child is read columnar where it is: Spark puts a row transition between a command and its child, and
 * adaptive execution plans the child on its own, so the batches are taken from below the transition of the
 * (final) child plan; a child that really produces rows is converted with Spark's `RowToColumnarExec`.
 */
case class VectorIcebergWriteExec(
    write: Write,
    spec: AnyRef,
    rowOrdinals: Array[Int],
    query: SparkPlan,
    refreshCache: () => Unit,
    operation: String,
    fileOrdinal: Int = -1,
    posOrdinal: Int = -1
) extends V2CommandExec
    with AdaptiveSparkPlanHelper
    with Logging {

  override def output: Seq[Attribute] = Nil
  override def children: Seq[SparkPlan] = Seq(query)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(query = newChildren.head)

  override lazy val metrics: Map[String, org.apache.spark.sql.execution.metric.SQLMetric] = Map(
    "rowInput" -> org.apache.spark.sql.execution.metric.SQLMetrics.createMetric(sparkContext, "child read as rows")
  )

  /** Whether this write copies the delete-free row groups of the files it rewrites (#901 M4a). */
  private def copyUnchanged: Boolean = fileOrdinal >= 0 && posOrdinal >= 0

  override def simpleString(maxFields: Int): String =
    s"VectorIcebergWriteExec $operation${if (copyUnchanged) " (copy-unchanged)" else ""}"

  override protected def run(): Seq[InternalRow] = {
    val batchWrite = write.toBatch
    val (rdd, fromRows) = VectorIcebergWriteExec.columnar(query)
    if (fromRows) longMetric("rowInput") += 1
    val nonEmpty =
      if (rdd.partitions.length == 0) session.sparkContext.parallelize(Seq.empty[ColumnarBatch], 1) else rdd
    val hadoopConf =
      session.sparkContext.broadcast(new SerializableConfiguration(session.sessionState.newHadoopConf()))
    val taskSpec = session.sparkContext.broadcast(spec)
    val ordinals = rowOrdinals
    val fileOrd = fileOrdinal
    val posOrd = posOrdinal
    val copy = copyUnchanged
    val messages = new mutable.ArrayBuffer[WriterCommitMessage]()
    try {
      messages ++= nonEmpty
        .mapPartitions { batches =>
          Iterator.single(
            if (copy)
              VectorIcebergWriteExec.rewriteTask(
                taskSpec.value,
                hadoopConf.value.value,
                ordinals,
                fileOrd,
                posOrd,
                batches
              )
            else
              VectorIcebergWriteExec.writeTask(taskSpec.value, hadoopConf.value.value, ordinals, batches)
          )
        }
        .collect()
      writeSummary(query) match {
        case Some(summary) => batchWrite.commit(messages.toArray, summary)
        case None => batchWrite.commit(messages.toArray)
      }
      logInfo(s"vecruntime: columnar Iceberg $operation committed ${messages.size} task message(s)")
    } catch {
      case t: Throwable =>
        try batchWrite.abort(messages.toArray)
        catch { case a: Throwable => t.addSuppressed(a) }
        throw t
    }
    refreshCache()
    Nil
  }

  /** A MERGE's summary for the commit, as Spark's `ReplaceDataExec` passes it (see [[VectorWriteDeltaExec]]). */
  private def writeSummary(plan: SparkPlan): Option[WriteSummary] =
    collectFirst(plan) {
      case m if m.getClass.getSimpleName == "MergeRowsExec" || m.getClass.getSimpleName == "VectorMergeRowsExec" => m
    }.map { n =>
      def v(k: String): Long = n.metrics.get(k).map(_.value).getOrElse(-1L)
      MergeSummaryImpl(
        v("numTargetRowsCopied"),
        v("numTargetRowsDeleted"),
        v("numTargetRowsUpdated"),
        v("numTargetRowsInserted"),
        v("numTargetRowsMatchedUpdated"),
        v("numTargetRowsMatchedDeleted"),
        v("numTargetRowsNotMatchedBySourceUpdated"),
        v("numTargetRowsNotMatchedBySourceDeleted")
      )
    }
}

object VectorIcebergWriteExec extends Logging {

  /**
   * The child's batches -- from below the row transition Spark (or adaptive execution) put above it -- and
   * whether they had to be built from rows.
   */
  def columnar(query: SparkPlan): (RDD[ColumnarBatch], Boolean) = {
    val plan = query match {
      case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
      case p => p
    }
    plan match {
      case t: ColumnarToRowTransition if t.child.supportsColumnar =>
        logDebug(s"reads ${t.child.nodeName} columnar, from below ${t.nodeName}")
        (t.child.executeColumnar(), false)
      case w: org.apache.spark.sql.execution.WholeStageCodegenExec
          if w.child.isInstanceOf[ColumnarToRowTransition] && w.child.children.head.supportsColumnar =>
        // A transition collapsed into a codegen stage (adaptive execution plans the child on its own).
        (w.child.children.head.executeColumnar(), false)
      case p if p.supportsColumnar =>
        logDebug(s"reads ${p.nodeName} columnar")
        (p.executeColumnar(), false)
      case p =>
        logInfo(s"vecruntime: the columnar Iceberg writer reads rows from ${p.nodeName} (RowToColumnarExec)")
        (RowToColumnarExec(p).executeColumnar(), true)
    }
  }

  /** One task: its batches through the bridge's task writer; Iceberg's commit message for the files. */
  def writeTask(
      spec: AnyRef,
      conf: org.apache.hadoop.conf.Configuration,
      ordinals: Array[Int],
      batches: Iterator[ColumnarBatch]
  ): WriterCommitMessage = {
    val ctx = TaskContext.get()
    val writer = IcebergWriteBridge.createTaskWriter(spec, ctx.partitionId(), ctx.taskAttemptId(), conf)
    try {
      while (batches.hasNext) {
        val batch = batches.next()
        val arena = Arena.ofConfined()
        try {
          val (cols, n) = VectorWriteTask2.columns(batch, ordinals, arena)
          writer.write(java.util.Arrays.asList(cols: _*), 0, n)
        } finally arena.close()
      }
      val files = writer.dataFiles()
      // Spark's output metrics (UI, listeners), as its DataWritingSparkTask reports them for a v2 write.
      val (records, bytes) = IcebergWriteBridge.fileTotals(files)
      val out = ctx.taskMetrics().outputMetrics
      out.setRecordsWritten(out.recordsWritten + records)
      out.setBytesWritten(out.bytesWritten + bytes)
      IcebergWriteBridge.taskCommit(files)
    } catch {
      case t: Throwable =>
        writer.abort(t)
        throw t
    }
  }

  /**
   * One task of a copy-on-write DELETE rewritten with row-group copies (#901 M4a). The batch's data columns
   * (at `ordinals`) and its `_file` / `_pos` columns are handed to the bridge's rewrite task writer, which
   * buffers by source file and, on close, copies the delete-free row groups of each file and re-encodes the
   * rest. Returns Iceberg's own commit message for the new data files.
   */
  def rewriteTask(
      spec: AnyRef,
      conf: org.apache.hadoop.conf.Configuration,
      ordinals: Array[Int],
      fileOrdinal: Int,
      posOrdinal: Int,
      batches: Iterator[ColumnarBatch]
  ): WriterCommitMessage = {
    val ctx = TaskContext.get()
    // Dense column list per batch: the data columns, then _file, then _pos. The writer reads data at
    // [0, ordinals.length), _file at ordinals.length, _pos at ordinals.length + 1.
    val all = ordinals :+ fileOrdinal :+ posOrdinal
    val dataIdx = ordinals.indices.toArray
    val writer =
      IcebergWriteBridge.createRewriteTaskWriter(
        spec,
        ctx.partitionId(),
        ctx.taskAttemptId(),
        conf,
        ordinals.length,
        ordinals.length + 1,
        dataIdx
      )
    try {
      while (batches.hasNext) {
        val batch = batches.next()
        val arena = Arena.ofConfined()
        try {
          val (cols, n) = VectorWriteTask2.columns(batch, all, arena)
          writer.write(java.util.Arrays.asList(cols: _*), 0, n)
        } finally arena.close()
      }
      val files = writer.dataFiles()
      val (records, bytes) = IcebergWriteBridge.fileTotals(files)
      val out = ctx.taskMetrics().outputMetrics
      out.setRecordsWritten(out.recordsWritten + records)
      out.setBytesWritten(out.bytesWritten + bytes)
      IcebergWriteBridge.taskCommit(files)
    } catch {
      case t: Throwable =>
        writer.abort(t)
        throw t
    }
  }
}
