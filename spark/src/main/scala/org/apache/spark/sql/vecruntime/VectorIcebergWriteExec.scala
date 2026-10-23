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
            take(plan, r.table, r.query, r.write, Some(ordinals.map(_.get)), r.originalTable)
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
      original: LogicalPlan = null
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
            plan.nodeName
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
    operation: String
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

  override def simpleString(maxFields: Int): String = s"VectorIcebergWriteExec $operation"

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
    val messages = new mutable.ArrayBuffer[WriterCommitMessage]()
    try {
      messages ++= nonEmpty
        .mapPartitions { batches =>
          Iterator.single(VectorIcebergWriteExec.writeTask(taskSpec.value, hadoopConf.value.value, ordinals, batches))
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
   *
   * A command declares a row child, so Spark puts a `ColumnarToRow` above the child's final plan and adaptive
   * execution wraps the whole thing in query-stage nodes. When the stage below that transition is actually
   * columnar -- our shuffle read, our Expand/Filter/MergeRows chain -- reading it row by row and converting
   * back with `RowToColumnarExec` is a pointless columnar->row->columnar round trip that drops the whole
   * merge-on-read MERGE/UPDATE write to rows (the `_spec_id`/`_partition`/`_file` clustering Iceberg requires
   * is satisfied by our columnar exchange, but the write never read it columnar). So descend through the
   * transparent adaptive wrappers (`AdaptiveSparkPlanExec`, the query stages, whole-stage codegen, the input
   * adapter) and peel the `ColumnarToRow` transition, and read the first columnar node directly. Only a child
   * that is genuinely row at the stage root (a Spark row MergeRows over a row source) is converted from rows.
   */
  def columnar(query: SparkPlan): (RDD[ColumnarBatch], Boolean) = {
    VectorIcebergWriteExec.columnarChild(query) match {
      case Some(columnar) =>
        logDebug(s"reads ${columnar.nodeName} columnar")
        (columnar.executeColumnar(), false)
      case None =>
        val rowRoot = VectorIcebergWriteExec.rowStageRoot(query)
        logInfo(s"vecruntime: the columnar Iceberg writer reads rows from ${rowRoot.nodeName} (RowToColumnarExec)")
        (RowToColumnarExec(rowRoot).executeColumnar(), true)
    }
  }

  /**
   * The first columnar node reachable from a command's child through the transparent adaptive wrappers Spark
   * puts above it, or None when the stage is genuinely row at its root. The wrappers are single-child nodes
   * whose output is their child's output unchanged: the adaptive plan (its final physical plan), a query stage
   * (its wrapped plan), whole-stage codegen, the input adapter, and the `ColumnarToRow` transition Spark
   * inserts because a command declares a row child. A node that already produces batches ends the descent;
   * the transition is peeled rather than executed so the batches below it are read directly.
   */
  def columnarChild(plan: SparkPlan): Option[SparkPlan] = plan match {
    case p if p.supportsColumnar => Some(p)
    case a: AdaptiveSparkPlanExec => columnarChild(a.finalPhysicalPlan)
    case q: org.apache.spark.sql.execution.adaptive.QueryStageExec => columnarChild(q.plan)
    case w: org.apache.spark.sql.execution.WholeStageCodegenExec => columnarChild(w.child)
    case i: org.apache.spark.sql.execution.InputAdapter => columnarChild(i.child)
    case t: ColumnarToRowTransition => columnarChild(t.child)
    case _ => None
  }

  /**
   * The row node to convert when the stage is genuinely row: the adaptive plan's final physical plan (or the
   * query stage's wrapped plan), so a `RowToColumnarExec` sits over the real stage root rather than over the
   * `AdaptiveSparkPlanExec` wrapper (which cannot be re-executed columnar).
   */
  def rowStageRoot(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => rowStageRoot(a.finalPhysicalPlan)
    case q: org.apache.spark.sql.execution.adaptive.QueryStageExec => rowStageRoot(q.plan)
    case p => p
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
}
