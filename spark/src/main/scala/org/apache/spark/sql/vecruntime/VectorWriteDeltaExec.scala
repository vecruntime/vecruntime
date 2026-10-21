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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{RowDeltaUtils, WriteDeltaProjections}
import org.apache.spark.sql.connector.write.{
  DeltaWrite,
  DeltaWriter,
  DeltaWriterFactory,
  MergeSummaryImpl,
  PhysicalWriteInfoImpl,
  WriteSummary,
  WriterCommitMessage
}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.TaskContext

/**
 * The columnar Iceberg v3 deletion-vector write command (#20). It replaces Spark's row-by-row
 * `WriteDeltaExec` on a format-version-3 merge-on-read table: instead of routing each deleted
 * `InternalRow` through Iceberg's per-row `DeltaWriter.delete`, each task groups the delete rows by
 * their `_file` metadata column (they arrive clustered by the write's REBALANCE exchange), builds one
 * deletion-vector bitmap per data file through the optional `iceberg-bridge` module, and emits a
 * `DeltaTaskCommit`. The driver hands the messages to Iceberg's OWN `DeltaBatchWrite.commit`
 * (obtained from `write.toBatch()`), so the commit -- snapshot summary, previous-DV merge, metrics --
 * is entirely Iceberg's; on failure it calls `abort`.
 *
 * <p>DELETE, UPDATE and MERGE. The insert half of UPDATE/MERGE (INSERT and REINSERT rows) is written
 * by Iceberg's own delta writer, from Iceberg's own writer factory, so data files, partitioning,
 * sizing and v3 row lineage stay Iceberg's; only the deletes take the columnar path.
 *
 * @param table          the Iceberg table, as `AnyRef` (the core has no Iceberg compile dep)
 * @param write          the Iceberg `DeltaWrite`
 * @param projections    Spark's row/rowId/metadata projections for the delta rows
 * @param query          the child plan producing the delta rows
 * @param refreshCache   recaches the target after the commit, as Spark's `WriteDeltaExec` does
 */
case class VectorWriteDeltaExec(
    table: AnyRef,
    write: DeltaWrite,
    projections: WriteDeltaProjections,
    query: SparkPlan,
    refreshCache: () => Unit
) extends V2CommandExec
    with AdaptiveSparkPlanHelper
    with Logging {

  override def output: Seq[Attribute] = Nil
  override def children: Seq[SparkPlan] = Seq(query)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(query = newChildren.head)

  override protected def run(): Seq[InternalRow] = {
    val batchWrite = write.toBatch
    val proj = projections
    val tbl = table
    // Read on the driver before the job, as Iceberg's own writer does, and broadcast to the tasks.
    val previous = session.sparkContext.broadcast(IcebergDvBridge.deletePlan(write))
    val columnarData = VectorWriteDeltaExec.columnarDataSpec(write, proj, conf)
    if (columnarData.isDefined) return runColumnar(batchWrite, previous, columnarData.get)
    val rdd: RDD[InternalRow] = query.execute()
    // UPDATE and MERGE carry an insert half (a row projection). Those rows go through Iceberg's OWN
    // delta writer, created from Iceberg's own writer factory exactly as Spark's WriteDeltaExec
    // does, so the data files, their partitioning, file sizing and v3 row lineage are Iceberg's.
    // Only the deletes are ours. Each task therefore returns two commit messages -- our DVs and
    // Iceberg's data files -- and Iceberg's commit folds every message into one RowDelta.
    val dataWriterFactory: Option[DeltaWriterFactory] =
      if (proj.rowProjection.isDefined) {
        Some(
          batchWrite
            .createBatchWriterFactory(PhysicalWriteInfoImpl(rdd.getNumPartitions))
            .asInstanceOf[DeltaWriterFactory]
        )
      } else None
    val messages = new mutable.ArrayBuffer[WriterCommitMessage]()
    try {
      val collected: Array[Seq[WriterCommitMessage]] =
        rdd
          .mapPartitions { iter =>
            Iterator.single(
              VectorWriteDeltaExec.writePartition(tbl, proj, previous.value, dataWriterFactory, iter)
            )
          }
          .collect()
      messages ++= collected.flatten.filter(_ != null)
      writeSummary(query) match {
        case Some(summary) => batchWrite.commit(messages.toArray, summary)
        case None => batchWrite.commit(messages.toArray)
      }
      logInfo(s"vecruntime: columnar v3 DV write committed ${messages.size} task message(s)")
    } catch {
      case t: Throwable =>
        try batchWrite.abort(messages.toArray)
        catch { case a: Throwable => t.addSuppressed(a) }
        throw t
    }
    refreshCache()
    Nil
  }

  /**
   * #901 M1: the whole write on the child's batches. A task walks each batch's operation lane: a DELETE's
   * `_file` / `_pos` (and `_spec_id` / `_partition`) feed the same per-file deletion vectors as the row path; the
   * INSERT and REINSERT rows of the batch go, picked out by row index, to the columnar data writer
   * (`IcebergDeltaDataWriter`), with their v3 row lineage from the metadata columns. Each task returns our DV
   * message and a data-files message; Iceberg's commit folds them into one `RowDelta`, as before.
   */
  private def runColumnar(
      batchWrite: org.apache.spark.sql.connector.write.DeltaBatchWrite,
      previous: org.apache.spark.broadcast.Broadcast[IcebergDvBridge.DeletePlan],
      data: VectorWriteDeltaExec.ColumnarData
  ): Seq[InternalRow] = {
    val (batches, fromRows) = VectorIcebergWriteExec.columnar(query)
    if (fromRows) {
      VectorWriteDeltaExec.rowInputWrites.incrementAndGet()
      logInfo("vecruntime: the columnar delta write reads rows from its child (RowToColumnarExec)")
    }
    val hadoopConf = session.sparkContext.broadcast(
      new org.apache.spark.util.SerializableConfiguration(session.sessionState.newHadoopConf())
    )
    val tbl = table
    val proj = projections
    val messages = new mutable.ArrayBuffer[WriterCommitMessage]()
    try {
      val collected = batches.mapPartitions { iter =>
        Iterator.single(
          VectorWriteDeltaExec.writeBatches(tbl, proj, previous.value, data, hadoopConf.value.value, iter)
        )
      }.collect()
      messages ++= collected.flatten.filter(_ != null)
      writeSummary(query) match {
        case Some(summary) => batchWrite.commit(messages.toArray, summary)
        case None => batchWrite.commit(messages.toArray)
      }
      logInfo(s"vecruntime: columnar delta write (deletion vectors + data files) committed ${messages.size} message(s)")
    } catch {
      case t: Throwable =>
        try batchWrite.abort(messages.toArray)
        catch { case a: Throwable => t.addSuppressed(a) }
        throw t
    }
    refreshCache()
    Nil
  }

  /**
   * The MERGE summary Spark's `WriteDeltaExec` hands to the commit, read from the MERGE's row
   * operator -- Spark's `MergeRowsExec` or our columnar one, which keeps the same metric names -- so
   * the snapshot summary carries the same `spark.merge.*`-style properties with the writer on or off.
   */
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

object VectorWriteDeltaExec extends Logging {

  /** #901 M1: the tasks that wrote a data half columnar (read by tests). */
  val columnarDataTasks = new java.util.concurrent.atomic.AtomicLong()

  /** Columnar delta writes whose child produced rows, converted by `RowToColumnarExec` (read by tests). */
  val rowInputWrites = new java.util.concurrent.atomic.AtomicLong()

  /**
   * #901 M1: what the columnar task needs -- the data writer's spec (null for a delete-only write), whether the
   * data files carry row lineage, and the ordinals, in the child's output, of the operation, `_file`, `_pos`,
   * `_spec_id`, `_partition`, the data columns and the lineage columns.
   */
  final case class ColumnarData(
      spec: AnyRef,
      lineage: Boolean,
      file: Int,
      pos: Int,
      specId: Int,
      partition: Int,
      partitionWidth: Int,
      data: Array[Int],
      lineageColumns: Array[Int]
  ) extends Serializable

  /**
   * The columnar task's plan for `write`, or None to keep the row path: the bridge's delta data writer present,
   * the columnar writer on (`spark.vecruntime.iceberg.writer.enabled`), and a data half (if any) it can write.
   */
  def columnarDataSpec(
      write: DeltaWrite,
      projections: WriteDeltaProjections,
      conf: org.apache.spark.sql.internal.SQLConf
  ): Option[ColumnarData] = {
    if (!io.vecruntime.spark.VectorConf.icebergWriterEnabled(conf) || !IcebergDeltaWriteBridge.isAvailable) return None
    if (!IcebergDeltaWriteBridge.isPositionDeltaWrite(write)) return None
    def ordinals(p: Option[org.apache.spark.sql.catalyst.ProjectingInternalRow]): Map[String, Int] =
      p.map(r => r.schema.fieldNames.zip(r.colOrdinals).toMap).getOrElse(Map.empty)
    val rowId = ordinals(Some(projections.rowIdProjection))
    val meta = ordinals(projections.metadataProjection)
    val partitionWidth = projections.metadataProjection
      .flatMap(m => m.schema.find(_.name == "_partition"))
      .map(_.dataType.asInstanceOf[org.apache.spark.sql.types.StructType].size)
      .getOrElse(0)
    val (spec, lineage, data, lineageColumns) = projections.rowProjection match {
      case None => (null, false, Array.emptyIntArray, Array.emptyIntArray)
      case Some(rows) =>
        IcebergDeltaWriteBridge.declineReason(write) match {
          case Some(why) =>
            if (io.vecruntime.spark.VectorConf.explainFallback(conf)) {
              logInfo(s"spark-vector: columnar delta data writer declined -- $why")
            }
            return None
          case None =>
        }
        val lineage = IcebergDeltaWriteBridge.rowLineage(write)
        val lineageColumns =
          if (!lineage) Array.emptyIntArray
          else {
            val l = Seq("_row_id", "_last_updated_sequence_number").map(meta.get)
            if (l.exists(_.isEmpty)) return None
            l.map(_.get).toArray
          }
        (IcebergDeltaWriteBridge.specOf(write), lineage, rows.colOrdinals.toArray, lineageColumns)
    }
    if (!rowId.contains("_file") || !rowId.contains("_pos")) return None
    Some(
      ColumnarData(
        spec,
        lineage,
        rowId("_file"),
        rowId("_pos"),
        meta.getOrElse("_spec_id", -1),
        meta.getOrElse("_partition", -1),
        partitionWidth,
        data,
        lineageColumns
      )
    )
  }

  /** The column `o` of `batch` as a lane, nested columns through Spark's getters (any row picked by index). */
  private def lane(batch: org.apache.spark.sql.vectorized.ColumnarBatch, o: Int, arena: java.lang.foreign.Arena)
      : io.vecruntime.kernels.VectorBuffers = {
    val cv = batch.column(o)
    cv.dataType() match {
      case dt @ (_: org.apache.spark.sql.types.StructType | _: org.apache.spark.sql.types.ArrayType |
          _: org.apache.spark.sql.types.MapType) =>
        io.vecruntime.spark.adapter.NestedColumns.adapt(cv, dt, batch.numRows(), arena)
      case _ => io.vecruntime.spark.adapter.ColumnVectorAdapters.adapt(cv, batch.numRows(), arena)
    }
  }

  /** #901 M1: one task of the columnar delta write. Returns our DV message and the data files' message. */
  private def writeBatches(
      table: AnyRef,
      projections: WriteDeltaProjections,
      rewritableDeletes: IcebergDvBridge.DeletePlan,
      plan: ColumnarData,
      conf: org.apache.hadoop.conf.Configuration,
      batches: Iterator[org.apache.spark.sql.vectorized.ColumnarBatch]
  ): Seq[WriterCommitMessage] = {
    val tc = TaskContext.get()
    val partitionId = if (tc != null) tc.partitionId() else 0
    val taskId = if (tc != null) tc.taskAttemptId() else 0L
    val deleteOnly = projections.rowProjection.isEmpty
    val writer = IcebergDvBridge.createTaskWriter(table, partitionId, taskId, rewritableDeletes)
    val dataWriter =
      if (deleteOnly) null
      else IcebergDeltaWriteBridge.createWriter(plan.spec, partitionId, taskId, conf, plan.lineage)
    val dataOrdinals = plan.data ++ plan.lineageColumns
    try {
      var currentFile: org.apache.spark.unsafe.types.UTF8String = null
      var currentSpec = 0
      var currentPartition: InternalRow = null
      val positions = new mutable.ArrayBuilder.ofLong
      var count = 0
      def flush(): Unit = {
        if (currentFile != null && count > 0) {
          writer.deleteFile(currentFile.toString, positions.result(), count, currentSpec, currentPartition)
        }
        positions.clear()
        count = 0
      }
      while (batches.hasNext) {
        val batch = batches.next()
        val n = batch.numRows()
        val selection = batch match {
          case s: io.vecruntime.spark.arrow.SelectedColumnarBatch if s.selectedCount() < n => s.selection()
          case _ => null
        }
        val ops = if (deleteOnly) null else batch.column(0)
        val fileCol = batch.column(plan.file)
        val posCol = batch.column(plan.pos)
        val specCol = if (plan.specId >= 0) batch.column(plan.specId) else null
        val partCol = if (plan.partition >= 0) batch.column(plan.partition) else null
        val rows = if (deleteOnly) null else new Array[Int](n)
        val reinsert = if (deleteOnly) null else new Array[Boolean](n)
        var k = 0
        var i = 0
        while (i < n) {
          if (selection == null || io.vecruntime.kernels.Bitmap.isSet(selection, i)) {
            val op = if (deleteOnly) RowDeltaUtils.DELETE_OPERATION else ops.getInt(i)
            op match {
              case RowDeltaUtils.DELETE_OPERATION =>
                val file = fileCol.getUTF8String(i)
                val spec = if (specCol == null) 0 else specCol.getInt(i)
                // A run ends where the file changes; an interleaved insert does not end it (the bridge merges a
                // file's runs into one DV).
                if (currentFile == null || file != currentFile || spec != currentSpec) {
                  flush()
                  currentFile = file.clone()
                  currentSpec = spec
                  currentPartition =
                    if (partCol == null || partCol.isNullAt(i)) null else partCol.getStruct(i).copy()
                }
                positions += posCol.getLong(i)
                count += 1
              case RowDeltaUtils.INSERT_OPERATION =>
                rows(k) = i; reinsert(k) = false; k += 1
              case RowDeltaUtils.REINSERT_OPERATION =>
                rows(k) = i; reinsert(k) = true; k += 1
              case RowDeltaUtils.UPDATE_OPERATION =>
                throw new IllegalStateException("UPDATE must be represented as delete and reinsert")
              case other => throw new IllegalStateException(s"unexpected row-level operation id $other")
            }
          }
          i += 1
        }
        if (k > 0) {
          val arena = java.lang.foreign.Arena.ofConfined()
          try {
            val columns = dataOrdinals.map(o => lane(batch, o, arena))
            dataWriter.write(java.util.Arrays.asList(columns: _*), rows, k, reinsert)
          } finally arena.close()
        }
      }
      flush()
      val ours = writer.commit()
      val data =
        if (dataWriter == null) null
        else {
          columnarDataTasks.incrementAndGet()
          IcebergDeltaWriteBridge.dataCommit(dataWriter.dataFiles())
        }
      Seq(ours, data)
    } catch {
      case t: Throwable =>
        if (dataWriter != null) {
          try dataWriter.abort(t)
          catch { case a: Throwable => t.addSuppressed(a) }
        }
        throw t
    } finally writer.close()
  }

  /**
   * One task: read the delta rows and dispatch on each row's operation, as Spark's
   * `DeltaWithMetadataWritingSparkTask` does. A DELETE's `_spec_id` / `_partition` / `_file` /
   * `_pos` feed runs of positions per data file into one deletion vector each through the bridge;
   * an INSERT or REINSERT goes to Iceberg's own delta writer (`dataWriterFactory`, present only when
   * the write has an insert half). Returns the task's commit messages: ours, then Iceberg's.
   */
  private def writePartition(
      table: AnyRef,
      projections: WriteDeltaProjections,
      rewritableDeletes: IcebergDvBridge.DeletePlan,
      dataWriterFactory: Option[DeltaWriterFactory],
      rows: Iterator[InternalRow]
  ): Seq[WriterCommitMessage] = {
    val tc = TaskContext.get()
    val partitionId = if (tc != null) tc.partitionId() else 0
    val taskId = if (tc != null) tc.taskAttemptId() else 0L

    val rowId = projections.rowIdProjection
    // Iceberg splits the row-level metadata across two projections: the rowId projection carries
    // {_file, _pos}, and the metadata projection carries {_spec_id, _partition} (see Iceberg's
    // SparkPositionDeltaWrite, which reads the spec and partition from `metadata`, not `rowId`).
    // Reading `_partition` from the rowId projection finds nothing, so every partition came out
    // null and a partitioned table's commit failed. Resolve ordinals by name in each projection so a
    // schema reordering across Iceberg versions cannot silently mis-read them.
    val rowIdSchema = rowId.schema
    val metadata = projections.metadataProjection
    val metaSchema = metadata.map(_.schema)
    def has(schema: Option[org.apache.spark.sql.types.StructType], name: String): Option[Int] =
      schema.filter(_.fieldNames.contains(name)).map(_.fieldIndex(name))

    val fileOrd = rowIdSchema.fieldIndex("_file")
    val posOrd = rowIdSchema.fieldIndex("_pos")
    val specOrdOpt = has(metaSchema, "_spec_id")
    val partOrdOpt = has(metaSchema, "_partition")
    val partWidth = partOrdOpt.map { o =>
      metaSchema.get.fields(o).dataType.asInstanceOf[org.apache.spark.sql.types.StructType].size
    }

    val writer = IcebergDvBridge.createTaskWriter(table, partitionId, taskId, rewritableDeletes)
    // Iceberg's own delta writer for the insert half; its delete half is never called, so it
    // contributes only data files (and its DV writer closes empty).
    val dataWriter: DeltaWriter[InternalRow] =
      dataWriterFactory.map(_.createWriter(partitionId, taskId)).orNull
    val rowProj = projections.rowProjection.orNull
    var dataCommitted = false
    try {
      var currentFile: String = null
      var currentSpec: Int = 0
      var currentPartition: InternalRow = null
      val positions = new mutable.ArrayBuilder.ofLong
      var count = 0

      def flush(): Unit = {
        if (currentFile != null && count > 0) {
          writer.deleteFile(currentFile, positions.result(), count, currentSpec, currentPartition)
        }
        positions.clear()
        count = 0
      }

      def delete(row: InternalRow): Unit = {
        rowId.project(row)
        metadata.foreach(_.project(row))
        val file = rowId.getUTF8String(fileOrd).toString
        val pos = rowId.getLong(posOrd)
        val specId = specOrdOpt.map(metadata.get.getInt).getOrElse(0)
        val partition: InternalRow = partOrdOpt match {
          case Some(o) if !metadata.get.isNullAt(o) => metadata.get.getStruct(o, partWidth.get)
          case _ => null
        }
        // A run ends where the file changes; an interleaved insert does not end it. A file whose
        // deletes come in several runs is still one DV: the bridge merges runs per data file.
        if (file != currentFile || specId != currentSpec) {
          flush()
          currentFile = file
          currentSpec = specId
          currentPartition = if (partition != null) partition.copy() else null
        }
        positions += pos
        count += 1
      }

      def dataFor(op: Int, row: InternalRow): DeltaWriter[InternalRow] = {
        if (dataWriter == null) throw new IllegalStateException(s"operation $op in a delete-only write")
        rowProj.project(row)
        dataWriter
      }

      while (rows.hasNext) {
        val row = rows.next()
        if (rowProj == null) {
          // Delete-only write (no row projection): every row is a delete.
          delete(row)
        } else {
          row.getInt(0) match {
            case RowDeltaUtils.DELETE_OPERATION => delete(row)
            case RowDeltaUtils.INSERT_OPERATION => dataFor(RowDeltaUtils.INSERT_OPERATION, row).insert(rowProj)
            case RowDeltaUtils.REINSERT_OPERATION =>
              // A reinsert carries the old row's metadata: Iceberg reads the v3 row lineage from it.
              val w = dataFor(RowDeltaUtils.REINSERT_OPERATION, row)
              metadata.foreach(_.project(row))
              w.reinsert(metadata.orNull, rowProj)
            case RowDeltaUtils.UPDATE_OPERATION =>
              // Iceberg's position deltas represent an UPDATE as a DELETE plus a REINSERT
              // (representUpdateAsDeleteAndInsert), so Spark never emits this for them.
              throw new IllegalStateException("UPDATE must be represented as delete and reinsert")
            case other => throw new IllegalStateException(s"unexpected row-level operation id $other")
          }
        }
      }
      flush()
      val ours = writer.commit()
      val data = if (dataWriter != null) { val m = dataWriter.commit(); dataCommitted = true; m }
      else null
      Seq(ours, data)
    } catch {
      case t: Throwable =>
        if (dataWriter != null && !dataCommitted) {
          try dataWriter.abort()
          catch { case a: Throwable => t.addSuppressed(a) }
        }
        throw t
    } finally {
      writer.close()
      if (dataWriter != null) dataWriter.close()
    }
  }
}
