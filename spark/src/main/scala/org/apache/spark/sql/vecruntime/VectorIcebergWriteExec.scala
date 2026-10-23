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
            // #901 M4a/M4b: a copy-on-write DELETE or UPDATE copies the row groups (DELETE) or column chunks
            // (UPDATE) it does not touch, when the flag is on and the scan projects _file / _pos (Iceberg's
            // copy-on-write scan does). For a DELETE every surviving row is an unchanged copy, so the per-column
            // encode mask is all-false (copy every data column). For an UPDATE the changed columns are extracted
            // from the rewrite plan (the SET assignments); untouched columns are copied, changed columns and the
            // v3 lineage columns are re-encoded. A MERGE is not eligible: its rewrite is a MergeRows that does
            // not carry _pos, so the per-row source position the copy path needs is unavailable.
            val copy: Option[VectorIcebergWriteStrategy.CopyPlan] =
              if (VectorConf.icebergCopyUnchangedEnabled(conf)) {
                (ordinal("_file"), ordinal("_pos")) match {
                  case (Some(f), Some(p)) if VectorIcebergWriteStrategy.isDelete(r) =>
                    // DELETE: copy every data column (encode nothing); the v3 lineage re-encode is decided by
                    // the planner from the source schema, as M4a does.
                    Some(VectorIcebergWriteStrategy.CopyPlan(f, p, names.map(VectorIcebergWriteStrategy.isLineage)))
                  case (Some(f), Some(p)) if VectorIcebergWriteStrategy.isUpdate(r) =>
                    VectorIcebergWriteStrategy.updateEncodeMask(r, names).flatMap { encode =>
                      // Decline when a CHANGED column is one the table partitions by: a row may move partition,
                      // which the copy path (one output file per source file/partition) cannot do -- fall back
                      // to Iceberg's native partitioned writer, which re-partitions.
                      val partCols = IcebergWriteBridge.partitionSourceNames(w).toSet
                      val changesPartition = names.indices.exists(i => encode(i) && partCols.contains(names(i)))
                      if (changesPartition) None
                      else Some(VectorIcebergWriteStrategy.CopyPlan(f, p, encode))
                    }
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
      copy: Option[VectorIcebergWriteStrategy.CopyPlan] = None
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
        // #901 M4a: for the copy path, cluster the input by _file with a plain hash partitioning
        // (RepartitionByExpression -> ShuffleExchange hashpartitioning(_file), ENSURE_REQUIREMENTS). This
        // replaces Iceberg's REBALANCE_PARTITIONS_BY_COL distribution: a plain hash exchange sends every row
        // of a file to one reducer and is never split by AQE's OptimizeSkewInRebalancePartitions (which only
        // touches REBALANCE exchanges), so a source file is whole in one task and its row groups can be copied.
        val child = copy match {
          case Some(cp) =>
            org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression(
              Seq(query.output(cp.fileOrdinal)),
              query,
              None
            )
          case None => query
        }
        Seq(
          VectorIcebergWriteExec(
            write.get,
            IcebergWriteBridge.specOf(write.get),
            rowOrdinals,
            planLater(child),
            VectorIcebergWriteStrategy.refreshCache(session, Option(original).getOrElse(table)),
            plan.nodeName,
            copy.map(_.fileOrdinal).getOrElse(-1),
            copy.map(_.posOrdinal).getOrElse(-1),
            copy.map(_.encodeColumns).orNull
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

  /** Whether a `ReplaceData` is a copy-on-write UPDATE. */
  def isUpdate(r: ReplaceData): Boolean =
    try r.operation.command() == org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
    catch { case _: Throwable => false }

  /** The two v3 row-lineage column names, which are always re-encoded on a copy (never copied byte for byte). */
  private val LineageColumns = Set("_row_id", "_last_updated_sequence_number")

  /** Whether a write-schema column is a v3 row-lineage column. */
  def isLineage(name: String): Boolean = LineageColumns.contains(name)

  /**
   * The copy plan for a copy-on-write rewrite eligible for the row-group / column-chunk copy path (#901 M4a,
   * M4b): the `_file` and `_pos` ordinals in the batch, and `encodeColumns`, a per-write-schema-column flag --
   * `true` to RE-ENCODE the column (a changed column, or a v3 lineage column), `false` to COPY its chunk byte
   * for byte. A DELETE encodes nothing but lineage; an UPDATE encodes its changed columns and lineage.
   */
  final case class CopyPlan(fileOrdinal: Int, posOrdinal: Int, encodeColumns: Array[Boolean])

  /**
   * The per-write-schema-column encode mask for a copy-on-write UPDATE, derived from the rewrite plan, or
   * `None` when the plan shape is not the one `RewriteUpdateTable` produces (then the whole write takes the
   * existing full-rewrite path -- conservative: an unrecognised plan copies nothing).
   *
   * Spark 4.1's `RewriteUpdateTable` builds `ReplaceData.query` as a tag `Project` over an assignment `Project`
   * over the table scan (`RelationV2`). In the assignment `Project` each data column's expression is either the
   * bare source attribute (unassigned) or `If(cond, newValue, oldAttr)` (assigned). A column is UNTOUCHED iff,
   * in every branch, the value is the SAME source attribute the column reads from -- the bare attribute, or
   * `If(c, attr, attr)` (a self-assignment `SET x = x`). Anything else -- a different attribute, a cast, a
   * literal, an arithmetic or string expression -- is CHANGED and must be re-encoded.
   *
   * The two v3 lineage columns are always re-encoded: Iceberg nulls a matched row's
   * `_last_updated_sequence_number` so the commit assigns the new one, and keeps `_row_id`; both are written by
   * the engine, not copied. A partition column that is CHANGED makes the whole file ineligible (rows may move
   * partition), handled downstream by the planner / task writer from the spec, not here.
   */
  def updateEncodeMask(r: ReplaceData, writeNames: Array[String]): Option[Array[Boolean]] = {
    import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, If, NamedExpression}
    import org.apache.spark.sql.catalyst.plans.logical.Project
    try {
      // Find the assignment Project: in the optimized plan the tag + assignment Projects collapse into ONE
      // Project that sits over the table scan subtree (possibly a Filter carrying a dynamic-pruning
      // expression). Its named expressions give each output column its value -- a bare attribute (untouched),
      // If(cond, newValue, oldAttr) (conditional assignment), or a computed expression (changed). We find the
      // Project whose child subtree's output carries `_pos` (the scan), and classify against that child's
      // output attributes (a Filter passes them through unchanged, so names still match the scan).
      def overScan(p: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Boolean =
        p.output.exists(_.name == "_pos")
      def unwrap(p: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Option[Project] = p match {
        case pr: Project if overScan(pr.child) => Some(pr)
        case other => other.children.iterator.flatMap(c => unwrap(c)).toSeq.headOption
      }
      unwrap(r.query).map { assign =>
        // The scan attributes by name (the assignment Project's child is the scan). A column is UNTOUCHED iff
        // its value is, in every branch, the scan attribute of the SAME column name -- a bare pass-through
        // `attr`, or `If(c, attr, attr)` (a conditional self-assignment). An unconditional `SET a = b` makes
        // a's value the scan attribute `b` (a different name) -> CHANGED; a literal/cast/arithmetic -> CHANGED.
        val scanByName: Map[String, Long] =
          assign.child.output.map(a => a.name -> a.exprId.id).toMap
        def isScanAttrNamed(e: Expression, name: String): Boolean = e match {
          case a: AttributeReference => scanByName.get(name).contains(a.exprId.id)
          case _ => false
        }
        def untouched(value: Expression, name: String): Boolean = value match {
          case a: AttributeReference => isScanAttrNamed(a, name)
          case If(_, t, f) => isScanAttrNamed(t, name) && isScanAttrNamed(f, name)
          case _ => false
        }
        // Per assignment-Project output column NAME -> untouched?
        val byName: Map[String, Boolean] = assign.projectList.collect {
          case ne: NamedExpression =>
            val value = ne match {
              case Alias(child, _) => child
              case a => a
            }
            ne.name -> untouched(value, ne.name)
        }.toMap
        writeNames.map { name =>
          if (isLineage(name)) true // lineage always re-encoded
          else byName.get(name) match {
            case Some(ut) => !ut // encode iff NOT untouched
            case None => true // not found -> be safe, re-encode
          }
        }
      }
    } catch { case _: Throwable => None }
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
    operation: String,
    fileOrdinal: Int = -1,
    posOrdinal: Int = -1,
    encodeColumns: Array[Boolean] = null
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
    val encodeCols = encodeColumns
    // #901 M4a safe by construction: copy only when the input is clustered by _file (a HashPartitioning on
    // exactly the _file attribute, after AQE). Our strategy wraps the copy child in RepartitionByExpression
    // (_file) so this holds; if for any reason it does not (AQE changed it, a plan we did not expect), fall
    // back to the streaming full rewrite rather than risk a split file.
    val copy = copyUnchanged && VectorIcebergWriteExec.clusteredByFile(query, fileOrd)
    if (copyUnchanged && !copy) {
      logInfo("vecruntime: M4a copy-unchanged declined -- input is not clustered by _file; full rewrite")
    }
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
                encodeCols,
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

  /**
   * Whether the resolved child's output is clustered by exactly the `_file` attribute at ordinal
   * `fileOrdinal` -- a `HashPartitioning` on that one attribute (#901 M4a). This is the safe-by-construction
   * gate for the copy path: a plain hash partitioning sends every row of a file to one task and is never
   * split by AQE skew handling, so a source file is whole in one task. Resolved through the adaptive final
   * plan and below the row transition, the same place [[columnar]] reads the batches.
   */
  def clusteredByFile(query: SparkPlan, fileOrdinal: Int): Boolean = {
    import org.apache.spark.sql.catalyst.expressions.AttributeSet
    import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioningLike, RangePartitioning}
    val plan = query match {
      case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
      case p => p
    }
    val child = plan match {
      case t: ColumnarToRowTransition => t.child
      case w: org.apache.spark.sql.execution.WholeStageCodegenExec
          if w.child.isInstanceOf[ColumnarToRowTransition] => w.child.children.head
      case p => p
    }
    if (fileOrdinal < 0 || fileOrdinal >= child.output.length) return false
    val fileAttr = child.output(fileOrdinal)
    val fileSet = AttributeSet(fileAttr)
    // Clustered by exactly _file: a hash (or range) partitioning whose expressions reference only _file. A
    // plain hash partitioning sends every row of a file to one reducer and is never split by AQE skew
    // handling (that only rewrites REBALANCE exchanges), so a source file is whole in one task.
    def onlyFile(exprs: Seq[org.apache.spark.sql.catalyst.expressions.Expression]): Boolean =
      exprs.nonEmpty && exprs.forall(e => e.references.nonEmpty && e.references.subsetOf(fileSet))
    val ok = child.outputPartitioning match {
      case h: HashPartitioningLike => onlyFile(h.expressions)
      case r: RangePartitioning => onlyFile(r.ordering.map(_.child))
      case _ => false
    }
    if (!ok) {
      logInfo(
        s"vecruntime: M4a clustering check -- outputPartitioning ${child.outputPartitioning.getClass.getSimpleName} of ${child.nodeName} is not a single-_file hash"
      )
    }
    ok
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
   * One task of a copy-on-write DELETE or UPDATE rewritten with the copy path (#901 M4a/M4b). The batch's data
   * columns (at `ordinals`) and its `_file` / `_pos` columns are handed to the bridge's rewrite task writer,
   * which buffers by source file and, on close, copies the untouched chunks of each file and re-encodes the
   * rest. `encodeColumns` is the per-write-schema-column re-encode mask: `true` to re-encode (a changed column
   * or a v3 lineage column), `false` to copy the chunk byte for byte. For a DELETE it is all-false except
   * lineage (copy every data column); for an UPDATE it carries the changed columns. Null means copy every data
   * column (the M4a default). Returns Iceberg's own commit message for the new data files.
   */
  def rewriteTask(
      spec: AnyRef,
      conf: org.apache.hadoop.conf.Configuration,
      ordinals: Array[Int],
      fileOrdinal: Int,
      posOrdinal: Int,
      encodeColumns: Array[Boolean],
      batches: Iterator[ColumnarBatch]
  ): WriterCommitMessage = {
    val ctx = TaskContext.get()
    // Dense column list per batch: the data columns, then _file, then _pos. The writer reads data at
    // [0, ordinals.length), _file at ordinals.length, _pos at ordinals.length + 1.
    val all = ordinals :+ fileOrdinal :+ posOrdinal
    val dataIdx = ordinals.indices.toArray
    // The encode mask is over the write-schema (data) columns, in the same order as `ordinals`; null -> copy
    // every data column. A changed data column (true) is re-encoded instead of copied.
    val encode = if (encodeColumns == null) new Array[Boolean](ordinals.length) else encodeColumns
    val writer =
      IcebergWriteBridge.createRewriteTaskWriter(
        spec,
        ctx.partitionId(),
        ctx.taskAttemptId(),
        conf,
        ordinals.length,
        ordinals.length + 1,
        dataIdx,
        encode
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
