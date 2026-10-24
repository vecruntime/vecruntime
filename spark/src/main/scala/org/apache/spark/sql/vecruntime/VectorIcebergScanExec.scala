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

import org.apache.hadoop.fs.Path
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.paths.SparkPath
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetUtils}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * #735: an Iceberg table read with our native Parquet reader over Iceberg's own planning. It wraps the
 * `BatchScanExec` Spark planned for an Iceberg scan and keeps everything Iceberg and Spark decided: the snapshot,
 * manifest and partition pruning, the residuals, runtime filtering and the task groups (`scan.inputRDD`'s
 * partitions). Per task group:
 *  - every task a Parquet data file with no delete files: the files are read by [[VectorParquetPartitionReader]]
 *    as Spark file splits (path, start, length), the columns matched by field id -- so renames and reordered
 *    columns read as Iceberg reads them -- with the same read-ahead and decode-ahead as our Parquet scan. A
 *    projected column a file lacks (added later) is read as nulls, as Iceberg reads it without an initial default
 *    (a column with one keeps Iceberg's scan);
 *  - a file with delete files (position deletes, a deletion vector, equality deletes): read natively on its own
 *    with its row positions (and its equality columns), the deleted rows deselected -- see [[IcebergTasks]];
 *  - anything else (encryption, another format, an equality column we cannot decode): Iceberg's own columnar
 *    reader for that group, whose batches
 *    our operators adapt as they do today.
 * The filter Spark keeps above the scan is pushed into the native reader (`pushedFilters`) to skip row groups and
 * pages by their statistics, each column matched to the file's by field id, never by name; the filter above still
 * decides which rows pass.
 */
case class VectorIcebergScanExec(
    scan: BatchScanExec,
    fieldIds: Seq[Int],
    // The filter above the scan, translated (see VectorColumnarRule): row groups are skipped by it, matched to each
    // file's columns by field id; the filter above still decides which rows pass.
    pushedFilters: Seq[org.apache.spark.sql.sources.Filter] = Nil,
    // Whether the parent is one of our operators, which reads a batch's selection (a file's deleted rows
    // deselected). Otherwise -- Spark's ColumnarToRow under a row operator -- every batch is compacted to its
    // selected rows, since Spark reads every physical row (see VectorColumnarRule.markSelectionProducers).
    emitSelection: Boolean = false
) extends LeafExecNode
    with VectorPlan
    with RuntimeFilterTarget {

  override val output: Seq[Attribute] = scan.output
  override def outputPartitioning: Partitioning = scan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = scan.outputOrdering

  /**
   * Filters a broadcast hash join above derived from its build keys (#610), set on the driver before this node's
   * RDD is made, as [[VectorParquetScanExec]] takes them. ANDed with `pushedFilters`, they skip row groups by their
   * statistics, each column matched by field id; the join above still decides which rows pass.
   */
  @transient @volatile private[vecruntime] var runtimeFilters: Seq[org.apache.spark.sql.sources.Filter] = Nil

  /** The runtime filters attached so far (tests). */
  def runtimeFilterCount: Int = runtimeFilters.size

  private[vecruntime] def addRuntimeFilter(f: org.apache.spark.sql.sources.Filter): Unit = synchronized {
    if (!runtimeFilters.contains(f)) runtimeFilters = runtimeFilters :+ f
  }

  /**
   * A data column of the files (not an Iceberg metadata column) whose name needs no quoting: the pushed filters
   * are matched to field ids by quoted name, and a runtime filter names the column as it is.
   */
  private[vecruntime] def isDataColumn(name: String): Boolean =
    output.zip(fieldIds).exists { case (a, id) =>
      a.name == name && IcebergTasks.columnKind(a.name, id) == IcebergTasks.DataColumn &&
      org.apache.spark.sql.catalyst.util.quoteIfNeeded(name) == name
    }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numRuntimeFilters" -> SQLMetrics.createMetric(sparkContext, "number of runtime filters from joins"),
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "numFilesPrunedByRuntimeFilter" ->
      SQLMetrics.createMetric(sparkContext, "data files not opened, pruned by a join's runtime filter"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numRowGroups" -> SQLMetrics.createMetric(sparkContext, "number of row groups read"),
    "numLateSkippedRows" -> SQLMetrics.createMetric(sparkContext, "rows the decode filter dropped before decoding"),
    "numPagesSkipped" -> SQLMetrics.createMetric(sparkContext, "pages the column index skipped"),
    "numRowGroupsLate" -> SQLMetrics.createMetric(sparkContext, "row groups late-materialized (auto)"),
    "numRowGroupsEager" -> SQLMetrics.createMetric(sparkContext, "row groups decoded eagerly (auto)"),
    "lateSelectivityPpm" ->
      SQLMetrics.createAverageMetric(sparkContext, "observed surviving fraction (ppm, auto late groups)"),
    "numNativeGroups" -> SQLMetrics.createMetric(sparkContext, "task groups read natively"),
    "numIcebergGroups" -> SQLMetrics.createMetric(sparkContext, "task groups read by Iceberg's reader"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")
  )

  override def doCanonicalize(): SparkPlan = copy(scan = scan.canonicalized.asInstanceOf[BatchScanExec])

  // The wrapped scan owns its runtime-filter subqueries; we never call its executeQuery, so start and await them
  // ourselves before its partitions are read (as VectorParquetScanExec does for a file scan).
  override protected def doPrepare(): Unit = {
    super.doPrepare()
    scan.prepare()
    org.apache.spark.sql.execution.vector.FileScanAccess.prepareAndWaitForSubqueries(scan)
  }

  override def simpleString(maxFields: Int): String = {
    val pushed = if (pushedFilters.isEmpty) "" else pushedFilters.mkString("PushedFilters: [", ", ", "] ")
    s"VectorIcebergScan $pushed${scan.simpleString(maxFields)}"
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val sqlConf = session.sessionState.conf
    // The static pushed filters and the joins' runtime filters (#610), read now: the joins attached them on the
    // driver before executing this side.
    val runtime = if (sqlConf.parquetFilterPushDown) runtimeFilters else Nil
    val filters = pushedFilters ++ runtime
    // #iceberg-file-pruning: the data-file locations a join's runtime filters prove cannot match, computed on the
    // driver from each file's Iceberg metrics (InclusiveMetricsEvaluator). Those files are dropped before the
    // partitions are built, so they are never opened (a HEAD + footer read each avoided), instead of being opened
    // and only their row groups skipped. Empty when pruning is off, there is no runtime filter, or none converts.
    val runtimeDropLocations: Set[String] =
      if (runtime.isEmpty || !io.vecruntime.spark.VectorConf.scanNativeIcebergRuntimeFilePruning(sqlConf)) Set.empty
      else IcebergTasks.runtimeFilePruneDropSet(scan, runtime, sqlConf.caseSensitiveAnalysis).getOrElse(Set.empty)
    // The count is exact and known on the driver (these files are never scheduled), so set the metric here.
    if (runtimeDropLocations.nonEmpty) longMetric("numFilesPrunedByRuntimeFilter") += runtimeDropLocations.size
    // As the Parquet scan: runtime filters prune row groups by statistics only; with no static filter but null
    // checks, Parquet's dictionary and bloom checks (an object-store request per row group) are off.
    val statsOnly = runtime.nonEmpty && pushedFilters.forall(_.isInstanceOf[org.apache.spark.sql.sources.IsNotNull])
    // The output's data columns (metadata columns aside), each carrying its Iceberg field id, so the reader
    // matches the file's columns by id. #20 M3a: `_row_id` and `_last_updated_sequence_number` are read from the
    // file too, by their reserved field id -- a file that materialises them (copy-on-write, compaction) carries
    // the column, a file that does not reads nulls and takes the per-file fallback (see `metadataLong`).
    val kinds = output.zip(fieldIds).map { case (a, id) => IcebergTasks.columnKind(a.name, id) }
    val dataColumns = output.zip(fieldIds).zip(kinds).collect {
      case ((a, id), IcebergTasks.DataColumn | IcebergTasks.RowIdColumn | IcebergTasks.SeqNumColumn) => (a, id)
    }
    val requiredSchema = StructType(dataColumns.map { case (a, id) =>
      StructField(
        a.name,
        // #nested: nested struct fields matched by their Iceberg field ids too.
        if (a.dataType.isInstanceOf[AtomicType]) a.dataType
        else IcebergTasks.withFieldIds(a.dataType, IcebergTasks.projectedType(scan, id)),
        a.nullable, {
          val md = new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id.toLong)
          // A v3 initial default: our reader fills it into files written before the column was added. The reserved
          // metadata ids have no projected field, so skip the lookup for them (metadataLong fills them instead).
          if (IcebergTasks.columnKind(a.name, id) == IcebergTasks.DataColumn) {
            IcebergTasks.initialDefaultSql(scan, id, a.dataType).foreach { sql =>
              md.putString(
                org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY,
                sql
              )
            }
          }
          md.build()
        }
      )
    })
    def confFor(schema: StructType) = {
      val hadoopConf = session.sessionState.newHadoopConf()
      // Spark's fallback reader takes the requested schema from the conf.
      hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, schema.json)
      hadoopConf.set(
        org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport.SPARK_ROW_SCHEMA,
        schema.json
      )
      hadoopConf.set(
        org.apache.parquet.hadoop.ParquetInputFormat.READ_SUPPORT_CLASS,
        classOf[ParquetReadSupport].getName
      )
      hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sqlConf.sessionLocalTimeZone)
      hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis)
      hadoopConf.setBoolean(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key, true)
      // Read by Spark's schema converter when a file falls back to Spark's reader (as the Parquet scan sets them):
      // e.g. a renamed column whose old name a new column took (idNameClash).
      hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, sqlConf.isParquetBinaryAsString)
      hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, sqlConf.isParquetINT96AsTimestamp)
      hadoopConf.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, sqlConf.parquetInferTimestampNTZEnabled)
      hadoopConf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, sqlConf.legacyParquetNanosAsLong)
      if (statsOnly) {
        hadoopConf.setBoolean(org.apache.parquet.hadoop.ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, false)
        hadoopConf.setBoolean(org.apache.parquet.hadoop.ParquetInputFormat.BLOOM_FILTERING_ENABLED, false)
      }
      session.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    }
    val confBroadcast = confFor(requiredSchema)
    val positionsConfBroadcast = confFor(VectorIcebergRDD.withPositions(requiredSchema))
    // Files with equality deletes are read with the equality columns the query does not project appended (each
    // by its field id), then the row positions; the extra columns are dropped once the deletes are applied.
    // None: an equality column our reader cannot decode, so those files keep Iceberg's reader.
    val tableSchema = IcebergTasks.tableSchema(scan)
    // The schema HISTORY (current + superseded): an equality delete can be keyed on a since-dropped column, whose
    // field id lives only in an older schema. Threaded into both the probe-column resolution and the filter build.
    val tableSchemas = IcebergTasks.tableSchemas(scan)
    val equalitySchema = IcebergTasks
      .equalityColumns(scan.inputRDD.partitions, tableSchemas, dataColumns.map(_._2).toSet)
      .map(extra => VectorIcebergRDD.withPositions(StructType(requiredSchema.fields ++ extra)))
    val equalityConfBroadcast = equalitySchema.map(confFor)

    // Pushed filters by field id: the data columns' quoted names, as the translated filters name them. A uuid
    // column is excluded (#rtypes): it reads as Spark's StringType (canonical text) but is stored as the raw
    // 16 bytes, so a string predicate against the file's byte statistics would skip the wrong row groups/pages.
    val filterFieldIds =
      if (filters.isEmpty || !sqlConf.parquetFilterPushDown) null
      else
        dataColumns.collect {
          case (a, id)
              if IcebergTasks.columnKind(a.name, id) != IcebergTasks.DataColumn ||
                !IcebergTasks.isUuid(scan, id) =>
            org.apache.spark.sql.catalyst.util.quoteIfNeeded(a.name) -> id
        }.toMap
    val m = ScanMetrics(
      longMetric("numFiles"),
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"),
      longMetric("numRowGroups"),
      longMetric("scanTime"),
      longMetric("numLateSkippedRows"),
      longMetric("numPagesSkipped"),
      longMetric("numRowGroupsLate"),
      longMetric("numRowGroupsEager"),
      longMetric("lateSelectivityPpm")
    )
    val rdd = new VectorIcebergRDD(
      session.sparkContext,
      VectorIcebergRDD.packed(scan, sqlConf, runtimeDropLocations),
      scan.readerFactory,
      fieldIds.toArray,
      confBroadcast,
      positionsConfBroadcast,
      equalityConfBroadcast,
      equalitySchema,
      tableSchema,
      tableSchemas,
      requiredSchema,
      dataColumns.map { case (a, _) => (a.name, a.dataType) }.toArray,
      kinds.toArray,
      math.max(64, sqlConf.parquetVectorizedReaderBatchSize / 64 * 64),
      NativeScanRebaseConf(
        sqlConf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ).toString,
        sqlConf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ).toString,
        sqlConf.isParquetINT96TimestampConversion,
        sqlConf.sessionLocalTimeZone
      ),
      io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchFiles(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchRowGroups(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetDecodeAhead(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetDecodeAheadVirtual(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetParallelDecode(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetParallelDecodeVirtual(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetParallelDecodeMinRowGroupBytes(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetDictionaryStrings(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchMaxBytes(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchExecutorMaxBytes(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetReuseDecompressBuffers(sqlConf),
      io.vecruntime.spark.VectorConf.scanNativeParquetPoolChunkBuffers(sqlConf),
      m,
      longMetric("numNativeGroups"),
      longMetric("numIcebergGroups"),
      if (filterFieldIds == null) Nil else filters,
      filterFieldIds,
      PushDownConf(
        sqlConf.parquetFilterPushDownDate,
        sqlConf.parquetFilterPushDownTimestamp,
        sqlConf.parquetFilterPushDownDecimal,
        sqlConf.parquetFilterPushDownStringPredicate,
        sqlConf.parquetFilterPushDownInFilterThreshold
      ),
      IcebergTasks.nameMapping(scan),
      // #901 R1: Iceberg's own column resolution (iceberg-bridge), when on and the bridge is on the classpath.
      // The schema history is passed so a since-dropped equality-delete key column (read as a hidden probe
      // column) resolves against the schema it was dropped from, not only the current schema.
      if (!io.vecruntime.spark.VectorConf.icebergReadSpecsEnabled(sqlConf)) null
      else
        ParquetFileResolver
          .iceberg(
            IcebergTasks.tableSchema(scan),
            IcebergTasks.nameMappingJson(scan),
            tableSchemas.asInstanceOf[Seq[AnyRef]].asJava
          )
          .orNull,
      io.vecruntime.spark.VectorConf.icebergNativeEqualityDeletesEnabled(sqlConf),
      // #20 M3a: the table's unified partition type (serializable) and the `_partition` output's Spark struct type,
      // used to build the per-file `_partition` constant; null when `_partition` is not projected.
      if (kinds.contains(IcebergTasks.PartitionColumn)) IcebergTasks.partitionType(scan) else null,
      output.zip(kinds).collectFirst { case (a, IcebergTasks.PartitionColumn) =>
        a.dataType.asInstanceOf[StructType]
      }.orNull,
      // #901 item 6: the output schema for the Iceberg row-reader fallback's row-to-columnar conversion.
      StructType(output.map(a => StructField(a.name, a.dataType, a.nullable))),
      runtimeDropLocations
    )
    val out: RDD[ColumnarBatch] =
      if (emitSelection) rdd else rdd.mapPartitionsInternal(_.map(VectorIcebergScanExec.compacted))
    // Counted by the task of partition 0, once per execution, as the Parquet scan counts them.
    if (runtime.isEmpty) out
    else {
      val counter = longMetric("numRuntimeFilters")
      val n = runtime.size.toLong
      out.mapPartitionsWithIndexInternal { (i, it) => if (i == 0) counter += n; it }
    }
  }

}

object VectorIcebergScanExec {

  /**
   * A batch whose deleted rows are deselected (a [[io.vecruntime.spark.arrow.SelectedColumnarBatch]]) as a plain
   * batch of its selected rows, for a consumer that reads every physical row: each column a view over the
   * selected row ids. Any other batch as it is.
   */
  def compacted(batch: ColumnarBatch): ColumnarBatch = batch match {
    case b: io.vecruntime.spark.arrow.SelectedColumnarBatch =>
      val n = b.numRows()
      val count = b.selectedCount()
      val columns = io.vecruntime.spark.arrow.SelectedColumnarBatch.columnsOf(b)
      if (count == n) new ColumnarBatch(columns, n)
      else {
        val rows = io.vecruntime.spark.arrow.RemappedColumnVector.rowsOf(b.selection(), n, count)
        new ColumnarBatch(columns.map(c => io.vecruntime.spark.arrow.RemappedColumnVector.of(c, rows)), count)
      }
    case other => other
  }
}

/**
 * The scan's RDD: the wrapped `BatchScanExec`'s own partitions (Iceberg's task groups after runtime filtering and
 * key grouping), each read natively when every task of its groups allows it, else by Iceberg's reader.
 */
private[vecruntime] final class VectorIcebergRDD(
    sc: org.apache.spark.SparkContext,
    @transient private val parts: Array[Partition],
    readerFactory: PartitionReaderFactory,
    fieldIds: Array[Int],
    confBroadcast: org.apache.spark.broadcast.Broadcast[SerializableConfiguration],
    positionsConfBroadcast: org.apache.spark.broadcast.Broadcast[SerializableConfiguration],
    equalityConfBroadcast: Option[org.apache.spark.broadcast.Broadcast[SerializableConfiguration]],
    equalitySchema: Option[StructType],
    tableSchema: AnyRef, // org.apache.iceberg.Schema (java.io.Serializable)
    tableSchemas: Seq[AnyRef], // the table's schema history, current first (each a serializable Iceberg Schema)
    requiredSchema: StructType,
    attrs: Array[(String, org.apache.spark.sql.types.DataType)], // the data columns, in output order
    kinds: Array[IcebergTasks.ColumnKind], // per output column
    batchSize: Int,
    rebaseConf: NativeScanRebaseConf,
    prefetchFiles: Int,
    prefetchRowGroups: Int,
    decodeAhead: Int,
    decodeAheadVirtual: Boolean,
    parallelDecode: Int,
    parallelDecodeVirtual: Boolean,
    parallelDecodeMinRowGroupBytes: Long,
    dictionaryStrings: Boolean,
    prefetchMaxBytes: Long,
    prefetchExecutorMaxBytes: Long,
    reuseDecompressBuffers: Boolean,
    poolChunkBuffers: Boolean,
    metrics: ScanMetrics,
    nativeGroups: SQLMetric,
    icebergGroups: SQLMetric,
    pushedFilters: Seq[org.apache.spark.sql.sources.Filter],
    filterFieldIds: Map[String, Int], // null: nothing pushed
    pushDown: PushDownConf,
    nameMapping: IcebergNameMapping, // #849: null when the table has none
    fileResolver: ParquetFileResolver, // #901 R1: null: Spark's clipParquetSchema
    nativeEqualityDeletes: Boolean, // #901 R2: probe equality deletes on the batch's lanes (iceberg-bridge)
    // #20 M3a: the table's unified partition type (org.apache.iceberg.types.Type, serializable) and the
    // `_partition` output's Spark struct type; both null when `_partition` is not projected.
    partitionType: AnyRef,
    partitionSparkType: StructType,
    // #901 item 6: the scan's output schema (names + types, in output order), to convert an Iceberg row-reader
    // group's InternalRows into columnar batches and reorder them into output order.
    outputSchema: StructType,
    // #iceberg-file-pruning: data-file locations a join's runtime filters proved cannot match. A native task
    // whose data file is in this set is dropped in `compute`, so the file is never opened. Empty: nothing pruned.
    runtimeDropLocations: Set[String]
) extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = parts

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    VectorIcebergRDD.inputsOf(split).flatMap(_.preferredLocations()).distinct

  // Iceberg metadata columns. `_pos` is the reader's row-index column; `_file` and `_spec_id` are per-file
  // constants, passed to the reader as the split's partition values (it appends them after the data columns).
  // #20 M3a: `_row_id` needs the row positions too (its fallback is `firstRowId + _pos`); `_partition`, `_row_id`
  // and `_last_updated_sequence_number` all need the per-file metadata, keyed by the batch's `_file` constant.
  private val needsMeta =
    kinds.exists(k =>
      k == IcebergTasks.PartitionColumn || k == IcebergTasks.RowIdColumn || k == IcebergTasks.SeqNumColumn
    )
  // #901 item 3: when _deleted is projected the scan keeps deleted rows and marks them, instead of dropping them.
  private val markDeleted = kinds.contains(IcebergTasks.DeletedColumn)
  private val deletedOrdinal = kinds.indexOf(IcebergTasks.DeletedColumn)
  private val needsPositions =
    kinds.contains(IcebergTasks.PosColumn) || kinds.contains(IcebergTasks.RowIdColumn)
  private val constants =
    kinds.exists(k => k == IcebergTasks.FileColumn || k == IcebergTasks.SpecIdColumn) || needsMeta
  private val constantSchema =
    if (constants) IcebergTasks.ConstantSchema else new StructType()
  // Whether a reader batch can be handed on as it is: only data columns, in output order.
  private val plain = kinds.forall(_ == IcebergTasks.DataColumn)

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val inputs = VectorIcebergRDD.inputsOf(split)
    // In order: a run of native groups is read as one file partition (so read-ahead crosses groups), each other
    // group by Iceberg's reader, the order of the groups kept.
    val segments = Seq.newBuilder[() => Iterator[ColumnarBatch]]
    val pending = Seq.newBuilder[PartitionedFile]
    var pendingCount = 0
    // #20 M3a: the per-file metadata of the three v3 columns, keyed by data-file location; empty when not needed.
    val fileMeta = scala.collection.mutable.HashMap.empty[String, IcebergTasks.FileMetadata]
    def flush(): Unit = if (pendingCount > 0) {
      val files = pending.result().toArray
      pending.clear()
      pendingCount = 0
      segments += (() => nativeIterator(split.index, files, fileMeta, context))
    }
    // Equality delete sets loaded once per Spark task: many data files share the same delete files.
    val equalitySets = scala.collection.mutable.HashMap.empty[(Seq[String], Seq[Int]), AnyRef]
    inputs.foreach { p =>
      IcebergTasks.nativeTasks(p, equality = equalitySchema.isDefined, constants = constants) match {
        case Some(tasks) =>
          nativeGroups += 1
          tasks.foreach { t =>
            // #iceberg-file-pruning: a data file a join's runtime filter proved cannot match is dropped here, so
            // its reader is never built and the file is never opened. (The pruned-file count is set on the driver.)
            if (!runtimeDropLocations.contains(t.location)) {
              if (needsMeta) fileMeta.getOrElseUpdate(t.location, IcebergTasks.fileMetadata(t.task, partitionType))
              if (t.deletes.isEmpty && t.equalityDeletes.isEmpty) {
                pending += t.file
                pendingCount += 1
              } else {
                // A file with delete files: read with its row positions (and its equality columns), the deleted
                // rows deselected (its delete files are loaded when its first batch is read).
                flush()
                segments += (() => deletedIterator(split.index, p, t, equalitySets, fileMeta, context))
              }
            }
          }
        case None =>
          icebergGroups += 1
          flush()
          segments += (() => icebergIterator(p, context))
      }
    }
    flush()
    segments.result().iterator.flatMap(make => make())
  }

  /**
   * The output columns of a reader batch laid out as [data columns][`extra` equality columns][row positions,
   * when `positions`][constants]: per output column, its ordinal in the batch. `_row_id` and
   * `_last_updated_sequence_number` are data columns (read from the file by their reserved id); `_partition` has
   * no reader column and maps to -1 (built as a constant in [[metaRebuild]]).
   */
  private def layout(extra: Int, positions: Boolean): Array[Int] = {
    val d = attrs.length
    val posOrdinal = d + extra
    val constBase = posOrdinal + (if (positions) 1 else 0)
    var data = 0
    kinds.map {
      case IcebergTasks.DataColumn | IcebergTasks.RowIdColumn | IcebergTasks.SeqNumColumn => data += 1; data - 1
      case IcebergTasks.PosColumn => posOrdinal
      case IcebergTasks.FileColumn => constBase
      case IcebergTasks.SpecIdColumn => constBase + 1
      case IcebergTasks.PartitionColumn => -1
      case IcebergTasks.DeletedColumn => -1
    }
  }

  /** The batch ordinal of the row positions column (`withPositions` appends it after the data + equality columns). */
  private def posOrdinalOf(extra: Int): Int = attrs.length + extra

  /** The batch ordinal of the `_file` constant (the reader appends `_file`, `_spec_id` after the positions). */
  private def fileOrdinalOf(extra: Int, positions: Boolean): Int = attrs.length + extra + (if (positions) 1 else 0)

  /**
   * #20 M3a: when the three v3 metadata columns are projected, replaces their borrowed columns in the output-order
   * array: `_partition` with a per-file struct constant, `_row_id` / `_last_updated_sequence_number` with a long
   * column coalescing the file's own values (where it materialises them) with the per-file fallback. The reader
   * batch identifies its file by the `_file` constant (one file per batch). null when no such column is projected.
   */
  private def metaRebuild(
      fileMeta: scala.collection.mutable.HashMap[String, IcebergTasks.FileMetadata],
      posOrdinal: Int,
      fileOrdinal: Int
  ): (ColumnarBatch, Array[ColumnVector]) => Unit = {
    if (!needsMeta) return null
    (batch: ColumnarBatch, columns: Array[ColumnVector]) => {
      val n = batch.numRows()
      val meta =
        if (n == 0) null
        else fileMeta.get(batch.column(fileOrdinal).getUTF8String(0).toString).orNull
      var j = 0
      while (j < kinds.length) {
        kinds(j) match {
          case IcebergTasks.PartitionColumn =>
            columns(j) = IcebergTasks.partitionConstant(
              partitionSparkType,
              if (meta == null) null else meta.partition,
              n
            )
          case IcebergTasks.RowIdColumn =>
            // The file's own `_row_id` where non-null, else `firstRowId + _pos` (null when `firstRowId` is null).
            val read = batch.column(layoutData(j))
            val pos = batch.column(posOrdinal)
            val first = if (meta == null) null else meta.firstRowId
            columns(j) =
              IcebergTasks.metadataLong(read, n, i => if (first == null) null else Long.box(first + pos.getLong(i)))
          case IcebergTasks.SeqNumColumn =>
            val read = batch.column(layoutData(j))
            val seq = if (meta == null) null else meta.seqNum
            columns(j) = IcebergTasks.metadataLong(read, n, _ => seq)
          case _ =>
        }
        j += 1
      }
    }
  }

  /** The data-column ordinal of output column `j` (its position among the file-read columns). */
  private def layoutData(j: Int): Int = {
    var data = 0
    var k = 0
    while (k < j) {
      kinds(k) match {
        case IcebergTasks.DataColumn | IcebergTasks.RowIdColumn | IcebergTasks.SeqNumColumn => data += 1
        case _ =>
      }
      k += 1
    }
    data
  }

  private def deletedIterator(
      index: Int,
      p: InputPartition,
      t: IcebergTasks.NativeTask,
      equalitySets: scala.collection.mutable.HashMap[(Seq[String], Seq[Int]), AnyRef],
      fileMeta: scala.collection.mutable.HashMap[String, IcebergTasks.FileMetadata],
      context: TaskContext
  ): Iterator[ColumnarBatch] = {
    lazy val deleted =
      if (t.deletes.isEmpty) Array.emptyLongArray else IcebergTasks.deletedPositions(p, t.location, t.deletes)
    if (t.equalityDeletes.isEmpty) {
      val out = layout(0, positions = true)
      val rebuild = metaRebuild(fileMeta, posOrdinalOf(0), fileOrdinalOf(0, positions = true))
      readerOf(index, Array(t.file), positionsConfBroadcast, VectorIcebergRDD.withPositions(requiredSchema), context)
        .map(b =>
          if (markDeleted) IcebergTasks.withDeletedMarked(b, out, deletedOrdinal, attrs.length, deleted, null, rebuild)
          else IcebergTasks.withoutDeleted(b, out, attrs.length, deleted, null, rebuild)
        )
    } else {
      val schema = equalitySchema.get
      val extra = schema.length - attrs.length - 1
      val out = layout(extra, positions = true)
      val rebuild = metaRebuild(fileMeta, posOrdinalOf(extra), fileOrdinalOf(extra, positions = true))
      lazy val equality =
        IcebergTasks.equalityFilter(p, t.equalityDeletes, tableSchemas, schema, equalitySets, nativeEqualityDeletes)
      readerOf(index, Array(t.file), equalityConfBroadcast.get, schema, context)
        .map(b =>
          if (markDeleted)
            IcebergTasks.withDeletedMarked(b, out, deletedOrdinal, schema.length - 1, deleted, equality(b), rebuild)
          else IcebergTasks.withoutDeleted(b, out, schema.length - 1, deleted, equality(b), rebuild)
        )
    }
  }

  private def nativeIterator(
      index: Int,
      files: Array[PartitionedFile],
      fileMeta: scala.collection.mutable.HashMap[String, IcebergTasks.FileMetadata],
      context: TaskContext
  ): Iterator[ColumnarBatch] =
    if (plain) readerOf(index, files, confBroadcast, requiredSchema, context)
    else if (needsPositions) {
      val out = layout(0, positions = true)
      val rebuild = metaRebuild(fileMeta, posOrdinalOf(0), fileOrdinalOf(0, positions = true))
      readerOf(index, files, positionsConfBroadcast, VectorIcebergRDD.withPositions(requiredSchema), context)
        .map(b =>
          if (markDeleted)
            IcebergTasks.withDeletedMarked(b, out, deletedOrdinal, attrs.length, Array.emptyLongArray, null, rebuild)
          else IcebergTasks.withoutDeleted(b, out, attrs.length, Array.emptyLongArray, null, rebuild)
        )
    } else {
      val out = layout(0, positions = false)
      val rebuild = metaRebuild(fileMeta, -1, fileOrdinalOf(0, positions = false))
      // #901 item 3: a file with no delete files has `_deleted` false in every row.
      if (markDeleted)
        readerOf(index, files, confBroadcast, requiredSchema, context)
          .map(b => IcebergTasks.withDeletedMarked(b, out, deletedOrdinal, -1, Array.emptyLongArray, null, rebuild))
      else
        readerOf(index, files, confBroadcast, requiredSchema, context).map(b =>
          IcebergTasks.reordered(b, out, rebuild)
        )
    }

  private def readerOf(
      index: Int,
      files: Array[PartitionedFile],
      conf: org.apache.spark.broadcast.Broadcast[SerializableConfiguration],
      schema: StructType,
      context: TaskContext
  ): Iterator[ColumnarBatch] = {
    val columns = attrs ++ (schema.fields.drop(attrs.length) ++ constantSchema.fields).map(f => (f.name, f.dataType))
    val reader = new VectorParquetPartitionReader(
      FilePartition(index, files),
      conf.value.value,
      schema,
      constantSchema,
      columns,
      batchSize,
      caseSensitive = false,
      useFieldId = true,
      pushedFilters = pushedFilters,
      pushDownDate = pushDown.date,
      pushDownTimestamp = pushDown.timestamp,
      pushDownDecimal = pushDown.decimal,
      pushDownStringPredicate = pushDown.stringPredicate,
      pushDownInFilterThreshold = pushDown.inFilterThreshold,
      rebaseConf = rebaseConf,
      prefetchFiles = prefetchFiles,
      prefetchRowGroups = prefetchRowGroups,
      metrics = metrics,
      context = context,
      handOff = decodeAhead > 0 || parallelDecode > 0,
      poolLimit = math.max(4, decodeAhead + 2),
      dictionaryStrings = dictionaryStrings,
      prefetchMaxBytes = prefetchMaxBytes,
      prefetchExecutorMaxBytes = prefetchExecutorMaxBytes,
      reuseDecompressBuffers = reuseDecompressBuffers,
      poolChunkBuffers = poolChunkBuffers,
      filterFieldIds = filterFieldIds,
      nameMapping = nameMapping,
      fileResolver = fileResolver,
      parallelDecode = parallelDecode,
      parallelDecodeMinRowGroupBytes = parallelDecodeMinRowGroupBytes
    )
    if (parallelDecode > 0) new ParallelDecodeIterator(reader, parallelDecode, parallelDecodeVirtual, context)
    else if (decodeAhead > 0) new DecodeAheadIterator(reader, decodeAhead, decodeAheadVirtual, context)
    else reader
  }

  private def icebergIterator(p: InputPartition, context: TaskContext): Iterator[ColumnarBatch] = {
    if (readerFactory.supportColumnarReads(p)) {
      val reader = readerFactory.createColumnarReader(p)
      context.addTaskCompletionListener[Unit](_ => reader.close())
      return new Iterator[ColumnarBatch] {
        private var advanced = false
        private var more = false
        override def hasNext: Boolean = {
          if (!advanced) { more = reader.next(); advanced = true }
          more
        }
        override def next(): ColumnarBatch = {
          if (!hasNext) throw new NoSuchElementException("end of the Iceberg task group")
          advanced = false
          reader.get()
        }
      }
    }
    // #861 / #901 item 6: Iceberg does not vectorize this group (nested columns, an ORC/Avro file, a delete our
    // reader cannot apply, or encryption). Read it with Iceberg's own ROW reader inside this node and convert the
    // rows to columnar batches, so the other task groups stay native. The row reader yields rows of the scan's
    // read schema (= the output schema, in output order), so the converter writes straight into output order.
    // Iceberg's reader reads the group exactly as it would on its own (its FileIO decrypts an encrypted file), so
    // no group is ever read with wrong values.
    val reader = readerFactory.createReader(p)
    context.addTaskCompletionListener[Unit](_ => reader.close())
    val converter = new org.apache.spark.sql.execution.vector.RowToColumnarAccess(outputSchema)
    new Iterator[ColumnarBatch] {
      private var pulled = false
      private var has = false
      private def advance(): Unit = if (!pulled) { has = reader.next(); pulled = true }
      override def hasNext: Boolean = { advance(); has }
      override def next(): ColumnarBatch = {
        advance()
        if (!has) throw new NoSuchElementException("end of the Iceberg task group")
        val vectors =
          org.apache.spark.sql.execution.vectorized.OnHeapColumnVector.allocateColumns(batchSize, outputSchema)
        val writable = vectors.asInstanceOf[Array[org.apache.spark.sql.execution.vectorized.WritableColumnVector]]
        var rows = 0
        while (has && rows < batchSize) {
          converter.convert(reader.get(), writable)
          rows += 1
          has = reader.next()
        }
        pulled = true
        val batch = new ColumnarBatch(vectors.asInstanceOf[Array[ColumnVector]], rows)
        context.addTaskCompletionListener[Unit](_ => batch.close())
        batch
      }
    }
  }
}

/** Spark's Parquet filter push-down switches, for the filters pushed into [[VectorIcebergRDD]]'s reader. */
private[vecruntime] final case class PushDownConf(
    date: Boolean,
    timestamp: Boolean,
    decimal: Boolean,
    stringPredicate: Boolean,
    inFilterThreshold: Int
)

/** Helpers of [[VectorIcebergRDD]]. */
/** Several of Iceberg's input partitions read as one Spark task (see [[VectorIcebergRDD.packed]]). */
private[vecruntime] final class VectorIcebergPartition(override val index: Int, val inputs: Seq[InputPartition])
    extends Partition

private[vecruntime] object VectorIcebergRDD {

  /** The Iceberg input partitions of one of the RDD's partitions, packed or the wrapped scan's own. */
  def inputsOf(split: Partition): Seq[InputPartition] = split match {
    case v: VectorIcebergPartition => v.inputs
    case p => shims.SparkShims.dataSourceInputPartitions(p)
  }

  /**
   * The wrapped scan's partitions coalesced only within Iceberg's own split boundary: Iceberg already grouped the
   * task files honouring `read.split.target-size`, `read.split.open-file-cost` and `read.split.planning-lookback`
   * (see [[io.vecruntime.spark.VectorConf.ScanNativeIcebergPackSplits]]), so we may merge adjacent groups ONLY up
   * to Iceberg's own target split size, weighing each group by Iceberg's own open-file-cost per file. This still
   * coalesces genuinely tiny groups (Iceberg's lookback can leave several small groups), but it never merges groups
   * Iceberg deliberately kept apart -- e.g. a table with a huge `read.split.open-file-cost` (one file per group, as
   * the Iceberg copy-on-write skew tests set it) keeps one partition per group, so the write produces one file per
   * group as Spark's own path does. The scan's own partitions when it reports a partitioning, when packing is off
   * or merges nothing, or when a group's size is unknown.
   */
  def packed(scan: BatchScanExec, conf: SQLConf, dropLocations: Set[String]): Array[Partition] = {
    val parts = scan.inputRDD.partitions
    if (
      parts.length < 2 || !io.vecruntime.spark.VectorConf.scanNativeIcebergPackSplits(conf) ||
      !scan.outputPartitioning.isInstanceOf[org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning]
    ) return parts
    // Iceberg's OWN split settings (table properties, with Iceberg's defaults), not Spark's file-scan knobs: using
    // Spark's spark.sql.files.openCostInBytes / maxPartitionBytes / minPartitionNum re-packs the groups to the
    // cluster's parallelism and merges groups Iceberg kept separate (a read.split.open-file-cost it set high), which
    // collapses the output-file count of a row-level write below what Iceberg planned.
    val openCost = IcebergTasks.splitOpenFileCost(scan)
    val targetSize = IcebergTasks.splitSize(scan)
    val inputs = parts.map(p => shims.SparkShims.dataSourceInputPartitions(p))
    val sizes = inputs.map(_.map(IcebergTasks.groupBytes(_, openCost)))
    if (sizes.exists(_.exists(_ < 0))) return parts
    // #iceberg-file-pruning (#987): a pruned file is never opened, so it carries no weight. Discount the pruned
    // files' bytes + open cost from each group's weight; a group left at weight 0 (every file pruned) is dropped
    // from packing entirely. The pruned tasks themselves are skipped in `compute`, so an un-dropped partial group
    // is still correct -- this only keeps the packing balanced and avoids scheduling an all-pruned group.
    val weights =
      if (dropLocations.isEmpty) sizes.map(_.sum)
      else inputs.map(ps => ps.map(p => IcebergTasks.groupBytes(p, openCost, dropLocations)).filter(_ >= 0).sum)
    // Never beyond Iceberg's own target split size: a merge up to targetSize keeps the file-size contract Iceberg's
    // grouping already enforced, so no group Iceberg kept separate (because merging would exceed its target) is
    // merged here either. We do NOT widen the split to weights.sum / parallelism the way a Spark file scan does;
    // openCost and targetSize come from the scan's own SparkReadConf (read option over table property), so a
    // per-scan split-size / file-open-cost (the bin-pack rewrite runner's) is honoured exactly as Iceberg does.
    val maxSplit = math.max(openCost, targetSize)
    val out = Array.newBuilder[Partition]
    var current = Seq.newBuilder[InputPartition]
    var currentSize = 0L
    var n = 0
    def close(): Unit = if (currentSize > 0) {
      out += new VectorIcebergPartition(n, current.result())
      n += 1
      current = Seq.newBuilder[InputPartition]
      currentSize = 0L
    }
    weights.indices.sortBy(i => -weights(i)).foreach { i =>
      // An all-pruned group (weight 0) is dropped: it would open no file and schedule an empty task.
      if (weights(i) > 0) {
        if (currentSize > 0 && currentSize + weights(i) > maxSplit) close()
        current ++= inputs(i)
        currentSize += weights(i)
      }
    }
    close()
    val packedParts = out.result()
    // Nothing merged AND nothing pruned: keep the scan's own partitions (identity). When pruning dropped whole
    // groups the packed list is the pruned one, even if its length matches.
    if (packedParts.length == parts.length && dropLocations.isEmpty) parts else packedParts
  }

  /**
   * `schema` with Spark's temporary row-index column last: the file's row positions, generated by our reader
   * (NativeParquetRowIndexColumn) and by Spark's fallback reader alike, for applying position deletes.
   */
  def withPositions(schema: StructType): StructType =
    schema.add(
      org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME,
      org.apache.spark.sql.types.LongType,
      nullable = false
    )
}

/** Plan-time checks for [[VectorIcebergScanExec]]. */
object VectorIcebergScanPlanner {

  /** The field ids of the scan's output columns when it can be read natively, else why not. */
  def plan(b: BatchScanExec, conf: SQLConf): Either[String, VectorIcebergScanExec] = {
    if (!b.scan.getClass.getName.startsWith("org.apache.iceberg.spark.source.")) return Left("not an Iceberg scan")
    if (b.outputOrdering.nonEmpty) return Left("Iceberg scan reports an ordering")
    val ids = IcebergTasks.outputFieldIds(b) match {
      case Left(reason) => return Left(reason)
      case Right(v) => v
    }
    // #nested: lists, maps and structs at any depth, as our Parquet reader reads them. The three v3 metadata
    // columns (#20 M3a) are not read from a file -- `_partition` is a per-file struct constant (empty for an
    // unpartitioned table), `_row_id` / `_last_updated_sequence_number` are longs filled per batch -- so their
    // types are not gated by the native reader's `isReadable`.
    b.output.zip(ids).foreach { case (a, id) =>
      if (
        IcebergTasks.columnKind(a.name, id) == IcebergTasks.DataColumn &&
        !io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(a.dataType)
      ) {
        return Left(s"unsupported column type ${a.name}:${a.dataType.simpleString}")
      }
    }
    // Iceberg's reader is not vectorized for some scans (nested columns). A task group our reader cannot read
    // natively (an ORC/Avro file, an encrypted file, a delete we cannot apply) is read by Iceberg's own ROW reader
    // inside the node and converted to batches (#861 / #901 item 6), so the other groups stay native. This is
    // admitted ONLY for a data-file scan every one of whose task groups is an Iceberg `FileScanTask` group -- never
    // a metadata table (`.files`, `.snapshots`, whose partitions are not `SparkInputPartition`/`FileScanTask`),
    // whose rows our reader cannot shape. A scan where NO group is native gains nothing and keeps Spark's scan.
    if (!b.supportsColumnar) {
      val kinds = b.output.zip(ids).map { case (a, id) => IcebergTasks.columnKind(a.name, id) }
      val constants = kinds.exists(k =>
        k == IcebergTasks.FileColumn || k == IcebergTasks.SpecIdColumn || k == IcebergTasks.PartitionColumn ||
          k == IcebergTasks.RowIdColumn || k == IcebergTasks.SeqNumColumn
      )
      val parts = b.inputPartitions
      val projected = b.output.zip(ids).zip(kinds).collect { case ((_, id), IcebergTasks.DataColumn) => id }.toSet
      val equality = IcebergTasks.equalityColumnsOf(parts, IcebergTasks.tableSchemas(b), projected).isDefined
      // An empty partition list -- every file pruned in Iceberg's planning (e.g. a selective uuid `=`) -- reads
      // nothing and is admitted: there is no metadata-table group to reject and no row group to fall back on.
      if (parts.nonEmpty) {
        if (!IcebergTasks.allDataFileGroups(parts)) {
          return Left("Iceberg scan is not vectorized and is not a data-file scan (metadata table)")
        }
        val native = parts.count(p => IcebergTasks.nativeTasks(p, equality, constants).isDefined)
        if (native == 0) {
          return Left("Iceberg scan is not vectorized and no task group reads natively")
        }
        // Some groups native, the rest read by Iceberg's row reader inside the node: admit it (item 6). When every
        // group is native (native == parts.length) nothing falls to Iceberg's reader, the pre-item-6 case.
      }
    }
    Right(VectorIcebergScanExec(b, ids))
  }
}

/**
 * Iceberg's scan and task objects, reached reflectively: the plugin has no compile-time Iceberg dependency. Only
 * public Iceberg API methods are called (some on package-private classes, hence `setAccessible`).
 */
private[vecruntime] object IcebergTasks {

  // Iceberg types whose Spark type and Parquet encoding our reader decodes as Iceberg's reader does. uuid (16
  // bytes in the file, a string in Spark) and fixed (a FIXED_LEN_BYTE_ARRAY, binary in Spark) are read natively
  // too (#rtypes); the v3 unknown type (NullType in Spark) is produced as a null column, see unsupportedType. Not:
  // time, timestamp_ns, geometry. A variant column is read natively too (see unsupportedType), but is never an
  // equality column.
  private val NativeTypes =
    Set(
      "BOOLEAN",
      "INTEGER",
      "LONG",
      "FLOAT",
      "DOUBLE",
      "DATE",
      "TIMESTAMP",
      "STRING",
      "BINARY",
      "DECIMAL",
      "FIXED",
      "UUID"
    )

  private def call(target: AnyRef, name: String): AnyRef = {
    var c: Class[_] = target.getClass
    while (c != null) {
      c.getDeclaredMethods.find(m => m.getName == name && m.getParameterCount == 0) match {
        case Some(m) =>
          m.setAccessible(true)
          return m.invoke(target)
        case None => c = c.getSuperclass
      }
    }
    // Interface default or public methods.
    val m = target.getClass.getMethod(name)
    m.setAccessible(true)
    m.invoke(target)
  }

  /** What an output column of the scan is: a data column read from the file, or an Iceberg metadata column. */
  sealed trait ColumnKind extends Serializable
  case object DataColumn extends ColumnKind
  case object FileColumn extends ColumnKind // _file: the data file's location
  case object PosColumn extends ColumnKind // _pos: the row's position in its file
  case object SpecIdColumn extends ColumnKind // _spec_id: the data file's partition spec id
  // _partition: the struct of the table's unified partition type, a per-file constant (#20 M3a).
  case object PartitionColumn extends ColumnKind
  // _row_id: the file's own `_row_id` column where it materialises it (copy-on-write, compaction), else
  // `firstRowId + _pos` (null when `firstRowId` is null) -- see `metadataLong`.
  case object RowIdColumn extends ColumnKind
  // _last_updated_sequence_number: the file's own column where it materialises it, else the data file's
  // `dataSequenceNumber`.
  case object SeqNumColumn extends ColumnKind
  // #901 item 3: _deleted (MetadataColumns.IS_DELETED), a boolean the scan computes -- false for a live row, true
  // for a row a position delete, deletion vector or equality delete marks, which is kept instead of dropped.
  case object DeletedColumn extends ColumnKind

  // Iceberg's reserved metadata column ids (MetadataColumns): every metadata column sits near Int.MaxValue.
  private val FileId = Int.MaxValue - 1
  private val PosId = Int.MaxValue - 2
  private val SpecIdId = Int.MaxValue - 4
  // #20 M3a: the three v3 merge-on-read metadata columns (MetadataColumns.PARTITION_COLUMN_ID, ROW_ID and
  // LAST_UPDATED_SEQUENCE_NUMBER). All three sit above MetadataIdFloor but are supported natively.
  private val PartitionId = Int.MaxValue - 5 // 2147483642
  private val RowIdId = Int.MaxValue - 107 // 2147483540
  private val SeqNumId = Int.MaxValue - 108 // 2147483539
  // #901 item 3: _deleted (MetadataColumns.IS_DELETED), a boolean computed by the scan.
  private val DeletedId = Int.MaxValue - 3 // 2147483644
  private val MetadataIdFloor = Int.MaxValue - 10000

  def columnKind(name: String, id: Int): ColumnKind =
    if (id == FileId && name == "_file") FileColumn
    else if (id == PosId && name == "_pos") PosColumn
    else if (id == SpecIdId && name == "_spec_id") SpecIdColumn
    else if (id == PartitionId && name == "_partition") PartitionColumn
    else if (id == RowIdId && name == "_row_id") RowIdColumn
    else if (id == SeqNumId && name == "_last_updated_sequence_number") SeqNumColumn
    else if (id == DeletedId && name == "_deleted") DeletedColumn
    else DataColumn

  /** The per-file constants a split carries as its partition values, in this order: `_file`, `_spec_id`. */
  val ConstantSchema: StructType = new StructType()
    .add("_file", org.apache.spark.sql.types.StringType, nullable = false)
    .add("_spec_id", org.apache.spark.sql.types.IntegerType, nullable = false)

  /**
   * The Iceberg field ids of `b`'s output, in order (Iceberg's reserved ids for the metadata columns `_file`,
   * `_pos` and `_spec_id`), or why the scan cannot be read natively.
   */
  def outputFieldIds(b: BatchScanExec): Either[String, Seq[Int]] =
    try {
      // #849: a name mapping (files written without field ids) is applied by our reader, see nameMapping.
      val schema = call(b.scan, "projection")
      val columns = call(schema, "columns").asInstanceOf[java.util.List[AnyRef]].asScala
      val byName = columns.map(f => call(f, "name").asInstanceOf[String].toLowerCase -> f).toMap
      Right(b.output.map { a =>
        a.name match {
          case "_file" => FileId
          case "_pos" => PosId
          case "_spec_id" => SpecIdId
          // #20 M3a: the three v3 merge-on-read metadata columns are read natively (constants or the file's own
          // column), see nativeTasks / metadataLong. Every other metadata column keeps Iceberg's reader.
          case "_partition" => PartitionId
          case "_row_id" => RowIdId
          case "_last_updated_sequence_number" => SeqNumId
          // #901 item 3: _deleted is computed by the scan (false for live rows, true for marked rows).
          case "_deleted" => DeletedId
          case _ =>
            val field =
              byName.getOrElse(a.name.toLowerCase, return Left(s"column ${a.name} not in the Iceberg projection"))
            val id = call(field, "fieldId").asInstanceOf[Integer].intValue()
            // Other metadata columns (row lineage aside, handled above) keep Iceberg's reader.
            if (id >= MetadataIdFloor) return Left(s"Iceberg metadata column ${a.name}")
            // A v3 initial default fills the column for files written before it was added; our reader reads nulls.
            // A v3 initial default fills the column for files written before it was added: read as an existence
            // default (a constant of the column's type) when it can be written as one, else Iceberg's reader.
            if (call(field, "initialDefault") != null && existenceDefaultSql(field, a.dataType).isEmpty) {
              return Left(s"Iceberg column ${a.name} has an initial default")
            }
            unsupportedType(call(field, "type"), a.name).foreach(reason => return Left(reason))
            id
        }
      })
    } catch {
      case e: ReflectiveOperationException => Left(s"Iceberg scan not recognised: $e")
    }

  /**
   * A top-level Iceberg column's v3 initial default as Spark's existence default (the SQL text Spark keeps under
   * `EXISTS_DEFAULT`), which our reader fills into the rows of a file written before the column was added. None
   * when the column has no initial default, or one of a type our reader cannot fill as a constant (only a lane
   * type, a string or a binary; see `missingReadable`).
   */
  def existenceDefaultSql(field: AnyRef, dt: org.apache.spark.sql.types.DataType): Option[String] = {
    import org.apache.spark.sql.types._
    val v = call(field, "initialDefault")
    if (v == null) return None
    def quoted(x: String) = "'" + x.replace("\\", "\\\\").replace("'", "\\'") + "'"
    val sql: Option[String] = (dt, v) match {
      case (BooleanType, b: java.lang.Boolean) => Some(b.toString)
      case (IntegerType, i: java.lang.Integer) => Some(s"CAST(${i.intValue()} AS INT)")
      case (LongType, l: java.lang.Long) => Some(s"CAST(${l.longValue()} AS BIGINT)")
      case (FloatType, f: java.lang.Float) => Some(s"CAST(${quoted(f.toString)} AS FLOAT)")
      case (DoubleType, d: java.lang.Double) => Some(s"CAST(${quoted(d.toString)} AS DOUBLE)")
      case (d: DecimalType, b: java.math.BigDecimal) =>
        Some(s"CAST(${quoted(b.toPlainString)} AS DECIMAL(${d.precision}, ${d.scale}))")
      case (StringType, cs: CharSequence) => Some(quoted(cs.toString))
      case (DateType, days: java.lang.Integer) =>
        Some(s"DATE ${quoted(java.time.LocalDate.ofEpochDay(days.longValue()).toString)}")
      case (TimestampType, micros: java.lang.Long) => Some(s"timestamp_micros(${micros.longValue()})")
      case (TimestampNTZType, micros: java.lang.Long) =>
        val t = java.time.LocalDateTime.ofEpochSecond(
          Math.floorDiv(micros.longValue(), 1000000L),
          (Math.floorMod(micros.longValue(), 1000000L) * 1000).toInt,
          java.time.ZoneOffset.UTC
        )
        Some(s"TIMESTAMP_NTZ ${quoted(t.toString.replace('T', ' '))}")
      case (BinaryType, bb: java.nio.ByteBuffer) =>
        val bytes = new Array[Byte](bb.remaining()); bb.duplicate().get(bytes)
        Some("X'" + bytes.map(b => f"${b & 0xff}%02X").mkString + "'")
      case _ => None
    }
    // Only a type our reader fills as a constant (an engine lane, a string or a binary).
    sql.filter { _ =>
      dt match {
        case StringType | BinaryType => true
        case a: AtomicType => io.vecruntime.spark.adapter.TypeMapping.vecTypeOf(a) != null
        case _ => false
      }
    }
  }

  /**
   * #901 item 4: a nested Iceberg field's initial default as an existence default (the SQL text our reader fills
   * into a file written before the field was added), or None when the field has no default or one of a type our
   * reader cannot fill as a constant. Only an atomic field carries a fillable default; a default on a struct, list
   * or map itself is not filled here (and Iceberg does not allow one).
   */
  private def nestedDefaultSql(field: AnyRef): Option[String] = {
    if (call(field, "initialDefault") == null) return None
    val icebergType = call(field, "type")
    if (!NativeTypes.contains(call(icebergType, "typeId").toString)) return None
    val cl = icebergType.getClass.getClassLoader
    val typeClass = Class.forName("org.apache.iceberg.types.Type", false, cl)
    val dt = Class
      .forName("org.apache.iceberg.spark.SparkSchemaUtil", false, cl)
      .getMethod("convert", typeClass)
      .invoke(null, icebergType)
      .asInstanceOf[org.apache.spark.sql.types.DataType]
    existenceDefaultSql(field, dt)
  }

  /**
   * Why an Iceberg column type is not read natively, or None: a flat type of [[NativeTypes]], or (#nested) a struct,
   * list or map whose every leaf is one, at any depth. A nested field with an initial default (#901 item 4) is read
   * natively when our reader can fill that default as a constant -- as it fills a top-level default (see
   * [[existenceDefaultSql]]) -- and a file missing the field returns the default, not null, as Iceberg's reader
   * does; a default of a type our reader cannot fill as a constant still keeps Iceberg's reader.
   */
  private def unsupportedType(icebergType: AnyRef, path: String): Option[String] =
    call(icebergType, "typeId").toString match {
      case "STRUCT" =>
        call(icebergType, "fields").asInstanceOf[java.util.List[AnyRef]].asScala.iterator.map { f =>
          val name = s"$path.${call(f, "name")}"
          if (call(f, "initialDefault") != null && nestedDefaultSql(f).isEmpty)
            Some(s"Iceberg field $name has an initial default our reader cannot fill as a constant")
          else unsupportedType(call(f, "type"), name)
        }.collectFirst { case Some(r) => r }
      case "LIST" => unsupportedType(call(icebergType, "elementType"), s"$path.element")
      case "MAP" =>
        unsupportedType(call(icebergType, "keyType"), s"$path.key")
          .orElse(unsupportedType(call(icebergType, "valueType"), s"$path.value"))
      case t if NativeTypes.contains(t) => None
      // #rtypes: the v3 unknown type is always null, never stored in a file; produced as a null column.
      case "UNKNOWN" => None
      // A v3 variant, shredded or not: Iceberg writes the spec's Parquet variant group (metadata, value and the
      // shredded typed_value), which our reader reads as it reads Spark's (#787, #812).
      case "VARIANT" => None
      case t => Some(s"Iceberg type $t of $path")
    }

  /**
   * `dt`, the Spark type of a column whose Iceberg type is `icebergType`, with every nested struct field carrying
   * its Iceberg field id as its Parquet field id, so the reader matches nested fields by id as it matches the
   * top-level columns: a renamed nested field reads as Iceberg reads it. Struct fields are paired by name (the
   * Spark type is Iceberg's projection converted); lists and maps are matched by their structure.
   */
  def withFieldIds(dt: org.apache.spark.sql.types.DataType, icebergType: AnyRef): org.apache.spark.sql.types.DataType =
    dt match {
      case st: StructType =>
        val fields = call(icebergType, "fields").asInstanceOf[java.util.List[AnyRef]].asScala
        val byName = fields.map(f => call(f, "name").asInstanceOf[String].toLowerCase -> f).toMap
        StructType(st.fields.map { f =>
          byName.get(f.name.toLowerCase) match {
            case Some(nf) =>
              val id = call(nf, "fieldId").asInstanceOf[Integer].longValue()
              val md = new MetadataBuilder().withMetadata(f.metadata)
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id)
              // #901 item 4: a nested field with a fillable initial default carries it as an existence default, so
              // the struct reader fills a file missing the field with the default rather than null.
              nestedDefaultSql(nf).foreach { sql =>
                md.putString(
                  org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY,
                  sql
                )
              }
              f.copy(dataType = withFieldIds(f.dataType, call(nf, "type")), metadata = md.build())
            case None => f
          }
        })
      case at: org.apache.spark.sql.types.ArrayType =>
        at.copy(elementType = withFieldIds(at.elementType, call(icebergType, "elementType")))
      case mt: org.apache.spark.sql.types.MapType =>
        mt.copy(
          keyType = withFieldIds(mt.keyType, call(icebergType, "keyType")),
          valueType = withFieldIds(mt.valueType, call(icebergType, "valueType"))
        )
      case other => other
    }

  /** The projected field `id`'s initial default as an existence default (see [[existenceDefaultSql]]). */
  def initialDefaultSql(b: BatchScanExec, id: Int, dt: org.apache.spark.sql.types.DataType): Option[String] =
    if (dt.isInstanceOf[AtomicType]) existenceDefaultSql(projectedField(b, id), dt) else None

  private def projectedField(b: BatchScanExec, id: Int): AnyRef = {
    val projection = call(b.scan, "projection")
    projection.getClass.getMethod("findField", classOf[Int]).invoke(projection, Int.box(id))
  }

  /** The Iceberg type of the projected field `id` of `b`'s scan. */
  def projectedType(b: BatchScanExec, id: Int): AnyRef = call(projectedField(b, id), "type")

  /** #rtypes: whether the projected field `id` of `b`'s scan is a uuid column (kept out of pushdown). */
  def isUuid(b: BatchScanExec, id: Int): Boolean = call(projectedType(b, id), "typeId").toString == "UUID"

  /**
   * One file scan task read natively: its split, the data file's location, its position delete files (and deletion
   * vector) and its equality delete files (those Iceberg's planning found apply to it, by sequence number). `task`
   * is Iceberg's `FileScanTask`, kept so [[fileMetadata]] can read the file's partition and v3 lineage constants.
   */
  final case class NativeTask(
      file: PartitionedFile,
      location: String,
      deletes: java.util.List[AnyRef],
      equalityDeletes: java.util.List[AnyRef],
      task: AnyRef
  )

  /**
   * The group's tasks as file splits when every task is an unencrypted Parquet data file whose delete files (if
   * any) are unencrypted position deletes, deletion vectors or -- when `equality` (every equality column is
   * readable natively) -- equality deletes; None when Iceberg must read it (another format, encryption, an equality
   * column our reader cannot decode). (Scan tasks carry no column statistics, so a projected column a file lacks
   * is found at open: the reader reads an optional missing column as nulls, as Iceberg does without an initial
   * default.)
   */
  def nativeTasks(p: InputPartition, equality: Boolean, constants: Boolean): Option[Array[NativeTask]] = {
    if (p.getClass.getName != "org.apache.iceberg.spark.source.SparkInputPartition") return None
    val tasks = call(call(p, "taskGroup"), "tasks").asInstanceOf[java.util.Collection[AnyRef]].asScala
    val out = tasks.map { task =>
      if (!task.getClass.getName.contains("FileScanTask")) return None
      val deletes = call(task, "deletes").asInstanceOf[java.util.List[AnyRef]]
      val positionDeletes = new java.util.ArrayList[AnyRef]()
      val equalityDeletes = new java.util.ArrayList[AnyRef]()
      if (deletes != null) {
        deletes.asScala.foreach { d =>
          // An encrypted delete file needs Iceberg's decryption.
          if (call(d, "keyMetadata") != null) return None
          call(d, "content").toString match {
            case "POSITION_DELETES" => positionDeletes.add(d)
            case "EQUALITY_DELETES" if equality => equalityDeletes.add(d)
            case _ => return None
          }
        }
      }
      val file = call(task, "file")
      if (call(file, "format").toString != "PARQUET") return None
      // An encrypted file is decrypted by Iceberg's FileIO (bulkDecrypt); our reader would read its bytes raw.
      if (call(file, "keyMetadata") != null) return None
      val location = call(file, "location").toString
      val size = call(file, "fileSizeInBytes").asInstanceOf[java.lang.Long].longValue()
      NativeTask(
        PartitionedFile(
          if (!constants) InternalRow.empty
          else InternalRow(
            org.apache.spark.unsafe.types.UTF8String.fromString(location),
            call(file, "specId").asInstanceOf[Integer].intValue()
          ),
          SparkPath.fromPath(new Path(location)),
          call(task, "start").asInstanceOf[java.lang.Long].longValue(),
          call(task, "length").asInstanceOf[java.lang.Long].longValue(),
          Array.empty[String],
          0L,
          size
        ),
        location,
        positionDeletes,
        equalityDeletes,
        task
      )
    }
    Some(out.toArray)
  }

  /**
   * #901 item 6: whether every input partition is an Iceberg `SparkInputPartition` whose task group holds only
   * `FileScanTask`s -- i.e. a data-file scan, not a metadata table (`.files`, `.snapshots`, `.manifests`, ...)
   * whose partitions and tasks are different shapes. Only such a scan may admit the row-reader fallback, since the
   * node reshapes the row reader's rows into the scan's output and a metadata table's rows are not data rows.
   */
  def allDataFileGroups(parts: Seq[InputPartition]): Boolean =
    parts.nonEmpty && parts.forall { p =>
      p.getClass.getName == "org.apache.iceberg.spark.source.SparkInputPartition" && {
        val tasks = call(call(p, "taskGroup"), "tasks").asInstanceOf[java.util.Collection[AnyRef]].asScala
        tasks.nonEmpty && tasks.forall(_.getClass.getName.contains("FileScanTask"))
      }
    }

  /**
   * #iceberg-file-pruning: the data-file LOCATIONS that `runtimeFilters` prove cannot match, so the files are
   * never opened. The runtime filters (a broadcast hash join's build-key min/max ranges and IN sets, #610) are
   * built directly into ONE Iceberg `Expression` (an IN set, a range `And(>=, <=)`, equality, null checks; any
   * other shape skipped, so pruning stays conservative). Iceberg's Spark scan strips per-file column bounds from
   * its task `DataFile`s, so they cannot be evaluated directly; instead a FRESH table scan is planned on the
   * driver on the SAME snapshot, with the scan's residual AND the runtime expression. Iceberg's manifest reader
   * applies its own `InclusiveMetricsEvaluator` to each entry while planning (re-reading the snapshot's manifests;
   * no data file is opened), yielding exactly the files that can still match -- the KEEP set.
   * Every task file NOT in the KEEP set is dropped. A file under metrics mode `none` (no bounds) stays in the
   * KEEP set, as the evaluator keeps it; truncated string bounds are handled by the evaluator itself.
   *
   * `None` when nothing is pruned -- no runtime filter converted, the snapshot has no files, or the Iceberg
   * classes are not on the classpath -- so the caller leaves the task list untouched. Dropping a data file is
   * always safe for its delete files (position deletes, deletion vectors and equality deletes only REMOVE rows,
   * so a file with no matching row has no matching row after deletes either) and for v3 row lineage (`_row_id` /
   * `_last_updated_sequence_number` are per-file, so dropping a file drops only its own rows). The re-plan is
   * driver-side and reads the snapshot's manifests again -- it opens no data file.
   */
  def runtimeFilePruneDropSet(
      scan: BatchScanExec,
      runtimeFilters: Seq[org.apache.spark.sql.sources.Filter],
      caseSensitive: Boolean
  ): Option[Set[String]] = {
    if (runtimeFilters.isEmpty) return None
    try {
      // The data-file locations our plan will read, and the classloader that reaches Iceberg's classes.
      val taskLocations = scala.collection.mutable.HashSet.empty[String]
      var loader: ClassLoader = null
      scan.inputRDD.partitions.foreach { part =>
        shims.SparkShims.dataSourceInputPartitions(part).foreach { p =>
          if (p.getClass.getName == "org.apache.iceberg.spark.source.SparkInputPartition") {
            call(call(p, "taskGroup"), "tasks").asInstanceOf[java.util.Collection[AnyRef]].asScala.foreach { task =>
              if (task.getClass.getName.contains("FileScanTask")) {
                if (loader == null) loader = task.getClass.getClassLoader
                taskLocations += call(call(task, "file"), "location").toString
              }
            }
          }
        }
      }
      if (taskLocations.isEmpty || loader == null) return None
      val cl = loader
      val exprClass = Class.forName("org.apache.iceberg.expressions.Expression", false, cl)
      val expressionsClass = Class.forName("org.apache.iceberg.expressions.Expressions", false, cl)
      // The #610 runtime filters are v1 `org.apache.spark.sql.sources.Filter`s (Iceberg 1.12 dropped the v1
      // `SparkFilters` converter). Build the Iceberg `Expression` DIRECTLY from the shapes #610 produces -- an IN
      // set, a range `And(>=lo, <=hi)`, equality, null checks -- skipping any other shape (so pruning stays
      // conservative: an unconvertible filter simply prunes nothing).
      val andM = expressionsClass.getMethod("and", exprClass, exprClass)
      val inM = expressionsClass.getMethod("in", classOf[String], classOf[java.lang.Iterable[_]])
      val equalM = expressionsClass.getMethod("equal", classOf[String], classOf[Object])
      val geM = expressionsClass.getMethod("greaterThanOrEqual", classOf[String], classOf[Object])
      val leM = expressionsClass.getMethod("lessThanOrEqual", classOf[String], classOf[Object])
      val notNullM = expressionsClass.getMethod("notNull", classOf[String])
      def lit(v: Any): AnyRef = v match {
        case us: org.apache.spark.unsafe.types.UTF8String => us.toString
        case other => other.asInstanceOf[AnyRef]
      }
      import org.apache.spark.sql.sources._
      def toIceberg(f: Filter): Option[AnyRef] = f match {
        case In(a, vs) if vs != null && vs.nonEmpty => Some(inM.invoke(null, a, vs.toSeq.map(lit).asJava))
        case EqualTo(a, v) if v != null => Some(equalM.invoke(null, a, lit(v)))
        case GreaterThan(a, v) if v != null => Some(geM.invoke(null, a, lit(v))) // widen to >= (sound, keeps more)
        case GreaterThanOrEqual(a, v) if v != null => Some(geM.invoke(null, a, lit(v)))
        case LessThan(a, v) if v != null => Some(leM.invoke(null, a, lit(v))) // widen to <=
        case LessThanOrEqual(a, v) if v != null => Some(leM.invoke(null, a, lit(v)))
        case IsNotNull(a) => Some(notNullM.invoke(null, a))
        case And(l, r) => for (x <- toIceberg(l); y <- toIceberg(r)) yield andM.invoke(null, x, y)
        case _ => None
      }
      val exprs = runtimeFilters.flatMap(toIceberg)
      if (exprs.isEmpty) return None
      val runtimeExpr = exprs.reduceLeft((a, b) => andM.invoke(null, a, b))

      // Iceberg's Spark scan STRIPS per-file column bounds from its task `DataFile`s (they keep only the record
      // count), so `task.file()` cannot be evaluated directly. Re-plan on the driver instead: a fresh table scan
      // on the SAME snapshot, with the scan's residual AND the runtime expression. Iceberg's manifest reader
      // applies its own `InclusiveMetricsEvaluator` to each entry's bounds while planning (the returned tasks need
      // no stats, so none are kept) and yields exactly the files that can still match. This opens no data file;
      // it reads the snapshot's manifests again. The KEEP set is those files; our task files NOT in it are pruned.
      val sparkScan = scan.scan // Iceberg's SparkBatchQueryScan
      val table = call(sparkScan, "table")
      val residual = call(sparkScan, "filter") // the scan's already-applied residual expression
      val fullExpr = if (residual == null) runtimeExpr else andM.invoke(null, residual, runtimeExpr)
      val tableScan0 = table.getClass.getMethod("newScan").invoke(table)
      val tableScanClass = Class.forName("org.apache.iceberg.TableScan", false, cl)
      val snapshotId = call(sparkScan, "snapshotId").asInstanceOf[java.lang.Long]
      val onSnapshot =
        if (snapshotId == null) tableScan0
        else tableScanClass.getMethod("useSnapshot", java.lang.Long.TYPE).invoke(tableScan0, snapshotId)
      val scanClass = Class.forName("org.apache.iceberg.Scan", false, cl)
      val caseSens =
        scanClass.getMethod("caseSensitive", java.lang.Boolean.TYPE)
          .invoke(onSnapshot, java.lang.Boolean.valueOf(caseSensitive))
      val filtered = scanClass.getMethod("filter", exprClass).invoke(caseSens, fullExpr)
      val plan = scanClass.getMethod("planFiles").invoke(filtered)
      val keep = scala.collection.mutable.HashSet.empty[String]
      val closeable = plan.asInstanceOf[java.lang.Iterable[AnyRef]]
      val it = closeable.iterator()
      try {
        while (it.hasNext) {
          val task = it.next()
          if (task.getClass.getName.contains("FileScanTask"))
            keep += call(call(task, "file"), "location").toString
        }
      } finally plan match {
          case c: java.io.Closeable => c.close()
          case _ =>
        }
      // Drop any file our plan holds that the re-plan with the runtime filter did NOT keep.
      val drop = taskLocations.diff(keep)
      if (drop.isEmpty) None else Some(drop.toSet)
    } catch {
      // Any Iceberg-API mismatch, a non-convertible shape, or the classes being absent: prune nothing (correct,
      // just not optimised). Never let pruning change results or fail the scan.
      case _: ReflectiveOperationException | _: ClassNotFoundException | _: NoClassDefFoundError => None
    }
  }

  /** The data-file location of one native `FileScanTask` (for pruning), via reflection. */
  def taskFileLocation(task: AnyRef): String = call(call(task, "file"), "location").toString

  /**
   * #20 M3a: the per-file constants of the three v3 merge-on-read metadata columns, for the data file of one
   * native task: the `_partition` struct as a Spark `InternalRow` (Iceberg's `PartitionUtil.constantsMap`
   * converted with `SparkUtil.internalToSpark`, keyed by `PARTITION_COLUMN_ID`), the file's `firstRowId` (null
   * when it has none) and its `dataSequenceNumber`. A file that materialises `_row_id` /
   * `_last_updated_sequence_number` (copy-on-write, compaction) is read from the file instead (see the scan's
   * `requiredSchema`); these are the fallbacks for the files that do not.
   */
  final case class FileMetadata(partition: InternalRow, firstRowId: java.lang.Long, seqNum: java.lang.Long)

  /** The table's unified partition type (`Partitioning.partitionType`), an `org.apache.iceberg.types.Type`. */
  def partitionType(b: BatchScanExec): AnyRef = {
    val table = call(b.scan, "table")
    val cl = table.getClass.getClassLoader
    val partitioning = Class.forName("org.apache.iceberg.Partitioning", false, cl)
    val tableClass = Class.forName("org.apache.iceberg.Table", false, cl)
    partitioning.getMethod("partitionType", tableClass).invoke(null, table)
  }

  def fileMetadata(task: AnyRef, partitionType: AnyRef): FileMetadata = {
    val cl = task.getClass.getClassLoader
    val sparkUtil = Class.forName("org.apache.iceberg.spark.SparkUtil", false, cl)
    val typeClass = Class.forName("org.apache.iceberg.types.Type", false, cl)
    val internalToSpark = sparkUtil.getMethod("internalToSpark", typeClass, classOf[Object])
    val convert: java.util.function.BiFunction[AnyRef, AnyRef, AnyRef] =
      (t, v) => internalToSpark.invoke(null, t, v)
    val util = Class.forName("org.apache.iceberg.util.PartitionUtil", false, cl)
    val contentScanTask = Class.forName("org.apache.iceberg.ContentScanTask", false, cl)
    val structTypeClass = Class.forName("org.apache.iceberg.types.Types$StructType", false, cl)
    val constantsMap = util.getMethod(
      "constantsMap",
      contentScanTask,
      structTypeClass,
      classOf[java.util.function.BiFunction[_, _, _]]
    )
    val constants =
      constantsMap.invoke(null, task, partitionType, convert).asInstanceOf[java.util.Map[Integer, AnyRef]]
    val partition = constants.get(Int.box(PartitionId)) match {
      case null => InternalRow.empty // an unpartitioned table's empty struct
      case row: InternalRow => row
      case other => other.asInstanceOf[InternalRow]
    }
    val file = call(task, "file")
    FileMetadata(
      partition,
      call(file, "firstRowId").asInstanceOf[java.lang.Long],
      call(file, "dataSequenceNumber")
        .asInstanceOf[java.lang.Long]
    )
  }

  /**
   * The bytes Spark would weigh a task group by: its files' bytes (Iceberg's `sizeBytes`) plus `openCost` per file,
   * or -1 when `p` is not an Iceberg input partition.
   */
  def groupBytes(p: InputPartition, openCost: Long): Long =
    if (p.getClass.getName != "org.apache.iceberg.spark.source.SparkInputPartition") -1L
    else {
      val group = call(p, "taskGroup")
      val bytes = call(group, "sizeBytes").asInstanceOf[java.lang.Long].longValue()
      val files = call(group, "filesCount").asInstanceOf[java.lang.Integer].intValue()
      bytes + files.toLong * openCost
    }

  /** A long table property of the scanned table, or `default` when unset or unparsable. */
  private def tableLongProperty(scan: BatchScanExec, key: String, default: Long): Long = {
    val props = call(call(scan.scan, "table"), "properties").asInstanceOf[java.util.Map[String, String]]
    Option(props.get(key)).flatMap(v => scala.util.Try(v.trim.toLong).toOption).getOrElse(default)
  }

  /**
   * The Iceberg scan's own `SparkReadConf` (`org.apache.iceberg.spark.source.SparkScan`'s private `readConf`
   * field), which resolves the split settings as Iceberg does -- a per-scan READ OPTION
   * (`spark.read().option("split-size"|"file-open-cost"|"lookback", ...)`, as the bin-pack rewrite runner sets
   * them) overriding the table property, then the table property, then Iceberg's default. None when the scan is
   * not a `SparkScan` or has no such field (an older Iceberg), where the caller falls back to the table property.
   */
  private def readConf(scan: BatchScanExec): Option[AnyRef] = {
    var c: Class[_] = scan.scan.getClass
    while (c != null) {
      try {
        val f = c.getDeclaredField("readConf")
        f.setAccessible(true)
        return Option(f.get(scan.scan))
      } catch { case _: NoSuchFieldException => c = c.getSuperclass }
    }
    None
  }

  /** Call a no-arg numeric method on the scan's `readConf` (e.g. `splitSize`), None if it or the conf is absent. */
  private def readConfLong(scan: BatchScanExec, method: String): Option[Long] =
    readConf(scan).flatMap { rc =>
      try {
        val m = rc.getClass.getMethod(method)
        m.setAccessible(true)
        Some(m.invoke(rc).asInstanceOf[Number].longValue())
      } catch { case _: ReflectiveOperationException => None }
    }

  /**
   * Iceberg's own split target size for THIS scan (`SparkReadConf.splitSize`): a per-scan `split-size` read option
   * (as `SparkBinPackFileRewriteRunner` / `SparkRewriteFileRunner` set via `spark.read().option(...)`) overriding
   * the table's `read.split.target-size`, then Iceberg's 128 MB default. Our packing never merges task groups
   * beyond this, so it respects the same file-size contract Iceberg's own grouping used for this very scan --
   * reading only the table property would diverge from Iceberg when a read option is in force.
   */
  def splitSize(scan: BatchScanExec): Long =
    readConfLong(scan, "splitSize").getOrElse(tableLongProperty(scan, "read.split.target-size", 128L * 1024 * 1024))

  /**
   * Iceberg's own per-file open cost for THIS scan (`SparkReadConf.splitOpenFileCost`): a per-scan `file-open-cost`
   * read option overriding the table's `read.split.open-file-cost`, then Iceberg's 4 MB default. A scan that sets
   * this very high (one file per group, as the Iceberg copy-on-write skew tests and the bin-pack copy runner's
   * split-declining do) makes each group weigh more than the split size, so our packing keeps one partition per
   * group, as Iceberg planned.
   */
  def splitOpenFileCost(scan: BatchScanExec): Long =
    readConfLong(
      scan,
      "splitOpenFileCost"
    ).getOrElse(tableLongProperty(scan, "read.split.open-file-cost", 4L * 1024 * 1024))

  /**
   * #iceberg-file-pruning: as [[groupBytes]] but discounting files whose location is in `dropLocations` (pruned,
   * never opened), so a group packs at only the weight of the files that are actually read. Walks the group's
   * `FileScanTask`s to sum each surviving data file's size + `openCost`; `-1` when `p` is not an Iceberg input
   * partition. `0` when every file is pruned.
   */
  def groupBytes(p: InputPartition, openCost: Long, dropLocations: Set[String]): Long = {
    if (p.getClass.getName != "org.apache.iceberg.spark.source.SparkInputPartition") return -1L
    if (dropLocations.isEmpty) return groupBytes(p, openCost)
    val tasks = call(call(p, "taskGroup"), "tasks").asInstanceOf[java.util.Collection[AnyRef]].asScala
    var total = 0L
    tasks.foreach { task =>
      if (!task.getClass.getName.contains("FileScanTask")) return groupBytes(p, openCost) // not a pure data group
      val file = call(task, "file")
      if (!dropLocations.contains(call(file, "location").toString)) {
        total += call(task, "length").asInstanceOf[java.lang.Long].longValue() + openCost
      }
    }
    total
  }

  /** Iceberg's own `BaseDeleteLoader`, opening delete files through the partition's `FileIO`, and its class. */
  private def deleteLoader(p: InputPartition): (Class[_], AnyRef) = {
    val cl = p.getClass.getClassLoader
    val deleteFileClass = Class.forName("org.apache.iceberg.DeleteFile", false, cl)
    val newInputFile =
      Class.forName("org.apache.iceberg.io.FileIO", false, cl).getMethod("newInputFile", deleteFileClass)
    val fileIo = call(p, "io")
    val open: java.util.function.Function[AnyRef, AnyRef] = d => newInputFile.invoke(fileIo, d)
    val loaderClass = Class.forName("org.apache.iceberg.data.BaseDeleteLoader", false, cl)
    (loaderClass, loaderClass.getConstructor(classOf[java.util.function.Function[_, _]]).newInstance(open))
  }

  /** The scanned table's current schema (an `org.apache.iceberg.Schema`, serializable), the ids eq-deletes use. */
  /** #849: the table's `schema.name-mapping.default`, parsed; null when it has none. */
  def nameMapping(b: BatchScanExec): IcebergNameMapping = {
    val props = call(call(b.scan, "table"), "properties").asInstanceOf[java.util.Map[String, String]]
    Option(props.get("schema.name-mapping.default")).map(IcebergNameMapping.parse).orNull
  }

  /** #901 R1: the table's `schema.name-mapping.default` JSON; null when it has none. */
  def nameMappingJson(b: BatchScanExec): String =
    call(call(b.scan, "table"), "properties").asInstanceOf[java.util.Map[String, String]]
      .get("schema.name-mapping.default")

  def tableSchema(b: BatchScanExec): AnyRef = call(call(b.scan, "table"), "schema")

  /**
   * Every schema in the table's history (current plus superseded), newest first by schema id. An equality delete
   * can be keyed on a column that has since been DROPPED from the current schema: the data files written before
   * the drop still carry that field id, and the delete still applies to them, so the field id is resolved against
   * these historical schemas, not only the current one. Robust to an older Iceberg without `schemas()`.
   */
  def tableSchemas(b: BatchScanExec): Seq[AnyRef] = {
    val table = call(b.scan, "table")
    val current = call(table, "schema")
    val history =
      try {
        val map = call(table, "schemas").asInstanceOf[java.util.Map[Integer, AnyRef]]
        if (map == null) Seq.empty
        else map.entrySet().asScala.toSeq.sortBy(-_.getKey.intValue()).map(_.getValue)
      } catch { case _: ReflectiveOperationException => Seq.empty }
    // Current first, then the rest of the history, de-duplicated by identity.
    (current +: history).distinct
  }

  /** The first of `schemas` whose struct contains every id in `ids` (so TypeUtil.select resolves them), or null. */
  private def schemaContainingAll(schemas: Seq[AnyRef], ids: Seq[Int]): AnyRef = {
    val findField = schemas.head.getClass.getMethod("findField", classOf[Int])
    schemas.find(s => ids.forall(id => findField.invoke(s, Int.box(id)) != null)).orNull
  }

  private def equalityFieldIds(d: AnyRef): Seq[Int] =
    call(d, "equalityFieldIds").asInstanceOf[java.util.List[Integer]].asScala.map(_.intValue()).toSeq

  /**
   * The columns to read besides the projected ones (`projected`, field ids) to apply the equality deletes of the
   * planned task groups `parts`: one per equality field the query does not project, named after its id and
   * carrying it as its Parquet field id. None when one of them is not a top-level column of a type our reader
   * decodes as Iceberg does: the files with those deletes then keep Iceberg's reader.
   */
  def equalityColumns(parts: Array[Partition], schemas: Seq[AnyRef], projected: Set[Int]): Option[Seq[StructField]] =
    equalityColumnsOf(parts.toSeq.flatMap(shims.SparkShims.dataSourceInputPartitions), schemas, projected)

  /** As [[equalityColumns]], over the scan's input partitions themselves (at plan time). */
  def equalityColumnsOf(
      inputs: Seq[InputPartition],
      schemas: Seq[AnyRef],
      projected: Set[Int]
  ): Option[Seq[StructField]] = {
    val ids = scala.collection.mutable.LinkedHashSet.empty[Int]
    // The distinct equality-field-id-sets across the delete files, so a nested ancestor read differently by two
    // groups can be detected and declined (the union read column would not match a group's delete schema).
    val idSets = scala.collection.mutable.LinkedHashSet.empty[Seq[Int]]
    inputs.foreach { p =>
      if (p.getClass.getName == "org.apache.iceberg.spark.source.SparkInputPartition") {
        call(call(p, "taskGroup"), "tasks").asInstanceOf[java.util.Collection[AnyRef]].asScala.foreach { task =>
          if (task.getClass.getName.contains("FileScanTask")) {
            val deletes = call(task, "deletes").asInstanceOf[java.util.List[AnyRef]]
            if (deletes != null) deletes.asScala.foreach { d =>
              if (call(d, "content").toString == "EQUALITY_DELETES") {
                val fieldIds = equalityFieldIds(d)
                ids ++= fieldIds
                idSets += fieldIds
              }
            }
          }
        }
      }
    }
    // The id is resolved against the schema HISTORY, not only the current schema (`schemas.head`): an equality
    // delete can be keyed on a since-DROPPED column (top level or a nested struct member). The data files written
    // before the drop still carry that field id; a file written after reads null for it (our reader fills nulls),
    // which is exactly the null key Iceberg compares a null delete key against. #956's nested-key resolution is
    // run against whichever schema in the history still holds the id.
    val cl = schemas.head.getClass.getClassLoader
    val schemaClass = Class.forName("org.apache.iceberg.Schema", false, cl)
    val typeClass = Class.forName("org.apache.iceberg.types.Type", false, cl)
    val convert = Class.forName("org.apache.iceberg.spark.SparkSchemaUtil", false, cl).getMethod("convert", typeClass)
    val projectStruct =
      Class.forName(
        "org.apache.iceberg.types.TypeUtil",
        false,
        cl
      ).getMethod("project", schemaClass, classOf[java.util.Set[_]])
    val findColumnName = schemaClass.getMethod("findColumnName", classOf[Int])
    val findFieldByName = schemaClass.getMethod("findField", classOf[String])
    val findFieldById = schemaClass.getMethod("findField", classOf[Int])
    // The first schema in the history (current first) that holds `id`, with its struct and column name.
    def schemaFor(id: Int): Option[AnyRef] = schemas.find(s => findFieldById.invoke(s, Int.box(id)) != null)
    // Resolve `id`'s top-level ancestor id (itself when top level) via the dotted column name, in the schema that
    // holds it. None when no schema in the history has the id.
    def topAncestorId(id: Int): Option[Int] = schemaFor(id).flatMap { s =>
      val columnName = findColumnName.invoke(s, Int.box(id))
      if (columnName == null) None
      else {
        val top = columnName.asInstanceOf[String].split("\\.", 2)(0)
        val topField = findFieldByName.invoke(s, top)
        if (topField == null) None else Some(call(topField, "fieldId").asInstanceOf[Integer].intValue())
      }
    }
    // Group the wanted (unprojected) equality ids by their top-level ancestor, preserving first-seen order.
    val wanted = ids.toSeq.filterNot(projected)
    val byAncestor = scala.collection.mutable.LinkedHashMap.empty[Int, scala.collection.mutable.LinkedHashSet[Int]]
    wanted.foreach { id =>
      topAncestorId(id) match {
        case None => return None
        case Some(anc) =>
          // The ancestor column is read as a pruned extra only when the query does not already project it; when it
          // does, the row probe reads the leaf from the projected struct and reshapes it to the delete schema (see
          // equalityFilter), so no second column of the same field id is added (which would collide) (#901 item 5).
          if (!projected.contains(anc))
            byAncestor.getOrElseUpdate(anc, scala.collection.mutable.LinkedHashSet.empty) += id
      }
    }
    // #901 item 5: a nested ancestor read under two different equality-field-id-sets would need a union read column
    // that no single group's delete schema matches; decline it (Iceberg's reader keeps those files).
    byAncestor.foreach { case (ancId, leafIds) =>
      if (leafIds.size > 1) {
        val sets = idSets.filter(s => s.exists(id => topAncestorId(id).contains(ancId))).toSeq
        if (sets.distinct.size > 1) return None
      }
    }
    val extra = byAncestor.toSeq.sortBy(_._1).map { case (ancId, leafIds) =>
      // The schema that holds this ancestor AND all of its needed leaves -- the current one, or an older one when
      // a (nested) key leaf has been dropped. For a dropped NESTED leaf the ancestor struct may still exist in the
      // current schema while the leaf lives only in history, so resolve against the schema that has every id.
      val holder = Option(schemaContainingAll(schemas, (leafIds.toSeq :+ ancId).distinct)).getOrElse(return None)
      val struct = call(holder, "asStruct")
      val fieldById = struct.getClass.getMethod("field", classOf[Int])
      val ancField = fieldById.invoke(struct, Int.box(ancId))
      if (ancField == null) return None
      val ancType = call(ancField, "type")
      if (call(ancType, "typeId").toString == "STRUCT") {
        // #901 item 5: a nested equality key. Project the holding schema to the ancestor's needed leaves and read
        // the ancestor struct, every nested field carrying its Iceberg id. The native lane probe does not decode
        // nested keys, so these groups take the row probe (equalityFilter), which reads the leaf through the struct
        // as Iceberg's accessor does -- a null leaf under a null parent struct, and null on post-drop files.
        val projectedSchema = projectStruct.invoke(null, holder, leafIds.map(Int.box).asJava).asInstanceOf[AnyRef]
        val projectedTop = fieldById.invoke(call(projectedSchema, "asStruct"), Int.box(ancId))
        if (projectedTop == null) return None
        val projectedType = call(projectedTop, "type")
        val sparkType = convert.invoke(null, projectedType).asInstanceOf[org.apache.spark.sql.types.DataType]
        if (!io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(sparkType)) return None
        StructField(
          s"_vecruntime_eq_$ancId",
          withFieldIds(sparkType, projectedType),
          nullable = true,
          new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, ancId.toLong).build()
        )
      } else {
        // A top-level (flat) equality key: one leaf id equal to the ancestor id.
        if (leafIds.size != 1 || leafIds.head != ancId) return None
        if (!NativeTypes.contains(call(ancType, "typeId").toString)) return None
        val dataType = convert.invoke(null, ancType).asInstanceOf[org.apache.spark.sql.types.DataType]
        if (!io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(dataType)) return None
        StructField(
          s"_vecruntime_eq_$ancId",
          dataType,
          nullable = true,
          new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, ancId.toLong).build()
        )
      }
    }
    Some(extra)
  }

  /**
   * The equality deletes of one data file as a per-batch test: for a batch read with `schema` (whose columns carry
   * their field ids), whether its row `i` is deleted. Each group of delete files with the same equality fields is
   * loaded by Iceberg's own `BaseDeleteLoader` into a `StructLikeSet` (once per Spark task, `cache`), and a row is
   * deleted when its values of those fields, seen through Iceberg's `InternalRowWrapper` (so values compare as
   * Iceberg compares them, nulls included), are in a set -- as Iceberg's `DeleteFilter` applies them.
   */
  def equalityFilter(
      p: InputPartition,
      deletes: java.util.List[AnyRef],
      tableSchemas: Seq[AnyRef],
      schema: StructType,
      cache: scala.collection.mutable.HashMap[(Seq[String], Seq[Int]), AnyRef],
      native: Boolean = false
  ): ColumnarBatch => (Int => Boolean) = {
    val cl = p.getClass.getClassLoader
    val (loaderClass, loader) = deleteLoader(p)
    val schemaClass = Class.forName("org.apache.iceberg.Schema", false, cl)
    val select = Class
      .forName("org.apache.iceberg.types.TypeUtil", false, cl)
      .getMethod("select", schemaClass, classOf[java.util.Set[_]])
    val load = loaderClass.getMethod("loadEqualityDeletes", classOf[java.lang.Iterable[_]], schemaClass)
    val structTypeClass = Class.forName("org.apache.iceberg.types.Types$StructType", false, cl)
    val wrapperClass = Class.forName("org.apache.iceberg.spark.source.InternalRowWrapper", false, cl)
    val wrapperCtor = wrapperClass.getDeclaredConstructor(classOf[StructType], structTypeClass)
    wrapperCtor.setAccessible(true)
    val wrap = wrapperClass.getDeclaredMethod("wrap", classOf[InternalRow])
    wrap.setAccessible(true)
    val contains =
      Class.forName("org.apache.iceberg.util.StructLikeSet", false, cl).getMethod("contains", classOf[Object])
    val ordinalOf = schema.fields.zipWithIndex.collect {
      case (f, i) if f.metadata.contains(ParquetUtils.FIELD_ID_METADATA_KEY) =>
        f.metadata.getLong(ParquetUtils.FIELD_ID_METADATA_KEY).toInt -> i
    }.toMap
    // #rtypes: the native set reads the delete files directly (below), through the partition's own FileIO -- the
    // same handle Iceberg's loader opens them with. `newInputFile(DeleteFile)` turns each selected delete file
    // into an InputFile for the bridge's copying reader.
    val deleteFileClass = Class.forName("org.apache.iceberg.DeleteFile", false, cl)
    val fileIo = call(p, "io")
    val newInputFile =
      Class.forName("org.apache.iceberg.io.FileIO", false, cl).getMethod("newInputFile", deleteFileClass)
    // #901 item 5: the delete-schema-shaped Spark type is built from the delete schema (not sliced out of the read
    // schema), so the InternalRowWrapper matches the Iceberg struct exactly even when a nested equality key is read
    // through an ancestor struct that is wider than the delete schema.
    val convertType =
      Class.forName("org.apache.iceberg.spark.SparkSchemaUtil", false, cl).getMethod("convert", schemaClass)
    // Per group: the set (loaded lazily, only for the row probe below), the batch ordinals of its delete-schema
    // top-level columns, the delete-schema-shaped Spark type, the Iceberg struct, the cache key, the Spark types
    // the data columns were read as (for the per-row reshape when a read column is wider than the delete column,
    // e.g. a projected ancestor struct holds fields the delete does not), and the delete schema + selected delete
    // files (for the native set, read straight from the files).
    val groups = deletes.asScala.groupBy(equalityFieldIds).toSeq.map { case (ids, files) =>
      // Resolve the group's equality ids against the schema HISTORY: a dropped equality key (top level or a nested
      // struct member) is absent from the current schema but present in an older one, and TypeUtil.select needs a
      // schema that holds every id.
      val resolveSchema = schemaContainingAll(tableSchemas, ids)
      require(resolveSchema != null, s"equality delete field ids $ids are in no schema of the table's history")
      val deleteSchema = select.invoke(null, resolveSchema, ids.map(Int.box).toSet.asJava)
      val deleteStruct = call(deleteSchema, "asStruct")
      val order = call(deleteSchema, "columns")
        .asInstanceOf[java.util.List[AnyRef]]
        .asScala
        .map(f => call(f, "fieldId").asInstanceOf[Integer].intValue())
        .toArray
      val cacheKey = (files.map(d => call(d, "location").toString).sorted.toSeq, ids)
      // Iceberg's own set, loaded only when the row probe below needs it: the native path reads the delete files
      // itself (EqualityDeleteSet.ofDeleteFiles), so an eager load here would read every delete file twice.
      val set = () =>
        cache.synchronized(
          cache.getOrElseUpdate(
            cacheKey, {
              ParquetFileResolver.icebergEqualityLoads.incrementAndGet()
              load.invoke(loader, files.asJava, deleteSchema)
            }
          )
        )
      val ordinals = order.map(ordinalOf)
      // The delete-schema Spark type (one field per top-level delete column, in order), matching deleteStruct, so
      // the wrapper lines up even when a nested key is read through a wider ancestor struct.
      val sparkType = convertType.invoke(null, deleteSchema).asInstanceOf[StructType]
      // The data columns' read Spark types, in the same order (a struct here may be wider than the delete field).
      val readTypes = ordinals.map(o => schema(o).dataType)
      (set, ordinals, sparkType, deleteStruct, cacheKey, readTypes, deleteSchema, files)
    }
    // #901 R2: each group as the bridge's EqualityDeleteSet, probed with the batch's lanes; None without the
    // bridge or for a key type it does not take, which keeps the row probe below.
    val nativeGroups: Option[Seq[(AnyRef, java.lang.reflect.Method, Array[Int])]] =
      if (!native) None
      else
        try {
          val cls = Class.forName("io.vecruntime.iceberg.read.EqualityDeleteSet", true, cl)
          val supports = cls.getMethod("supports", Class.forName("org.apache.iceberg.types.Type", false, cl))
          val schemaCls = Class.forName("org.apache.iceberg.Schema", false, cl)
          val ofDeleteFiles = cls.getMethod("ofDeleteFiles", schemaCls, classOf[java.lang.Iterable[_]])
          val clear = cls.getMethod(
            "clearDeleted",
            classOf[Array[io.vecruntime.kernels.VectorBuffers]],
            Integer.TYPE,
            classOf[java.lang.foreign.MemorySegment]
          )
          val all = groups.map { case (_, ordinals, _, struct, cacheKey, _, deleteSchema, files) =>
            val types = call(struct, "fields").asInstanceOf[java.util.List[AnyRef]].asScala.map(call(_, "type"))
            if (!types.forall(t => supports.invoke(null, t).asInstanceOf[java.lang.Boolean].booleanValue())) None
            else {
              // #rtypes: the native set is read STRAIGHT from the delete files, encoding each record's key before
              // the next is read, so a fixed/binary equality delete is not collapsed to one key the way Iceberg's
              // BaseDeleteLoader StructLikeSet collapses it (FixedReader buffer reuse + reference-only set copy).
              // Built once per task and group (the key's "#native" marks it); applicability is already decided --
              // `files` are only the delete files selected for this data file.
              val built = cache.synchronized {
                cache.getOrElseUpdate(
                  (cacheKey._1 :+ "#native", cacheKey._2), {
                    val inputs: java.lang.Iterable[AnyRef] = files.map(d => newInputFile.invoke(fileIo, d)).asJava
                    ofDeleteFiles.invoke(null, deleteSchema, inputs)
                  }
                )
              }
              Some((built, clear, ordinals))
            }
          }
          if (all.forall(_.isDefined)) Some(all.map(_.get)) else None
        } catch {
          // No bridge, or a bridge class a type/relocation mismatch keeps us from using: drop to the row probe
          // below, which reads deletes through Iceberg's own BaseDeleteLoader and is correct for every key type
          // except the fixed/binary StructLikeSet collapse the native path exists to avoid. The reflective reader
          // raises EqualityDeleteSet.UnsupportedEqualityReaderException (reaching us as InvocationTargetException)
          // when Iceberg's generic Parquet reader API is absent/reshaped; a shaded-vs-unshaded parquet MessageType
          // mismatch surfaces as ClassCastException/LinkageError. All mean "native not usable here" -> fall back.
          case _: ClassNotFoundException | _: NoClassDefFoundError => None
          case e: java.lang.reflect.InvocationTargetException if isNativeEqualityUnsupported(e.getCause) => None
          case _: ClassCastException | _: LinkageError =>
            ParquetFileResolver.nativeEqualityFallbacks.incrementAndGet()
            None
        }
    if (nativeGroups.isDefined) {
      val native = nativeGroups.get
      return batch => {
        val n = batch.numRows()
        val words = new Array[Long](math.max(1, (n + 63) >>> 6))
        java.util.Arrays.fill(words, -1L)
        val live = java.lang.foreign.MemorySegment.ofArray(words)
        val arena = java.lang.foreign.Arena.ofConfined()
        try {
          native.foreach { case (set, clear, ordinals) =>
            val columns =
              ordinals.map(o => io.vecruntime.spark.adapter.ColumnVectorAdapters.adapt(batch.column(o), n, arena))
            clear.invoke(set, columns, Int.box(n), live)
          }
        } finally arena.close()
        ParquetFileResolver.nativeEqualityBatches.incrementAndGet()
        (i: Int) => (words(i >>> 6) & (1L << (i & 63))) == 0
      }
    }
    batch => {
      val probes = groups.map { case (loadSet, ordinals, sparkType, icebergStruct, _, readTypes, _, _) =>
        val set = loadSet()
        val wrapper = wrapperCtor.newInstance(sparkType, icebergStruct)
        val row = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(ordinals.length)
        val deleteTypes = sparkType.fields.map(_.dataType)
        (i: Int) => {
          var k = 0
          while (k < ordinals.length) {
            val c = batch.column(ordinals(k))
            row.update(
              k,
              if (c.isNullAt(i)) null
              else IcebergTasks.reshapeToDelete(
                batch.getRow(i).get(ordinals(k), readTypes(k)),
                readTypes(k),
                deleteTypes(k)
              )
            )
            k += 1
          }
          contains.invoke(set, wrap.invoke(wrapper, row)).asInstanceOf[java.lang.Boolean].booleanValue()
        }
      }
      (i: Int) => probes.exists(probe => probe(i))
    }
  }

  /**
   * #shading: whether a Throwable thrown out of the native equality-delete reader means "not usable against this
   * classpath" -- so the scan should fall back to Iceberg's own delete loader rather than fail the query. True for
   * the bridge's `EqualityDeleteSet.UnsupportedEqualityReaderException` (matched by name, since iceberg-bridge is
   * reflected, not a compile dependency of this module in this direction) and for a shaded-vs-unshaded Parquet
   * `MessageType` mismatch surfacing as `ClassCastException`/`LinkageError`. Walks the cause chain.
   */
  private def isNativeEqualityUnsupported(t: Throwable): Boolean = {
    var c = t
    var depth = 0
    while (c != null && depth < 8) {
      val n = c.getClass.getName
      if (
        n == "io.vecruntime.iceberg.read.EqualityDeleteSet$UnsupportedEqualityReaderException"
        || c.isInstanceOf[ClassCastException]
        || c.isInstanceOf[LinkageError]
      ) return true
      c = c.getCause
      depth += 1
    }
    false
  }

  /**
   * #901 item 5: reshape a value read with Spark type `read` into Spark type `want` for the equality-delete probe.
   * Identical types pass through. A struct read wider than wanted (a projected ancestor holding fields the delete
   * does not) is narrowed to `want`'s fields, matched by name (field-id matching already aligned the readers), and
   * reshaped recursively; a null struct stays null. Only structs can differ (the delete leaves are atomic).
   */
  def reshapeToDelete(
      value: Any,
      read: org.apache.spark.sql.types.DataType,
      want: org.apache.spark.sql.types.DataType
  ): Any = (read, want) match {
    case _ if value == null => null
    case (r: StructType, w: StructType) if r != w =>
      val src = value.asInstanceOf[InternalRow]
      val out = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(w.length)
      var j = 0
      while (j < w.length) {
        val wf = w.fields(j)
        val ri = r.fieldIndex(wf.name)
        out.update(
          j,
          if (src.isNullAt(ri)) null
          else reshapeToDelete(src.get(ri, r.fields(ri).dataType), r.fields(ri).dataType, wf.dataType)
        )
        j += 1
      }
      out
    case _ => value
  }

  /**
   * The deleted row positions of one data file, ascending, from its position delete files or deletion vector,
   * loaded by Iceberg's own `BaseDeleteLoader` through the partition's `FileIO` (as Iceberg's reader loads
   * them, so position deletes and deletion vectors keep Iceberg's semantics, including the DV's referenced-file
   * check).
   */
  def deletedPositions(p: InputPartition, location: String, deletes: java.util.List[AnyRef]): Array[Long] = {
    val cl = p.getClass.getClassLoader
    val (loaderClass, loader) = deleteLoader(p)
    val index = loaderClass
      .getMethod("loadPositionDeletes", classOf[java.lang.Iterable[_]], classOf[CharSequence])
      .invoke(loader, deletes, location)
    val positions = new scala.collection.mutable.ArrayBuilder.ofLong
    val collect: java.util.function.LongConsumer = pos => positions += pos
    Class
      .forName("org.apache.iceberg.deletes.PositionDeleteIndex", false, cl)
      .getMethod("forEach", classOf[java.util.function.LongConsumer])
      .invoke(index, collect)
    val sorted = positions.result()
    java.util.Arrays.sort(sorted)
    sorted
  }

  /**
   * A reader batch's columns taken in output order (`out`: per output column, its ordinal in `batch`, -1 for a
   * built column). `rebuild`, when set, replaces the three v3 metadata columns (#20 M3a) in place.
   */
  def reordered(
      batch: ColumnarBatch,
      out: Array[Int],
      rebuild: (ColumnarBatch, Array[org.apache.spark.sql.vectorized.ColumnVector]) => Unit = null
  ): ColumnarBatch = {
    val columns = out.map(o => if (o < 0) null else batch.column(o))
    if (rebuild != null) rebuild(batch, columns)
    batch match {
      case s: io.vecruntime.spark.arrow.SelectedColumnarBatch =>
        io.vecruntime.spark.arrow.SelectedColumnarBatch.ofHeap(
          columns,
          s.numRows(),
          s.selection(),
          s.selectedCount(),
          false
        )
      case _ => new ColumnarBatch(columns, batch.numRows())
    }
  }

  /**
   * A long metadata column for a reader batch: the file's own value where it is non-null, else `fallback(i)`
   * (which may be null). `read` is the column read from the file (an all-null [[NativeParquetNullColumn]] when the
   * file does not materialise the column). Built into a Spark `OnHeapColumnVector` the batch owns; it is closed by
   * `SelectedColumnarBatch` / `ColumnarBatch` with the rest of the batch's built columns.
   */
  def metadataLong(
      read: org.apache.spark.sql.vectorized.ColumnVector,
      n: Int,
      fallback: Int => java.lang.Long
  ): org.apache.spark.sql.vectorized.ColumnVector = {
    val v = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector(n, org.apache.spark.sql.types.LongType)
    var i = 0
    while (i < n) {
      if (read != null && !read.isNullAt(i)) v.putLong(i, read.getLong(i))
      else {
        val f = fallback(i)
        if (f == null) v.putNull(i) else v.putLong(i, f.longValue())
      }
      i += 1
    }
    v
  }

  /**
   * The `_partition` struct as a batch-length column (#20 M3a). An `OnHeapColumnVector` of the Spark struct type
   * (Spark allocates one writable child per partition field) with every row set to the file's partition values:
   * the parent marked not-null and each leaf child filled from the partition `InternalRow`. This handles the empty
   * struct of an unpartitioned table (a not-null zero-field row) and a non-empty one alike. `row` null -- a file
   * with no partition -- is an all-null column. The batch owns and closes it.
   */
  def partitionConstant(
      st: StructType,
      row: InternalRow,
      n: Int
  ): org.apache.spark.sql.vectorized.ColumnVector = {
    val cv = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector(math.max(1, n), st)
    // An unpartitioned table has an empty partition struct; Iceberg's own reader returns `_partition` null there,
    // so match it rather than emitting a zero-field row.
    if (row == null || st.isEmpty) { cv.putNulls(0, n); return cv }
    cv.putNotNulls(0, n)
    var f = 0
    while (f < st.length) {
      fillStructChild(
        cv.getChild(f).asInstanceOf[org.apache.spark.sql.execution.vectorized.WritableColumnVector],
        st.fields(f).dataType,
        row,
        f,
        n
      )
      f += 1
    }
    cv
  }

  /** Fill a writable child vector with the same value in every one of `n` rows (partition fields are atomic). */
  private def fillStructChild(
      child: org.apache.spark.sql.execution.vectorized.WritableColumnVector,
      dt: org.apache.spark.sql.types.DataType,
      row: InternalRow,
      ordinal: Int,
      n: Int
  ): Unit = {
    import org.apache.spark.sql.types._
    if (row.isNullAt(ordinal)) { child.putNulls(0, n); return }
    child.putNotNulls(0, n)
    dt match {
      case BooleanType => val v = row.getBoolean(ordinal); var i = 0; while (i < n) { child.putBoolean(i, v); i += 1 }
      case ByteType => val v = row.getByte(ordinal); var i = 0; while (i < n) { child.putByte(i, v); i += 1 }
      case ShortType => val v = row.getShort(ordinal); var i = 0; while (i < n) { child.putShort(i, v); i += 1 }
      case IntegerType | DateType | _: YearMonthIntervalType =>
        val v = row.getInt(ordinal); var i = 0; while (i < n) { child.putInt(i, v); i += 1 }
      case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
        val v = row.getLong(ordinal); var i = 0; while (i < n) { child.putLong(i, v); i += 1 }
      case FloatType => val v = row.getFloat(ordinal); var i = 0; while (i < n) { child.putFloat(i, v); i += 1 }
      case DoubleType => val v = row.getDouble(ordinal); var i = 0; while (i < n) { child.putDouble(i, v); i += 1 }
      case s: StringType =>
        val v = row.getUTF8String(ordinal); val bytes = v.getBytes
        var i = 0; while (i < n) { child.putByteArray(i, bytes); i += 1 }
      case BinaryType =>
        val v = row.getBinary(ordinal); var i = 0; while (i < n) { child.putByteArray(i, v); i += 1 }
      case d: DecimalType =>
        val v = row.getDecimal(ordinal, d.precision, d.scale); var i = 0
        while (i < n) { child.putDecimal(i, v, d.precision); i += 1 }
      case other =>
        throw new UnsupportedOperationException(s"unsupported _partition field type ${other.simpleString}")
    }
  }

  /**
   * #901 item 3: `batch` with its columns in output order (`out`), keeping EVERY row and filling the `_deleted`
   * boolean at `deletedOut` -- true for a row a position/DV delete (`deleted` positions, matched against the file
   * positions at `posOrdinal`) or an equality delete (`equalityDeleted`) marks, false otherwise -- as Iceberg's
   * reader does when `_deleted` is projected (it keeps deleted rows instead of dropping them). The batch's own
   * selection (from the reader) is preserved; deletes do not deselect here. `rebuild`, when set, fills the v3
   * metadata columns first. The data columns stay the reader's (borrowed).
   */
  def withDeletedMarked(
      batch: ColumnarBatch,
      out: Array[Int],
      deletedOut: Int,
      posOrdinal: Int,
      deleted: Array[Long],
      equalityDeleted: Int => Boolean,
      rebuild: (ColumnarBatch, Array[org.apache.spark.sql.vectorized.ColumnVector]) => Unit = null
  ): ColumnarBatch = {
    val n = batch.numRows()
    val columns = out.map(o => if (o < 0) null else batch.column(o))
    if (rebuild != null) rebuild(batch, columns)
    val flags =
      new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector(
        math.max(1, n),
        org.apache.spark.sql.types.BooleanType
      )
    flags.putBooleans(0, n, false)
    if (deleted.nonEmpty && n > 0) {
      val pos = batch.column(posOrdinal)
      var i = 0
      var j = lowerBound(deleted, pos.getLong(0))
      while (i < n && j < deleted.length) {
        val p = pos.getLong(i)
        while (j < deleted.length && deleted(j) < p) j += 1
        if (j < deleted.length && deleted(j) == p) flags.putBoolean(i, true)
        i += 1
      }
    }
    if (equalityDeleted != null) {
      var i = 0
      while (i < n) { if (equalityDeleted(i)) flags.putBoolean(i, true); i += 1 }
    }
    columns(deletedOut) = flags
    batch match {
      case s: io.vecruntime.spark.arrow.SelectedColumnarBatch =>
        io.vecruntime.spark.arrow.SelectedColumnarBatch.ofHeap(columns, n, s.selection(), s.selectedCount(), false)
      case _ => new ColumnarBatch(columns, n)
    }
  }

  /**
   * `batch` with its columns taken in output order (`out`: per output column, its ordinal in `batch`, -1 for a
   * built column) -- dropping the equality columns the query does not project and, unless `_pos` is an output, the
   * file's row positions at `posOrdinal` (Spark's temporary row-index column, made by our reader and by Spark's
   * fallback reader alike) -- with the rows at `deleted` positions deselected and, when `equalityDeleted` is set,
   * the rows it reports deleted (by row of the batch). `rebuild`, when set, replaces the three v3 metadata columns
   * (#20 M3a) in place before the selection is applied. Rows of a batch come in file order, so the walk through
   * `deleted` is one pass. The data columns stay the reader's (borrowed).
   */
  def withoutDeleted(
      batch: ColumnarBatch,
      out: Array[Int],
      posOrdinal: Int,
      deleted: Array[Long],
      equalityDeleted: Int => Boolean,
      rebuild: (ColumnarBatch, Array[org.apache.spark.sql.vectorized.ColumnVector]) => Unit = null
  ): ColumnarBatch = {
    val n = batch.numRows()
    val columns = out.map(o => if (o < 0) null else batch.column(o))
    if (rebuild != null) rebuild(batch, columns)
    val base = batch match {
      case s: io.vecruntime.spark.arrow.SelectedColumnarBatch => s
      case _ => null
    }
    val pos = batch.column(posOrdinal)
    var j = if (n == 0) deleted.length else lowerBound(deleted, pos.getLong(0))
    val noPositions = n == 0 || j == deleted.length || deleted(j) > pos.getLong(n - 1)
    if (noPositions && (equalityDeleted == null || n == 0)) {
      return if (base == null) new ColumnarBatch(columns, n)
      else io.vecruntime.spark.arrow.SelectedColumnarBatch.ofHeap(
        columns,
        n,
        base.selection(),
        base.selectedCount(),
        false
      )
    }
    val words = new Array[Long]((n + 63) >>> 6)
    val bits = java.lang.foreign.MemorySegment.ofArray(words)
    if (base != null) io.vecruntime.kernels.BitmapKernels.copy(base.selection(), bits, n)
    else {
      java.util.Arrays.fill(words, -1L)
      if ((n & 63) != 0) words(words.length - 1) = (1L << (n & 63)) - 1
    }
    if (!noPositions) {
      var i = 0
      while (i < n && j < deleted.length) {
        val p = pos.getLong(i)
        while (j < deleted.length && deleted(j) < p) j += 1
        if (j < deleted.length && deleted(j) == p) words(i >>> 6) &= ~(1L << (i & 63))
        i += 1
      }
    }
    if (equalityDeleted != null) {
      var i = 0
      while (i < n) {
        if ((words(i >>> 6) & (1L << (i & 63))) != 0 && equalityDeleted(i)) words(i >>> 6) &= ~(1L << (i & 63))
        i += 1
      }
    }
    var count = 0
    var w = 0
    while (w < words.length) { count += java.lang.Long.bitCount(words(w)); w += 1 }
    io.vecruntime.spark.arrow.SelectedColumnarBatch.ofHeap(columns, n, bits, count, false)
  }

  private def lowerBound(a: Array[Long], v: Long): Int = {
    var lo = 0
    var hi = a.length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      if (a(mid) < v) lo = mid + 1 else hi = mid
    }
    lo
  }
}
