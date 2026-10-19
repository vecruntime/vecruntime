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
import org.apache.spark.sql.vectorized.ColumnarBatch
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
    pushedFilters: Seq[org.apache.spark.sql.sources.Filter] = Nil
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
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numRowGroups" -> SQLMetrics.createMetric(sparkContext, "number of row groups read"),
    "numLateSkippedRows" -> SQLMetrics.createMetric(sparkContext, "rows the decode filter dropped before decoding"),
    "numPagesSkipped" -> SQLMetrics.createMetric(sparkContext, "pages the column index skipped"),
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
    // As the Parquet scan: runtime filters prune row groups by statistics only; with no static filter but null
    // checks, Parquet's dictionary and bloom checks (an object-store request per row group) are off.
    val statsOnly = runtime.nonEmpty && pushedFilters.forall(_.isInstanceOf[org.apache.spark.sql.sources.IsNotNull])
    // The output's data columns (metadata columns aside), each carrying its Iceberg field id, so the reader
    // matches the file's columns by id.
    val kinds = output.zip(fieldIds).map { case (a, id) => IcebergTasks.columnKind(a.name, id) }
    val dataColumns = output.zip(fieldIds).zip(kinds).collect { case ((a, id), IcebergTasks.DataColumn) => (a, id) }
    val requiredSchema = StructType(dataColumns.map { case (a, id) =>
      StructField(
        a.name,
        // #nested: nested struct fields matched by their Iceberg field ids too.
        if (a.dataType.isInstanceOf[AtomicType]) a.dataType
        else IcebergTasks.withFieldIds(a.dataType, IcebergTasks.projectedType(scan, id)),
        a.nullable, {
          val md = new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id.toLong)
          // A v3 initial default: our reader fills it into files written before the column was added.
          IcebergTasks.initialDefaultSql(scan, id, a.dataType).foreach { sql =>
            md.putString(
              org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY,
              sql
            )
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
    val equalitySchema = IcebergTasks
      .equalityColumns(scan.inputRDD.partitions, tableSchema, dataColumns.map(_._2).toSet)
      .map(extra => VectorIcebergRDD.withPositions(StructType(requiredSchema.fields ++ extra)))
    val equalityConfBroadcast = equalitySchema.map(confFor)

    // Pushed filters by field id: the data columns' quoted names, as the translated filters name them.
    val filterFieldIds =
      if (filters.isEmpty || !sqlConf.parquetFilterPushDown) null
      else dataColumns.map { case (a, id) => org.apache.spark.sql.catalyst.util.quoteIfNeeded(a.name) -> id }.toMap
    val m = ScanMetrics(
      longMetric("numFiles"),
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"),
      longMetric("numRowGroups"),
      longMetric("scanTime"),
      longMetric("numLateSkippedRows"),
      longMetric("numPagesSkipped")
    )
    val rdd = new VectorIcebergRDD(
      session.sparkContext,
      VectorIcebergRDD.packed(scan, sqlConf, session.leafNodeDefaultParallelism),
      scan.readerFactory,
      fieldIds.toArray,
      confBroadcast,
      positionsConfBroadcast,
      equalityConfBroadcast,
      equalitySchema,
      tableSchema,
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
      )
    )
    // Counted by the task of partition 0, once per execution, as the Parquet scan counts them.
    if (runtime.isEmpty) rdd
    else {
      val counter = longMetric("numRuntimeFilters")
      val n = runtime.size.toLong
      rdd.mapPartitionsWithIndexInternal { (i, it) => if (i == 0) counter += n; it }
    }
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
    requiredSchema: StructType,
    attrs: Array[(String, org.apache.spark.sql.types.DataType)], // the data columns, in output order
    kinds: Array[IcebergTasks.ColumnKind], // per output column
    batchSize: Int,
    rebaseConf: NativeScanRebaseConf,
    prefetchFiles: Int,
    prefetchRowGroups: Int,
    decodeAhead: Int,
    decodeAheadVirtual: Boolean,
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
    pushDown: PushDownConf
) extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = parts

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    VectorIcebergRDD.inputsOf(split).flatMap(_.preferredLocations()).distinct

  // Iceberg metadata columns. `_pos` is the reader's row-index column; `_file` and `_spec_id` are per-file
  // constants, passed to the reader as the split's partition values (it appends them after the data columns).
  private val needsPositions = kinds.contains(IcebergTasks.PosColumn)
  private val constants = kinds.exists(k => k == IcebergTasks.FileColumn || k == IcebergTasks.SpecIdColumn)
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
    def flush(): Unit = if (pendingCount > 0) {
      val files = pending.result().toArray
      pending.clear()
      pendingCount = 0
      segments += (() => nativeIterator(split.index, files, context))
    }
    // Equality delete sets loaded once per Spark task: many data files share the same delete files.
    val equalitySets = scala.collection.mutable.HashMap.empty[(Seq[String], Seq[Int]), AnyRef]
    inputs.foreach { p =>
      IcebergTasks.nativeTasks(p, equality = equalitySchema.isDefined, constants = constants) match {
        case Some(tasks) =>
          nativeGroups += 1
          tasks.foreach { t =>
            if (t.deletes.isEmpty && t.equalityDeletes.isEmpty) {
              pending += t.file
              pendingCount += 1
            } else {
              // A file with delete files: read with its row positions (and its equality columns), the deleted
              // rows deselected (its delete files are loaded when its first batch is read).
              flush()
              segments += (() => deletedIterator(split.index, p, t, equalitySets, context))
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
   * when `positions`][constants]: per output column, its ordinal in the batch.
   */
  private def layout(extra: Int, positions: Boolean): Array[Int] = {
    val d = attrs.length
    val posOrdinal = d + extra
    val constBase = posOrdinal + (if (positions) 1 else 0)
    var data = 0
    kinds.map {
      case IcebergTasks.DataColumn => data += 1; data - 1
      case IcebergTasks.PosColumn => posOrdinal
      case IcebergTasks.FileColumn => constBase
      case IcebergTasks.SpecIdColumn => constBase + 1
    }
  }

  private def deletedIterator(
      index: Int,
      p: InputPartition,
      t: IcebergTasks.NativeTask,
      equalitySets: scala.collection.mutable.HashMap[(Seq[String], Seq[Int]), AnyRef],
      context: TaskContext
  ): Iterator[ColumnarBatch] = {
    lazy val deleted =
      if (t.deletes.isEmpty) Array.emptyLongArray else IcebergTasks.deletedPositions(p, t.location, t.deletes)
    if (t.equalityDeletes.isEmpty) {
      val out = layout(0, positions = true)
      readerOf(index, Array(t.file), positionsConfBroadcast, VectorIcebergRDD.withPositions(requiredSchema), context)
        .map(b => IcebergTasks.withoutDeleted(b, out, attrs.length, deleted, null))
    } else {
      val schema = equalitySchema.get
      val out = layout(schema.length - attrs.length - 1, positions = true)
      lazy val equality = IcebergTasks.equalityFilter(p, t.equalityDeletes, tableSchema, schema, equalitySets)
      readerOf(index, Array(t.file), equalityConfBroadcast.get, schema, context)
        .map(b => IcebergTasks.withoutDeleted(b, out, schema.length - 1, deleted, equality(b)))
    }
  }

  private def nativeIterator(
      index: Int,
      files: Array[PartitionedFile],
      context: TaskContext
  ): Iterator[ColumnarBatch] =
    if (plain) readerOf(index, files, confBroadcast, requiredSchema, context)
    else if (needsPositions) {
      val out = layout(0, positions = true)
      readerOf(index, files, positionsConfBroadcast, VectorIcebergRDD.withPositions(requiredSchema), context)
        .map(b => IcebergTasks.withoutDeleted(b, out, attrs.length, Array.emptyLongArray, null))
    } else {
      val out = layout(0, positions = false)
      readerOf(index, files, confBroadcast, requiredSchema, context).map(b => IcebergTasks.reordered(b, out))
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
      handOff = decodeAhead > 0,
      poolLimit = math.max(4, decodeAhead + 2),
      dictionaryStrings = dictionaryStrings,
      prefetchMaxBytes = prefetchMaxBytes,
      prefetchExecutorMaxBytes = prefetchExecutorMaxBytes,
      reuseDecompressBuffers = reuseDecompressBuffers,
      poolChunkBuffers = poolChunkBuffers,
      filterFieldIds = filterFieldIds
    )
    if (decodeAhead > 0) new DecodeAheadIterator(reader, decodeAhead, decodeAheadVirtual, context) else reader
  }

  private def icebergIterator(p: InputPartition, context: TaskContext): Iterator[ColumnarBatch] = {
    // The planner admits a scan Iceberg does not vectorize only when every task group reads natively.
    if (!readerFactory.supportColumnarReads(p)) {
      throw new IllegalStateException("an Iceberg task group needs Iceberg's reader, which is not columnar here")
    }
    val reader = readerFactory.createColumnarReader(p)
    context.addTaskCompletionListener[Unit](_ => reader.close())
    new Iterator[ColumnarBatch] {
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
   * The wrapped scan's partitions packed as Spark packs a file scan's splits (`FilePartition.maxSplitBytes` and
   * `getFilePartitions`), see [[io.vecruntime.spark.VectorConf.ScanNativeIcebergPackSplits]]: each input partition
   * weighs its task group's bytes plus `spark.sql.files.openCostInBytes` per file, the largest first, next-fit into
   * tasks of at most `min(maxPartitionBytes, max(openCost, total / minPartitionNum))`. The scan's own partitions
   * when it reports a partitioning, when packing is off or merges nothing, or when a group's size is unknown.
   */
  def packed(scan: BatchScanExec, conf: SQLConf, leafParallelism: Int): Array[Partition] = {
    val parts = scan.inputRDD.partitions
    if (
      parts.length < 2 || !io.vecruntime.spark.VectorConf.scanNativeIcebergPackSplits(conf) ||
      !scan.outputPartitioning.isInstanceOf[org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning]
    ) return parts
    val openCost = conf.filesOpenCostInBytes
    val inputs = parts.map(p => shims.SparkShims.dataSourceInputPartitions(p))
    val sizes = inputs.map(_.map(IcebergTasks.groupBytes(_, openCost)))
    if (sizes.exists(_.exists(_ < 0))) return parts
    val weights = sizes.map(_.sum)
    val minPartitions = conf.filesMinPartitionNum.getOrElse(leafParallelism).max(1)
    val maxSplit = math.min(conf.filesMaxPartitionBytes, math.max(openCost, weights.sum / minPartitions))
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
      if (currentSize > 0 && currentSize + weights(i) > maxSplit) close()
      current ++= inputs(i)
      currentSize += weights(i)
    }
    close()
    val packedParts = out.result()
    if (packedParts.length == parts.length) parts else packedParts
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
    // #nested: lists, maps and structs at any depth, as our Parquet reader reads them.
    b.output.foreach { a =>
      if (!io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(a.dataType)) {
        return Left(s"unsupported column type ${a.name}:${a.dataType.simpleString}")
      }
    }
    // Iceberg's reader is not vectorized for some scans (nested columns): every task group must then be read
    // natively, since the node cannot fall back to Iceberg's columnar reader for one of them.
    if (!b.supportsColumnar) {
      val kinds = b.output.zip(ids).map { case (a, id) => IcebergTasks.columnKind(a.name, id) }
      val constants = kinds.exists(k => k == IcebergTasks.FileColumn || k == IcebergTasks.SpecIdColumn)
      val parts = b.inputPartitions
      val projected = b.output.zip(ids).zip(kinds).collect { case ((_, id), IcebergTasks.DataColumn) => id }.toSet
      val equality = IcebergTasks.equalityColumnsOf(parts, IcebergTasks.tableSchema(b), projected).isDefined
      if (!parts.forall(p => IcebergTasks.nativeTasks(p, equality, constants).isDefined)) {
        return Left("Iceberg scan is not vectorized and a task group needs Iceberg's reader")
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

  // Iceberg types whose Spark type and Parquet encoding our reader decodes as Iceberg's reader does. Not: uuid
  // (16 bytes in the file, a string in Spark), fixed, time, timestamp_ns, variant, geometry, unknown.
  private val NativeTypes =
    Set("BOOLEAN", "INTEGER", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "STRING", "BINARY", "DECIMAL")

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

  // Iceberg's reserved metadata column ids (MetadataColumns): every metadata column sits near Int.MaxValue.
  private val FileId = Int.MaxValue - 1
  private val PosId = Int.MaxValue - 2
  private val SpecIdId = Int.MaxValue - 4
  private val MetadataIdFloor = Int.MaxValue - 10000

  def columnKind(name: String, id: Int): ColumnKind =
    if (id == FileId && name == "_file") FileColumn
    else if (id == PosId && name == "_pos") PosColumn
    else if (id == SpecIdId && name == "_spec_id") SpecIdColumn
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
      val table = call(b.scan, "table")
      val props = call(table, "properties").asInstanceOf[java.util.Map[String, String]]
      if (props.containsKey("schema.name-mapping.default")) {
        return Left("Iceberg table has a name mapping (files may carry no field ids)")
      }
      val schema = call(b.scan, "projection")
      val columns = call(schema, "columns").asInstanceOf[java.util.List[AnyRef]].asScala
      val byName = columns.map(f => call(f, "name").asInstanceOf[String].toLowerCase -> f).toMap
      Right(b.output.map { a =>
        a.name match {
          case "_file" => FileId
          case "_pos" => PosId
          case "_spec_id" => SpecIdId
          case _ =>
            val field =
              byName.getOrElse(a.name.toLowerCase, return Left(s"column ${a.name} not in the Iceberg projection"))
            val id = call(field, "fieldId").asInstanceOf[Integer].intValue()
            // Other metadata columns (_deleted, _partition, row lineage) keep Iceberg's reader.
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
   * Why an Iceberg column type is not read natively, or None: a flat type of [[NativeTypes]], or (#nested) a struct,
   * list or map whose every leaf is one, at any depth. A nested field with an initial default is not: Iceberg fills
   * it for files written before it was added, our reader reads nulls.
   */
  private def unsupportedType(icebergType: AnyRef, path: String): Option[String] =
    call(icebergType, "typeId").toString match {
      case "STRUCT" =>
        call(icebergType, "fields").asInstanceOf[java.util.List[AnyRef]].asScala.iterator.map { f =>
          val name = s"$path.${call(f, "name")}"
          if (call(f, "initialDefault") != null) Some(s"Iceberg field $name has an initial default")
          else unsupportedType(call(f, "type"), name)
        }.collectFirst { case Some(r) => r }
      case "LIST" => unsupportedType(call(icebergType, "elementType"), s"$path.element")
      case "MAP" =>
        unsupportedType(call(icebergType, "keyType"), s"$path.key")
          .orElse(unsupportedType(call(icebergType, "valueType"), s"$path.value"))
      case t if NativeTypes.contains(t) => None
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
              f.copy(
                dataType = withFieldIds(f.dataType, call(nf, "type")),
                metadata = new MetadataBuilder().withMetadata(f.metadata)
                  .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build()
              )
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

  /**
   * One file scan task read natively: its split, the data file's location, its position delete files (and deletion
   * vector) and its equality delete files (those Iceberg's planning found apply to it, by sequence number).
   */
  final case class NativeTask(
      file: PartitionedFile,
      location: String,
      deletes: java.util.List[AnyRef],
      equalityDeletes: java.util.List[AnyRef]
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
        equalityDeletes
      )
    }
    Some(out.toArray)
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

  /** Iceberg's own `BaseDeleteLoader`, opening delete files through the partition's `FileIO`, and its class. */
  private def deleteLoader(p: InputPartition): (Class[_], AnyRef) = {
    val cl = p.getClass.getClassLoader
    val deleteFileClass = Class.forName("org.apache.iceberg.DeleteFile", false, cl)
    val newInputFile =
      Class.forName("org.apache.iceberg.io.FileIO", false, cl).getMethod("newInputFile", deleteFileClass)
    val io = call(p, "io")
    val open: java.util.function.Function[AnyRef, AnyRef] = d => newInputFile.invoke(io, d)
    val loaderClass = Class.forName("org.apache.iceberg.data.BaseDeleteLoader", false, cl)
    (loaderClass, loaderClass.getConstructor(classOf[java.util.function.Function[_, _]]).newInstance(open))
  }

  /** The scanned table's current schema (an `org.apache.iceberg.Schema`, serializable), the ids equality deletes use. */
  def tableSchema(b: BatchScanExec): AnyRef = call(call(b.scan, "table"), "schema")

  private def equalityFieldIds(d: AnyRef): Seq[Int] =
    call(d, "equalityFieldIds").asInstanceOf[java.util.List[Integer]].asScala.map(_.intValue()).toSeq

  /**
   * The columns to read besides the projected ones (`projected`, field ids) to apply the equality deletes of the
   * planned task groups `parts`: one per equality field the query does not project, named after its id and
   * carrying it as its Parquet field id. None when one of them is not a top-level column of a type our reader
   * decodes as Iceberg does: the files with those deletes then keep Iceberg's reader.
   */
  def equalityColumns(parts: Array[Partition], schema: AnyRef, projected: Set[Int]): Option[Seq[StructField]] =
    equalityColumnsOf(parts.toSeq.flatMap(shims.SparkShims.dataSourceInputPartitions), schema, projected)

  /** As [[equalityColumns]], over the scan's input partitions themselves (at plan time). */
  def equalityColumnsOf(inputs: Seq[InputPartition], schema: AnyRef, projected: Set[Int]): Option[Seq[StructField]] = {
    val ids = scala.collection.mutable.LinkedHashSet.empty[Int]
    inputs.foreach { p =>
      if (p.getClass.getName == "org.apache.iceberg.spark.source.SparkInputPartition") {
        call(call(p, "taskGroup"), "tasks").asInstanceOf[java.util.Collection[AnyRef]].asScala.foreach { task =>
          if (task.getClass.getName.contains("FileScanTask")) {
            val deletes = call(task, "deletes").asInstanceOf[java.util.List[AnyRef]]
            if (deletes != null) deletes.asScala.foreach { d =>
              if (call(d, "content").toString == "EQUALITY_DELETES") ids ++= equalityFieldIds(d)
            }
          }
        }
      }
    }
    val cl = schema.getClass.getClassLoader
    val struct = call(schema, "asStruct")
    val typeClass = Class.forName("org.apache.iceberg.types.Type", false, cl)
    val convert = Class.forName("org.apache.iceberg.spark.SparkSchemaUtil", false, cl).getMethod("convert", typeClass)
    val extra = ids.toSeq.filterNot(projected).sorted.map { id =>
      // Top-level fields only: a nested equality field is not a column our reader reads on its own.
      val field = struct.getClass.getMethod("field", classOf[Int]).invoke(struct, Int.box(id))
      if (field == null) return None
      val icebergType = call(field, "type")
      if (!NativeTypes.contains(call(icebergType, "typeId").toString)) return None
      val dataType = convert.invoke(null, icebergType).asInstanceOf[org.apache.spark.sql.types.DataType]
      if (!io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(dataType)) return None
      StructField(
        s"_vecruntime_eq_$id",
        dataType,
        nullable = true,
        new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id.toLong).build()
      )
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
      tableSchema: AnyRef,
      schema: StructType,
      cache: scala.collection.mutable.HashMap[(Seq[String], Seq[Int]), AnyRef]
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
    // Per group: the set, the batch ordinals of its fields (in the delete schema's order) and their Spark types.
    val groups = deletes.asScala.groupBy(equalityFieldIds).toSeq.map { case (ids, files) =>
      val deleteSchema = select.invoke(null, tableSchema, ids.map(Int.box).toSet.asJava)
      val order = call(deleteSchema, "columns")
        .asInstanceOf[java.util.List[AnyRef]]
        .asScala
        .map(f => call(f, "fieldId").asInstanceOf[Integer].intValue())
        .toArray
      val set = cache.getOrElseUpdate(
        (files.map(d => call(d, "location").toString).sorted.toSeq, ids),
        load.invoke(loader, files.asJava, deleteSchema)
      )
      val ordinals = order.map(ordinalOf)
      val sparkType =
        StructType(ordinals.map(o => schema(o).copy(metadata = org.apache.spark.sql.types.Metadata.empty)))
      (set, ordinals, sparkType, call(deleteSchema, "asStruct"))
    }
    batch => {
      val probes = groups.map { case (set, ordinals, sparkType, icebergStruct) =>
        val wrapper = wrapperCtor.newInstance(sparkType, icebergStruct)
        val row = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(ordinals.length)
        val types = sparkType.fields.map(_.dataType)
        (i: Int) => {
          var k = 0
          while (k < ordinals.length) {
            val c = batch.column(ordinals(k))
            row.update(k, if (c.isNullAt(i)) null else batch.getRow(i).get(ordinals(k), types(k)))
            k += 1
          }
          contains.invoke(set, wrap.invoke(wrapper, row)).asInstanceOf[java.lang.Boolean].booleanValue()
        }
      }
      (i: Int) => probes.exists(probe => probe(i))
    }
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

  /** A reader batch's columns taken in output order (`out`: per output column, its ordinal in `batch`). */
  def reordered(batch: ColumnarBatch, out: Array[Int]): ColumnarBatch = {
    val columns = out.map(batch.column)
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
   * `batch` with its columns taken in output order (`out`: per output column, its ordinal in `batch`) -- dropping
   * the equality columns the query does not project and, unless `_pos` is an output, the file's row positions at
   * `posOrdinal` (Spark's temporary row-index column, made by our reader and by Spark's fallback reader alike) --
   * with the rows at `deleted` positions deselected and, when `equalityDeleted` is set, the rows it reports
   * deleted (by row of the batch). Rows of a batch come in file order, so the walk through `deleted` is one pass.
   * The columns stay the reader's (borrowed).
   */
  def withoutDeleted(
      batch: ColumnarBatch,
      out: Array[Int],
      posOrdinal: Int,
      deleted: Array[Long],
      equalityDeleted: Int => Boolean
  ): ColumnarBatch = {
    val n = batch.numRows()
    val columns = out.map(batch.column)
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
