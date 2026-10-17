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

import io.vecruntime.spark.adapter.TypeMapping
import io.vecruntime.spark.arrow.{ArrowOutput, SelectedColumnarBatch, VectorAllocators}
import io.vecruntime.spark.parquet.{
  NativeParquetColumn,
  NativeParquetColumnReader,
  NativeParquetListReader,
  NativeParquetStructReader
}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.{
  ParquetFilters,
  ParquetReadSupport,
  ParquetWriteSupport,
  SparkToParquetSchemaConverter
}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, DateType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * Our own Parquet scan (#559, slice 1), behind `spark.vecruntime.scan.nativeParquet.enabled`. It decodes
 * Parquet pages straight into our Arrow-layout vectors through [[NativeParquetColumnReader]] (the reused
 * vectors and the injected parquet-java BytePacker), one pass, no Spark `ColumnVector` in between -- the
 * chain above is ours from the leaf.
 *
 * It wraps the `FileSourceScanExec` Spark planned and reuses everything Spark already computed: the
 * dynamically selected partitions (DPP applied), the file splitting (`FileScanRDD` partitions:
 * maxSplitBytes / openCostBytes / bucketing), the pushed data filters and the required + partition schema.
 * Per `PartitionedFile` it opens a `ParquetFileReader` ONCE -- one `getFileStatus` (so the store's status,
 * with its etag on S3A, reaches the stream) and the footer is read once and the reader
 * built from it over the same stream -- clips the requested columns exactly as Spark's
 * `ParquetReadSupport` does (case sensitivity, field ids), sets a `FilterCompat` filter from the pushed
 * filters so `readNextFilteredRowGroup` does row-group and column-index page skipping, and drives one
 * [[NativeParquetColumnReader]] per column. Partition-value columns are constant columns
 * ([[ArrowOutput.constant]]); a row group larger than `spark.sql.parquet.columnarReaderBatchSize` is
 * streamed as `batchSize`-row batches decoded straight into batch-owned Arrow vectors (no copy).
 *
 * `doExecute` = `ColumnarToRowExec(this).doExecute()` (via [[VectorPlan]]). Reports `scan.output` /
 * `outputPartitioning` / `outputOrdering` verbatim, so `EnsureRequirements` (which ran before this
 * replacement) still holds.
 */
case class VectorParquetScanExec(
    scan: FileSourceScanExec,
    /** #611: the condition of the filter above, decoded first (see `markDecodeFilters`); None by default. */
    decodeFilter: Option[org.apache.spark.sql.catalyst.expressions.Expression] = None
) extends LeafExecNode with VectorPlan {

  override val output: Seq[Attribute] = scan.output
  override def outputPartitioning: Partitioning = scan.outputPartitioning
  override def outputOrdering: Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] = scan.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numRowGroups" -> SQLMetrics.createMetric(sparkContext, "number of row groups read"),
    "numRuntimeFilters" -> SQLMetrics.createMetric(sparkContext, "number of runtime filters from joins"),
    "numLateSkippedRows" -> SQLMetrics.createMetric(sparkContext, "rows the decode filter dropped before decoding"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")
  )

  /**
   * Filters a broadcast hash join above derived from its build keys (#610), set on the driver before this
   * node's RDD is made (the join's `doExecuteColumnar` attaches them, then executes its streamed side). They
   * are ANDed with the static pushed filters, so they skip row groups through the same statistics and
   * dictionary paths. Data columns only: never a partition column.
   */
  @transient @volatile private[vecruntime] var runtimeFilters: Seq[org.apache.spark.sql.sources.Filter] = Nil

  /** The runtime filters attached so far (tests). */
  def runtimeFilterCount: Int = runtimeFilters.size

  private[vecruntime] def addRuntimeFilter(f: org.apache.spark.sql.sources.Filter): Unit = synchronized {
    if (!runtimeFilters.contains(f)) runtimeFilters = runtimeFilters :+ f
  }

  /** Whether `name` is a column of the files (not a partition column) this scan reads. */
  private[vecruntime] def isDataColumn(name: String): Boolean =
    scan.requiredSchema.fieldNames.contains(name) && !scan.relation.partitionSchema.fieldNames.contains(name)

  override def doCanonicalize(): SparkPlan = VectorParquetScanExec(
    scan.canonicalized.asInstanceOf[FileSourceScanExec],
    decodeFilter.map(e => org.apache.spark.sql.catalyst.plans.QueryPlan.normalizeExpressions(e, output))
  )

  // Prepare the wrapped scan and WAIT for ALL its subqueries before this node's RDD reads
  // `scan.inputRDD.partitions` (which evaluates the selected partitions and reads the partition filters).
  // The scan owns its subqueries -- dynamic-partition-pruning InSubqueryExec, but also scalar and REUSED
  // subqueries in its partition filters -- and Spark normally starts them (prepareSubqueries) and waits on
  // them (waitForSubqueries, from executeQuery) when the scan is the executed node. We wrap it and never call
  // its executeQuery, so we must do both ourselves; otherwise reading the partitions throws "... has not
  // finished" for a subquery that was started but never awaited (e.g. a ReusedSubquery in a partition
  // filter). Both are public on SparkPlan and idempotent.
  override protected def doPrepare(): Unit = {
    super.doPrepare()
    scan.prepare()
    org.apache.spark.sql.execution.vector.FileScanAccess.prepareAndWaitForSubqueries(scan)
  }

  override def simpleString(maxFields: Int): String =
    s"VectorParquetScan ${decodeFilter.map(f => s"DecodeFilter: ${f.sql} ").getOrElse("")}${scan.simpleString(maxFields)}"

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val relation = scan.relation
    val session = relation.sparkSession
    val sqlConf = session.sessionState.conf
    val requiredSchema = scan.requiredSchema
    val partitionSchema = relation.partitionSchema
    val outputAttrs = scan.output
    val hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
    // Spark's own reader records the read schema on the conf for ParquetReadSupport; we clip ourselves,
    // but keep the write-support keys so a nested/complex fallback path (should one slip through) is sane.
    hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)
    // The flags ParquetFileFormat.setupHadoopConf gives its reader: Spark's fallback reader for a file we do
    // not decode (SparkFallbackFileReader) converts the file schema with these.
    hadoopConf.set(org.apache.parquet.hadoop.ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, session.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      session.sessionState.conf.nestedSchemaPruningEnabled
    )
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, session.sessionState.conf.caseSensitiveAnalysis)
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, session.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, session.sessionState.conf.isParquetINT96AsTimestamp)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key,
      session.sessionState.conf.parquetInferTimestampNTZEnabled
    )
    hadoopConf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, session.sessionState.conf.legacyParquetNanosAsLong)
    // #610: runtime filters prune row groups by statistics only. A dictionary (or bloom) check costs a read of
    // the key column's dictionary page per row group -- an extra object-store request each -- and a key domain
    // too wide for its statistics to prune (an IN list Parquet widens to its range) gains nothing from it. So
    // with runtime filters and no static filter but null checks, Parquet's dictionary and bloom paths are off.
    if (runtimeFilters.nonEmpty && sqlConf.parquetFilterPushDown) {
      val static = org.apache.spark.sql.execution.vector.FileScanAccess.pushedDownFilters(scan)
      if (static.forall(_.isInstanceOf[org.apache.spark.sql.sources.IsNotNull])) {
        hadoopConf.setBoolean(org.apache.parquet.hadoop.ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, false)
        hadoopConf.setBoolean(org.apache.parquet.hadoop.ParquetInputFormat.BLOOM_FILTERING_ENABLED, false)
      }
    }
    val confBroadcast = session.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // Batch size, rounded DOWN to a multiple of 64 (min 64) so every batch offset into a row group is a
    // multiple of 64 -- the alignment VectorBuffers.slice requires when the adapter slices a batch view.
    val batchSize = math.max(64, sqlConf.parquetVectorizedReaderBatchSize / 64 * 64)
    val caseSensitive = sqlConf.caseSensitiveAnalysis
    val useFieldId = sqlConf.parquetFieldIdReadEnabled
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val filterPushDown = sqlConf.parquetFilterPushDown
    val pushedFilters =
      if (filterPushDown)
        org.apache.spark.sql.execution.vector.FileScanAccess.pushedDownFilters(scan) ++ runtimeFilters
      else Seq.empty
    // The rebase modes and INT96 conversion are resolved per file from its footer, as Spark resolves them
    // (DataSourceUtils.datetimeRebaseSpec / int96RebaseSpec): a file whose datetime values need a rebase is
    // read by Spark's reader with that file's modes; the native path decodes CORRECTED files only.
    val rebaseConf = NativeScanRebaseConf(
      sqlConf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ).toString,
      sqlConf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ).toString,
      sqlConf.isParquetINT96TimestampConversion,
      sqlConf.sessionLocalTimeZone
    )
    val prefetchFiles = io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchFiles(sqlConf)
    val prefetchRowGroups = io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchRowGroups(sqlConf)
    val prefetchMaxBytes = io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchMaxBytes(sqlConf)
    val prefetchExecutorMaxBytes = io.vecruntime.spark.VectorConf.scanNativeParquetPrefetchExecutorMaxBytes(sqlConf)
    val decodeAhead = io.vecruntime.spark.VectorConf.scanNativeParquetDecodeAhead(sqlConf)
    val decodeAheadVirtual = io.vecruntime.spark.VectorConf.scanNativeParquetDecodeAheadVirtual(sqlConf)
    val dictionaryStrings = io.vecruntime.spark.VectorConf.scanNativeParquetDictionaryStrings(sqlConf)
    val reuseDecompressBuffers = io.vecruntime.spark.VectorConf.scanNativeParquetReuseDecompressBuffers(sqlConf)
    val poolChunkBuffers = io.vecruntime.spark.VectorConf.scanNativeParquetPoolChunkBuffers(sqlConf)
    // #568 item 9: the file-constant `_metadata` fields the scan outputs after its partition columns (file_path,
    // file_name, file_size, file_block_start, file_block_length, file_modification_time), with the format's
    // extractors, as FileScanRDD fills them per file.
    val metadataFields = scan.fileConstantMetadataColumns.map(a => (a.name, a.dataType)).toArray
    val metadataExtractors = relation.fileFormat.fileConstantMetadataExtractors

    val m = ScanMetrics(
      longMetric("numFiles"),
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"),
      longMetric("numRowGroups"),
      longMetric("scanTime"),
      longMetric("numLateSkippedRows")
    )
    val required = requiredSchema
    val partSchema = partitionSchema
    val attrs = outputAttrs.map(a => (a.name, a.dataType)).toArray
    // #611: the decode filter, compiled over the output, and the data columns it reads.
    val compiledFilter = decodeFilter.map(cond =>
      io.vecruntime.spark.expr.ExpressionCompiler.compilePredicate(cond, outputAttrs) match {
        case Right(e) => e
        case Left(reason) => throw new IllegalStateException(s"cannot compile the decode filter: $reason")
      }
    ).orNull
    val filterColumns = decodeFilter.map { cond =>
      val ids = cond.references.map(_.exprId).toSet
      outputAttrs.take(requiredSchema.length).map(a => ids.contains(a.exprId)).toArray
    }.orNull

    val rdd = new VectorParquetRDD(
      session.sparkContext,
      scan,
      confBroadcast,
      required,
      partSchema,
      attrs,
      batchSize,
      caseSensitive,
      useFieldId,
      pushedFilters,
      pushDownDate,
      pushDownTimestamp,
      pushDownDecimal,
      pushDownStringPredicate,
      pushDownInFilterThreshold,
      rebaseConf,
      prefetchFiles,
      prefetchRowGroups,
      decodeAhead,
      decodeAheadVirtual,
      dictionaryStrings,
      reuseDecompressBuffers,
      poolChunkBuffers,
      m,
      compiledFilter,
      filterColumns,
      metadataFields,
      metadataExtractors,
      prefetchMaxBytes,
      prefetchExecutorMaxBytes
    )
    // Counted by the task of partition 0, once per execution (a driver-side count is reset when a plan
    // whose stages are already materialised runs again).
    val runtime = if (filterPushDown) runtimeFilters.size.toLong else 0L
    if (runtime == 0L) rdd
    else {
      val counter = longMetric("numRuntimeFilters")
      rdd.mapPartitionsWithIndexInternal { (i, it) => if (i == 0) counter += runtime; it }
    }
  }
}

/** The scan's SQL metrics, flushed per task. */
final case class ScanMetrics(
    numFiles: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    numRowGroups: SQLMetric,
    scanTime: SQLMetric,
    numLateSkippedRows: SQLMetric
) extends Serializable

/**
 * The columnar RDD: its partitions are exactly the wrapped scan's `FileScanRDD` partitions (Spark's file
 * splitting, DPP and bucketing already applied), and `compute` reads each `FilePartition`'s files with our
 * native reader.
 */
private[vecruntime] final class VectorParquetRDD(
    sc: org.apache.spark.SparkContext,
    @transient private val scan: FileSourceScanExec,
    confBroadcast: org.apache.spark.broadcast.Broadcast[SerializableConfiguration],
    requiredSchema: StructType,
    partitionSchema: StructType,
    attrs: Array[(String, org.apache.spark.sql.types.DataType)],
    batchSize: Int,
    caseSensitive: Boolean,
    useFieldId: Boolean,
    pushedFilters: Seq[org.apache.spark.sql.sources.Filter],
    pushDownDate: Boolean,
    pushDownTimestamp: Boolean,
    pushDownDecimal: Boolean,
    pushDownStringPredicate: Boolean,
    pushDownInFilterThreshold: Int,
    rebaseConf: NativeScanRebaseConf,
    prefetchFiles: Int,
    prefetchRowGroups: Int,
    decodeAhead: Int,
    decodeAheadVirtual: Boolean,
    dictionaryStrings: Boolean,
    reuseDecompressBuffers: Boolean,
    poolChunkBuffers: Boolean,
    metrics: ScanMetrics,
    decodeFilter: io.vecruntime.spark.expr.VectorExpr,
    filterColumns: Array[Boolean],
    metadataFields: Array[(String, org.apache.spark.sql.types.DataType)],
    metadataExtractors: Map[String, PartitionedFile => Any],
    prefetchMaxBytes: Long,
    prefetchExecutorMaxBytes: Long
) extends RDD[ColumnarBatch](sc, Nil) {

  // The wrapped scan's file partitions (DPP / bucketing / splitting applied). Read lazily inside
  // getPartitions -- not in the constructor -- so a dynamic-partition-pruning subquery has finished before
  // the selected partitions are evaluated (reading them early throws "dynamicpruning has not finished").
  // A non-bucketed scan is planned as Spark plans it without building `scan.inputRDD`, whose Parquet reader
  // broadcasts the whole Hadoop conf a second time on the driver (#672); a bucketed one reuses inputRDD's.
  override protected def getPartitions: Array[Partition] =
    org.apache.spark.sql.execution.vector.FileScanAccess.filePartitions(scan) match {
      case Some(parts) => parts.toArray[Partition]
      case None => scan.inputRDD.partitions
    }

  // The hosts FileScanRDD would prefer: up to three holding the most of the partition's bytes, `localhost`
  // dropped (S3A reports it for every block, so on S3 this is empty and the task NO_PREF, as Spark's).
  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[FilePartition].preferredLocations().toSeq

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val reader = new VectorParquetPartitionReader(
      split.asInstanceOf[FilePartition],
      confBroadcast.value.value,
      requiredSchema,
      partitionSchema,
      attrs,
      batchSize,
      caseSensitive,
      useFieldId,
      pushedFilters,
      pushDownDate,
      pushDownTimestamp,
      pushDownDecimal,
      pushDownStringPredicate,
      pushDownInFilterThreshold,
      rebaseConf,
      prefetchFiles,
      prefetchRowGroups,
      metrics,
      context,
      handOff = decodeAhead > 0,
      // Up to `decodeAhead` queued, one handed out and one being decoded per column.
      poolLimit = math.max(4, decodeAhead + 2),
      dictionaryStrings = dictionaryStrings,
      decodeFilter = decodeFilter,
      filterColumns = filterColumns,
      metadataFields = metadataFields,
      metadataExtractors = metadataExtractors,
      prefetchMaxBytes = prefetchMaxBytes,
      prefetchExecutorMaxBytes = prefetchExecutorMaxBytes,
      reuseDecompressBuffers = reuseDecompressBuffers,
      poolChunkBuffers = poolChunkBuffers
    )
    if (decodeAhead > 0) new DecodeAheadIterator(reader, decodeAhead, decodeAheadVirtual, context) else reader
  }
}

/**
 * Reads one [[FilePartition]] as a stream of columnar batches. Per `PartitionedFile`: opens a
 * `ParquetFileReader`, clips the requested schema, sets the row-group filter, and drives one
 * [[NativeParquetColumnReader]] per data column. Each `batchSize`-row batch is decoded straight into
 * batch-owned Arrow vectors ([[NativeParquetColumnReader.readBatch]], streaming across pages) with the
 * partition-value constant columns appended; the batch owns its vectors and releases them on the next
 * batch / close (data vectors return to the reader's pool).
 */
private[vecruntime] final class VectorParquetPartitionReader(
    partition: FilePartition,
    hadoopConf: Configuration,
    requiredSchema: StructType,
    partitionSchema: StructType,
    attrs: Array[(String, org.apache.spark.sql.types.DataType)],
    batchSize: Int,
    caseSensitive: Boolean,
    useFieldId: Boolean,
    pushedFilters: Seq[org.apache.spark.sql.sources.Filter],
    pushDownDate: Boolean,
    pushDownTimestamp: Boolean,
    pushDownDecimal: Boolean,
    pushDownStringPredicate: Boolean,
    pushDownInFilterThreshold: Int,
    rebaseConf: NativeScanRebaseConf,
    prefetchFiles: Int,
    prefetchRowGroups: Int,
    metrics: ScanMetrics,
    context: TaskContext,
    handOff: Boolean = false, // #606: a DecodeAheadIterator owns release and close
    poolLimit: Int = 4,
    dictionaryStrings: Boolean = false, // #612: dictionary-encoded strings as dictionary vectors
    // #611: a predicate over the output, compiled, decoded first; and which data columns it reads
    decodeFilter: io.vecruntime.spark.expr.VectorExpr = null,
    filterColumns: Array[Boolean] = null,
    // #568 item 9: the file-constant metadata columns after the partition columns, and their extractors
    metadataFields: Array[(String, org.apache.spark.sql.types.DataType)] = Array.empty,
    metadataExtractors: Map[String, PartitionedFile => Any] = Map.empty,
    // #568: the read-ahead's byte caps, per task and over the executor (Long.MaxValue: none)
    prefetchMaxBytes: Long = Long.MaxValue,
    prefetchExecutorMaxBytes: Long = Long.MaxValue,
    // #735: open files with a reused-buffer codec factory (zstd/snappy) and/or a pooled heap chunk allocator.
    reuseDecompressBuffers: Boolean = false,
    poolChunkBuffers: Boolean = false,
    // #735: pushed filters name the requested columns, matched to each file's columns by field id (quoted
    // column name -> field id); null: by name, as Spark does. Iceberg files are never rebased.
    filterFieldIds: Map[String, Int] = null
) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  private val allocator = VectorAllocators.newChild("VectorParquetScanExec")
  private val files: BufferedIterator[PartitionedFile] = partition.files.iterator.buffered
  private val dataColumnCount = requiredSchema.length

  // #568 read-ahead byte cap: the compressed bytes this task's read-ahead holds (files opened ahead, counted at
  // their split's length, and row groups read ahead, at their requested column chunks' size), each queued
  // item's share kept beside it so it is given back when the item is used or dropped. The executor's total
  // is VectorParquetPartitionReader.prefetchInFlight.
  private var aheadBytes = 0L
  private val aheadSizes = new java.util.ArrayDeque[java.lang.Long]()
  private val rgAheadSizes = new java.util.ArrayDeque[java.lang.Long]()
  private var requestedPaths: Set[Seq[String]] = Set.empty // the current file's clipped leaves
  private var currentFile: PartitionedFile = _ // the file being read (its batches carry it, #568)

  /** Whether one more read-ahead of `bytes` fits; one queued item is always allowed (`queued` = 0). */
  private def mayReadAhead(bytes: Long, queued: Int): Boolean =
    queued == 0 || (
      aheadBytes + bytes <= prefetchMaxBytes &&
        VectorParquetPartitionReader.prefetchInFlight.get() + bytes <= prefetchExecutorMaxBytes
    )

  private def reserve(bytes: Long): Unit = {
    aheadBytes += bytes
    val now = VectorParquetPartitionReader.prefetchInFlight.addAndGet(bytes)
    VectorParquetPartitionReader.prefetchPeak.accumulateAndGet(now, math.max)
  }

  private def unreserve(bytes: java.lang.Long): Unit = if (bytes != null) {
    aheadBytes -= bytes
    VectorParquetPartitionReader.prefetchInFlight.addAndGet(-bytes)
  }

  /** Row group `i` (of the current file's, after statistics pruning) at its requested column chunks' size. */
  private def rowGroupBytes(i: Int): Long = {
    val blocks = reader.getRowGroups
    if (i >= blocks.size()) 0L
    else blocks.get(i).getColumns.asScala.filter(c => requestedPaths.contains(c.getPath.toArray.toSeq))
      .map(_.getTotalSize).sum
  }

  // Per-file state.
  private var reader: ParquetFileReader = _
  private var columnReaders: Array[NativeParquetColumn] = _
  private var rowGroupIndex = 0
  private var partitionValues: InternalRow = _ // this file's partition values (for constant columns)
  private val metadataCount = metadataFields.length
  // `_metadata.row_index` (#568 item 9): the temporary row-index column of the requested schema, or -1.
  private val rowIndexOrdinal =
    org.apache.spark.sql.execution.datasources.parquet.ParquetRowIndexUtil.findRowIndexColumnIndexInSchema(
      requiredSchema
    )
  // Constant columns after the data columns: the partition values, then the file-constant metadata (#568 item 9).
  private val constantCount = partitionSchema.length + metadataCount
  private val metadataValues = new Array[Any](metadataCount) // this file's metadata values
  // A fallback file's metadata columns, appended to Spark's batches (on-heap constants; closed with the file).
  private var fallbackMetadata: Array[ColumnVector] = _

  // Current row group state.
  private var rgRows = 0
  private var rgOffset = 0
  // #611: the decode filter is dropped for the rest of a row group after LateDenseBatches batches in a row kept
  // more than LateMaxFraction of their rows: such a batch is decoded whole anyway, and the filter above the
  // scan evaluates the same predicate again, so filtering in the scan only adds a pass. Reset per row group.
  private var lateOff = false
  private var lateDenseRun = 0
  private var emitted: ColumnarBatch = _
  private var emittedVectors: Array[AnyRef] = _ // batch-owned data vectors to release
  private var emittedReaders: Array[NativeParquetColumn] =
    _ // the readers that made them (#612: an id batch must go back to its own)
  private var emittedPartitionVectors: Array[ColumnVector] = _ // batch-owned partition constant columns to close
  private var fallback: Iterator[ColumnarBatch] = _ // Spark's reader for a file we cannot decode

  if (context != null && !handOff) context.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean = {
    // A file whose encodings we cannot decode falls back to Spark's reader for that file.
    if (fallback != null) {
      if (fallback.hasNext) return true
      closeFile()
    }
    // Rows left in the current row group?
    if (rgRows > 0 && rgOffset < rgRows) return true
    // Next row group in the current file, or advance files.
    while (true) {
      if (reader != null) {
        val t0 = System.nanoTime()
        val store = readNextFilteredRowGroup()
        if (store != null) {
          val rgRowCount = store.getRowCount.toInt
          startRowGroup(store, rgRowCount)
          metrics.scanTime += (System.nanoTime() - t0) / 1000000L
          metrics.numRowGroups += 1
          rgRows = rgRowCount
          rgOffset = 0
          lateOff = false
          lateDenseRun = 0
          if (rgRows > 0) return true
        } else {
          metrics.scanTime += (System.nanoTime() - t0) / 1000000L
          closeFile()
        }
      }
      if (reader == null && fallback == null) {
        if (!files.hasNext && ahead.isEmpty) return false
        openNextFile()
        if (fallback != null && fallback.hasNext) return true
      }
    }
    false
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    if (fallback != null) return nextOwned().batch
    releaseEmitted()
    val o = nextOwned()
    emitted = o.batch
    emittedVectors = o.vectors
    emittedReaders = o.readers
    emittedPartitionVectors = o.partitionVectors
    emitted
  }

  /**
   * The next batch together with what it owns, for a consumer that releases batches itself
   * ([[DecodeAheadIterator]], #606): nothing is released here, the caller hands the batch back with
   * [[releaseOwned]]. A batch of Spark's fallback reader is `recycled` -- that reader reuses it on its next
   * call, so it must be consumed before the reader is advanced.
   */
  def nextOwned(): VectorParquetPartitionReader.OwnedBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    if (fallback != null) {
      // Spark's reader owns and recycles its batch; do not wrap or close it here.
      val b = fallback.next()
      metrics.numOutputBatches += 1
      metrics.numOutputRows += b.numRows()
      return new VectorParquetPartitionReader.OwnedBatch(
        withFallbackMetadata(b),
        null,
        null,
        null,
        recycled = true,
        file = currentFile
      )
    }
    val n = math.min(batchSize, rgRows - rgOffset)
    val columns = new Array[ColumnVector](attrs.length)
    val dataVectors = new Array[AnyRef](dataColumnCount)
    // With a decode filter, a column skipped whole is a null vector the batch owns, closed with the constants.
    val partitionVectors =
      new Array[ColumnVector](constantCount + (if (decodeFilter == null) 0 else dataColumnCount))
    var selected: SelectedColumnarBatch = null
    val late = decodeFilter != null && !lateOff
    try {
      var c = 0
      while (c < dataColumnCount) {
        // Decode this batch's rows STRAIGHT into a fresh batch-owned Arrow vector (no row-group vector, no
        // copy of the group, no Arrow getNullCount): the reader streams pages and resumes mid-run. The
        // batch owns the vector; released to the reader's pool on the next batch / close. Self-contained,
        // so a retained or serialized batch ships only its own rows. With a decode filter (#611) only the
        // filter's columns are decoded here; the rest wait for its selection.
        if (!late || filterColumns(c)) {
          val fv = columnReaders(c).readBatch(n)
          dataVectors(c) = fv
          columns(c) = columnReaders(c).wrap(fv)
        } else columns(c) = PlaceholderColumn
        c += 1
      }
      // Partition-value constant columns follow the data columns, ordered to the output: a constant vector
      // of batch length n, owned by the batch (closed on release). They are tiny.
      var p = 0
      while (p < partitionSchema.length) {
        val f = partitionSchema.fields(p)
        val v = partitionValues.get(p, f.dataType)
        val col =
          if (v == null) ArrowOutput.nulls(f.name, f.dataType, n, allocator)
          else ArrowOutput.constant(f.name, f.dataType, v, n, allocator)
        columns(dataColumnCount + p) = col
        partitionVectors(p) = col
        p += 1
      }
      var k = 0
      while (k < metadataCount) {
        val (name, dt) = metadataFields(k)
        val v = metadataValues(k)
        val col =
          if (v == null) ArrowOutput.nulls(name, dt, n, allocator) else ArrowOutput.constant(name, dt, v, n, allocator)
        columns(dataColumnCount + partitionSchema.length + k) = col
        partitionVectors(partitionSchema.length + k) = col
        k += 1
      }
      if (late) selected = applyDecodeFilter(n, columns, dataVectors, partitionVectors)
    } catch {
      case t: Throwable =>
        var k = 0
        while (k < dataColumnCount) {
          if (dataVectors(k) != null) columnReaders(k).release(dataVectors(k))
          k += 1
        }
        var pp = 0
        while (pp < partitionVectors.length) {
          if (partitionVectors(pp) != null) partitionVectors(pp).close()
          pp += 1
        }
        throw t
    }
    rgOffset += n
    val batch = if (selected != null) selected else new ColumnarBatch(columns, n)
    metrics.numOutputBatches += 1
    metrics.numOutputRows += n
    if (inputMetrics != null) inputMetrics.incRecordsRead(n)
    updateBytesRead()
    new VectorParquetPartitionReader.OwnedBatch(
      batch,
      dataVectors,
      partitionVectors,
      columnReaders,
      recycled = false,
      file = currentFile
    )
  }

  /**
   * #611: the decode filter over a batch whose filter columns are decoded: evaluates it, then decodes the
   * other data columns at its survivors only -- in full when every row survives, skipped whole (a null
   * vector) when none does, else at the selected rows ([[NativeParquetColumnReader.readBatchSelected]]).
   * Returns the batch with its selection, or null when every row survives (a plain batch).
   */
  private def applyDecodeFilter(
      n: Int,
      columns: Array[ColumnVector],
      dataVectors: Array[AnyRef],
      owned: Array[ColumnVector]
  ): SelectedColumnarBatch =
    EvalContexts.withBatch(new ColumnarBatch(columns, n)) { ctx =>
      val (selection, count) = io.vecruntime.spark.expr.VectorExpr.selection(decodeFilter.eval(ctx), ctx)
      if (count > n * VectorParquetPartitionReader.LateMaxFraction) {
        lateDenseRun += 1
        if (lateDenseRun >= VectorParquetPartitionReader.LateDenseBatches) lateOff = true
      } else lateDenseRun = 0
      var c = 0
      while (c < dataColumnCount) {
        if (!filterColumns(c)) {
          val r = columnReaders(c)
          if (count == 0 && attrs(c)._2.isInstanceOf[AtomicType]) {
            r.skipRows(n)
            val (name, dt) = attrs(c)
            val v = ArrowOutput.nulls(name, dt, n, allocator)
            owned(constantCount + c) = v
            columns(c) = v
          } else {
            // At the survivors only when they are few and the column is a string: a dense or scattered selection
            // is many short runs, and a run costs a call into the decoder; a fixed-width column is unpacked a
            // whole batch at once for less than that walk. Otherwise the batch is decoded whole and the
            // selection carried.
            val sparse = count <= n * VectorParquetPartitionReader.LateMaxFraction && r.decodesSelectedFaster()
            val fv = if (count == n || !sparse) r.readBatch(n) else r.readBatchSelected(n, selection)
            dataVectors(c) = fv
            columns(c) = r.wrap(fv)
          }
        }
        c += 1
      }
      metrics.numLateSkippedRows += n - count
      // On the heap: under decode-ahead the batch is made on the producer thread and read on the task thread.
      if (count == n) null else SelectedColumnarBatch.ofHeap(columns, n, selection, count, false)
    }

  /** A decode-filtered batch's selection lives in its own arena (its columns are released as usual). */
  private def closeSelection(b: ColumnarBatch): Unit = b match {
    case s: SelectedColumnarBatch => s.close()
    case _ =>
  }

  /** Returns a batch from [[nextOwned]]: its data vectors to their readers' pools, its constants closed. */
  def releaseOwned(o: VectorParquetPartitionReader.OwnedBatch): Unit = if (o != null && !o.recycled) {
    closeSelection(o.batch)
    var c = 0
    while (c < o.vectors.length) {
      // A reader closed since (its file is done) closes the vector instead of pooling it.
      if (o.vectors(c) != null) o.readers(c).release(o.vectors(c))
      c += 1
    }
    var p = 0
    while (p < o.partitionVectors.length) {
      if (o.partitionVectors(p) != null) o.partitionVectors(p).close()
      p += 1
    }
  }

  /**
   * A file opened ahead of use: its reader (which owns the stream), its clipped schema and, when read ahead,
   * its first row group -- or a marker that the file must fall over to Spark's reader.
   */
  private final class Prepared(
      val file: PartitionedFile,
      val reader: ParquetFileReader,
      val clipped: MessageType,
      val firstRead: Boolean,
      val firstStore: org.apache.parquet.column.page.PageReadStore,
      val needsFallback: Boolean,
      val fallbackModes: FallbackModes = null
  )

  // Prefetch (`spark.vecruntime.scan.nativeParquet.prefetchFiles` = N > 0): the next N files of the split
  // are opened -- HEAD, footer, first row group -- on background threads while the current one decodes.
  // TPC-DS store_sales at 1 TB is 14.6k files of ~7 MB (mostly one row group each), so a split is a run of
  // small files and the per-file open + footer + first-group latency was serialized on the task thread
  // (task threads parked on S3 >= 66% of the time; #559/#566). Each Prepared is built entirely by ONE
  // background thread and handed over through its Future, so no ParquetFileReader is ever used by two
  // threads at once.
  private val ahead = new java.util.ArrayDeque[java.util.concurrent.Future[Prepared]]()
  private var installedFirst: org.apache.parquet.column.page.PageReadStore = _
  private var installedFirstPending = false

  private def openNextFile(): Unit = {
    if (context != null) context.killTaskIfInterrupted()
    val depth = prefetchFiles
    if (depth <= 0) {
      install(prepare(files.next(), readFirst = false))
      return
    }
    topUp(depth)
    // Take the open and its size together, before waiting: a failed open (or a kill while waiting) must not
    // leave its size queued, unpaired with any open, where close() would never give it back.
    val f = ahead.poll()
    unreserve(aheadSizes.poll())
    val p = VectorParquetPartitionReader.await(f)
    topUp(depth)
    install(p)
  }

  /**
   * Keeps `depth + 1` files (the one about to be used plus `depth` more) opening in the background, within
   * the read-ahead byte caps (#568).
   */
  private def topUp(depth: Int): Unit = {
    while (ahead.size() < depth + 1 && files.hasNext && mayReadAhead(files.head.length, ahead.size())) {
      val file = files.next()
      reserve(file.length)
      aheadSizes.add(file.length)
      ahead.add(
        java.util.concurrent.CompletableFuture.supplyAsync(
          () => {
            // The open runs for this task: carry its TaskContext onto the prefetch thread.
            onPrefetchThread(prepare(file, readFirst = true))
          },
          VectorParquetPartitionReader.prefetchPool
        )
      )
    }
  }

  /** Closes every file opened ahead and not used (task end, limit). */
  private def drainAhead(): Unit = {
    while (!ahead.isEmpty) {
      val f = ahead.poll()
      unreserve(aheadSizes.poll())
      try {
        val p = f.get()
        if (p.reader != null) p.reader.close()
      } catch { case _: Throwable => () }
    }
  }

  // Field-id matching (#568): Spark's ParquetReadSupport fails a file with no field ids when the read schema has
  // some, unless `fieldId.read.ignoreMissing` is set; such a file goes to Spark's reader, which raises that error.
  private lazy val ignoreMissingFieldIds =
    hadoopConf.getBoolean(SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key, false)
  private lazy val requestedHasFieldIds =
    org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.hasFieldIds(requiredSchema)

  /**
   * Opens one file: touches no reader state, so it may run on a prefetch thread. Returns the reader built
   * over a single stream from the footer just read (the reader owns the stream), or a fallback marker.
   */
  private def prepare(file: PartitionedFile, readFirst: Boolean): Prepared = {
    val path = file.toPath
    val start = file.start
    val end = file.start + file.length
    // One getFileStatus per split, on purpose: the store's own status carries what a status built from the
    // PartitionedFile's length + modification time cannot -- on S3A the etag and version id. Without them
    // the S3 Analytics Accelerator stream (S3A's default) issues its own HEAD per open from its metadata
    // store and does not get the object metadata it keys its reads on: at 1 TB that made q88 131 s against
    // 103 s with this HEAD (bisect on #559). newStream() then opens through fs.openFile(...).withFileStatus
    // (status) with the Parquet read policy, so the open itself issues no second HEAD.
    val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
    // Open the file ONCE: a single SeekableInputStream, read the footer from it with the split range, then
    // build the reader from THAT footer + the SAME stream -- no reopen, no second footer read. The split
    // range keeps the row groups whose midpoint falls in the range, so a file Spark split into several
    // PartitionedFiles reads each row group in exactly one split.
    val rangeOpts = HadoopReadOptions.builder(hadoopConf, path).withRange(start, end).build()
    var stream: SeekableInputStream = null
    var r: ParquetFileReader = null
    try {
      stream = inputFile.newStream()
      val footer: ParquetMetadata = ParquetFileReader.readFooter(inputFile, rangeOpts, stream)
      val fileSchema = footer.getFileMetaData.getSchema
      val clipped = ParquetReadSupport.clipParquetSchema(fileSchema, requiredSchema, caseSensitive, useFieldId, false)
      val modes = fallbackModes(footer)
      // Encoding is only knowable at read time. If any required column chunk uses an encoding we do not
      // decode (BYTE_STREAM_SPLIT on FIXED_LEN_BYTE_ARRAY, ...) or a column's physical type is not its lane's,
      // fall the WHOLE FILE over to Spark's vectorized reader -- with this file's rebase modes -- for correct
      // results, no mid-decode crash. A date or timestamp file in any rebase mode is read natively (#568 item
      // 6): its readers apply the file's LEGACY rebase, EXCEPTION check and INT96 conversion (ParquetRebase).
      if (
        hasUnsupportedEncodingInFooter(footer, clipped) ||
        (useFieldId && !ignoreMissingFieldIds && requestedHasFieldIds && !VectorParquetScanExec.hasFieldIds(
          fileSchema
        )) ||
        (useFieldId && requestedHasFieldIds && VectorParquetScanExec.idNameClash(requiredSchema, clipped))
      ) {
        stream.close()
        stream = null
        return new Prepared(file, null, clipped, false, null, needsFallback = true, modes)
      }
      // The reader owns the stream from here (closed by reader.close()); clear our handle.
      val readOptsBuilder = HadoopReadOptions
        .builder(hadoopConf, path)
        .withRange(start, end)
        .withRecordFilter(rowGroupFilter(clipped, footer))
        // Column-index page filtering (#568): readNextFilteredRowGroup then drops the PAGES outside the filter's row
        // ranges, and the store carries the selected row indexes; each flat leaf follows them (its pages carry
        // their first row index), so every column emits the same selected rows. A repeated leaf (a list or map)
        // counts entries, not rows, so a file with one keeps whole row groups. Parquet's own switch
        // (`parquet.filter.columnindex.enabled`) turns it off. The predicate is still applied above the scan.
        .useColumnIndexFilter(
          hadoopConf.getBoolean(org.apache.parquet.hadoop.ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, true) &&
            clipped.getColumns.asScala.forall(_.getMaxRepetitionLevel == 0)
        )
      // #735: a reused-buffer codec factory (zstd/snappy decompress through one growable byte[] per chunk,
      // read in place by #786) and/or a bounded heap pool for parquet-java's per-row-group chunk buffers.
      io.vecruntime.spark.parquet.NativeParquetReaderOptions.apply(
        readOptsBuilder,
        hadoopConf,
        reuseDecompressBuffers,
        poolChunkBuffers
      )
      val readOpts = readOptsBuilder.build()
      r = ParquetFileReader.open(inputFile, footer, readOpts, stream)
      stream = null
      r.setRequestedSchema(clipped)
      val first = if (readFirst) r.readNextFilteredRowGroup() else null
      val p = new Prepared(file, r, clipped, readFirst, first, needsFallback = false, modes)
      r = null
      p
    } finally {
      if (stream != null) stream.close()
      if (r != null) r.close()
    }
  }

  /** Makes a prepared file the current one (task thread): column readers, partition values, metrics. */
  private def install(p: Prepared): Unit = {
    if (p.needsFallback) {
      fallback = new SparkFallbackFileReader(
        p.file,
        hadoopConf,
        requiredSchema,
        partitionSchema,
        batchSize,
        context,
        p.fallbackModes
      )
      buildPartitionColumns(p.file)
      if (metadataCount > 0) {
        val row = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(metadataValues.map(identity))
        fallbackMetadata = Array.tabulate[ColumnVector](metadataCount) { k =>
          val v = new org.apache.spark.sql.execution.vectorized.ConstantColumnVector(batchSize, metadataFields(k)._2)
          org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.populate(v, row, k)
          v
        }
      }
      metrics.numFiles += 1
      return
    }
    reader = p.reader
    requestedPaths = p.clipped.getColumns.asScala.map(_.getPath.toSeq).toSet
    rowGroupIndex = 0
    installedFirst = p.firstStore
    installedFirstPending = p.firstRead
    val columns = p.clipped.getColumns
    val fileSchema = p.reader.getFileMetaData.getSchema
    // #568 item 6: this file's calendar rebase and INT96 conversion, for every date and timestamp leaf.
    val m = p.fallbackModes
    val fileRebase =
      if (m == null) io.vecruntime.spark.parquet.ParquetRebase.CORRECTED
      else
        new io.vecruntime.spark.parquet.ParquetRebase(m.datetimeMode, m.datetimeTz, m.int96Mode, m.int96Tz, m.convertTz)
    columnReaders = new Array[NativeParquetColumn](dataColumnCount)
    // Map each required data column to its clipped ColumnDescriptor by leaf name; a struct (#568) to its group.
    var i = 0
    while (i < dataColumnCount) {
      val field = requiredSchema.fields(i)
      columnReaders(i) = field.dataType match {
        case _ if i == rowIndexOrdinal =>
          new io.vecruntime.spark.parquet.NativeParquetRowIndexColumn(field.name, allocator)
        case dt if !NativeParquetStructReader.holds(fileSchema, Array(clippedName(p.clipped, i))) =>
          // #568 schema evolution: a column added since the file was written, null in every row.
          new io.vecruntime.spark.parquet.NativeParquetNullColumn(field.name, dt, allocator)
        case st: StructType =>
          val name = clippedName(p.clipped, i)
          NativeParquetStructReader.create(p.clipped, fileSchema, Array(name), st, field.name, batchSize, allocator)
        case dt @ (_: org.apache.spark.sql.types.ArrayType | _: org.apache.spark.sql.types.MapType) =>
          NativeParquetListReader.create(
            p.clipped,
            fileSchema,
            Array(clippedName(p.clipped, i)),
            dt,
            field.name,
            batchSize,
            allocator
          )
        case dt =>
          val leafName = clippedName(p.clipped, i)
          val cd = columns.asScala.find(_.getPath()(0).equals(leafName))
            .getOrElse(throw new IllegalStateException(s"column ${field.name} missing from clipped schema"))
          new NativeParquetColumnReader(
            cd,
            io.vecruntime.spark.parquet.NativeParquetSupport.decodeLane(dt),
            dt,
            field.name,
            batchSize,
            allocator
          )
      }
      columnReaders(i).setPoolLimit(poolLimit)
      columnReaders(i).setDictionaryIds(dictionaryStrings)
      columnReaders(i).setRebase(fileRebase)
      // #735: a repeated leaf must copy a page out of the decompressor's reused buffer (it queues several).
      columnReaders(i).setReuseDecompress(reuseDecompressBuffers)
      i += 1
    }
    buildPartitionColumns(p.file)
    metrics.numFiles += 1
  }

  /**
   * Spark's batch for a fallback file, with this file's metadata columns after its partition columns (Spark's
   * reader emits data and partition columns only; FileScanRDD appends the metadata the same way).
   */
  private def withFallbackMetadata(b: ColumnarBatch): ColumnarBatch =
    if (metadataCount == 0) b
    else {
      val cols = new Array[ColumnVector](b.numCols() + metadataCount)
      var i = 0
      while (i < b.numCols()) {
        cols(i) = b.column(i)
        i += 1
      }
      System.arraycopy(fallbackMetadata, 0, cols, b.numCols(), metadataCount)
      new ColumnarBatch(cols, b.numRows())
    }

  /** The clipped schema's own name for the top-level column `name` (the file's spelling of it). */
  /**
   * The clipped name of requested data column `ordinal`. `clipParquetSchema` keeps the requested order, so it is
   * the clipped schema's field at the same position -- under field-id matching (#568) the file's own name, which
   * may differ from the requested one, or a generated name for an id the file lacks.
   */
  private def clippedName(clipped: MessageType, ordinal: Int): String =
    if (clipped.getFieldCount == requiredSchema.length) clipped.getFields.get(ordinal).getName
    else topLevelName(clipped, requiredSchema.fields(ordinal).name)

  private def topLevelName(clipped: MessageType, name: String): String =
    clipped.getFields.asScala.find(_.getName.equalsIgnoreCase(name)).map(_.getName).getOrElse(name)

  /** Record this file's partition values; per-batch constant columns are built from them in next(). */
  private def buildPartitionColumns(file: PartitionedFile): Unit = {
    partitionValues = file.partitionValues
    // input_file_name() and its siblings read the task thread's holder, as FileScanRDD sets it per file. Under
    // decode-ahead (handOff) this runs on the producer thread, whose holder is the task thread's own
    // (InputFileBlockHolder is inheritable): DecodeAheadIterator sets it from each batch's file as the task
    // thread takes the batch, so it names the file of the rows being consumed, not the one decoded ahead.
    currentFile = file
    if (!handOff) org.apache.spark.rdd.InputFileBlockHolder.set(file.urlEncodedPath, file.start, file.length)
    if (metadataCount > 0) {
      val row = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(metadataCount)
      org.apache.spark.sql.vecruntime.shims.SparkShims.fileConstantMetadata(
        row,
        metadataFields.map(_._1).toSeq,
        metadataFields.map(_._2).toSeq,
        file,
        metadataExtractors
      )
      var k = 0
      while (k < metadataCount) {
        metadataValues(k) = row.get(k, metadataFields(k)._2)
        k += 1
      }
    }
  }

  private def startRowGroup(store: org.apache.parquet.column.page.PageReadStore, rowCount: Int): Unit = {
    var c = 0
    while (c < dataColumnCount) {
      columnReaders(c).startRowGroup(store, rowCount)
      c += 1
    }
  }

  private def readNextFilteredRowGroup(): org.apache.parquet.column.page.PageReadStore = {
    val store =
      if (installedFirstPending) {
        // The first row group was read when the file was opened ahead.
        installedFirstPending = false
        val s = installedFirst
        installedFirst = null
        s
      } else if (!rgAhead.isEmpty) {
        unreserve(rgAheadSizes.poll())
        VectorParquetPartitionReader.await(rgAhead.poll())
      } else if (rgExhausted) {
        null
      } else {
        reader.readNextFilteredRowGroup()
      }
    rowGroupIndex += 1
    if (store == null) {
      // The file is exhausted: every read chained behind this one returns null too (see topUpRowGroups).
      rgExhausted = true
      drainRowGroups()
    } else if (prefetchRowGroups > 0) {
      topUpRowGroups(prefetchRowGroups)
    }
    store
  }

  // Row groups read ahead within the current file (a file with several row groups): a chain of
  // CompletableFutures, each stage reading the NEXT row group after the previous stage completed, so the
  // ParquetFileReader is still used by one thread at a time and in order. A store is in-memory compressed
  // pages; decompression and decode stay on the task thread, in row-group order.
  private val rgAhead =
    new java.util.ArrayDeque[java.util.concurrent.CompletableFuture[org.apache.parquet.column.page.PageReadStore]]()
  private var rgTail: java.util.concurrent.CompletableFuture[org.apache.parquet.column.page.PageReadStore] = _
  private var rgExhausted = false

  /**
   * Keeps `depth` row groups of the current file reading ahead, chained so the reads stay sequential, within
   * the read-ahead byte caps (#568). The next chained read is row group `rowGroupIndex + rgAhead.size()`.
   */
  private def topUpRowGroups(depth: Int): Unit = {
    val r = reader
    val pool = VectorParquetPartitionReader.prefetchPool
    while (
      rgAhead.size() < depth && !rgExhausted &&
      mayReadAhead(rowGroupBytes(rowGroupIndex + rgAhead.size()), rgAhead.size())
    ) {
      val bytes = rowGroupBytes(rowGroupIndex + rgAhead.size())
      reserve(bytes)
      rgAheadSizes.add(bytes)
      val next =
        if (rgTail == null) {
          java.util.concurrent.CompletableFuture.supplyAsync(() => readWithTask(r), pool)
        } else {
          // A null (end of file) or a failure ends the chain: later stages do not touch the reader again.
          rgTail.thenApplyAsync(
            (prev: org.apache.parquet.column.page.PageReadStore) => if (prev == null) null else readWithTask(r),
            pool
          )
        }
      rgAhead.add(next)
      rgTail = next
    }
  }

  private def readWithTask(r: ParquetFileReader): org.apache.parquet.column.page.PageReadStore =
    onPrefetchThread(r.readNextFilteredRowGroup())

  /**
   * Runs one prefetched step for this task on a prefetch thread: the task's TaskContext is set for it, and
   * the FileSystem bytes it reads (Hadoop counts them per thread, so the task thread's own callback cannot
   * see them) are added to the task's input metrics.
   */
  private def onPrefetchThread[T](step: => T): T = {
    TaskContext.setTaskContext(context)
    val bytesOnThisThread = SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
    try step
    finally {
      prefetchedBytes.addAndGet(bytesOnThisThread())
      TaskContext.unset()
    }
  }

  // The task's input metrics, kept the way FileScanRDD keeps them: records per batch, and bytes as the task
  // thread's FileSystem read count plus what the prefetch threads read for this task.
  private val inputMetrics = if (context != null) context.taskMetrics().inputMetrics else null
  private val existingBytesRead = if (inputMetrics != null) inputMetrics.bytesRead else 0L
  private val taskThreadBytes: () => Long = SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
  private val prefetchedBytes = new java.util.concurrent.atomic.AtomicLong()

  private def updateBytesRead(): Unit =
    if (inputMetrics != null) inputMetrics.setBytesRead(existingBytesRead + taskThreadBytes() + prefetchedBytes.get())

  /** Waits for every row-group read in flight (results dropped) so the reader can be closed safely. */
  private def drainRowGroups(): Unit = {
    if (rgTail != null) {
      try rgTail.join()
      catch { case _: Throwable => () }
    }
    rgAhead.clear()
    while (!rgAheadSizes.isEmpty) unreserve(rgAheadSizes.poll())
    rgTail = null
  }

  /**
   * Whether the native readers fill a clipped leaf the file does not hold with nulls as Spark's reader does
   * (#568 schema evolution). Its shallowest missing node must be:
   *   - a top-level column, or a field of a struct at any depth (inside a list's or a map's struct too, which the
   *     tree reader fills; the parent group is a struct when it carries no LIST or MAP annotation);
   *   - optional (Spark fails a REQUIRED missing column: "Required column is missing in data file");
   *   - when top level, a column without an existence default (Spark fills that default instead);
   *   - when a struct field, in a struct the file holds some other field of (its nulls come from that field;
   *     when it holds none, Spark 4.1 reads an extra unrequested field, which our readers do not).
   */
  private def missingReadable(clipped: MessageType, file: MessageType, path: Array[String]): Boolean = {
    var k = 0
    while (k < path.length && NativeParquetStructReader.holds(file, path.take(k + 1))) k += 1
    if (k == path.length) return true // held after all
    var t: org.apache.parquet.schema.Type = clipped
    var j = 0
    while (j < k) {
      t = t.asGroupType().getType(path(j))
      if (t.isPrimitive) return false
      j += 1
    }
    val parent = t.asGroupType()
    if (k > 0 && parent.getLogicalTypeAnnotation != null) return false
    val missing = parent.getType(path(k))
    if (!missing.isRepetition(org.apache.parquet.schema.Type.Repetition.OPTIONAL)) return false
    if (k == 0) {
      val ordinal = clipped.getFieldIndex(path(0))
      ordinal >= requiredSchema.length || !requiredSchema.fields(ordinal).metadata.contains(
        org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY
      )
    } else {
      parent.getFields.asScala.exists(f => NativeParquetStructReader.holds(file, path.take(k) :+ f.getName))
    }
  }

  /**
   * True if any required column chunk in any row group uses a value encoding the native decoder does not
   * support (PLAIN and dictionary; the v2 encodings on the physical types in `supportsEncoding`). Read from
   * the footer's column-chunk metadata, so the decision is made once per file at open time -- a
   * BYTE_STREAM_SPLIT FIXED_LEN_BYTE_ARRAY column, say, falls the file over to Spark's reader with no
   * mid-decode failure.
   */
  private def hasUnsupportedEncodingInFooter(footer: ParquetMetadata, clipped: MessageType): Boolean = {
    val wanted = new java.util.HashSet[String]()
    clipped.getColumns.forEach(cd => wanted.add(cd.getPath()(0).toLowerCase(java.util.Locale.ROOT)))
    // The planner admits a column by its Spark type; the file decides how it is stored. A decimal(p <= 18) may
    // be INT32, INT64, FIXED_LEN_BYTE_ARRAY or BYTE_ARRAY in a given file, and only the first two have a
    // decode path today -- the bytes of the others are not an INT64 lane. Check every requested column's
    // physical type against its lane, per file, before any decode.
    var f = 0
    while (f < requiredSchema.length) {
      val field = requiredSchema.fields(f)
      field.dataType match {
        case _ if f == rowIndexOrdinal => // generated from the row group's offset, not stored
        case _
            if !NativeParquetStructReader.holds(
              footer.getFileMetaData.getSchema,
              Array(clippedName(clipped, f))
            ) => // not in the file: a null column when missingReadable admits it (below)
        case st: StructType =>
          // #568: every leaf of the struct, in the clipped group's order, stored as its lane needs (a leaf the file
          // lacks is a null column: missingReadable, below).
          var ok = true
          val file = footer.getFileMetaData.getSchema
          val shape = NativeParquetStructReader.forEachLeaf(
            clipped,
            file,
            Array(clippedName(clipped, f)),
            st,
            (cd: org.apache.parquet.column.ColumnDescriptor, dt: org.apache.spark.sql.types.DataType) =>
              if (file.containsPath(cd.getPath) && !VectorParquetScanExec.physicalMatches(dt, cd.getPrimitiveType))
                ok = false
          )
          if (!shape || !ok) return true
        case dt @ (_: org.apache.spark.sql.types.ArrayType | _: org.apache.spark.sql.types.MapType) =>
          // #568: a list or map of leaves, each leaf repeated once and stored as its lane needs.
          var ok = true
          val file = footer.getFileMetaData.getSchema
          val shape = NativeParquetListReader.forEachLeaf(
            clipped,
            file,
            Array(clippedName(clipped, f)),
            dt,
            (cd: org.apache.parquet.column.ColumnDescriptor, leafType: org.apache.spark.sql.types.DataType) =>
              if (
                file.containsPath(cd.getPath) && !VectorParquetScanExec.physicalMatches(leafType, cd.getPrimitiveType)
              )
                ok = false
          )
          if (!shape || !ok) return true
        case dt =>
          val leafName = clippedName(clipped, f)
          val cd = clipped.getColumns.asScala.find(_.getPath()(0).equals(leafName))
          if (cd.isDefined && !VectorParquetScanExec.physicalMatches(dt, cd.get.getPrimitiveType)) {
            return true
          }
      }
      f += 1
    }
    // A requested column the file does not hold (a field added since the file was written): clipping adds it to
    // the requested schema with no column chunk behind it. Spark's reader fills it with nulls, and so do ours
    // (#568 schema evolution, NativeParquetNullColumn) where the missing node is a top-level column or a field of
    // structs only; anything else the file lacks falls over (missingReadable).
    val fileSchema = footer.getFileMetaData.getSchema
    // The row-index column is generated (NativeParquetRowIndexColumn), never stored.
    val rowIndexName = if (rowIndexOrdinal >= 0) requiredSchema.fields(rowIndexOrdinal).name else null
    if (
      clipped.getColumns.asScala.exists(cd =>
        !fileSchema.containsPath(cd.getPath) &&
          !(cd.getPath.length == 1 && cd.getPath()(0).equalsIgnoreCase(rowIndexName)) &&
          !missingReadable(clipped, fileSchema, cd.getPath)
      )
    ) return true
    val blocks = footer.getBlocks
    var b = 0
    while (b < blocks.size()) {
      val cols = blocks.get(b).getColumns
      var c = 0
      while (c < cols.size()) {
        val cc = cols.get(c)
        val leaf = cc.getPath.toArray()(0).toLowerCase(java.util.Locale.ROOT)
        if (wanted.contains(leaf)) {
          val it = cc.getEncodings.iterator()
          while (it.hasNext) {
            if (!VectorParquetScanExec.supportsEncoding(it.next().name(), cc.getPrimitiveType.getPrimitiveTypeName)) {
              return true
            }
          }
        }
        c += 1
      }
      b += 1
    }
    false
  }

  /** Whether any requested column holds dates or timestamps: the only columns a calendar rebase touches. */
  /**
   * This file's rebase modes and INT96 time-zone conversion, resolved as Spark's `ParquetFileFormat` resolves
   * them: from the writer's version and legacy-calendar keys in the footer, else the session configuration.
   * The native path reads a datetime file only when its mode is CORRECTED; the fallback reader is built with
   * these modes, so a LEGACY or EXCEPTION file gets Spark's own rebase (or Spark's own error).
   */
  private def fallbackModes(footer: ParquetMetadata): FallbackModes = {
    val kv = footer.getFileMetaData.getKeyValueMetaData
    val lookup: String => String = k => kv.get(k)
    val dt =
      org.apache.spark.sql.execution.datasources.DataSourceUtils.datetimeRebaseSpec(lookup, rebaseConf.datetimeMode)
    val i96 = org.apache.spark.sql.execution.datasources.DataSourceUtils.int96RebaseSpec(lookup, rebaseConf.int96Mode)
    val createdBy = footer.getFileMetaData.getCreatedBy
    val convertTz =
      if (rebaseConf.int96Convert && (createdBy == null || !createdBy.startsWith("parquet-mr"))) rebaseConf.sessionTz
      else null
    FallbackModes(
      dt.mode.toString,
      dt.originTimeZone.getOrElse(rebaseConf.sessionTz),
      i96.mode.toString,
      i96.originTimeZone.getOrElse(rebaseConf.sessionTz),
      convertTz
    )
  }

  private def rowGroupFilter(clipped: MessageType, footer: ParquetMetadata): FilterCompat.Filter = {
    if (pushedFilters.isEmpty) {
      return FilterCompat.NOOP
    }
    // The file's own datetime rebase spec (#568 item 6), as ParquetFileFormat builds its ParquetFilters: a
    // LEGACY file's date and timestamp statistics are in the hybrid calendar, so the filter values are rebased
    // to match them.
    val kv = footer.getFileMetaData.getKeyValueMetaData
    val rebaseSpec =
      org.apache.spark.sql.execution.datasources.DataSourceUtils.datetimeRebaseSpec(
        // #735: Iceberg writes proleptic dates and timestamps and its readers never rebase.
        if (filterFieldIds != null) (_: String) => null else (k: String) => kv.get(k),
        if (filterFieldIds != null) "CORRECTED" else rebaseConf.datetimeMode
      )
    // #735: by field id, each filter renamed to the file's own column with that id; a filter on a column the
    // file lacks is dropped (see renameByFieldId).
    val filters =
      if (filterFieldIds == null) pushedFilters
      else VectorParquetScanExec.renameByFieldId(pushedFilters, filterFieldIds, footer.getFileMetaData.getSchema)
    if (filters.isEmpty) {
      return FilterCompat.NOOP
    }
    // Over the FILE's schema, as ParquetFileFormat builds its ParquetFilters: a column the file lacks (schema
    // evolution) or holds under another name (field-id matching, #568) gets no row-group predicate, as in Spark.
    val parquetFilters = new ParquetFilters(
      footer.getFileMetaData.getSchema,
      pushDownDate,
      pushDownTimestamp,
      pushDownDecimal,
      pushDownStringPredicate,
      pushDownInFilterThreshold,
      caseSensitive,
      rebaseSpec
    )
    val predicates = filters.flatMap(f => parquetFilters.createFilter(f))
    if (predicates.isEmpty) {
      FilterCompat.NOOP
    } else {
      FilterCompat.get(predicates.reduce((a, b) => org.apache.parquet.filter2.predicate.FilterApi.and(a, b)))
    }
  }

  private def releaseEmitted(): Unit = if (emitted != null) {
    closeSelection(emitted)
    // Return the batch's owned data vectors to their reader's pool for reuse. Do NOT close the ColumnarBatch
    // (that would also close the file-owned partition constant columns / their views); the data vectors are
    // released here, the partition constants live until closeFile.
    if (emittedVectors != null) {
      var c = 0
      while (c < dataColumnCount) {
        if (emittedVectors(c) != null) {
          // Back to the reader that made it: its pool, or closed if that reader (its file) is closed.
          emittedReaders(c).release(emittedVectors(c))
        }
        c += 1
      }
      emittedVectors = null
    }
    if (emittedPartitionVectors != null) {
      var p = 0
      while (p < emittedPartitionVectors.length) {
        if (emittedPartitionVectors(p) != null) emittedPartitionVectors(p).close()
        p += 1
      }
      emittedPartitionVectors = null
    }
    emitted = null
  }

  private def closeFile(): Unit = {
    if (fallbackMetadata != null) {
      fallbackMetadata.foreach(_.close())
      fallbackMetadata = null
    }
    if (fallback != null) {
      fallback match {
        case c: AutoCloseable => c.close()
        case _ =>
      }
      fallback = null
    }
    if (columnReaders != null) {
      columnReaders.foreach(r => if (r != null) r.close())
      columnReaders = null
    }
    if (reader != null) {
      drainRowGroups()
      reader.close()
      reader = null
    }
    installedFirst = null
    installedFirstPending = false
    rgExhausted = false
    rgRows = 0
    rgOffset = 0
  }

  override def close(): Unit = {
    try {
      releaseEmitted()
      closeFile()
    } finally {
      try drainAhead()
      finally {
        // A closed reader holds no read-ahead bytes, whatever path the task took (#568).
        while (!aheadSizes.isEmpty) unreserve(aheadSizes.poll())
        while (!rgAheadSizes.isEmpty) unreserve(rgAheadSizes.poll())
        if (aheadBytes != 0) {
          VectorParquetPartitionReader.prefetchInFlight.addAndGet(-aheadBytes)
          aheadBytes = 0
        }
      }
    }
    org.apache.spark.rdd.InputFileBlockHolder.unset()
    updateBytesRead()
    allocator.close()
  }
}

private[vecruntime] object VectorParquetPartitionReader {

  /** #611: the largest surviving fraction a batch is decoded at its survivors for; `vecruntime.lateMaterialization.maxFraction`. */
  val LateMaxFraction: Double =
    java.lang.Double.parseDouble(System.getProperty("vecruntime.lateMaterialization.maxFraction", "0.05"))

  /** #611: dense batches in a row that turn the decode filter off for the rest of the row group; `vecruntime.lateMaterialization.denseBatches`. */
  val LateDenseBatches: Int = Integer.getInteger("vecruntime.lateMaterialization.denseBatches", 2)

  /**
   * A decoded batch with what it owns (#606): its data vectors (each returned to `readers(c)`'s pool) and
   * partition constant columns (closed), or a `recycled` batch of Spark's fallback reader that owns nothing
   * and is reused by that reader's next call.
   */
  final class OwnedBatch(
      val batch: ColumnarBatch,
      val vectors: Array[AnyRef],
      val partitionVectors: Array[ColumnVector],
      val readers: Array[io.vecruntime.spark.parquet.NativeParquetColumn],
      val recycled: Boolean,
      // #568: the file the batch was read from; DecodeAheadIterator sets InputFileBlockHolder from it on the task
      // thread, as the batches are handed out (the files are opened on the producer thread)
      val file: PartitionedFile = null
  )

  /**
   * The prefetch threads (`prefetchFiles` or `prefetchRowGroups` > 0): daemon platform threads, cached and
   * reused, one per file open / row-group read in flight, so the number alive is bounded by the executor's
   * task slots times each reader's depth rather than by a pool size.
   *
   * Not virtual threads: a file open runs Parquet's and the S3 client's class initializers and loggers, and on
   * JDK 25 a virtual thread blocked in a class initializer (or waiting on one) still pins its carrier. On
   * 1 TB TPC-DS q88 every carrier of one executor ended up pinned behind `BloomFilterImpl.<clinit>`, which
   * waited for a log4j lock held by an unmounted virtual thread that could no longer get a carrier: the
   * executor's 13 tasks hung with no CPU use. Platform threads cannot starve that way.
   */
  lazy val prefetchPool: java.util.concurrent.ExecutorService = {
    val count = new java.util.concurrent.atomic.AtomicInteger()
    java.util.concurrent.Executors.newCachedThreadPool { (r: Runnable) =>
      val t = new Thread(r, s"vecruntime-parquet-prefetch-${count.getAndIncrement()}")
      t.setDaemon(true)
      t
    }
  }

  /**
   * #568: the compressed bytes every task's read-ahead holds in this executor (files opened ahead and row
   * groups read ahead, not yet used), against `prefetchExecutorMaxBytes`; and its peak (read by tests).
   */
  val prefetchInFlight = new java.util.concurrent.atomic.AtomicLong()
  val prefetchPeak = new java.util.concurrent.atomic.AtomicLong()

  /** The value of a prefetched step; its failure is rethrown as the original exception. */
  def await[T](f: java.util.concurrent.Future[T]): T =
    try f.get()
    catch { case e: java.util.concurrent.ExecutionException => throw e.getCause }
}

object VectorParquetScanExec {

  /** #568: the executor's read-ahead bytes in flight and their peak (tests read these; resetPrefetchPeak). */
  def prefetchInFlightBytes: Long = VectorParquetPartitionReader.prefetchInFlight.get()
  def prefetchPeakBytes: Long = VectorParquetPartitionReader.prefetchPeak.get()
  def resetPrefetchPeak(): Unit = VectorParquetPartitionReader.prefetchPeak.set(0)

  /** The Parquet value/level encodings the native decoder handles; any other encoding falls the file over. */
  val SupportedEncodings: Set[String] = Set("PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED")

  import PrimitiveType.PrimitiveTypeName.{BINARY, DOUBLE, FIXED_LEN_BYTE_ARRAY, INT32, INT64}

  /**
   * Encodings the native decoder handles only on some physical types (#559): DELTA_BINARY_PACKED on
   * INT32/INT64, DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY on BINARY (the UTF8 lane), BYTE_STREAM_SPLIT on INT32/INT64/DOUBLE
   * (not FLOAT or FIXED_LEN_BYTE_ARRAY, which have no lane).
   */
  private val TypedEncodings: Map[String, Set[PrimitiveType.PrimitiveTypeName]] = Map(
    "DELTA_BINARY_PACKED" -> Set(INT32, INT64),
    "DELTA_LENGTH_BYTE_ARRAY" -> Set(BINARY),
    "DELTA_BYTE_ARRAY" -> Set(BINARY, FIXED_LEN_BYTE_ARRAY),
    // #812: FIXED_LEN_BYTE_ARRAY too, put back into PLAIN order per page by the decoder.
    "BYTE_STREAM_SPLIT" -> Set(INT32, INT64, DOUBLE, PrimitiveType.PrimitiveTypeName.FLOAT, FIXED_LEN_BYTE_ARRAY)
  )

  /**
   * #735: `filters`, written over the requested columns (`ids`: quoted name -> field id), renamed to the columns
   * of `fileSchema` (top level) carrying those ids -- so after a rename, or a dropped column whose name a new
   * column took, a filter is tested against the column the reader reads, never one that merely shares its name.
   * A top-level conjunct naming a column the file lacks (or holds under a repeated id) is dropped: the column
   * reads as nulls there and the filter above the scan decides. Inside `Or` / `Not` nothing is dropped: such a
   * filter is renamed whole or not pushed. The result only skips row groups, so dropping is always safe.
   */
  def renameByFieldId(
      filters: Seq[org.apache.spark.sql.sources.Filter],
      ids: Map[String, Int],
      fileSchema: MessageType
  ): Seq[org.apache.spark.sql.sources.Filter] = {
    import org.apache.spark.sql.sources._
    val fields: Seq[org.apache.parquet.schema.Type] = fileSchema.getFields.asScala.toSeq.filter(_.getId != null)
    val byId: Map[Int, String] = fields.groupBy(_.getId.intValue).collect {
      case (id, ts) if ts.size == 1 => id -> org.apache.spark.sql.catalyst.util.quoteIfNeeded(ts.head.getName)
    }
    val names: Map[String, String] = ids.flatMap { case (n, id) => byId.get(id).map(n -> _) }
    def rename(f: Filter): Option[Filter] = {
      def col(n: String)(make: String => Filter): Option[Filter] = names.get(n).map(make)
      f match {
        case EqualTo(a, v) => col(a)(EqualTo(_, v))
        case EqualNullSafe(a, v) => col(a)(EqualNullSafe(_, v))
        case GreaterThan(a, v) => col(a)(GreaterThan(_, v))
        case GreaterThanOrEqual(a, v) => col(a)(GreaterThanOrEqual(_, v))
        case LessThan(a, v) => col(a)(LessThan(_, v))
        case LessThanOrEqual(a, v) => col(a)(LessThanOrEqual(_, v))
        case In(a, vs) => col(a)(In(_, vs))
        case IsNull(a) => col(a)(IsNull(_))
        case IsNotNull(a) => col(a)(IsNotNull(_))
        case StringStartsWith(a, v) => col(a)(StringStartsWith(_, v))
        case StringEndsWith(a, v) => col(a)(StringEndsWith(_, v))
        case StringContains(a, v) => col(a)(StringContains(_, v))
        case And(l, r) => for (x <- rename(l); y <- rename(r)) yield And(x, y)
        case Or(l, r) => for (x <- rename(l); y <- rename(r)) yield Or(x, y)
        case Not(c) => rename(c).map(Not(_))
        case AlwaysTrue() | AlwaysFalse() => Some(f)
        case _ => None
      }
    }
    def conjuncts(f: Filter): Seq[Filter] = f match {
      case And(l, r) => conjuncts(l) ++ conjuncts(r)
      case other => Seq(other)
    }
    filters.flatMap(conjuncts).flatMap(rename)
  }

  /** Whether any field of a Parquet schema, at any depth, carries a field id (Spark's ParquetReadSupport check). */
  def hasFieldIds(t: org.apache.parquet.schema.Type): Boolean =
    (t.getId != null && !t.isInstanceOf[MessageType]) ||
      (!t.isPrimitive && t.asGroupType().getFields.asScala.exists(hasFieldIds))

  /**
   * Under field-id matching (#568): whether some clipped field, matched by id, carries the name of a DIFFERENT
   * requested sibling. Spark's reader pairs the clipped fields with the read schema by name after clipping by id
   * (ParquetToSparkSchemaConverter), so it reads such a field as that sibling's type -- a type mismatch, or
   * another column's values. The native readers pair by position, so such a file goes to Spark's reader for
   * Spark's outcome. Walks structs, and lists' and maps' structs, as clipped (Spark's three- and two-level lists).
   */
  def idNameClash(dt: org.apache.spark.sql.types.DataType, t: org.apache.parquet.schema.Type): Boolean = {
    import org.apache.spark.sql.types._
    if (t.isPrimitive) return false
    val g = t.asGroupType()
    dt match {
      case st: StructType =>
        if (g.getFieldCount != st.length) return false
        st.fields.indices.exists { i =>
          val name = g.getType(i).getName
          (!name.equalsIgnoreCase(st.fields(i).name) &&
            st.fields.exists(_.name.equalsIgnoreCase(name))) || idNameClash(st.fields(i).dataType, g.getType(i))
        }
      case at: ArrayType if g.getFieldCount == 1 =>
        val r = g.getType(0)
        val element =
          if (
            !r.isPrimitive && r.asGroupType().getFieldCount == 1 && r.getName != "array" &&
            r.getName != g.getName + "_tuple"
          ) r.asGroupType().getType(0)
          else r
        idNameClash(at.elementType, element)
      case mt: MapType
          if g.getFieldCount == 1 && !g.getType(0).isPrimitive && g.getType(0).asGroupType().getFieldCount == 2 =>
        val kv = g.getType(0).asGroupType()
        idNameClash(mt.keyType, kv.getType(0)) || idNameClash(mt.valueType, kv.getType(1))
      case _ => false
    }
  }

  /**
   * True if a file column stored as `physical` decodes into the lane of the Spark type `dt`: INT32 for int and
   * date (and widened into a bigint lane); INT64 for bigint; DOUBLE; BOOLEAN; BINARY for string; INT32 / INT64
   * for a decimal with precision <= 18 (a FIXED_LEN_BYTE_ARRAY or BYTE_ARRAY decimal is not an INT64 lane).
   * Any other pairing makes the file fall over to Spark's reader.
   */
  def physicalMatches(dt: org.apache.spark.sql.types.DataType, physical: PrimitiveType): Boolean = {
    import org.apache.spark.sql.types._
    val p = physical.getPrimitiveTypeName
    dt match {
      case IntegerType | DateType | ByteType | ShortType => p == INT32
      case LongType =>
        // INT32 widens by sign extension; an unsigned INT32 (UINT_32, which Spark reads as a long) by zero
        // extension (#812, Spark's UnsignedIntegerUpdater).
        p == INT64 || p == INT32
      // #812: ANSI intervals, as Spark's updaters read them: year-month on INT32, day-time on INT64 (micros).
      case _: YearMonthIntervalType => p == INT32
      case _: DayTimeIntervalType => p == INT64
      case TimestampType =>
        // INT64 MICROS or MILLIS, as Spark's ParquetVectorUpdaterFactory matches them (either isAdjustedToUTC), and
        // INT96 (#568 item 6: converted at flush, with the file's INT96 rebase and time-zone conversion). NANOS
        // falls over per file.
        p == PrimitiveType.PrimitiveTypeName.INT96 || p == INT64 && (physical.getLogicalTypeAnnotation match {
          case t: org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation =>
            t.getUnit == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS ||
            t.getUnit == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS
          case _ => false
        })
      case DoubleType =>
        // #568 schema evolution: Spark's IntegerToDoubleUpdater (any INT32) and FloatToDoubleUpdater.
        p == DOUBLE || p == INT32 || p == PrimitiveType.PrimitiveTypeName.FLOAT
      // Columns without an engine lane (#559, option A): the scan decodes them on a lane of the same layout.
      case FloatType => p == PrimitiveType.PrimitiveTypeName.FLOAT
      case BinaryType => p == BINARY || p == FIXED_LEN_BYTE_ARRAY
      case TimestampNTZType =>
        // INT64 MICROS or MILLIS not adjusted to UTC (Spark refuses a UTC-adjusted column as TIMESTAMP_NTZ), or a
        // DATE (#568 schema evolution: Spark's DateToTimestampNTZUpdater, midnight UTC after the date rebase).
        (p == INT32 && physical.getLogicalTypeAnnotation
          .isInstanceOf[org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation]) ||
        p == INT64 && (physical.getLogicalTypeAnnotation match {
          case t: org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation =>
            !t.isAdjustedToUTC && (t.getUnit == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS ||
              t.getUnit == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS)
          case _ => false
        })
      case BooleanType => p == PrimitiveType.PrimitiveTypeName.BOOLEAN
      case _: StringType => p == BINARY
      case d: DecimalType =>
        // Spark's isDecimalTypeMatched: the file's decimal (p, s) reads as (p', s') when s' >= s and the precision
        // grows by at least as much as the scale (#568 schema evolution: the unscaled values times 10^(s' - s));
        // an un-annotated or signed-integer INT32 / INT64 reads as a decimal with room for its integer digits
        // (10 / 20). INT32 and INT64 feed the INT64 lane only (p' <= 18); the DECIMAL128 lane takes
        // FIXED_LEN_BYTE_ARRAY and BINARY, converted from big-endian bytes (flushDecimal).
        val matched = physical.getLogicalTypeAnnotation match {
          // #812: an unsigned INT64 (UINT_64) reads as decimal(20, 0) only (Spark's UnsignedLongUpdater).
          case i: org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation
              if !i.isSigned && p == INT64 =>
            i.getBitWidth == 64 && d.precision == 20 && d.scale == 0
          case a: org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation =>
            val scaleUp = d.scale - a.getScale
            scaleUp >= 0 && d.precision - a.getPrecision >= scaleUp
          case null | (_: org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation)
              if !isUnsigned(physical) && (p == INT32 || p == INT64) =>
            d.precision - d.scale >= (if (p == INT32) 10 else 20)
          case _ => false
        }
        // #812: INT32 / INT64 feed the DECIMAL128 lane too, each value sign-extended (zero-extended for
        // UINT_64) to 16 bytes and rescaled.
        matched && (p == INT32 || p == INT64 || p == FIXED_LEN_BYTE_ARRAY || p == BINARY)
      case _ => false
    }
  }

  private def isUnsigned(physical: PrimitiveType): Boolean = physical.getLogicalTypeAnnotation match {
    case i: org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation => !i.isSigned
    case _ => false
  }

  /** True if a column chunk of physical type `physical` using `encoding` can be decoded natively. */
  def supportsEncoding(encoding: String, physical: PrimitiveType.PrimitiveTypeName): Boolean =
    SupportedEncodings.contains(encoding) || TypedEncodings.get(encoding).exists(_.contains(physical))
}

/**
 * Reads ONE file that the native decoder cannot handle (an unsupported encoding found in its footer) with
 * Spark's own vectorized Parquet reader, so the query's results stay correct with no mid-decode failure.
 * Spark's reader builds the batch INCLUDING the partition columns (in output order) from the partition
 * schema and this file's partition values, so the emitted batches match the node's output exactly; they are
 * Spark's own `OffHeapColumnVector`s, which the operator above adapts through the same seam as any Spark
 * scan (`ColumnVectorAdapters`). The reader recycles its batch, so this iterator never closes an emitted
 * one.
 */
/**
 * The session's datetime settings the native scan resolves per file: the rebase modes in read
 * (`spark.sql.parquet.datetimeRebaseModeInRead`, `spark.sql.parquet.int96RebaseModeInRead`), the INT96
 * time-zone conversion flag and the session time zone.
 */
private[vecruntime] final case class NativeScanRebaseConf(
    datetimeMode: String,
    int96Mode: String,
    int96Convert: Boolean,
    sessionTz: String
)

/** One file's resolved modes for Spark's fallback reader; `convertTz` is null when no INT96 conversion applies. */
private[vecruntime] final case class FallbackModes(
    datetimeMode: String,
    datetimeTz: String,
    int96Mode: String,
    int96Tz: String,
    convertTz: String
)

private[vecruntime] final class SparkFallbackFileReader(
    file: PartitionedFile,
    hadoopConf: Configuration,
    requiredSchema: StructType,
    partitionSchema: StructType,
    batchSize: Int,
    context: TaskContext,
    modes: FallbackModes
) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  // A file we cannot decode natively falls back to Spark's own vectorized reader, opened as Spark's
  // ParquetFileFormat opens it: over this split's byte range (so a file split into several PartitionedFiles
  // returns each row group once, from the split that owns its midpoint), with the scan's Hadoop conf carrying
  // the requested schema and ParquetFileFormat's reader flags (INT96 as timestamp, binary as string, ...),
  // and with this file's own rebase modes and INT96 conversion (VectorParquetPartitionReader.fallbackModes).
  // The simpler initialize(path, columns) reads the whole file and forces int96AsTimestamp off.
  private val reader = {
    val r = new org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader(
      if (modes.convertTz == null) null else java.time.ZoneId.of(modes.convertTz),
      modes.datetimeMode,
      modes.datetimeTz,
      modes.int96Mode,
      modes.int96Tz,
      true,
      batchSize
    )
    val split = new org.apache.hadoop.mapred.FileSplit(file.toPath, file.start, file.length, Array.empty[String])
    val attempt = new org.apache.hadoop.mapreduce.TaskAttemptID(
      new org.apache.hadoop.mapreduce.TaskID(
        new org.apache.hadoop.mapreduce.JobID(),
        org.apache.hadoop.mapreduce.TaskType.MAP,
        0
      ),
      0
    )
    r.initialize(split, new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(hadoopConf, attempt))
    r.initBatch(partitionSchema, file.partitionValues)
    r.enableReturningBatches()
    r
  }
  if (context != null) context.addTaskCompletionListener[Unit](_ => close())

  private var advanced = false
  private var hasRow = false

  override def hasNext: Boolean = {
    if (reader == null) {
      return false
    }
    if (!advanced) {
      // A read error carries the file, as FileScanRDD attaches it (FAILED_READ_FILE.*): the same error Spark's scan
      // would raise for this file.
      hasRow =
        try reader.nextBatch()
        catch {
          case e: Throwable =>
            throw org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2.attachFilePath(file.urlEncodedPath, e)
        }
      advanced = true
    }
    hasRow
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    advanced = false
    reader.resultBatch()
  }

  override def close(): Unit = if (reader != null) reader.close()
}

/**
 * The planner's decision for a `FileSourceScanExec`: convert it to [[VectorParquetScanExec]] when the flag
 * is on and the scan is a supported flat Parquet scan, else the recorded reason. Encoding is only knowable
 * at read time, so slice 1 supports PLAIN / RLE_DICTIONARY and the reader fails over on any other page --
 * documented in docs/operators.md.
 */
object VectorParquetScanPlanner {

  def reason(scan: FileSourceScanExec, conf: SQLConf): Option[String] = {
    val relation = scan.relation
    if (!relation.fileFormat.isInstanceOf[org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat]) {
      return Some("scan is not Parquet")
    }
    // #568: structs of readable fields, and top-level lists and maps of leaves, are read natively; a list of lists or
    // of structs, or a list or a map inside a struct, keeps Spark's scan.
    val nested = scan.requiredSchema.fields.find(f =>
      !f.dataType.isInstanceOf[AtomicType] && !io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(f.dataType)
    )
    nested.foreach(f => return Some(s"nested or complex column ${f.name}:${f.dataType.simpleString}"))
    // Every required column must be a type the native reader actually decodes -- ONE shared source of truth
    // (NativeParquetSupport) so the plan-time check and the runtime decoder cannot drift. Excludes FLOAT,
    // TIMESTAMP_NTZ, BINARY, wide decimals and collated strings; each file's physical types are checked again
    // at open (physicalMatches).
    scan.requiredSchema.fields.foreach { f =>
      if (!io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(f.dataType)) {
        return Some(s"unsupported column type ${f.name}:${f.dataType.simpleString}")
      }
    }
    // Partition columns become constant columns via ArrowOutput.constant; they must be a type it can emit.
    relation.partitionSchema.fields.foreach { f =>
      if (!io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(f.dataType)) {
        return Some(s"unsupported partition column type ${f.name}:${f.dataType.simpleString}")
      }
    }
    // The datetime rebase mode is resolved per file at open (VectorParquetPartitionReader.fallbackModes): a
    // file that needs a rebase, or whose mode is EXCEPTION, is read by Spark's reader with its own modes.
    // Bucketed scans (#568) read natively: the scan reports Spark's own outputPartitioning (HashPartitioning over
    // the bucket columns) and reads Spark's own bucketed FilePartitions (`scan.inputRDD.partitions`: one per bucket,
    // pruned by optionalBucketSet, coalesced when Spark coalesces them), so each task reads its bucket's files.
    None
  }
}
