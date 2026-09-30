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

import scala.jdk.CollectionConverters._

import io.vecruntime.spark.adapter.TypeMapping
import io.vecruntime.spark.arrow.{ArrowOutput, SlicedColumnVector, VectorAllocators, VectorArrowColumnVector}
import io.vecruntime.spark.parquet.NativeParquetColumnReader
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.MessageType
import org.apache.spark.{Partition, TaskContext}
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
 * Per `PartitionedFile` it opens a `ParquetFileReader`, clips the requested columns exactly as Spark's
 * `ParquetReadSupport` does (case sensitivity, field ids), sets a `FilterCompat` filter from the pushed
 * filters so `readNextFilteredRowGroup` does row-group and column-index page skipping, and drives one
 * [[NativeParquetColumnReader]] per column. Partition-value columns are constant columns
 * ([[ArrowOutput.constant]]); a row group larger than `spark.sql.parquet.columnarReaderBatchSize` is
 * emitted as batch-sized offset views ([[SlicedColumnVector]]) with no copy.
 *
 * `doExecute` = `ColumnarToRowExec(this).doExecute()` (via [[VectorPlan]]). Reports `scan.output` /
 * `outputPartitioning` / `outputOrdering` verbatim, so `EnsureRequirements` (which ran before this
 * replacement) still holds.
 */
case class VectorParquetScanExec(scan: FileSourceScanExec) extends LeafExecNode with VectorPlan {

  override val output: Seq[Attribute] = scan.output
  override def outputPartitioning: Partitioning = scan.outputPartitioning
  override def outputOrdering: Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] = scan.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numRowGroups" -> SQLMetrics.createMetric(sparkContext, "number of row groups read"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")
  )

  override def doCanonicalize(): SparkPlan = VectorParquetScanExec(scan.canonicalized.asInstanceOf[FileSourceScanExec])

  // Prepare the wrapped scan and run its dynamic-partition-pruning subqueries before this node's RDD reads
  // `scan.inputRDD.partitions` (which evaluates the selected partitions); without this, accessing the
  // partitions throws "dynamicpruning ... has not finished". The scan owns the DPP subqueries (in its
  // partitionFilters), so we resolve them here -- Spark's own scan does this via the framework when it is
  // the executed node, which it is not once we wrap it.
  override protected def doPrepare(): Unit = {
    super.doPrepare()
    scan.prepare()
    scan.partitionFilters.foreach(_.foreach {
      case org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression(
            in: org.apache.spark.sql.execution.InSubqueryExec
          ) =>
        in.updateResult()
      case _ =>
    })
  }

  override def simpleString(maxFields: Int): String =
    s"VectorParquetScan ${scan.simpleString(maxFields)}"

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
      if (filterPushDown) org.apache.spark.sql.execution.vector.FileScanAccess.pushedDownFilters(scan) else Seq.empty
    val datetimeRebase = "CORRECTED" // the planner refuses anything else (see VectorParquetScanPlanner)

    val m = ScanMetrics(
      longMetric("numFiles"),
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"),
      longMetric("numRowGroups"),
      longMetric("scanTime")
    )
    val required = requiredSchema
    val partSchema = partitionSchema
    val attrs = outputAttrs.map(a => (a.name, a.dataType)).toArray

    new VectorParquetRDD(
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
      datetimeRebase,
      m
    )
  }
}

/** The scan's SQL metrics, flushed per task. */
final case class ScanMetrics(
    numFiles: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    numRowGroups: SQLMetric,
    scanTime: SQLMetric
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
    datetimeRebase: String,
    metrics: ScanMetrics
) extends RDD[ColumnarBatch](sc, Nil) {

  // Reuse the wrapped scan's file partitions verbatim (DPP / bucketing / splitting applied). Read lazily
  // inside getPartitions -- not in the constructor -- so a dynamic-partition-pruning subquery has finished
  // before `scan.inputRDD` is materialised (accessing it early throws "dynamicpruning has not finished").
  override protected def getPartitions: Array[Partition] = scan.inputRDD.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] =
    new VectorParquetPartitionReader(
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
      datetimeRebase,
      metrics,
      context
    )
}

/**
 * Reads one [[FilePartition]] as a stream of columnar batches. Per `PartitionedFile`: opens a
 * `ParquetFileReader`, clips the requested schema, sets the row-group filter, and drives one
 * [[NativeParquetColumnReader]] per data column. A row group is decoded into the readers' reused vectors
 * and then emitted as `batchSize`-row offset views ([[SlicedColumnVector]]) with the partition-value
 * constant columns appended; the whole row group is kept alive until its last batch is consumed.
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
    datetimeRebase: String,
    metrics: ScanMetrics,
    context: TaskContext
) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  private val allocator = VectorAllocators.newChild("VectorParquetScanExec")
  private val files: Iterator[PartitionedFile] = partition.files.iterator
  private val dataColumnCount = requiredSchema.length

  // Per-file state.
  private var reader: ParquetFileReader = _
  private var columnReaders: Array[NativeParquetColumnReader] = _
  private var blocks: java.util.List[BlockMetaData] = _
  private var rowGroupIndex = 0
  private var partitionColumns: Array[ColumnVector] = _ // constant columns for the current file
  private var partitionColumnRows = 0 // the length the partition constant columns were sized to

  // Current row group state (the reused vectors + how far we have emitted).
  private var rgVectors: Array[org.apache.arrow.vector.FieldVector] = _
  private var rgColumns: Array[ColumnVector] = _
  private var rgRows = 0
  private var rgOffset = 0
  private var emitted: ColumnarBatch = _
  private var fallback: Iterator[ColumnarBatch] = _ // Spark's reader for a file we cannot decode

  if (context != null) context.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean = {
    // A file whose encodings we cannot decode falls back to Spark's reader for that file.
    if (fallback != null) {
      if (fallback.hasNext) return true
      closeFile()
    }
    // Rows left in the current row group?
    if (rgVectors != null && rgOffset < rgRows) return true
    // Next row group in the current file, or advance files.
    while (true) {
      if (reader != null) {
        val t0 = System.nanoTime()
        val store = readNextFilteredRowGroup()
        if (store != null) {
          val rgRowCount = store.getRowCount.toInt
          decodeRowGroup(store, rgRowCount)
          metrics.scanTime += (System.nanoTime() - t0) / 1000000L
          metrics.numRowGroups += 1
          rgRows = rgRowCount
          rgOffset = 0
          if (rgRows > 0) return true
        } else {
          metrics.scanTime += (System.nanoTime() - t0) / 1000000L
          closeFile()
        }
      }
      if (reader == null && fallback == null) {
        if (!files.hasNext) return false
        openFile(files.next())
        if (fallback != null && fallback.hasNext) return true
      }
    }
    false
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    if (fallback != null) {
      // Spark's reader owns and recycles its batch; do not wrap or close it here.
      val b = fallback.next()
      metrics.numOutputBatches += 1
      metrics.numOutputRows += b.numRows()
      return b
    }
    releaseEmitted()
    val n = math.min(batchSize, rgRows - rgOffset)
    val columns = new Array[ColumnVector](attrs.length)
    var c = 0
    while (c < dataColumnCount) {
      columns(c) = SlicedColumnVector.of(rgColumns(c), rgOffset, n, rgRows)
      c += 1
    }
    // Partition-value constant columns follow the data columns, ordered to the output.
    var p = 0
    while (p < partitionColumns.length) {
      columns(dataColumnCount + p) = SlicedColumnVector.of(partitionColumns(p), rgOffset, n, partitionColumnRows)
      p += 1
    }
    rgOffset += n
    emitted = new ColumnarBatch(columns, n)
    metrics.numOutputBatches += 1
    metrics.numOutputRows += n
    emitted
  }

  private def openFile(file: PartitionedFile): Unit = {
    if (context != null) context.killTaskIfInterrupted()
    reader = ParquetFileReader.open(org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(file.toPath, hadoopConf))
    val fileSchema = reader.getFileMetaData.getSchema
    val clipped = ParquetReadSupport.clipParquetSchema(fileSchema, requiredSchema, caseSensitive, useFieldId, false)
    // Encoding is only knowable at read time (slice 1). If any required column chunk uses an encoding we
    // do not decode (DELTA_*, BYTE_STREAM_SPLIT, ...), fall the WHOLE FILE over to Spark's vectorized
    // reader, adapting nothing further -- correct results, no mid-decode crash.
    if (hasUnsupportedEncoding(reader, clipped)) {
      reader.close()
      reader = null
      fallback = new SparkFallbackFileReader(file, hadoopConf, requiredSchema, partitionSchema, batchSize, context)
      metrics.numFiles += 1
      return
    }
    reader.setRequestedSchema(clipped)
    setRowGroupFilter(clipped)
    blocks = reader.getFooter.getBlocks
    rowGroupIndex = 0
    val columns = clipped.getColumns
    columnReaders = new Array[NativeParquetColumnReader](dataColumnCount)
    // Map each required data column to its clipped ColumnDescriptor by leaf name (flat schema only).
    var i = 0
    while (i < dataColumnCount) {
      val field = requiredSchema.fields(i)
      val cd = columns.asScala.find(_.getPath()(0).equalsIgnoreCase(field.name))
        .getOrElse(throw new IllegalStateException(s"column ${field.name} missing from clipped schema"))
      columnReaders(i) =
        new NativeParquetColumnReader(cd, TypeMapping.vecTypeOf(field.dataType), field.dataType, field.name, allocator)
      i += 1
    }
    buildPartitionColumns(file)
    metrics.numFiles += 1
  }

  /** Constant columns for the partition values of this file, sized to the file's largest row group. */
  private def buildPartitionColumns(file: PartitionedFile): Unit = {
    partitionColumns = new Array[ColumnVector](partitionSchema.length)
    if (partitionSchema.isEmpty) {
      partitionColumnRows = 0
      return
    }
    var maxRg = 0
    var b = 0
    while (b < blocks.size()) {
      maxRg = math.max(maxRg, blocks.get(b).getRowCount.toInt)
      b += 1
    }
    partitionColumnRows = maxRg
    val values = file.partitionValues
    var p = 0
    while (p < partitionSchema.length) {
      val f = partitionSchema.fields(p)
      val v = values.get(p, f.dataType)
      partitionColumns(p) =
        if (v == null) ArrowOutput.nulls(f.name, f.dataType, maxRg, allocator)
        else ArrowOutput.constant(f.name, f.dataType, v, maxRg, allocator)
      p += 1
    }
  }

  private def decodeRowGroup(store: org.apache.parquet.column.page.PageReadStore, rowCount: Int): Unit = {
    rgVectors = new Array[org.apache.arrow.vector.FieldVector](dataColumnCount)
    rgColumns = new Array[ColumnVector](dataColumnCount)
    var c = 0
    while (c < dataColumnCount) {
      val fv = columnReaders(c).readRowGroup(store.getPageReader(columnReaders(c).column()), rowCount)
      rgVectors(c) = fv
      // Borrowed wrapper (the reader owns and recycles fv); SlicedColumnVector views it per batch.
      rgColumns(c) = ArrowOutput.wrap(fv, attrs(c)._2) match {
        case v: VectorArrowColumnVector => v.borrow()
        case other => other
      }
      c += 1
    }
  }

  private def readNextFilteredRowGroup(): org.apache.parquet.column.page.PageReadStore = {
    val store = reader.readNextFilteredRowGroup()
    rowGroupIndex += 1
    store
  }

  /**
   * True if any required column chunk in any row group uses a value encoding the native decoder does not
   * support (slice 1 decodes PLAIN and dictionary only). Read from the footer's column-chunk metadata, so
   * the decision is made once per file at open time -- a DELTA_* / BYTE_STREAM_SPLIT column falls the file
   * over to Spark's reader with no mid-decode failure.
   */
  private def hasUnsupportedEncoding(rdr: ParquetFileReader, clipped: MessageType): Boolean = {
    val wanted = new java.util.HashSet[String]()
    clipped.getColumns.forEach(cd => wanted.add(cd.getPath()(0).toLowerCase(java.util.Locale.ROOT)))
    val blocks = rdr.getFooter.getBlocks
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
            if (!VectorParquetScanExec.SupportedEncodings.contains(it.next().name())) {
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

  private def setRowGroupFilter(clipped: MessageType): Unit = {
    if (pushedFilters.isEmpty) {
      return
    }
    val rebaseSpec = org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec(
      org.apache.spark.sql.internal.LegacyBehaviorPolicy.withName(datetimeRebase)
    )
    val parquetFilters = new ParquetFilters(
      clipped,
      pushDownDate,
      pushDownTimestamp,
      pushDownDecimal,
      pushDownStringPredicate,
      pushDownInFilterThreshold,
      caseSensitive,
      rebaseSpec
    )
    val predicates = pushedFilters.flatMap(f => parquetFilters.createFilter(f))
    if (predicates.nonEmpty) {
      val combined = predicates.reduce((a, b) => org.apache.parquet.filter2.predicate.FilterApi.and(a, b))
      reader.setRequestedSchema(clipped) // already set; keep before applying the filter
      org.apache.parquet.hadoop.ParquetInputFormat.setFilterPredicate(hadoopConf, combined)
      filterCompat = FilterCompat.get(combined)
    }
  }

  private var filterCompat: FilterCompat.Filter = FilterCompat.NOOP

  private def releaseEmitted(): Unit = if (emitted != null) {
    emitted.close() // SlicedColumnVector.close() is a no-op; the row-group vectors are reader-owned
    emitted = null
  }

  private def closeFile(): Unit = {
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
    if (partitionColumns != null) {
      partitionColumns.foreach(v => if (v != null) v.close())
      partitionColumns = null
    }
    if (reader != null) {
      reader.close()
      reader = null
    }
    rgVectors = null
    rgRows = 0
    rgOffset = 0
  }

  override def close(): Unit = {
    releaseEmitted()
    closeFile()
    allocator.close()
  }
}

object VectorParquetScanExec {

  /** The Parquet value/level encodings the native decoder handles; any other encoding falls the file over. */
  val SupportedEncodings: Set[String] = Set("PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "RLE", "BIT_PACKED")
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
private[vecruntime] final class SparkFallbackFileReader(
    file: PartitionedFile,
    hadoopConf: Configuration,
    requiredSchema: StructType,
    partitionSchema: StructType,
    batchSize: Int,
    context: TaskContext
) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  private val reader =
    new org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader(true, batchSize)

  reader.initialize(file.toPath.toString, java.util.Arrays.asList(requiredSchema.fieldNames: _*))
  reader.initBatch(partitionSchema, file.partitionValues)
  reader.enableReturningBatches()
  if (context != null) context.addTaskCompletionListener[Unit](_ => close())

  private var advanced = false
  private var hasRow = false

  override def hasNext: Boolean = {
    if (!advanced) {
      hasRow = reader.nextBatch()
      advanced = true
    }
    hasRow
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    advanced = false
    reader.resultBatch()
  }

  override def close(): Unit = reader.close()
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
    // Nested / complex required columns are not supported (flat only).
    val nested = scan.requiredSchema.fields.find(f => !f.dataType.isInstanceOf[AtomicType])
    nested.foreach(f => return Some(s"nested or complex column ${f.name}:${f.dataType.simpleString}"))
    // Every required column must be a type the native reader actually decodes -- ONE shared source of truth
    // (NativeParquetSupport) so the plan-time check and the runtime decoder cannot drift. Excludes BOOLEAN,
    // TINYINT/SMALLINT, TIMESTAMP/TIMESTAMP_NTZ, BINARY, wide decimals and collated strings.
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
    // Non-CORRECTED date rebase is refused (read as Spark's). Timestamps are not a supported type at all
    // (NativeParquetSupport excludes them), so only DATE reaches here.
    if (scan.requiredSchema.fields.exists(_.dataType.isInstanceOf[DateType])) {
      val mode = conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ).toString
      if (mode != "CORRECTED") {
        return Some(s"date rebase mode $mode (only CORRECTED)")
      }
    }
    // Bucketed scans: leave to Spark unless bucketing is disabled for this scan (reproducing Spark's
    // bucket-to-partition mapping ourselves is out of slice 1).
    if (relation.bucketSpec.isDefined && !scan.disableBucketedScan) {
      return Some("bucketed scan")
    }
    None
  }
}
