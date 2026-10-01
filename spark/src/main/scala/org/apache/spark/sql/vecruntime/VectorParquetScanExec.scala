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
import io.vecruntime.spark.arrow.{ArrowOutput, VectorAllocators}
import io.vecruntime.spark.parquet.NativeParquetColumnReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.SeekableInputStream
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
 * Per `PartitionedFile` it opens a `ParquetFileReader` ONCE -- no `getFileStatus` HEAD (the InputFile is
 * built from the `PartitionedFile`'s length + modification time) and the footer is read once and the reader
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
  private var rowGroupIndex = 0
  private var partitionValues: InternalRow = _ // this file's partition values (for constant columns)

  // Current row group state.
  private var rgRows = 0
  private var rgOffset = 0
  private var emitted: ColumnarBatch = _
  private var emittedVectors: Array[org.apache.arrow.vector.FieldVector] = _ // batch-owned data vectors to release
  private var emittedPartitionVectors: Array[ColumnVector] = _ // batch-owned partition constant columns to close
  private var fallback: Iterator[ColumnarBatch] = _ // Spark's reader for a file we cannot decode

  if (context != null) context.addTaskCompletionListener[Unit](_ => close())

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
    val dataVectors = new Array[org.apache.arrow.vector.FieldVector](dataColumnCount)
    val partitionVectors = new Array[ColumnVector](partitionSchema.length)
    try {
      var c = 0
      while (c < dataColumnCount) {
        // Decode this batch's rows STRAIGHT into a fresh batch-owned Arrow vector (no row-group vector, no
        // copy of the group, no Arrow getNullCount): the reader streams pages and resumes mid-run. The
        // batch owns the vector; released to the reader's pool on the next batch / close. Self-contained,
        // so a retained or serialized batch ships only its own rows.
        val fv = columnReaders(c).readBatch(n)
        dataVectors(c) = fv
        columns(c) = ArrowOutput.wrap(fv, attrs(c)._2)
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
    emitted = new ColumnarBatch(columns, n)
    emittedVectors = dataVectors
    emittedPartitionVectors = partitionVectors
    metrics.numOutputBatches += 1
    metrics.numOutputRows += n
    emitted
  }

  private def openFile(file: PartitionedFile): Unit = {
    if (context != null) context.killTaskIfInterrupted()
    val path = file.toPath
    val start = file.start
    val end = file.start + file.length
    // No HEAD: build the InputFile from the length and modification time Spark already put in the
    // PartitionedFile, instead of HadoopInputFile.fromPath -> fs.getFileStatus (one S3AStoreImpl.headObject
    // -> network round trip per split before any read). HadoopInputFile.fromStatus trusts the status as
    // given and never re-stats, so the open path makes zero getFileStatus calls.
    val status = new FileStatus(file.fileSize, false, 0, 0L, file.modificationTime, path)
    val inputFile = HadoopInputFile.fromStatus(status, hadoopConf)
    // Open the file ONCE: a single SeekableInputStream, read the footer from it with the split range, then
    // build the reader from THAT footer + the SAME stream (ParquetFileReader(InputFile, ParquetMetadata,
    // options, stream)) -- no reopen, no second footer read. The split range (file.start ..
    // file.start+file.length) keeps the row groups whose midpoint falls in the range, so a file Spark split
    // into several PartitionedFiles reads each row group in exactly one split (no duplicate rows, no
    // duplicate I/O; 8 TPC-DS store_sales files exceed the 128 MB default split). The previous code opened
    // the file twice (footer+schema, then reopened with the filter) -> two opens and two footer reads per
    // split, on top of AAL's own footer parse.
    val rangeOpts = HadoopReadOptions.builder(hadoopConf, path).withRange(start, end).build()
    var stream: SeekableInputStream = null
    try {
      stream = inputFile.newStream()
      val footer: ParquetMetadata = ParquetFileReader.readFooter(inputFile, rangeOpts, stream)
      val fileSchema = footer.getFileMetaData.getSchema
      val clipped = ParquetReadSupport.clipParquetSchema(fileSchema, requiredSchema, caseSensitive, useFieldId, false)
      // Encoding is only knowable at read time (slice 1). If any required column chunk uses an encoding we
      // do not decode (DELTA_*, BYTE_STREAM_SPLIT, ...), fall the WHOLE FILE over to Spark's vectorized
      // reader, adapting nothing further -- correct results, no mid-decode crash. (Spark's reader honors the
      // split range itself from the PartitionedFile, so the fallback is not double-counted either.)
      if (hasUnsupportedEncodingInFooter(footer, clipped)) {
        stream.close()
        stream = null
        fallback = new SparkFallbackFileReader(file, hadoopConf, requiredSchema, partitionSchema, batchSize, context)
        metrics.numFiles += 1
        return
      }
      // Build the reader from the footer we already read, over the SAME stream, with the range AND the
      // pushed filter so readNextFilteredRowGroup skips row groups / pages. The reader now owns the stream
      // (closed by reader.close()); clear our local handle so the finally below does not double-close it.
      val readOpts = HadoopReadOptions
        .builder(hadoopConf, path)
        .withRange(start, end)
        .withRecordFilter(rowGroupFilter(clipped))
        .build()
      reader = ParquetFileReader.open(inputFile, footer, readOpts, stream)
      stream = null
      reader.setRequestedSchema(clipped)
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
          new NativeParquetColumnReader(
            cd,
            TypeMapping.vecTypeOf(field.dataType),
            field.dataType,
            field.name,
            batchSize,
            allocator
          )
        i += 1
      }
      buildPartitionColumns(file)
      metrics.numFiles += 1
    } finally {
      // Reached only when the reader was NOT built from the stream (an exception, or a path that returns
      // early without taking ownership). The success path and the fallback path both null `stream` out
      // after handing it over / closing it, so this never double-closes.
      if (stream != null) stream.close()
    }
  }

  /** Record this file's partition values; per-batch constant columns are built from them in next(). */
  private def buildPartitionColumns(file: PartitionedFile): Unit = {
    partitionValues = file.partitionValues
  }

  private def startRowGroup(store: org.apache.parquet.column.page.PageReadStore, rowCount: Int): Unit = {
    var c = 0
    while (c < dataColumnCount) {
      columnReaders(c).startRowGroup(store.getPageReader(columnReaders(c).column()), rowCount)
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
  private def hasUnsupportedEncodingInFooter(footer: ParquetMetadata, clipped: MessageType): Boolean = {
    val wanted = new java.util.HashSet[String]()
    clipped.getColumns.forEach(cd => wanted.add(cd.getPath()(0).toLowerCase(java.util.Locale.ROOT)))
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

  private def rowGroupFilter(clipped: MessageType): FilterCompat.Filter = {
    if (pushedFilters.isEmpty) {
      return FilterCompat.NOOP
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
    if (predicates.isEmpty) {
      FilterCompat.NOOP
    } else {
      FilterCompat.get(predicates.reduce((a, b) => org.apache.parquet.filter2.predicate.FilterApi.and(a, b)))
    }
  }

  private def releaseEmitted(): Unit = if (emitted != null) {
    // Return the batch's owned data vectors to their reader's pool for reuse. Do NOT close the ColumnarBatch
    // (that would also close the file-owned partition constant columns / their views); the data vectors are
    // released here, the partition constants live until closeFile.
    if (emittedVectors != null) {
      var c = 0
      while (c < dataColumnCount) {
        if (emittedVectors(c) != null) {
          // Return to the reader's pool if the reader is still open; otherwise (file already closed) close.
          if (columnReaders != null && columnReaders(c) != null) columnReaders(c).release(emittedVectors(c))
          else emittedVectors(c).close()
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
      reader.close()
      reader = null
    }
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

  // A file we cannot decode natively falls back to Spark's own vectorized reader. That reader's simple
  // initialize(path, columns) reads the WHOLE file with no split range, so to avoid duplicate rows when the
  // file was split into several PartitionedFiles we read it on the FIRST split only (start == 0) and emit
  // nothing for later splits. Correct: the whole-file read returns every row exactly once.
  private val readsFile: Boolean = file.start == 0L

  private val reader =
    if (readsFile) new org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader(true, batchSize)
    else null

  if (readsFile) {
    reader.initialize(file.toPath.toString, java.util.Arrays.asList(requiredSchema.fieldNames: _*))
    reader.initBatch(partitionSchema, file.partitionValues)
    reader.enableReturningBatches()
  }
  if (context != null) context.addTaskCompletionListener[Unit](_ => close())

  private var advanced = false
  private var hasRow = false

  override def hasNext: Boolean = {
    if (reader == null) {
      return false
    }
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
