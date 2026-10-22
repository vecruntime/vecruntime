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

import java.lang.foreign.{Arena, MemorySegment}
import java.util.Date

import scala.jdk.CollectionConverters._

import io.vecruntime.kernels.{ArrowLayout, CompactKernels, SegmentVectorBuffers, VecType, VectorBuffers}
import io.vecruntime.parquet.write.ColumnarParquetWriter
import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.adapter.ColumnVectorAdapters
import io.vecruntime.spark.arrow.SelectedColumnarBatch
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.{CodecFactory, ParquetOutputFormat}
import org.apache.parquet.hadoop.util.HadoopOutputFile

import org.apache.spark.{SPARK_VERSION_SHORT, TaskContext}
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{
  BaseDynamicPartitionDataWriter,
  BucketingUtils,
  FileFormatDataWriter,
  OutputWriter,
  OutputWriterFactory,
  WriteFilesExec,
  WriteFilesExecBase,
  WriteFilesSpec,
  WriteJobDescription,
  WriteTaskResult
}
import org.apache.spark.sql.execution.datasources.parquet.{
  ParquetFileFormat,
  ParquetReadSupport,
  ParquetWriteSupport,
  SparkToParquetSchemaConverter
}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * #64: writes Parquet files from our columnar batches. It replaces Spark's `WriteFilesExec` under the write
 * command, so Spark keeps the command, the job (`FileFormatWriter.write`), the commit protocol, the job
 * description and the post-write refreshes; what changes is the task: our batches go straight to
 * [[VectorParquetOutputWriter]] (the `parquet-writer` module's [[ColumnarParquetWriter]]), with no
 * columnar-to-row conversion and no `ParquetWriteSupport`.
 *
 * The task mirrors `FileFormatWriter.executeTask` with `SingleDirectoryDataWriter`: the same task attempt context,
 * the committer's `newTaskTempFile` for each file (so file names and the commit are Spark's), `maxRecordsPerFile`
 * honoured by splitting a batch between files, the job's stats trackers told of every file and row, and the
 * committer's `abortTask` on failure; partitioned and bucketed writes go through
 * [[VectorDynamicPartitionDataWriter]] (Spark's `DynamicPartitionDataSingleWriter`). Which writes convert:
 * [[VectorWritePlanner]].
 */
case class VectorWriteFilesExec(
    child: SparkPlan,
    fileFormat: org.apache.spark.sql.execution.datasources.FileFormat,
    partitionColumns: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
    bucketSpec: Option[org.apache.spark.sql.catalyst.catalog.BucketSpec],
    options: Map[String, String],
    staticPartitions: Map[String, String]
) extends WriteFilesExecBase with VectorPlan {

  override protected def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    val rdd = child.executeColumnar()
    // SPARK-23271: an empty input still runs one task, which writes a file holding only the schema.
    val nonEmpty =
      if (rdd.partitions.length == 0) session.sparkContext.parallelize(Seq.empty[ColumnarBatch], 1) else rdd
    val spec = writeFilesSpec.description
    val description = new WriteJobDescription(
      spec.uuid,
      spec.serializableHadoopConf,
      new VectorParquetOutputWriterFactory(spec.outputWriterFactory),
      spec.allColumns,
      spec.dataColumns,
      spec.partitionColumns,
      spec.bucketSpec,
      spec.path,
      spec.customPartitionLocations,
      spec.maxRecordsPerFile,
      spec.timeZoneId,
      spec.statsTrackers
    )
    val ordinals = spec.dataColumns.map(a => child.output.indexWhere(_.exprId == a.exprId)).toArray
    require(ordinals.forall(_ >= 0), s"write columns ${spec.dataColumns} not all in ${child.output}")
    val allOrdinals = spec.allColumns.map(a => child.output.indexWhere(_.exprId == a.exprId)).toArray
    require(allOrdinals.forall(_ >= 0), s"columns ${spec.allColumns} not all in ${child.output}")
    // Spark decides whether the task uses concurrent writers (its spec is defined then); the sort it would run
    // past the limit is ours, by the partition columns and the bucket id (FileFormatWriter's required ordering).
    val concurrent = writeFilesSpec.concurrentOutputWriterSpecFunc(child).map { c =>
      val order =
        VectorWritePlanner.concurrentSortOrder(spec.partitionColumns, spec.bucketSpec.map(_.bucketIdExpression))
      val keys = order.map(o =>
        VectorSortPlanner.compileKey(
          o,
          child.output
        ).fold(r => throw new IllegalStateException(s"sort key ${o.sql}: $r"), k => k)
      )
      VectorConcurrentWriters(
        c.maxWriters,
        keys.toArray,
        order.map(_ => true).toArray,
        order.map(_ => true).toArray,
        child.output.map(a => (a.name, a.dataType)).toArray,
        new VectorMetrics(
          sortMetric("numInputBatches"),
          sortMetric("numOutputBatches"),
          sortMetric("numOutputRows"),
          sortMetric("sortTime")
        ),
        VectorConf.sortRunRows(conf),
        VectorConf.sortSpillBytes(conf, sparkContext.getConf),
        VectorSortIterator.memoryMode(sparkContext.getConf) // #879: the runs' pool, resolved at plan time
      )
    }
    val committer = writeFilesSpec.committer
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    nonEmpty.mapPartitionsInternal { batches =>
      val ctx = TaskContext.get()
      Iterator(
        VectorWriteTask.execute(
          description,
          jobTrackerID,
          ctx.stageId(),
          ctx.partitionId(),
          ctx.taskAttemptId().toInt & Int.MaxValue,
          committer,
          batches,
          ordinals,
          allOrdinals,
          concurrent
        )
      )
    }
  }

  private def sortMetric(name: String) =
    org.apache.spark.sql.execution.metric.SQLMetrics.createMetric(sparkContext, s"concurrent writers' sort $name")

  override protected def stringArgs: Iterator[Any] = Iterator(child)

  override protected def withNewChildInternal(newChild: SparkPlan): VectorWriteFilesExec = copy(child = newChild)
}

/** Which `WriteFilesExec` nodes convert, and why the others do not (the reasons tests and the UI show). */
object VectorWritePlanner {

  /** Hadoop / Parquet options the prototype does not reproduce: the write keeps Spark's writer. */
  private val UnsupportedOptions = Seq(
    "parquet.crypto.factory.class",
    "parquet.encryption.kms.client.class"
  )

  def reason(w: WriteFilesExec, session: SparkSession): Option[String] = {
    val conf = session.sessionState.conf
    if (w.fileFormat.getClass != classOf[ParquetFileFormat]) {
      Some(s"columnar write: format ${w.fileFormat.getClass.getSimpleName} is not Parquet")
    } else if (concurrentKeysReason(w, conf).isDefined) {
      concurrentKeysReason(w, conf)
    } else if (w.options.contains(BucketingUtils.optionForHiveCompatibleBucketWrite)) {
      Some("columnar write: Hive-compatible bucketing is not supported yet (#842)")
    } else if (!w.child.supportsColumnar && !io.vecruntime.spark.VectorConf.writeColumnarFromRows(conf)) {
      Some(s"columnar write: child ${w.child.nodeName} is not columnar")
    } else {
      val hadoopConf = session.sessionState.newHadoopConfWithOptions(w.options)
      def option(k: String): Option[String] = w.options.get(k).orElse(Option(hadoopConf.get(k)))
      val options = UnsupportedOptions.find(k => option(k).exists(v => v != "false"))
      // The per-column options the writer reads (#842); any other `parquet.*#<column>` key keeps Spark's writer.
      val readPerColumn = Seq(
        ParquetOutputFormat.ENABLE_DICTIONARY,
        ParquetOutputFormat.BLOOM_FILTER_ENABLED,
        ParquetOutputFormat.BLOOM_FILTER_EXPECTED_NDV,
        ParquetOutputFormat.BLOOM_FILTER_FPP,
        ParquetOutputFormat.BLOOM_FILTER_CANDIDATES_NUMBER
      ).map(_ + "#")
      lazy val perColumn = (hadoopConf.iterator().asScala.map(_.getKey) ++ w.options.keys)
        .find(k => k.startsWith("parquet.") && k.contains("#") && !readPerColumn.exists(k.startsWith))
      // The columnar writer writes Parquet v2 only (data page v2, the v2 encodings); a write that asks for v1 by
      // name keeps Spark's writer.
      val v1 =
        option("parquet.writer.version").exists(v => v.equalsIgnoreCase("v1") || v.equalsIgnoreCase("PARQUET_1_0"))
      if (v1) {
        Some("columnar write: parquet.writer.version is v1, the columnar writer writes Parquet v2")
      } else {
        options
          .map(k => s"columnar write: $k is not supported yet (#842)")
          .orElse(perColumn.map(k => s"columnar write: per-column option $k is not supported yet (#842)"))
          .orElse(w.child.output.iterator.flatMap(a => typeReason(a.name, a.dataType, conf)).nextOption())
      }
    }
  }

  /**
   * FileFormatWriter's required ordering of a dynamic-partition write: the partition columns, then the bucket id,
   * ascending (nulls first).
   */
  def concurrentSortOrder(
      partitionColumns: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
      bucketId: Option[org.apache.spark.sql.catalyst.expressions.Expression]
  ): Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] =
    (partitionColumns ++ bucketId).map(e =>
      org.apache.spark.sql.catalyst.expressions.SortOrder(e, org.apache.spark.sql.catalyst.expressions.Ascending)
    )

  /**
   * With concurrent writers (`spark.sql.maxConcurrentOutputFileWriters` > 0 and no bucket sort columns) the input
   * is not sorted, and past the writer limit the task sorts the rest itself -- with our sort, whose keys must
   * compile; why not, if one does not.
   */
  private def concurrentKeysReason(w: WriteFilesExec, conf: SQLConf): Option[String] = {
    val dynamic = w.partitionColumns.nonEmpty || w.bucketSpec.isDefined
    if (!dynamic || conf.maxConcurrentOutputFileWriters <= 0 || w.bucketSpec.exists(_.sortColumnNames.nonEmpty)) None
    else {
      val dataColumns = w.child.output.filterNot(a => w.partitionColumns.exists(_.exprId == a.exprId))
      val bucketId = org.apache.spark.sql.execution.datasources.V1WritesUtils
        .getWriterBucketSpec(w.bucketSpec, dataColumns, w.options)
        .map(_.bucketIdExpression)
      concurrentSortOrder(w.partitionColumns, bucketId).iterator
        .flatMap(o => VectorSortPlanner.compileKey(o, w.child.output).left.toOption.map(r => s"${o.sql}: $r"))
        .nextOption()
        .map(r => s"columnar write: concurrent writers' sort key $r")
    }
  }

  private def typeReason(name: String, dt: DataType, conf: SQLConf): Option[String] = {
    val rebased = conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE) != LegacyBehaviorPolicy.CORRECTED
    val int96Rebased = conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE) != LegacyBehaviorPolicy.CORRECTED
    val int96 = conf.parquetOutputTimestampType == SQLConf.ParquetOutputTimestampType.INT96
    dt match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | BinaryType => None
      case _: StringType => None
      case DateType if rebased =>
        Some(s"columnar write: column $name is a date and datetimeRebaseModeInWrite is not CORRECTED")
      case DateType => None
      case TimestampType if int96 && int96Rebased =>
        Some(s"columnar write: column $name is an INT96 timestamp and int96RebaseModeInWrite is not CORRECTED")
      case TimestampType if !int96 && rebased =>
        Some(s"columnar write: column $name is a timestamp and datetimeRebaseModeInWrite is not CORRECTED")
      case TimestampType | TimestampNTZType => None
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS && conf.writeLegacyParquetFormat =>
        Some(s"columnar write: column $name is a decimal(${d.precision},${d.scale}) and writeLegacyFormat is on")
      case _: DecimalType => None
      // Structs, arrays and maps (#842) in Spark's standard layout, of the types above.
      case _: StructType | _: ArrayType | _: MapType if conf.writeLegacyParquetFormat =>
        Some(s"columnar write: column $name is nested and writeLegacyFormat is on")
      case s: StructType if s.isEmpty => Some(s"columnar write: column $name is an empty struct")
      case s: StructType =>
        s.fields.iterator.flatMap(f => typeReason(s"$name.${f.name}", f.dataType, conf)).nextOption()
      case a: ArrayType => typeReason(s"$name.element", a.elementType, conf)
      case m: MapType =>
        typeReason(s"$name.key", m.keyType, conf).orElse(typeReason(s"$name.value", m.valueType, conf))
      case other => Some(s"columnar write: column $name has type ${other.simpleString}, not supported yet (#842)")
    }
  }
}

/** The write task: `FileFormatWriter.executeTask` for our batches. */
private[vecruntime] object VectorWriteTask {

  def execute(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      batches: Iterator[ColumnarBatch],
      ordinals: Array[Int],
      allOrdinals: Array[Int],
      concurrent: Option[VectorConcurrentWriters]
  ): WriteTaskResult = {
    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)
    val taskAttemptContext: TaskAttemptContext = {
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)
      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }
    committer.setupTask(taskAttemptContext)
    var dataWriter: FileFormatDataWriter with VectorBatchWriter = null
    // As FileFormatDataWriter.enrichWriteError around its writes: a fetch failure as it is; a file that already
    // exists as the fast-fail exception under spark.sql.execution.fastFailOnFileFormatOutput, else, as any other
    // error of the writer, TASK_WRITE_FAILED. An error computing the input (the batches) propagates as it is.
    def write(f: => Unit): Unit =
      try f
      catch {
        case e: org.apache.spark.shuffle.FetchFailedException => throw e
        case e: org.apache.hadoop.fs.FileAlreadyExistsException
            if org.apache.spark.sql.internal.SQLConf.get.fastFailFileFormatOutput =>
          throw new org.apache.spark.TaskOutputFileAlreadyExistException(e)
        case t: Throwable =>
          throw org.apache.spark.sql.errors.QueryExecutionErrors.taskFailedWhileWritingRowsError(description.path, t)
      }
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      // As Spark: an empty partition other than the first writes no file (the first writes the schema).
      val writeFile = sparkPartitionId == 0 || batches.hasNext
      dataWriter =
        if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
          new VectorSingleDirectoryDataWriter(description, taskAttemptContext, committer, writeFile)
        } else {
          new VectorDynamicPartitionDataWriter(description, taskAttemptContext, committer, allOrdinals, concurrent)
        }
      dataWriter match {
        case d: VectorDynamicPartitionDataWriter if concurrent.isDefined =>
          // `sorted` first: a reader may reuse its vectors on `hasNext`, and the pending rows still borrow them.
          while (!d.sorted && batches.hasNext) { val b = batches.next(); write(d.writeBatch(b, ordinals)) }
          if (d.sorted) {
            // The limit was reached: the rest, sorted, through the same writer (the sort keeps the columns).
            val rest = d.sortedRest(batches)
            try while (rest.hasNext) { val b = rest.next(); write(d.writeBatch(b, ordinals)) }
            finally rest.close()
          }
        case _ =>
          while (batches.hasNext) {
            val b = batches.next()
            write(dataWriter.writeBatch(b, ordinals))
          }
      }
      dataWriter.commit()
    })(
      catchBlock = {
        if (dataWriter != null) dataWriter.abort() else committer.abortTask(taskAttemptContext)
      },
      finallyBlock = {
        if (dataWriter != null) dataWriter.close()
      }
    )
  }
}

/**
 * `SingleDirectoryDataWriter` for batches: one file at a time from the committer, rolled at `maxRecordsPerFile`
 * rows, each row counted by the stats trackers (Spark's `BasicWriteTaskStatsTracker` counts rows and ignores the
 * row object, so it is handed `null`).
 */
private[vecruntime] class VectorSingleDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    writeFile: Boolean
) extends FileFormatDataWriter(description, taskAttemptContext, committer, Map.empty)
    with VectorBatchWriter {

  private var fileCounter = 0
  private var recordsInFile = 0L

  if (writeFile) newOutputWriter()

  private def newOutputWriter(): Unit = {
    recordsInFile = 0
    releaseResources()
    val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val currentPath = committer.newTaskTempFile(taskAttemptContext, None, FileNameSpec("", f"-c$fileCounter%03d" + ext))
    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext
    )
    statsTrackers.foreach(_.newFile(currentPath))
  }

  override def write(record: InternalRow): Unit =
    throw new UnsupportedOperationException("VectorWriteFilesExec writes batches")

  override def writeBatch(batch: ColumnarBatch, ordinals: Array[Int]): Unit = {
    val arena = Arena.ofConfined()
    try {
      val (cols, n) = VectorWriteTask2.columns(batch, ordinals, arena)
      var from = 0
      while (from < n) {
        if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
          fileCounter += 1
          newOutputWriter()
        }
        val room =
          if (description.maxRecordsPerFile > 0) math.min(n - from, description.maxRecordsPerFile - recordsInFile).toInt
          else n - from
        val to = from + room
        val w = currentWriter.asInstanceOf[VectorParquetOutputWriter]
        w.writeColumns(cols, from, to)
        var r = from
        while (r < to) {
          statsTrackers.foreach(_.newRow(w.path(), null))
          r += 1
        }
        recordsInFile += to - from
        from = to
      }
    } finally arena.close()
  }
}

/**
 * Concurrent output writers for one write (`spark.sql.maxConcurrentOutputFileWriters` > 0): the writer limit, and
 * our sort of the rest of a task's input once it is reached -- by the partition columns, then the bucket id, as
 * Spark's `FileFormatWriter` sorts it (compiled sort keys over the child's output).
 */
private[vecruntime] final case class VectorConcurrentWriters(
    maxWriters: Int,
    keys: Array[io.vecruntime.spark.expr.VectorExpr],
    ascending: Array[Boolean],
    nullsFirst: Array[Boolean],
    outputAttrs: Array[(String, DataType)],
    metrics: VectorMetrics,
    runRows: Int,
    spillBytes: Long,
    memoryMode: org.apache.spark.memory.MemoryMode
)

/** A data writer fed batches: `ordinals` are the data columns' positions in the batch. */
private[vecruntime] trait VectorBatchWriter {
  def writeBatch(batch: ColumnarBatch, ordinals: Array[Int]): Unit
}

/**
 * `DynamicPartitionDataSingleWriter` for batches (#842): partitioned and/or bucketed writes over input sorted by
 * the partition columns and then the bucket id, as `V1Writes` orders a `WriteFiles` child. Each batch is cut into
 * runs of equal partition values and bucket id; every run is written as one range of the batch's columns into
 * the current file, and a new run opens the next file through Spark's own `renewCurrentWriter` -- so the
 * partition directories (escaping, `__HIVE_DEFAULT_PARTITION__`, custom partition locations), the
 * `-bucketid.cNNN` file names, `maxRecordsPerFile` and the stats trackers' `newPartition` / `newFile` are Spark's.
 *
 * Partition values are compared lane by lane between adjacent rows and projected (Spark's `getPartitionValues`)
 * once per run. Bucket ids are Spark's `pmod(murmur3hash(columns, 42), numBuckets)` computed per batch by the
 * shuffle's hash kernels over the data lanes; a bucket expression of another shape (or key types those kernels
 * do not hash as Spark does) is evaluated row by row with Spark's own projection.
 */
private[vecruntime] class VectorDynamicPartitionDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    allOrdinals: Array[Int],
    concurrent: Option[VectorConcurrentWriters] = None
) extends BaseDynamicPartitionDataWriter(description, taskAttemptContext, committer, Map.empty)
    with VectorBatchWriter {

  import org.apache.spark.sql.catalyst.expressions.{
    Attribute,
    CollationAwareMurmur3Hash,
    Literal,
    Murmur3Hash,
    Pmod,
    UnsafeRow
  }
  import io.vecruntime.kernels.PartitionKernels
  import io.vecruntime.kernels.PartitionKernels.KeyKind

  private var currentPartitionValues: Option[UnsafeRow] = None
  private var currentBucketId: Option[Int] = None

  /** Positions of the partition columns in `allColumns` (and so in the reordered batch), with their types. */
  private val partitionOrdinals: Array[Int] =
    description.partitionColumns.map(p => description.allColumns.indexWhere(_.exprId == p.exprId)).toArray
  private val partitionTypes: Array[DataType] = description.partitionColumns.map(_.dataType).toArray

  /**
   * The bucket keys as positions in the data columns with their hash kinds, when the bucket expression is Spark's
   * `Pmod(Murmur3Hash(keys, 42), n)` over data columns of types the kernels hash as Spark does.
   */
  private val vectorBuckets: Option[(Array[Int], Array[KeyKind], Int)] = description.bucketSpec.flatMap { spec =>
    spec.bucketIdExpression match {
      case p: Pmod =>
        val hashed = p.left match {
          case Murmur3Hash(children, 42) => Some(children)
          case CollationAwareMurmur3Hash(children, 42) => Some(children)
          case _ => None
        }
        (hashed, p.right) match {
          case (Some(children), Literal(n: Int, IntegerType)) if children.forall(_.isInstanceOf[Attribute]) =>
            val keys =
              children.map(c => description.dataColumns.indexWhere(_.exprId == c.asInstanceOf[Attribute].exprId))
            val kinds = children.map(c => VectorDynamicPartitionDataWriter.hashKind(c.dataType))
            if (keys.forall(_ >= 0) && kinds.forall(_ != null)) Some((keys.toArray, kinds.toArray, n)) else None
          case _ => None
        }
      case _ => None
    }
  }

  override def write(record: InternalRow): Unit =
    throw new UnsupportedOperationException("VectorWriteFilesExec writes batches")

  override def writeBatch(batch: ColumnarBatch, ordinals: Array[Int]): Unit = {
    val arena = Arena.ofConfined()
    try {
      val (cols, n) = VectorWriteTask2.columns(batch, ordinals, arena)
      if (n == 0) return
      // The batch's columns in allColumns order, which Spark's projections are bound to; `rowOf(k)` is the
      // physical row of logical (selected) row k.
      val all = new ColumnarBatch(allOrdinals.map(batch.column), batch.numRows())
      val physical: Array[Int] = batch match {
        case s: SelectedColumnarBatch if s.selectedCount() < batch.numRows() =>
          VectorWriteTask2.selectedRows(s.selection(), batch.numRows(), n)
        case _ => null
      }
      def rowOf(k: Int): Int = if (physical == null) k else physical(k)
      val bucketIds: Array[Int] = if (!isBucketed) null else bucketIdsOf(cols, n, all, rowOf)

      var from = 0
      while (from < n) {
        // The run of rows equal to `from` in partition values and bucket id.
        var to = from + 1
        while (
          to < n && samePartition(
            all,
            rowOf(to - 1),
            rowOf(to)
          ) && (bucketIds == null || bucketIds(to) == bucketIds(from))
        ) {
          to += 1
        }
        startRun(all, rowOf(from), if (bucketIds == null) None else Some(bucketIds(from)))
        writeRun(cols, from, to)
        from = to
        if (concurrent.isDefined && sorted && !sortStarted && from < n) {
          // The writers reached their limit within this batch: its remaining rows go to the sort first.
          pending = SelectedColumnarBatch.ofIndices(
            SelectedColumnarBatch.columnsOf(batch),
            batch.numRows(),
            Array.tabulate(n - from)(k => rowOf(from + k)),
            n - from,
            false
          )
          return
        }
      }
    } finally arena.close()
  }

  /** Opens the run's file when its partition or bucket differs from the current one (Spark's `write`). */
  private def startRun(all: ColumnarBatch, row: Int, bucketId: Option[Int]): Unit = {
    val partitionValues = if (isPartitioned) Some(getPartitionValues(all.getRow(row))) else None
    if (concurrent.isDefined) {
      if (currentWriter == null || currentPartitionValues != partitionValues || currentBucketId != bucketId) {
        switchConcurrent(partitionValues.map(_.copy()), bucketId)
      }
    } else if (currentWriter == null || currentPartitionValues != partitionValues || currentBucketId != bucketId) {
      if (isPartitioned && currentPartitionValues != partitionValues) {
        currentPartitionValues = Some(partitionValues.get.copy())
        statsTrackers.foreach(_.newPartition(currentPartitionValues.get))
      }
      if (isBucketed) currentBucketId = bucketId
      fileCounter = 0
      renewCurrentWriter(currentPartitionValues, currentBucketId, closeCurrentWriter = true)
    }
  }

  // --- Concurrent writers (`spark.sql.maxConcurrentOutputFileWriters` > 0): Spark's
  // `DynamicPartitionDataConcurrentWriter` for batches. The input is not sorted; up to `maxWriters` files stay
  // open, one per partition and bucket, and each run goes to its own. Opening the `maxWriters`-th file switches
  // to sorted mode: the rest of the input (the batch's remaining rows, then every later batch) is sorted by our
  // sort and written as runs, a file closed when its run ends.

  private final class Status(var writer: OutputWriter, var records: Long, var counter: Int)

  private val writers = scala.collection.mutable.HashMap[(Option[UnsafeRow], Option[Int]), Status]()

  /** Whether the writers reached their limit (always, without concurrent writers). */
  private[vecruntime] var sorted: Boolean = concurrent.isEmpty

  /** Whether the sorted rest of the input has started. */
  private var sortStarted: Boolean = false

  /** The rows of the batch during which the limit was reached that are still to write, as a selection. */
  private var pending: ColumnarBatch = null

  /** `pending` once handed to the sort, closed (its selection) with the writer. */
  private var sortedPending: ColumnarBatch = null

  private def currentKey: (Option[UnsafeRow], Option[Int]) = (currentPartitionValues, currentBucketId)

  /** Stores the current writer's file and counters under its partition and bucket. */
  private def saveStatus(): Unit = writers.get(currentKey).foreach { s =>
    s.writer = currentWriter
    s.records = recordsInFile
    s.counter = fileCounter
  }

  /** Spark's `setupCurrentWriterUsingMap`: makes the writer of `(partitionValues, bucketId)` current. */
  private def switchConcurrent(partitionValues: Option[UnsafeRow], bucketId: Option[Int]): Unit = {
    if (currentWriter != null) {
      if (!sorted || !sortStarted) saveStatus()
      else {
        writers.remove(currentKey)
        releaseCurrentWriter()
      }
    }
    val key = (partitionValues, bucketId)
    if (isPartitioned && currentPartitionValues != partitionValues && !writers.contains(key)) {
      statsTrackers.foreach(_.newPartition(partitionValues.get))
    }
    currentPartitionValues = partitionValues
    currentBucketId = bucketId
    writers.get(key) match {
      case Some(s) =>
        currentWriter = s.writer
        recordsInFile = s.records
        fileCounter = s.counter
      case None =>
        fileCounter = 0
        renewCurrentWriter(partitionValues, bucketId, closeCurrentWriter = false)
        writers.put(key, new Status(currentWriter, recordsInFile, fileCounter))
        if (!sorted && writers.size >= concurrent.get.maxWriters) sorted = true
    }
  }

  /**
   * The rest of the input in our sort's order, once the writers reached their limit: the pending rows of the
   * batch that reached it, then `rest`. Spark's `clearCurrentWriterStatus` first.
   */
  private[vecruntime] def sortedRest(rest: Iterator[ColumnarBatch]): VectorSortIterator = {
    val c = concurrent.get
    if (currentWriter != null) saveStatus()
    currentPartitionValues = None
    currentBucketId = None
    currentWriter = null
    recordsInFile = 0
    fileCounter = 0
    sortStarted = true
    val first = pending
    pending = null
    sortedPending = first
    new VectorSortIterator(
      Option(first).iterator ++ rest,
      c.keys,
      c.ascending,
      c.nullsFirst,
      c.outputAttrs,
      c.metrics,
      runRows = c.runRows,
      spillBytes = c.spillBytes,
      memoryMode = c.memoryMode
    )
  }

  override protected def releaseResources(): Unit =
    if (concurrent.isEmpty) super.releaseResources()
    else {
      if (pending != null) { pending.close(); pending = null }
      if (sortedPending != null) { sortedPending.close(); sortedPending = null }
      if (currentWriter != null && !writers.contains(currentKey)) super.releaseResources()
      else if (currentWriter != null) saveStatus()
      currentWriter = null
      writers.values.foreach { s =>
        if (s.writer != null) {
          try {
            s.writer.close()
            statsTrackers.foreach(_.closeFile(s.writer.path()))
          } finally s.writer = null
        }
      }
      writers.clear()
    }

  /** Rows `[from, to)` into the current file, rolling files at `maxRecordsPerFile`. */
  private def writeRun(cols: Array[VectorBuffers], from0: Int, to0: Int): Unit = {
    var from = from0
    while (from < to0) {
      if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
        renewCurrentWriterIfTooManyRecords(currentPartitionValues, currentBucketId)
        if (concurrent.isDefined) saveStatus()
      }
      val room =
        if (description.maxRecordsPerFile > 0) math.min(to0 - from, description.maxRecordsPerFile - recordsInFile).toInt
        else to0 - from
      val to = from + room
      val w = currentWriter.asInstanceOf[VectorParquetOutputWriter]
      w.writeColumns(cols, from, to)
      var r = from
      while (r < to) {
        statsTrackers.foreach(_.newRow(w.path(), null))
        r += 1
      }
      recordsInFile += to - from
      from = to
    }
  }

  private def bucketIdsOf(cols: Array[VectorBuffers], n: Int, all: ColumnarBatch, rowOf: Int => Int): Array[Int] = {
    val ids = new Array[Int](n)
    vectorBuckets match {
      case Some((keys, kinds, num)) =>
        PartitionKernels.hashPartitionIds(keys.map(cols), kinds, n, num, new Array[Int](n), ids)
      case None =>
        var k = 0
        while (k < n) {
          ids(k) = getBucketId(all.getRow(rowOf(k)))
          k += 1
        }
    }
    ids
  }

  /** Whether physical rows `a` and `b` hold the same partition values (nulls equal), as `UnsafeRow` equality. */
  private def samePartition(all: ColumnarBatch, a: Int, b: Int): Boolean = {
    var i = 0
    while (i < partitionOrdinals.length) {
      val c = all.column(partitionOrdinals(i))
      val na = c.isNullAt(a)
      if (na != c.isNullAt(b)) return false
      if (!na) {
        val same = partitionTypes(i) match {
          case BooleanType => c.getBoolean(a) == c.getBoolean(b)
          case ByteType => c.getByte(a) == c.getByte(b)
          case ShortType => c.getShort(a) == c.getShort(b)
          case IntegerType | DateType => c.getInt(a) == c.getInt(b)
          case LongType | TimestampType | TimestampNTZType => c.getLong(a) == c.getLong(b)
          case FloatType =>
            java.lang.Float.floatToIntBits(c.getFloat(a)) == java.lang.Float.floatToIntBits(c.getFloat(b))
          case DoubleType =>
            java.lang.Double.doubleToLongBits(c.getDouble(a)) == java.lang.Double.doubleToLongBits(c.getDouble(b))
          case _: StringType => c.getUTF8String(a) == c.getUTF8String(b)
          case BinaryType => java.util.Arrays.equals(c.getBinary(a), c.getBinary(b))
          case d: DecimalType => c.getDecimal(a, d.precision, d.scale) == c.getDecimal(b, d.precision, d.scale)
          case _ => getPartitionValues(all.getRow(a)).copy() == getPartitionValues(all.getRow(b))
        }
        if (!same) return false
      }
      i += 1
    }
    true
  }
}

private[vecruntime] object VectorDynamicPartitionDataWriter {

  /**
   * The kernels' hash kind for a bucket key of type `dt` when it equals Spark's `Murmur3Hash` of that type over
   * our lane; null otherwise (the bucket ids are then evaluated by Spark's projection).
   */
  def hashKind(dt: DataType): io.vecruntime.kernels.PartitionKernels.KeyKind = {
    import io.vecruntime.kernels.PartitionKernels.KeyKind
    dt match {
      case ByteType | ShortType | IntegerType | DateType => KeyKind.INT
      case LongType | TimestampType | TimestampNTZType => KeyKind.LONG
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS => KeyKind.LONG
      case _: DecimalType => KeyKind.DECIMAL128
      case DoubleType => KeyKind.DOUBLE
      case FloatType => KeyKind.FLOAT
      case BooleanType => KeyKind.BOOL
      case s: StringType if s.supportsBinaryEquality => KeyKind.UTF8
      case _ => null
    }
  }
}

private[vecruntime] object VectorWriteTask2 {

  /** The data columns of `batch` as `VectorBuffers` (at `ordinals`), compacted to its selection; and their rows. */
  def columns(batch: ColumnarBatch, ordinals: Array[Int], arena: Arena): (Array[VectorBuffers], Int) = {
    val rows = batch.numRows()
    val selection = batch match {
      case s: SelectedColumnarBatch if s.selectedCount() < rows => Some(s)
      case _ => None
    }
    // A struct, array or map column (#842) as its nested lane: read in place from our scan; built from Spark's
    // getters (plain strings, which the gather below takes) when the batch has a selection to apply.
    def adapt(o: Int): VectorBuffers = {
      val cv = batch.column(o)
      cv.dataType() match {
        case dt @ (_: StructType | _: ArrayType | _: MapType | _: org.apache.spark.sql.types.VariantType)
            if selection.isEmpty =>
          io.vecruntime.spark.adapter.NestedColumns.adaptForWrite(cv, dt, rows, arena)
        case dt @ (_: StructType | _: ArrayType | _: MapType | _: org.apache.spark.sql.types.VariantType) =>
          io.vecruntime.spark.adapter.NestedColumns.adapt(cv, dt, rows, arena)
        case _ => ColumnVectorAdapters.adapt(cv, rows, arena)
      }
    }
    val columns = ordinals.map(adapt)
    selection match {
      case Some(s) =>
        lazy val selected = selectedRows(s.selection(), rows, s.selectedCount())
        val compacted = columns.map { c =>
          if (c.`type`().isNested())
            io.vecruntime.spark.expr.ElementAccessExpr.gather(c, selected, s.selectedCount(), arena)
          else compact(c, s.selection(), s.selectedCount(), arena)
        }
        (compacted, s.selectedCount())
      case None => (columns, rows)
    }
  }

  /** The physical rows set in a selection bitmap of `rows` bits, `count` of them, in order. */
  def selectedRows(selection: MemorySegment, rows: Int, count: Int): Array[Int] = {
    val out = new Array[Int](count)
    var k = 0
    var w = 0
    while (w * 64 < rows) {
      var word =
        if ((w + 1) * 64 <= rows) selection.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, w * 8L)
        else {
          var v = 0L
          var b = w * 64
          while (b < rows) {
            if ((selection.get(java.lang.foreign.ValueLayout.JAVA_BYTE, (b >>> 3).toLong) & (1 << (b & 7))) != 0) {
              v |= 1L << (b - w * 64)
            }
            b += 1
          }
          v
        }
      while (word != 0) {
        out(k) = w * 64 + java.lang.Long.numberOfTrailingZeros(word)
        k += 1
        word &= word - 1
      }
      w += 1
    }
    out
  }

  /** A column compacted to the selected rows, into `arena` (dictionary ids keep their dictionary). */
  def compact(in: VectorBuffers, selection: MemorySegment, count: Int, arena: Arena): VectorBuffers = {
    val validity = if (in.hasNulls) ArrowLayout.allocateBitmap(arena, count) else null
    if (in.`type`() == VecType.UTF8 && !in.isDictionaryEncoded) {
      val bytes = CompactKernels.selectedUtf8Bytes(in, selection)
      val offsets = ArrowLayout.allocateOffsets(arena, count)
      val data = ArrowLayout.allocateBytes(arena, bytes)
      CompactKernels.compactUtf8(in, selection, count, offsets, data, validity)
      SegmentVectorBuffers.utf8(count, validity, offsets, data)
    } else {
      val physical = if (in.isDictionaryEncoded) VecType.INT32 else in.`type`()
      val data =
        if (physical == VecType.BOOL) ArrowLayout.allocateBitmap(arena, count)
        else ArrowLayout.allocateData(arena, physical, count)
      CompactKernels.compactFixed(in, selection, count, data, validity)
      if (in.isDictionaryEncoded) SegmentVectorBuffers.dictionaryUtf8(count, validity, data, in.dictionary())
      else SegmentVectorBuffers.fixedWidth(in.`type`(), count, validity, data)
    }
  }
}

/**
 * Opens a [[VectorParquetOutputWriter]] per file. The file extension comes from Spark's own factory (the codec's
 * extension plus `.parquet`), so the files are named as Spark names them.
 */
private[vecruntime] class VectorParquetOutputWriterFactory(spark: OutputWriterFactory) extends OutputWriterFactory {

  override def getFileExtension(context: TaskAttemptContext): String = spark.getFileExtension(context)

  override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter =
    new VectorParquetOutputWriter(path, dataSchema, context.getConfiguration)
}

/**
 * One Parquet file written from columns. The schema is Spark's own (`SparkToParquetSchemaConverter` over the same
 * configuration `ParquetFileFormat.prepareWrite` filled in), the footer carries the keys `ParquetWriteSupport`
 * writes (Spark's version and the Spark schema), and the page, dictionary, row-group, statistics and codec
 * settings are read from the same `parquet.*` keys parquet-java's `ParquetOutputFormat` reads.
 */
private[vecruntime] class VectorParquetOutputWriter(val filePath: String, dataSchema: StructType, conf: Configuration)
    extends OutputWriter {

  private val codecs = new CodecFactory(conf, ParquetOutputFormat.getPageSize(conf))

  private val writer: ColumnarParquetWriter = {
    val schema = new SparkToParquetSchemaConverter(conf).convert(dataSchema)
    val builder = ParquetProperties
      .builder()
      .withPageSize(ParquetOutputFormat.getPageSize(conf))
      .withDictionaryPageSize(ParquetOutputFormat.getDictionaryPageSize(conf))
      .withDictionaryEncoding(ParquetOutputFormat.getEnableDictionary(conf))
      .withPageRowCountLimit(conf.getInt(
        "parquet.page.row.count.limit",
        ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT
      ))
      .withColumnIndexTruncateLength(
        conf.getInt("parquet.columnindex.truncate.length", ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH)
      )
      .withStatisticsTruncateLength(
        conf.getInt("parquet.statistics.truncate.length", ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH)
      )
      .withPageWriteChecksumEnabled(ParquetOutputFormat.getPageWriteChecksumEnabled(conf))
    // Per-column dictionary switches (`parquet.enable.dictionary#<column path>`), as ParquetOutputFormat reads them.
    val dictionaryPrefix = ParquetOutputFormat.ENABLE_DICTIONARY + "#"
    conf.iterator().asScala.map(e => e.getKey -> e.getValue).filter(_._1.startsWith(dictionaryPrefix)).foreach {
      case (k, v) => builder.withDictionaryEncoding(k.substring(dictionaryPrefix.length), v.trim.toBoolean)
    }
    // Bloom filters (#842), global and per column, as ParquetOutputFormat reads them.
    builder
      .withBloomFilterEnabled(ParquetOutputFormat.getBloomFilterEnabled(conf))
      .withMaxBloomFilterBytes(ParquetOutputFormat.getBloomFilterMaxBytes(conf))
      .withAdaptiveBloomFilterEnabled(ParquetOutputFormat.getAdaptiveBloomFilterEnabled(conf))
    conf.iterator().asScala.map(e => e.getKey -> e.getValue.trim).foreach {
      case (k, v) if k.startsWith(ParquetOutputFormat.BLOOM_FILTER_ENABLED + "#") =>
        builder.withBloomFilterEnabled(k.substring(ParquetOutputFormat.BLOOM_FILTER_ENABLED.length + 1), v.toBoolean)
      case (k, v) if k.startsWith(ParquetOutputFormat.BLOOM_FILTER_EXPECTED_NDV + "#") =>
        builder.withBloomFilterNDV(k.substring(ParquetOutputFormat.BLOOM_FILTER_EXPECTED_NDV.length + 1), v.toLong)
      case (k, v) if k.startsWith(ParquetOutputFormat.BLOOM_FILTER_FPP + "#") =>
        builder.withBloomFilterFPP(k.substring(ParquetOutputFormat.BLOOM_FILTER_FPP.length + 1), v.toDouble)
      case (k, v) if k.startsWith(ParquetOutputFormat.BLOOM_FILTER_CANDIDATES_NUMBER + "#") =>
        builder.withBloomFilterCandidatesNumber(
          k.substring(ParquetOutputFormat.BLOOM_FILTER_CANDIDATES_NUMBER.length + 1),
          v.toInt
        )
      case _ =>
    }
    val props = builder.build()
    val footer = Map(
      org.apache.spark.sql.SPARK_VERSION_METADATA_KEY -> SPARK_VERSION_SHORT,
      ParquetReadSupport.SPARK_METADATA_KEY -> conf.get(ParquetWriteSupport.SPARK_ROW_SCHEMA, dataSchema.json)
    ).asJava
    val out = HadoopOutputFile.fromPath(new Path(filePath), conf)
    try {
      new ColumnarParquetWriter(
        out,
        schema,
        props,
        codecs.getCompressor(ParquetOutputFormat.getCompression(conf)),
        ParquetOutputFormat.getLongBlockSize(conf),
        footer
      )
    } catch {
      case t: Throwable =>
        codecs.release()
        throw t
    }
  }

  def writeColumns(columns: Array[VectorBuffers], from: Int, to: Int): Unit =
    writer.write(columns.toSeq.asJava, from, to)

  override def write(row: InternalRow): Unit =
    throw new UnsupportedOperationException("VectorParquetOutputWriter writes columns")

  override def close(): Unit =
    try writer.close()
    finally codecs.release()

  override def path(): String = filePath
}
