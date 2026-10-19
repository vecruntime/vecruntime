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
 * committer's `abortTask` on failure. Only unpartitioned, unbucketed writes convert ([[VectorWritePlanner]]).
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
          ordinals
        )
      )
    }
  }

  override protected def stringArgs: Iterator[Any] = Iterator(child)

  override protected def withNewChildInternal(newChild: SparkPlan): VectorWriteFilesExec = copy(child = newChild)
}

/** Which `WriteFilesExec` nodes convert, and why the others do not (the reasons tests and the UI show). */
object VectorWritePlanner {

  /** Hadoop / Parquet options the prototype does not reproduce: the write keeps Spark's writer. */
  private val UnsupportedOptions = Seq(
    "parquet.bloom.filter.enabled",
    "parquet.crypto.factory.class",
    "parquet.encryption.kms.client.class"
  )

  def reason(w: WriteFilesExec, session: SparkSession): Option[String] = {
    val conf = session.sessionState.conf
    if (w.fileFormat.getClass != classOf[ParquetFileFormat]) {
      Some(s"columnar write: format ${w.fileFormat.getClass.getSimpleName} is not Parquet")
    } else if (w.partitionColumns.nonEmpty) {
      Some("columnar write: partitioned writes are not supported yet (#842)")
    } else if (w.bucketSpec.isDefined) {
      Some("columnar write: bucketed writes are not supported yet (#842)")
    } else if (!w.child.supportsColumnar) {
      Some(s"columnar write: child ${w.child.nodeName} is not columnar")
    } else {
      val hadoopConf = session.sessionState.newHadoopConfWithOptions(w.options)
      def option(k: String): Option[String] = w.options.get(k).orElse(Option(hadoopConf.get(k)))
      val options = UnsupportedOptions.find(k => option(k).exists(v => v != "false"))
      lazy val perColumn = (hadoopConf.iterator().asScala.map(_.getKey) ++ w.options.keys)
        .find(k => k.startsWith("parquet.") && k.contains("#"))
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
      ordinals: Array[Int]
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
    var dataWriter: VectorSingleDirectoryDataWriter = null
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      // As Spark: an empty partition other than the first writes no file (the first writes the schema).
      val writeFile = sparkPartitionId == 0 || batches.hasNext
      dataWriter = new VectorSingleDirectoryDataWriter(description, taskAttemptContext, committer, writeFile)
      while (batches.hasNext) {
        dataWriter.writeBatch(batches.next(), ordinals)
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
) extends FileFormatDataWriter(description, taskAttemptContext, committer, Map.empty) {

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

  def writeBatch(batch: ColumnarBatch, ordinals: Array[Int]): Unit = {
    val arena = Arena.ofConfined()
    try {
      val rows = batch.numRows()
      val columns = ordinals.map(o => ColumnVectorAdapters.adapt(batch.column(o), rows, arena))
      val (cols, n) = batch match {
        case s: SelectedColumnarBatch if s.selectedCount() < rows =>
          (columns.map(c => VectorWriteTask2.compact(c, s.selection(), s.selectedCount(), arena)), s.selectedCount())
        case _ => (columns, rows)
      }
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

private[vecruntime] object VectorWriteTask2 {

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
    val props = ParquetProperties
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
      .build()
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
