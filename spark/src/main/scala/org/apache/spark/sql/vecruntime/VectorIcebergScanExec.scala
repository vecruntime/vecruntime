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
 *  - anything else (delete files, another format): Iceberg's own columnar reader for that group, whose batches
 *    our operators adapt as they do today.
 * No filter is pushed into the native reader yet: Iceberg's reader only skips row groups by the residual, and
 * Spark keeps the filter above the scan, so reading every row group returns the same rows.
 */
case class VectorIcebergScanExec(scan: BatchScanExec, fieldIds: Seq[Int]) extends LeafExecNode with VectorPlan {

  override val output: Seq[Attribute] = scan.output
  override def outputPartitioning: Partitioning = scan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = scan.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numRowGroups" -> SQLMetrics.createMetric(sparkContext, "number of row groups read"),
    "numLateSkippedRows" -> SQLMetrics.createMetric(sparkContext, "rows the decode filter dropped before decoding"),
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

  override def simpleString(maxFields: Int): String = s"VectorIcebergScan ${scan.simpleString(maxFields)}"

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val sqlConf = session.sessionState.conf
    // The data columns, each carrying its Iceberg field id, so the reader matches the file's columns by id.
    val requiredSchema = StructType(output.zip(fieldIds).map { case (a, id) =>
      StructField(
        a.name,
        a.dataType,
        a.nullable,
        new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id.toLong).build()
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
      session.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    }
    val confBroadcast = confFor(requiredSchema)
    val positionsConfBroadcast = confFor(VectorIcebergRDD.withPositions(requiredSchema))

    val m = ScanMetrics(
      longMetric("numFiles"),
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"),
      longMetric("numRowGroups"),
      longMetric("scanTime"),
      longMetric("numLateSkippedRows")
    )
    new VectorIcebergRDD(
      session.sparkContext,
      scan.inputRDD.partitions,
      scan.readerFactory,
      fieldIds.toArray,
      confBroadcast,
      positionsConfBroadcast,
      requiredSchema,
      output.map(a => (a.name, a.dataType)).toArray,
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
      m,
      longMetric("numNativeGroups"),
      longMetric("numIcebergGroups")
    )
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
    requiredSchema: StructType,
    attrs: Array[(String, org.apache.spark.sql.types.DataType)],
    batchSize: Int,
    rebaseConf: NativeScanRebaseConf,
    prefetchFiles: Int,
    prefetchRowGroups: Int,
    decodeAhead: Int,
    decodeAheadVirtual: Boolean,
    dictionaryStrings: Boolean,
    prefetchMaxBytes: Long,
    prefetchExecutorMaxBytes: Long,
    metrics: ScanMetrics,
    nativeGroups: SQLMetric,
    icebergGroups: SQLMetric
) extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = parts

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    shims.SparkShims.dataSourceInputPartitions(split).flatMap(_.preferredLocations()).distinct

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val inputs = shims.SparkShims.dataSourceInputPartitions(split)
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
    inputs.foreach { p =>
      IcebergTasks.nativeTasks(p) match {
        case Some(tasks) =>
          nativeGroups += 1
          tasks.foreach { t =>
            if (t.deletes.isEmpty) {
              pending += t.file
              pendingCount += 1
            } else {
              // A file with position deletes or a deletion vector: read with its row positions, the deleted ones
              // deselected (its delete files are loaded when its first batch is read).
              flush()
              segments += (() => deletedIterator(split.index, p, t, context))
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

  private def deletedIterator(
      index: Int,
      p: InputPartition,
      t: IcebergTasks.NativeTask,
      context: TaskContext
  ): Iterator[ColumnarBatch] = {
    lazy val deleted = IcebergTasks.deletedPositions(p, t.location, t.deletes)
    readerOf(index, Array(t.file), positionsConfBroadcast, VectorIcebergRDD.withPositions(requiredSchema), context)
      .map(b => IcebergTasks.withoutDeleted(b, deleted))
  }

  private def nativeIterator(
      index: Int,
      files: Array[PartitionedFile],
      context: TaskContext
  ): Iterator[ColumnarBatch] = readerOf(index, files, confBroadcast, requiredSchema, context)

  private def readerOf(
      index: Int,
      files: Array[PartitionedFile],
      conf: org.apache.spark.broadcast.Broadcast[SerializableConfiguration],
      schema: StructType,
      context: TaskContext
  ): Iterator[ColumnarBatch] = {
    val columns =
      if (schema.length == attrs.length) attrs
      else attrs :+ ((schema.last.name, schema.last.dataType))
    val reader = new VectorParquetPartitionReader(
      FilePartition(index, files),
      conf.value.value,
      schema,
      new StructType(),
      columns,
      batchSize,
      caseSensitive = false,
      useFieldId = true,
      pushedFilters = Nil,
      pushDownDate = false,
      pushDownTimestamp = false,
      pushDownDecimal = false,
      pushDownStringPredicate = false,
      pushDownInFilterThreshold = 0,
      rebaseConf = rebaseConf,
      prefetchFiles = prefetchFiles,
      prefetchRowGroups = prefetchRowGroups,
      metrics = metrics,
      context = context,
      handOff = decodeAhead > 0,
      poolLimit = math.max(4, decodeAhead + 2),
      dictionaryStrings = dictionaryStrings,
      prefetchMaxBytes = prefetchMaxBytes,
      prefetchExecutorMaxBytes = prefetchExecutorMaxBytes
    )
    if (decodeAhead > 0) new DecodeAheadIterator(reader, decodeAhead, decodeAheadVirtual, context) else reader
  }

  private def icebergIterator(p: InputPartition, context: TaskContext): Iterator[ColumnarBatch] = {
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

/** Helpers of [[VectorIcebergRDD]]. */
private[vecruntime] object VectorIcebergRDD {

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
    if (!b.supportsColumnar) return Left("Iceberg scan is not vectorized")
    if (b.outputOrdering.nonEmpty) return Left("Iceberg scan reports an ordering")
    val ids = IcebergTasks.outputFieldIds(b) match {
      case Left(reason) => return Left(reason)
      case Right(v) => v
    }
    b.output.foreach { a =>
      if (
        !a.dataType.isInstanceOf[AtomicType] || !io.vecruntime.spark.parquet.NativeParquetSupport.isReadable(a.dataType)
      ) {
        return Left(s"unsupported column type ${a.name}:${a.dataType.simpleString}")
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

  /** The Iceberg field ids of `b`'s output, in order, or why the scan cannot be read natively. */
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
        val field = byName.getOrElse(a.name.toLowerCase, return Left(s"column ${a.name} not in the Iceberg projection"))
        val id = call(field, "fieldId").asInstanceOf[Integer].intValue()
        // Metadata columns (_file, _pos, _deleted, _spec_id, _partition, row lineage) have reserved ids at the top.
        if (id >= Int.MaxValue - 10000) return Left(s"Iceberg metadata column ${a.name}")
        // A v3 initial default fills the column for files written before it was added; our reader reads nulls.
        if (call(field, "initialDefault") != null) return Left(s"Iceberg column ${a.name} has an initial default")
        val typeId = call(call(field, "type"), "typeId").toString
        if (!NativeTypes.contains(typeId)) return Left(s"Iceberg type $typeId of ${a.name}")
        id
      })
    } catch {
      case e: ReflectiveOperationException => Left(s"Iceberg scan not recognised: $e")
    }

  /** One file scan task read natively: its split, the data file's location, and its position delete files. */
  final case class NativeTask(file: PartitionedFile, location: String, deletes: java.util.List[AnyRef])

  /**
   * The group's tasks as file splits when every task is an unencrypted Parquet data file whose delete files (if
   * any) are all position deletes or deletion vectors; None when Iceberg must read it (equality deletes, another
   * format, encryption). (Scan tasks carry no column statistics, so a projected column a file lacks is found at
   * open: the reader reads an optional missing column as nulls, as Iceberg does without an initial default.)
   */
  def nativeTasks(p: InputPartition): Option[Array[NativeTask]] = {
    if (p.getClass.getName != "org.apache.iceberg.spark.source.SparkInputPartition") return None
    val tasks = call(call(p, "taskGroup"), "tasks").asInstanceOf[java.util.Collection[AnyRef]].asScala
    val out = tasks.map { task =>
      if (!task.getClass.getName.contains("FileScanTask")) return None
      val deletes = call(task, "deletes").asInstanceOf[java.util.List[AnyRef]]
      val positionDeletes =
        if (deletes == null) java.util.Collections.emptyList[AnyRef]()
        else {
          deletes.asScala.foreach { d =>
            // Equality deletes need the rows' values (Iceberg's DeleteFilter); an encrypted delete file needs
            // Iceberg's decryption.
            if (call(d, "content").toString != "POSITION_DELETES" || call(d, "keyMetadata") != null) return None
          }
          deletes
        }
      val file = call(task, "file")
      if (call(file, "format").toString != "PARQUET") return None
      // An encrypted file is decrypted by Iceberg's FileIO (bulkDecrypt); our reader would read its bytes raw.
      if (call(file, "keyMetadata") != null) return None
      val location = call(file, "location").toString
      val size = call(file, "fileSizeInBytes").asInstanceOf[java.lang.Long].longValue()
      NativeTask(
        PartitionedFile(
          InternalRow.empty,
          SparkPath.fromPath(new Path(location)),
          call(task, "start").asInstanceOf[java.lang.Long].longValue(),
          call(task, "length").asInstanceOf[java.lang.Long].longValue(),
          Array.empty[String],
          0L,
          size
        ),
        location,
        positionDeletes
      )
    }
    Some(out.toArray)
  }

  /**
   * The deleted row positions of one data file, ascending, from its position delete files or deletion vector,
   * loaded by Iceberg's own `BaseDeleteLoader` through the partition's `FileIO` (as Iceberg's reader loads
   * them, so position deletes and deletion vectors keep Iceberg's semantics, including the DV's referenced-file
   * check).
   */
  def deletedPositions(p: InputPartition, location: String, deletes: java.util.List[AnyRef]): Array[Long] = {
    val cl = p.getClass.getClassLoader
    val deleteFileClass = Class.forName("org.apache.iceberg.DeleteFile", false, cl)
    val newInputFile =
      Class.forName("org.apache.iceberg.io.FileIO", false, cl).getMethod("newInputFile", deleteFileClass)
    val io = call(p, "io")
    val open: java.util.function.Function[AnyRef, AnyRef] = d => newInputFile.invoke(io, d)
    val loaderClass = Class.forName("org.apache.iceberg.data.BaseDeleteLoader", false, cl)
    val loader = loaderClass.getConstructor(classOf[java.util.function.Function[_, _]]).newInstance(open)
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
   * `batch` without its last column -- the file's row positions (Spark's temporary row-index column, made by our
   * reader and by Spark's fallback reader alike) -- and with the rows at `deleted` positions deselected. Rows of a
   * batch come in file order, so the walk through `deleted` is one pass. The columns stay the reader's (borrowed).
   */
  def withoutDeleted(batch: ColumnarBatch, deleted: Array[Long]): ColumnarBatch = {
    val n = batch.numRows()
    val ord = batch.numCols() - 1
    val columns = Array.tabulate[org.apache.spark.sql.vectorized.ColumnVector](ord)(batch.column)
    val base = batch match {
      case s: io.vecruntime.spark.arrow.SelectedColumnarBatch => s
      case _ => null
    }
    val pos = batch.column(ord)
    var j = if (n == 0) deleted.length else lowerBound(deleted, pos.getLong(0))
    if (n == 0 || j == deleted.length || deleted(j) > pos.getLong(n - 1)) {
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
    var i = 0
    while (i < n && j < deleted.length) {
      val p = pos.getLong(i)
      while (j < deleted.length && deleted(j) < p) j += 1
      if (j < deleted.length && deleted(j) == p) words(i >>> 6) &= ~(1L << (i & 63))
      i += 1
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
