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

import io.vecruntime.kernels.VectorBuffers
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.write.{Write, WriterCommitMessage}

/**
 * Reflective hook to the columnar Iceberg data-file writer in the optional `iceberg-bridge` module (#842,
 * W2): `IcebergDataCommitBridge` (driver side: what Iceberg resolved for the write, why a write declines, the
 * per-task commit message) and `IcebergDataTaskWriter` (one task's data files). Like [[IcebergDvBridge]], the
 * plugin core has no compile-time Iceberg dependency, so every call passes and returns only Spark, JDK and
 * engine types; without the bridge or Iceberg the hooks report unavailable and Spark's writer stays.
 */
object IcebergWriteBridge {

  /** The rows and bytes of a task's data files (an `org.apache.iceberg.ContentFile[]`), read reflectively. */
  def fileTotals(files: AnyRef): (Long, Long) = files match {
    case a: Array[_] if a.nonEmpty =>
      // Through the public interface: the implementations (GenericDataFile, ...) are not public classes.
      val cf = Class.forName("org.apache.iceberg.ContentFile", false, a.head.getClass.getClassLoader)
      val recordCount = cf.getMethod("recordCount")
      val size = cf.getMethod("fileSizeInBytes")
      a.foldLeft((0L, 0L)) { case ((r, b), f) =>
        (
          r + recordCount.invoke(f).asInstanceOf[Long],
          b + size.invoke(f).asInstanceOf[Long]
        )
      }
    case _ => (0L, 0L)
  }
  private val CommitBridge = "org.apache.iceberg.spark.source.IcebergDataCommitBridge"
  private val TaskWriter = "io.vecruntime.iceberg.write.IcebergDataTaskWriter"
  private val Spec = "io.vecruntime.iceberg.write.IcebergWriteSpec"

  private def load(name: String): Option[Class[_]] =
    try Some(Class.forName(name, true, getClass.getClassLoader))
    catch { case _: ClassNotFoundException | _: NoClassDefFoundError => None }

  private lazy val commitBridge: Option[Class[_]] = load(CommitBridge)
  private lazy val taskWriter: Option[Class[_]] = load(TaskWriter)
  private lazy val spec: Option[Class[_]] = load(Spec)

  /** Whether the bridge module (with the columnar data writer) and Iceberg are on the classpath. */
  def isAvailable: Boolean = commitBridge.isDefined && taskWriter.isDefined && spec.isDefined

  private def static(name: String, args: AnyRef*): AnyRef = {
    val c = commitBridge.getOrElse(throw new IllegalStateException("vecruntime: iceberg-bridge not on the classpath"))
    c.getMethods.find(m => m.getName == name && m.getParameterCount == args.length).get.invoke(null, args: _*)
  }

  /** Whether `write` is Iceberg's Spark batch write. */
  def isSparkWrite(write: Write): Boolean = static("isSparkWrite", write).asInstanceOf[Boolean]

  /** Why the columnar writer cannot write `write`'s data files (None when it can). */
  def declineReason(write: Write): Option[String] = Option(static("declineReason", write).asInstanceOf[String])

  /** Whether the write's output spec is unpartitioned (#901 M4a copies files one spec at a time). */
  def isUnpartitioned(write: Write): Boolean = static("isUnpartitioned", write).asInstanceOf[Boolean]

  /** The top-level field names of the write's Iceberg write schema, in order. */
  def writeSchemaNames(write: Write): Array[String] = static("writeSchemaNames", write).asInstanceOf[Array[String]]

  /** What the tasks need (a serializable `IcebergWriteSpec`), read from Iceberg's `SparkWrite` on the driver. */
  def specOf(write: Write): AnyRef = static("specOf", write)

  /** The task's commit message for its data files (a `DataFile[]`, as the task writer returns it). */
  def taskCommit(files: AnyRef): WriterCommitMessage =
    static("taskCommit", files).asInstanceOf[WriterCommitMessage]

  /** One task's data-file writer. */
  def createTaskWriter(
      spec: AnyRef,
      partitionId: Int,
      taskId: Long,
      conf: Configuration
  ): IcebergDataTaskWriterHandle = {
    val c = taskWriter.getOrElse(throw new IllegalStateException("vecruntime: iceberg-bridge not on the classpath"))
    val ctor = c.getConstructors.find(_.getParameterCount == 4).get
    new IcebergDataTaskWriterHandle(
      ctor.newInstance(spec, Integer.valueOf(partitionId), java.lang.Long.valueOf(taskId), conf).asInstanceOf[AnyRef]
    )
  }

  private val RewriteWriter = "io.vecruntime.iceberg.write.IcebergRewriteFileTaskWriter"
  private lazy val rewriteWriter: Option[Class[_]] = load(RewriteWriter)

  /**
   * One task's copy-on-write DELETE rewrite writer (#901 M4a): it buffers the surviving rows by `_file` and, on
   * close, copies each file's delete-free row groups and re-encodes the rest. Same `write`/`dataFiles`/`abort`
   * shape as [[createTaskWriter]], plus the `_file` / `_pos` ordinals and the data columns' ordinals in the
   * batch it is handed.
   */
  def createRewriteTaskWriter(
      spec: AnyRef,
      partitionId: Int,
      taskId: Long,
      conf: Configuration,
      fileOrdinal: Int,
      posOrdinal: Int,
      dataOrdinals: Array[Int]
  ): IcebergDataTaskWriterHandle = {
    val c = rewriteWriter.getOrElse(throw new IllegalStateException("vecruntime: iceberg-bridge not on the classpath"))
    val ctor = c.getConstructors.find(_.getParameterCount == 7).get
    new IcebergDataTaskWriterHandle(
      ctor.newInstance(
        spec,
        Integer.valueOf(partitionId),
        java.lang.Long.valueOf(taskId),
        conf,
        Integer.valueOf(fileOrdinal),
        Integer.valueOf(posOrdinal),
        dataOrdinals
      ).asInstanceOf[AnyRef]
    )
  }
}

/** Reflective wrapper over one `IcebergDataTaskWriter`, used on the executor; its methods are looked up once. */
final class IcebergDataTaskWriterHandle(instance: AnyRef) {
  private val cls = instance.getClass
  private val writeM = cls.getMethods.find(m => m.getName == "write" && m.getParameterCount == 3).get
  private val dataFilesM = cls.getMethod("dataFiles")
  private val abortM = cls.getMethod("abort", classOf[Throwable])

  def write(columns: java.util.List[VectorBuffers], first: Int, end: Int): Unit =
    try writeM.invoke(instance, columns, Integer.valueOf(first), Integer.valueOf(end))
    catch { case e: java.lang.reflect.InvocationTargetException => throw e.getCause }

  /** The task's `DataFile[]`, as an opaque reference for [[IcebergWriteBridge.taskCommit]]. */
  def dataFiles(): AnyRef =
    try dataFilesM.invoke(instance)
    catch { case e: java.lang.reflect.InvocationTargetException => throw e.getCause }

  def abort(cause: Throwable): Unit = abortM.invoke(instance, cause)
}

/**
 * #901 M1: the data half of a merge-on-read write through the bridge (`IcebergDeltaDataBridge`,
 * `IcebergDeltaDataWriter`), reached reflectively like the rest of the Iceberg write path.
 */
object IcebergDeltaWriteBridge {
  private val Bridge = "org.apache.iceberg.spark.source.IcebergDeltaDataBridge"
  private val Writer = "io.vecruntime.iceberg.write.IcebergDeltaDataWriter"

  private def load(name: String): Option[Class[_]] =
    try Some(Class.forName(name, true, getClass.getClassLoader))
    catch { case _: ClassNotFoundException | _: NoClassDefFoundError => None }

  private lazy val bridge: Option[Class[_]] = load(Bridge)
  private lazy val writer: Option[Class[_]] = load(Writer)

  def isAvailable: Boolean = bridge.isDefined && writer.isDefined

  private def static(name: String, args: AnyRef*): AnyRef = {
    val c = bridge.getOrElse(throw new IllegalStateException("vecruntime: iceberg-bridge not on the classpath"))
    try c.getMethods.find(m => m.getName == name && m.getParameterCount == args.length).get.invoke(null, args: _*)
    catch { case e: java.lang.reflect.InvocationTargetException => throw e.getCause }
  }

  def isPositionDeltaWrite(write: AnyRef): Boolean = static("isPositionDeltaWrite", write).asInstanceOf[Boolean]
  def declineReason(write: AnyRef): Option[String] = Option(static("declineReason", write).asInstanceOf[String])
  def rowLineage(write: AnyRef): Boolean = static("rowLineage", write).asInstanceOf[Boolean]
  def specOf(write: AnyRef): AnyRef = static("specOf", write)
  def dataCommit(files: AnyRef): org.apache.spark.sql.connector.write.WriterCommitMessage =
    static("dataCommit", files).asInstanceOf[org.apache.spark.sql.connector.write.WriterCommitMessage]

  def createWriter(
      spec: AnyRef,
      partitionId: Int,
      taskId: Long,
      conf: Configuration,
      lineage: Boolean
  ): IcebergDeltaDataWriterHandle = {
    val c = writer.getOrElse(throw new IllegalStateException("vecruntime: iceberg-bridge not on the classpath"))
    val ctor = c.getConstructors.find(_.getParameterCount == 5).get
    new IcebergDeltaDataWriterHandle(
      ctor.newInstance(
        spec,
        Integer.valueOf(partitionId),
        java.lang.Long.valueOf(taskId),
        conf,
        java.lang.Boolean.valueOf(lineage)
      ).asInstanceOf[AnyRef]
    )
  }
}

final class IcebergDeltaDataWriterHandle(instance: AnyRef) {
  private val cls = instance.getClass
  private val writeM = cls.getMethods.find(m => m.getName == "write" && m.getParameterCount == 4).get
  private val dataFilesM = cls.getMethod("dataFiles")
  private val abortM = cls.getMethod("abort", classOf[Throwable])

  def write(columns: java.util.List[VectorBuffers], rows: Array[Int], count: Int, reinsert: Array[Boolean]): Unit =
    try writeM.invoke(instance, columns, rows, Integer.valueOf(count), reinsert)
    catch { case e: java.lang.reflect.InvocationTargetException => throw e.getCause }

  def dataFiles(): AnyRef =
    try dataFilesM.invoke(instance)
    catch { case e: java.lang.reflect.InvocationTargetException => throw e.getCause }

  def abort(cause: Throwable): Unit = abortM.invoke(instance, cause)
}
