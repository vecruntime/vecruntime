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
