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

import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.types.StructType

/**
 * #901 R1: a table format's own column resolution, used by [[VectorParquetPartitionReader]] in place of Spark's
 * `clipParquetSchema` and its field-id metadata. Given a data file's schema as written, it returns that schema with
 * the field ids its columns were matched by, and the clipped schema the reader reads: `clipParquetSchema`'s shape
 * -- one field per requested top-level column, in the requested order, nested fields likewise -- with each column
 * the file holds under the file's own name and type, and each one it does not under a name the file does not use.
 * Left: why the file goes to Spark's reader.
 */
trait ParquetFileResolver extends Serializable {
  def clip(file: MessageType, requested: StructType): Either[String, ParquetFileResolver.Resolved]
}

object ParquetFileResolver {

  /** The file's schema with the matched field ids, and the clipped schema to read. */
  final case class Resolved(fileWithIds: MessageType, clipped: MessageType)

  /** The data files this executor's native Parquet readers handed to Spark's reader (read by tests). */
  val sparkReaderFiles = new java.util.concurrent.atomic.AtomicLong()

  /** #901 R2: the batches whose equality deletes were probed on their lanes (read by tests). */
  val nativeEqualityBatches = new java.util.concurrent.atomic.AtomicLong()

  /**
   * #rtypes: how many times Iceberg's own `BaseDeleteLoader.loadEqualityDeletes` was invoked to load an equality
   * delete group into a `StructLikeSet` (read by tests). The native probe reads the delete files itself and must
   * NOT trigger this; it bumps only when the row-probe fallback actually needs Iceberg's set.
   */
  val icebergEqualityLoads = new java.util.concurrent.atomic.AtomicLong()

  /**
   * #shading: how many times the native equality-delete reader was unusable against the Iceberg on the classpath
   * (a shaded-vs-unshaded Parquet `MessageType` mismatch, or an absent/reshaped generic-reader API) and the scan
   * fell back to Iceberg's own `BaseDeleteLoader` row probe (read by tests). Zero on a matched classpath.
   */
  val nativeEqualityFallbacks = new java.util.concurrent.atomic.AtomicLong()

  private val IcebergResolver = "io.vecruntime.iceberg.spark.SparkParquetFileResolver"

  /**
   * The Iceberg resolver (iceberg-bridge's `IcebergReadSpecs`) for a table's schema and name mapping JSON (null:
   * none); None when the bridge is not on the classpath, in which case the scan resolves columns as before.
   * `historySchemas` is the table's superseded schemas (newest first): a requested field id absent from the
   * current schema -- an equality delete keyed on a since-dropped column, read as a hidden probe column -- is
   * resolved against these, so our reader reads that column from the files that predate the drop by its field id.
   */
  def iceberg(
      tableSchema: AnyRef,
      nameMappingJson: String,
      historySchemas: java.util.List[AnyRef] = java.util.Collections.emptyList()
  ): Option[ParquetFileResolver] =
    try {
      val cls = Class.forName(IcebergResolver, true, Thread.currentThread().getContextClassLoader)
      Some(
        cls.getConstructor(classOf[Object], classOf[String], classOf[java.util.List[_]])
          .newInstance(tableSchema, nameMappingJson, historySchemas)
          .asInstanceOf[ParquetFileResolver]
      )
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => None
    }
}
