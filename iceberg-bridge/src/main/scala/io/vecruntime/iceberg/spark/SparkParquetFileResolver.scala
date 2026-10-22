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
package io.vecruntime.iceberg.spark

import scala.jdk.CollectionConverters._

import io.vecruntime.iceberg.read.IcebergReadSpecs
import io.vecruntime.iceberg.read.IcebergReadSpecs._
import org.apache.iceberg.Schema
import org.apache.iceberg.mapping.{NameMapping, NameMappingParser}
import org.apache.iceberg.types.Types
import org.apache.parquet.schema.{GroupType, MessageType}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetUtils, SparkToParquetSchemaConverter}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.vecruntime.ParquetFileResolver

/**
 * #901 R1: the native Iceberg scan's column resolution, Iceberg's own rules (IcebergReadSpecs) in the shape the
 * plugin's Parquet reader reads. The requested Spark schema names each column by its Iceberg field id (its
 * metadata, nested fields too); the projection is the table schema's fields of those ids, pruned to the requested
 * nested fields. The clipped schema has one field per requested column in the requested order: the file's own
 * field (its name, its type, only the requested nested fields) when the file holds it, else a field under a name
 * the file does not use, which the reader reads as nulls or the column's existence default. A requested column
 * without a field id (the reader's row-index column) is such a missing field too.
 */
final class SparkParquetFileResolver(tableSchema: Object, nameMappingJson: String) extends ParquetFileResolver {

  private val schema = tableSchema.asInstanceOf[Schema]
  @transient private lazy val mapping: NameMapping =
    if (nameMappingJson == null) null else NameMappingParser.fromJson(nameMappingJson)
  @transient private lazy val converter = new SparkToParquetSchemaConverter()

  override def clip(file: MessageType, requested: StructType): Either[String, ParquetFileResolver.Resolved] = {
    val projected = requested.fields.toSeq.map(f => SparkParquetFileResolver.idOf(f).map(id => (f, id)))
    val fields = projected.flatten.map { case (f, id) =>
      Option(schema.findField(id)) match {
        case Some(ice) => project(ice, f.dataType)
        case None => Left(s"column ${f.name}: no field $id in the table schema")
      }
    }
    fields.collectFirst { case Left(why) => why } match {
      case Some(why) => Left(why)
      case None =>
        val p =
          try IcebergReadSpecs.resolve(Types.StructType.of(fields.map(_.toOption.get).asJava), file, mapping)
          catch { case e: IllegalArgumentException => return Left(e.getMessage) }
        if (p.unsupported() != null) Left(p.unsupported())
        else {
          val nodes = p.columns().asScala.iterator
          val clipped = projected.zip(requested.fields).map {
            case (Some(_), f) => shaped(nodes.next(), f.dataType)
            case (None, f) => missing(f.name, f.dataType, None)
          }
          Right(ParquetFileResolver.Resolved(p.fileWithIds(), new MessageType(file.getName, clipped.asJava)))
        }
    }
  }

  /** The table schema's field `ice`, its nested fields pruned to the ones `dt` requests (by their field ids). */
  private def project(ice: Types.NestedField, dt: DataType): Either[String, Types.NestedField] =
    (ice.`type`(), dt) match {
      case (s: Types.StructType, st: StructType) =>
        val children = st.fields.toSeq.map { sf =>
          SparkParquetFileResolver.idOf(sf).flatMap(id => Option(s.field(id))) match {
            case Some(c) => project(c, sf.dataType)
            case None => Left(s"nested field ${sf.name} of ${ice.name()} has no Iceberg field id")
          }
        }
        children.collectFirst { case Left(why) => why }.toLeft(
          Types.NestedField.from(ice).ofType(Types.StructType.of(children.map(_.toOption.get).asJava)).build()
        )
      case (l: Types.ListType, at: ArrayType) =>
        project(l.fields().get(0), at.elementType).map { e =>
          val lt =
            if (e.isOptional) Types.ListType.ofOptional(e.fieldId(), e.`type`())
            else Types.ListType.ofRequired(e.fieldId(), e.`type`())
          Types.NestedField.from(ice).ofType(lt).build()
        }
      case (m: Types.MapType, mt: MapType) =>
        for (k <- project(m.fields().get(0), mt.keyType); v <- project(m.fields().get(1), mt.valueType)) yield {
          val t =
            if (v.isOptional) Types.MapType.ofOptional(k.fieldId(), v.fieldId(), k.`type`(), v.`type`())
            else Types.MapType.ofRequired(k.fieldId(), v.fieldId(), k.`type`(), v.`type`())
          Types.NestedField.from(ice).ofType(t).build()
        }
      case (t, _) if t.isPrimitiveType => Right(ice)
      case (t, other) => Left(s"field ${ice.name()} of type $t requested as ${other.simpleString}")
    }

  /** The clipped field of a resolved node: the file's field with its nested fields shaped likewise. */
  private def shaped(n: Node, dt: DataType): org.apache.parquet.schema.Type = (n, dt) match {
    case (l: Leaf, _) => l.column().getPrimitiveType
    case (s: Struct, st: StructType) =>
      val g = s.fileType().asGroupType()
      val fields = s.fields().asScala.zip(st.fields).map { case (c, sf) => shaped(c, sf.dataType) }
      // A struct holding none of its projected fields: the level field after them, as Spark's clipping appends it
      // (the struct reader reads it for the struct's nulls only).
      g.withNewFields((fields ++ Option(s.levelField())).asJava)
    case (l: ListNode, at: ArrayType) =>
      val g = l.fileType().asGroupType()
      val repeated = g.getType(0)
      val element = shaped(l.element(), at.elementType)
      if (SparkParquetFileResolver.elementIsRepeated(g)) g.withNewFields(java.util.List.of(element))
      else g.withNewFields(java.util.List.of[org.apache.parquet.schema.Type](
        repeated.asGroupType().withNewFields(java.util.List.of(element))
      ))
    case (m: MapNode, mt: MapType) =>
      val g = m.fileType().asGroupType()
      val kv = g.getType(0).asGroupType()
      g.withNewFields(
        java.util.List.of[org.apache.parquet.schema.Type](
          kv.withNewFields(java.util.List.of(shaped(m.key(), mt.keyType), shaped(m.value(), mt.valueType)))
        )
      )
    case (missingNode: Missing, _) => missing(missingNode.field().name(), dt, Some(missingNode.field().fieldId()))
    case (other, _) => throw new IllegalStateException(s"unexpected resolution $other for ${dt.simpleString}")
  }

  /** A field the file does not hold, under a name it does not use: Spark's Parquet type for `dt`. */
  private def missing(name: String, dt: DataType, id: Option[Int]): org.apache.parquet.schema.Type = {
    val t =
      converter.convert(StructType(Seq(StructField(SparkParquetFileResolver.MissingPrefix + name, dt)))).getType(0)
    id.fold(t)(t.withId)
  }
}

object SparkParquetFileResolver {

  /** The prefix of a clipped field the file does not hold (no Parquet writer names its columns so). */
  val MissingPrefix = "_vecruntime_missing_"

  def idOf(f: StructField): Option[Int] =
    if (f.metadata.contains(ParquetUtils.FIELD_ID_METADATA_KEY))
      Some(f.metadata.getLong(ParquetUtils.FIELD_ID_METADATA_KEY).toInt)
    else None

  private def elementIsRepeated(g: GroupType): Boolean = {
    val r = g.getType(0)
    r.isPrimitive || r.asGroupType().getFieldCount != 1 || r.getName == "array" || r.getName == g.getName + "_tuple"
  }
}
