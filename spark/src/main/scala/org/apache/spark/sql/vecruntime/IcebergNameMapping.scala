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

import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, Type}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, MetadataBuilder, StructField, StructType}

/**
 * #849: an Iceberg table's name mapping (`schema.name-mapping.default`), which gives the data files written
 * without Parquet field ids -- a table migrated or imported from plain Parquet (`migrate`, `add_files`) -- the
 * Iceberg field id of each of their columns by name. Iceberg applies it in `ApplyNameMapping` to its own
 * (relocated) Parquet types; this is the same walk over Spark's, so our reader matches those files' columns by
 * field id as it matches every other file's.
 *
 * The JSON is Iceberg's `NameMappingParser` format: a list of fields, each with an optional `field-id`, its
 * `names` (aliases), and optional nested `fields`. A list's element is the nested field named `element`, a map's
 * key and value `key` and `value`, as Iceberg writes them. A column the mapping does not name gets no id, and reads
 * as Iceberg reads it: missing, so nulls.
 */
final case class IcebergNameMapping(fields: Seq[IcebergNameMapping.Mapped]) extends Serializable {

  /** `schema` with the field id of every column the mapping names (the file schema of a file without ids). */
  def apply(schema: MessageType): MessageType =
    new MessageType(schema.getName, IcebergNameMapping.mapFields(schema.getFields.asScala.toSeq, fields).asJava)

  /**
   * The requested schema `required` (Iceberg field ids in its metadata) with every field renamed to the name its
   * id has in `file` (a file without ids, through this mapping) and no field ids: how Spark's own reader reads such
   * a file by name, for a file our reader hands to it.
   */
  def byName(required: StructType, file: MessageType): StructType =
    IcebergNameMapping.renameStruct(required, apply(file))
}

object IcebergNameMapping {

  final case class Mapped(id: Option[Int], names: Seq[String], fields: Seq[Mapped]) extends Serializable

  /** Parses `schema.name-mapping.default`. */
  def parse(json: String): IcebergNameMapping = {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    def fieldsOf(node: com.fasterxml.jackson.databind.JsonNode): Seq[Mapped] =
      if (node == null || node.isNull) Nil
      else
        node.elements().asScala.toSeq.map { f =>
          val id = Option(f.get("field-id")).filterNot(_.isNull).map(_.asInt())
          val names = Option(f.get("names")).map(_.elements().asScala.map(_.asText()).toSeq).getOrElse(Nil)
          Mapped(id, names, fieldsOf(f.get("fields")))
        }
    IcebergNameMapping(fieldsOf(mapper.readTree(json)))
  }

  private def find(fields: Seq[Mapped], name: String): Option[Mapped] = fields.find(_.names.contains(name))

  private[vecruntime] def mapFields(types: Seq[Type], mapping: Seq[Mapped]): Seq[Type] =
    types.map(t => mapType(t, find(mapping, t.getName)))

  private def withId(t: Type, m: Option[Mapped]): Type = m.flatMap(_.id).fold(t)(t.withId)

  private def mapType(t: Type, m: Option[Mapped]): Type = {
    val nested = m.map(_.fields).getOrElse(Nil)
    val mapped: Type =
      if (t.isPrimitive) t
      else {
        val g = t.asGroupType()
        g.getLogicalTypeAnnotation match {
          case _: LogicalTypeAnnotation.ListLogicalTypeAnnotation if g.getFieldCount == 1 =>
            g.withNewFields(java.util.List.of(mapList(g.getType(0), find(nested, "element"))))
          case _: LogicalTypeAnnotation.MapLogicalTypeAnnotation if g.getFieldCount == 1 && !g.getType(0).isPrimitive =>
            val kv = g.getType(0).asGroupType()
            val kvFields = kv.getFields.asScala.toSeq.zipWithIndex.map { case (f, i) =>
              mapType(f, find(nested, if (i == 0) "key" else "value"))
            }
            g.withNewFields(java.util.List.of[Type](kv.withNewFields(kvFields.asJava)))
          case _ => g.withNewFields(mapFields(g.getFields.asScala.toSeq, nested).asJava)
        }
      }
    withId(mapped, m)
  }

  /** A LIST group's repeated field: the three-level form's element inside it, or the two-level form's itself. */
  private def mapList(repeated: Type, element: Option[Mapped]): Type =
    if (!repeated.isPrimitive && repeated.asGroupType().getFieldCount == 1 && !isTwoLevelGroup(repeated)) {
      val g = repeated.asGroupType()
      g.withNewFields(java.util.List.of(mapType(g.getType(0), element)))
    } else mapType(repeated, element)

  // Parquet's backward-compatibility rules: a repeated group named `array` or `<list>_tuple` is the element.
  private def isTwoLevelGroup(t: Type): Boolean = t.getName == "array" || t.getName.endsWith("_tuple")

  private def idOf(f: StructField): Option[Int] =
    if (f.metadata.contains(ParquetUtils.FIELD_ID_METADATA_KEY))
      Some(f.metadata.getLong(ParquetUtils.FIELD_ID_METADATA_KEY).toInt)
    else None

  private def withoutId(f: StructField): org.apache.spark.sql.types.Metadata =
    new MetadataBuilder().withMetadata(f.metadata).remove(ParquetUtils.FIELD_ID_METADATA_KEY).build()

  private def renameStruct(st: StructType, group: GroupType): StructType = {
    val byId = group.getFields.asScala.collect { case t if t.getId != null => t.getId.intValue() -> t }.toMap
    StructType(st.fields.map { f =>
      idOf(f).flatMap(byId.get) match {
        case Some(t) => f.copy(name = t.getName, dataType = renameType(f.dataType, t), metadata = withoutId(f))
        // Not in this file: a name the file cannot hold, so Spark reads the column as missing (nulls).
        case None => f.copy(name = s"_vecruntime_missing_${f.name}", metadata = withoutId(f))
      }
    })
  }

  private def renameType(dt: DataType, t: Type): DataType = (dt, t) match {
    case (st: StructType, g: Type) if !g.isPrimitive => renameStruct(st, g.asGroupType())
    case (at: ArrayType, g: Type) if !g.isPrimitive && g.asGroupType().getFieldCount == 1 =>
      val repeated = g.asGroupType().getType(0)
      val element =
        if (!repeated.isPrimitive && repeated.asGroupType().getFieldCount == 1 && !isTwoLevelGroup(repeated))
          repeated.asGroupType().getType(0)
        else repeated
      at.copy(elementType = renameType(at.elementType, element))
    case (mt: MapType, g: Type)
        if !g.isPrimitive && g.asGroupType().getFieldCount == 1 && !g.asGroupType().getType(0).isPrimitive =>
      val kv = g.asGroupType().getType(0).asGroupType()
      mt.copy(keyType = renameType(mt.keyType, kv.getType(0)), valueType = renameType(mt.valueType, kv.getType(1)))
    case _ => dt
  }
}
