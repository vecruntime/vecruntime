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
package io.vecruntime.iceberg.read

import scala.jdk.CollectionConverters._

import io.vecruntime.iceberg.read.IcebergReadSpecs._
import org.apache.iceberg.mapping.{MappedField, MappedFields, NameMapping}
import org.apache.iceberg.types.{Type, Types}
import org.apache.iceberg.types.Types.NestedField.{optional, required}
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.scalatest.funsuite.AnyFunSuite

/**
 * IcebergReadSpecs (#901 R1): an Iceberg projection resolved against a Parquet file schema by field id -- renames,
 * reorders, added and dropped columns, a new column taking an old one's name, nested evolution, promotions, the
 * table's name mapping for files without ids, Iceberg's positional fallback ids -- and every leaf's read spec equal
 * to the one the Spark adapter builds for the Spark type Iceberg maps the Iceberg type to.
 */
class IcebergReadSpecsSuite extends AnyFunSuite {

  private def file(s: String): MessageType = MessageTypeParser.parseMessageType(s)

  private def struct(fields: Types.NestedField*): Types.StructType = Types.StructType.of(fields: _*)

  private def paths(p: FileProjection): Seq[String] =
    p.requested().getColumns.asScala.map(_.getPath.mkString(".")).toSeq

  private val evolved = file(
    """message table {
      |  optional int32 a = 1;
      |  optional binary old_name (STRING) = 2;
      |  optional int64 dropped = 3;
      |  optional group st = 4 {
      |    optional int32 x = 5;
      |    optional binary y (STRING) = 6;
      |  }
      |  optional float f = 7;
      |  optional int32 promoted = 8;
      |  optional int64 dec (DECIMAL(9, 2)) = 9;
      |}""".stripMargin
  )

  test("renamed, reordered, added and dropped columns resolve by field id; the file's names play no part") {
    val p = resolve(
      struct(
        optional(8, "promoted", Types.LongType.get()), // promoted int -> long, moved first
        optional(2, "new_name", Types.StringType.get()), // renamed
        optional(10, "added", Types.IntegerType.get()), // added after the file
        optional(1, "a", Types.IntegerType.get())
      ),
      evolved,
      null
    )
    assert(p.unsupported() == null)
    assert(paths(p) == Seq("promoted", "old_name", "a"), "the requested schema, in projection order")
    val cols = p.columns().asScala
    assert(cols(0).asInstanceOf[Leaf].column().getPath.toSeq == Seq("promoted"))
    assert(cols(1).asInstanceOf[Leaf].column().getPath.toSeq == Seq("old_name"))
    assert(cols(2).isInstanceOf[Missing] && cols(2).asInstanceOf[Missing].initialDefault() == null)
    assert(cols(3).asInstanceOf[Leaf].column().getPath.toSeq == Seq("a"))
  }

  test("a new column taking a dropped column's old name is missing in the old file, not the old column") {
    // `old_name` (id 2) was renamed; a new column took the name `old_name` with id 11.
    val p = resolve(struct(optional(11, "old_name", Types.StringType.get())), evolved, null)
    assert(p.columns().get(0).isInstanceOf[Missing])
    assert(paths(p).isEmpty)
  }

  test("nested evolution: a renamed and an added struct field, a struct none of whose fields the file holds") {
    val p = resolve(
      struct(
        optional(
          4,
          "st",
          struct(optional(6, "y_renamed", Types.StringType.get()), optional(12, "z", Types.LongType.get()))
        ),
        optional(13, "st2", struct(optional(14, "q", Types.IntegerType.get())))
      ),
      evolved,
      null
    )
    assert(paths(p) == Seq("st.y"))
    val st = p.columns().get(0).asInstanceOf[Struct]
    assert(st.fields().get(0).asInstanceOf[Leaf].column().getPath.toSeq == Seq("st", "y"))
    assert(st.fields().get(1).isInstanceOf[Missing])
    assert(p.columns().get(1).isInstanceOf[Missing])
    // The file holds `st` but none of the fields projected from it: a present struct of nulls, its nulls read from
    // the group's narrowest leaf (x, int32), which the requested schema carries after the projected fields.
    val empty = resolve(struct(optional(4, "st", struct(optional(12, "z", Types.LongType.get())))), evolved, null)
    assert(empty.unsupported() == null)
    val es = empty.columns().get(0).asInstanceOf[Struct]
    assert(es.fields().get(0).isInstanceOf[Missing] && es.levelField().getName == "x")
    assert(paths(empty) == Seq("st.x"))
  }

  test("initial defaults fill a missing field; a required field without one fails as Iceberg's reader does") {
    val withDefault = Types.NestedField.optional("d").withId(20).ofType(Types.IntegerType.get())
      .withInitialDefault(org.apache.iceberg.expressions.Literal.of(42)).build()
    val p = resolve(struct(withDefault), evolved, null)
    assert(p.columns().get(0).asInstanceOf[Missing].initialDefault() == 42)
    val e =
      intercept[IllegalArgumentException](resolve(struct(required(21, "r", Types.IntegerType.get())), evolved, null))
    assert(e.getMessage.contains("Missing required field: r"))
  }

  test("promotions: int -> long, float -> double, decimal precision; an unsupported pairing is reported") {
    val p = resolve(
      struct(
        optional(8, "promoted", Types.LongType.get()),
        optional(7, "f", Types.DoubleType.get()),
        optional(9, "dec", Types.DecimalType.of(30, 2))
      ),
      evolved,
      null
    )
    assert(p.unsupported() == null)
    val specs = p.columns().asScala.map(_.asInstanceOf[Leaf].spec().toString)
    assert(specs(0) == "LeafReadSpec[INT64]")
    assert(specs(1).contains("widen 2"), specs(1)) // WIDEN_FLOAT_TO_DOUBLE
    assert(specs(2).contains("DECIMAL128") && specs(2).contains("widen 5"), specs(2)) // WIDEN_LONG_TO_DEC128
    val bad = resolve(struct(optional(9, "dec", Types.DecimalType.of(9, 3))), evolved, null) // a scale change
    assert(bad.unsupported() != null)
  }

  test("lists and maps, three- and two-level lists, resolve their element, key and value by id") {
    val f = file(
      """message t {
        |  optional group xs (LIST) = 1 { repeated group list { optional int32 element = 2; } }
        |  optional group legacy (LIST) = 3 { repeated int64 array = 4; }
        |  optional group m (MAP) = 5 {
        |    repeated group key_value { required binary key (STRING) = 6; optional group value = 7 { optional int32 v = 8; optional int32 w = 9; } }
        |  }
        |}""".stripMargin
    )
    val p = resolve(
      struct(
        optional(
          5,
          "m",
          Types.MapType.ofOptional(6, 7, Types.StringType.get(), struct(optional(9, "w", Types.IntegerType.get())))
        ),
        optional(1, "xs", Types.ListType.ofOptional(2, Types.LongType.get())),
        optional(3, "legacy", Types.ListType.ofRequired(4, Types.LongType.get()))
      ),
      f,
      null
    )
    assert(p.unsupported() == null, String.valueOf(p.unsupported()))
    assert(paths(p) == Seq("m.key_value.key", "m.key_value.value.w", "xs.list.element", "legacy.array"))
    val xs = p.columns().get(1).asInstanceOf[ListNode]
    assert(xs.element().asInstanceOf[Leaf].spec().lane == io.vecruntime.kernels.VecType.INT64) // int -> long
  }

  test("a file without ids resolves through the table's name mapping, aliases included") {
    val noIds = file(
      """message spark_schema {
        |  optional int32 legacy_a;
        |  optional binary s (STRING);
        |  optional group st { optional int32 x; optional int32 unmapped; }
        |}""".stripMargin
    )
    val mapping = NameMapping.of(
      MappedField.of(1, java.util.List.of("a", "legacy_a")),
      MappedField.of(2, "s"),
      MappedField.of(4, "st", MappedFields.of(MappedField.of(5, "x")))
    )
    val p = resolve(
      struct(
        optional(1, "a", Types.IntegerType.get()),
        optional(
          4,
          "st",
          struct(optional(5, "x", Types.IntegerType.get()), optional(6, "unmapped", Types.IntegerType.get()))
        ),
        optional(2, "s", Types.StringType.get())
      ),
      noIds,
      mapping
    )
    assert(paths(p) == Seq("legacy_a", "st.x", "s"))
    assert(
      p.columns().get(1).asInstanceOf[Struct].fields().get(1).isInstanceOf[Missing],
      "a column the mapping does not name"
    )
  }

  test("a file without ids and no name mapping falls back to positional ids, as Iceberg does") {
    val noIds = file("message m { optional int32 p; optional binary q (STRING); }")
    val p =
      resolve(struct(optional(2, "q", Types.StringType.get()), optional(1, "p", Types.IntegerType.get())), noIds, null)
    assert(paths(p) == Seq("q", "p"))
  }

  test("metadata columns are left to the scan") {
    val p = resolve(
      struct(org.apache.iceberg.MetadataColumns.FILE_PATH, optional(1, "a", Types.IntegerType.get())),
      evolved,
      null
    )
    assert(p.columns().get(0).isInstanceOf[Metadata])
    assert(paths(p) == Seq("a"))
  }

  test("the row-lineage columns are read from a file that materialises them, else missing") {
    val lineage = struct(
      optional(1, "a", Types.IntegerType.get()),
      org.apache.iceberg.MetadataColumns.ROW_ID,
      org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER
    )
    val (rowId, seq) = (
      org.apache.iceberg.MetadataColumns.ROW_ID.fieldId(),
      org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()
    )
    val rewritten = file(
      s"""message table {
         |  optional int32 a = 1;
         |  optional int64 _row_id = $rowId;
         |  optional int64 _last_updated_sequence_number = $seq;
         |}""".stripMargin
    )
    val read = resolve(lineage, rewritten, null)
    assert(read.columns().asScala.forall(_.isInstanceOf[Leaf]))
    assert(paths(read) == Seq("a", "_row_id", "_last_updated_sequence_number"))
    val original = resolve(lineage, file("message table { optional int32 a = 1; }"), null)
    assert(original.columns().get(1).isInstanceOf[Missing] && original.columns().get(2).isInstanceOf[Missing])
    assert(paths(original) == Seq("a"))
  }

  // ------------------------------------------------------------- the Spark adapter's specs, type by type

  private val pairings: Seq[(Type.PrimitiveType, String)] = Seq(
    Types.BooleanType.get() -> "optional boolean c",
    Types.IntegerType.get() -> "optional int32 c",
    Types.IntegerType.get() -> "optional int32 c (INTEGER(16, true))",
    Types.LongType.get() -> "optional int64 c",
    Types.LongType.get() -> "optional int32 c",
    Types.FloatType.get() -> "optional float c",
    Types.DoubleType.get() -> "optional double c",
    Types.DoubleType.get() -> "optional float c",
    Types.DateType.get() -> "optional int32 c (DATE)",
    Types.TimestampType.withZone() -> "optional int64 c (TIMESTAMP(MICROS, true))",
    Types.TimestampType.withZone() -> "optional int64 c (TIMESTAMP(MILLIS, true))",
    Types.TimestampType.withoutZone() -> "optional int64 c (TIMESTAMP(MICROS, false))",
    Types.StringType.get() -> "optional binary c (STRING)",
    Types.BinaryType.get() -> "optional binary c",
    Types.DecimalType.of(9, 2) -> "optional int32 c (DECIMAL(9, 2))",
    Types.DecimalType.of(18, 2) -> "optional int64 c (DECIMAL(18, 2))",
    Types.DecimalType.of(18, 2) -> "optional int32 c (DECIMAL(9, 2))",
    Types.DecimalType.of(30, 2) -> "optional fixed_len_byte_array(13) c (DECIMAL(30, 2))",
    Types.DecimalType.of(30, 2) -> "optional int64 c (DECIMAL(18, 2))"
  )

  test("every leaf spec equals the Spark adapter's for the Spark type Iceberg maps the Iceberg type to") {
    pairings.foreach { case (iceberg, column) =>
      val parquet = file(s"message m { $column = 1; }")
      val ours = resolve(struct(optional(1, "c", iceberg)), parquet, null).columns().get(0)
      assert(ours.isInstanceOf[Leaf], s"$iceberg <- $column: not resolved")
      val spec = ours.asInstanceOf[Leaf].spec()
      val sparkType = org.apache.iceberg.spark.SparkSchemaUtil.convert(iceberg)
      val spark = io.vecruntime.spark.parquet.NativeParquetSupport.leafSpec(parquet.getColumns.get(0), sparkType)
      assert(spec.toString == spark.toString, s"$iceberg <- $column")
      assert(spec.arrowType() == spark.arrowType(), s"$iceberg <- $column: arrow type")
    }
  }
}
