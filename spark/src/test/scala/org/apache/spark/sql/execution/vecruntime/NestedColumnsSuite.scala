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
package org.apache.spark.sql.execution.vecruntime

import io.vecruntime.spark.adapter.{NestedColumns, TypeMapping}
import io.vecruntime.spark.arrow.{VectorAllocators, VectorNestedColumnVector}
import io.vecruntime.spark.test.SparkVectorFunSuite
import io.vecruntime.kernels.{NestedVectorBuffers, VecType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/**
 * Nested lanes (#705): Spark's array, map and struct column vectors adapted into LIST / MAP /
 * STRUCT `NestedVectorBuffers` and handed back to Spark as `VectorNestedColumnVector`s read the same
 * rows -- null values, null elements, empty arrays and maps, nested elements, every leaf lane.
 */
class NestedColumnsSuite extends SparkVectorFunSuite {

  private val schemaSql =
    """i int,
      |ai array<int>,
      |as array<string>,
      |ab array<binary>,
      |ad array<decimal(10,2)>,
      |aw array<decimal(30,4)>,
      |at array<tinyint>,
      |ats array<timestamp>,
      |aa array<array<bigint>>,
      |m map<string, double>,
      |mb map<int, array<boolean>>,
      |s struct<a: int, b: string, c: array<date>>,
      |ss array<struct<x: bigint, y: map<string, int>>>""".stripMargin

  private def rows(): (StructType, Array[InternalRow]) = {
    val df = spark.range(0, 700).selectExpr(
      "cast(id as int) as i",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else array(cast(id as int), null, cast(-id as int)) end as ai",
      "case when id % 5 = 0 then null else array(concat('k', id), null, '', 'δ😀') end as as",
      "case when id % 9 = 0 then null else array(unhex('FF00'), null, unhex(hex(id))) end as ab",
      "array(cast(id / 7 as decimal(10,2)), null) as ad",
      "array(cast(id as decimal(30,4)) * 1000000000000, null) as aw",
      "array(cast(id % 100 - 50 as tinyint)) as at",
      "array(timestamp_micros(id * 1000003), null) as ats",
      "case when id % 13 = 0 then null else array(array(id, null), array(), null) end as aa",
      "case when id % 6 = 0 then null when id % 8 = 0 then map() " +
        "else map(concat('a', id), cast(id as double) / 3, 'b', null) end as m",
      "map(cast(id as int), array(true, null, false)) as mb",
      "case when id % 4 = 0 then null else named_struct('a', cast(id as int), 'b', " +
        "case when id % 3 = 0 then null else concat('s', id) end, 'c', array(date_add(date'2020-01-01', cast(id as int)))) end as s",
      "array(named_struct('x', id, 'y', map('q', cast(id as int))), null) as ss"
    )
    val schema = df.schema
    (schema, df.queryExecution.toRdd.map(_.copy()).collect())
  }

  test("every nested column has a nested lane, and flat types none") {
    val (schema, _) = rows()
    schema.fields.drop(1).foreach(f => assert(TypeMapping.nestedTypeOf(f.dataType) != null, f.name))
    assert(TypeMapping.nestedTypeOf(IntegerType) == null)
    assert(TypeMapping.vecTypeOf(ArrayType(IntegerType)) == null, "operators keep carrying nested columns")
    assert(TypeMapping.nestedTypeOf(ArrayType(CalendarIntervalType)) == null)
    assert(TypeMapping.nestedTypeOf(MapType(StringType, ArrayType(FloatType))) == null, "float has no lane (#712)")
    assert(TypeMapping.laneOf(MapType(StringType, IntegerType)) == VecType.MAP)
  }

  test("Spark's writable vectors round-trip through nested lanes") {
    val (schema, input) = rows()
    val vectors = OnHeapColumnVector.allocateColumns(input.length, schema)
    val converter = new RowToColumnConverter(schema)
    input.foreach(r =>
      converter.convert(r, vectors.map(v => v: org.apache.spark.sql.execution.vectorized.WritableColumnVector))
    )
    val allocator = VectorAllocators.newChild("NestedColumnsSuite")
    val arena = java.lang.foreign.Arena.ofConfined()
    try {
      val out: Array[ColumnVector] = schema.fields.indices.map { c =>
        val dt = schema.fields(c).dataType
        if (c == 0) vectors(c): ColumnVector
        else {
          val lane = NestedColumns.adapt(vectors(c), dt, input.length, arena)
          assert(lane.length() == input.length)
          assert(lane.`type`() == TypeMapping.nestedTypeOf(dt))
          VectorNestedColumnVector.of(schema.fields(c).name, dt, lane, allocator)
        }
      }.toArray
      val batch = new ColumnarBatch(out, input.length)
      val back = (0 until input.length).map(r => batch.getRow(r).copy())
      val toScala = org.apache.spark.sql.catalyst.CatalystTypeConverters.createToScalaConverter(schema)
      def norm(v: Any): Any = v match {
        case b: Array[Byte] => b.toSeq
        case s: scala.collection.Seq[_] => s.map(norm)
        case m: scala.collection.Map[_, _] => m.map { case (k, x) => (norm(k), norm(x)) }
        case r: org.apache.spark.sql.Row => r.toSeq.map(norm)
        case x => x
      }
      input.indices.foreach { r =>
        assert(norm(toScala(back(r))) == norm(toScala(input(r))), s"row $r")
      }
      // A slice of a nested lane on a 64-row boundary reads the same values.
      val ai = NestedColumns.adapt(vectors(1), schema.fields(1).dataType, input.length, arena)
      val sliced = ai.slice(64, 200).asInstanceOf[NestedVectorBuffers]
      val sv = VectorNestedColumnVector.of("ai", schema.fields(1).dataType, sliced, allocator)
      (0 until 136).foreach { r =>
        val expected = input(64 + r).getArray(1)
        if (expected == null) assert(sv.isNullAt(r))
        else assert(sv.getArray(r).copy().toSeq[Any](IntegerType) == expected.toSeq[Any](IntegerType), s"slice row $r")
      }
      sv.close()
      batch.close()
      assert(allocator.getAllocatedMemory == 0, "every child vector is released with its column")
    } finally {
      arena.close()
      allocator.close()
    }
  }
}
