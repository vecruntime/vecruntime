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
package io.vecruntime.shuffle

import java.lang.foreign.Arena

import io.vecruntime.kernels.Bitmap
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.scalatest.funsuite.AnyFunSuite

/**
 * A struct column whose vector has no children -- Spark's `ConstantColumnVector` of a null struct, which a
 * partitioned MERGE carries as the `_partition` of its inserted rows -- flattens to all-null lanes.
 */
class StructFlatteningSuite extends AnyFunSuite {

  private val schema = StructType(
    Seq(
      StructField("k", IntegerType),
      StructField(
        "part",
        StructType(Seq(StructField("p", IntegerType), StructField("q", StructType(Seq(StructField("x", StringType))))))
      )
    )
  )

  test("a null struct constant, nested struct included, flattens to all-null lanes") {
    val n = 5
    val k = new OnHeapColumnVector(n, IntegerType)
    (0 until n).foreach(i => k.putInt(i, i))
    val part = new ConstantColumnVector(n, schema.fields(1).dataType)
    part.setNull()
    // Spark 4.1's null struct constant leaves its children unset (the case this covers); Spark 4.2 sets them.
    // Either way the struct must flatten to all-null lanes.
    val layout = StructFlattening.plan(schema).get
    val arena = Arena.ofConfined()
    try {
      val columns: Array[ColumnVector] = Array(k, part)
      val buffers = Array(io.vecruntime.spark.adapter.ColumnVectorAdapters.adapt(k, n, arena), null)
      val flat = layout.flatten(columns, buffers, n, arena)
      assert(flat.length == layout.flatSchema.fields.length)
      // Every validity lane of the struct and its nested struct is false; every leaf is null.
      layout.flatSchema.fields.zip(flat).drop(1).foreach { case (f, lane) =>
        (0 until n).foreach { i =>
          if (f.dataType == BooleanType && !f.nullable) {
            assert(!Bitmap.isSet(lane.data(), i), s"${f.name} row $i should be null")
          } else {
            assert(lane.validity() != null && !Bitmap.isSet(lane.validity(), i), s"${f.name} row $i should be null")
          }
        }
      }
    } finally arena.close()
  }

  test("a struct with no child at a non-null row is refused") {
    val dt = schema.fields(1).dataType
    val part = new ConstantColumnVector(3, dt)
    val e = intercept[IllegalStateException](StructFlattening.nullChild(part, 0, 3))
    assert(e.getMessage.contains("non-null row"))
  }
}
