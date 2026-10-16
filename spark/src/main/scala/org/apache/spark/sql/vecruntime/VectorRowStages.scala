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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SpecializedGetters, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ShuffledRowRDD, UnsafeRowSerializer}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType,
  StructType,
  TimestampNTZType,
  TimestampType
}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * The row-based tail shared by the operators whose final step is a bounded merge of a few rows per
 * partition (`ORDER BY ... LIMIT`, `LIMIT`): batches become `UnsafeRow`s for Spark's own
 * single-partition shuffle, and the handful of surviving rows become one columnar batch again for
 * the `ColumnarToRowExec` above. Everything here sees at most `limit x partitions` rows.
 */
object VectorRowStages {

  /**
   * Every row of every batch as an `UnsafeRow` copy. A batch is released when the next is requested,
   * so the rows must be copied out of it.
   */
  def toUnsafeRows(batches: Iterator[ColumnarBatch], attrs: Seq[Attribute]): Iterator[InternalRow] = {
    val toUnsafe = UnsafeProjection.create(attrs, attrs)
    batches.flatMap(batch => batch.rowIterator().asScala.map(row => toUnsafe(row).copy()))
  }

  /** `rows` shuffled into one partition through Spark's exchange machinery. */
  def singlePartition(
      rows: RDD[InternalRow],
      attrs: Seq[Attribute],
      writeMetrics: Map[String, SQLMetric],
      readMetrics: Map[String, SQLMetric]
  ): RDD[InternalRow] =
    new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        rows,
        attrs,
        SinglePartition,
        new UnsafeRowSerializer(attrs.size),
        writeMetrics
      ),
      readMetrics
    )

  /**
   * One columnar batch of on-heap vectors holding `rows`. Covers exactly the Spark types
   * `TypeMapping` supports (the planners refuse any other output type), so every case is a plain
   * `WritableColumnVector` put.
   */
  def toBatch(schema: StructType, rows: Array[InternalRow]): ColumnarBatch = {
    val n = rows.length
    val vectors: Array[OnHeapColumnVector] = OnHeapColumnVector.allocateColumns(n, schema)
    var c = 0
    while (c < vectors.length) {
      val v = vectors(c)
      val dt = schema(c).dataType
      if (dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType] || dt.isInstanceOf[StructType]) {
        // An array, map or struct (#705): appended row by row, its elements and fields into the child vectors.
        var i = 0
        while (i < n) {
          appendValue(v, dt, rows(i), c)
          i += 1
        }
        c += 1
      } else {
        writeFlat(v, dt, rows, n, c)
        c += 1
      }
    }
    new ColumnarBatch(vectors.map(v => v: ColumnVector), n)
  }

  private def writeFlat(v: OnHeapColumnVector, dt: DataType, rows: Array[InternalRow], n: Int, c: Int): Unit = {
    var i = 0
    while (i < n) {
      val row = rows(i)
      if (row.isNullAt(c)) v.putNull(i)
      else putLeaf(v, i, dt, row, c)
      i += 1
    }
  }

  private def putLeaf(v: WritableColumnVector, i: Int, dt: DataType, row: SpecializedGetters, c: Int): Unit =
    dt match {
      case BooleanType => v.putBoolean(i, row.getBoolean(c))
      case IntegerType | DateType => v.putInt(i, row.getInt(c))
      case ByteType => v.putByte(i, row.getByte(c))
      case ShortType => v.putShort(i, row.getShort(c))
      case LongType | TimestampType | TimestampNTZType => v.putLong(i, row.getLong(c))
      case DoubleType => v.putDouble(i, row.getDouble(c))
      case FloatType => v.putFloat(i, row.getFloat(c)) // #712
      case d: DecimalType => v.putDecimal(i, row.getDecimal(c, d.precision, d.scale), d.precision)
      case StringType => val b = row.getUTF8String(c).getBytes; v.putByteArray(i, b, 0, b.length)
      case BinaryType => val b = row.getBinary(c); v.putByteArray(i, b, 0, b.length) // #705
      case other => throw new IllegalStateException(s"unsupported output type ${other.simpleString}")
    }

  /** Appends value `c` of `g` (an element, a field or a row's column) to `v`, nested values recursively. */
  private def appendValue(v: WritableColumnVector, dt: DataType, g: SpecializedGetters, c: Int): Unit =
    if (g.isNullAt(c)) {
      dt match {
        case _: StructType => v.appendStruct(true)
        case _ => v.appendNull()
      }
    } else dt match {
      case a: ArrayType =>
        val arr = g.getArray(c)
        v.appendArray(arr.numElements())
        var j = 0
        while (j < arr.numElements()) {
          appendValue(v.arrayData(), a.elementType, arr, j)
          j += 1
        }
      case m: MapType =>
        val map = g.getMap(c)
        val keys = map.keyArray()
        val values = map.valueArray()
        v.appendArray(map.numElements())
        var j = 0
        while (j < map.numElements()) {
          appendValue(v.getChild(0), m.keyType, keys, j)
          appendValue(v.getChild(1), m.valueType, values, j)
          j += 1
        }
      case s: StructType =>
        val struct = g.getStruct(c, s.length)
        v.appendStruct(false)
        var f = 0
        while (f < s.length) {
          appendValue(v.getChild(f), s(f).dataType, struct, f)
          f += 1
        }
      case leaf =>
        val i = v.getElementsAppended
        v.appendNotNull()
        putLeaf(v, i, leaf, g, c)
    }
}
