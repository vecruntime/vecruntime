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

import io.vecruntime.kernels.{Decimal128, NestedVectorBuffers, PartitionKernels, VecType, VectorBuffers}
import io.vecruntime.spark.adapter.TypeMapping
import org.apache.spark.sql.types._

/**
 * Spark's `Murmur3Hash` over a nested key column (#705): an array, map or struct read as a nested lane,
 * mixed into each row's running hash exactly as `HashExpression` does -- an array's non-null elements in
 * order, a struct's non-null fields in order, a map's entries key then (non-null) value, each leaf as
 * Spark hashes its type; a null value leaves the hash unchanged.
 */
object NestedKeyHash {

  def mix(v: VectorBuffers, dt: DataType, hashes: Array[Int], n: Int): Unit = {
    var r = 0
    while (r < n) {
      if (!v.isNull(r)) hashes(r) = value(v, dt, r, hashes(r))
      r += 1
    }
  }

  private def value(v: VectorBuffers, dt: DataType, i: Int, seed: Int): Int = dt match {
    case a: ArrayType =>
      val l = v.asInstanceOf[NestedVectorBuffers]
      val e = l.children().get(0)
      var h = seed
      var j = l.start(i)
      while (j < l.end(i)) { if (!e.isNull(j)) h = value(e, a.elementType, j, h); j += 1 }
      h
    case m: MapType =>
      val l = v.asInstanceOf[NestedVectorBuffers]
      val k = l.children().get(0)
      val x = l.children().get(1)
      var h = seed
      var j = l.start(i)
      while (j < l.end(i)) {
        h = value(k, m.keyType, j, h)
        if (!x.isNull(j)) h = value(x, m.valueType, j, h)
        j += 1
      }
      h
    case s: StructType =>
      val l = v.asInstanceOf[NestedVectorBuffers]
      var h = seed
      var f = 0
      while (f < s.fields.length) {
        val c = l.children().get(f)
        if (!c.isNull(i)) h = value(c, s.fields(f).dataType, i, h)
        f += 1
      }
      h
    case IntegerType | DateType | ByteType | ShortType | _: org.apache.spark.sql.types.YearMonthIntervalType =>
      PartitionKernels.hashInt(v.getInt(i), seed)
    case LongType | TimestampType | TimestampNTZType | _: org.apache.spark.sql.types.TimeType | _: org.apache.spark.sql.types.DayTimeIntervalType =>
      PartitionKernels.hashLong(v.getLong(i), seed)
    case d: DecimalType => decimal(v, d, i, seed)
    case DoubleType => PartitionKernels.hashLong(PartitionKernels.doubleBits(v.getDouble(i)), seed)
    case BooleanType => PartitionKernels.hashInt(if (v.getBoolean(i)) 1 else 0, seed)
    case StringType | BinaryType =>
      val o = v.offsets()
      val start = o.get(VectorBuffers.LE_INT, i.toLong << 2)
      val end = o.get(VectorBuffers.LE_INT, (i + 1).toLong << 2)
      PartitionKernels.hashUnsafeBytes(v.data(), start.toLong, end - start, seed)
    case other => throw new IllegalArgumentException(s"no nested key hash for ${other.simpleString}")
  }

  /**
   * A decimal as Spark hashes it: precision <= 18 by its unscaled long, wider by the bytes of its unscaled
   * BigInteger (#732). The read follows the leaf's physical lane, not the Spark type, so a narrow decimal
   * held in a DECIMAL128 lane still hashes by its unscaled value; any other lane fails instead of hashing
   * the wrong bytes.
   */
  private def decimal(v: VectorBuffers, d: DecimalType, i: Int, seed: Int): Int = v.`type`() match {
    case VecType.INT64 =>
      require(
        d.precision <= TypeMapping.MAX_DECIMAL_PRECISION,
        s"${d.simpleString} key held in an INT64 lane"
      )
      PartitionKernels.hashLong(v.getLong(i), seed)
    case VecType.DECIMAL128 =>
      val lo = Decimal128.lo(v.data(), i)
      if (d.precision <= TypeMapping.MAX_DECIMAL_PRECISION) PartitionKernels.hashLong(lo, seed)
      else {
        val bytes = Decimal128.toBigInteger(Decimal128.hi(v.data(), i), lo).toByteArray
        PartitionKernels.hashUnsafeBytes(bytes, seed)
      }
    case other => throw new IllegalArgumentException(s"no nested key hash for ${d.simpleString} in a $other lane")
  }

  /** `pmod(hash, n)` of each row's hash, Spark's partition id ([[PartitionKernels.pmod]]). */
  def ids(hashes: Array[Int], n: Int, numPartitions: Int, ids: Array[Int]): Unit = {
    var r = 0
    while (r < n) { ids(r) = PartitionKernels.pmod(hashes(r), numPartitions); r += 1 }
  }

}
