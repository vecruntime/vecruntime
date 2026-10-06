/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, LongType}
import org.apache.spark.util.sketch.BloomFilter

/**
 * A partitioned runtime bloom filter (#653): `B` sub-filters, sub-filter `b` holding the keys whose hash `h`
 * has `pmod(h, B) = b`. One value holds them all:
 * `int B, then per bucket: int length (-1 = no key in the bucket), the bucket's serialized BloomFilter`.
 *
 * A filter sized for tens of millions of keys cannot be a single `bloom_filter_agg`: Spark caps one at
 * `maxNumItems` / `maxNumBits`, and every map task would ship a full-size partial (#646). Split by the hash,
 * each sub-filter stays within the caps and is built after the keys' shuffle by bucket, one task per bucket.
 */
object PartitionedBloomFilter {

  def pack(filters: Array[Array[Byte]]): Array[Byte] = {
    val size = 4 + filters.iterator.map(f => 4 + (if (f == null) 0 else f.length)).sum
    val buf = ByteBuffer.allocate(size)
    buf.putInt(filters.length)
    filters.foreach { f =>
      if (f == null) buf.putInt(-1) else { buf.putInt(f.length); buf.put(f) }
    }
    buf.array()
  }

  /** The sub-filters, `null` for a bucket that saw no key. */
  def unpack(bytes: Array[Byte]): Array[BloomFilter] = {
    val buf = ByteBuffer.wrap(bytes)
    val n = buf.getInt()
    Array.fill(n) {
      val len = buf.getInt()
      if (len < 0) null
      else {
        val f = BloomFilter.readFrom(new ByteArrayInputStream(bytes, buf.position(), len))
        buf.position(buf.position() + len)
        f
      }
    }
  }

  /** Whether `h` may be a key: its bucket's sub-filter says so. An empty bucket holds no key. */
  def mightContain(filters: Array[BloomFilter], h: Long): Boolean = {
    val b = Math.floorMod(h, filters.length.toLong).toInt
    val f = filters(b)
    f != null && f.mightContainLong(h)
  }
}

/** The buffer of [[PartitionedBloomFilterAgg]]: the serialized sub-filter of each bucket seen so far. */
final class PartitionedBloomBuffer(val filters: Array[Array[Byte]])

/**
 * Packs `(bucket, serialized sub-filter)` rows into one [[PartitionedBloomFilter]] value. The input is the
 * per-bucket `bloom_filter_agg` (one row per bucket); a null filter (a bucket without keys) stays null.
 */
case class PartitionedBloomFilterAgg(
    bucket: Expression,
    filter: Expression,
    numBuckets: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[PartitionedBloomBuffer]
    with BinaryLike[Expression] {

  override def left: Expression = bucket
  override def right: Expression = filter
  override def nullable: Boolean = true
  override def dataType: DataType = BinaryType
  override def prettyName: String = "partitioned_bloom_filter"

  override def checkInputDataTypes(): TypeCheckResult =
    if (bucket.dataType == IntegerType && filter.dataType == BinaryType && numBuckets > 0)
      TypeCheckResult.TypeCheckSuccess
    else TypeCheckResult.TypeCheckFailure(
      s"needs (int, binary) and buckets > 0, got ${bucket.dataType}, ${filter.dataType}"
    )

  override def createAggregationBuffer(): PartitionedBloomBuffer =
    new PartitionedBloomBuffer(new Array[Array[Byte]](numBuckets))

  override def update(buffer: PartitionedBloomBuffer, input: InternalRow): PartitionedBloomBuffer = {
    val b = bucket.eval(input)
    val f = filter.eval(input)
    if (b != null && f != null) buffer.filters(b.asInstanceOf[Int]) = f.asInstanceOf[Array[Byte]]
    buffer
  }

  override def merge(buffer: PartitionedBloomBuffer, other: PartitionedBloomBuffer): PartitionedBloomBuffer = {
    var i = 0
    while (i < numBuckets) {
      if (other.filters(i) != null) buffer.filters(i) = other.filters(i) // one row per bucket: never two
      i += 1
    }
    buffer
  }

  override def eval(buffer: PartitionedBloomBuffer): Any =
    if (buffer.filters.forall(_ == null)) null else PartitionedBloomFilter.pack(buffer.filters)

  override def serialize(buffer: PartitionedBloomBuffer): Array[Byte] = PartitionedBloomFilter.pack(buffer.filters)

  override def deserialize(bytes: Array[Byte]): PartitionedBloomBuffer = {
    val buf = ByteBuffer.wrap(bytes)
    val n = buf.getInt()
    new PartitionedBloomBuffer(Array.fill(n) {
      val len = buf.getInt()
      if (len < 0) null
      else { val a = new Array[Byte](len); buf.get(a); a }
    })
  }

  override def withNewMutableAggBufferOffset(o: Int): PartitionedBloomFilterAgg = copy(mutableAggBufferOffset = o)
  override def withNewInputAggBufferOffset(o: Int): PartitionedBloomFilterAgg = copy(inputAggBufferOffset = o)
  override protected def withNewChildrenInternal(l: Expression, r: Expression): PartitionedBloomFilterAgg =
    copy(bucket = l, filter = r)
}

/**
 * `might_contain` over a [[PartitionedBloomFilter]]: `filter` is a scalar subquery's packed value, `value` the
 * key's `xxhash64`. A null filter (no creation key at all) gives null, as Spark's `might_contain` does, which a
 * `Filter` drops like false.
 */
case class PartitionedBloomMightContain(filter: Expression, value: Expression)
    extends BinaryExpression
    with Predicate
    with CodegenFallback {

  override def left: Expression = filter
  override def right: Expression = value
  override def nullable: Boolean = true
  override def prettyName: String = "might_contain_partitioned"

  override def checkInputDataTypes(): TypeCheckResult =
    if (filter.dataType == BinaryType && value.dataType == LongType) TypeCheckResult.TypeCheckSuccess
    else TypeCheckResult.TypeCheckFailure(s"needs (binary, long), got ${filter.dataType}, ${value.dataType}")

  @transient private lazy val filters: Array[BloomFilter] = filter.eval() match {
    case null => null
    case bytes: Array[Byte] => PartitionedBloomFilter.unpack(bytes)
  }

  override def eval(input: InternalRow): Any = {
    val fs = filters
    if (fs == null) return null
    val v = value.eval(input)
    if (v == null) null else PartitionedBloomFilter.mightContain(fs, v.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = super.doGenCode(ctx, ev)

  override protected def withNewChildrenInternal(l: Expression, r: Expression): PartitionedBloomMightContain =
    copy(filter = l, value = r)
}
