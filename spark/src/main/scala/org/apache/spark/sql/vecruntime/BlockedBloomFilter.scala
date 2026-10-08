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

import java.nio.{ByteBuffer, ByteOrder}

import io.vecruntime.kernels.BloomKernels
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{BinaryType, DataType, LongType}

/**
 * A split-block bloom filter (#659), the layout of Parquet's and Impala's: 256-bit blocks of eight 32-bit words.
 * A key picks one block from its hash and sets one bit in each of the block's eight words, so a probe reads a
 * single 32-byte block -- one cache line -- where Spark's `BloomFilterImpl` reads `k` bits scattered over the whole
 * array. At 1 TB that difference is the probe's cost: q93's `store_sales` scan probes 2.9G rows against a
 * multi-megabyte filter, and Spark's layout cost more scan task time (+27 %) than the 41 GB of shuffle it saved.
 *
 * The input is the key's `xxhash64`, which also picks the sub-filter of a [[PartitionedBloomFilter]] by
 * `pmod(h, B)`; it is remixed here so that the block and bit choices do not depend on the bucket.
 */
object BlockedBloomFilter {
  private val Magic = 0x53424246 // "SBBF"

  /** The words of an empty filter of at least `bits` bits (whole 256-bit blocks, at least one). */
  def create(bits: Long): Array[Int] = BloomKernels.create(bits)

  // The layout and hashing are defined once, in the kernel (#664), so the vectorised probe and these agree.
  def put(words: Array[Int], h: Long): Unit = BloomKernels.put(words, h)

  def mightContain(words: Array[Int], h: Long): Boolean = BloomKernels.mightContain(words, h)

  /** ORs `other` into `into`; both have the same number of words (the same aggregate literals). */
  def merge(into: Array[Int], other: Array[Int]): Unit = {
    require(into.length == other.length, s"blocked bloom filters of ${into.length} and ${other.length} words")
    var i = 0
    while (i < into.length) { into(i) |= other(i); i += 1 }
  }

  def serialize(words: Array[Int]): Array[Byte] = {
    val buf = ByteBuffer.allocate(8 + 4 * words.length).order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(Magic).putInt(words.length)
    buf.asIntBuffer().put(words)
    buf.array()
  }

  def deserialize(bytes: Array[Byte], offset: Int, length: Int): Array[Int] = {
    val buf = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN)
    val magic = buf.getInt()
    require(magic == Magic, f"not a blocked bloom filter (magic 0x$magic%08x)")
    val n = buf.getInt()
    require(n > 0 && n % 8 == 0 && 4L * n <= length - 8, s"blocked bloom filter of $n words in $length bytes")
    val words = new Array[Int](n)
    buf.slice().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(words)
    words
  }

  def deserialize(bytes: Array[Byte]): Array[Int] = deserialize(bytes, 0, bytes.length)
}

/** The buffer of [[BlockedBloomFilterAgg]]: no words until the first non-null key. */
final class BlockedBloomBuffer(var words: Array[Int])

/**
 * `bloom_filter_agg` with the [[BlockedBloomFilter]] layout: the `xxhash64` of each non-null key, into a filter of
 * `numBits` bits. Null with no key, as `bloom_filter_agg` is.
 */
case class BlockedBloomFilterAgg(
    child: Expression,
    numBits: Long,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[BlockedBloomBuffer]
    with UnaryLike[Expression] {

  override def nullable: Boolean = true
  override def dataType: DataType = BinaryType
  override def prettyName: String = "blocked_bloom_filter_agg"

  override def checkInputDataTypes(): TypeCheckResult =
    if (child.dataType == LongType && numBits > 0) TypeCheckResult.TypeCheckSuccess
    else TypeCheckResult.TypeCheckFailure(s"needs a long hash and bits > 0, got ${child.dataType}, $numBits")

  override def createAggregationBuffer(): BlockedBloomBuffer = new BlockedBloomBuffer(null)

  override def update(buffer: BlockedBloomBuffer, input: InternalRow): BlockedBloomBuffer = {
    val v = child.eval(input)
    if (v != null) {
      if (buffer.words == null) buffer.words = BlockedBloomFilter.create(numBits)
      BlockedBloomFilter.put(buffer.words, v.asInstanceOf[Long])
    }
    buffer
  }

  override def merge(buffer: BlockedBloomBuffer, other: BlockedBloomBuffer): BlockedBloomBuffer = {
    if (other.words != null) {
      if (buffer.words == null) buffer.words = other.words.clone()
      else BlockedBloomFilter.merge(buffer.words, other.words)
    }
    buffer
  }

  override def eval(buffer: BlockedBloomBuffer): Any =
    if (buffer.words == null) null else BlockedBloomFilter.serialize(buffer.words)

  // An empty array stands for "no key yet"; a serialized filter is never empty (it has a header).
  override def serialize(buffer: BlockedBloomBuffer): Array[Byte] =
    if (buffer.words == null) Array.emptyByteArray else BlockedBloomFilter.serialize(buffer.words)

  override def deserialize(bytes: Array[Byte]): BlockedBloomBuffer =
    new BlockedBloomBuffer(if (bytes.isEmpty) null else BlockedBloomFilter.deserialize(bytes))

  override def withNewMutableAggBufferOffset(o: Int): BlockedBloomFilterAgg = copy(mutableAggBufferOffset = o)
  override def withNewInputAggBufferOffset(o: Int): BlockedBloomFilterAgg = copy(inputAggBufferOffset = o)
  override protected def withNewChildInternal(c: Expression): BlockedBloomFilterAgg = copy(child = c)
}
