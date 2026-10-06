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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.util.sketch.BloomFilter

/** The buffer of [[BloomFilterMerge]]: no filter until the first non-null input. */
final class BloomFilterMergeBuffer(var filter: BloomFilter)

/**
 * ORs serialized bloom filters (`bloom_filter_agg` results) into one, in the same serialization, so the
 * result is read by `might_contain` exactly as a single `bloom_filter_agg` over all the input would be.
 *
 * `FactBloomFilter` (#646) builds a filter in two levels: `bloom_filter_agg` grouped by a bucket of the
 * creation side's partition id, then this over the few bucket filters. Every input has the same number of
 * bits and hash functions (the same `bloom_filter_agg` literals), so the OR is the filter a single level
 * would have built, bit for bit. A null input (a bucket that saw no non-null key) is skipped; with no
 * non-null input the result is null, as `bloom_filter_agg`'s is with no key.
 */
case class BloomFilterMerge(child: Expression, mutableAggBufferOffset: Int = 0, inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[BloomFilterMergeBuffer]
    with UnaryLike[Expression] {

  override def nullable: Boolean = true
  override def dataType: DataType = BinaryType
  override def prettyName: String = "bloom_filter_merge"

  override def checkInputDataTypes(): org.apache.spark.sql.catalyst.analysis.TypeCheckResult =
    if (child.dataType == BinaryType) org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
    else org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure(
      s"needs binary input, got ${child.dataType}"
    )

  override def createAggregationBuffer(): BloomFilterMergeBuffer = new BloomFilterMergeBuffer(null)

  private def mergeInto(buffer: BloomFilterMergeBuffer, f: BloomFilter): BloomFilterMergeBuffer = {
    if (f != null) { if (buffer.filter == null) buffer.filter = f else buffer.filter.mergeInPlace(f) }
    buffer
  }

  override def update(buffer: BloomFilterMergeBuffer, input: InternalRow): BloomFilterMergeBuffer =
    child.eval(input) match {
      case null => buffer
      case bytes: Array[Byte] => mergeInto(buffer, BloomFilterAggregate.deserialize(bytes))
    }

  override def merge(buffer: BloomFilterMergeBuffer, other: BloomFilterMergeBuffer): BloomFilterMergeBuffer =
    mergeInto(buffer, other.filter)

  override def eval(buffer: BloomFilterMergeBuffer): Any =
    if (buffer.filter == null || buffer.filter.cardinality() == 0) null
    else BloomFilterAggregate.serialize(buffer.filter)

  // An empty array stands for "no filter yet"; a real serialized filter is never empty (it has a header).
  override def serialize(buffer: BloomFilterMergeBuffer): Array[Byte] =
    if (buffer.filter == null) Array.emptyByteArray else BloomFilterAggregate.serialize(buffer.filter)

  override def deserialize(bytes: Array[Byte]): BloomFilterMergeBuffer =
    new BloomFilterMergeBuffer(if (bytes.isEmpty) null else BloomFilterAggregate.deserialize(bytes))

  override def withNewMutableAggBufferOffset(newOffset: Int): BloomFilterMerge =
    copy(mutableAggBufferOffset = newOffset)
  override def withNewInputAggBufferOffset(newOffset: Int): BloomFilterMerge = copy(inputAggBufferOffset = newOffset)
  override protected def withNewChildInternal(newChild: Expression): BloomFilterMerge = copy(child = newChild)
}
