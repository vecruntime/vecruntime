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
import java.math.BigInteger

import io.vecruntime.kernels.{ArrowLayout, PartitionKernels}
import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/**
 * NestedKeyHash's decimal read and partition ids (#732): a decimal hashes as Spark's Murmur3Hash whatever
 * lane holds it, a lane that cannot hold a decimal fails, and the ids are PartitionKernels.pmod's.
 */
class NestedKeyHashSuite extends AnyFunSuite {

  private val Seed = 42

  private def spark(unscaled: Long, d: DecimalType): Int =
    Murmur3HashFunction.hash(Decimal(BigDecimal(unscaled, d.scale), d.precision, d.scale), d, Seed).toInt

  private def ours(v: io.vecruntime.kernels.VectorBuffers, d: DataType, n: Int): Array[Int] = {
    val h = Array.fill(n)(Seed)
    NestedKeyHash.mix(v, d, h, n)
    h
  }

  private val values = Array(0L, 1L, -1L, 123456789L, -987654321012345678L, 999999999999999999L)

  test("a narrow decimal hashes as Spark's in an INT64 and in a DECIMAL128 lane") {
    val d = DecimalType(18, 2)
    val arena = Arena.ofConfined()
    try {
      val expected = values.map(spark(_, d))
      val long = ArrowLayout.ofLongs(arena, values, new Array[Boolean](values.length))
      val wide =
        ArrowLayout.ofDecimal128(arena, values.map(BigInteger.valueOf), new Array[Boolean](values.length))
      assert(ours(long, d, values.length).toSeq === expected.toSeq)
      assert(ours(wide, d, values.length).toSeq === expected.toSeq)
    } finally arena.close()
  }

  test("a wide decimal hashes as Spark's in a DECIMAL128 lane, and an INT64 lane refuses it") {
    val d = DecimalType(38, 4)
    val big =
      Array(BigInteger.TEN.pow(30).negate(), BigInteger.valueOf(7), BigInteger.TEN.pow(37).subtract(BigInteger.ONE))
    val arena = Arena.ofConfined()
    try {
      val expected = big.map { u =>
        Murmur3HashFunction.hash(Decimal(BigDecimal(new java.math.BigDecimal(u, d.scale)), 38, 4), d, Seed).toInt
      }
      val wide = ArrowLayout.ofDecimal128(arena, big, new Array[Boolean](big.length))
      assert(ours(wide, d, big.length).toSeq === expected.toSeq)
      val long = ArrowLayout.ofLongs(arena, Array(7L), Array(false))
      intercept[IllegalArgumentException](ours(long, d, 1))
    } finally arena.close()
  }

  test("a decimal in a lane that cannot hold one fails instead of hashing the wrong bytes") {
    val arena = Arena.ofConfined()
    try {
      val dbl = ArrowLayout.ofDoubles(arena, Array(1.5), Array(false))
      val e = intercept[IllegalArgumentException](ours(dbl, DecimalType(9, 2), 1))
      assert(e.getMessage.contains("FLOAT64"))
    } finally arena.close()
  }

  test("partition ids are PartitionKernels.pmod of each hash") {
    val hashes = Array(0, 1, -1, 7, -7, Int.MinValue, Int.MaxValue, 123456, -654321)
    Seq(1, 2, 7, 200, 900).foreach { p =>
      val ids = new Array[Int](hashes.length)
      NestedKeyHash.ids(hashes, hashes.length, p, ids)
      assert(ids.toSeq === hashes.map(PartitionKernels.pmod(_, p)).toSeq, s"$p partitions")
      assert(ids.forall(i => i >= 0 && i < p))
    }
  }
}
