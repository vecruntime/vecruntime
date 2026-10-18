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
package io.vecruntime.spark.arrow

import java.nio.charset.StandardCharsets

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #812: [[VectorArrowColumnVector]] reads null bits, ints, longs and strings straight from the buffers' addresses.
 * Every read must equal Spark's own [[ArrowColumnVector]] over the same vector, across byte boundaries of the
 * validity bitmap, and after the vector is refilled with more rows (its buffers reallocated) under the same wrapper.
 */
class VectorArrowColumnVectorSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val allocator = new RootAllocator(Long.MaxValue)

  override protected def afterAll(): Unit = {
    allocator.close()
    super.afterAll()
  }

  private def nullAt(i: Int): Boolean = i % 3 == 0 || i % 8 == 7

  private def fill(iv: IntVector, lv: BigIntVector, sv: VarCharVector, n: Int, salt: Int): Unit = {
    iv.reset(); lv.reset(); sv.reset()
    (0 until n).foreach { i =>
      if (nullAt(i + salt)) { iv.setNull(i); lv.setNull(i); sv.setNull(i) }
      else {
        iv.setSafe(i, i * 31 - 1000 + salt)
        lv.setSafe(i, (i.toLong << 33) - salt)
        sv.setSafe(i, ("v" * (i % 4) + i + "é").getBytes(StandardCharsets.UTF_8))
      }
    }
    iv.setValueCount(n); lv.setValueCount(n); sv.setValueCount(n)
  }

  private def check(ours: VectorArrowColumnVector, spark: ArrowColumnVector, n: Int, what: String): Unit =
    (0 until n).foreach { i =>
      assert(ours.isNullAt(i) === spark.isNullAt(i), s"$what isNullAt($i)")
    }

  test("null bits, ints, longs and strings read as Spark's ArrowColumnVector reads them, also after a refill") {
    val iv = new IntVector("i", allocator)
    val lv = new BigIntVector("l", allocator)
    val sv = new VarCharVector("s", allocator)
    try {
      iv.allocateNew(4); lv.allocateNew(4); sv.allocateNew(4)
      val (oi, ol, os) = (
        new VectorArrowColumnVector(iv, false),
        new VectorArrowColumnVector(lv, false),
        new VectorArrowColumnVector(sv, false)
      )
      val (si, sl, ss) = (new ArrowColumnVector(iv), new ArrowColumnVector(lv), new ArrowColumnVector(sv))
      // A small batch, then one large enough that every buffer is reallocated under the same wrappers.
      for ((n, salt) <- Seq((21, 0), (5000, 1), (3, 2))) {
        fill(iv, lv, sv, n, salt)
        check(oi, si, n, s"int n=$n")
        check(ol, sl, n, s"long n=$n")
        check(os, ss, n, s"string n=$n")
        (0 until n).filterNot(i => si.isNullAt(i)).foreach { i =>
          assert(oi.getInt(i) === si.getInt(i), s"getInt($i) n=$n")
          assert(ol.getLong(i) === sl.getLong(i), s"getLong($i) n=$n")
          assert(os.getUTF8String(i) === ss.getUTF8String(i), s"getUTF8String($i) n=$n")
        }
        (0 until n).filter(i => si.isNullAt(i)).foreach { i =>
          assert(os.getUTF8String(i) == null, s"getUTF8String($i) of a null slot n=$n")
        }
      }
    } finally {
      iv.close(); lv.close(); sv.close()
    }
  }
}
