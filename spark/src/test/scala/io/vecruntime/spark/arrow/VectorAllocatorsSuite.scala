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

import io.vecruntime.spark.VectorConf
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

/** #868: the executor's cap on Arrow memory, and the numbers its metrics report. */
class VectorAllocatorsSuite extends AnyFunSuite {

  test("#868: past the executor's Arrow cap an allocation fails instead of growing the process") {
    VectorAllocators.setLimit(4L << 20)
    try {
      val child = VectorAllocators.newChild("VectorAllocatorsSuite")
      try {
        val held = child.buffer(2L << 20)
        try {
          assert(VectorAllocators.allocatedBytes() >= (2L << 20))
          assert(VectorAllocators.peakBytes() >= (2L << 20))
          intercept[OutOfMemoryException](child.buffer(4L << 20).close())
        } finally held.close()
      } finally child.close()
    } finally VectorAllocators.setLimit(0L)
    assert(VectorAllocators.limit() === Long.MaxValue, "0 removes the cap")
  }

  test("#868: spark.vecruntime.arrow.maxBytes reads size strings, 0 or unset for no cap") {
    assert(VectorConf.arrowMaxBytes(new SparkConf(false)) === 0L)
    assert(VectorConf.arrowMaxBytes(new SparkConf(false).set(VectorConf.ArrowMaxBytes, "2g")) === (2L << 30))
  }
}
