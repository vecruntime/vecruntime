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

import org.scalatest.funsuite.AnyFunSuite

/** #928 step 2: the gather counter's classification of an index list. */
class ArrowOutputGatherRunSuite extends AnyFunSuite {

  test("#928: consecutive rows from 0, from elsewhere, and anything else") {
    assert(ArrowOutput.consecutiveRun(Array(0, 1, 2, 3), 0, 4) == "fromZero")
    assert(ArrowOutput.consecutiveRun(Array(9, 5, 6, 7), 1, 4) == "slice")
    assert(ArrowOutput.consecutiveRun(Array(0, 2, 3), 0, 3) == null)
    assert(ArrowOutput.consecutiveRun(Array(4, 3), 0, 2) == null)
    assert(ArrowOutput.consecutiveRun(Array(1, 2), 1, 1) == null)
  }
}
