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
package org.apache.spark.sql.vecruntime.shims

import io.vecruntime.kernels.TranscendentalKernels
import org.apache.spark.sql.execution.SampleExec

/**
 * What differs between the Spark feature lines vecruntime builds for (#639). This copy is Spark 4.2's;
 * `src/main/spark-4.1` holds 4.1's.
 */
object SparkShims {

  /** The Spark line this build targets. */
  val sparkLine: String = "4.2"

  /**
   * A sample's seed. 4.2 made it optional and resolves a missing one once per plan node (`resolvedSeed`);
   * reading that keeps our sample on the rows Spark's would have drawn.
   */
  def sampleSeed(s: SampleExec): Long = s.resolvedSeed

  /** Spark 4.2 rewrote `Asinh` and `Acosh` with fdlibm's algorithms. */
  val asinh: TranscendentalKernels.Fn = TranscendentalKernels.Fn.ASINH_FDLIBM
  val acosh: TranscendentalKernels.Fn = TranscendentalKernels.Fn.ACOSH_FDLIBM

  /**
   * Whether Spark itself merges scalar subqueries that differ only in their filters: 4.2's MergeSubplans does,
   * with `spark.sql.optimizer.mergeSubplans.filterPropagation.enabled` (on by default).
   */
  val sparkMergesFilteredSubqueries: Boolean = true
}
