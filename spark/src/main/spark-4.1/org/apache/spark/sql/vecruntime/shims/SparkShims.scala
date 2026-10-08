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
 * What differs between the Spark feature lines vecruntime builds for (#639). This copy is Spark 4.1's;
 * `src/main/spark-4.2` holds 4.2's. The build adds the copy of the line it targets (`-Pspark-4.2`).
 */
object SparkShims {

  /** The Spark line this build targets. */
  val sparkLine: String = "4.1"

  /** A sample's seed (4.2 made it optional). */
  def sampleSeed(s: SampleExec): Long = s.seed

  /** Spark 4.1's `Asinh` / `Acosh` formulas. */
  val asinh: TranscendentalKernels.Fn = TranscendentalKernels.Fn.ASINH
  val acosh: TranscendentalKernels.Fn = TranscendentalKernels.Fn.ACOSH

  /**
   * Whether Spark itself merges scalar subqueries that differ only in their filters. 4.1's
   * MergeScalarSubqueries does not, so MergeFilteredAggregates does it.
   */
  val sparkMergesFilteredSubqueries: Boolean = false
}
