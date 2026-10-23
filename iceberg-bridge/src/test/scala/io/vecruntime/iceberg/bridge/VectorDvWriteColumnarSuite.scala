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
package io.vecruntime.iceberg.bridge

import org.apache.spark.sql.vecruntime.VectorWriteDeltaExec

/**
 * #901 M1: every case of [[VectorDvWriteSuite]] -- DELETE, UPDATE and MERGE on unpartitioned and partitioned v3
 * tables, repeated, aborted, insert-only -- with the whole delta write columnar: the deletes as deletion vectors
 * and the INSERT / REINSERT half through the columnar data writer, compared with Spark's writer (contents, one
 * live DV per file, DV cardinality, the original rows' `_row_id`, snapshot counts).
 */
class VectorDvWriteColumnarSuite extends VectorDvWriteSuite {

  override protected def columnarData: Boolean = true

  // The counters are JVM-global and other suites sharing the JVM bump them, so this suite measures from its own
  // start (suite bodies run their tests in declaration order, after construction).
  private val rowInputAtStart = VectorWriteDeltaExec.rowInputWrites.get()
  private val columnarAtStart = VectorWriteDeltaExec.columnarDataTasks.get()

  test("the data half of the UPDATEs and MERGEs above ran on the columnar data writer") {
    val columnar = VectorWriteDeltaExec.columnarDataTasks.get() - columnarAtStart
    val rowInput = VectorWriteDeltaExec.rowInputWrites.get() - rowInputAtStart
    assert(columnar > 0, "no task wrote its data half columnar")
    info(s"delta writes whose child produced rows: $rowInput")
    // With the columnar exchange, the UPDATE's Expand (carrying `_partition`) and the MERGE's rows stay batches.
    assert(rowInput == 0, "a delta write read rows through RowToColumnarExec")
  }
}
