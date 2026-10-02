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

import java.util.concurrent.{Callable, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite

/**
 * The native Parquet scan's read-ahead pool (#559). A file open runs Parquet's and the S3 client's class
 * initializers and loggers; on JDK 25 a virtual thread blocked in (or waiting on) a class initializer still
 * pins its carrier, and on 1 TB TPC-DS q88 that pinned every carrier of one executor behind
 * `BloomFilterImpl.<clinit>` waiting for a log4j lock: the executor's 13 tasks hung with no CPU use.
 */
class VectorParquetPrefetchPoolSuite extends AnyFunSuite {

  private def onPool(): Thread =
    VectorParquetPartitionReader.prefetchPool
      .submit(new Callable[Thread] { def call(): Thread = Thread.currentThread() })
      .get(30, TimeUnit.SECONDS)

  test("prefetch steps run on daemon platform threads, not virtual threads") {
    val t = onPool()
    assert(!t.isVirtual, s"prefetch ran on a virtual thread: $t")
    assert(t.isDaemon, s"prefetch thread is not a daemon: $t")
    assert(t.getName.startsWith("vecruntime-parquet-prefetch-"), t.getName)
  }
}
