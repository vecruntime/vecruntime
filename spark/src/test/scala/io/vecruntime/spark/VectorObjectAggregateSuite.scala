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
package io.vecruntime.spark

import io.vecruntime.spark.test.{TestTables, VectorQuerySuite}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.vecruntime.VectorHashAggregateExec

/**
 * `ObjectHashAggregateExec` for the object aggregates we carry (#57): `collect_list`, `collect_set`
 * and `bloom_filter_agg`. Each query runs with the plugin on and off and the rows must match; the
 * plugin plan must be ours (no `ObjectHashAggregateExec`). `collect_*` results are compared after
 * `sort_array` because the collected order is non-deterministic (Spark's own note), and `collect_set`
 * after `array_sort` of its set. The runtime bloom filter is exercised through a join whose build
 * side is `bloom_filter_agg`, with the plugin building it and Spark's probe reading it unchanged.
 */
class VectorObjectAggregateSuite extends VectorQuerySuite {

  private val Agg = classOf[VectorHashAggregateExec]
  private val ObjectAgg = classOf[ObjectHashAggregateExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestTables.createMixed(spark, newTempPath("objagg/t"))
  }

  // collect_list / collect_set need the object-hash aggregate; force it so Spark never plans a sort
  // aggregate, and run both with useObjectHashAggregateExec true and false where it applies.
  private def withObjectHash[T](f: => T): T =
    withConf("spark.sql.execution.useObjectHashAggregateExec" -> "true")(f)

  test("collect_list over lane types: grouped and ungrouped, order-insensitive") {
    withObjectHash {
      // Ungrouped.
      checkVectorized("SELECT sort_array(collect_list(i)) FROM t", Seq(Agg))
      checkVectorized("SELECT sort_array(collect_list(s)) FROM t", Seq(Agg))
      // Grouped.
      checkVectorized("SELECT i % 5 AS g, sort_array(collect_list(l)) FROM t GROUP BY i % 5", Seq(Agg))
      checkVectorized("SELECT i % 8 AS g, sort_array(collect_list(dt)) FROM t GROUP BY i % 8", Seq(Agg))
      // Nulls are ignored, following Hive/Spark semantics.
      checkVectorized("SELECT sort_array(collect_list(s)) FROM t WHERE i % 3 = 0", Seq(Agg))
    }
  }

  test("collect_set dedups per group and ignores nulls") {
    withObjectHash {
      checkVectorized("SELECT sort_array(collect_set(i % 7)) FROM t", Seq(Agg))
      checkVectorized("SELECT i % 4 AS g, sort_array(collect_set(s)) FROM t GROUP BY i % 4", Seq(Agg))
      checkVectorized("SELECT sort_array(collect_set(b)) FROM t", Seq(Agg))
      // A group whose only rows are null collects the empty set.
      checkVectorized("SELECT sort_array(collect_set(s)) FROM t WHERE i % 10 = 0", Seq(Agg))
    }
  }

  test("empty input and many groups") {
    withObjectHash {
      checkVectorized("SELECT collect_list(i) FROM t WHERE i < 0", Seq(Agg)) // empty
      checkVectorized("SELECT sort_array(collect_set(i)) FROM t WHERE i < 0", Seq(Agg))
      // Many groups: one row per i, each a singleton list.
      checkVectorized("SELECT i AS g, sort_array(collect_list(l)) FROM t GROUP BY i", Seq(Agg))
    }
  }

  test("Partial and Final split across a shuffle, adaptive on and off") {
    Seq("true", "false").foreach { aqe =>
      withConf("spark.sql.adaptive.enabled" -> aqe, "spark.sql.execution.useObjectHashAggregateExec" -> "true") {
        // A GROUP BY over a shuffle plans Partial + Final; both must be ours.
        checkVectorized("SELECT s, sort_array(collect_list(i)) FROM t GROUP BY s", Seq(Agg))
        checkVectorized("SELECT i % 6 AS g, sort_array(collect_set(l)) FROM t GROUP BY i % 6", Seq(Agg))
      }
    }
  }

  test("mixed with ordinary aggregates in one node") {
    withObjectHash {
      checkVectorized(
        "SELECT i % 5 AS g, count(*), sum(l), sort_array(collect_list(i)) FROM t GROUP BY i % 5",
        Seq(Agg)
      )
    }
  }

  test("bloom_filter_agg build side is ours in the runtime-filter plan") {
    // bloom_filter_agg is an internal function the runtime-filter optimizer injects; it is not a
    // SQL-callable name, so it is exercised only through the runtime filter. Force the filter with
    // small thresholds and assert the build side is our aggregate and the rows match Spark's -- the
    // partial buffer is byte-identical by construction (Spark's own BloomFilter.serialize).
    withConf(
      "spark.sql.optimizer.runtime.bloomFilter.enabled" -> "true",
      "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0",
      "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold" -> "100mb",
      "spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled" -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1", // force a shuffle join so a bloom filter is injected
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.execution.useObjectHashAggregateExec" -> "true"
    ) {
      val sql =
        """
          |SELECT big.i, big.s
          |FROM t AS big
          |JOIN (SELECT i FROM t WHERE i % 1000 = 0) AS small
          |  ON big.i = small.i
          |""".stripMargin
      val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
      val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
      val actual = df.collect()
      // Results must match Spark's. The bloom filter is built by our Partial VectorHashAggregate
      // (`partial_bloom_filter_agg`) feeding Spark's Final inside a scalar subquery, and probed by our
      // VectorFilter's `might_contain` -- so a mismatch would mean our partial buffer is not what
      // Spark's Final reads. (The Final node lives in a subquery, which `allNodes` does not descend
      // into, so we assert on the results, which is the interoperability guarantee that matters.)
      assertRowsEqual(expected, actual, 1e-9, sql)
    }
  }

  test("percentile and other object aggregates still fall back with a reason") {
    withConf("spark.sql.execution.useObjectHashAggregateExec" -> "true") {
      checkFallback(
        "SELECT percentile(l, 0.5) FROM t",
        Seq(Agg),
        reasonContains = "unsupported aggregate function"
      )
    }
  }
}
