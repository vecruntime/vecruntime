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

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain

/** A bloom filter from a smaller fact onto a larger one it joins (#641), against Spark with the plugin off. */
class FactBloomFilterSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // q93-like: sales with many tickets, returns for one ticket in twenty (about 20x smaller).
    // A wide, incompressible column makes the sales file well over 10x the returns file, as at 1 TB.
    (0 until 40000).map(i => (i.toLong / 4, (i % 97).toLong, (i % 13).toLong, f"${i * 2654435761L}%040d"))
      .toDF("ss_ticket_number", "ss_item_sk", "ss_quantity", "ss_pad")
      .write.mode("overwrite").parquet(newTempPath("fbloom/store_sales"))
    spark.read.parquet(newTempPath("fbloom/store_sales")).createOrReplaceTempView("store_sales")
    (0 until 2000).map(i => (i.toLong * 5, (i % 97).toLong, (i % 3).toLong))
      .toDF("sr_ticket_number", "sr_item_sk", "sr_return_quantity")
      .write.mode("overwrite").parquet(newTempPath("fbloom/store_returns"))
    spark.read.parquet(newTempPath("fbloom/store_returns")).createOrReplaceTempView("store_returns")
  }

  /** Shuffle joins only, and no minimum scan size, so the small test tables qualify. */
  private def shuffleOnly[T](f: => T): T = withConf(
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0"
  )(f)

  private def run(sql: String): DataFrame = shuffleOnly {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** Bloom filters on the sales side's keys in the optimized plan. */
  private def salesBlooms(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f }
      .flatMap(_.condition.collect { case b: BloomFilterMightContain => b })
      .count(_.valueExpression.references.exists(_.name.startsWith("ss_")))

  // The creation side is reduced by a filter (#650): without statistics a whole, unfiltered table gets no filter.
  // `isnotnull(nullif(..))` drops a third of the returns, and Spark's own runtime filter does not count it as
  // selective, so only this rule's filters are in the plan.
  private val q93 =
    """SELECT ss_item_sk, sum(ss_quantity - sr_return_quantity) q, count(*) c, max(ss_pad) p
      |FROM store_sales JOIN (SELECT * FROM store_returns WHERE isnotnull(nullif(sr_return_quantity, 0))) r
      |  ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
      |GROUP BY ss_item_sk""".stripMargin

  private val q93Unfiltered =
    """SELECT ss_item_sk, sum(ss_quantity - sr_return_quantity) q, count(*) c, max(ss_pad) p
      |FROM store_sales JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
      |GROUP BY ss_item_sk""".stripMargin

  test("q93's shape: the larger fact gets bloom filters from the smaller one, results unchanged") {
    val df = run(q93)
    assert(salesBlooms(df) >= 1, df.queryExecution.optimizedPlan.treeString)
    // Spark's own runtime filter adds none here: the creation side has no selective filter.
    val off = withConf(VectorConf.FactBloomFilterEnabled -> "false")(run(q93))
    assert(salesBlooms(off) === 0, off.queryExecution.optimizedPlan.treeString)
  }

  test("the probe joins the fact's own filter directly above the scan, keeping its partition pruning") {
    import org.apache.spark.sql.catalyst.expressions.DynamicPruning
    import org.apache.spark.sql.catalyst.plans.logical.Filter
    import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec}
    try {
      // The sales partitioned by a date key and pruned through a filtered date table, as store_sales at 1 TB.
      spark.table("store_sales").selectExpr("*", "ss_item_sk % 7 AS ss_date").write.partitionBy("ss_date")
        .saveAsTable("fb_psales")
      spark.range(0, 7).selectExpr("id AS d_date", "id % 2 AS d_odd").write.saveAsTable("fb_pdate")
      val sql =
        """SELECT ss_item_sk, sum(ss_quantity - sr_return_quantity) q, count(*) c, max(ss_pad) p
          |FROM fb_psales JOIN fb_pdate ON ss_date = d_date
          |JOIN (SELECT * FROM store_returns WHERE isnotnull(nullif(sr_return_quantity, 0))) r
          |  ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
          |WHERE d_odd = 1 AND ss_quantity > 2
          |GROUP BY ss_item_sk""".stripMargin
      run(sql) // results unchanged (broadcast-reuse pruning only, as by default)
      // The physical planning, not run: a non-broadcast pruning subquery is what puts the expression in the plan.
      withConf("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "false") {
        shuffleOnly {
          val qe = withPlugin(enabled = true)(spark.sql(sql).queryExecution)
          val plan = qe.optimizedPlan
          assert(salesBlooms(spark.sql(sql)) >= 1, plan.treeString)
          assert(!plan.exists { case Filter(_, _: Filter) => true; case _ => false }, plan.treeString)
          // The scan took the pruning expression as a partition filter; no row filter evaluates it.
          val physical = qe.sparkPlan
          assert(
            physical.collect { case f: FileSourceScanExec => f }.exists(_.partitionFilters.exists(
              _.isInstanceOf[DynamicPruning]
            )),
            physical.treeString
          )
          assert(
            physical.collect { case f: FilterExec => f }.forall(!_.condition.exists(_.isInstanceOf[DynamicPruning])),
            physical.treeString
          )
        }
      }
    } finally {
      Seq("fb_psales", "fb_pdate").foreach { t =>
        spark.sessionState.catalog.dropTable(
          org.apache.spark.sql.catalyst.TableIdentifier(t),
          ignoreIfNotExists = true,
          purge = false
        )
      }
    }
  }

  test("the filter's Final aggregate over a row exchange stays Spark's, not ours over RowToColumnar (#647)") {
    import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, Final}
    import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
    object Plans extends org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
    // One partial filter per creation-side map task; Spark's RowToColumnarExec batches them by row count,
    // which overflows a 2 GB vector at 1 TB. Many small files give the creation side many map tasks here.
    val df = withConf("spark.sql.files.maxPartitionBytes" -> "4096", "spark.sql.files.openCostInBytes" -> "0")(run(q93))
    val all: Seq[SparkPlan] = Plans.collectWithSubqueries(df.queryExecution.executedPlan) { case p => p }
    def isBloomFinal(p: SparkPlan): Boolean = {
      val exprs = p match {
        case a: org.apache.spark.sql.execution.aggregate.BaseAggregateExec => a.aggregateExpressions
        case v: org.apache.spark.sql.vecruntime.VectorHashAggregateExec => v.aggregateExpressions
        case _ => Nil
      }
      exprs.exists(e => e.mode == Final && e.aggregateFunction.isInstanceOf[BloomFilterAggregate])
    }
    val finals = all.filter(isBloomFinal)
    assert(finals.nonEmpty, "no bloom filter aggregate found:\n" + all.headOption.map(_.treeString).getOrElse(""))
    val overR2C = finals.filter(_.children.exists(_.isInstanceOf[RowToColumnarExec]))
    assert(overR2C.isEmpty, "a bloom aggregate reads RowToColumnarExec:\n" + overR2C.map(_.treeString).mkString("\n"))
  }

  test("sides of similar size get no filter") {
    val sql =
      "SELECT count(*), max(a.ss_pad) FROM store_sales a JOIN store_sales b ON a.ss_ticket_number = b.ss_ticket_number"
    assert(salesBlooms(run(sql)) === 0)
  }

  test("a broadcastable creation side gets no filter") {
    val expected = withPlugin(enabled = false)(spark.sql(q93).collect())
    val df = withConf("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0") {
      withPlugin(enabled = true) { val d = spark.sql(q93); d.collect(); d }
    }
    assertRowsEqual(expected, df.collect(), 1e-9, q93)
    assert(salesBlooms(df) === 0)
  }

  test("the preserved side of an outer join gets no filter") {
    val sql =
      """SELECT count(*), count(sr_ticket_number) FROM store_sales LEFT JOIN store_returns
        |ON ss_ticket_number = sr_ticket_number""".stripMargin
    assert(salesBlooms(run(sql)) === 0)
  }

  test("the switch turns the rewrite off") {
    withConf(VectorConf.FactBloomFilterEnabled -> "false")(assert(salesBlooms(run(q93)) === 0))
  }

  /** The filters' subquery plans in the optimized plan. */
  private def filterSubqueries(df: DataFrame): Seq[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] =
    df.queryExecution.optimizedPlan.collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f }
      .flatMap(_.condition.collect { case b: BloomFilterMightContain => b.bloomFilterExpression })
      .flatMap(_.collect { case s: org.apache.spark.sql.catalyst.expressions.ScalarSubquery => s.plan })

  private def mergesInTwoLevels(p: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Boolean =
    p.exists {
      case a: org.apache.spark.sql.catalyst.plans.logical.Aggregate =>
        a.aggregateExpressions.exists(_.exists(_.isInstanceOf[org.apache.spark.sql.vecruntime.BloomFilterMerge]))
      case _ => false
    }

  test("the filter is merged in two levels, a bucket of the partition id first (#646)") {
    // Many small files: many creation-side tasks, so the per-task partials go through the buckets.
    val df = withConf("spark.sql.files.maxPartitionBytes" -> "4096", "spark.sql.files.openCostInBytes" -> "0")(run(q93))
    val subs = filterSubqueries(df)
    assert(subs.nonEmpty && subs.forall(mergesInTwoLevels), subs.map(_.treeString).mkString("\n"))
    // mergeBuckets = 1 is the single-level filter, with the same results.
    val one = withConf(VectorConf.FactBloomFilterMergeBuckets -> "1")(run(q93))
    val subs1 = filterSubqueries(one)
    assert(subs1.nonEmpty && !subs1.exists(mergesInTwoLevels), subs1.map(_.treeString).mkString("\n"))
  }

  test("bloom_filter_merge of bucket filters is the single-level filter, bit for bit; nulls are skipped") {
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.expressions.BoundReference
    import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
    import org.apache.spark.sql.types.BinaryType
    import org.apache.spark.util.sketch.BloomFilter
    val (items, bits) = (5000L, 1L << 16)
    def filterOf(keys: Seq[Long]): BloomFilter = { val f = BloomFilter.create(items, bits); keys.foreach(f.putLong); f }
    val all = (0L until 5000L).map(_ * 7919L)
    val single = BloomFilterAggregate.serialize(filterOf(all))
    val merge = org.apache.spark.sql.vecruntime.BloomFilterMerge(BoundReference(0, BinaryType, nullable = true))
    // Three buckets plus a null (a bucket with no key), split across two partial buffers merged together.
    val parts = all.grouped(1700).map(ks => BloomFilterAggregate.serialize(filterOf(ks))).toSeq
    val b1 = merge.createAggregationBuffer()
    merge.update(b1, InternalRow(parts.head))
    merge.update(b1, InternalRow(null))
    val b2 = merge.createAggregationBuffer()
    parts.tail.foreach(p => merge.update(b2, InternalRow(p)))
    val roundTrip = merge.deserialize(merge.serialize(b2))
    val out = merge.eval(merge.merge(b1, roundTrip)).asInstanceOf[Array[Byte]]
    assert(java.util.Arrays.equals(out, single))
    // Only nulls (or nothing): null, as bloom_filter_agg is with no key.
    val empty = merge.createAggregationBuffer()
    merge.update(empty, InternalRow(null))
    assert(merge.eval(merge.merge(empty, merge.deserialize(merge.serialize(merge.createAggregationBuffer())))) == null)
  }

  /** Bloom filters on the sales side, by the sales key they probe. */
  private def salesBloomKeys(df: DataFrame): Set[String] =
    df.queryExecution.optimizedPlan.collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f }
      .flatMap(_.condition.collect { case b: BloomFilterMightContain => b })
      .flatMap(_.valueExpression.references.map(_.name)).filter(_.startsWith("ss_")).toSet

  test("without statistics, a whole unfiltered creation side gets no filter: it would prune nothing (#650)") {
    assert(salesBlooms(run(q93Unfiltered)) === 0)
  }

  test("with ANALYZE column statistics, the filter follows the keys' distinct counts, not filters (#650)") {
    // Catalog copies with ANALYZE ... FOR ALL COLUMNS; spark.sql.cbo.enabled stays off.
    try {
      spark.table("store_sales").write.saveAsTable("fb_sales")
      spark.table("store_returns").write.saveAsTable("fb_returns")
      spark.sql("ANALYZE TABLE fb_sales COMPUTE STATISTICS FOR ALL COLUMNS")
      spark.sql("ANALYZE TABLE fb_returns COMPUTE STATISTICS FOR ALL COLUMNS")
      val sql = q93Unfiltered.replace("store_sales", "fb_sales").replace("store_returns", "fb_returns")
      // ticket numbers: 2000 of 10000 sales tickets have a return (0.2 <= 0.5) -> a filter, though unfiltered;
      // item keys: all 97 items on both sides (1.0) -> none, as for a whole dimension.
      assert(salesBloomKeys(run(sql)) === Set("ss_ticket_number"))
      // A stricter maxSelectivity than the ticket key's 0.2 declines it too.
      withConf(VectorConf.FactBloomFilterMaxSelectivity -> "0.1")(assert(salesBlooms(run(sql)) === 0))
      // Unless the creation side is reduced by a filter: the statistics describe the whole table, not the
      // reduced side, so the reduction's evidence still fires (as without statistics).
      val reduced = q93.replace("store_sales", "fb_sales").replace("store_returns", "fb_returns")
      withConf(VectorConf.FactBloomFilterMaxSelectivity -> "0.1")(assert(salesBlooms(run(reduced)) >= 1))
    } finally {
      Seq("fb_sales", "fb_returns").foreach { t =>
        spark.sessionState.catalog.dropTable(
          org.apache.spark.sql.catalyst.TableIdentifier(t),
          ignoreIfNotExists = true,
          purge = false
        )
      }
    }
  }

  /** Partitioned filters (#653) on the sales side's keys in the optimized plan. */
  private def partitionedBlooms(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f }
      .flatMap(_.condition.collect { case p: org.apache.spark.sql.vecruntime.PartitionedBloomMightContain => p })
      .size

  test("a filter too large for one bloom_filter_agg is partitioned by hash bucket, results unchanged (#653)") {
    // maxNumItems 100: the returns side's ~1300 keys need 50-item sub-filters, ~27 buckets.
    withConf("spark.sql.optimizer.runtime.bloomFilter.maxNumItems" -> "100") {
      val df = run(q93)
      assert(partitionedBlooms(df) >= 1, df.queryExecution.optimizedPlan.treeString)
      assert(salesBlooms(df) === 0, df.queryExecution.optimizedPlan.treeString) // no plain filter instead
      // A total size cap the filter cannot meet even at 4 bits a key (~400 keys, 1024 bits): declined, not built
      // saturated.
      withConf(VectorConf.FactBloomFilterMaxTotalBits -> "1024") {
        val small = run(q93)
        assert(
          partitionedBlooms(small) === 0 && salesBlooms(small) === 0,
          small.queryExecution.optimizedPlan.treeString
        )
      }
    }
  }

  test("partitioned filter: every key is found in its bucket's sub-filter; pack and unpack round-trip (#653)") {
    import org.apache.spark.sql.vecruntime.PartitionedBloomFilter
    import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
    import org.apache.spark.util.sketch.BloomFilter
    val buckets = 7
    val keys = (0L until 20000L).map(i => org.apache.spark.sql.catalyst.expressions.XXH64.hashLong(i * 31L, 42L))
    val subs = Array.tabulate(buckets) { b =>
      val ks = keys.filter(h => Math.floorMod(h, buckets.toLong) == b)
      if (b == 3) null // a bucket without keys
      else { val f = BloomFilter.create(5000L, 40000L); ks.foreach(f.putLong); BloomFilterAggregate.serialize(f) }
    }
    val filters = PartitionedBloomFilter.unpack(PartitionedBloomFilter.pack(subs))
    assert(filters.length == buckets && filters(3) == null)
    assert(keys.filter(h => Math.floorMod(h, buckets.toLong) != 3).forall(PartitionedBloomFilter.mightContain(
      filters,
      _
    )))
    assert(keys.filter(h => Math.floorMod(h, buckets.toLong) == 3).forall(!PartitionedBloomFilter.mightContain(
      filters,
      _
    )))
  }
}
