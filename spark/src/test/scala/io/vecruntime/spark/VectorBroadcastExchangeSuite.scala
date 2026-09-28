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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.vecruntime.{
  PlanUtils,
  VectorBroadcastExchangeExec,
  VectorBroadcastHashJoinExec,
  VectorBroadcastNestedLoopJoinExec
}

/**
 * The columnar broadcast exchange (#325): a hash-join broadcast over one of our plans travels as
 * Arrow batches and our join builds from them; a Spark consumer of the same exchange still gets
 * Spark's relation. The join semantics themselves are VectorJoinSuite's (which runs over this
 * exchange by default); this suite pins the exchange's placement and its bridges.
 */
class VectorBroadcastExchangeSuite extends VectorQuerySuite {

  private val BHJ = classOf[VectorBroadcastHashJoinExec]
  private val VBX = classOf[VectorBroadcastExchangeExec]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestTables.createMixed(spark, newTempPath("bx/t"))
    val dim = newTempPath("bx/dim")
    spark
      .range(0, 60)
      .selectExpr(
        "cast(id % 50 as int) as di",
        "if(id % 9 = 0, null, id * 3) as dl",
        "concat('s', id % 50) as ds",
        "cast(id as decimal(5,1)) as dd",
        "concat('name', id) as name",
        "cast(id as double) * 1.5 as weight"
      )
      .write
      .mode("overwrite")
      .parquet(dim)
    spark.read.parquet(dim).createOrReplaceTempView("dim")
    val tk = newTempPath("bx/tk")
    spark.sql("SELECT *, cast(i % 50 as int) AS i50 FROM t").write.mode("overwrite").parquet(tk)
    spark.read.parquet(tk).createOrReplaceTempView("tk")
    // A fact table partitioned on the join key, for dynamic partition pruning.
    val fact = newTempPath("bx/fact")
    spark.sql("SELECT i, l, s, i50 AS p FROM tk").write.mode("overwrite").partitionBy("p").parquet(fact)
    spark.read.parquet(fact).createOrReplaceTempView("fact")
  }

  /** Every broadcast exchange of the plan (Spark's or ours), through adaptive stages and reuse. */
  private def exchanges(df: DataFrame): Seq[SparkPlan] =
    PlanUtils.allNodes(finalPlan(df)).collect {
      case b: BroadcastExchangeExec => b
      case v: VectorBroadcastExchangeExec => v
    }

  private def assertNoRowConversionBelow(df: DataFrame): Unit =
    nodesOf[VectorBroadcastExchangeExec](df).foreach { e =>
      assert(
        !e.child.isInstanceOf[ColumnarToRowExec],
        s"a row conversion below our exchange\n${finalPlan(df).treeString}"
      )
    }

  private val queries = Seq(
    "SELECT tk.i, tk.s, dim.name FROM tk JOIN dim ON tk.i50 = dim.di",
    "SELECT tk.i, dim.name, dim.weight FROM tk JOIN dim ON tk.s = dim.ds WHERE tk.i < 5000",
    "SELECT tk.i, dim.name FROM tk JOIN dim ON cast(tk.i50 as decimal(5,1)) = dim.dd",
    "SELECT count(*), sum(dim.weight) FROM tk JOIN dim ON tk.i50 = dim.di AND tk.s = dim.ds",
    "SELECT tk.i, dim.name FROM tk LEFT JOIN dim ON tk.l = dim.dl",
    "SELECT tk.i FROM tk LEFT SEMI JOIN dim ON tk.i50 = dim.di AND dim.weight > 30",
    "SELECT tk.i FROM tk LEFT ANTI JOIN dim ON tk.s = dim.ds AND dim.weight > 30",
    "SELECT i, l FROM tk WHERE l NOT IN (SELECT dl FROM dim WHERE dl IS NOT NULL)"
  )

  test("a hash-join broadcast over our plan is ours, with and without adaptive execution") {
    Seq(true, false).foreach { aqe =>
      withConf("spark.sql.adaptive.enabled" -> aqe.toString) {
        queries.foreach { q =>
          val df = checkVectorized(q, Seq(BHJ, VBX))
          assert(
            exchanges(df).forall(_.isInstanceOf[VectorBroadcastExchangeExec]),
            s"a Spark broadcast exchange left (aqe=$aqe): $q\n${finalPlan(df).treeString}"
          )
          assertNoRowConversionBelow(df)
        }
      }
    }
  }

  test("the null-aware anti join's singleton regimes read the columnar broadcast") {
    // Adaptive execution short-circuits both regimes before any join runs; with it off the operator does.
    withConf("spark.sql.adaptive.enabled" -> "false") {
      val allNull = checkVectorized("SELECT i, l FROM tk WHERE l NOT IN (SELECT dl FROM dim)", Seq(BHJ, VBX))
      assert(allNull.collect().isEmpty)
      val empty =
        checkVectorized("SELECT i, l FROM tk WHERE l NOT IN (SELECT dl FROM dim WHERE dl > 1000000)", Seq(BHJ, VBX))
      assert(empty.collect().length === spark.table("tk").count())
    }
  }

  test("the exchange can be switched off; the join then reads Spark's relation") {
    withConf(VectorConf.BroadcastExchangeEnabled -> "false") {
      val df = checkVectorized(queries.head, Seq(BHJ, classOf[BroadcastExchangeExec]))
      assert(nodesOf[VectorBroadcastExchangeExec](df).isEmpty, finalPlan(df).treeString)
    }
  }

  test("a Spark join over our exchange gets Spark's relation (the bridge)") {
    // Our join switched off, the exchange kept: Spark's BroadcastHashJoinExec reads our exchange through
    // executeBroadcast, which builds Spark's relation from the batches.
    Seq(true, false).foreach { aqe =>
      withConf("spark.sql.adaptive.enabled" -> aqe.toString, VectorConf.BroadcastHashJoinEnabled -> "false") {
        queries.foreach { q =>
          val df = checkVectorized(q, Seq(VBX, classOf[BroadcastHashJoinExec]))
          assert(nodesOf[VectorBroadcastHashJoinExec](df).isEmpty)
          assert(
            nodesOf[ColumnarToRowExec](df).forall(c => VectorBroadcastExchangeExec.unapply(c.child).isEmpty),
            s"a row conversion over our broadcast (aqe=$aqe)\n${finalPlan(df).treeString}"
          )
        }
      }
    }
  }

  test("dynamic partition pruning over a join whose broadcast is ours keeps the right answer") {
    Seq(true, false).foreach { aqe =>
      withConf(
        "spark.sql.adaptive.enabled" -> aqe.toString,
        "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "true"
      ) {
        val q = "SELECT fact.i, fact.s, dim.name FROM fact JOIN dim ON fact.p = dim.di WHERE dim.weight < 20"
        val df = checkVectorized(q, Seq(BHJ))
        // Record how pruning fared with the exchange ours: Spark reuses the join's broadcast for the
        // partition filter only when it finds a matching broadcast exchange of its own.
        val pruned = finalPlan(df).toString.contains("dynamicpruning") || PlanUtils
          .allNodes(finalPlan(df))
          .exists(_.expressions.exists(_.toString.contains("dynamicpruning")))
        info(s"aqe=$aqe: dynamic pruning filter present=$pruned")
      }
    }
  }

  test("an empty build side broadcasts no batches and joins to nothing") {
    checkVectorized("SELECT tk.i, dim.name FROM tk JOIN dim ON tk.i50 = dim.di WHERE dim.weight > 1000000", Seq())
    checkVectorized(
      "SELECT tk.i, dim.name FROM tk LEFT JOIN dim ON tk.i50 = dim.di AND dim.weight > 1000000 WHERE tk.i < 100",
      Seq()
    )
  }

  test("a dictionary-encoded string build side (an aggregate's keys, #377) is decoded into the broadcast") {
    val q = "SELECT tk.i, a.s, a.n FROM tk JOIN (SELECT s, count(*) AS n FROM tk GROUP BY s) a ON tk.s = a.s"
    checkVectorized(q, Seq())
    val df = spark.sql(q)
    df.collect()
    assert(
      nodesOf[VectorBroadcastExchangeExec](df).nonEmpty,
      s"the exchange over the aggregate is ours\n${finalPlan(df).treeString}"
    )
  }

  private val nestedLoopQueries = Seq(
    "SELECT tk.i, dim.name FROM tk JOIN dim ON tk.i50 < dim.di AND tk.i < 200",
    "SELECT tk.i, dim.name FROM tk LEFT JOIN dim ON tk.i50 > dim.di + 40 WHERE tk.i < 300",
    "SELECT i FROM tk WHERE EXISTS (SELECT 1 FROM dim WHERE dim.di > tk.i50 + 45)",
    "SELECT i FROM tk WHERE NOT EXISTS (SELECT 1 FROM dim WHERE dim.di > tk.i50 + 45)",
    // The build side pruned to no columns: only its row count matters.
    "SELECT tk.i FROM tk CROSS JOIN (SELECT di FROM dim WHERE di < 3) d WHERE tk.i < 50"
  )

  test("a nested-loop join's identity broadcast over our plan is ours, with and without adaptive execution") {
    Seq(true, false).foreach { aqe =>
      withConf("spark.sql.adaptive.enabled" -> aqe.toString) {
        nestedLoopQueries.foreach { q =>
          val df = checkVectorized(q, Seq(classOf[VectorBroadcastNestedLoopJoinExec], VBX))
          assert(
            nodesOf[BroadcastExchangeExec](df).isEmpty,
            s"a Spark broadcast exchange left (aqe=$aqe): $q\n${finalPlan(df).treeString}"
          )
        }
      }
    }
  }

  test("a Spark nested-loop join over our identity broadcast gets Spark's rows (the bridge)") {
    Seq(true, false).foreach { aqe =>
      withConf("spark.sql.adaptive.enabled" -> aqe.toString, VectorConf.BroadcastNestedLoopJoinEnabled -> "false") {
        nestedLoopQueries.foreach { q =>
          val df = checkVectorized(q, Seq(VBX, classOf[BroadcastNestedLoopJoinExec]))
          assert(nodesOf[VectorBroadcastNestedLoopJoinExec](df).isEmpty, finalPlan(df).treeString)
        }
      }
    }
  }
}
