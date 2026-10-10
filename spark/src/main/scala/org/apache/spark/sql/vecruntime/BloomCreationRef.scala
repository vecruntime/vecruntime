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
package org.apache.spark.sql.vecruntime

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Coalesce, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{
  BaseSubqueryExec,
  LeafExecNode,
  ReusedSubqueryExec,
  ScalarSubquery,
  SortExec,
  SparkPlan,
  SparkStrategy,
  SubqueryExec
}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.types.{BinaryType, BooleanType}

/**
 * The creation side of a `FactBloomFilter` filter, as its build subquery reads it (#659): a reference to the
 * join input the filter comes from, opaque to the optimizer, so the subquery does not plan a second copy of it.
 * [[ShareBloomCreationExchange]] replaces its physical placeholder with that join input's own shuffle exchange;
 * AQE's stage cache then materialises the exchange once, for the join and for the filter, and the build is a
 * read of a stage the join needs anyway -- EMR Serverless's `GenerateBloomFilter` over a `ReusedExchange`.
 */
case class BloomCreationRef(refId: Long, output: Seq[Attribute], sizeInBytes: BigInt) extends LeafNode {
  override def computeStats(): Statistics = Statistics(sizeInBytes = sizeInBytes)
}

object BloomCreationRef {
  private val ids = new AtomicLong()
  def nextId(): Long = ids.incrementAndGet()
}

/** The physical placeholder of [[BloomCreationRef]]; never executed (it is replaced, or its filter dropped). */
case class BloomCreationRefExec(refId: Long, output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"bloom filter creation reference $refId was not replaced by its join's exchange")
}

/** Plans [[BloomCreationRef]] as its placeholder. */
object BloomCreationRefStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case BloomCreationRef(id, output, _) => BloomCreationRefExec(id, output) :: Nil
    case _ => Nil
  }
}

/**
 * Query-stage preparation rule (AQE, after `EnsureRequirements`): for each shuffle join, a bloom filter
 * subquery on one side whose creation reference names the other side gets that side's shuffle exchange in place
 * of the reference. A reference no join claims -- the join became a broadcast, or the plan changed -- turns its
 * filter into a null literal, which the probe (`coalesce(might_contain(...), true)`) passes every row through.
 */
case class ShareBloomCreationExchange(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!plan.exists(_.expressions.exists(e => e.exists(isBloomSubquery)))) return plan
    val joined = plan.transformUp {
      case j: SortMergeJoinExec => share(j, j.left, j.right).map(c => j.withNewChildren(c)).getOrElse(j)
      case j: ShuffledHashJoinExec => share(j, j.left, j.right).map(c => j.withNewChildren(c)).getOrElse(j)
    }
    // Any reference left unclaimed: drop its filter rather than execute a placeholder.
    val dropped = joined.transformUp { case p =>
      p.transformExpressionsUp {
        case s: ScalarSubquery if refsIn(s).nonEmpty => Literal.create(null, BinaryType)
      }
    }
    // A dropped filter's probe is folded away: `coalesce(might_contain(null, h), true)` is true. Left in place, the
    // columnar filter cannot compile a probe of a literal and the whole scan falls back to Spark's row filter and
    // a row exchange (q50 at 1 TB: store_sales' shuffle 51 GiB -> 118.6 GiB, the query 2.3x slower, #659).
    dropped.transformUp { case p =>
      p.transformExpressionsUp {
        case Coalesce(Seq(m, Literal(true, BooleanType))) if probesNull(m) => Literal.TrueLiteral
      }
    }
  }

  private def probesNull(e: org.apache.spark.sql.catalyst.expressions.Expression): Boolean = e match {
    case p: PartitionedBloomMightContain => isNullLiteral(p.filter)
    case b: org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain => isNullLiteral(b.bloomFilterExpression)
    case _ => false
  }

  private def isNullLiteral(e: org.apache.spark.sql.catalyst.expressions.Expression): Boolean = e match {
    case Literal(null, _) => true
    case _ => false
  }

  private def isBloomSubquery(e: org.apache.spark.sql.catalyst.expressions.Expression): Boolean = e match {
    case s: ScalarSubquery => refsIn(s).nonEmpty
    case _ => false
  }

  /**
   * The subquery's adaptive plan. Spark merges scalar subqueries over the same plan (the filters of a two-key
   * join, `MergeScalarSubqueries`): later occurrences are a `ReusedSubqueryExec` of the first.
   */
  private def adaptive(s: ScalarSubquery): Option[(SubqueryExec, AdaptiveSparkPlanExec)] = unwrap(s.plan) match {
    case sq @ SubqueryExec(_, a: AdaptiveSparkPlanExec, _) => Some((sq, a))
    case _ => None
  }

  private def unwrap(p: BaseSubqueryExec): BaseSubqueryExec = p match {
    case r: ReusedSubqueryExec => unwrap(r.child)
    case other => other
  }

  private def refsIn(s: ScalarSubquery): Seq[BloomCreationRefExec] =
    adaptive(s).toSeq.flatMap(_._2.inputPlan.collect { case r: BloomCreationRefExec => r })

  /** The shuffle exchange a join side starts with (under the sort a sort-merge join puts on it). */
  private def exchangeOf(side: SparkPlan): Option[ShuffleExchangeExec] = side match {
    case e: ShuffleExchangeExec => Some(e)
    case s: SortExec => exchangeOf(s.child)
    case _ => None
  }

  private def share(j: SparkPlan, left: SparkPlan, right: SparkPlan): Option[Seq[SparkPlan]] = {
    val l = replaceIn(left, right)
    val r = replaceIn(right, left)
    if (l.eq(left) && r.eq(right)) None else Some(Seq(l, r))
  }

  /** `side` with its filters' creation references pointing at `other`'s exchange, when they name it. */
  private def replaceIn(side: SparkPlan, other: SparkPlan): SparkPlan = exchangeOf(other) match {
    case None => side
    case Some(exchange) =>
      val available = exchange.outputSet
      // One replacement per subquery, shared by all its occurrences (the first and any reused ones).
      val replaced = new java.util.IdentityHashMap[SubqueryExec, SubqueryExec]()
      side.transformUp { case p =>
        p.transformExpressionsUp {
          case s: ScalarSubquery if refsIn(s).nonEmpty =>
            adaptive(s) match {
              case Some((sq, a)) =>
                val refs = refsIn(s)
                // The reference must name this join's other side: its attributes are that side's output.
                val names = refs.forall(r => AttributeSet(r.output).subsetOf(available) && r.output.nonEmpty)
                // An exchange over a dynamically pruned scan cannot be shared: the subquery's own adaptive plan
                // cannot reuse the join's broadcast for the pruning, drops it, and reads the whole table (q50's
                // store_returns: 277M rows against 2M) in a stage of its own. Such a filter is dropped.
                val pruned = exchange.exists(_.expressions.exists(_.exists(
                  _.isInstanceOf[org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression]
                )))
                if (!names) s
                else if (pruned) Literal.create(null, BinaryType)
                else {
                  val shared = Option(replaced.get(sq)).getOrElse {
                    // The exchange takes the reference's logical link: the subquery's AQE re-plans from its logical
                    // plan as stages complete, and maps a finished stage back by that link -- without it the
                    // reference would be planned again as its placeholder.
                    val input = a.inputPlan.transformUp { case r: BloomCreationRefExec =>
                      val ex = exchange.copy()
                      r.logicalLink.foreach(ex.setLogicalLink)
                      ex
                    }
                    val fresh = sq.copy(child = a.copy(inputPlan = input))
                    replaced.put(sq, fresh)
                    fresh
                  }
                  s.plan match {
                    case _: ReusedSubqueryExec => s.copy(plan = ReusedSubqueryExec(shared))
                    case _ => s.copy(plan = shared)
                  }
                }
              case None => s
            }
        }
      }
  }
}
