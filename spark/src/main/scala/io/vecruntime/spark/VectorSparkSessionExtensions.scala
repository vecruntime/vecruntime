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

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.vecruntime.{
  MergeFilteredAggregates,
  SelfJoinToAggregate,
  VectorColumnarRule,
  VectorWriteDeltaStrategy
}

/**
 * Registers the planner rule. Enable with
 * `--conf spark.sql.extensions=io.vecruntime.spark.VectorSparkSessionExtensions`, or let
 * [[VectorPlugin]] do it through `--conf spark.plugins=io.vecruntime.spark.VectorPlugin`.
 */
class VectorSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(session => VectorColumnarRule(session))
    // Logical rewrites EMR Serverless also does (spark.vecruntime.optimizer.*); off with the plugin.
    extensions.injectOptimizerRule(session => SelfJoinToAggregate(session))
    extensions.injectOptimizerRule(session => MergeFilteredAggregates(session))
    // Once, before Spark's own DPP rules, after filters were pushed down (#633).
    extensions.injectPreCBORule(session => org.apache.spark.sql.vecruntime.DppThroughAggregate(session))
    extensions.injectPreCBORule(session => org.apache.spark.sql.vecruntime.TransitiveDpp(session))
    // The columnar v3 deletion-vector writer (#20): a planner strategy over the logical WriteDelta,
    // gated on spark.vecruntime.iceberg.dvWriter.enabled and a v3 target; declines to Spark otherwise.
    extensions.injectPlannerStrategy(session => VectorWriteDeltaStrategy(session))
    // Last optimizer batch ("User Provided Optimizers"), after Spark's final filter push-down, which
    // would move its pulled-up predicates back (#632). Extensions cannot inject there; the analyzer
    // runs before the first optimization, so a post-hoc resolution rule registers it once per session.
    extensions.injectPostHocResolutionRule(session =>
      org.apache.spark.sql.vecruntime.RegisterLateOptimizerRules(session)
    )
  }
}
