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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Adds vecruntime's late logical rules to the session's last optimizer batch ("User Provided
 * Optimizers", `spark.experimental.extraOptimizations`), which runs after Spark's final filter
 * push-down. `SparkSessionExtensions` has no injection point there, and the session state is not
 * built yet when extension builders run, so this analyzer rule registers them on first use and
 * otherwise returns the plan unchanged. Rules already present (a cloned session) are not added twice.
 */
case class RegisterLateOptimizerRules(session: SparkSession) extends Rule[LogicalPlan] {

  @volatile private var registered = false

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!registered) synchronized {
      if (!registered) {
        val exp = session.experimental
        if (!exp.extraOptimizations.exists(_.isInstanceOf[SharedAggregateInputs])) {
          exp.extraOptimizations = exp.extraOptimizations :+ SharedAggregateInputs(session)
        }
        if (!exp.extraOptimizations.exists(_.isInstanceOf[NarrowBelowJoin])) {
          exp.extraOptimizations = exp.extraOptimizations :+ NarrowBelowJoin(session)
        }
        if (!exp.extraOptimizations.exists(_.isInstanceOf[AggregateBelowJoin])) {
          exp.extraOptimizations = exp.extraOptimizations :+ AggregateBelowJoin(session)
        }
        if (!exp.extraOptimizations.exists(_.isInstanceOf[FactBloomFilter])) {
          exp.extraOptimizations = exp.extraOptimizations :+ FactBloomFilter(session)
        }
        registered = true
      }
    }
    plan
  }
}
