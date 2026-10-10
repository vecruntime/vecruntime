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

/** Test-only bridge to `VectorBroadcastExchangeExec`'s hooks (they are `private[vecruntime]`). */
object BroadcastTestHooks {

  /**
   * Runs `f` with every broadcast pausing `millis` after its completion promise is fulfilled and before
   * `relationFuture` returns: the window in which AQE sees the stage materialised but the relation future
   * is not yet done (#697).
   */
  def withDelayAfterCompletion[T](millis: Long)(f: => T): T = {
    VectorBroadcastExchangeExec.afterCompletionForTests = () => Thread.sleep(millis)
    try f
    finally VectorBroadcastExchangeExec.afterCompletionForTests = () => ()
  }
}
