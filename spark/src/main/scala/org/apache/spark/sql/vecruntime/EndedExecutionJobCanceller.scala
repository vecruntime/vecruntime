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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * Cancels the jobs still running under a SQL execution once that execution has ended (#788).
 *
 * A query that returns while AQE is still materialising a stage nobody will read leaves that stage running:
 * at 3 TB, q16 returned from a plan with an empty relation 9 ms after AQE submitted a 2,909-task scan stage,
 * and the stage ran on for 11 s, holding every slot of the next query (q17: 5 s became 16 s). #694 already
 * skips a stage submitted after the end (`VectorShuffleExchangeExec.executionEnded`); this covers one
 * submitted just before it. A job carries its execution in the `spark.sql.execution.id` property; when
 * `SparkListenerSQLExecutionEnd` for that id arrives, every job of it still running is cancelled.
 *
 * Jobs started after the end (a local iterator's later partitions) carry no execution id and are untouched.
 * Off with `spark.vecruntime.cancelEndedExecutionJobs=false`.
 *
 * @param cancel cancels a job by id with a reason (`SparkContext.cancelJob`), injected for tests
 */
final class EndedExecutionJobCanceller(cancel: (Int, String) => Unit) extends SparkListener with Logging {

  /** Running job id -> its SQL execution id. */
  private val running = new ConcurrentHashMap[Int, Long]()

  override def onJobStart(e: SparkListenerJobStart): Unit = {
    val id = Option(e.properties).flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
    id.flatMap(s => scala.util.Try(s.toLong).toOption).foreach(x => running.put(e.jobId, x))
  }

  override def onJobEnd(e: SparkListenerJobEnd): Unit = running.remove(e.jobId)

  override def onOtherEvent(e: SparkListenerEvent): Unit = e match {
    case end: SparkListenerSQLExecutionEnd =>
      running.forEach { (job, exec) =>
        if (exec == end.executionId && running.remove(job) != null) {
          logInfo(s"Cancelling job $job: its SQL execution ${end.executionId} has ended (#788)")
          cancel(job, s"SQL execution ${end.executionId} ended; nothing reads this job's output (#788)")
        }
      }
    case _ =>
  }

  /** Jobs tracked as running (tests). */
  private[vecruntime] def tracked: Int = running.size
}
