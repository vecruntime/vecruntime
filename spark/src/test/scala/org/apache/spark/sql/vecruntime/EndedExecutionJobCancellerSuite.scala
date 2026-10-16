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

import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import io.vecruntime.spark.test.SparkVectorFunSuite
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

class EndedExecutionJobCancellerSuite extends SparkVectorFunSuite {

  private def props(exec: Option[Long]): Properties = {
    val p = new Properties()
    exec.foreach(e => p.setProperty(SQLExecution.EXECUTION_ID_KEY, e.toString))
    p
  }

  test("an ended execution's running jobs are cancelled, and only those (#788)") {
    val cancelled = ArrayBuffer.empty[Int]
    val l = new EndedExecutionJobCanceller((job, _) => cancelled.synchronized(cancelled += job))
    l.onJobStart(SparkListenerJobStart(1, 0L, Nil, props(Some(7L))))
    l.onJobStart(SparkListenerJobStart(2, 0L, Nil, props(Some(7L))))
    l.onJobStart(SparkListenerJobStart(3, 0L, Nil, props(Some(8L))))
    l.onJobStart(SparkListenerJobStart(4, 0L, Nil, props(None))) // no execution: never tracked
    l.onJobEnd(SparkListenerJobEnd(2, 1L, JobSucceeded)) // finished before the end: not cancelled
    assert(l.tracked === 2)
    l.onOtherEvent(SparkListenerSQLExecutionEnd(7L, 2L))
    assert(cancelled.toSeq === Seq(1))
    assert(l.tracked === 1)
    l.onOtherEvent(SparkListenerSQLExecutionEnd(7L, 3L)) // a second end changes nothing
    assert(cancelled.toSeq === Seq(1))
  }

  test("a job still running when its execution ends is cancelled on the cluster (#788)") {
    val sc = spark.sparkContext
    val l = new EndedExecutionJobCanceller((job, reason) => sc.cancelJob(job, reason))
    sc.addSparkListener(l)
    try {
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, "987654")
      val f = sc.parallelize(1 to 2, 2).map { i => Thread.sleep(60000); i }.collectAsync()
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
      val deadline = System.nanoTime() + 30.seconds.toNanos
      while (l.tracked == 0 && System.nanoTime() < deadline) Thread.sleep(50)
      assert(l.tracked === 1, "the job never registered with its execution id")
      val t0 = System.nanoTime()
      sc.listenerBus.post(SparkListenerSQLExecutionEnd(987654L, System.currentTimeMillis()))
      val e = intercept[Exception](scala.concurrent.Await.result(f, 30.seconds))
      assert(String.valueOf(e.getMessage).contains("#788") || String.valueOf(e.getCause).contains("#788"), e)
      assert((System.nanoTime() - t0).nanos < 30.seconds)
    } finally sc.removeSparkListener(l)
  }
}
