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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkEnv

/**
 * #926: a task's share of the executor's off-heap pool, evaluated in the task, not divided once at plan time.
 *
 * The operators' caps used to be `offHeap.size / spark.executor.cores` for every task, so a narrow late stage -- a
 * few tasks on an executor whose pool is otherwise idle -- spilled at the same 1/cores share as a stage that fills
 * every slot. Spark's own pool already lets a task hold up to `pool / N` of the N tasks holding memory; this share
 * follows the same idea while keeping room for the tasks that have not started:
 *
 * {{{
 * reserve = (slots - running) * pool / (2 * slots)     // Spark's minimum for every slot not yet running
 * share   = max(static, (pool - pinned - reserve) / running)
 * }}}
 *
 * With every slot running the reserve is 0 and the share is the static one, as before (`max` keeps it there even
 * when pinned memory would lower it). With 3 of 15 running each gets about a fifth of the pool. The reserve keeps a
 * build admitted in a narrow stage -- which cannot give memory back yet (#875) -- from taking what Spark guarantees
 * a task that starts later.
 *
 * `running` counts this executor's tasks between the plugin's task hooks ([[taskStarted]], [[taskEnded]]). Off
 * unless `spark.vecruntime.memory.liveTaskShare.enabled`; off, or without a SparkEnv, the static share is returned.
 */
object TaskShare {

  private val running = new AtomicInteger()

  @volatile private var enabled = false

  /** Called by the executor plugin when it starts. */
  def configure(on: Boolean): Unit = enabled = on

  def isEnabled: Boolean = enabled

  /** The plugin's task hooks. */
  def taskStarted(): Unit = running.incrementAndGet()
  def taskEnded(): Unit = running.updateAndGet(n => math.max(0, n - 1))

  /** Tasks of this executor running now. */
  def runningTasks: Int = running.get()

  /** The share for a task now, at least `static`; `static` itself when off or without a pool. */
  def bytes(static: Long): Long = {
    if (!enabled || static <= 0L) return static
    val env = SparkEnv.get
    if (env == null) return static
    val pool = env.conf.getSizeAsBytes("spark.memory.offHeap.size", "0")
    if (pool <= 0L || !env.conf.getBoolean("spark.memory.offHeap.enabled", false)) return static
    val slots = math.max(
      1,
      env.conf.getInt("spark.executor.cores", 1) / math.max(1, env.conf.getInt("spark.task.cpus", 1))
    )
    of(pool, slots, running.get(), PinnedMemory.bytes, static)
  }

  /** The formula, for `running` of `slots` tasks over `pool` bytes of which `pinned` are held unspillable. */
  private[vecruntime] def of(pool: Long, slots: Int, running: Int, pinned: Long, static: Long): Long = {
    val active = math.min(slots, math.max(1, running))
    val reserve = (slots - active).toLong * (pool / (2L * slots))
    math.max(static, math.max(0L, pool - math.max(0L, pinned) - reserve) / active)
  }
}
