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

import org.scalatest.funsuite.AnyFunSuite

/**
 * #926: the live share -- the static share when every slot runs, more for a narrow stage, never into the room
 * reserved for the slots not yet running, and the static share whenever the feature is off.
 */
class TaskShareSuite extends AnyFunSuite {

  private val Pool = 30L << 30
  private val Slots = 15
  private val Static = Pool / Slots

  test("#926: with every slot running the share is the static one") {
    assert(TaskShare.of(Pool, Slots, Slots, 0L, Static) === Static)
    // Pinned memory would lower it; the static share stays the floor, as before #926.
    assert(TaskShare.of(Pool, Slots, Slots, 4L << 30, Static) === Static)
  }

  test("#926: a narrow stage gets more, and the slots not yet running keep Spark's minimum") {
    val three = TaskShare.of(Pool, Slots, 3, 0L, Static)
    // reserve = 12 slots * pool / 30 = 0.4 pool; (pool - reserve) / 3 = 0.2 pool.
    assert(three === (Pool - 12L * (Pool / 30)) / 3)
    assert(three > 2 * Static)
    val reserve = (Slots - 3).toLong * (Pool / (2L * Slots))
    assert(3L * three + reserve <= Pool, "the narrow stage leaves the late slots their minimum")
    // One task alone: everything but the reserve.
    assert(TaskShare.of(Pool, Slots, 1, 0L, Static) === Pool - 14L * (Pool / 30))
    // Pinned memory is not shared out.
    assert(TaskShare.of(Pool, Slots, 3, 6L << 30, Static) < three)
  }

  test("#926: running counts are clamped to the slots, and the counter never goes negative") {
    assert(TaskShare.of(Pool, Slots, 40, 0L, Static) === Static)
    assert(TaskShare.of(Pool, Slots, 0, 0L, Static) === TaskShare.of(Pool, Slots, 1, 0L, Static))
    val before = TaskShare.runningTasks
    TaskShare.taskStarted()
    assert(TaskShare.runningTasks === before + 1)
    TaskShare.taskEnded()
    TaskShare.taskEnded()
    TaskShare.taskEnded()
    assert(TaskShare.runningTasks >= 0)
  }

  test("#926: off (the default), or with no share, the static value comes back unchanged") {
    val was = TaskShare.isEnabled
    try {
      TaskShare.configure(false)
      assert(TaskShare.bytes(Static) === Static)
      TaskShare.configure(true)
      assert(TaskShare.bytes(0L) === 0L, "no fair share (an explicit cap) stays none")
    } finally TaskShare.configure(was)
  }
}
