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
package io.vecruntime.spark.expr

import java.io.ByteArrayInputStream

import org.apache.spark.sql.catalyst.expressions.XXH64
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.sketch.BloomFilter

/**
 * One deserialised runtime bloom filter per executor JVM, shared by every task that probes it.
 *
 * A runtime filter's bytes are a scalar subquery's result, stored in the plan, so each task receives its
 * own copy and, without this cache, its own `BloomFilter.readFrom` -- a second allocation of the filter's
 * size and a long-by-long read. At Spark's default cap (8 MB) that is cheap; a filter sized for a large
 * creation side (#641) is ~128 MB per task. Here the first task deserialises it and the others reuse it.
 *
 * The key is the content: the bytes' length and their 64-bit xxHash. Hashing is a single pass at memory
 * speed, much cheaper than `readFrom`, and makes a hit independent of plan identity -- the same query
 * re-run against changed data produces different bytes and so a different entry, never a stale filter.
 * Two concurrent tasks for the same filter wait on one deserialisation rather than both doing it.
 *
 * Bounded by total filter bytes, least-recently-used out first. A filter larger than the budget is
 * deserialised per caller and not kept.
 */
object BloomFilterCache {

  private final case class Key(length: Int, hash: Long)

  /** Holds the bytes until the filter is built once, then drops them. */
  private final class Entry(private var bytes: Array[Byte]) {
    val size: Long = bytes.length.toLong
    lazy val filter: BloomFilter = {
      val f = BloomFilter.readFrom(new ByteArrayInputStream(bytes))
      bytes = null
      f
    }
  }

  /** A quarter of the executor's max heap, at least 64 MB. */
  private[expr] val defaultBudget: Long = math.max(64L << 20, Runtime.getRuntime.maxMemory / 4)

  @volatile private var budget: Long = defaultBudget

  // Access order: iteration starts at the least recently used entry.
  private val entries = new java.util.LinkedHashMap[Key, Entry](16, 0.75f, true)
  private var total = 0L
  private var builds = 0L

  /** The filter the bytes serialise; deserialised at most once per JVM while the entry stays cached. */
  def get(bytes: Array[Byte]): BloomFilter = {
    if (bytes.length > budget) return BloomFilter.readFrom(new ByteArrayInputStream(bytes))
    val key = Key(bytes.length, XXH64.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, 42L))
    val entry = entries.synchronized {
      val e = entries.get(key)
      if (e != null) e
      else {
        val n = new Entry(bytes)
        entries.put(key, n)
        total += n.size
        builds += 1
        evict()
        n
      }
    }
    entry.filter // outside the map lock: other filters stay reachable while this one is read
  }

  private def evict(): Unit = {
    val it = entries.values.iterator
    while (total > budget && it.hasNext) {
      val e = it.next()
      it.remove()
      total -= e.size
    }
  }

  /** Test hooks: entries built since the last clear, and a different budget. */
  private[spark] def buildCount: Long = entries.synchronized(builds)
  private[spark] def cachedBytes: Long = entries.synchronized(total)

  private[spark] def clearForTest(newBudget: Long = defaultBudget): Unit = entries.synchronized {
    entries.clear()
    total = 0L
    builds = 0L
    budget = newBudget
  }
}
