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

import java.util.concurrent.{ArrayBlockingQueue, Semaphore, TimeUnit}

import org.apache.spark.TaskContext
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Decode-ahead for the native Parquet scan (#606): runs a [[VectorParquetPartitionReader]] on a producer thread
 * of its own, up to `depth` batches ahead of the task thread, so decoding overlaps the operators the task
 * thread runs above the scan. Batches come out in the reader's order, so the output is the reader's.
 *
 *  - Ownership: a batch is the consumer's until its next `next()`, when its vectors go back to their readers'
 *    pools (`releaseOwned`). A batch of Spark's fallback reader is recycled by that reader, so the producer
 *    waits until the consumer has moved past it (`hasNext` hands it back, as Spark's own reader invalidates
 *    its batch on `hasNext`).
 *  - Threads: one fresh thread per task, virtual by default. On JDK 25 a monitor no longer pins a virtual
 *    thread, but a class initializer still does (#566's q88 hang), so the first batch is decoded on the task
 *    thread before the producer starts: the decode path's classes are initialized by then. File opens keep
 *    running on the platform prefetch pool. A fresh thread also keeps the FileSystem byte count right: the
 *    reader's thread-bytes callback counts a thread it has not seen from zero.
 *  - Close (task end, LIMIT, failure): the producer is told to stop, the queue is drained (unblocking it),
 *    it is joined, every batch not handed out is released, and only then is the reader closed.
 *  - Failure: the producer's exception is queued in order and rethrown on the task thread.
 */
private[vecruntime] final class DecodeAheadIterator(
    inner: VectorParquetPartitionReader,
    depth: Int,
    virtualThreads: Boolean,
    context: TaskContext
) extends Iterator[ColumnarBatch]
    with AutoCloseable {
  import VectorParquetPartitionReader.OwnedBatch

  private final class Failure(val t: Throwable)
  private object End

  private val queue = new ArrayBlockingQueue[AnyRef](math.max(1, depth))
  private val handBack = new Semaphore(0) // released when the consumer is done with a recycled batch
  @volatile private var stopping = false
  private var producer: Thread = _
  private var started = false
  private var closed = false
  private var peeked: AnyRef = _
  private var current: OwnedBatch = _ // handed out, released on the next next() / close
  private var currentRecycledPending = false // a recycled batch handed out and not yet handed back

  if (context != null) context.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean = {
    if (closed) return false
    // Spark's reader invalidates its batch on hasNext; hand a recycled batch back so the producer may go on.
    handBackRecycled()
    if (peeked == null) peeked = take()
    peeked match {
      case End => false
      case f: Failure => throw f.t
      case _ => true
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    releaseCurrent()
    val o = peeked.asInstanceOf[OwnedBatch]
    peeked = null
    current = o
    currentRecycledPending = o.recycled
    o.batch
  }

  private def take(): AnyRef =
    if (!started) {
      started = true
      // The first batch on the task thread: class initialization happens here, not on a virtual thread.
      val first: AnyRef =
        try { if (inner.hasNext) inner.nextOwned() else End }
        catch { case t: Throwable => new Failure(t) }
      first match {
        case o: OwnedBatch => startProducer(waitFirst = o.recycled)
        case _ =>
      }
      first
    } else {
      try queue.take()
      catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw e
      }
    }

  private def startProducer(waitFirst: Boolean): Unit = {
    val run: Runnable = () => produce(waitFirst)
    val name = s"vecruntime-parquet-decode-${if (context != null) context.taskAttemptId() else -1}"
    producer =
      if (virtualThreads) Thread.ofVirtual().name(name).unstarted(run)
      else Thread.ofPlatform().daemon(true).name(name).unstarted(run)
    DecodeAheadIterator.started.incrementAndGet()
    if (producer.isVirtual) DecodeAheadIterator.startedVirtual.incrementAndGet()
    producer.start()
  }

  private def produce(waitFirst: Boolean): Unit = {
    if (context != null) TaskContext.setTaskContext(context)
    try {
      if (waitFirst) handBack.acquire()
      while (!stopping && inner.hasNext) {
        val o = inner.nextOwned()
        if (!offer(o)) { inner.releaseOwned(o); return }
        if (o.recycled) handBack.acquire()
      }
      if (!stopping) offer(End)
    } catch {
      case _: InterruptedException if stopping =>
      case t: Throwable => if (!stopping) offer(new Failure(t))
    } finally {
      if (context != null) TaskContext.unset()
    }
  }

  /** Puts `item` unless stopping; waits in short slices so a stop is noticed while the queue is full. */
  private def offer(item: AnyRef): Boolean = {
    while (!stopping) {
      if (queue.offer(item, 10, TimeUnit.MILLISECONDS)) return true
    }
    false
  }

  private def handBackRecycled(): Unit = if (currentRecycledPending) {
    currentRecycledPending = false
    handBack.release()
  }

  private def releaseCurrent(): Unit = if (current != null) {
    handBackRecycled()
    inner.releaseOwned(current)
    current = null
  }

  private def releaseItem(item: AnyRef): Unit = item match {
    case o: OwnedBatch => inner.releaseOwned(o)
    case _ =>
  }

  override def close(): Unit = if (!closed) {
    closed = true
    stopping = true
    // A killed task's thread arrives here interrupted (it was blocked in take()): clear the flag for the
    // drain and join, so a timed poll does not throw before the reader is closed, and restore it after.
    val interrupted = Thread.interrupted()
    try {
      if (producer != null) {
        handBack.release() // a producer waiting on a recycled batch
        while (producer.isAlive) {
          // Drain so a producer blocked on a full queue sees the stop; release what it had queued.
          val item =
            try queue.poll(10, TimeUnit.MILLISECONDS)
            catch { case _: InterruptedException => null }
          if (item != null) releaseItem(item)
        }
      }
      var item = queue.poll()
      while (item != null) { releaseItem(item); item = queue.poll() }
      if (peeked != null) { releaseItem(peeked); peeked = null }
      releaseCurrent()
    } finally {
      inner.close()
      if (interrupted) Thread.currentThread().interrupt()
    }
  }
}

object DecodeAheadIterator {

  /** Producers started in this JVM, and how many of them were virtual threads (read by tests). */
  val started = new java.util.concurrent.atomic.AtomicLong()
  val startedVirtual = new java.util.concurrent.atomic.AtomicLong()
}
