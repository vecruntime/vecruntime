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

import java.util.concurrent.{ArrayBlockingQueue, Callable, ExecutorService, Executors, Future, TimeUnit}

import org.apache.spark.TaskContext
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Parallel row-group decode for the native Parquet scan (#606): the row groups AFTER the one the task thread
 * consumes are decoded by HELPER threads, each on its own column-reader set and vectors, and handed back in
 * file order so the output is identical to the serial reader's.
 *
 * This is the issue's "a helper decodes the row group after next into its own ColumnChunkDecoder and vectors"
 * made concrete, the step the single-producer decode-ahead ([[DecodeAheadIterator]]) deliberately did not take
 * (one decode thread per task). It is OFF by default (`parallelDecode` = 0): a cluster running as many tasks as
 * cores keeps every core busy already, so a second decode thread per task only contends there (the 1 TB
 * TPC-DS A/B measured +1-5%); the win is where cores sit idle -- few large files with many large row groups,
 * and stage tails. It engages per file: only a file with at least two row groups at or above
 * `parallelDecode.minRowGroupBytes` is decoded in parallel.
 *
 * ## How it stays correct
 *
 *  - '''The lead owns I/O and ordering.''' The [[VectorParquetPartitionReader]] passed in keeps all of file
 *    iteration, read-ahead (`prefetchFiles` / `prefetchRowGroups`, which has already fetched a row group's
 *    compressed pages by the time a job is handed out), per-file fallback, metrics and order. `pollJob` is
 *    called only on this (consumer) thread, so no `ParquetFileReader` is ever used by two threads at once.
 *  - '''Jobs stay in order.''' `pollJob` yields, strictly in file+row-group order, either a row-group job
 *    (decoded on a helper) or a `LeadBatch` marker (a fallback or an ineligible native file, decoded on the
 *    lead by [[VectorParquetPartitionReader.leadBatch]]). Each unit is appended to an in-order queue, so the
 *    consumer always takes results in the order the lead produced them.
 *  - '''Ownership: a reader set per JOB.''' Each row-group job is decoded by its OWN fresh column-reader set in
 *    its OWN child allocator, so two jobs running at once (the point of this iterator) never share a stateful,
 *    single-instance reader or a vector. The job's set and allocator are closed the moment the consumer has
 *    released the last of that job's batches -- deterministic, not at task end -- so a steady state holds only
 *    the in-flight jobs' sets. A batch's vectors go back to their own job's reader pool on the consumer's next
 *    `next()` ([[VectorParquetPartitionReader.releaseOwned]]); nothing is handed to two consumers or released
 *    twice.
 *  - '''Threads: virtual with the JDK 25 rules.''' Helpers do pure CPU decode (no I/O, no file open), so a
 *    virtual thread fits. Class-init pinning (#566) is avoided because the FIRST job of a task is decoded on
 *    the consumer thread before any helper runs (the decode path's classes are initialized by then); file
 *    opens stay on the lead's platform prefetch pool. A `platform` switch keeps platform threads for an A/B.
 *  - '''Bounded in flight.''' At most `depth` + 1 units are outstanding, so the extra memory is a bounded
 *    number of row groups' decoded batches, and the consumer blocks on the pool rather than running ahead.
 *  - '''Close / LIMIT / failure.''' On task end, a LIMIT or a failure the consumer stops polling, cancels and
 *    drains the queued units (releasing every decoded batch and closing every in-flight job's set / allocator),
 *    and only then closes the lead. A helper's exception is raised on the consumer thread when it takes that
 *    job's result, in order.
 */
private[vecruntime] final class ParallelDecodeIterator(
    lead: VectorParquetPartitionReader,
    depth: Int,
    virtualThreads: Boolean,
    context: TaskContext
) extends Iterator[ColumnarBatch]
    with AutoCloseable {
  import VectorParquetPartitionReader.{LeadBatch, NoJob, OwnedBatch, RowGroupJob}

  /**
   * One job's decode result: its batches, and the reader set / allocator that produced them (null for a
   * LeadBatch, whose vectors belong to the lead's own readers). `remaining` counts the batches not yet
   * released; the set and allocator are closed when it hits 0.
   */
  private final class JobResult(
      val batches: java.util.List[OwnedBatch],
      val readers: Array[io.vecruntime.spark.parquet.NativeParquetColumn],
      val alloc: org.apache.arrow.memory.BufferAllocator
  ) {
    var remaining: Int = if (batches == null) 0 else batches.size()
    def closeSet(): Unit = if (alloc != null) {
      try if (readers != null) readers.foreach(r => if (r != null) r.close())
      finally alloc.close()
    }
  }

  /** An ordered unit: the future of a job's result, and whether it is helper-decoded (count on drain). */
  private final class Slot(val future: Future[JobResult], val fromJob: Boolean)

  private val helpers = math.max(1, depth)

  private val pool: ExecutorService = {
    val tf: java.util.concurrent.ThreadFactory = (r: Runnable) => {
      val name = s"vecruntime-parquet-pdecode-${if (context != null) context.taskAttemptId() else -1}"
      ParallelDecodeIterator.started.incrementAndGet()
      if (virtualThreads) {
        ParallelDecodeIterator.startedVirtual.incrementAndGet()
        Thread.ofVirtual().name(name).unstarted(r)
      } else Thread.ofPlatform().daemon(true).name(name).unstarted(r)
    }
    Executors.newFixedThreadPool(helpers, tf)
  }

  // At most `depth` + 1 units outstanding, so a bounded set of row groups is in flight.
  private val slots = new ArrayBlockingQueue[Slot](helpers + 1)
  // The job result whose batches are currently being drained, and its batch cursor into it.
  private var currentResult: JobResult = _
  private val pending = new java.util.ArrayDeque[OwnedBatch]()
  private var current: OwnedBatch = _ // handed out, released on the next next() / close
  private var currentOwner: JobResult = _ // the result `current` belongs to (its set closes at remaining 0)

  @volatile private var closed = false
  private var drained = false // pollJob returned NoJob; no more units will be enqueued
  private var primed = false // the first unit runs inline (class-init warm-up)
  private var lastFile: org.apache.spark.sql.execution.datasources.PartitionedFile = _

  if (context != null) context.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean = {
    if (closed) return false
    if (!pending.isEmpty) return true
    fill()
    val slot = slots.poll()
    if (slot == null) return false
    val res =
      try slot.future.get()
      catch {
        case e: java.util.concurrent.ExecutionException => throw (if (e.getCause != null) e.getCause else e)
      }
    currentResult = res
    var i = 0
    while (i < res.batches.size()) {
      val b = res.batches.get(i)
      // A LeadBatch was already counted by nextOwned; count helper-decoded job batches here, on this thread.
      if (slot.fromJob) lead.countOutput(b.batch.numRows())
      pending.add(b)
      i += 1
    }
    if (pending.isEmpty) { if (res.remaining == 0) res.closeSet(); hasNext }
    else true
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more batches")
    releaseCurrent()
    val o = pending.poll()
    current = o
    currentOwner = currentResult
    // input_file_name() (#568): set the task thread's holder from the file of the batch being consumed.
    if (o.file != null && (o.file ne lastFile)) {
      org.apache.spark.rdd.InputFileBlockHolder.set(o.file.urlEncodedPath, o.file.start, o.file.length)
      lastFile = o.file
    }
    o.batch
  }

  /** Enqueues units until the slots queue is full or the split is drained. The first unit runs inline. */
  private def fill(): Unit = {
    if (!primed && !drained) {
      primed = true
      enqueueOne(inline = true)
    }
    while (!drained && slots.remainingCapacity() > 0) enqueueOne(inline = false)
  }

  private def enqueueOne(inline: Boolean): Unit = {
    if (drained) return
    lead.pollJob() match {
      case NoJob => drained = true
      case LeadBatch =>
        // The lead decodes this one itself (fallback or ineligible native file), on the consumer thread.
        val b = lead.leadBatch()
        val res = new JobResult(java.util.List.of(b), null, null)
        slots.add(new Slot(ParallelDecodeIterator.done(res), fromJob = false))
      case job: RowGroupJob =>
        val f: Future[JobResult] =
          if (inline) ParallelDecodeIterator.done(decode(job))
          else
            pool.submit(new Callable[JobResult] {
              override def call(): JobResult = {
                if (context != null) TaskContext.setTaskContext(context)
                try decode(job)
                finally if (context != null) TaskContext.unset()
              }
            })
        slots.add(new Slot(f, fromJob = true))
      case other => throw new IllegalStateException(s"unexpected pollJob result: $other")
    }
  }

  /** Decodes one job with its own fresh reader set and child allocator (on whatever thread calls this). */
  private def decode(job: RowGroupJob): JobResult = {
    val alloc = io.vecruntime.spark.arrow.VectorAllocators.newChild("VectorParquetScanExec-pdecode")
    val readers = lead.readersForJob(job, alloc)
    try new JobResult(lead.decodeJob(job, readers, alloc), readers, alloc)
    catch {
      case t: Throwable =>
        try readers.foreach(r => if (r != null) r.close())
        finally alloc.close()
        throw t
    }
  }

  private def releaseCurrent(): Unit = if (current != null) {
    lead.releaseOwned(current)
    current = null
    if (currentOwner != null) {
      currentOwner.remaining -= 1
      if (currentOwner.remaining == 0) currentOwner.closeSet()
      currentOwner = null
    }
  }

  override def close(): Unit = if (!closed) {
    closed = true
    drained = true
    val interrupted = Thread.interrupted()
    try {
      // Release the batch handed out, and the batches queued for the current result.
      releaseCurrent()
      var o = pending.poll()
      while (o != null) {
        lead.releaseOwned(o)
        if (currentResult != null) {
          currentResult.remaining -= 1
          if (currentResult.remaining == 0) currentResult.closeSet()
        }
        o = pending.poll()
      }
      // Cancel and drain every queued slot, releasing its batches and closing its set.
      var slot = slots.poll()
      while (slot != null) {
        slot.future.cancel(true)
        try {
          val res = slot.future.get()
          var i = 0
          while (i < res.batches.size()) { lead.releaseOwned(res.batches.get(i)); i += 1 }
          res.closeSet()
        } catch { case _: Throwable => () }
        slot = slots.poll()
      }
      pool.shutdownNow()
      try pool.awaitTermination(30, TimeUnit.SECONDS)
      catch { case _: InterruptedException => Thread.currentThread().interrupt() }
    } finally {
      lead.close()
      if (interrupted) Thread.currentThread().interrupt()
    }
  }
}

object ParallelDecodeIterator {

  /** Helper threads started in this JVM, and how many were virtual (read by tests). */
  val started = new java.util.concurrent.atomic.AtomicLong()
  val startedVirtual = new java.util.concurrent.atomic.AtomicLong()

  /** An already-completed future wrapping `v`. */
  private def done[T](v: T): Future[T] = java.util.concurrent.CompletableFuture.completedFuture(v)
}
