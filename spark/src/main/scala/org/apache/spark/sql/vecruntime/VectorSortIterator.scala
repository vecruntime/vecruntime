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

import java.lang.foreign.Arena

import scala.collection.mutable.ArrayBuffer

import io.vecruntime.kernels.{ColumnBuilder, RunMerge, SortKernels, VecType, VectorBuffers}
import io.vecruntime.spark.arrow.{ArrowOutput, ArrowVectorBuffers, VectorAllocators}
import io.vecruntime.spark.adapter.TypeMapping
import io.vecruntime.spark.expr.{ColumnRef, EvalContext, VectorExpr}
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Drains the partition into sorted runs of `runRows` rows, then emits the rows in order -- at most
 * `limit` of them -- merging the runs when there are several.
 *
 * Memory is bounded by `spillBytes` (#416): every run lives in an arena of its own, the bytes
 * appended are counted, and once the resident runs pass the budget a newly sealed run is written
 * to a local Arrow IPC file in sorted order and its arena closed. What stays resident is at most the
 * budget plus one run. A spilled run takes part in the merge as a refillable run of [[RunMerge]]:
 * one batch of it is loaded at a time, the merge stops at a batch boundary so the rows already
 * emitted can be gathered from the batch they refer to, and the next batch is loaded after. An
 * output batch is therefore up to `OutputBatchSize` rows, fewer where a spilled run's batch ended.
 */
private[vecruntime] object VectorSortIterator {

  /** For tests driving the operators without a SparkEnv: the memory mode to report instead of the conf's. */
  @volatile private[vecruntime] var modeOverride: org.apache.spark.memory.MemoryMode = null

  /** #867: the pool the sort's memory comes from: off-heap execution memory when Spark has one. */
  def memoryMode(): org.apache.spark.memory.MemoryMode = {
    if (modeOverride != null) return modeOverride
    val env = org.apache.spark.SparkEnv.get
    if (env != null && env.conf.getBoolean("spark.memory.offHeap.enabled", false))
      org.apache.spark.memory.MemoryMode.OFF_HEAP
    else org.apache.spark.memory.MemoryMode.ON_HEAP
  }
}

private[vecruntime] class VectorSortIterator(
    input: Iterator[ColumnarBatch],
    keyExprs: Array[VectorExpr],
    ascending: Array[Boolean],
    nullsFirst: Array[Boolean],
    outputAttrs: Array[(String, DataType)],
    metrics: VectorMetrics,
    limit: Int = Int.MaxValue,
    runRows: Int = 1 << 20,
    spillBytes: Long = Long.MaxValue
) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  /**
   * Whether an output column is an array, map or struct (#705). Such a column is copied by the column builders'
   * nested lanes and gathered as a nested lane, which only the single in-memory run does: the partition is then
   * one run, never spilled -- the runs' merge gather and the spill files hold lanes only.
   */
  private val hasNested: Boolean = outputAttrs.exists { case (_, dt) => TypeMapping.nestedTypeOf(dt) != null }
  private val runRowLimit: Int = if (hasNested) Int.MaxValue else runRows
  private val spillLimit: Long = if (hasNested) Long.MaxValue else spillBytes

  /** A run the merge reads: its key columns and output columns as they currently stand. */
  private sealed trait Run {
    def kept: Int
    def keys: Array[VectorBuffers]
    def columns: Array[VectorBuffers]
    def permutation: Array[Int]
    def close(): Unit
  }

  /**
   * A sealed run held in memory: its output columns, its key columns and the permutation that
   * orders it, in an arena of its own so that spilling it frees its memory. Under a limit only the
   * first `limit` rows of the order can ever be emitted, so that is all the merge walks (`kept`): a
   * top-N over a large partition costs the merge n rows per run, not the partition.
   */
  private final class MemoryRun(
      val columns: Array[VectorBuffers],
      val keys: Array[VectorBuffers],
      val rows: Int,
      val bytes: Long,
      val arena: Arena,
      builders: Array[ColumnBuilder] // #867: owning builders whose views the run holds
  ) extends Run {
    val permutation: Array[Int] = SortKernels.sortIndices(keys, ascending, nullsFirst, rows)
    val kept: Int = math.min(rows, limit)

    /** #868: the run's heap side -- its permutation, and at the merge its normalised keys -- for the on-heap pool. */
    def heapBytes: Long = rows.toLong * heapBytesPerRow
    def close(): Unit = { builders.foreach(_.close()); arena.close() }
  }

  /**
   * A run spilled to a local Arrow IPC file in sorted order: its `kept` rows in batches of
   * `OutputBatchSize`, the output columns first and the computed keys after them. Read back one
   * batch at a time: `columns` and `keys` are the batch currently loaded, already in order.
   */
  private final class SpilledRun(val file: java.io.File, val kept: Int) extends Run {
    private var channel: java.nio.channels.FileChannel = _
    private var reader: org.apache.arrow.vector.ipc.ArrowStreamReader = _
    var columns: Array[VectorBuffers] = _
    var keys: Array[VectorBuffers] = _
    var rows: Int = 0
    def permutation: Array[Int] = null

    /** Loads the next batch; false when the run is read through. */
    def load(): Boolean = {
      if (reader == null) {
        channel = java.nio.channels.FileChannel.open(file.toPath, java.nio.file.StandardOpenOption.READ)
        reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(channel, allocator)
      }
      if (!reader.loadNextBatch()) { rows = 0; return false }
      val root = reader.getVectorSchemaRoot
      rows = root.getRowCount
      columns = Array.tabulate(numColumns)(c => ArrowVectorBuffers.forRead(root.getVector(c)))
      keys = new Array[VectorBuffers](keyExprs.length)
      var computedIdx = 0
      var k = 0
      while (k < keys.length) {
        if (keyColumn(k) >= 0) keys(k) = columns(keyColumn(k))
        else { keys(k) = ArrowVectorBuffers.forRead(root.getVector(numColumns + computedIdx)); computedIdx += 1 }
        k += 1
      }
      true
    }

    def close(): Unit = {
      if (reader != null) { reader.close(); reader = null }
      if (channel != null) { channel.close(); channel = null }
      file.delete()
    }
  }

  private val OutputBatchSize = 4096

  private val allocator: BufferAllocator = VectorAllocators.newChild("VectorSortExec")

  private val numColumns = outputAttrs.length

  /** Key c is output column `keyColumn(c)`, or -1 when it is a computed expression with its own chunks. */
  private val keyColumn: Array[Int] = keyExprs.map {
    case ColumnRef(ordinal, _) => ordinal
    case _ => -1
  }
  private val computedKeys: Array[Int] = keyColumn.indices.filter(keyColumn(_) < 0).toArray

  private var sorted = false
  private val runs = ArrayBuffer.empty[Run]

  /** Bytes of the sealed runs held in memory: their builders' allocated capacity (#867). */
  private var residentBytes = 0L

  /**
   * #867: the sort's memory from Spark's execution memory, as the task's share of the executor's pool: OFF_HEAP
   * when `spark.memory.offHeap.enabled` (our runs are native), else ON_HEAP as the hash aggregate's (#367). The
   * runs' allocated bytes are acquired as they grow, a refusal seals and spills, and another consumer of the task
   * can ask the sort to spill its sealed runs while it drains. `spillBytes` stays a hard cap.
   */
  private val consumer: SortMemoryConsumer =
    Option(TaskContext.get()).map(tc => new SortMemoryConsumer(tc.taskMemoryManager())).orNull
  private var reserved = 0L
  private var draining = false

  private final class SortMemoryConsumer(tmm: org.apache.spark.memory.TaskMemoryManager)
      extends org.apache.spark.memory.MemoryConsumer(tmm, tmm.pageSizeBytes(), VectorSortIterator.memoryMode()) {
    override def spill(size: Long, trigger: org.apache.spark.memory.MemoryConsumer): Long =
      if ((trigger eq this) || !draining || hasNested) 0L else spillResident(size, 0L)._1
  }

  /**
   * #868: the runs' heap side in the on-heap pool when the runs themselves are off-heap. A resident run keeps an
   * `int` permutation per row, and the merge normalises every key of every resident run into a `long` per row (and
   * a null flag per row where the key has nulls) -- with a pool-sized off-heap budget that is gigabytes of heap per
   * executor, which a small heap cannot hold (q14a at 10 TB with a 20g heap). A refusal spills like the native one.
   * With an on-heap pool the native consumer is already on-heap, and the heap side rides on its cap.
   */
  private lazy val heapBytesPerRow: Long = 4L + 9L * keyExprs.length
  private val heapConsumer: HeapSideConsumer =
    if (VectorSortIterator.memoryMode() != org.apache.spark.memory.MemoryMode.OFF_HEAP) null
    else Option(TaskContext.get()).map(tc => new HeapSideConsumer(tc.taskMemoryManager())).orNull
  private var heapReserved = 0L
  private var residentHeap = 0L

  private final class HeapSideConsumer(tmm: org.apache.spark.memory.TaskMemoryManager)
      extends org.apache.spark.memory.MemoryConsumer(
        tmm,
        tmm.pageSizeBytes(),
        org.apache.spark.memory.MemoryMode.ON_HEAP
      ) {
    override def spill(size: Long, trigger: org.apache.spark.memory.MemoryConsumer): Long =
      if ((trigger eq this) || !draining || hasNested) 0L else spillResident(0L, size)._2
  }

  private def holdHeap(bytes: Long): Boolean = {
    if (heapConsumer == null || bytes <= heapReserved) return true
    heapReserved += heapConsumer.acquireMemory(bytes - heapReserved)
    heapReserved >= bytes
  }

  private def releaseHeapTo(bytes: Long): Unit =
    if (heapConsumer != null && heapReserved > bytes) {
      val give = heapReserved - math.max(bytes, 0L)
      heapConsumer.freeMemory(give)
      heapReserved -= give
    }

  /** Holds `bytes` of the pool for the sort, acquiring what is missing; false when the pool grants less. */
  private def hold(bytes: Long): Boolean = {
    if (consumer == null || bytes <= reserved) return true
    reserved += consumer.acquireMemory(bytes - reserved)
    reserved >= bytes
  }

  /** Returns what the sort holds past `bytes` to the pool. */
  private def releaseTo(bytes: Long): Unit =
    if (consumer != null && reserved > bytes) {
      val give = reserved - math.max(bytes, 0L)
      consumer.freeMemory(give)
      reserved -= give
    }

  /**
   * Spills sealed in-memory runs, the largest first, until `size` native bytes and `heapSize` heap bytes are freed;
   * the native and heap bytes freed.
   */
  private def spillResident(size: Long, heapSize: Long): (Long, Long) = {
    var freed = 0L
    var heapFreed = 0L
    val inMemory = runs.indices.collect { case i if runs(i).isInstanceOf[MemoryRun] => i }
      .sortBy(i => -runs(i).asInstanceOf[MemoryRun].bytes)
    val it = inMemory.iterator
    while ((freed < size || heapFreed < heapSize) && it.hasNext) {
      val i = it.next()
      val r = runs(i).asInstanceOf[MemoryRun]
      runs(i) = spill(r)
      residentBytes -= r.bytes
      residentHeap -= r.heapBytes
      freed += r.bytes
      heapFreed += r.heapBytes
    }
    if (freed > 0) releaseTo(reserved - freed)
    if (heapFreed > 0) releaseHeapTo(heapReserved - heapFreed)
    (freed, heapFreed)
  }

  /** Runs written to disk and their bytes, for the operator's description and the tests. */
  var spilledRuns = 0
  var spilledBytes = 0L

  /** One run: its columns and permutation. Several: the merge, and every output column's run columns. */
  private var columns: Array[VectorBuffers] = _
  private var permutation: Array[Int] = _
  private var merge: RunMerge = _
  private var runColumns: Array[Array[VectorBuffers]] = _
  // One heap mirror of the runs' offsets and validity per output column (#565), rebound per batch.
  private var runMirrors: Array[io.vecruntime.kernels.RunMirrors] = _
  private var offsetScratch: Array[Int] = _
  private var runOf: Array[Int] = _
  private var rowOf: Array[Int] = _
  private var total = 0
  private var emitted = 0
  private var current: ColumnarBatch = _
  private var closed = false

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  /** Rough size of `count` rows of `in`: the fixed width, or a string column's mean bytes per row plus its offset. */
  /** Column `c` of the batch as a lane: an array, map or struct read into a nested lane (#705). */
  private def inputLane(ctx: EvalContext, batch: ColumnarBatch, c: Int): VectorBuffers = {
    val dt = outputAttrs(c)._2
    if (TypeMapping.nestedTypeOf(dt) == null) ctx.input(c)
    else io.vecruntime.spark.adapter.NestedColumns.adapt(batch.column(c), dt, ctx.numRows, ctx.arena)
  }

  /** Rows `permutation[from, to)` of a nested lane, handed to Spark as its nested column. */
  private def nestedGather(
      name: String,
      dt: DataType,
      in: VectorBuffers,
      permutation: Array[Int],
      from: Int,
      to: Int
  ): ColumnVector = {
    val arena = Arena.ofConfined()
    try {
      val rows = java.util.Arrays.copyOfRange(permutation, from, to)
      io.vecruntime.spark.arrow.VectorNestedColumnVector.of(
        name,
        dt,
        io.vecruntime.spark.expr.ElementAccessExpr.gather(in, rows, to - from, arena),
        allocator
      )
    } finally arena.close()
  }

  /** Writes `run`'s kept rows in sorted order to a local file and frees its memory. */
  private def spill(run: MemoryRun): SpilledRun = {
    val file = AggregateSpill.newFile()
    val types: Array[(String, DataType)] = outputAttrs ++ computedKeys.map(k => (s"key$k", keyExprs(k).dataType))
    Option(file.getParentFile).foreach(_.mkdirs())
    val channel = java.nio.channels.FileChannel.open(
      file.toPath,
      java.nio.file.StandardOpenOption.CREATE,
      java.nio.file.StandardOpenOption.WRITE,
      java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
    )
    var writer: org.apache.arrow.vector.ipc.ArrowStreamWriter = null
    var root: org.apache.arrow.vector.VectorSchemaRoot = null
    try {
      var from = 0
      while (from < run.kept) {
        val to = math.min(run.kept, from + OutputBatchSize)
        val vectors = new Array[org.apache.arrow.vector.FieldVector](types.length)
        var c = 0
        while (c < types.length) {
          val (name, dt) = types(c)
          val source = if (c < numColumns) run.columns(c) else run.keys(computedKeys(c - numColumns))
          vectors(c) =
            AggregateSpill.vectorOf(ArrowOutput.gather(name, dt, source, run.permutation, from, to, allocator))
          c += 1
        }
        if (writer == null) {
          root = new org.apache.arrow.vector.VectorSchemaRoot(java.util.Arrays.asList(vectors: _*))
          writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, channel)
          writer.start()
        } else {
          // The root keeps its first vectors: later batches move theirs in.
          c = 0
          while (c < types.length) {
            vectors(c).makeTransferPair(root.getVector(c)).transfer(); vectors(c).close(); c += 1
          }
        }
        root.setRowCount(to - from)
        writer.writeBatch()
        from = to
      }
      if (writer != null) writer.end()
      AggregateSpill.reportSpill(channel.size(), run.bytes) // before the writer's close, which closes the channel
    } finally {
      if (writer != null) writer.close()
      if (root != null) root.close()
      channel.close()
    }
    spilledRuns += 1
    spilledBytes += run.bytes
    run.close()
    new SpilledRun(file, run.kept)
  }

  private def drainAndSort(): Unit = {
    if (sorted) return
    sorted = true
    var columnBuilders: Array[ColumnBuilder] = null
    var keyBuilders: Array[ColumnBuilder] = null
    var runArena: Arena = null
    var runTotal = 0

    /** Seals the builders into a run: plain string columns (a dictionary decoded once), the keys, the permutation. */
    def seal(): Unit = {
      if (runTotal > 0) {
        val cols = columnBuilders.map(_.view()).map { v =>
          if (v.`type`() == VecType.UTF8 && v.isDictionaryEncoded) ArrowOutput.decodeDictionary(v, runArena) else v
        }
        val computed = keyBuilders.map(_.view())
        val keys = new Array[VectorBuffers](keyExprs.length)
        var computedIdx = 0
        var k = 0
        while (k < keys.length) {
          if (keyColumn(k) >= 0) keys(k) = cols(keyColumn(k))
          else { keys(k) = computed(computedIdx); computedIdx += 1 }
          k += 1
        }
        val builders = columnBuilders ++ keyBuilders
        val bytes = builders.map(_.allocatedBytes()).sum
        val run = new MemoryRun(cols, keys, runTotal, bytes, runArena, builders)
        total += runTotal
        residentBytes += bytes
        residentHeap += run.heapBytes
        // Past the budget -- the cap, or what the pool grants this task (#867), native or heap side (#868) -- the
        // newest run goes to disk in sorted order and its memory is returned, then the largest resident runs until
        // the rest fit.
        if (!hasNested && (residentBytes > spillLimit || !hold(residentBytes) || !holdHeap(residentHeap))) {
          runs += spill(run)
          residentBytes -= bytes
          residentHeap -= run.heapBytes
          val allowed = if (consumer == null) spillLimit else math.min(spillLimit, reserved)
          val heapAllowed = if (heapConsumer == null) Long.MaxValue else heapReserved
          if (residentBytes > allowed || residentHeap > heapAllowed)
            spillResident(math.max(0L, residentBytes - allowed), math.max(0L, residentHeap - heapAllowed))
          releaseTo(residentBytes)
          releaseHeapTo(residentHeap)
        } else {
          runs += run
        }
        runArena = null
      } else if (runArena != null) {
        (columnBuilders ++ keyBuilders).foreach(_.close())
        runArena.close(); runArena = null
      }
      columnBuilders = null
      keyBuilders = null
      runTotal = 0
    }

    draining = true
    while (input.hasNext) {
      val batch = input.next()
      if (batch.numRows() > 0) {
        metrics.timed {
          metrics.numInputBatches += 1
          EvalContexts.withBatch(batch) { ctx =>
            val count = ctx.selectedCount
            if (count > 0) {
              // #867: reserve before growing. A builder doubles when it fills, so the append that crosses a
              // capacity boundary can double the run's bytes at once; when the grown run would not fit the
              // budget, the run is sealed (and spilled) as it is and the batch starts a fresh one. The computed
              // keys are counted at their current capacity; the check after the append covers their growth.
              if (columnBuilders != null && !hasNested && runTotal > 0) {
                var projected = residentBytes
                var c = 0
                while (c < numColumns) {
                  projected += columnBuilders(c).bytesAfterAppend(inputLane(ctx, batch, c), ctx.selection, count)
                  c += 1
                }
                keyBuilders.foreach(b => projected += b.allocatedBytes())
                if (projected > spillLimit || !hold(projected)) seal()
              }
              if (columnBuilders == null) {
                val expected = math.max(math.min(runRowLimit, count), 1)
                runArena = Arena.ofShared()
                // #867: owning builders -- a buffer the run outgrows is freed at once, where the run's shared arena
                // kept every doubling's leftovers (a column of C bytes held 2C). Nested lanes stay in the arena.
                def builder(t: VecType): ColumnBuilder =
                  if (t.isNested) new ColumnBuilder(runArena, t, expected) else ColumnBuilder.owning(t, expected)
                columnBuilders = Array.tabulate(numColumns)(c => builder(inputLane(ctx, batch, c).`type`()))
                keyBuilders = Array.tabulate(computedKeys.length)(k => builder(keyExprs(computedKeys(k)).vecType))
              }
              var c = 0
              while (c < numColumns) {
                val in = inputLane(ctx, batch, c)
                columnBuilders(c).append(in, ctx.selection, count)
                c += 1
              }
              var k = 0
              while (k < computedKeys.length) {
                val v = keyExprs(computedKeys(k)).eval(ctx)
                keyBuilders(k).append(v, ctx.selection, count)
                k += 1
              }
              runTotal += count
              // #867: the run being built counts too: once it and the sealed runs pass the budget (the cap or the
              // pool's grant) it is sealed now rather than at its row limit, and sealing spills.
              if (runTotal >= runRowLimit) seal()
              else if (!hasNested && runTotal >= OutputBatchSize) {
                val building = (columnBuilders ++ keyBuilders).map(_.allocatedBytes()).sum
                if (residentBytes + building > spillLimit || !hold(residentBytes + building)) seal()
              }
            }
          }
        }
      }
    }
    metrics.timed {
      seal()
      draining = false
      // Every spilled run's first batch is loaded so the merge can seat it; an empty one leaves.
      val loaded = runs.filter {
        case r: SpilledRun => r.load()
        case _ => true
      }
      runs.clear(); runs ++= loaded
      if (runs.length == 1 && runs.head.isInstanceOf[MemoryRun]) {
        columns = runs.head.columns
        permutation = runs.head.permutation
      } else if (runs.nonEmpty) {
        merge = new RunMerge(
          runs.map(_.keys).toArray,
          runs.map(_.permutation).toArray,
          runs.map { case r: MemoryRun => r.kept; case r: SpilledRun => r.rows }.toArray,
          ascending,
          nullsFirst,
          runs.map(_.isInstanceOf[SpilledRun]).toArray
        )
        runColumns = Array.tabulate(numColumns)(c => runs.map(_.columns(c)).toArray)
        runOf = new Array[Int](OutputBatchSize)
        rowOf = new Array[Int](OutputBatchSize)
        runMirrors = Array.fill(numColumns)(new io.vecruntime.kernels.RunMirrors)
        offsetScratch = new Array[Int](OutputBatchSize + 1)
      }
      // A top-N emits only the head of the order (every run was still sorted whole).
      total = math.min(total, limit)
    }
  }

  override def hasNext: Boolean = {
    drainAndSort()
    emitted < total
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("no more sorted rows")
    releaseCurrent()
    val from = emitted
    val out = new Array[ColumnVector](numColumns)
    var count = 0
    metrics.timed {
      if (merge == null) {
        val to = math.min(total, from + OutputBatchSize)
        count = to - from
        var c = 0
        while (c < numColumns) {
          val (name, dt) = outputAttrs(c)
          out(c) =
            if (TypeMapping.nestedTypeOf(dt) == null)
              ArrowOutput.gather(name, dt, columns(c), permutation, from, to, allocator)
            else nestedGather(name, dt, columns(c), permutation, from, to)
          c += 1
        }
      } else {
        // A batch ends early when a spilled run's loaded piece is used up: the rows so far are
        // gathered from the pieces as they stand, then that run is refilled from its file.
        count = merge.next(runOf, rowOf, math.min(OutputBatchSize, total - from))
        var c = 0
        while (c < numColumns) {
          val (name, dt) = outputAttrs(c)
          out(c) = ArrowOutput.gatherRuns(
            name,
            dt,
            runColumns(c),
            runOf,
            rowOf,
            count,
            allocator,
            runMirrors(c),
            offsetScratch
          )
          c += 1
        }
        val ex = merge.exhausted()
        if (ex >= 0) {
          val run = runs(ex).asInstanceOf[SpilledRun]
          if (run.load()) {
            merge.refill(ex, run.keys, null, run.rows)
            c = 0
            while (c < numColumns) { runColumns(c)(ex) = run.columns(c); c += 1 }
          } else merge.finish(ex)
        }
      }
    }
    emitted = from + count
    metrics.numOutputBatches += 1
    metrics.numOutputRows += count
    current = new ColumnarBatch(out, count)
    current
  }

  private def releaseCurrent(): Unit = {
    if (current != null) { current.close(); current = null }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      releaseCurrent()
      columns = null
      permutation = null
      merge = null
      runColumns = null
      runs.foreach(_.close())
      runs.clear()
      releaseTo(0L)
      releaseHeapTo(0L)
      allocator.close()
    }
  }
}
