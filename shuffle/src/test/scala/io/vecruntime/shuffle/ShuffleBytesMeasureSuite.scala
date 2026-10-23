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
package io.vecruntime.shuffle

import java.lang.foreign.Arena
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

import scala.collection.mutable

import io.vecruntime.kernels.ArrowLayout
import io.vecruntime.spark.arrow.ArrowOutput
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.compression.CompressionUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite

/**
 * #932 step 3: where the columnar shuffle's bytes go, on a store_sales-like slice partitioned by
 * `ss_store_sk`. The 1 TB run wrote 118.6 GB of Arrow IPC (zstd 3) for 99.4 GB of zstd Parquet
 * output, in 1279 blocks of ~175 KB per reduce task. This suite reproduces that write on a local
 * slice and measures the three levers #932 names:
 *
 *  - dictionary encoding of low-cardinality strings (bytes with and without);
 *  - the zstd level (1 / 3 / 6: size against compress CPU);
 *  - per-block framing overhead (the record batches per block, and the dictionary section and
 *    our unit headers that ride the stream).
 *
 * It prints a table and asserts the writer is already on the winning side of each lever (the confs
 * exist: `spark.vecruntime.shuffle.writer.dictionaryMaxRatio`, `.zstdLevel`, and the file-dictionary
 * format), so a regression that turned one off would fail here. It does not open a Spark session:
 * the writer is driven directly for exact control of the configuration under test.
 */
class ShuffleBytesMeasureSuite extends AnyFunSuite {

  /**
   * The numeric core of TPC-DS store_sales (surrogate keys as INT, measures as DECIMAL(7,2)) plus
   * the two low-cardinality string columns a partitioned write often carries alongside it (a status
   * code and a channel label), so the dictionary lever has something to act on. store_sales itself
   * has no string column, which is the point the pure-numeric sub-measurement makes.
   */
  private val numericCore: Seq[StructField] = Seq(
    StructField("ss_sold_date_sk", IntegerType),
    StructField("ss_sold_time_sk", IntegerType),
    StructField("ss_item_sk", IntegerType),
    StructField("ss_customer_sk", IntegerType),
    StructField("ss_cdemo_sk", IntegerType),
    StructField("ss_hdemo_sk", IntegerType),
    StructField("ss_addr_sk", IntegerType),
    StructField("ss_store_sk", IntegerType),
    StructField("ss_promo_sk", IntegerType),
    StructField("ss_ticket_number", LongType),
    StructField("ss_quantity", IntegerType),
    StructField("ss_wholesale_cost", DecimalType(7, 2)),
    StructField("ss_list_price", DecimalType(7, 2)),
    StructField("ss_sales_price", DecimalType(7, 2)),
    StructField("ss_ext_sales_price", DecimalType(7, 2)),
    StructField("ss_ext_wholesale_cost", DecimalType(7, 2)),
    StructField("ss_net_profit", DecimalType(7, 2))
  )
  private val stringCols: Seq[StructField] =
    Seq(StructField("ss_status", StringType), StructField("ss_channel", StringType))

  private val numStores = 100 // the partition key's cardinality (1 TB store_sales has ~1000)
  private val statuses = Array("ORDERED", "SHIPPED", "RETURNED", "CANCELLED")
  private val channels = Array("store", "web", "catalog")

  /** One record batch of `n` rows of the slice; a deterministic pseudo-random fill with store_sales-like ranges. */
  private def batch(
      arena: Arena,
      allocator: BufferAllocator,
      schema: StructType,
      n: Int,
      seed: Long
  ): ColumnarBatch = {
    val rnd = new java.util.Random(seed)
    val cols = mutable.ArrayBuffer.empty[ColumnVector]
    val all = arena.allocate(io.vecruntime.kernels.Bitmap.bytesFor(n), 8)
    io.vecruntime.kernels.Bitmap.fill(all, n, true)
    schema.fields.foreach { f =>
      val buffers = f.name match {
        case "ss_store_sk" => ArrowLayout.ofInts(arena, Array.fill(n)(rnd.nextInt(numStores)), Array.fill(n)(false))
        case "ss_ticket_number" =>
          ArrowLayout.ofLongs(arena, Array.fill(n)(rnd.nextInt(2000000).toLong), Array.fill(n)(false))
        case "ss_quantity" => ArrowLayout.ofInts(arena, Array.fill(n)(1 + rnd.nextInt(100)), Array.fill(n)(false))
        case "ss_status" => ArrowLayout.ofStrings(arena, Array.fill(n)(statuses(rnd.nextInt(statuses.length))))
        case "ss_channel" => ArrowLayout.ofStrings(arena, Array.fill(n)(channels(rnd.nextInt(channels.length))))
        case _ =>
          f.dataType match {
            case IntegerType =>
              // surrogate keys: a few million distinct, ~2% null (store_sales has nullable dims)
              ArrowLayout.ofInts(
                arena,
                Array.fill(n)(rnd.nextInt(3000000)),
                Array.tabulate(n)(_ => rnd.nextInt(50) == 0)
              )
            case _: DecimalType =>
              ArrowLayout.ofLongs(arena, Array.fill(n)(rnd.nextInt(1000000).toLong), Array.fill(n)(false))
            case other => throw new IllegalArgumentException(other.toString)
          }
      }
      cols += ArrowOutput.compact(f.name, f.dataType, buffers, all, n, allocator)
    }
    new ColumnarBatch(cols.toArray, n)
  }

  /** Writes `rows` rows of `schema`, repartitioned by ss_store_sk, with the given config; returns (file, compressNanos). */
  private def write(
      allocator: BufferAllocator,
      dir: Path,
      schema: StructType,
      rows: Int,
      dictionaryMaxRatio: Double,
      zstdLevel: Int,
      compression: Option[CompressionUtil.CodecType] = Some(CompressionUtil.CodecType.ZSTD),
      fileDictionary: Boolean = true
  ): (Path, Long) = {
    val tag =
      s"${dictionaryMaxRatio}-$zstdLevel-${compression.map(_.toString).getOrElse("none")}-fd$fileDictionary-${schema.fields.length}"
    val path = dir.resolve(s"ss-$tag.ipc")
    val storeOrdinal = schema.fieldIndex("ss_store_sk")
    val writer = new PartitionedIpcWriter(
      schema,
      numStores,
      allocator,
      path,
      flushBytes = 1L << 20,
      compression = compression,
      batchRows = 8192,
      batchBytes = 1L << 20,
      bufferBytes = 64L << 20,
      dictionaryMaxRatio = dictionaryMaxRatio,
      dictionaryCapBytes = PartitionedIpcWriter.DictionaryCapBytes,
      fileDictionary = fileDictionary,
      zstdLevel = zstdLevel
    )
    val arena = Arena.ofConfined()
    var compressNanos = 0L
    try {
      var written = 0
      var seed = 1L
      while (written < rows) {
        val n = math.min(8192, rows - written)
        val b = batch(arena, allocator, schema, n, seed)
        val ids = new Array[Int](n)
        (0 until n).foreach(r => ids(r) = b.column(storeOrdinal).getInt(r))
        val start = System.nanoTime()
        try writer.write(b, ids)
        finally { compressNanos += System.nanoTime() - start; b.close() }
        written += n
        seed += 1
      }
      writer.finish()
    } finally { arena.close(); writer.close() }
    (path, compressNanos)
  }

  /** Totals over a written file's partition streams: bytes, record batches, dictionary batches, unit-header bytes, dict section. */
  private final case class Framing(
      fileBytes: Long,
      partitionBytes: Long,
      dictSectionBytes: Long,
      recordBatches: Int,
      dictionaryBatches: Int,
      unitHeaderBytes: Long,
      nonEmptyPartitions: Int
  )

  /** Decompresses and walks a file's partition streams, counting messages and our unit headers (#416). */
  private def framing(path: Path, compression: Option[CompressionUtil.CodecType]): Framing = {
    val ch = FileChannel.open(path, StandardOpenOption.READ)
    try {
      val index = PartitionedIpcFile.readIndex(ch)
      val size = ch.size()
      val tail = ByteBuffer.allocate(12)
      ch.read(tail, size - 12)
      tail.flip()
      val (_, dLen) = PartitionedIpcFile.readDictSpan(ch, size - 12 - tail.getInt)
      var records = 0
      var dicts = 0
      var unitBytes = 0L
      var nonEmpty = 0
      var p = 0
      while (p < index.numPartitions) {
        val len = index.lengths(p)
        if (len > 0) {
          nonEmpty += 1
          val raw = new Array[Byte](len.toInt)
          val buf = ByteBuffer.wrap(raw)
          while (buf.hasRemaining) {
            val k = ch.read(buf, index.offsets(p) + buf.position())
            if (k < 0) buf.position(buf.limit())
          }
          val decompressed = {
            val in = ShuffleCompression.decompressing(new java.io.ByteArrayInputStream(raw), compression)
            val out = new java.io.ByteArrayOutputStream()
            val tmp = new Array[Byte](1 << 16)
            var k = in.read(tmp)
            while (k >= 0) { out.write(tmp, 0, k); k = in.read(tmp) }
            out.toByteArray
          }
          // Walk our unit headers; for each, read the following IPC message with Arrow's reader to
          // learn its body length and advance exactly past it.
          var pos = 0
          while (pos + 12 <= decompressed.length) {
            val hdr = ByteBuffer.wrap(decompressed, pos, 12) // writeUnitHeader uses big-endian (default)
            val magic = hdr.getInt
            require(magic == PartitionedIpcFile.UnitMagic, f"bad unit magic 0x$magic%08x at $pos")
            val kind = hdr.getInt
            val value = hdr.getInt
            pos += 12
            unitBytes += 12
            if (kind == PartitionedIpcFile.UnitRecordBatch) { pos += value; unitBytes += value; records += 1 }
            else dicts += 1
            // The IPC message prefix: [0xFFFFFFFF continuation][int32 metadataLen], then metadata
            // (padded), then body. readMessage reads only the metadata (no body, no allocation), so
            // nothing leaks; getMessageLength is the prefix+metadata length and bodyLength the body.
            val channel = new org.apache.arrow.vector.ipc.ReadChannel(
              java.nio.channels.Channels
                .newChannel(new java.io.ByteArrayInputStream(decompressed, pos, decompressed.length - pos))
            )
            val meta = org.apache.arrow.vector.ipc.message.MessageSerializer.readMessage(channel)
            channel.close()
            pos += meta.getMessageLength + meta.getMessageBodyLength.toInt
          }
        }
        p += 1
      }
      Framing(size, index.lengths.sum, dLen, records, dicts, unitBytes, nonEmpty)
    } finally ch.close()
  }

  test("#932: shuffle bytes -- dictionary, zstd level and per-block framing on a store_sales slice") {
    val allocator = new RootAllocator()
    val dir = Files.createTempDirectory("ssbytes")
    val rows = 400000 // ~4000 rows per store over 100 stores: a realistic reduce-partition size
    val withStrings = StructType((numericCore ++ stringCols).toArray)
    val numericOnly = StructType(numericCore.toArray)
    val table = mutable.ArrayBuffer.empty[String]
    def mb(bytes: Long): String = f"${bytes.toDouble / (1 << 20)}%.2f"

    try {
      // 1) dictionary on vs off, for the slice with strings.
      val (dictOnPath, _) = write(allocator, dir, withStrings, rows, dictionaryMaxRatio = 0.5, zstdLevel = 3)
      val (dictOffPath, _) = write(allocator, dir, withStrings, rows, dictionaryMaxRatio = 0.0, zstdLevel = 3)
      val dictOn = Files.size(dictOnPath)
      val dictOff = Files.size(dictOffPath)
      val (numPath, _) = write(allocator, dir, numericOnly, rows, dictionaryMaxRatio = 0.5, zstdLevel = 3)
      val numBytes = Files.size(numPath)

      table += "\n=== #932 shuffle-bytes measurement (store_sales slice, %d rows, %d stores) ===".format(
        rows,
        numStores
      )
      table += "-- dictionary encoding of low-cardinality strings (ss_status, ss_channel), zstd 3 --"
      table += f"  with-strings, dictionary ON  : ${mb(dictOn)} MiB"
      table += f"  with-strings, dictionary OFF : ${mb(dictOff)} MiB"
      table += f"  dictionary saves             : ${mb(dictOff - dictOn)} MiB (${100.0 * (dictOff - dictOn) / dictOff}%.1f%%)"
      table += f"  numeric-only (no strings)    : ${mb(numBytes)} MiB  -- the dictionary lever does not apply"

      // 2) zstd level sweep on the with-strings slice: size against compress CPU.
      table += "-- zstd level (size vs write CPU, fastest of 3 runs), dictionary ON, with-strings --"
      table += f"  ${"level"}%-6s ${"MiB"}%-10s ${"write ms"}%-12s ${"vs zstd3 size"}%-14s"
      var z3Bytes = 0L
      Seq(1, 3, 6).foreach { lvl =>
        var best = Long.MaxValue
        var bytes = 0L
        (0 until 3).foreach { _ =>
          val (p, ns) = write(allocator, dir, withStrings, rows, dictionaryMaxRatio = 0.5, zstdLevel = lvl)
          bytes = Files.size(p)
          best = math.min(best, ns)
        }
        if (lvl == 3) z3Bytes = bytes
        val rel = if (z3Bytes == 0) "" else f"${100.0 * bytes / z3Bytes}%.1f%%"
        table += f"  $lvl%-6d ${mb(bytes)}%-10s ${best.toDouble / 1e6}%-12.1f $rel%-14s"
      }

      // 3) per-block framing.
      val fr = framing(dictOnPath, Some(CompressionUtil.CodecType.ZSTD))
      val batchesPerBlock = fr.recordBatches.toDouble / math.max(1, fr.nonEmptyPartitions)
      table += "-- per-block framing (dictionary ON, zstd 3, with-strings) --"
      table += f"  non-empty partitions (blocks): ${fr.nonEmptyPartitions}"
      table += f"  record batches total         : ${fr.recordBatches} (${batchesPerBlock}%.2f per block)"
      table += f"  dictionary batches total     : ${fr.dictionaryBatches} (one section per file, not per block)"
      table += f"  our unit-header bytes        : ${fr.unitHeaderBytes} (${fr.unitHeaderBytes.toDouble / math.max(1, fr.recordBatches)}%.1f per record batch, uncompressed)"
      table += f"  file-dictionary section      : ${mb(fr.dictSectionBytes)} MiB (${100.0 * fr.dictSectionBytes / fr.fileBytes}%.2f%% of file)"

      // 4) file-dictionary vs per-block dictionary.
      val (perBlockPath, _) =
        write(allocator, dir, withStrings, rows, dictionaryMaxRatio = 0.5, zstdLevel = 3, fileDictionary = false)
      val perBlock = Files.size(perBlockPath)
      table += "-- one dictionary per file vs per block (#416) --"
      table += f"  file-dictionary (Flight)     : ${mb(dictOn)} MiB"
      table += f"  per-block dictionary (block) : ${mb(perBlock)} MiB"
      table += f"  file-dictionary saves        : ${mb(perBlock - dictOn)} MiB (${100.0 * (perBlock - dictOn) / math.max(perBlock, 1)}%.1f%%)"

      // 5) raw as the ceiling.
      val (rawPath, _) =
        write(allocator, dir, withStrings, rows, dictionaryMaxRatio = 0.5, zstdLevel = 3, compression = None)
      val raw = Files.size(rawPath)
      table += "-- compression headroom --"
      table += f"  uncompressed (none)          : ${mb(raw)} MiB"
      table += f"  zstd 3                       : ${mb(dictOn)} MiB (${100.0 * dictOn / raw}%.1f%% of raw)"

      info(table.mkString("\n"))

      // Guardrails against a regression that flips a default off.
      assert(dictOn < dictOff, s"dictionary must not make the string slice larger (${mb(dictOn)} vs ${mb(dictOff)})")
      assert(
        dictOn <= perBlock,
        s"one dictionary per file must not be larger than per block (${mb(dictOn)} vs ${mb(perBlock)})"
      )
      assert(dictOn < raw, "zstd 3 must be smaller than uncompressed")
      assert(
        fr.dictionaryBatches <= stringCols.size,
        "the file-dictionary format writes one dictionary per string column, not per block"
      )
      assert(batchesPerBlock >= 1.0, "every non-empty block has at least one record batch")
    } finally allocator.close()
  }
}
