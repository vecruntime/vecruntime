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

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

class ScanIoStatsSuite extends AnyFunSuite {

  test("off by default; a line counts the bytes read on every thread") {
    assert(!ScanIoStats.enabled)
    val dir = Files.createTempDirectory("scan-io-stats-")
    try {
      val f = dir.resolve("a.bin")
      Files.write(f, Array.fill[Byte](4096)(7))
      val fs = new Path(f.toUri).getFileSystem(new Configuration())
      def bytesRead(l: String): Long = "fsBytesRead=(\\d+)".r.findFirstMatchIn(l).get.group(1).toLong
      val before = bytesRead(ScanIoStats.line(fs))
      // Read on another thread: the per-thread count of THIS thread would not see it, the line does.
      val t = new Thread(() => {
        val in = fs.open(new Path(f.toUri))
        try { val buf = new Array[Byte](4096); var n = 0; while (n >= 0) n = in.read(buf) }
        finally in.close()
      })
      t.start(); t.join()
      val l = ScanIoStats.line(fs)
      assert(l.startsWith("#638: io "), l)
      assert(bytesRead(l) - before >= 4096, l)
      ScanIoStats.register(fs) // off: registers nothing, starts no thread
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

  test("#638: a Parquet input stream's own statistics count the bytes read on any thread") {
    val dir = Files.createTempDirectory("scan-io-stats-")
    try {
      val f = dir.resolve("b.bin")
      Files.write(f, Array.fill[Byte](8192)(3))
      val in = org.apache.parquet.hadoop.util.HadoopInputFile
        .fromPath(new Path(f.toUri), new Configuration())
        .newStream()
      try {
        val stats = VectorParquetScanExec.streamStatistics(in)
        assert(stats.isDefined, "the local file system's stream keeps IOStatistics")
        val all = java.util.Collections.singletonList(stats.get)
        val before = VectorParquetScanExec.streamReadBytes(all)
        // Read on another thread: neither the task thread's nor a prefetch thread's per-thread count sees it.
        val t = new Thread(() => { val buf = new Array[Byte](8192); in.readFully(buf) })
        t.start(); t.join()
        assert(VectorParquetScanExec.streamReadBytes(all) - before >= 8192)
      } finally in.close()
    } finally {
      Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
    }
  }

  test("#638: a stream that keeps no statistics counts nothing") {
    val in = new org.apache.parquet.io.SeekableInputStream {
      override def getPos: Long = 0L
      override def seek(newPos: Long): Unit = ()
      override def readFully(bytes: Array[Byte]): Unit = ()
      override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit = ()
      override def read(buf: java.nio.ByteBuffer): Int = -1
      override def readFully(buf: java.nio.ByteBuffer): Unit = ()
      override def read(): Int = -1
    }
    assert(VectorParquetScanExec.streamStatistics(in).isEmpty)
    assert(VectorParquetScanExec.streamReadBytes(java.util.Collections.emptyList()) == 0L)
  }
}
