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
package io.vecruntime.iceberg.write

import java.lang.foreign.Arena
import java.nio.file.{Files, Path}
import java.util.{List => JList, Map => JMap}

import scala.jdk.CollectionConverters._

import io.vecruntime.kernels.{ArrowLayout, VectorBuffers}
import io.vecruntime.parquet.write.ColumnarParquetWriter
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{MetricsConfig, Schema}
import org.apache.iceberg.types.Types
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.{CodecFactory, ParquetFileReader}
import org.apache.parquet.hadoop.metadata.{CompressionCodecName, ParquetMetadata}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.scalatest.funsuite.AnyFunSuite

/**
 * [[RowGroupCopyPlanner]]: the per-source-file decision of a copy-on-write DELETE rewrite (#901 M4a). The source
 * file is a real multi-row-group Parquet file written by [[ColumnarParquetWriter]], so the row-group ranges are
 * the real ones; the plan is checked against deleted positions placed in chosen groups, and the schema and NaN
 * guards against evolved and NaN-tracking tables.
 */
class RowGroupCopyPlannerSuite extends AnyFunSuite {

  private val conf = new Configuration()

  private val WriteSchema: MessageType = MessageTypeParser.parseMessageType(
    "message table { optional int64 id = 1; optional binary s (STRING) = 2; }"
  )
  private val IceSchema = new Schema(
    Types.NestedField.optional(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "s", Types.StringType.get())
  )

  private def dir: Path = Files.createTempDirectory("m4aplan")

  /** A source file of `groups` row groups of `perGroup` rows each. */
  private def writeSource(groups: Int, perGroup: Int, schema: MessageType = WriteSchema): org.apache.hadoop.fs.Path = {
    val p = new org.apache.hadoop.fs.Path(dir.resolve("src.parquet").toString)
    val codecs = new CodecFactory(conf, 4096)
    val props = ParquetProperties.builder().withPageSize(4096).withDictionaryPageSize(4096).build()
    val w = new ColumnarParquetWriter(
      HadoopOutputFile.fromPath(p, conf),
      schema,
      props,
      codecs.getCompressor(CompressionCodecName.SNAPPY),
      1L << 20,
      JMap.of()
    )
    try {
      for (g <- 0 until groups) {
        val id = Array.tabulate(perGroup)(k => (g.toLong * perGroup) + k)
        val s = id.map(v => "v" + v)
        val a = Arena.ofConfined()
        try {
          val cols: JList[VectorBuffers] =
            JList.of(ArrowLayout.ofLongs(a, id, new Array[Boolean](perGroup)), ArrowLayout.ofStrings(a, s))
          w.write(cols, 0, perGroup)
        } finally a.close()
        w.endRowGroup()
      }
    } finally {
      w.close()
      codecs.release()
    }
    p
  }

  private def footer(p: org.apache.hadoop.fs.Path): ParquetMetadata = {
    val r = ParquetFileReader.open(HadoopInputFile.fromPath(p, conf))
    try r.getFooter
    finally r.close()
  }

  private def plan(f: ParquetMetadata, deleted: Array[Long]): RowGroupCopyPlanner =
    RowGroupCopyPlanner.plan(f, WriteSchema, deleted, IceSchema, MetricsConfig.getDefault())

  test("no deletes copies every group") {
    val p = plan(footer(writeSource(4, 500)), Array.emptyLongArray)
    assert(p.copyable())
    assert(p.copiedGroups() == 4)
    assert(p.groups().asScala.forall(_.action() == RowGroupCopyPlanner.Action.COPY))
  }

  test("deletes in one group re-encode only that group") {
    val p = plan(footer(writeSource(4, 500)), Array(1001L, 1400L)) // both in group 2 ([1000,1500))
    assert(p.copyable())
    assert(p.copiedGroups() == 3)
    val g = p.groups()
    assert(g.get(0).action() == RowGroupCopyPlanner.Action.COPY)
    assert(g.get(1).action() == RowGroupCopyPlanner.Action.COPY)
    assert(g.get(2).action() == RowGroupCopyPlanner.Action.REENCODE)
    assert(g.get(3).action() == RowGroupCopyPlanner.Action.COPY)
  }

  test("every row of a group deleted skips it") {
    val deleted = (100L until 200L).toArray ++ Array(250L) // all of group 1, one of group 2
    val p = plan(footer(writeSource(3, 100)), deleted)
    assert(p.copyable())
    val g = p.groups()
    assert(g.get(0).action() == RowGroupCopyPlanner.Action.COPY)
    assert(g.get(1).action() == RowGroupCopyPlanner.Action.SKIP)
    assert(g.get(2).action() == RowGroupCopyPlanner.Action.REENCODE)
    assert(p.copiedGroups() == 1)
  }

  test("group ranges match the footer") {
    val p = plan(footer(writeSource(5, 300)), Array.emptyLongArray)
    var expected = 0L
    p.groups().asScala.foreach { g =>
      assert(g.start() == expected, s"group ${g.ordinal()} start")
      assert(g.rowCount() == 300)
      expected += g.rowCount()
    }
    assert(expected == 1500L)
  }

  test("schema-evolved file falls back") {
    val evolved = MessageTypeParser.parseMessageType(
      "message table { optional int64 id = 1; optional binary s (STRING) = 2; optional int32 extra = 3; }"
    )
    val p = new org.apache.hadoop.fs.Path(dir.resolve("evolved.parquet").toString)
    val codecs = new CodecFactory(conf, 4096)
    val props = ParquetProperties.builder().withPageSize(4096).build()
    val w = new ColumnarParquetWriter(
      HadoopOutputFile.fromPath(p, conf),
      evolved,
      props,
      codecs.getCompressor(CompressionCodecName.SNAPPY),
      1L << 20,
      JMap.of()
    )
    try {
      val a = Arena.ofConfined()
      try w.write(
          JList.of(
            ArrowLayout.ofLongs(a, Array(1L), new Array[Boolean](1)),
            ArrowLayout.ofStrings(a, Array("x")),
            ArrowLayout.ofInts(a, Array(7), new Array[Boolean](1))
          ),
          0,
          1
        )
      finally a.close()
    } finally { w.close(); codecs.release() }
    val planner = plan(footer(p), Array.emptyLongArray)
    assert(!planner.copyable())
    assert(planner.fallbackReason().contains("schema"))
  }

  test("tables tracking NaN fall back when any group is copied, but not when metrics are off") {
    val floatWrite = MessageTypeParser.parseMessageType(
      "message table { optional int64 id = 1; optional double d = 2; }"
    )
    val floatIce = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "d", Types.DoubleType.get())
    )
    val p = new org.apache.hadoop.fs.Path(dir.resolve("floats.parquet").toString)
    val codecs = new CodecFactory(conf, 4096)
    val props = ParquetProperties.builder().withPageSize(4096).build()
    val w = new ColumnarParquetWriter(
      HadoopOutputFile.fromPath(p, conf),
      floatWrite,
      props,
      codecs.getCompressor(CompressionCodecName.SNAPPY),
      1L << 20,
      JMap.of()
    )
    try {
      for (g <- 0 until 3) {
        val id = Array.tabulate(200)(k => (g.toLong * 200) + k)
        val d = id.map(_ * 0.5)
        val a = Arena.ofConfined()
        try w.write(
            JList.of(
              ArrowLayout.ofLongs(a, id, new Array[Boolean](200)),
              ArrowLayout.ofDoubles(a, d, new Array[Boolean](200))
            ),
            0,
            200
          )
        finally a.close()
        w.endRowGroup()
      }
    } finally { w.close(); codecs.release() }

    val tracked =
      RowGroupCopyPlanner.plan(footer(p), floatWrite, Array(5L), floatIce, MetricsConfig.getDefault())
    assert(!tracked.copyable(), "default metrics track NaN for a double column")
    assert(tracked.fallbackReason().contains("NaN"))

    val off = MetricsConfig.fromProperties(JMap.of("write.metadata.metrics.column.d", "none"))
    val untracked = RowGroupCopyPlanner.plan(footer(p), floatWrite, Array(5L), floatIce, off)
    assert(untracked.copyable(), "a double column with metrics off needs no NaN count")
  }
}
