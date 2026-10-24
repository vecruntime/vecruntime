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
 * [[BinPackCopyPlanner#footerEligible]]: the footer-only half of the per-file copy decision of a bin-pack
 * rewrite (#957 M4c). A real multi-row-group Parquet file is written with [[ColumnarParquetWriter]], then its
 * footer is checked against the table write schema, codec and metrics config. The task-level guards (deletes,
 * spec) are covered end-to-end in [[VectorBinPackRewriteSuite]] because they need a real scan task.
 */
class BinPackCopyPlannerSuite extends AnyFunSuite {

  private val conf = new Configuration()

  private val WriteSchema: MessageType = MessageTypeParser.parseMessageType(
    "message table { optional int64 id = 1; optional binary s (STRING) = 2; }"
  )
  private val IceSchema = new Schema(
    Types.NestedField.optional(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "s", Types.StringType.get())
  )
  // A schema with a double column, to exercise the NaN-tracking guard.
  private val FloatWriteSchema: MessageType = MessageTypeParser.parseMessageType(
    "message table { optional int64 id = 1; optional double f = 2; }"
  )
  private val FloatIceSchema = new Schema(
    Types.NestedField.optional(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "f", Types.DoubleType.get())
  )

  private def dir: Path = Files.createTempDirectory("m4cplan")

  /** A source file of `groups` row groups of `perGroup` rows each, in the given codec. */
  private def writeSource(
      groups: Int,
      perGroup: Int,
      codec: CompressionCodecName = CompressionCodecName.SNAPPY,
      floatCol: Boolean = false
  ): org.apache.hadoop.fs.Path = {
    val schema = if (floatCol) FloatWriteSchema else WriteSchema
    val p = new org.apache.hadoop.fs.Path(dir.resolve("src.parquet").toString)
    val codecs = new CodecFactory(conf, 4096)
    val props = ParquetProperties.builder().withPageSize(4096).withDictionaryPageSize(4096).build()
    val w = new ColumnarParquetWriter(
      HadoopOutputFile.fromPath(p, conf),
      schema,
      props,
      codecs.getCompressor(codec),
      1L << 20,
      JMap.of()
    )
    try {
      for (g <- 0 until groups) {
        val id = Array.tabulate(perGroup)(k => (g.toLong * perGroup) + k)
        val a = Arena.ofConfined()
        try {
          val cols: JList[VectorBuffers] =
            if (floatCol) {
              val fv = id.map(v => v.toDouble / 3)
              JList.of(
                ArrowLayout.ofLongs(a, id, new Array[Boolean](perGroup)),
                ArrowLayout.ofDoubles(a, fv, new Array[Boolean](perGroup))
              )
            } else {
              val s = id.map(v => "v" + v)
              JList.of(ArrowLayout.ofLongs(a, id, new Array[Boolean](perGroup)), ArrowLayout.ofStrings(a, s))
            }
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

  private def props(codec: String): JMap[String, String] =
    JMap.of("write.parquet.compression-codec", codec)

  test("a clean file matching the write schema and codec is copyable") {
    val f = footer(writeSource(4, 500))
    val plan = BinPackCopyPlanner.footerEligible(f, WriteSchema, IceSchema, MetricsConfig.getDefault(), props("snappy"))
    assert(plan.copyable(), "expected copyable but declined: " + String.valueOf(plan.fallbackReason()))
  }

  test("schema evolution (extra column in the write schema) declines copy") {
    val evolved = MessageTypeParser.parseMessageType(
      "message table { optional int64 id = 1; optional binary s (STRING) = 2; optional int32 extra = 3; }"
    )
    val f = footer(writeSource(3, 300))
    val plan = BinPackCopyPlanner.footerEligible(f, evolved, IceSchema, MetricsConfig.getDefault(), props("snappy"))
    assert(!plan.copyable())
    assert(plan.fallbackReason().contains("schema"))
  }

  test("a codec different from the table codec declines copy") {
    val f = footer(writeSource(3, 300, codec = CompressionCodecName.SNAPPY))
    val plan = BinPackCopyPlanner.footerEligible(f, WriteSchema, IceSchema, MetricsConfig.getDefault(), props("zstd"))
    assert(!plan.copyable())
    assert(plan.fallbackReason().contains("codec"))
  }

  test("a table tracking a float NaN count declines copy") {
    val f = footer(writeSource(3, 300, floatCol = true))
    // Default metrics config tracks (truncate) every column, so the float column's NaN count is reported.
    val plan = BinPackCopyPlanner.footerEligible(
      f,
      FloatWriteSchema,
      FloatIceSchema,
      MetricsConfig.getDefault(),
      props("snappy")
    )
    assert(!plan.copyable())
    assert(plan.fallbackReason().contains("NaN"))
  }

  test("a float column whose metrics are turned off is copyable") {
    val f = footer(writeSource(3, 300, floatCol = true))
    val mc = MetricsConfig.fromProperties(JMap.of("write.metadata.metrics.column.f", "none"))
    val plan = BinPackCopyPlanner.footerEligible(f, FloatWriteSchema, FloatIceSchema, mc, props("snappy"))
    assert(plan.copyable(), "expected copyable but declined: " + String.valueOf(plan.fallbackReason()))
  }
}
