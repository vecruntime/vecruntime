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
package org.apache.parquet.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.{ColumnDescriptor, ParquetProperties}
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.column.values.factory.ValuesWriterFactory
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
 * Test-only writer of Parquet files whose value encodings parquet-java's writer does not choose on its own
 * (#559): `DELTA_LENGTH_BYTE_ARRAY` for every BINARY column, `BYTE_STREAM_SPLIT` for INT32/INT64/DOUBLE.
 * parquet-java writes `DELTA_BYTE_ARRAY` for v2 strings, and through `ParquetOutputFormat` there is no
 * switch for either encoding, so this goes through `ParquetWriter`'s package-private constructor with a
 * `ValuesWriterFactory` that picks the writer per column -- the reason it lives in this package.
 */
object V2EncodingWriter {

  private final class Factory extends ValuesWriterFactory {
    private var props: ParquetProperties = _
    override def initialize(p: ParquetProperties): Unit = props = p
    override def newValuesWriter(d: ColumnDescriptor): ValuesWriter = {
      val init = props.getInitialSlabSize
      val page = props.getPageSizeThreshold
      val alloc = HeapByteBufferAllocator.getInstance()
      d.getPrimitiveType.getPrimitiveTypeName match {
        case PrimitiveTypeName.BINARY => new DeltaLengthByteArrayValuesWriter(init, page, alloc)
        case PrimitiveTypeName.INT32 =>
          new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(init, page, alloc)
        case PrimitiveTypeName.INT64 =>
          new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(init, page, alloc)
        case PrimitiveTypeName.DOUBLE =>
          new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(init, page, alloc)
        case other => throw new IllegalArgumentException(s"no test writer for $other")
      }
    }
  }

  /** Writes `rows` to `path` with the encodings above, data page `v2` or v1, small pages and row groups. */
  def write(path: String, schema: MessageType, rows: Iterator[Group], v2: Boolean): Unit = {
    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val props = ParquetProperties
      .builder()
      .withWriterVersion(if (v2) ParquetProperties.WriterVersion.PARQUET_2_0
      else ParquetProperties.WriterVersion.PARQUET_1_0)
      .withDictionaryEncoding(false)
      .withPageSize(4 * 1024)
      .withValuesWriterFactory(new Factory)
      .build()
    val writer = new ParquetWriter[Group](
      HadoopOutputFile.fromPath(new Path(path), conf),
      ParquetFileWriter.Mode.OVERWRITE,
      new GroupWriteSupport(),
      CompressionCodecName.SNAPPY,
      64L * 1024,
      false,
      conf,
      0,
      props,
      null
    )
    try rows.foreach(writer.write)
    finally writer.close()
  }
}
