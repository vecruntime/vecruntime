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
package io.vecruntime.iceberg.write;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * The two places where the Iceberg runtime and our writer meet across
 * parquet-java's relocation (#842). {@code iceberg-spark-runtime} shades
 * parquet-java to {@code org.apache.iceberg.shaded.org.apache.parquet}, while
 * {@code ColumnarParquetWriter} is built on the parquet-java Spark ships. The
 * two are converted through their stable, serialized forms -- the schema's text
 * and the footer's Thrift bytes -- so the field ids, logical types and
 * statistics are Iceberg's own on both sides and nothing about them is
 * re-derived here.
 */
final class IcebergParquetInterop {

    private IcebergParquetInterop() {}

    private static final org.apache.parquet.format.converter.ParquetMetadataConverter TO_THRIFT = new org.apache.parquet.format.converter.ParquetMetadataConverter();

    /**
     * Built once: the read options' default Thrift message-size limit is the
     * one a file reader uses (the no-argument converter has none, -1, and
     * refuses to read), and building them creates a Hadoop {@code
     * Configuration}, which parses the default XML resources -- milliseconds a
     * file must not pay.
     */
    private static final org.apache.iceberg.shaded.org.apache.parquet.format.converter.ParquetMetadataConverter FROM_THRIFT = new org.apache.iceberg.shaded.org.apache.parquet.format.converter.ParquetMetadataConverter(org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions.builder()
            .build());

    /**
     * The Parquet schema Iceberg writes for {@code schema}: Iceberg's own conversion
     * ({@code ParquetSchemaUtil.convert}), field ids included, re-parsed into
     * parquet-java's unshaded {@link MessageType}.
     */
    static MessageType fileSchema(Schema schema, String name) {
        org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType shaded = ParquetSchemaUtil.convert(schema, name);
        MessageType unshaded = MessageTypeParser.parseMessageType(shaded.toString());
        if (!unshaded.toString().equals(shaded.toString())) {
            // The text form is the contract between the two copies; a difference means one of
            // them prints or parses an annotation the other does not know.
            throw new IllegalStateException("Parquet schema did not survive conversion: " + shaded + " vs " + unshaded);
        }
        return unshaded;
    }

    /**
     * {@code footer} as the Iceberg runtime's (shaded) {@code ParquetMetadata},
     * by writing it to its Thrift form and reading that back, as both copies do
     * for a file's footer.
     */
    static org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.ParquetMetadata shadedFooter(ParquetMetadata footer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        org.apache.parquet.format.Util.writeFileMetaData(TO_THRIFT.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer), bytes);
        return FROM_THRIFT.readParquetMetadata(new ByteArrayInputStream(bytes.toByteArray()), org.apache.iceberg.shaded.org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER);
    }

    /**
     * An Iceberg {@code OutputFile} as the {@code OutputFile} parquet-java
     * writes to.
     */
    static org.apache.parquet.io.OutputFile parquetOutput(org.apache.iceberg.io.OutputFile file) {
        return new org.apache.parquet.io.OutputFile() {
            @Override
            public org.apache.parquet.io.PositionOutputStream create(long blockSizeHint) {
                return wrap(file.create());
            }

            @Override
            public org.apache.parquet.io.PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return wrap(file.createOrOverwrite());
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return file.location();
            }
        };
    }

    private static org.apache.parquet.io.PositionOutputStream wrap(org.apache.iceberg.io.PositionOutputStream out) {
        return new org.apache.parquet.io.PositionOutputStream() {
            @Override
            public long getPos() throws IOException {
                return out.getPos();
            }

            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }
}
