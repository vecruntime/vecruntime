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
import java.lang.reflect.Method;
import java.util.stream.Stream;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.NameMapping;
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
 *
 * <p>#shading: which relocation Iceberg's own classes carry is NOT fixed. On the
 * shaded {@code iceberg-spark-runtime} jar (production, and our own iceberg-bridge
 * suites) {@code ParquetSchemaUtil.convert} / {@code ParquetUtil.footerMetrics}
 * bind to {@code org.apache.iceberg.shaded.org.apache.parquet.*}; on Iceberg's own
 * Gradle test classpath (the upstream-suite harness) they bind to the UNSHADED
 * {@code org.apache.parquet.*}. If this class named either relocation in its
 * bytecode it would {@code ClassCastException} under the other (an unshaded
 * {@code MessageType} assigned to a shaded-typed local, or a shaded footer handed
 * to an unshaded {@code footerMetrics}). So every call that touches Iceberg's
 * parquet flavour is made REFLECTIVELY and the footer is re-materialised into
 * whichever flavour the loaded {@code ParquetUtil} expects -- the same bytecode
 * then works under both layouts. Our OWN side ({@code ParquetFileWriter},
 * {@code MessageTypeParser}, the Thrift {@code ParquetMetadataConverter}) is
 * always unshaded and is named directly.
 */
final class IcebergParquetInterop {

    private IcebergParquetInterop() {}

    /**
     * Our (unshaded) Thrift converter: turns our writer's footer into the
     * portable Thrift bytes.
     */
    private static final org.apache.parquet.format.converter.ParquetMetadataConverter TO_THRIFT = new org.apache.parquet.format.converter.ParquetMetadataConverter();

    // ---- reflected Iceberg-flavour parquet handles, resolved once against the classpath ---------
    private static final ClassLoader CL = loader();

    /**
     * The package Iceberg's own parquet classes live under: shaded on the
     * runtime jar, unshaded on Iceberg's test classpath. Derived from the
     * actual return type of ParquetSchemaUtil.convert.
     */
    private static final String ICE_PARQUET_PKG = detectIcebergParquetPackage();

    private static ClassLoader loader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return cl != null ? cl : IcebergParquetInterop.class.getClassLoader();
    }

    private static String detectIcebergParquetPackage() {
        try {
            Class<?> schemaUtil = Class.forName("org.apache.iceberg.parquet.ParquetSchemaUtil", true, CL);
            for (Method m : schemaUtil.getMethods()) {
                if (m.getName().equals("convert") && m.getParameterCount() == 2 && m.getParameterTypes()[0] == Schema.class) {
                    String ret = m.getReturnType().getName(); // ...parquet.schema.MessageType
                    int i = ret.indexOf(".schema.MessageType");
                    if (i > 0) {
                        return ret.substring(0, i); // ...parquet
                    }
                }
            }
        } catch (ReflectiveOperationException e) {
            // fall through to the shaded default (production layout)
        }
        return "org.apache.iceberg.shaded.org.apache.parquet";
    }

    /**
     * The Parquet schema Iceberg writes for {@code schema}: Iceberg's own
     * conversion ({@code ParquetSchemaUtil.convert}, field ids included),
     * re-parsed into parquet-java's unshaded {@link MessageType}. The convert
     * result is captured as {@code Object} and read through its text form, so
     * this never names Iceberg's parquet relocation.
     */
    static MessageType fileSchema(Schema schema, String name) {
        try {
            Class<?> schemaUtil = Class.forName("org.apache.iceberg.parquet.ParquetSchemaUtil", true, CL);
            Method convert = schemaUtil.getMethod("convert", Schema.class, String.class);
            Object iceMsgType = convert.invoke(null, schema, name); // Iceberg-flavour MessageType
            String text = String.valueOf(iceMsgType);
            MessageType unshaded = MessageTypeParser.parseMessageType(text);
            if (!unshaded.toString().equals(text)) {
                // The text form is the contract between the two copies; a difference means one of
                // them prints or parses an annotation the other does not know.
                throw new IllegalStateException("Parquet schema did not survive conversion: " + text + " vs " + unshaded);
            }
            return unshaded;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("ParquetSchemaUtil.convert unavailable", e);
        }
    }

    /**
     * Iceberg's {@code Metrics} for a just-written file, computed by {@code
     * ParquetUtil.footerMetrics} over our writer's {@code footer}. The footer
     * (unshaded, parquet-java's) is written to its Thrift form and re-read with
     * Iceberg's OWN parquet converter -- shaded or unshaded, whichever the
     * loaded {@code ParquetUtil} expects -- so the {@code ParquetMetadata}
     * handed to {@code footerMetrics} is of its own flavour. Both the converter
     * and the {@code footerMetrics} call are made reflectively, so no
     * relocation is pinned in this class's bytecode.
     */
    static Metrics footerMetrics(ParquetMetadata footer, Stream<?> fieldMetrics, Object metricsConfig,
            NameMapping nameMapping)
            throws IOException {
        // 1. our footer -> portable Thrift bytes
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        org.apache.parquet.format.Util.writeFileMetaData(TO_THRIFT.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer), bytes);
        try {
            // 2. Thrift bytes -> Iceberg-flavour ParquetMetadata, via Iceberg's own converter class.
            Class<?> convCls = Class.forName(ICE_PARQUET_PKG + ".format.converter.ParquetMetadataConverter", true, CL);
            Class<?> readOptsCls = Class.forName(ICE_PARQUET_PKG + ".ParquetReadOptions", true, CL);
            Object readOpts = readOptsCls.getMethod("builder").invoke(null);
            readOpts = readOpts.getClass()
                               .getMethod("build")
                               .invoke(readOpts);
            Object converter = convCls.getConstructor(readOptsCls).newInstance(readOpts);
            Object noFilter = convCls.getField("NO_FILTER").get(null);
            Class<?> filterCls = noFilter.getClass();
            // readParquetMetadata(InputStream, Filter) -- Filter is a nested type of the converter.
            Method readMeta = null;
            for (Method m : convCls.getMethods()) {
                if (m.getName().equals("readParquetMetadata") && m.getParameterCount() == 2 && m.getParameterTypes()[0] == java.io.InputStream.class) {
                    readMeta = m;
                    break;
                }
            }
            if (readMeta == null) {
                throw new IllegalStateException("ParquetMetadataConverter.readParquetMetadata(InputStream, Filter) not found");
            }
            Object iceFooter = readMeta.invoke(converter, new ByteArrayInputStream(bytes.toByteArray()), filterCls.cast(noFilter));

            // 3. ParquetUtil.footerMetrics(ParquetMetadata, Stream, MetricsConfig, NameMapping)
            Class<?> parquetUtil = Class.forName("org.apache.iceberg.parquet.ParquetUtil", true, CL);
            Method footerMetrics = null;
            for (Method m : parquetUtil.getMethods()) {
                if (m.getName().equals("footerMetrics") && m.getParameterCount() == 4) {
                    footerMetrics = m;
                    break;
                }
            }
            if (footerMetrics == null) {
                throw new IllegalStateException("ParquetUtil.footerMetrics(ParquetMetadata, Stream, MetricsConfig, NameMapping) not found");
            }
            return (Metrics) footerMetrics.invoke(null, iceFooter, fieldMetrics, metricsConfig, nameMapping);
        } catch (ReflectiveOperationException e) {
            Throwable c = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
            if (c instanceof RuntimeException re) {
                throw re;
            }
            if (c instanceof Error err) {
                throw err;
            }
            throw new IllegalStateException("ParquetUtil.footerMetrics failed", c);
        }
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

    /**
     * An Iceberg {@code InputFile} as the {@code InputFile} parquet-java reads
     * from (#957 M4c). The distributed bin-pack copy opens each source file on
     * the executor through the table's {@code FileIO}, not Hadoop, so the copy
     * runs wherever Spark places the task and uses the table's configured IO.
     */
    static org.apache.parquet.io.InputFile parquetInput(org.apache.iceberg.io.InputFile file) {
        return new org.apache.parquet.io.InputFile() {
            @Override
            public long getLength() throws IOException {
                return file.getLength();
            }

            @Override
            public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
                return wrapIn(file.newStream());
            }
        };
    }

    private static org.apache.parquet.io.SeekableInputStream wrapIn(org.apache.iceberg.io.SeekableInputStream in) {
        return new org.apache.parquet.io.DelegatingSeekableInputStream(in) {
            @Override
            public long getPos() throws IOException {
                return in.getPos();
            }

            @Override
            public void seek(long newPos) throws IOException {
                in.seek(newPos);
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
