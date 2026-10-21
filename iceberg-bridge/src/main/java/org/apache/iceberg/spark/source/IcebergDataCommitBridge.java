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
package org.apache.iceberg.spark.source;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import io.vecruntime.iceberg.write.IcebergWriteSpec;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Same-package bridge for the columnar Iceberg data-file writer (#842, W2),
 * like {@link IcebergDvCommitBridge} for deletion vectors. It lives in {@code
 * org.apache.iceberg.spark.source} to build Iceberg's package-private {@code
 * SparkWrite.TaskCommit} -- the per-task commit message every {@code
 * SparkWrite} batch write accepts -- and to read what Iceberg resolved for the
 * write ({@code SparkWrite}'s own table, write schema, output spec, file-size
 * target, write properties and operation id).
 *
 * <p>The commit itself stays Iceberg's: the driver hands these messages to the
 * {@code BatchWrite} of the same {@code Write} ({@code write.toBatch()}), which
 * is an append, a dynamic or by-filter overwrite, or a copy-on-write rewrite as
 * Iceberg decided, with its own snapshot properties, isolation checks, WAP
 * handling and metrics. The plugin core reaches this class reflectively.
 */
public final class IcebergDataCommitBridge {

    private IcebergDataCommitBridge() {}

    /**
     * Whether {@code write} is Iceberg's Spark batch write (the only kind the
     * columnar writer takes).
     */
    public static boolean isSparkWrite(Write write) {
        return write instanceof SparkWrite;
    }

    /**
     * Why the columnar writer cannot write {@code write}'s data files, or
     * {@code null} when it can: the file format must be Parquet, the output
     * spec unpartitioned (until W3), the table unencrypted, and every write
     * schema type one the columnar Parquet writer encodes.
     */
    public static String declineReason(Write write) {
        if (!(write instanceof SparkWrite w)) {
            return "the write is not Iceberg's SparkWrite";
        }
        FileFormat format = field(w, "format", FileFormat.class);
        if (format != FileFormat.PARQUET) {
            return "data file format " + format + " is not Parquet";
        }
        Table table = field(w, "table", Table.class);
        int specId = field(w, "outputSpecId", Integer.class);
        if (!table.specs()
                  .get(specId)
                  .isUnpartitioned()) {
            return "partitioned output spec (" + table.specs().get(specId) + ") is not supported yet";
        }
        if (table.properties().keySet().stream()
                .anyMatch(k -> k.startsWith("encryption."))) {
            return "the table is encrypted";
        }
        Schema schema = field(w, "writeSchema", Schema.class);
        String type = unsupportedType(schema.asStruct());
        return type == null ? null : "write schema type " + type + " is not supported";
    }

    private static String unsupportedType(Type type) {
        switch (type.typeId()) {
            case BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, STRING, BINARY, DECIMAL:
                return null;
            case STRUCT:
                for (Types.NestedField f : type.asStructType().fields()) {
                    String t = unsupportedType(f.type());
                    if (t != null) {
                        return t;
                    }
                }
                return null;
            case LIST:
                return floating(type.asListType()
                                    .elementType())
                        ? type.toString() : unsupportedType(type.asListType()
                        .elementType());
            case MAP:
                if (floating(type.asMapType()
                                 .keyType())
                        || floating(type.asMapType()
                                        .valueType())) {
                    return type.toString();
                }
                String k = unsupportedType(type.asMapType()
                        .keyType());
                return k != null ? k : unsupportedType(type.asMapType()
                        .valueType());
            default:
                // TIME, TIMESTAMP_NANO, UUID, FIXED, VARIANT, GEOMETRY, GEOGRAPHY, UNKNOWN: not encoded natively yet.
                return type.toString();
        }
    }

    /**
     * A float or double anywhere under {@code type} (refused inside lists and
     * maps, see FloatingMetrics).
     */
    private static boolean floating(Type type) {
        if (type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE) {
            return true;
        }
        if (type.isNestedType()) {
            return type.asNestedType().fields().stream()
                    .anyMatch(f -> floating(f.type()));
        }
        return false;
    }

    /**
     * What the tasks need, read from Iceberg's {@code SparkWrite}: a
     * serializable copy of the table, the write schema, the output spec id, the
     * target file size, the table properties overlaid with the write's own, and
     * the operation id Iceberg names the write's files with.
     */
    public static IcebergWriteSpec specOf(Write write) {
        SparkWrite w = (SparkWrite) write;
        Table table = field(w, "table", Table.class);
        Map<String, String> props = new HashMap<>(table.properties());
        Map<?, ?> writeProps = field(w, "writeProperties", Map.class);
        for (Map.Entry<?, ?> e : writeProps.entrySet()) {
            props.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
        }
        return new IcebergWriteSpec(SerializableTable.copyOf(table), field(w, "writeSchema", Schema.class), field(w, "outputSpecId", Integer.class),
                field(w, "targetFileSize", Long.class), props, field(w, "queryId", String.class));
    }

    /**
     * The top-level field names of the write's Iceberg write schema, in order:
     * the data columns and, for a format v3 rewrite, the row lineage Iceberg
     * appends ({@code MetadataColumns.schemaWithRowLineage}).
     */
    public static String[] writeSchemaNames(Write write) {
        Schema schema = field((SparkWrite) write, "writeSchema", Schema.class);
        return schema.columns().stream()
                .map(Types.NestedField::name)
                .toArray(String[]::new);
    }

    /**
     * One task's commit message for {@code files}, as Iceberg's own data writer
     * returns it.
     */
    public static WriterCommitMessage taskCommit(DataFile[] files) {
        return new SparkWrite.TaskCommit(files);
    }

    private static <T> T field(SparkWrite w, String name, Class<T> type) {
        try {
            Field f = SparkWrite.class.getDeclaredField(name);
            f.setAccessible(true);
            return type.cast(f.get(w));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("SparkWrite." + name + " is not readable in this Iceberg version", e);
        }
    }
}
