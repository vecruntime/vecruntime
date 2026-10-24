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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * The data half of a merge-on-read write (#901 M1): what the columnar data
 * writer needs from Iceberg's {@code SparkPositionDeltaWrite} -- whose fields
 * and {@code Context} are package-private, hence this package -- to write the
 * INSERT and REINSERT rows of a {@code DELETE} / {@code UPDATE} / {@code MERGE}
 * as Iceberg's own delta writer would, and the commit message carrying those
 * data files.
 *
 * <p>Iceberg's delta writer writes inserts with the table's current spec, into
 * files of the context's target size and data schema; on a table with row
 * lineage (format v3) the schema carries {@code _row_id} and {@code
 * _last_updated_sequence_number} after the data columns, a REINSERT taking both
 * from the row's metadata and an INSERT leaving them null ({@code
 * ExtractRowLineage}).
 */
public final class IcebergDeltaDataBridge {

    private IcebergDeltaDataBridge() {}

    /** Whether {@code write} is Iceberg's position delta write. */
    public static boolean isPositionDeltaWrite(DeltaWrite write) {
        return write instanceof SparkPositionDeltaWrite;
    }

    /**
     * Whether the write's data files carry row lineage (the table supports it:
     * format v3 and later).
     */
    public static boolean rowLineage(DeltaWrite write) {
        return TableUtil.supportsRowLineage(field(write, "table", Table.class));
    }

    /**
     * Why the columnar data writer cannot write the data half of {@code write},
     * or null when it can: Parquet data files, a current spec whose sources the
     * batches carry, an unencrypted table, and data types the columnar Parquet
     * writer encodes.
     */
    public static String declineReason(DeltaWrite write) {
        if (!(write instanceof SparkPositionDeltaWrite)) {
            return "the write is not Iceberg's SparkPositionDeltaWrite";
        }
        FileFormat format = context(write, "dataFileFormat", FileFormat.class);
        if (format != FileFormat.PARQUET) {
            return "data file format " + format + " is not Parquet";
        }
        Table table = field(write, "table", Table.class);
        Schema schema = writeSchema(write);
        PartitionSpec spec = table.spec();
        if (!spec.isUnpartitioned()) {
            String partitions = IcebergWriteSpec.unsupportedPartitioning(spec, schema);
            if (partitions != null) {
                return partitions;
            }
        }
        if (table.properties().keySet().stream()
                .anyMatch(k -> k.startsWith("encryption."))) {
            return "the table is encrypted";
        }
        String type = IcebergDataCommitBridge.unsupportedType(schema.asStruct());
        return type == null ? null : "data schema type " + type + " is not supported";
    }

    /**
     * The data files' schema: the context's data schema, with the row lineage
     * columns when the table has them.
     */
    public static Schema writeSchema(DeltaWrite write) {
        Schema data = context(write, "dataSchema", Schema.class);
        return rowLineage(write) ? MetadataColumns.schemaWithRowLineage(data) : data;
    }

    /**
     * What the tasks need: a serializable copy of the table, the data schema
     * (lineage included), the current spec, the target data file size, the
     * table properties overlaid with the write's own, the query id the files
     * are named by, and fanout -- inserts arrive in the order of the deleted
     * rows' files, not clustered by the new rows' partition.
     */
    public static IcebergWriteSpec specOf(DeltaWrite write) {
        Table table = field(write, "table", Table.class);
        Map<String, String> props = new HashMap<>(table.properties());
        Map<?, ?> writeProps = field(write, "writeProperties", Map.class);
        for (Map.Entry<?, ?> e : writeProps.entrySet()) {
            props.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
        }
        return new IcebergWriteSpec(
                SerializableTable.copyOf(table),
                writeSchema(write),
                table.spec().specId(),
                context(write, "targetDataFileSize", Long.class),
                props,
                context(write, "queryId", String.class),
                !table.spec().isUnpartitioned(),
                field(write, "sortOrderId", Integer.class));
    }

    /**
     * One task's commit message for its data files, as Iceberg's delta writer
     * reports them.
     */
    public static WriterCommitMessage dataCommit(DataFile[] files) {
        return new SparkPositionDeltaWrite.DeltaTaskCommit(WriteResult.builder()
                .addDataFiles(files)
                .build());
    }

    /** A value of the write's (private) {@code Context}, by its accessor's name. */
    private static <T> T context(Object w, String name, Class<T> type) {
        Object context = field(w, "context", Object.class);
        try {
            java.lang.reflect.Method m = context.getClass().getDeclaredMethod(name);
            m.setAccessible(true);
            return type.cast(m.invoke(context));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("SparkPositionDeltaWrite.Context." + name + " is not readable", e);
        }
    }

    private static <T> T field(Object w, String name, Class<T> type) {
        try {
            Field f = SparkPositionDeltaWrite.class.getDeclaredField(name);
            f.setAccessible(true);
            return type.cast(f.get(w));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("SparkPositionDeltaWrite." + name + " is not readable", e);
        }
    }
}
