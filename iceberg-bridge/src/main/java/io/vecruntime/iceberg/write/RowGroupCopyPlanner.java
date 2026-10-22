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

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Decides, for a copy-on-write {@code DELETE} rewriting one source data file,
 * which of the file's row groups can be copied byte for byte and which must be
 * decoded, filtered and re-encoded (#901 M4a). This is the pure decision the
 * per-file rewrite acts on; it reads only the source footer and the set of
 * deleted positions, so it is unit-testable without a Spark session.
 *
 * <p>A row group of the source file covers the row positions {@code [start,
 * start + rowCount)}, where {@code start} is the sum of the earlier groups'
 * row counts (Parquet row groups are contiguous and in order, so {@code _pos}
 * -- the row's position in its file -- maps to a group by range). For each
 * group:
 *
 * <ul>
 *   <li>no deleted position in its range -&gt; <b>copy</b> the group's bytes;
 *   <li>at least one but not all -&gt; <b>re-encode</b> its surviving rows;
 *   <li>all deleted -&gt; <b>skip</b> the group.
 * </ul>
 *
 * <p>The whole file falls back to the normal full rewrite ({@link
 * #copyable()} is false) when any guard fails:
 *
 * <ul>
 *   <li><b>schema:</b> the source file's Parquet schema must equal the write
 *       schema exactly (same columns, order, physical and logical types,
 *       repetition and field ids). After schema evolution it differs and a
 *       copied group's pages would be invalid under the new footer. On a v3
 *       table the write schema carries {@code _row_id} and {@code
 *       _last_updated_sequence_number}; a source file that does not materialise
 *       them does not match, so a copied group could not carry the lineage
 *       Iceberg's rewrite assigns -- the exact-schema guard covers v3 lineage
 *       with no extra check.
 *   <li><b>NaN counts:</b> a Parquet footer carries no NaN count, so a copied
 *       group cannot contribute one. When the table tracks a {@code float} or
 *       {@code double} column's metrics at {@code counts} or finer (so its
 *       {@code nan_value_count} is reported) and any group is copied rather
 *       than fully rewritten, the file's NaN count would be wrong. The file
 *       then falls back to the full rewrite, which decodes every value and
 *       counts NaNs through {@link FloatingMetrics}, exactly as a copy-free
 *       M4a and Iceberg's own writer do.
 * </ul>
 */
public final class RowGroupCopyPlanner {

    /** What to do with one source row group. */
    public enum Action {
        COPY,
        REENCODE,
        SKIP
    }

    /** One source row group: its ordinal, its row range, and the action. */
    public record Group(int ordinal, long start, long rowCount,
                        Action action) {}

    private final boolean copyable;
    private final String fallbackReason;
    private final List<Group> groups;
    private final MessageType fileSchema;

    private RowGroupCopyPlanner(boolean copyable, String fallbackReason, List<Group> groups,
            MessageType fileSchema) {
        this.copyable = copyable;
        this.fallbackReason = fallbackReason;
        this.groups = groups;
        this.fileSchema = fileSchema;
    }

    /**
     * Plans the rewrite of one source file.
     *
     * @param footer the source file's Parquet footer
     * @param writeSchema the Parquet schema the writer produces for the table
     *     (field ids and all), i.e. what a copied group must already match
     * @param deletedPositions the file positions ({@code _pos}) deleted from
     *     this file, in any order
     * @param icebergSchema the table's Iceberg write schema (for the NaN guard)
     * @param metricsConfig the table's metrics modes
     */
    public static RowGroupCopyPlanner plan(ParquetMetadata footer, MessageType writeSchema, long[] deletedPositions,
            Schema icebergSchema, MetricsConfig metricsConfig) {
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        List<BlockMetaData> blocks = footer.getBlocks();

        // Row group row ranges, in file order.
        long[] start = new long[blocks.size()];
        long s = 0;
        for (int g = 0; g < blocks.size(); g++) {
            start[g] = s;
            s += blocks.get(g).getRowCount();
        }
        long total = s;

        // Deleted count per group, by binary-searching each position's group.
        long[] deletedInGroup = new long[blocks.size()];
        for (long pos : deletedPositions) {
            int g = groupOf(start, pos, total);
            if (g >= 0) {
                deletedInGroup[g]++;
            }
        }

        List<Group> groups = new ArrayList<>(blocks.size());
        boolean anyCopy = false;
        for (int g = 0; g < blocks.size(); g++) {
            long rows = blocks.get(g).getRowCount();
            Action action;
            if (deletedInGroup[g] == 0) {
                action = Action.COPY;
                anyCopy = true;
            } else if (deletedInGroup[g] >= rows) {
                action = Action.SKIP;
            } else {
                action = Action.REENCODE;
            }
            groups.add(new Group(g, start[g], rows, action));
        }

        if (!writeSchema.equals(fileSchema)) {
            return new RowGroupCopyPlanner(false, "source file schema differs from the write schema", groups, fileSchema);
        }
        // Only a copy loses NaN information; a file re-encoded group by group counts NaNs like the full rewrite.
        if (anyCopy && tracksNaN(icebergSchema, metricsConfig)) {
            return new RowGroupCopyPlanner(false, "the table tracks NaN counts for a float/double column and a copied group has none",
                    groups, fileSchema);
        }
        return new RowGroupCopyPlanner(true, null, groups, fileSchema);
    }

    /** The group whose range contains {@code pos}, or -1 when out of range. */
    private static int groupOf(long[] start, long pos, long total) {
        if (pos < 0 || pos >= total) {
            return -1;
        }
        int lo = 0;
        int hi = start.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (start[mid] <= pos) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /**
     * Whether the table reports a NaN count for any float or double field: its
     * metrics mode is {@code counts}, {@code truncate} or {@code full} (not
     * {@code none}). A footer cannot supply NaN counts, so a copied group would
     * make the file's NaN count wrong.
     */
    static boolean tracksNaN(Schema schema, MetricsConfig metricsConfig) {
        return anyFloatingTracked(schema.asStruct(), metricsConfig);
    }

    private static boolean anyFloatingTracked(Types.StructType struct, MetricsConfig metricsConfig) {
        for (Types.NestedField field : struct.fields()) {
            if (floatingTracked(field, metricsConfig)) {
                return true;
            }
        }
        return false;
    }

    private static boolean floatingTracked(Types.NestedField field, MetricsConfig metricsConfig) {
        Type type = field.type();
        switch (type.typeId()) {
            case FLOAT:
            case DOUBLE:
                MetricsModes.MetricsMode mode = metricsConfig.columnMode(field.name());
                return !(mode instanceof MetricsModes.None);
            case STRUCT:
                return anyFloatingTracked(type.asStructType(), metricsConfig);
            case LIST:
                return anyFloatingNested(type.asListType()
                        .elementType(),
                        metricsConfig);
            case MAP:
                return anyFloatingNested(type.asMapType()
                        .keyType(),
                        metricsConfig)
                        || anyFloatingNested(type.asMapType()
                        .valueType(),
                        metricsConfig);
            default:
                return false;
        }
    }

    // A float/double nested under a list or map: the mode lookup by column name is coarse for nested fields, so
    // be conservative -- if the structure contains a float or double at all, treat it as tracked unless the
    // table turned all metrics off by default.
    private static boolean anyFloatingNested(Type type, MetricsConfig metricsConfig) {
        switch (type.typeId()) {
            case FLOAT:
            case DOUBLE:
                return !(metricsConfig.columnMode("*") instanceof MetricsModes.None);
            case STRUCT:
                for (Types.NestedField f : type.asStructType().fields()) {
                    if (anyFloatingNested(f.type(), metricsConfig)) {
                        return true;
                    }
                }
                return false;
            case LIST:
                return anyFloatingNested(type.asListType()
                        .elementType(),
                        metricsConfig);
            case MAP:
                return anyFloatingNested(type.asMapType()
                        .keyType(),
                        metricsConfig)
                        || anyFloatingNested(type.asMapType()
                        .valueType(),
                        metricsConfig);
            default:
                return false;
        }
    }

    /**
     * Whether the file can be rewritten with row-group copies ({@code false}
     * -&gt; full rewrite).
     */
    public boolean copyable() {
        return copyable;
    }

    /** Why the file is not copyable, or {@code null} when it is. */
    public String fallbackReason() {
        return fallbackReason;
    }

    /** The per-group plan, in file order. */
    public List<Group> groups() {
        return groups;
    }

    /** The source file's Parquet schema. */
    public MessageType fileSchema() {
        return fileSchema;
    }

    /** How many groups the plan copies. */
    public int copiedGroups() {
        return (int) groups.stream()
                .filter(g -> g.action() == Action.COPY)
                .count();
    }
}
