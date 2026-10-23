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
                        Action action, boolean mixed) {

        /**
         * A group whose action needs no mix flag (REENCODE, SKIP, or a
         * whole-group COPY).
         */
        public Group(int ordinal, long start, long rowCount,
                     Action action) {
            this(ordinal, start, rowCount, action, false);
        }

        /**
         * Whether a COPY group copies EVERY column byte for byte, as opposed to
         * a mixed group that copies some columns and encodes others (v3
         * lineage, or an M4b column subset). A mixed group's per-column copy
         * mask is {@link RowGroupCopyPlanner#copyMask()} (schema-level, shared
         * by every mixed group of the file).
         */
        public boolean copiesWholeGroup() {
            return action == Action.COPY && !mixed;
        }
    }

    private final boolean copyable;
    private final String fallbackReason;
    private final List<Group> groups;
    private final MessageType fileSchema;
    // The schema the OUTPUT file uses on the copy path: the source schema for a whole-group copy (v2), or the
    // full write schema (data + trailing lineage) for a v3 mixed-group copy. Null when not copyable.
    private final MessageType copyOutputSchema;
    // Per output-schema column, true to copy its chunk from the source, false to encode it. Null for a
    // whole-group copy (every column copied). Set for the v3 lineage case (data copied, lineage encoded).
    private final boolean[] copyMask;

    private RowGroupCopyPlanner(boolean copyable, String fallbackReason, List<Group> groups,
            MessageType fileSchema, MessageType copyOutputSchema, boolean[] copyMask) {
        this.copyable = copyable;
        this.fallbackReason = fallbackReason;
        this.groups = groups;
        this.fileSchema = fileSchema;
        this.copyOutputSchema = copyOutputSchema;
        this.copyMask = copyMask;
    }

    /**
     * Plans the rewrite of one source file.
     *
     * @param footer the source file's Parquet footer
     * @param writeSchema the Parquet schema the writer produces for the table
     *     (field ids and all), i.e. what a copied group must already match --
     *     on a v3 table it carries the two trailing row-lineage columns
     * @param deletedPositions the file positions ({@code _pos}) deleted from
     *     this file, in any order
     * @param icebergSchema the table's Iceberg write schema (for the NaN guard)
     * @param metricsConfig the table's metrics modes
     */
    public static RowGroupCopyPlanner plan(ParquetMetadata footer, MessageType writeSchema, long[] deletedPositions,
            Schema icebergSchema, MetricsConfig metricsConfig) {
        return plan(footer, writeSchema, deletedPositions, icebergSchema, metricsConfig, null);
    }

    /**
     * As {@link #plan(ParquetMetadata, MessageType, long[], Schema,
     * MetricsConfig)}, but with a per-write-column {@code encodeColumns} mask
     * (#901 M4b): {@code true} re-encodes the column (a changed UPDATE column,
     * or a v3 lineage column), {@code false} copies its chunk byte for byte.
     * {@code null} means the M4a default -- copy every data column and, on a v3
     * source missing lineage, encode only the two lineage columns.
     *
     * <p>A copied column keeps the SOURCE's field (its repetition, so a
     * REQUIRED source chunk's pages, which carry no definition levels, stay
     * valid); an encoded column uses the WRITE schema's field. The copy is
     * declined when a column marked for copy is not present in the source at
     * its position (schema evolution).
     */
    public static RowGroupCopyPlanner plan(ParquetMetadata footer, MessageType writeSchema, long[] deletedPositions,
            Schema icebergSchema, MetricsConfig metricsConfig, boolean[] encodeColumns) {
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

        // The copy mode and the mask a COPY group uses.
        MessageType copyOutputSchema;
        boolean[] copyMask;
        if (encodeColumns != null) {
            // #901 M4b: a per-column copy/encode mask (changed UPDATE columns + v3 lineage are encoded, the
            // rest copied). Build the output schema per column and verify every copied column matches the
            // source at its position.
            CopyColumns cc = copyColumns(writeSchema, fileSchema, encodeColumns);
            copyOutputSchema = cc == null ? null : cc.outputSchema;
            copyMask = cc == null ? null : cc.copyMask;
        } else if (compatibleForCopy(writeSchema, fileSchema)) {
            // Exact match (v2, or a v3 source that materialises lineage): copy the whole group.
            copyOutputSchema = fileSchema;
            copyMask = null;
        } else {
            // v3 source missing lineage: copy the data chunks, encode the two lineage chunks.
            boolean[] lineageMask = lineageCopyMask(writeSchema, fileSchema);
            if (lineageMask != null) {
                copyOutputSchema = lineageOutputSchema(writeSchema, fileSchema);
                copyMask = lineageMask;
            } else {
                copyOutputSchema = null;
                copyMask = null;
            }
        }

        List<Group> groups = new ArrayList<>(blocks.size());
        boolean anyCopy = false;
        // A mixed COPY group copies some columns and encodes others; true exactly when the file is copyable
        // through the lineage mask (a v3 source missing lineage). A whole-group copy has no mask.
        boolean mixed = copyMask != null;
        for (int g = 0; g < blocks.size(); g++) {
            long rows = blocks.get(g).getRowCount();
            Action action;
            boolean groupMixed = false;
            if (deletedInGroup[g] == 0) {
                action = Action.COPY;
                groupMixed = mixed;
                anyCopy = true;
            } else if (deletedInGroup[g] >= rows) {
                action = Action.SKIP;
            } else {
                action = Action.REENCODE;
            }
            groups.add(new Group(g, start[g], rows, action, groupMixed));
        }

        if (copyOutputSchema == null) {
            return new RowGroupCopyPlanner(false, "source file schema differs from the write schema",
                    groups, fileSchema, null, null);
        }
        // Only a copy loses NaN information; a file re-encoded group by group counts NaNs like the full rewrite.
        if (anyCopy && tracksNaN(icebergSchema, metricsConfig)) {
            return new RowGroupCopyPlanner(false, "the table tracks NaN counts for a float/double column and a copied group has none",
                    groups, fileSchema, null, null);
        }
        // #901 M4b cost guard: a mixed copy pays a per-group overhead (footer/index reads, gather + encode of
        // the changed columns, two-pass chunk writes). It only pays off when the COPIED columns are a
        // meaningful share of the group's bytes. When an UPDATE changes almost everything (only one or two
        // narrow columns untouched), copying saves little and the overhead can make it slower than the full
        // rewrite -- so fall back. Measured from the footer: the copied columns' compressed bytes as a fraction
        // of the whole group, over the first group (representative; all groups share the schema/mask).
        if (copyMask != null && !blocks.isEmpty()) {
            double copiedFraction = copiedByteFraction(blocks.get(0), fileSchema, copyMask);
            if (copiedFraction < COPY_MIN_BYTE_FRACTION) {
                return new RowGroupCopyPlanner(false, "copied columns are only "
                        + Math.round(copiedFraction * 100)
                        + "% of the group's bytes (< "
                        + Math.round(COPY_MIN_BYTE_FRACTION * 100)
                        + "%); the full rewrite is cheaper",
                        groups, fileSchema, null, null);
            }
        }
        return new RowGroupCopyPlanner(true, null, groups, fileSchema, copyOutputSchema, copyMask);
    }

    /**
     * Below this copied-byte share of a group, a mixed copy is not worth its
     * overhead (M4b cost guard).
     */
    static final double COPY_MIN_BYTE_FRACTION = 0.25;

    /**
     * The fraction of {@code block}'s total compressed column-chunk bytes that
     * the copied columns account for. A copied field's bytes are all its leaf
     * chunks' compressed sizes; the denominator is every leaf chunk. Lineage
     * columns (always encoded, source-absent) are not in the source block, so
     * they do not appear.
     */
    private static double copiedByteFraction(BlockMetaData block, MessageType fileSchema, boolean[] copyMask) {
        long copied = 0;
        long total = 0;
        // Map each source leaf chunk to its top-level field, so a field's copy flag applies to all its leaves.
        List<org.apache.parquet.hadoop.metadata.ColumnChunkMetaData> chunks = block.getColumns();
        int chunkIdx = 0;
        for (int f = 0; f < fileSchema.getFieldCount(); f++) {
            int leaves = leafCount(fileSchema.getType(f));
            boolean copyThis = f < copyMask.length && copyMask[f];
            for (int l = 0;
                 l < leaves && chunkIdx < chunks.size();
                 l++, chunkIdx++) {
                long sz = chunks.get(chunkIdx).getTotalSize();
                total += sz;
                if (copyThis) {
                    copied += sz;
                }
            }
        }
        return total == 0 ? 0.0 : (double) copied / total;
    }

    /** Leaf (primitive) column count under a schema field. */
    private static int leafCount(org.apache.parquet.schema.Type type) {
        if (type.isPrimitive()) {
            return 1;
        }
        int n = 0;
        for (org.apache.parquet.schema.Type f : type.asGroupType().getFields()) {
            n += leafCount(f);
        }
        return n;
    }

    /**
     * When the source file carries every DATA column of the write schema but
     * NOT its two trailing v3 row-lineage columns ({@code _row_id}, {@code
     * _last_updated_sequence_number}), the per-output-column copy mask for a
     * copy: {@code true} for each data column (copied byte for byte), {@code
     * false} for the two lineage columns (encoded from the scan's resolved
     * values). {@code null} when this is not that case (not a v3 source missing
     * lineage, or the data columns do not match).
     *
     * <p>This is #901 M4a step 1: a v3 source written by an ordinary append
     * does not materialise lineage, but the scan resolves each surviving row's
     * {@code _row_id} ({@code first_row_id + position}, or the file's own
     * column) and {@code _last_updated_sequence_number} (the data file's data
     * sequence number) into the batch. A copy-on-write DELETE then keeps those
     * values: the data chunks are byte-identical to the source and are copied,
     * and only the two lineage chunks are encoded. The output file uses the
     * write schema, so the lineage columns are in its footer.
     */
    static boolean[] lineageCopyMask(MessageType writeSchema, MessageType fileSchema) {
        int wf = writeSchema.getFieldCount();
        int ff = fileSchema.getFieldCount();
        // The write schema must be the file schema plus EXACTLY the two trailing lineage columns.
        if (wf != ff + 2) {
            return null;
        }
        if (!isLineageField(writeSchema.getType(ff), ROW_ID_FIELD_ID, "_row_id") || !isLineageField(writeSchema.getType(ff + 1), SEQ_NUM_FIELD_ID, "_last_updated_sequence_number")) {
            return null;
        }
        // The leading write-schema columns must be the file's own columns (same ids, types, order) -- reuse the
        // exact-match guard on the write schema's data prefix against the file schema.
        MessageType writeData = new MessageType(writeSchema.getName(),
                writeSchema.getFields().subList(0, ff));
        if (!compatibleForCopy(writeData, fileSchema)) {
            return null;
        }
        boolean[] mask = new boolean[wf];
        for (int i = 0; i < ff; i++) {
            mask[i] = true; // data columns: copy
        }
        // lineage columns stay false: encode
        return mask;
    }

    /**
     * Iceberg's reserved field ids for the v3 row-lineage columns
     * (MetadataColumns).
     */
    private static final int ROW_ID_FIELD_ID = Integer.MAX_VALUE - 107;

    private static final int SEQ_NUM_FIELD_ID = Integer.MAX_VALUE - 108;

    /** The output schema and copy mask for a per-column M4b copy. */
    private static final class CopyColumns {
        final MessageType outputSchema;
        final boolean[] copyMask;

        CopyColumns(MessageType outputSchema, boolean[] copyMask) {
            this.outputSchema = outputSchema;
            this.copyMask = copyMask;
        }
    }

    /**
     * Builds the output schema and copy mask for a per-column copy (#901 M4b),
     * or {@code null} when it is not safe. {@code encodeColumns} marks each
     * write-schema column to re-encode ({@code true}) or copy ({@code false}).
     * A copied column takes the SOURCE field at its position (keeping its
     * repetition, so a REQUIRED source chunk's level-free pages stay valid); an
     * encoded column takes the WRITE-schema field. The source file's columns are
     * the leading data columns in write-schema order (v3 lineage is always
     * encoded and trailing), so copied column {@code i} is source column
     * {@code i}. Declined ({@code null}) if any copied column is absent from the
     * source or does not match it by id/type/name (schema evolution), or if
     * nothing is copied.
     */
    private static CopyColumns copyColumns(MessageType writeSchema, MessageType fileSchema, boolean[] encodeColumns) {
        int wf = writeSchema.getFieldCount();
        if (encodeColumns.length != wf) {
            return null;
        }
        List<org.apache.parquet.schema.Type> out = new ArrayList<>(wf);
        boolean[] copyMask = new boolean[wf];
        boolean anyCopy = false;
        for (int i = 0; i < wf; i++) {
            if (encodeColumns[i]) {
                // The mixed-group writer can ENCODE only flat columns; a changed NESTED column declines the
                // whole copy (full rewrite). An untouched nested column is fine -- it is copied byte for byte.
                if (!writeSchema.getType(i).isPrimitive()) {
                    return null;
                }
                out.add(writeSchema.getType(i));
                copyMask[i] = false;
            } else {
                if (i >= fileSchema.getFieldCount()) {
                    return null;
                }
                org.apache.parquet.schema.Type src = fileSchema.getType(i);
                if (!sameColumnIgnoringRepetition(writeSchema.getType(i), src)) {
                    return null;
                }
                out.add(src);
                copyMask[i] = true;
                anyCopy = true;
            }
        }
        if (!anyCopy) {
            return null;
        }
        return new CopyColumns(new MessageType(fileSchema.getName(), out), copyMask);
    }

    /**
     * Whether two fields are the same column up to repetition (id, name,
     * physical and logical type).
     */
    private static boolean sameColumnIgnoringRepetition(org.apache.parquet.schema.Type a, org.apache.parquet.schema.Type b) {
        return normalizeRepetition(a).equals(normalizeRepetition(b));
    }

    /**
     * The output schema for a v3 mixed copy: the SOURCE file's own data columns,
     * unchanged (so a copied chunk's pages stay valid -- in particular a
     * REQUIRED source column keeps REQUIRED, since its pages carry no definition
     * levels), followed by the write schema's two trailing lineage columns
     * ({@code _row_id}, {@code _last_updated_sequence_number}), which are
     * encoded. The lineage columns carry Iceberg's reserved field ids, so the
     * footer metrics key the whole file by Iceberg's ids.
     */
    static MessageType lineageOutputSchema(MessageType writeSchema, MessageType fileSchema) {
        int ff = fileSchema.getFieldCount();
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>(ff + 2);
        fields.addAll(fileSchema.getFields());
        fields.add(writeSchema.getType(ff));
        fields.add(writeSchema.getType(ff + 1));
        return new MessageType(fileSchema.getName(), fields);
    }

    /**
     * Whether {@code type} is the lineage column with field id {@code id} and
     * the given name.
     */
    private static boolean isLineageField(org.apache.parquet.schema.Type type, int id, String name) {
        return type.isPrimitive()
                && type.getId() != null
                && type.getId().intValue() == id
                && type.getName().equals(name);
    }

    /**
     * The schema the output file uses on the copy path, or null when not
     * copyable.
     */
    public MessageType copyOutputSchema() {
        return copyOutputSchema;
    }

    /**
     * The per-output-column copy mask a MIXED copy group uses: {@code true} to
     * copy the column's chunk from the source, {@code false} to encode it.
     * Schema-level (the same for every mixed group of the file), so a {@link
     * Group#mixed() mixed} group reads it here rather than carrying its own.
     * {@code null} when no group is mixed (a whole-group copy, or not copyable).
     */
    public boolean[] copyMask() {
        return copyMask;
    }

    /**
     * How many output columns the copy path writes (data, plus v3 lineage).
     * Equals {@link #copyOutputSchema}'s field count.
     */
    public int outputFieldCount() {
        return copyOutputSchema == null ? fileSchema.getFieldCount() : copyOutputSchema.getFieldCount();
    }

    /**
     * Whether a source file of schema {@code fileSchema} can have its row
     * groups copied into an output file of the source's own schema (#901 M4a).
     * The output schema IS the source schema, so a copied group's pages are
     * trivially valid; what this guards is that the source really is the table
     * the write schema describes -- same fields, same order, same field ids,
     * same physical and logical types -- so the copied-and-re-encoded file is a
     * faithful rewrite of the same rows.
     *
     * <p>Requiredness (REQUIRED vs OPTIONAL) is ALLOWED to differ and is taken
     * from the source: Iceberg's own writer marks an always-non-null column
     * REQUIRED in the file it writes, while the copy-on-write write schema
     * (converted from the Spark DELETE projection) marks every column OPTIONAL.
     * That difference alone must not force a full rewrite -- a REQUIRED source
     * column holds no nulls, so copying its pages is sound, and a re-encoded
     * group of survivors from that column holds no nulls either (guarded in the
     * re-encode path). Any OTHER difference (a renamed, added, dropped,
     * reordered or promoted column, a different field id or logical type)
     * leaves the schemas incompatible and the file falls back to the full
     * rewrite. The comparison is by the Parquet text form -- the contract
     * parquet-java uses between two schemas, carrying ids and logical-type
     * annotations -- with repetition normalised to OPTIONAL on both sides.
     *
     * <p>On a format v3 table the write schema carries two trailing row-lineage
     * columns ({@code _row_id} and {@code _last_updated_sequence_number}). A
     * copy-on-write DELETE must preserve each surviving row's {@code _row_id},
     * and a source file written by an ordinary append does NOT materialise
     * lineage -- it inherits it from the data file's {@code first_row_id} plus
     * the row position. Copying or re-encoding such a file into a NEW data file
     * would re-inherit lineage from the new file's {@code first_row_id} and new
     * positions, changing the surviving rows' {@code _row_id} after deletions
     * shift them. So a source that does not already materialise lineage is NOT
     * copyable on v3 and falls back to the full rewrite, which writes each
     * surviving row's original {@code _row_id} explicitly. This is enforced by
     * the exact field-count-and-ids match below: the source must carry the same
     * columns the write schema has, lineage included.
     */
    static boolean compatibleForCopy(MessageType writeSchema, MessageType fileSchema) {
        if (writeSchema.equals(fileSchema)) {
            return true;
        }
        return normalizeRepetition(writeSchema).equals(normalizeRepetition(fileSchema));
    }

    /**
     * How many data columns the source file actually has: its own field count,
     * which is the number of columns a re-encoded group writes. Equal to the
     * write schema's field count whenever the file is copyable.
     */
    public int dataFieldCount() {
        return fileSchema.getFieldCount();
    }

    /**
     * {@code type} with every REQUIRED repetition rewritten to OPTIONAL,
     * recursively.
     */
    private static org.apache.parquet.schema.Type normalizeRepetition(org.apache.parquet.schema.Type type) {
        org.apache.parquet.schema.Type.Repetition rep = type.isRepetition(org.apache.parquet.schema.Type.Repetition.REPEATED) ? org.apache.parquet.schema.Type.Repetition.REPEATED : org.apache.parquet.schema.Type.Repetition.OPTIONAL;
        if (type.isPrimitive()) {
            org.apache.parquet.schema.PrimitiveType p = type.asPrimitiveType();
            org.apache.parquet.schema.Types.PrimitiveBuilder<org.apache.parquet.schema.PrimitiveType> b = org.apache.parquet.schema.Types.primitive(p.getPrimitiveTypeName(), rep);
            if (p.getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                b.length(p.getTypeLength());
            }
            if (p.getLogicalTypeAnnotation() != null) {
                b.as(p.getLogicalTypeAnnotation());
            }
            if (p.getId() != null) {
                b.id(p.getId()
                      .intValue());
            }
            return b.named(p.getName());
        }
        org.apache.parquet.schema.GroupType g = type.asGroupType();
        java.util.List<org.apache.parquet.schema.Type> children = new ArrayList<>(g.getFieldCount());
        for (org.apache.parquet.schema.Type f : g.getFields()) {
            children.add(normalizeRepetition(f));
        }
        if (g instanceof MessageType) {
            return new MessageType(g.getName(), children);
        }
        org.apache.parquet.schema.GroupType built = rep == org.apache.parquet.schema.Type.Repetition.REPEATED ? org.apache.parquet.schema.Types.repeatedGroup()
                .as(g.getLogicalTypeAnnotation())
                .addFields(children.toArray(new org.apache.parquet.schema.Type[0]))
                .named(g.getName())
                : org.apache.parquet.schema.Types.optionalGroup()
                .as(g.getLogicalTypeAnnotation())
                .addFields(children.toArray(new org.apache.parquet.schema.Type[0]))
                .named(g.getName());
        if (g.getId() != null) {
            return built.withId(g.getId()
                                 .intValue());
        }
        return built;
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
