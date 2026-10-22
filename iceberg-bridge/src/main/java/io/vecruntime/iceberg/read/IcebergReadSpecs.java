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
package io.vecruntime.iceberg.read;

import java.util.ArrayList;
import java.util.List;

import io.vecruntime.parquet.read.LeafReadSpec;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

/**
 * An Iceberg projection resolved against one data file's Parquet schema (#901
 * R1), with no Spark type in the middle: each projected field is found in the
 * file by its Iceberg field id -- the file's own ids; else the table's name
 * mapping's; else, as Iceberg's {@code ParquetSchemaUtil.addFallbackIds} does,
 * the top-level columns' positions -- and becomes
 *
 * <ul>
 * <li>a {@link Leaf} with its {@link LeafReadSpec} (promotions included),
 * <li>a {@link Struct}, {@link ListNode} or {@link MapNode} of resolved
 * children,
 * <li>a {@link Missing} field: added after the file was written, so null in
 * every row, or its {@code initial-default} (format v3),
 * <li>a {@link Metadata} column ({@code _file}, {@code _pos}, ...), which the
 * scan fills.
 * </ul>
 *
 * The names a file gave its columns play no part, so a renamed column, a new
 * column taking an old one's name, or columns reordered resolve as Iceberg's
 * reader resolves them. {@link FileProjection#requested()} is the file's schema
 * pruned to the projected columns, for parquet-java's {@code
 * setRequestedSchema}.
 */
public final class IcebergReadSpecs {

    private IcebergReadSpecs() {}

    /** A resolved projected field. */
    public sealed interface Node permits Leaf, Struct, ListNode, MapNode, Missing, Metadata {
        Types.NestedField field();
    }

    /** A primitive read from {@code column} with {@code spec}. */
    public record Leaf(Types.NestedField field, ColumnDescriptor column, LeafReadSpec spec) implements Node {}

    /**
     * A struct present in the file (as {@code fileType}, field ids applied);
     * its fields resolved in projection order. {@code levelField}: when the
     * file holds none of them, one of the group's own leaves, read only for
     * the struct's nulls (a present struct of null fields where the group is
     * defined, as Iceberg reads it); null otherwise.
     */
    public record Struct(Types.NestedField field, org.apache.parquet.schema.Type fileType, List<Node> fields,
                         org.apache.parquet.schema.Type levelField)
            implements Node {}

    /** A list present in the file (as {@code fileType}). */
    public record ListNode(Types.NestedField field, org.apache.parquet.schema.Type fileType, Node element) implements Node {}

    /** A map present in the file (as {@code fileType}). */
    public record MapNode(Types.NestedField field, org.apache.parquet.schema.Type fileType, Node key,
                          Node value)
            implements Node {}

    /**
     * A field the file does not hold: its {@code initialDefault} (Iceberg's
     * internal representation) in every row, or null in every row when it has
     * none.
     */
    public record Missing(Types.NestedField field, Object initialDefault) implements Node {}

    /**
     * The row-lineage metadata columns a v3 data file may materialise
     * ({@code _row_id}, {@code _last_updated_sequence_number}): read from the
     * file like a data column (missing, so null, where the file lacks them),
     * the scan deriving the value of a null from the file's first row id or
     * data sequence number.
     */
    static boolean isLineage(int id) {
        return id == MetadataColumns.ROW_ID.fieldId() || id == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
    }

    /**
     * One of Iceberg's metadata columns ({@link MetadataColumns}), filled by
     * the scan.
     */
    public record Metadata(Types.NestedField field) implements Node {}

    /**
     * One file's resolution: the file's schema with the field ids it was
     * matched by ({@code fileWithIds}: the file's own, the name mapping's or
     * the positional ones), that schema pruned to what is read, each projected
     * top-level field resolved, and, when some field cannot be read natively,
     * why (the file then keeps Iceberg's reader).
     */
    public record FileProjection(MessageType fileWithIds, MessageType requested, List<Node> columns,
            String unsupported) {}

    /**
     * Resolves {@code projection} against {@code file}, matching by field id
     * (the file's, else {@code mapping}'s when non-null, else the top-level
     * columns' positions).
     *
     * @throws IllegalArgumentException a required projected field the file does
     *     not hold and that has no initial default, as Iceberg's reader fails
     */
    public static FileProjection resolve(Types.StructType projection, MessageType file, NameMapping mapping) {
        MessageType withIds = hasIds(file)
                ? file
                : mapping != null ? applyMapping(file, mapping) : fallbackIds(file);
        Resolver r = new Resolver(withIds);
        List<Node> columns = new ArrayList<>();
        List<org.apache.parquet.schema.Type> kept = new ArrayList<>();
        for (Types.NestedField f : projection.fields()) {
            boolean filled = MetadataColumns.isMetadataColumn(f.fieldId()) && !isLineage(f.fieldId());
            org.apache.parquet.schema.Type t = filled ? null : byId(withIds, f.fieldId());
            Node n = filled ? new Metadata(f) : r.node(f, t, new String[0]);
            columns.add(n);
            org.apache.parquet.schema.Type pruned = n instanceof Missing || n instanceof Metadata
                    ? null
                    : prune(n, t);
            if (pruned != null) {
                kept.add(pruned);
            }
        }
        return new FileProjection(withIds, new MessageType(file.getName(), kept), List.copyOf(columns), r.unsupported);
    }

    // ------------------------------------------------------------------ resolution

    private static final class Resolver {
        private final MessageType file;
        private String unsupported;

        Resolver(MessageType file) {
            this.file = file;
        }

        Node node(Types.NestedField f, org.apache.parquet.schema.Type t, String[] parentPath) {
            if (f.type().typeId() == Type.TypeID.UNKNOWN) {
                // The v3 unknown type (#rtypes): always null, never stored in a file. Null in every row, as
                // Iceberg's reader produces it (Spark sees it as NullType).
                return new Missing(f, null);
            }
            if (t == null) {
                if (f.isRequired() && f.initialDefault() == null) {
                    throw new IllegalArgumentException("Missing required field: " + f.name());
                }
                return new Missing(f, f.initialDefault());
            }
            String[] path = append(parentPath, t.getName());
            Type type = f.type();
            if (type.isPrimitiveType()) {
                if (!t.isPrimitive()) {
                    return unsupported(f, "field " + f.name() + " is a group in the file");
                }
                LeafReadSpec spec = IcebergLeafSpecs.of(type.asPrimitiveType(), t.asPrimitiveType());
                if (spec == null) {
                    return unsupported(f, "field "
                            + f.name()
                            + " of type "
                            + type
                            + " stored as "
                            + t.asPrimitiveType().getPrimitiveTypeName());
                }
                return new Leaf(f, file.getColumnDescription(path), spec);
            }
            if (t.isPrimitive()) {
                return unsupported(f, "field " + f.name() + " is a primitive in the file");
            }
            GroupType g = t.asGroupType();
            if (type.isStructType()) {
                List<Node> fields = new ArrayList<>();
                boolean any = false;
                for (Types.NestedField c : type.asStructType().fields()) {
                    Node n = node(c, byId(g, c.fieldId()), path);
                    any |= !(n instanceof Missing);
                    fields.add(n);
                }
                if (any) {
                    return new Struct(f, t, List.copyOf(fields), null);
                }
                // A struct the file holds but none of whose projected fields it does: Iceberg reads it as a present
                // struct of nulls (null only where the group is). Its nulls come from one of the group's own leaves,
                // read only for its definition levels -- the narrowest single non-repeated primitive child.
                org.apache.parquet.schema.Type level = levelField(g);
                return level == null ? unsupported(f, "struct " + f.name() + " holds none of its projected fields and no leaf to read its nulls from") : new Struct(f, t, List.copyOf(fields), level);
            }
            if (type.isListType()) {
                org.apache.parquet.schema.Type element = listElement(g);
                if (element == null) {
                    return unsupported(f, "list " + f.name() + " has no element in the file");
                }
                Types.NestedField e = type.asListType()
                        .fields()
                        .get(0);
                String[] elementParent = elementIsRepeated(g) ? path : append(path, g.getType(0)
                        .getName());
                Node n = node(e, idOrNull(element, e.fieldId()), elementParent);
                return n instanceof Missing ? unsupported(f, "list " + f.name() + "'s element is not the file's") : new ListNode(f, t, n);
            }
            if (type.isMapType()) {
                if (g.getFieldCount() != 1 || g.getType(0).isPrimitive() || g.getType(0)
                        .asGroupType()
                        .getFieldCount()
                        != 2) {
                    return unsupported(f, "map " + f.name() + " is not a key-value group in the file");
                }
                GroupType kv = g.getType(0).asGroupType();
                String[] kvPath = append(path, kv.getName());
                Types.NestedField k = type.asMapType()
                        .fields()
                        .get(0);
                Types.NestedField v = type.asMapType()
                        .fields()
                        .get(1);
                Node key = node(
                        k,
                        idOrNull(kv.getType(0), k.fieldId()),
                        kvPath);
                Node value = node(
                        v,
                        idOrNull(kv.getType(1), v.fieldId()),
                        kvPath);
                if (key instanceof Missing || value instanceof Missing) {
                    return unsupported(f, "map " + f.name() + "'s key or value is not the file's");
                }
                return new MapNode(f, t, key, value);
            }
            return unsupported(f, "field " + f.name() + " of type " + type);
        }

        private Node unsupported(Types.NestedField f, String why) {
            if (unsupported == null) {
                unsupported = why;
            }
            return new Missing(f, null);
        }
    }

    /**
     * A list group's element: the three-level form's {@code element} inside its
     * repeated group, or the two-level form's repeated field itself (a
     * primitive, a group of several fields, or a group named {@code array} or
     * {@code <list>_tuple}), as parquet-java's and Iceberg's list readers take
     * them.
     */
    static org.apache.parquet.schema.Type listElement(GroupType list) {
        if (list.getFieldCount() != 1) {
            return null;
        }
        org.apache.parquet.schema.Type repeated = list.getType(0);
        if (!repeated.isRepetition(org.apache.parquet.schema.Type.Repetition.REPEATED)) {
            return null;
        }
        return elementIsRepeated(list) ? repeated : repeated.asGroupType().getType(0);
    }

    /**
     * Whether a (one-field) list group's repeated field is its element itself:
     * the two-level form.
     */
    static boolean elementIsRepeated(GroupType list) {
        org.apache.parquet.schema.Type repeated = list.getType(0);
        if (repeated.isPrimitive()) {
            return true;
        }
        GroupType r = repeated.asGroupType();
        return r.getFieldCount() != 1 || r.getName().equals("array") || r.getName().equals(list.getName() + "_tuple");
    }

    // ------------------------------------------------------------------ pruning

    /**
     * {@code t} keeping only what {@code n} reads (struct fields in projection
     * order).
     */
    private static org.apache.parquet.schema.Type prune(Node n, org.apache.parquet.schema.Type t) {
        if (n instanceof Leaf) {
            return t;
        }
        GroupType g = t.asGroupType();
        if (n instanceof Struct s) {
            List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
            for (Node c : s.fields()) {
                if (!(c instanceof Missing)) {
                    org.apache.parquet.schema.Type ct = byId(g, c.field()
                            .fieldId());
                    fields.add(prune(c, ct));
                }
            }
            if (s.levelField() != null) {
                fields.add(s.levelField());
            }
            return g.withNewFields(fields);
        }
        if (n instanceof ListNode l) {
            org.apache.parquet.schema.Type element = listElement(g);
            org.apache.parquet.schema.Type repeated = g.getType(0);
            if (elementIsRepeated(g)) {
                return g.withNewFields(List.of(prune(l.element(), repeated)));
            }
            return g.withNewFields(List.of(repeated.asGroupType()
                    .withNewFields(List.of(prune(l.element(), element)))));
        }
        MapNode m = (MapNode) n;
        GroupType kv = g.getType(0).asGroupType();
        return g.withNewFields(
                List.of(
                        kv.withNewFields(
                                List.of(
                                        prune(m.key(), kv.getType(0)),
                                        prune(m.value(), kv.getType(1))))));
    }

    /**
     * The narrowest single non-repeated primitive child of {@code g}, read only
     * for a struct's nulls; null when it has none.
     */
    static org.apache.parquet.schema.Type levelField(GroupType g) {
        org.apache.parquet.schema.Type best = null;
        int bestWidth = Integer.MAX_VALUE;
        for (org.apache.parquet.schema.Type c : g.getFields()) {
            if (!c.isPrimitive() || c.isRepetition(org.apache.parquet.schema.Type.Repetition.REPEATED)) {
                continue;
            }
            int width = switch (c.asPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN -> 1;
                case INT32, FLOAT -> 4;
                case INT64, DOUBLE -> 8;
                case INT96 -> 12;
                default -> 16;
            };
            if (width < bestWidth) {
                best = c;
                bestWidth = width;
            }
        }
        return best;
    }

    // ------------------------------------------------------------------ field ids

    private static org.apache.parquet.schema.Type byId(GroupType g, int id) {
        for (org.apache.parquet.schema.Type t : g.getFields()) {
            if (t.getId() != null && t.getId().intValue() == id) {
                return t;
            }
        }
        return null;
    }

    private static org.apache.parquet.schema.Type idOrNull(org.apache.parquet.schema.Type t, int id) {
        // A list element, map key or value matches by id when the file carries one, else by position (Iceberg's
        // readers take the element, key and value positionally).
        return t.getId() == null || t.getId().intValue() == id
                ? t
                : null;
    }

    /**
     * Whether any field, at any depth, carries a field id (Iceberg's {@code
     * ParquetSchemaUtil.hasIds}).
     */
    static boolean hasIds(org.apache.parquet.schema.Type t) {
        if (!(t instanceof MessageType) && t.getId() != null) {
            return true;
        }
        if (t.isPrimitive()) {
            return false;
        }
        for (org.apache.parquet.schema.Type c : t.asGroupType().getFields()) {
            if (hasIds(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Iceberg's {@code addFallbackIds}: the top-level columns numbered 1, 2,
     * ... by position.
     */
    static MessageType fallbackIds(MessageType file) {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        int id = 1;
        for (org.apache.parquet.schema.Type t : file.getFields()) {
            fields.add(t.withId(id++));
        }
        return new MessageType(file.getName(), fields);
    }

    /**
     * Iceberg's {@code ApplyNameMapping}: each column the mapping names given
     * its field id, by path of names.
     */
    static MessageType applyMapping(MessageType file, NameMapping mapping) {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        for (org.apache.parquet.schema.Type t : file.getFields()) {
            fields.add(mapped(t, List.of(t.getName()), mapping));
        }
        return new MessageType(file.getName(), fields);
    }

    private static org.apache.parquet.schema.Type mapped(org.apache.parquet.schema.Type t, List<String> path, NameMapping mapping) {
        MappedField m = mapping.find(path);
        org.apache.parquet.schema.Type out = t;
        if (!t.isPrimitive()) {
            GroupType g = t.asGroupType();
            LogicalTypeAnnotation a = g.getLogicalTypeAnnotation();
            List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
            if (a instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation && g.getFieldCount() == 1) {
                org.apache.parquet.schema.Type repeated = g.getType(0);
                org.apache.parquet.schema.Type element = listElement(g);
                List<String> elementPath = append(path, "element");
                if (element == null || elementIsRepeated(g)) {
                    fields.add(element == null ? repeated : mapped(repeated, elementPath, mapping));
                } else {
                    fields.add(repeated.asGroupType()
                                       .withNewFields(List.of(mapped(element, elementPath, mapping))));
                }
            } else if (a instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation && g.getFieldCount() == 1 && !g.getType(0).isPrimitive()) {
                GroupType kv = g.getType(0).asGroupType();
                List<org.apache.parquet.schema.Type> kvFields = new ArrayList<>();
                for (int i = 0; i < kv.getFieldCount(); i++) {
                    kvFields.add(mapped(kv.getType(i), append(path, i == 0 ? "key" : "value"), mapping));
                }
                fields.add(kv.withNewFields(kvFields));
            } else {
                for (org.apache.parquet.schema.Type c : g.getFields()) {
                    fields.add(mapped(c, append(path, c.getName()), mapping));
                }
            }
            out = g.withNewFields(fields);
        }
        return m != null && m.id() != null
                ? out.withId(m.id())
                : out;
    }

    private static String[] append(String[] path, String name) {
        String[] out = java.util.Arrays.copyOf(path, path.length + 1);
        out[path.length] = name;
        return out;
    }

    private static List<String> append(List<String> path, String name) {
        List<String> out = new ArrayList<>(path);
        out.add(name);
        return out;
    }
}
