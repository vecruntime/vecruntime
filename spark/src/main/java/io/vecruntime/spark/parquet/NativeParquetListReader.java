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
package io.vecruntime.spark.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import io.vecruntime.parquet.read.ListColumnReader;
import io.vecruntime.spark.arrow.VectorNestedColumnVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A list or map column of the native Parquet scan (#568, work item 8): an
 * {@code array<leaf>}, or a {@code map<leaf, leaf>}, at the top of the schema.
 * Its leaves (the element; or the key and the value) are repeated once: each
 * value slot of the column chunk is an ENTRY with a repetition and a
 * definition level. Repetition level 0 starts a row. Below the list's own
 * definition level the row's list is null; below the repeated group's level
 * it is empty (one entry, no element); at or above it the entry is an
 * element, null below the leaf's maximum level. So the batch's offsets and
 * nulls come from the first leaf's levels, and each leaf decodes only its
 * element entries, back to back ({@link NativeParquetColumnReader#readElements}).
 *
 * <p>Every list encoding Spark reads as an array of a leaf has one repeated
 * node on the leaf's path -- the three-level {@code LIST}, the legacy
 * two-level form, a bare {@code repeated} leaf -- and the levels are taken
 * from that node and its parent. An array of structs of leaves has one leaf
 * per field under the same struct group, all in one entry stream: the
 * offsets come from the first field, an element's struct is null below the
 * struct group's level, and each field decodes its own elements. Lists of
 * lists and lists inside structs repeat more than once or mix repeated and
 * unrepeated leaves; they keep Spark's scan.
 */
public final class NativeParquetListReader extends ListColumnReader implements NativeParquetColumn {

    private final DataType type;
    // The base class's leaf readers, as Spark leaves: wrap() asks each for its column.
    private final NativeParquetColumnReader first; // the element, or the key
    private final NativeParquetColumnReader second; // the value, or null
    private final NativeParquetColumnReader[] fields; // an array or map of structs: each field's leaf

    private NativeParquetListReader(DataType type, NativeParquetColumnReader first, NativeParquetColumnReader second,
            int listDef, int elementDef) {
        super(type instanceof MapType, first, second, listDef, elementDef);
        this.type = type;
        this.first = first;
        this.second = second;
        this.fields = null;
    }

    private NativeParquetListReader(DataType type, NativeParquetColumnReader[] fields, int listDef,
            int elementDef, int structDef) {
        super(type instanceof MapType, fields, listDef, elementDef, structDef);
        this.type = type;
        this.first = fields[0];
        this.second = null;
        this.fields = fields;
    }

    /**
     * The reader of the top-level list or map {@code top} (its name in the
     * clipped schema) of Spark type {@code type}; {@link #forEachLeaf} must
     * have accepted it.
     */
    public static NativeParquetColumn create(MessageType clipped, String top, DataType type,
            String name, int batchRows, BufferAllocator allocator) {
        return create(clipped, new String[] {top}, type, name, batchRows,
                allocator);
    }

    /**
     * The reader of the list or map at {@code path} (a top-level field, or a
     * struct's field, #568).
     */
    public static NativeParquetColumn create(MessageType clipped, String[] path, DataType type,
            String name, int batchRows, BufferAllocator allocator) {
        return create(clipped, null, path, type, name, batchRows,
                allocator);
    }

    /**
     * {@link #create(MessageType, String[], DataType, String, int, BufferAllocator)}
     * over a file whose schema is {@code file} (null: every field is in it): a
     * list or map with a struct field the file lacks is read by the tree
     * reader, which fills it with nulls (#568 schema evolution).
     */
    public static NativeParquetColumn create(
            MessageType clipped,
            MessageType file,
            String[] path,
            DataType type,
            String name,
            int batchRows,
            BufferAllocator allocator) {
        if (NativeParquetTreeReader.needed(type) || lacksSome(clipped, file, path)) {
            // #568: nesting below the list or map this reader does not follow (a struct holding a list, ...), or
            // a struct field the file lacks.
            return NativeParquetTreeReader.create(clipped, file, path, type, name, batchRows,
                    allocator);
        }
        if (NativeParquetNestedListReader.arrayDepth(type) > 1) {
            // #568: a list of lists, any depth, over one leaf.
            return NativeParquetNestedListReader.create(clipped, path, (ArrayType) type, name, batchRows,
                    allocator);
        }
        List<ColumnDescriptor> leaves = leavesUnder(clipped, path);
        ColumnDescriptor a = leaves.get(0);
        String[] rep = repeatedPath(clipped, a.getPath());
        int elementDef = clipped.getMaxDefinitionLevel(rep);
        int listDef = rep.length == 1 ? 0 : clipped.getMaxDefinitionLevel(java.util.Arrays.copyOf(rep, rep.length - 1));
        if (type instanceof ArrayType at) {
            DataType et = at.elementType();
            if (et instanceof StructType st) {
                // #568: an array of structs of leaves. Each field's leaf, by position in the struct's order (the
                // clipped schema keeps it; under field-id matching a leaf keeps the file's name).
                NativeParquetColumnReader[] readers = new NativeParquetColumnReader[st.fields().length];
                for (int i = 0; i < readers.length; i++) {
                    StructField f = st.fields()[i];
                    ColumnDescriptor cd = leaves.get(i);
                    readers[i] = new NativeParquetColumnReader(cd, NativeParquetSupport.decodeLane(f.dataType()), f.dataType(),
                            name + ".element." + f.name(), batchRows, allocator);
                }
                String[] leaf0 = leaves.get(0).getPath();
                int structDef = clipped.getMaxDefinitionLevel(java.util.Arrays.copyOf(leaf0, leaf0.length - 1));
                return new NativeParquetListReader(at, readers, listDef, elementDef, structDef);
            }
            return new NativeParquetListReader(
                    type,
                    new NativeParquetColumnReader(a, NativeParquetSupport.decodeLane(et), et, name + ".element",
                            batchRows, allocator),
                    null,
                    listDef,
                    elementDef);
        }
        MapType mt = (MapType) type;
        if (mt.valueType() instanceof StructType st) {
            // #568: a map of structs of leaves: the key, then each value field's leaf matched by name, in order.
            List<ColumnDescriptor> values = leaves.subList(1, leaves.size());
            NativeParquetColumnReader[] readers = new NativeParquetColumnReader[st.fields().length + 1];
            readers[0] = new NativeParquetColumnReader(a, NativeParquetSupport.decodeLane(mt.keyType()), mt.keyType(),
                    name + ".key", batchRows, allocator);
            for (int i = 0; i < st.fields().length; i++) {
                StructField f = st.fields()[i];
                ColumnDescriptor cd = values.get(i);
                readers[i + 1] = new NativeParquetColumnReader(cd, NativeParquetSupport.decodeLane(f.dataType()), f.dataType(),
                        name + ".value." + f.name(), batchRows, allocator);
            }
            String[] v0 = values.get(0).getPath();
            int structDef = clipped.getMaxDefinitionLevel(java.util.Arrays.copyOf(v0, v0.length - 1));
            return new NativeParquetListReader(mt, readers, listDef, elementDef, structDef);
        }
        ColumnDescriptor v = leaves.get(1);
        return new NativeParquetListReader(
                type,
                new NativeParquetColumnReader(a, NativeParquetSupport.decodeLane(mt.keyType()), mt.keyType(),
                        name + ".key", batchRows, allocator),
                new NativeParquetColumnReader(v, NativeParquetSupport.decodeLane(mt.valueType()), mt.valueType(),
                        name + ".value", batchRows, allocator),
                listDef,
                elementDef);
    }

    /**
     * Whether the file ({@code null}: every field is in it) lacks some leaf of
     * the clipped list or map at {@code path}.
     */
    static boolean lacksSome(MessageType clipped, MessageType file, String[] path) {
        if (file == null) {
            return false;
        }
        for (ColumnDescriptor cd : leavesUnder(clipped, path)) {
            if (!file.containsPath(cd.getPath())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The leaf columns at or under {@code path} (a bare repeated primitive is
     * its own leaf), in schema order.
     */
    static List<ColumnDescriptor> leavesUnder(MessageType clipped, String[] path) {
        List<ColumnDescriptor> out = new ArrayList<>();
        for (ColumnDescriptor cd : clipped.getColumns()) {
            String[] p = cd.getPath();
            if (p.length >= path.length && java.util.Arrays.equals(java.util.Arrays.copyOf(p, path.length), path)) {
                out.add(cd);
            }
        }
        return out;
    }

    /**
     * The node at {@code path} in the clipped schema, or null when a step is
     * missing.
     */
    static Type nodeAt(MessageType clipped, String[] path) {
        Type t = clipped;
        for (String s : path) {
            if (t.isPrimitive() || !t.asGroupType().containsField(s)) {
                return null;
            }
            t = t.asGroupType().getType(s);
        }
        return t;
    }

    /** The path down to the one repeated node on a leaf's path. */
    private static String[] repeatedPath(MessageType clipped, String[] leaf) {
        Type t = clipped;
        for (int i = 0; i < leaf.length; i++) {
            t = t.asGroupType().getType(leaf[i]);
            if (t.isRepetition(Type.Repetition.REPEATED)) {
                return java.util.Arrays.copyOf(leaf, i + 1);
            }
        }
        throw new IllegalStateException("no repeated node on " + String.join(".", leaf));
    }

    /**
     * Checks the clipped shape of the top-level list or map {@code top} of
     * Spark type {@code type} and calls {@code leaf} with each leaf's
     * descriptor and Spark type. False when the reader cannot follow it: not
     * one leaf (an array) or two (a map, key first), or a leaf repeated other
     * than once, or a map whose key and value sit under different repeated
     * groups. The file then goes to Spark's reader.
     */
    public static boolean forEachLeaf(MessageType clipped, String top, DataType type,
            BiConsumer<ColumnDescriptor, DataType> leaf) {
        return forEachLeaf(clipped, new String[] {top}, type, leaf);
    }

    /**
     * {@link #forEachLeaf(MessageType, String, DataType, BiConsumer)} for the
     * list or map at {@code path}.
     */
    public static boolean forEachLeaf(MessageType clipped, String[] path, DataType type,
            BiConsumer<ColumnDescriptor, DataType> leaf) {
        return forEachLeaf(clipped, null, path, type, leaf);
    }

    /**
     * {@link #forEachLeaf(MessageType, String[], DataType, BiConsumer)} over a
     * file whose schema is {@code file} (null: every field is in it): a list or
     * map with a struct field the file lacks is the tree reader's shape.
     */
    public static boolean forEachLeaf(MessageType clipped, MessageType file, String[] path,
            DataType type, BiConsumer<ColumnDescriptor, DataType> leaf) {
        if (NativeParquetTreeReader.needed(type) || lacksSome(clipped, file, path)) {
            return NativeParquetTreeReader.forEachLeaf(clipped, file, path, type, leaf);
        }
        if (NativeParquetNestedListReader.arrayDepth(type) > 1) {
            return NativeParquetNestedListReader.forEachLeaf(clipped, path, (ArrayType) type, leaf);
        }
        Type node = nodeAt(clipped, path);
        if (node == null) {
            return false;
        }
        if (node.isPrimitive() && !node.isRepetition(Type.Repetition.REPEATED)) {
            return false;
        }
        List<ColumnDescriptor> leaves = leavesUnder(clipped, path);
        for (ColumnDescriptor cd : leaves) {
            if (cd.getMaxRepetitionLevel() != 1) {
                return false;
            }
        }
        if (type instanceof ArrayType at && at.elementType() instanceof StructType st) {
            // An array of structs of leaves: one leaf per field, in order, all under the same struct group.
            if (leaves.size() != st.fields().length) {
                return false;
            }
            String[] parent = null;
            for (int i = 0; i < leaves.size(); i++) {
                String[] p = leaves.get(i).getPath();
                String[] up = java.util.Arrays.copyOf(p, p.length - 1);
                if (parent != null && !java.util.Arrays.equals(parent, up)) {
                    return false;
                }
                parent = up;
            }
            String[] rep = repeatedPath(clipped, leaves.get(0)
                    .getPath());
            if (parent.length != rep.length && parent.length != rep.length + 1) {
                return false;
            }
            for (int i = 0; i < leaves.size(); i++) {
                leaf.accept(leaves.get(i), st.fields()[i].dataType());
            }
            return true;
        }
        if (type instanceof ArrayType at) {
            if (leaves.size() != 1) {
                return false;
            }
            leaf.accept(leaves.get(0), at.elementType());
            return true;
        }
        if (type instanceof MapType mt && mt.valueType() instanceof StructType st) {
            // A map of structs of leaves: the key under the repeated group, then one leaf per value field, in
            // order, all under the same value group directly below it.
            if (leaves.size() != st.fields().length + 1) {
                return false;
            }
            String[] rep = repeatedPath(clipped, leaves.get(0)
                    .getPath());
            if (leaves.get(0).getPath().length != rep.length + 1) {
                return false;
            }
            for (int i = 0; i < st.fields().length; i++) {
                String[] p = leaves.get(i + 1).getPath();
                if (p.length != rep.length + 2 || !java.util.Arrays.equals(java.util.Arrays.copyOf(p, rep.length), rep) || !p[rep.length].equals(leaves.get(1).getPath()[rep.length])) {
                    return false;
                }
            }
            leaf.accept(leaves.get(0), mt.keyType());
            for (int i = 0; i < st.fields().length; i++) {
                leaf.accept(leaves.get(i + 1), st.fields()[i].dataType());
            }
            return true;
        }
        if (type instanceof MapType mt) {
            if (leaves.size() != 2) {
                return false;
            }
            String[] k = repeatedPath(clipped, leaves.get(0)
                    .getPath());
            String[] v = repeatedPath(clipped, leaves.get(1)
                    .getPath());
            if (!java.util.Arrays.equals(k, v) || leaves.get(0).getPath().length != k.length + 1) {
                return false;
            }
            leaf.accept(leaves.get(0), mt.keyType());
            leaf.accept(leaves.get(1), mt.valueType());
            return true;
        }
        return false;
    }

    @Override
    public ColumnVector wrap(Object batch) {
        ListColumnReader.Batch b = (ListColumnReader.Batch) batch;
        if (fields != null && type instanceof MapType mt) {
            StructType st = (StructType) mt.valueType();
            ColumnVector[] kids = new ColumnVector[fields.length - 1];
            for (int i = 1; i < fields.length; i++) {
                kids[i - 1] = fields[i].wrap(b.fs[i]);
            }
            return VectorNestedColumnVector.map(mt, b.rows, b.nulls, b.offsets, fields[0].wrap(b.fs[0]),
                    VectorNestedColumnVector.struct(st, b.elements, b.elementNulls, kids));
        }
        if (type instanceof ArrayType at) {
            if (fields != null) {
                StructType st = (StructType) at.elementType();
                ColumnVector[] kids = new ColumnVector[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    kids[i] = fields[i].wrap(b.fs[i]);
                }
                return VectorNestedColumnVector.list(at, b.rows, b.nulls, b.offsets, VectorNestedColumnVector.struct(st, b.elements, b.elementNulls, kids));
            }
            return VectorNestedColumnVector.list(at, b.rows, b.nulls, b.offsets, first.wrap(b.a));
        }
        return VectorNestedColumnVector.map((MapType) type, b.rows, b.nulls, b.offsets,
                first.wrap(b.a), second.wrap(b.b));
    }
}
