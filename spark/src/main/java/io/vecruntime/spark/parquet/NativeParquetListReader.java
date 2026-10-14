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

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import io.vecruntime.spark.arrow.VectorNestedColumnVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
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
public final class NativeParquetListReader implements NativeParquetColumn {

    private final DataType type;
    private final NativeParquetColumnReader first; // the element, or the key
    private final NativeParquetColumnReader second; // the value, or null
    private final int listDef; // the list is present at this level (0: never null)
    private final int elementDef; // the entry is an element at this level
    // An array of structs of leaves: one reader per field, in the struct's order (fields[0] is `first`), and the
    // level at which an element's struct is present (elementDef when the repeated group is the struct itself).
    private final NativeParquetColumnReader[] fields;
    private final int structDef;

    private NativeParquetListReader(DataType type, NativeParquetColumnReader first, NativeParquetColumnReader second,
            int listDef, int elementDef) {
        this.type = type;
        this.first = first;
        this.second = second;
        this.listDef = listDef;
        this.elementDef = elementDef;
        this.fields = null;
        this.structDef = elementDef;
        first.setDictionaryIds(false);
        if (second != null) {
            second.setDictionaryIds(false);
        }
    }

    private NativeParquetListReader(ArrayType type, NativeParquetColumnReader[] fields, int listDef,
            int elementDef, int structDef) {
        this.type = type;
        this.first = fields[0];
        this.second = null;
        this.listDef = listDef;
        this.elementDef = elementDef;
        this.fields = fields;
        this.structDef = structDef;
        for (NativeParquetColumnReader f : fields) {
            f.setDictionaryIds(false);
        }
    }

    /**
     * The reader of the top-level list or map {@code top} (its name in the
     * clipped schema) of Spark type {@code type}; {@link #forEachLeaf} must
     * have accepted it.
     */
    public static NativeParquetColumn create(MessageType clipped, String top, DataType type,
            String name, int batchRows, BufferAllocator allocator) {
        if (NativeParquetNestedListReader.arrayDepth(type) > 1) {
            // #568: a list of lists, any depth, over one leaf.
            return NativeParquetNestedListReader.create(clipped, top, (ArrayType) type, name, batchRows,
                    allocator);
        }
        List<ColumnDescriptor> leaves = leavesUnder(clipped, top);
        ColumnDescriptor a = leaves.get(0);
        String[] rep = repeatedPath(clipped, a.getPath());
        int elementDef = clipped.getMaxDefinitionLevel(rep);
        int listDef = rep.length == 1 ? 0 : clipped.getMaxDefinitionLevel(java.util.Arrays.copyOf(rep, rep.length - 1));
        if (type instanceof ArrayType at) {
            DataType et = at.elementType();
            if (et instanceof StructType st) {
                // #568: an array of structs of leaves. Each field's leaf, matched by name in the struct's order.
                NativeParquetColumnReader[] readers = new NativeParquetColumnReader[st.fields().length];
                for (int i = 0; i < readers.length; i++) {
                    StructField f = st.fields()[i];
                    ColumnDescriptor cd = fieldLeaf(leaves, f.name());
                    readers[i] = new NativeParquetColumnReader(cd, NativeParquetSupport.decodeLane(f.dataType()), f.dataType(),
                            name + ".element." + f.name(), batchRows, allocator);
                }
                String[] leaf0 = fieldLeaf(leaves, st.fields()[0].name()).getPath();
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
     * The leaf of struct field {@code field} among an array of structs' leaves
     * (its last path step).
     */
    private static ColumnDescriptor fieldLeaf(List<ColumnDescriptor> leaves, String field) {
        for (ColumnDescriptor cd : leaves) {
            String[] p = cd.getPath();
            if (p[p.length - 1].equalsIgnoreCase(field)) {
                return cd;
            }
        }
        throw new IllegalStateException("struct field " + field + " missing from the clipped list");
    }

    /** The leaf columns under the top-level field {@code top}, in schema order. */
    private static List<ColumnDescriptor> leavesUnder(MessageType clipped, String top) {
        List<ColumnDescriptor> out = new ArrayList<>();
        for (ColumnDescriptor cd : clipped.getColumns()) {
            if (cd.getPath()[0].equals(top)) {
                out.add(cd);
            }
        }
        return out;
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
        if (NativeParquetNestedListReader.arrayDepth(type) > 1) {
            return NativeParquetNestedListReader.forEachLeaf(clipped, top, (ArrayType) type, leaf);
        }
        if (!clipped.containsField(top)) {
            return false;
        }
        Type node = clipped.getType(top);
        if (node.isPrimitive() && !node.isRepetition(Type.Repetition.REPEATED)) {
            return false;
        }
        List<ColumnDescriptor> leaves = leavesUnder(clipped, top);
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
                if (!p[p.length - 1].equalsIgnoreCase(st.fields()[i].name())) {
                    return false;
                }
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

    /**
     * A list batch: its rows, null rows ({@code null}: none), offsets and each
     * leaf's elements.
     */
    private static final class Batch {
        final int rows;
        final boolean[] nulls;
        final int[] offsets;
        final FieldVector a;
        final FieldVector b;
        final FieldVector[] fs; // an array of structs: each field's elements
        final boolean[] elementNulls; // and its null structs (null: none)
        final int elements;

        Batch(int rows, boolean[] nulls, int[] offsets,
              FieldVector a, FieldVector b) {
            this.rows = rows;
            this.nulls = nulls;
            this.offsets = offsets;
            this.a = a;
            this.b = b;
            this.fs = null;
            this.elementNulls = null;
            this.elements = 0;
        }

        Batch(int rows, boolean[] nulls, int[] offsets,
              FieldVector[] fs, boolean[] elementNulls, int elements) {
            this.rows = rows;
            this.nulls = nulls;
            this.offsets = offsets;
            this.a = null;
            this.b = null;
            this.fs = fs;
            this.elementNulls = elementNulls;
            this.elements = elements;
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        first.startRowGroup(store, rows);
        if (second != null) {
            second.startRowGroup(store, rows);
        }
        for (NativeParquetColumnReader f : others()) {
            f.startRowGroup(store, rows);
        }
    }

    @Override
    public Object readBatch(int n) {
        int entries = first.entriesForRows(n);
        int[] r = first.repLevels();
        int[] d = first.defLevels();
        int off = first.levelOffset();
        int[] offsets = new int[n + 1];
        boolean[] nulls = null;
        int row = -1;
        int count = 0;
        boolean[] elementNulls = null;
        for (int k = 0; k < entries; k++) {
            int def = d[off + k];
            if (r[off + k] == 0) {
                row++;
                offsets[row] = count;
                if (def < listDef) {
                    if (nulls == null) {
                        nulls = new boolean[n];
                    }
                    nulls[row] = true;
                }
            }
            if (def >= elementDef) {
                if (fields != null && def < structDef) {
                    if (elementNulls == null) {
                        elementNulls = new boolean[entries];
                    }
                    elementNulls[count] = true;
                }
                count++;
            }
        }
        if (row != n - 1) {
            throw new IllegalStateException("a list column ended at row " + (row + 1) + " of " + n);
        }
        offsets[n] = count;
        if (fields != null) {
            FieldVector[] fs = new FieldVector[fields.length];
            try {
                fs[0] = first.readElements(n, entries, elementDef);
                for (int i = 1; i < fields.length; i++) {
                    fs[i] = fields[i].readElements(n, fields[i].entriesForRows(n), elementDef);
                }
            } catch (RuntimeException | Error e) {
                for (int i = 0; i < fs.length; i++) {
                    fields[i].release(fs[i]);
                }
                throw e;
            }
            return new Batch(n, nulls, offsets, fs, elementNulls, count);
        }
        FieldVector a = first.readElements(n, entries, elementDef);
        FieldVector b = null;
        if (second != null) {
            try {
                b = second.readElements(n, second.entriesForRows(n), elementDef);
            } catch (RuntimeException | Error e) {
                first.release(a);
                throw e;
            }
        }
        return new Batch(n, nulls, offsets, a, b);
    }

    @Override
    public Object readBatchSelected(int n, MemorySegment selection) {
        return readBatch(n);
    }

    @Override
    public boolean decodesSelectedFaster() {
        return false;
    }

    @Override
    public void skipRows(int n) {
        first.skipRows(n);
        if (second != null) {
            second.skipRows(n);
        }
        for (NativeParquetColumnReader f : others()) {
            f.skipRows(n);
        }
    }

    @Override
    public ColumnVector wrap(Object batch) {
        Batch b = (Batch) batch;
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

    @Override
    public void release(Object batch) {
        if (batch != null) {
            Batch b = (Batch) batch;
            if (fields != null) {
                for (int i = 0; i < fields.length; i++) {
                    fields[i].release(b.fs[i]);
                }
                return;
            }
            first.release(b.a);
            if (second != null) {
                second.release(b.b);
            }
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        first.setPoolLimit(limit);
        if (second != null) {
            second.setPoolLimit(limit);
        }
        for (NativeParquetColumnReader f : others()) {
            f.setPoolLimit(limit);
        }
    }

    /**
     * Off: a list's strings are read by value through its elements, not as
     * dictionary ids.
     */
    @Override
    public void setDictionaryIds(boolean enabled) {
        first.setDictionaryIds(false);
        if (second != null) {
            second.setDictionaryIds(false);
        }
    }

    @Override
    public int rowsRemaining() {
        return first.rowsRemaining();
    }

    @Override
    public void close() {
        first.close();
        if (second != null) {
            second.close();
        }
        for (NativeParquetColumnReader f : others()) {
            f.close();
        }
    }

    /** An array of structs' readers after the first; none otherwise. */
    private NativeParquetColumnReader[] others() {
        return fields == null ? new NativeParquetColumnReader[0] : java.util.Arrays.copyOfRange(fields, 1, fields.length);
    }
}
