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
public final class NativeParquetListReader implements NativeParquetColumn, RowLevels {

    private final DataType type;
    private final NativeParquetColumnReader first; // the element, or the key
    private final NativeParquetColumnReader second; // the value, or null
    private final int listDef; // the list is present at this level (0: never null)
    private final int elementDef; // the entry is an element at this level
    // An array of structs of leaves: one reader per field, in the struct's order (fields[0] is `first`), and the
    // level at which an element's struct is present (elementDef when the repeated group is the struct itself).
    // A map of structs (#568): fields[0] is the key, fields[1..] the value struct's fields, and structDef the level
    // at which an entry's value struct is present.
    private final NativeParquetColumnReader[] fields;
    private final int structDef;
    private int[] rowLevels = new int[0]; // each row's first-entry level, last batch

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

    private NativeParquetListReader(DataType type, NativeParquetColumnReader[] fields, int listDef,
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
        return create(clipped, new String[] {top}, type, name, batchRows,
                allocator);
    }

    /**
     * The reader of the list or map at {@code path} (a top-level field, or a
     * struct's field, #568).
     */
    public static NativeParquetColumn create(MessageType clipped, String[] path, DataType type,
            String name, int batchRows, BufferAllocator allocator) {
        if (NativeParquetTreeReader.needed(type)) {
            // #568: nesting below the list or map this reader does not follow (a struct holding a list, ...).
            return NativeParquetTreeReader.create(clipped, path, type, name, batchRows, allocator);
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
        if (mt.valueType() instanceof StructType st) {
            // #568: a map of structs of leaves: the key, then each value field's leaf matched by name, in order.
            List<ColumnDescriptor> values = leaves.subList(1, leaves.size());
            NativeParquetColumnReader[] readers = new NativeParquetColumnReader[st.fields().length + 1];
            readers[0] = new NativeParquetColumnReader(a, NativeParquetSupport.decodeLane(mt.keyType()), mt.keyType(),
                    name + ".key", batchRows, allocator);
            for (int i = 0; i < st.fields().length; i++) {
                StructField f = st.fields()[i];
                ColumnDescriptor cd = fieldLeaf(values, f.name());
                readers[i + 1] = new NativeParquetColumnReader(cd, NativeParquetSupport.decodeLane(f.dataType()), f.dataType(),
                        name + ".value." + f.name(), batchRows, allocator);
            }
            String[] v0 = fieldLeaf(values, st.fields()[0].name()).getPath();
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
        if (NativeParquetTreeReader.needed(type)) {
            return NativeParquetTreeReader.forEachLeaf(clipped, path, type, leaf);
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
                if (p.length != rep.length + 2
                        || !java.util.Arrays.equals(java.util.Arrays.copyOf(p, rep.length), rep)
                        || !p[p.length - 1].equalsIgnoreCase(st.fields()[i].name())
                        || !p[rep.length].equals(leaves.get(1).getPath()[rep.length])) {
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
        if (rowLevels.length < n) {
            rowLevels = new int[n];
        }
        boolean[] nulls = null;
        int row = -1;
        int count = 0;
        boolean[] elementNulls = null;
        for (int k = 0; k < entries; k++) {
            int def = d[off + k];
            if (r[off + k] == 0) {
                row++;
                offsets[row] = count;
                rowLevels[row] = def;
                if (def < listDef) {
                    if (nulls == null) {
                        nulls = new boolean[n];
                    }
                    nulls[row] = true;
                }
            }
            if (def >= elementDef) {
                if (fields != null && !(type instanceof MapType) && def < structDef) {
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
                    int e = fields[i].entriesForRows(n);
                    if (i == 1 && type instanceof MapType) {
                        // A map's value struct is null where its first field's level is below the struct's; the
                        // key is required, so its own levels cannot tell.
                        elementNulls = valueNulls(fields[1], e, count);
                    }
                    fs[i] = fields[i].readElements(n, e, elementDef);
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

    /**
     * The null value structs among a map's next {@code entries} entries ({@code
     * null}: none).
     */
    private boolean[] valueNulls(NativeParquetColumnReader valueField, int entries, int elements) {
        int[] d = valueField.defLevels();
        int off = valueField.levelOffset();
        boolean[] out = null;
        int at = 0;
        for (int k = 0; k < entries; k++) {
            int def = d[off + k];
            if (def >= elementDef) {
                if (def < structDef) {
                    if (out == null) {
                        out = new boolean[elements];
                    }
                    out[at] = true;
                }
                at++;
            }
        }
        return out;
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

    @Override
    public void setRebase(ParquetRebase rebase) {
        first.setRebase(rebase);
        if (second != null) {
            second.setRebase(rebase);
        }
        for (NativeParquetColumnReader f : others()) {
            f.setRebase(rebase);
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

    @Override
    public int[] rowLevels() {
        return rowLevels;
    }

    /** Always kept: one store per row while the offsets are built. */
    @Override
    public void recordRowLevels() {}
}
