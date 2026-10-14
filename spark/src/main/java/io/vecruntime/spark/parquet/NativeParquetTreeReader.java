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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A list or map column of the native Parquet scan with any nesting below it
 * (#568): lists, maps and structs mixed at any depth over flat leaves -- a list
 * of structs holding a list, a map of structs holding a map, a list of lists of
 * structs, and so on. The shapes {@link NativeParquetListReader} and {@link
 * NativeParquetNestedListReader} read stay with them; this reader takes the
 * rest.
 *
 * <p>Every leaf of a column chunk is a stream of entries, each with a
 * repetition and a definition level, and every leaf under a node encodes that
 * node's structure identically (Dremel). So each node computes its instances,
 * nulls and offsets from the levels of the first leaf below it:
 *
 * <ul>
 *   <li>A node inside {@code j} enclosing lists exists at an entry whose
 *       repetition level is at most {@code j} and whose definition level
 *       reaches the innermost enclosing list's element level ({@code existDef};
 *       0 at the top, where every row is an instance).
 *   <li>A list or map node at repetition depth {@code k = j + 1} is null below
 *       {@code listDef}, and has an element at an entry whose repetition level
 *       is at most {@code k} and whose definition level reaches {@code
 *       elemDef}.
 *   <li>A struct node is null below its own definition level.
 *   <li>A leaf decodes its elements back to back at {@code existDef} ({@link
 *       NativeParquetColumnReader#readElements}).
 * </ul>
 *
 * The node tree is built from the clipped Parquet schema and the Spark type
 * together: Spark's three-level {@code LIST}, the legacy two-level forms
 * (Spark's {@code isElementType} rule), a bare repeated field, and {@code MAP}
 * groups.
 */
public final class NativeParquetTreeReader implements NativeParquetColumn, RowLevels {

    private abstract static class Node {
        DataType type;
        int depth; // enclosing lists
        int existDef; // the innermost enclosing list's element level (0 at the top)
        Leaf rep; // the first leaf below: its levels give this node's structure
    }

    private static final class ListNode extends Node {
        int listDef;
        int elemDef;
        Node element; // or a map's key
        Node value; // a map's value, else null
    }

    private static final class StructNode extends Node {
        int structDef;
        Node[] fields;
    }

    private static final class Leaf extends Node {
        ColumnDescriptor column;
        NativeParquetColumnReader reader;
        int entries; // this batch
    }

    /**
     * A struct field the file does not hold (#568 schema evolution): null in
     * every instance of its struct, as Spark's reader makes it.
     */
    private static final class Missing extends Node {}

    /** One node's result in a batch. */
    private static final class Res {
        int count;
        boolean[] nulls;
        int[] offsets;
        Res[] kids;
        FieldVector vector;
    }

    private final ListNode root;
    private final List<Leaf> leaves;
    private int[] rowLevels = new int[0];

    private NativeParquetTreeReader(ListNode root, List<Leaf> leaves) {
        this.root = root;
        this.leaves = leaves;
        for (Leaf l : leaves) {
            l.reader.setDictionaryIds(false);
        }
    }

    // ----- building the node tree from the clipped schema and the Spark type -----

    /**
     * Whether a list or map type needs this reader: some nesting the other list
     * readers do not follow.
     */
    static boolean needed(DataType dt) {
        if (dt instanceof ArrayType at) {
            DataType e = at.elementType();
            if (isLeaf(e)) {
                return false;
            }
            if (e instanceof ArrayType) {
                DataType i = e;
                while (i instanceof ArrayType a) {
                    i = a.elementType();
                }
                return !isLeaf(i);
            }
            if (e instanceof StructType st) {
                return !allLeaves(st);
            }
            return true;
        }
        if (dt instanceof MapType mt) {
            if (!isLeaf(mt.keyType())) {
                return true;
            }
            DataType v = mt.valueType();
            return !(isLeaf(v) || (v instanceof StructType st && allLeaves(st)));
        }
        return false;
    }

    private static boolean isLeaf(DataType dt) {
        return !(dt instanceof ArrayType || dt instanceof MapType || dt instanceof StructType);
    }

    private static boolean allLeaves(StructType st) {
        for (StructField f : st.fields()) {
            if (!isLeaf(f.dataType())) {
                return false;
            }
        }
        return st.fields().length > 0;
    }

    private static String[] child(String[] path, String name) {
        String[] p = java.util.Arrays.copyOf(path, path.length + 1);
        p[path.length] = name;
        return p;
    }

    /**
     * Spark's rule for a list's repeated child being the element itself (the
     * legacy two-level forms).
     */
    private static boolean isElementType(Type repeated, String parentName) {
        return repeated.isPrimitive()
                || repeated.asGroupType().getFieldCount() > 1
                || repeated.getName().equals("array")
                || repeated.getName().equals(parentName + "_tuple");
    }

    /**
     * Builds the node for Spark type {@code dt} at Parquet node {@code t}
     * ({@code path}); null if unfollowable.
     */
    private static Node build(
            MessageType clipped,
            MessageType file,
            String[] path,
            Type t,
            DataType dt,
            int depth,
            int existDef,
            boolean asElement,
            List<Leaf> leaves) {
        if (!asElement && t.isRepetition(Type.Repetition.REPEATED) && !(dt instanceof ArrayType)) {
            return null;
        }
        if (dt instanceof ArrayType at) {
            ListNode n = new ListNode();
            Type elementType;
            String[] elementPath;
            boolean elementAsElement = true;
            if (!asElement && t.isRepetition(Type.Repetition.REPEATED)) {
                // A bare repeated field: the field is the list's repeated node and its element.
                n.listDef = path.length == 1 ? 0 : clipped.getMaxDefinitionLevel(java.util.Arrays.copyOf(path, path.length - 1));
                n.elemDef = clipped.getMaxDefinitionLevel(path);
                elementType = t;
                elementPath = path;
            } else {
                if (t.isPrimitive() || t.asGroupType().getFieldCount() != 1) {
                    return null;
                }
                Type r = t.asGroupType().getType(0);
                if (!r.isRepetition(Type.Repetition.REPEATED)) {
                    return null;
                }
                String[] rp = child(path, r.getName());
                n.listDef = clipped.getMaxDefinitionLevel(path);
                n.elemDef = clipped.getMaxDefinitionLevel(rp);
                if (isElementType(r, t.getName())) {
                    elementType = r;
                    elementPath = rp;
                } else {
                    if (r.asGroupType().getFieldCount() != 1) {
                        return null;
                    }
                    elementType = r.asGroupType().getType(0);
                    elementPath = child(rp, elementType.getName());
                    elementAsElement = false;
                }
            }
            n.type = dt;
            n.depth = depth;
            n.existDef = existDef;
            n.element = build(clipped, file, elementPath, elementType, at.elementType(),
                    depth + 1, n.elemDef, elementAsElement, leaves);
            if (n.element == null) {
                return null;
            }
            n.rep = n.element.rep;
            return n;
        }
        if (dt instanceof MapType mt) {
            if (t.isPrimitive() || t.asGroupType().getFieldCount() != 1) {
                return null;
            }
            Type r = t.asGroupType().getType(0);
            if (!r.isRepetition(Type.Repetition.REPEATED) || r.isPrimitive() || r.asGroupType().getFieldCount() != 2) {
                return null;
            }
            String[] rp = child(path, r.getName());
            ListNode n = new ListNode();
            n.type = dt;
            n.depth = depth;
            n.existDef = existDef;
            n.listDef = clipped.getMaxDefinitionLevel(path);
            n.elemDef = clipped.getMaxDefinitionLevel(rp);
            GroupType kv = r.asGroupType();
            Type k = kv.getType(0);
            Type v = kv.getType(1);
            n.element = build(
                    clipped,
                    file,
                    child(rp, k.getName()),
                    k,
                    mt.keyType(),
                    depth + 1,
                    n.elemDef,
                    false,
                    leaves);
            if (n.element == null) {
                return null;
            }
            n.value = build(
                    clipped,
                    file,
                    child(rp, v.getName()),
                    v,
                    mt.valueType(),
                    depth + 1,
                    n.elemDef,
                    false,
                    leaves);
            if (n.value == null) {
                return null;
            }
            n.rep = n.element.rep;
            return n;
        }
        if (dt instanceof StructType st) {
            if (t.isPrimitive()) {
                return null;
            }
            GroupType g = t.asGroupType();
            StructField[] fs = st.fields();
            if (fs.length == 0 || g.getFieldCount() != fs.length) {
                return null;
            }
            StructNode n = new StructNode();
            n.type = dt;
            n.depth = depth;
            n.existDef = existDef;
            n.structDef = clipped.getMaxDefinitionLevel(path);
            n.fields = new Node[fs.length];
            for (int i = 0; i < fs.length; i++) {
                // By position: the clipped schema keeps the requested order (under field-id matching a field keeps
                // the file's name).
                Type ct = g.getType(i);
                String[] cp = child(path, ct.getName());
                if (file != null && !NativeParquetStructReader.holds(file, cp)) {
                    Missing m = new Missing();
                    m.type = fs[i].dataType();
                    m.depth = depth;
                    m.existDef = existDef;
                    n.fields[i] = m;
                    continue;
                }
                n.fields[i] = build(clipped, file, cp, ct, fs[i].dataType(),
                        depth, existDef, false, leaves);
                if (n.fields[i] == null) {
                    return null;
                }
                if (n.rep == null) {
                    n.rep = n.fields[i].rep;
                }
            }
            // The struct's structure comes from a field the file holds; holding none, it is not followable.
            return n.rep == null ? null : n;
        }
        if (!t.isPrimitive()) {
            return null;
        }
        ColumnDescriptor cd = clipped.getColumnDescription(path);
        if (cd.getMaxRepetitionLevel() != depth || depth == 0) {
            return null;
        }
        Leaf l = new Leaf();
        l.type = dt;
        l.depth = depth;
        l.existDef = existDef;
        l.column = cd;
        l.rep = l;
        leaves.add(l);
        return l;
    }

    private static ListNode buildRoot(MessageType clipped, MessageType file, String[] path,
            DataType type, List<Leaf> leaves) {
        Type t = NativeParquetListReader.nodeAt(clipped, path);
        if (t == null) {
            return null;
        }
        Node n = build(clipped, file, path, t, type, 0,
                0, false, leaves);
        return n instanceof ListNode ln ? ln : null;
    }

    /**
     * Checks the shape and calls {@code leaf} for each leaf; false when the
     * reader cannot follow it. {@code file}: the file's schema, whose missing
     * struct fields are null (null: every field is in it).
     */
    public static boolean forEachLeaf(MessageType clipped, MessageType file, String[] path,
            DataType type, BiConsumer<ColumnDescriptor, DataType> leaf) {
        List<Leaf> leaves = new ArrayList<>();
        if (buildRoot(clipped, file, path, type, leaves) == null) {
            return false;
        }
        for (Leaf l : leaves) {
            leaf.accept(l.column, l.type);
        }
        return true;
    }

    /**
     * The reader of the list or map at {@code path}; {@link #forEachLeaf} must
     * have accepted it.
     */
    public static NativeParquetTreeReader create(
            MessageType clipped,
            MessageType file,
            String[] path,
            DataType type,
            String name,
            int batchRows,
            BufferAllocator allocator) {
        List<Leaf> leaves = new ArrayList<>();
        ListNode root = buildRoot(clipped, file, path, type, leaves);
        if (root == null) {
            throw new IllegalStateException("unfollowable nested column " + String.join(".", path));
        }
        int i = 0;
        for (Leaf l : leaves) {
            l.reader = new NativeParquetColumnReader(l.column, NativeParquetSupport.decodeLane(l.type), l.type, name + ".leaf" + i++,
                    batchRows, allocator);
        }
        return new NativeParquetTreeReader(root, leaves);
    }

    // ----- a batch -----

    /** A batch: the root's result, and the leaves' vectors to release. */
    private static final class Batch {
        final Res root;
        final FieldVector[] vectors;

        Batch(Res root, FieldVector[] vectors) {
            this.root = root;
            this.vectors = vectors;
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        for (Leaf l : leaves) {
            l.reader.startRowGroup(store, rows);
        }
    }

    @Override
    public Object readBatch(int n) {
        for (Leaf l : leaves) {
            l.entries = l.reader.entriesForRows(n);
        }
        if (rowLevels.length < n) {
            rowLevels = new int[n];
        }
        // Structure first, from the leaves' levels (each leaf's levels stay put until it decodes).
        Res res = structure(root, n);
        FieldVector[] vectors = new FieldVector[leaves.size()];
        try {
            for (int i = 0; i < vectors.length; i++) {
                Leaf l = leaves.get(i);
                vectors[i] = l.reader.readElements(n, l.entries, l.existDef);
            }
        } catch (RuntimeException | Error e) {
            for (int i = 0; i < vectors.length; i++) {
                if (vectors[i] != null) {
                    leaves.get(i).reader.release(vectors[i]);
                }
            }
            throw e;
        }
        attach(root, res, vectors);
        return new Batch(res, vectors);
    }

    /**
     * A node's instances, nulls and offsets from its first leaf's levels; its
     * children's in turn.
     */
    private Res structure(Node node, int rows) {
        Res res = new Res();
        if (node instanceof Leaf) {
            return res;
        }
        Leaf rl = node.rep;
        int[] r = rl.reader.repLevels();
        int[] d = rl.reader.defLevels();
        int off = rl.reader.levelOffset();
        int e = rl.entries;
        int j = node.depth;
        if (node instanceof ListNode ln) {
            int k = j + 1;
            int[] offsets = new int[(j == 0 ? rows : e) + 1];
            boolean[] nulls = null;
            int inst = 0;
            int elems = 0;
            for (int x = 0; x < e; x++) {
                int rep = r[off + x];
                int def = d[off + x];
                if (rep <= j && def >= node.existDef) {
                    if (j == 0) {
                        rowLevels[inst] = def;
                    }
                    offsets[inst] = elems;
                    if (def < ln.listDef) {
                        if (nulls == null) {
                            nulls = new boolean[offsets.length];
                        }
                        nulls[inst] = true;
                    }
                    inst++;
                }
                if (rep <= k && def >= ln.elemDef) {
                    elems++;
                }
            }
            if (j == 0 && inst != rows) {
                throw new IllegalStateException("a list column ended at row " + inst + " of " + rows);
            }
            offsets[inst] = elems;
            res.count = inst;
            res.nulls = nulls;
            res.offsets = offsets;
            res.kids = ln.value == null ? new Res[] {structure(ln.element, rows)} : new Res[] {structure(ln.element, rows), structure(ln.value, rows)};
            return res;
        }
        StructNode sn = (StructNode) node;
        boolean[] nulls = null;
        int inst = 0;
        for (int x = 0; x < e; x++) {
            int def = d[off + x];
            if (r[off + x] <= j && def >= node.existDef) {
                if (def < sn.structDef) {
                    if (nulls == null) {
                        nulls = new boolean[e];
                    }
                    nulls[inst] = true;
                }
                inst++;
            }
        }
        res.count = inst;
        res.nulls = nulls;
        res.kids = new Res[sn.fields.length];
        for (int i = 0; i < sn.fields.length; i++) {
            if (sn.fields[i] instanceof Missing) {
                Res m = new Res();
                m.count = inst;
                res.kids[i] = m;
                continue;
            }
            res.kids[i] = structure(sn.fields[i], rows);
        }
        return res;
    }

    /** Puts each leaf's decoded vector into its result. */
    private void attach(Node node, Res res, FieldVector[] vectors) {
        if (node instanceof Missing) {
            return;
        }
        if (node instanceof Leaf l) {
            res.vector = vectors[leaves.indexOf(l)];
            res.count = res.vector.getValueCount();
            return;
        }
        if (node instanceof ListNode ln) {
            attach(ln.element, res.kids[0], vectors);
            if (ln.value != null) {
                attach(ln.value, res.kids[1], vectors);
            }
            return;
        }
        StructNode sn = (StructNode) node;
        for (int i = 0; i < sn.fields.length; i++) {
            attach(sn.fields[i], res.kids[i], vectors);
        }
    }

    private ColumnVector wrap(Node node, Res res) {
        if (node instanceof Missing) {
            // Spark's own all-null vector (setMissing), as its reader makes a missing column; on the heap, so
            // nothing to release with the batch.
            org.apache.spark.sql.execution.vectorized.OnHeapColumnVector v = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector(Math.max(1, res.count), node.type);
            v.setMissing();
            return v;
        }
        if (node instanceof Leaf l) {
            return l.reader.wrap(res.vector);
        }
        if (node instanceof ListNode ln) {
            ColumnVector e = wrap(ln.element, res.kids[0]);
            if (ln.value == null) {
                return VectorNestedColumnVector.list((ArrayType) ln.type, res.count, res.nulls, res.offsets,
                        e);
            }
            return VectorNestedColumnVector.map((MapType) ln.type, res.count, res.nulls, res.offsets,
                    e, wrap(ln.value, res.kids[1]));
        }
        StructNode sn = (StructNode) node;
        ColumnVector[] kids = new ColumnVector[sn.fields.length];
        for (int i = 0; i < kids.length; i++) {
            kids[i] = wrap(sn.fields[i], res.kids[i]);
        }
        return VectorNestedColumnVector.struct((StructType) sn.type, res.count, res.nulls, kids);
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
        for (Leaf l : leaves) {
            l.reader.skipRows(n);
        }
    }

    @Override
    public ColumnVector wrap(Object batch) {
        return wrap(root, ((Batch) batch).root);
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            FieldVector[] vs = ((Batch) batch).vectors;
            for (int i = 0; i < vs.length; i++) {
                if (vs[i] != null) {
                    leaves.get(i).reader.release(vs[i]);
                }
            }
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        for (Leaf l : leaves) {
            l.reader.setPoolLimit(limit);
        }
    }

    /**
     * The file's calendar rebase and INT96 conversion, for every leaf (#568
     * item 6).
     */
    @Override
    public void setRebase(ParquetRebase rebase) {
        for (Leaf l : leaves) {
            l.reader.setRebase(rebase);
        }
    }

    @Override
    public void setDictionaryIds(boolean enabled) {
        for (Leaf l : leaves) {
            l.reader.setDictionaryIds(false);
        }
    }

    @Override
    public int rowsRemaining() {
        return leaves.get(0).reader.rowsRemaining();
    }

    @Override
    public void close() {
        for (Leaf l : leaves) {
            l.reader.close();
        }
    }

    @Override
    public int[] rowLevels() {
        return rowLevels;
    }

    /** Always kept: one store per row while the root's offsets are built. */
    @Override
    public void recordRowLevels() {}
}
