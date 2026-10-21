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
package io.vecruntime.parquet.read;

import java.lang.foreign.MemorySegment;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;

/**
 * A list or map column with any nesting below it (#568; Spark-free since #900):
 * lists, maps and structs mixed at any depth over flat leaves. Every leaf of a
 * column chunk is a stream of entries, each with a repetition and a definition
 * level, and every leaf under a node encodes that node's structure identically
 * (Dremel). So each node computes its instances, nulls and offsets from the
 * levels of the first leaf below it:
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
 *       LeafColumnReader#readElements}).
 * </ul>
 *
 * The engine's adapter builds the node tree from the clipped Parquet schema and
 * its own type (each node carries that type as an opaque {@link Node#type}) and
 * wraps a batch's {@link Res} tree as its nested column.
 */
public class TreeColumnReader implements ColumnReader, RowLevels {

    /** A node of the tree; {@code type} is the engine's type for it, opaque here. */
    public abstract static class Node {
        public Object type;
        public int depth; // enclosing lists
        public int existDef; // the innermost enclosing list's element level (0 at the top)
        public Leaf rep; // the first leaf below: its levels give this node's structure
    }

    /** A list, or a map ({@code value} non-null). */
    public static final class ListNode extends Node {
        public int listDef;
        public int elemDef;
        public Node element; // or a map's key
        public Node value; // a map's value, else null
    }

    public static final class StructNode extends Node {
        public int structDef;
        public Node[] fields;
    }

    public static final class Leaf extends Node {
        public ColumnDescriptor column;
        public LeafColumnReader reader;
        int entries; // this batch
    }

    /**
     * A struct field the file does not hold (#568 schema evolution): null in
     * every instance of its struct.
     */
    public static final class Missing extends Node {}

    /** One node's result in a batch. */
    public static final class Res {
        public int count;
        public boolean[] nulls;
        public int[] offsets;
        public Res[] kids;
        public FieldVector vector;
    }

    private final ListNode root;
    private final List<Leaf> leaves;
    private int[] rowLevels = new int[0];

    /**
     * The tree under {@code root}; {@code leaves} are its leaves in order, each
     * with its reader set.
     */
    public TreeColumnReader(ListNode root, List<Leaf> leaves) {
        this.root = root;
        this.leaves = leaves;
        for (Leaf l : leaves) {
            l.reader.setDictionaryIds(false);
        }
    }

    /** The root node, for the adapter's wrap. */
    protected ListNode root() {
        return root;
    }

    /** A batch's result tree (the root's {@link Res}). */
    public static Res result(Object batch) {
        return ((Batch) batch).root;
    }

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

    @Override
    public void setReuseDecompress(boolean reuse) {
        for (Leaf l : leaves) {
            l.reader.setReuseDecompress(reuse);
        }
    }

    /**
     * The file's calendar rebase and INT96 conversion, for every leaf (#568
     * item 6).
     */
    @Override
    public void setRebase(io.vecruntime.parquet.read.CalendarRebase rebase) {
        for (Leaf l : leaves) {
            l.reader.setRebase(rebase);
        }
    }

    @Override
    public void setDictionaryIds(boolean enabled) {
        for (Leaf l : leaves) {
            l.reader.setDictionaryIds(enabled); // #568: every string leaf as dictionary ids (#612)
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
