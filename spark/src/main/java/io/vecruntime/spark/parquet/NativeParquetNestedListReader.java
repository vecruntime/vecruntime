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
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A list of lists column of the native Parquet scan (#568): an {@code
 * array<array<...<leaf>>>} of depth {@code k >= 2} at the top of the schema,
 * over ONE flat leaf repeated {@code k} times. Each value slot of the leaf's
 * chunk is an entry with a repetition level {@code r} and a definition level
 * {@code d}. On the leaf's path the {@code j}-th repeated node {@code R_j}
 * (repetition level {@code j}) gives two thresholds: the depth-{@code j} list
 * is present at {@code listDef[j]} (its parent's maximum definition level) and
 * an entry is one of its elements at {@code elemDef[j]} ({@code R_j}'s own).
 *
 * <p>An entry opens a new depth-{@code j} list where {@code r <= j - 1} and the
 * list exists there (depth 1 always; deeper, where {@code d >= elemDef[j -
 * 1]}); the list is null below {@code listDef[j]}. It opens a new depth-{@code
 * j} element where {@code r <= j} and {@code d >= elemDef[j]}. One pass over
 * the leaf's levels builds every depth's offsets and nulls; the leaf decodes
 * its elements back to back ({@link NativeParquetColumnReader#readElements} at
 * {@code elemDef[k]}).
 */
public final class NativeParquetNestedListReader implements NativeParquetColumn {

    private final ArrayType type;
    private final NativeParquetColumnReader leaf;
    private final int depth;
    private final int[] listDef; // 1-based: listDef[j], elemDef[j] for j in 1..depth
    private final int[] elemDef;

    private NativeParquetNestedListReader(ArrayType type, NativeParquetColumnReader leaf, int[] listDef,
            int[] elemDef) {
        this.type = type;
        this.leaf = leaf;
        this.depth = listDef.length - 1;
        this.listDef = listDef;
        this.elemDef = elemDef;
        leaf.setDictionaryIds(false);
    }

    /**
     * The array nesting depth of {@code dt} (0: not an array) and its innermost
     * element type.
     */
    static int arrayDepth(DataType dt) {
        int k = 0;
        while (dt instanceof ArrayType at) {
            k++;
            dt = at.elementType();
        }
        return k;
    }

    private static DataType innermost(DataType dt) {
        while (dt instanceof ArrayType at) {
            dt = at.elementType();
        }
        return dt;
    }

    /**
     * The reader of the top-level list of lists {@code top}; {@link
     * #forEachLeaf} must have accepted it.
     */
    public static NativeParquetNestedListReader create(MessageType clipped, String top, ArrayType type,
            String name, int batchRows, BufferAllocator allocator) {
        ColumnDescriptor cd = leavesUnder(clipped, top).get(0);
        int k = arrayDepth(type);
        int[] listDef = new int[k + 1];
        int[] elemDef = new int[k + 1];
        thresholds(clipped, cd.getPath(), listDef, elemDef);
        DataType et = innermost(type);
        return new NativeParquetNestedListReader(
                type,
                new NativeParquetColumnReader(cd, NativeParquetSupport.decodeLane(et), et, name + ".element",
                        batchRows, allocator),
                listDef,
                elemDef);
    }

    /**
     * Fills each depth's list and element thresholds from the repeated nodes on
     * {@code leaf}'s path.
     */
    private static void thresholds(MessageType clipped, String[] leaf, int[] listDef,
            int[] elemDef) {
        Type t = clipped;
        int j = 0;
        for (int i = 0; i < leaf.length; i++) {
            t = t.asGroupType().getType(leaf[i]);
            if (t.isRepetition(Type.Repetition.REPEATED)) {
                j++;
                String[] at = java.util.Arrays.copyOf(leaf, i + 1);
                elemDef[j] = clipped.getMaxDefinitionLevel(at);
                listDef[j] = i == 0 ? 0 : clipped.getMaxDefinitionLevel(java.util.Arrays.copyOf(leaf, i));
            }
        }
    }

    private static List<ColumnDescriptor> leavesUnder(MessageType clipped, String top) {
        List<ColumnDescriptor> out = new ArrayList<>();
        for (ColumnDescriptor cd : clipped.getColumns()) {
            if (cd.getPath()[0].equals(top)) {
                out.add(cd);
            }
        }
        return out;
    }

    /**
     * Checks the clipped shape of the top-level list of lists {@code top}: one
     * leaf, repeated exactly as deep as {@code type} nests arrays, and no
     * repeated leaf of its own (a bare repeated primitive would be a repeated
     * node with no list group above it at the innermost depth). Calls {@code
     * leaf} with the leaf's descriptor and Spark type. False when the reader
     * cannot follow it.
     */
    public static boolean forEachLeaf(MessageType clipped, String top, ArrayType type,
            BiConsumer<ColumnDescriptor, DataType> leaf) {
        if (!clipped.containsField(top) || clipped.getType(top).isPrimitive()) {
            return false;
        }
        List<ColumnDescriptor> leaves = leavesUnder(clipped, top);
        int k = arrayDepth(type);
        if (leaves.size() != 1 || leaves.get(0).getMaxRepetitionLevel() != k) {
            return false;
        }
        leaf.accept(leaves.get(0), innermost(type));
        return true;
    }

    /**
     * A batch: each depth's lists (rows at depth 1), their nulls and offsets,
     * and the leaf's elements.
     */
    private static final class Batch {
        final int[] lists; // lists[j]: depth-j lists
        final boolean[][] nulls; // nulls[j]: null depth-j lists (null: none)
        final int[][] offsets; // offsets[j]: lists[j] + 1 offsets into depth j's elements
        final FieldVector elements;

        Batch(int[] lists, boolean[][] nulls, int[][] offsets,
              FieldVector elements) {
            this.lists = lists;
            this.nulls = nulls;
            this.offsets = offsets;
            this.elements = elements;
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        leaf.startRowGroup(store, rows);
    }

    @Override
    public Object readBatch(int n) {
        int entries = leaf.entriesForRows(n);
        int[] r = leaf.repLevels();
        int[] d = leaf.defLevels();
        int off = leaf.levelOffset();
        int[] lists = new int[depth + 1];
        int[] elems = new int[depth + 1];
        boolean[][] nulls = new boolean[depth + 1][];
        int[][] offsets = new int[depth + 1][];
        // Each depth's list count is bounded by the entries (and depth 1 by the rows).
        offsets[1] = new int[n + 1];
        for (int j = 2; j <= depth; j++) {
            offsets[j] = new int[entries + 1];
        }
        for (int e = 0; e < entries; e++) {
            int rep = r[off + e];
            int def = d[off + e];
            for (int j = 1; j <= depth; j++) {
                boolean exists = j == 1 || def >= elemDef[j - 1];
                if (rep <= j - 1 && exists) {
                    int at = lists[j];
                    offsets[j][at] = elems[j];
                    if (def < listDef[j]) {
                        if (nulls[j] == null) {
                            nulls[j] = new boolean[j == 1 ? n : entries];
                        }
                        nulls[j][at] = true;
                    }
                    lists[j]++;
                }
                if (rep <= j && def >= elemDef[j]) {
                    elems[j]++;
                }
            }
        }
        if (lists[1] != n) {
            throw new IllegalStateException("a list column ended at row " + lists[1] + " of " + n);
        }
        for (int j = 1; j <= depth; j++) {
            offsets[j][lists[j]] = elems[j];
        }
        FieldVector v = leaf.readElements(n, entries, elemDef[depth]);
        return new Batch(lists, nulls, offsets, v);
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
        leaf.skipRows(n);
    }

    @Override
    public ColumnVector wrap(Object batch) {
        Batch b = (Batch) batch;
        ArrayType[] types = new ArrayType[depth + 1];
        DataType t = type;
        for (int j = 1; j <= depth; j++) {
            types[j] = (ArrayType) t;
            t = types[j].elementType();
        }
        ColumnVector inner = leaf.wrap(b.elements);
        for (int j = depth; j >= 1; j--) {
            inner = VectorNestedColumnVector.list(types[j], b.lists[j], b.nulls[j], b.offsets[j], inner);
        }
        return inner;
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            leaf.release(((Batch) batch).elements);
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        leaf.setPoolLimit(limit);
    }

    @Override
    public void setDictionaryIds(boolean enabled) {
        leaf.setDictionaryIds(false);
    }

    @Override
    public int rowsRemaining() {
        return leaf.rowsRemaining();
    }

    @Override
    public void close() {
        leaf.close();
    }
}
