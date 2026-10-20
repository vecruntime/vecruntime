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

import java.util.List;
import java.util.function.BiConsumer;

import io.vecruntime.parquet.read.NestedListColumnReader;
import io.vecruntime.spark.arrow.VectorNestedColumnVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
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
public final class NativeParquetNestedListReader extends NestedListColumnReader implements NativeParquetColumn {

    private final ArrayType type;
    private final NativeParquetColumnReader leaf; // the base class's leaf, as a Spark leaf
    private final int depth;

    private NativeParquetNestedListReader(ArrayType type, NativeParquetColumnReader leaf, int[] listDef,
            int[] elemDef) {
        super(leaf, listDef, elemDef);
        this.type = type;
        this.leaf = leaf;
        this.depth = listDef.length - 1;
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
    public static NativeParquetNestedListReader create(MessageType clipped, String[] path, ArrayType type,
            String name, int batchRows, BufferAllocator allocator) {
        ColumnDescriptor cd = NativeParquetListReader.leavesUnder(clipped, path).get(0);
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

    /**
     * Checks the clipped shape of the top-level list of lists {@code top}: one
     * leaf, repeated exactly as deep as {@code type} nests arrays, and no
     * repeated leaf of its own (a bare repeated primitive would be a repeated
     * node with no list group above it at the innermost depth). Calls {@code
     * leaf} with the leaf's descriptor and Spark type. False when the reader
     * cannot follow it.
     */
    public static boolean forEachLeaf(MessageType clipped, String[] path, ArrayType type,
            BiConsumer<ColumnDescriptor, DataType> leaf) {
        Type node = NativeParquetListReader.nodeAt(clipped, path);
        if (node == null || node.isPrimitive()) {
            return false;
        }
        List<ColumnDescriptor> leaves = NativeParquetListReader.leavesUnder(clipped, path);
        int k = arrayDepth(type);
        if (leaves.size() != 1 || leaves.get(0).getMaxRepetitionLevel() != k) {
            return false;
        }
        leaf.accept(leaves.get(0), innermost(type));
        return true;
    }

    @Override
    public ColumnVector wrap(Object batch) {
        NestedListColumnReader.Batch b = (NestedListColumnReader.Batch) batch;
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
}
