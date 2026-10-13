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
import java.util.function.BiConsumer;

import io.vecruntime.spark.arrow.VectorNestedColumnVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A struct column of the native Parquet scan (#568, work item 8): one reader
 * per field, a {@link NativeParquetColumnReader} for a leaf and another of
 * these for a nested struct. A struct has no column chunk of its own; Parquet
 * records its nulls in its leaves' definition levels. The struct is present on
 * a row where a leaf's level reaches the struct's own definition level (the
 * optional groups from the root down to the struct), so its validity comes
 * from its first leaf's levels -- every leaf under it carries the same
 * information about it. That is how Spark's vectorized reader computes a
 * struct's nulls too.
 *
 * <p>Each field decodes its rows straight into its own batch vector, as a
 * flat column does; the batch is Spark's struct column over the fields'
 * columns ({@link VectorNestedColumnVector#struct}), which our operators read
 * through {@code getStruct} / {@code getChild}. A struct holding an array or a
 * map is not read here: those need repetition levels, and the planner keeps
 * Spark's scan for them.
 */
public final class NativeParquetStructReader implements NativeParquetColumn {

    private final StructType type;
    private final NativeParquetColumn[] fields;
    // The leaf whose definition levels give this struct's validity, and the level at which the struct is present;
    // null / 0 when no group from the root down to the struct is optional (never null).
    private final NativeParquetColumnReader levelLeaf;
    private final int defLevel;

    private NativeParquetStructReader(StructType type, NativeParquetColumn[] fields, NativeParquetColumnReader levelLeaf,
            int defLevel) {
        this.type = type;
        this.fields = fields;
        this.levelLeaf = defLevel > 0 ? levelLeaf : null;
        this.defLevel = defLevel;
        if (this.levelLeaf != null) {
            this.levelLeaf.setRecordLevels(true);
        }
    }

    /**
     * The reader of the struct of Spark type {@code type} at {@code path} (file
     * names) in the clipped schema {@code clipped}; each leaf decodes {@code
     * batchRows}-row batches into vectors of {@code allocator}. The clipped
     * group must hold the struct's fields in order ({@link #forEachLeaf}).
     */
    public static NativeParquetStructReader create(MessageType clipped, String[] path, StructType type,
            String name, int batchRows, BufferAllocator allocator) {
        GroupType group = clipped.getType(path).asGroupType();
        StructField[] fs = type.fields();
        NativeParquetColumn[] fields = new NativeParquetColumn[fs.length];
        NativeParquetColumnReader first = null;
        for (int i = 0; i < fs.length; i++) {
            String[] childPath = child(path, group.getType(i)
                    .getName());
            DataType dt = fs[i].dataType();
            String childName = name + "." + fs[i].name();
            if (dt instanceof StructType st) {
                NativeParquetStructReader s = create(clipped, childPath, st, childName, batchRows, allocator);
                fields[i] = s;
                if (first == null) {
                    first = s.firstLeaf();
                }
            } else {
                ColumnDescriptor cd = clipped.getColumnDescription(childPath);
                NativeParquetColumnReader leaf = new NativeParquetColumnReader(cd, NativeParquetSupport.decodeLane(dt), dt, childName, batchRows,
                        allocator);
                fields[i] = leaf;
                if (first == null) {
                    first = leaf;
                }
            }
        }
        return new NativeParquetStructReader(type, fields, first, clipped.getMaxDefinitionLevel(path));
    }

    private NativeParquetColumnReader firstLeaf() {
        NativeParquetColumn f = fields[0];
        return f instanceof NativeParquetStructReader s ? s.firstLeaf() : (NativeParquetColumnReader) f;
    }

    private static String[] child(String[] path, String name) {
        String[] p = java.util.Arrays.copyOf(path, path.length + 1);
        p[path.length] = name;
        return p;
    }

    /**
     * Walks the leaves of the struct of Spark type {@code type} at {@code path}
     * in the clipped schema, calling {@code leaf} with each leaf's descriptor
     * and Spark type. Returns false when the clipped group does not hold the
     * struct's fields in order as groups for structs and primitives for leaves
     * -- a shape the reader cannot follow, so the file goes to Spark's reader.
     */
    public static boolean forEachLeaf(MessageType clipped, String[] path, StructType type,
            BiConsumer<ColumnDescriptor, DataType> leaf) {
        // MessageType.containsPath is true for leaf paths only: walk the groups by name.
        Type t = clipped;
        for (String p : path) {
            if (t.isPrimitive() || !t.asGroupType().containsField(p)) {
                return false;
            }
            t = t.asGroupType().getType(p);
        }
        if (t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
            return false;
        }
        GroupType group = t.asGroupType();
        StructField[] fs = type.fields();
        if (fs.length == 0 || group.getFieldCount() != fs.length) {
            return false;
        }
        for (int i = 0; i < fs.length; i++) {
            Type ct = group.getType(i);
            if (!ct.getName().equalsIgnoreCase(fs[i].name()) || ct.isRepetition(Type.Repetition.REPEATED)) {
                return false;
            }
            String[] childPath = child(path, ct.getName());
            DataType dt = fs[i].dataType();
            if (dt instanceof StructType st) {
                if (!forEachLeaf(clipped, childPath, st, leaf)) {
                    return false;
                }
            } else {
                if (!ct.isPrimitive()) {
                    return false;
                }
                leaf.accept(clipped.getColumnDescription(childPath), dt);
            }
        }
        return true;
    }

    /**
     * A struct batch: its rows, its null rows ({@code null}: none) and each
     * field's batch.
     */
    private static final class Batch {
        final int rows;
        final boolean[] nulls;
        final Object[] fields;

        Batch(int rows, boolean[] nulls, Object[] fields) {
            this.rows = rows;
            this.nulls = nulls;
            this.fields = fields;
        }
    }

    @Override
    public void startRowGroup(PageReadStore store, int rows) {
        for (NativeParquetColumn f : fields) {
            f.startRowGroup(store, rows);
        }
    }

    @Override
    public Object readBatch(int n) {
        Object[] kids = new Object[fields.length];
        try {
            for (int i = 0; i < fields.length; i++) {
                kids[i] = fields[i].readBatch(n);
            }
        } catch (RuntimeException | Error e) {
            releaseFields(kids);
            throw e;
        }
        // Read after every field: the level leaf may be under a nested field, and its levels stay valid until
        // its next batch.
        boolean[] nulls = null;
        if (levelLeaf != null) {
            int[] levels = levelLeaf.batchLevels();
            int d = defLevel;
            for (int r = 0; r < n; r++) {
                if (levels[r] < d) {
                    if (nulls == null) {
                        nulls = new boolean[n];
                    }
                    nulls[r] = true;
                }
            }
        }
        return new Batch(n, nulls, kids);
    }

    /** Decoded whole: the batch's selection excludes the other rows. */
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
        for (NativeParquetColumn f : fields) {
            f.skipRows(n);
        }
    }

    @Override
    public ColumnVector wrap(Object batch) {
        Batch b = (Batch) batch;
        ColumnVector[] cols = new ColumnVector[fields.length];
        for (int i = 0; i < fields.length; i++) {
            cols[i] = fields[i].wrap(b.fields[i]);
        }
        return VectorNestedColumnVector.struct(type, b.rows, b.nulls, cols);
    }

    @Override
    public void release(Object batch) {
        if (batch != null) {
            releaseFields(((Batch) batch).fields);
        }
    }

    private void releaseFields(Object[] kids) {
        for (int i = 0; i < kids.length; i++) {
            if (kids[i] != null) {
                fields[i].release(kids[i]);
            }
        }
    }

    @Override
    public void setPoolLimit(int limit) {
        for (NativeParquetColumn f : fields) {
            f.setPoolLimit(limit);
        }
    }

    /**
     * Off for a struct's strings: they are read by value through the struct's
     * fields, not as dictionary ids.
     */
    @Override
    public void setDictionaryIds(boolean enabled) {
        for (NativeParquetColumn f : fields) {
            f.setDictionaryIds(false);
        }
    }

    @Override
    public int rowsRemaining() {
        return fields[0].rowsRemaining();
    }

    @Override
    public void close() {
        for (NativeParquetColumn f : fields) {
            f.close();
        }
    }
}
