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
public final class NativeParquetStructReader implements NativeParquetColumn, RowLevels {

    private final StructType type;
    private final NativeParquetColumn[] fields;
    // The field whose per-row levels give this struct's validity (its first: a leaf, a struct, a list or a map --
    // each carries the same information about the struct), and the level at which the struct is present;
    // null / 0 when no group from the root down to the struct is optional (never null).
    private final RowLevels levelSource;
    private final int defLevel;
    // The first field the file holds: a struct's levels come from it (fields the file lacks are null columns).
    private final RowLevels levelField;

    private NativeParquetStructReader(StructType type, NativeParquetColumn[] fields, int levelIndex,
            int defLevel) {
        this.type = type;
        this.fields = fields;
        this.levelField = (RowLevels) fields[levelIndex];
        this.levelSource = defLevel > 0 ? levelField : null;
        this.defLevel = defLevel;
        if (this.levelSource != null) {
            this.levelSource.recordRowLevels();
        }
    }

    @Override
    public int[] rowLevels() {
        return levelField.rowLevels();
    }

    @Override
    public void recordRowLevels() {
        levelField.recordRowLevels();
    }

    /**
     * The reader of the struct of Spark type {@code type} at {@code path} (file
     * names) in the clipped schema {@code clipped}; each leaf decodes {@code
     * batchRows}-row batches into vectors of {@code allocator}. The clipped
     * group must hold the struct's fields in order ({@link #forEachLeaf}).
     */
    public static NativeParquetStructReader create(MessageType clipped, String[] path, StructType type,
            String name, int batchRows, BufferAllocator allocator) {
        return create(clipped, null, path, type, name, batchRows,
                allocator);
    }

    /**
     * {@link #create(MessageType, String[], StructType, String, int, BufferAllocator)}
     * over a file whose schema is {@code file}: a field the file does not hold
     * (#568 schema evolution) is a {@link NativeParquetNullColumn}, and the
     * struct's nulls come from its first field the file holds -- as Spark's
     * {@code ParquetColumnVector} takes them from a non-missing child. {@code
     * file} null: every field is in the file. At least one field must be.
     */
    public static NativeParquetStructReader create(
            MessageType clipped,
            MessageType file,
            String[] path,
            StructType type,
            String name,
            int batchRows,
            BufferAllocator allocator) {
        GroupType group = clipped.getType(path).asGroupType();
        StructField[] fs = type.fields();
        NativeParquetColumn[] fields = new NativeParquetColumn[fs.length];
        int levelField = -1;
        for (int i = 0; i < fs.length; i++) {
            String[] childPath = child(path, group.getType(i)
                    .getName());
            DataType dt = fs[i].dataType();
            String childName = name + "." + fs[i].name();
            if (file != null && !holds(file, childPath)) {
                fields[i] = new NativeParquetNullColumn(childName, dt, allocator);
                continue;
            }
            if (levelField < 0) {
                levelField = i;
            }
            if (dt instanceof StructType st) {
                fields[i] = create(clipped, file, childPath, st, childName, batchRows,
                        allocator);
            } else if (dt instanceof org.apache.spark.sql.types.ArrayType || dt instanceof org.apache.spark.sql.types.MapType) {
                // #568: a list or map field, read by the list reader at its path under the struct.
                fields[i] = NativeParquetListReader.create(clipped, childPath, dt, childName, batchRows, allocator);
            } else {
                ColumnDescriptor cd = clipped.getColumnDescription(childPath);
                fields[i] = new NativeParquetColumnReader(cd, NativeParquetSupport.decodeLane(dt), dt, childName, batchRows,
                        allocator);
            }
        }
        if (levelField < 0) {
            throw new IllegalStateException("struct " + name + ": the file holds none of its fields");
        }
        return new NativeParquetStructReader(type, fields, levelField, clipped.getMaxDefinitionLevel(path));
    }

    /**
     * Whether the file holds the field at {@code path} (file names: the clipped
     * schema keeps the file's own names for the fields it matched), a leaf or a
     * group.
     */
    public static boolean holds(MessageType file, String[] path) {
        Type t = file;
        for (String p : path) {
            if (t.isPrimitive() || !t.asGroupType().containsField(p)) {
                return false;
            }
            t = t.asGroupType().getType(p);
        }
        return true;
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
            DataType dt = fs[i].dataType();
            if (!ct.getName().equalsIgnoreCase(fs[i].name())) {
                return false;
            }
            String[] childPath = child(path, ct.getName());
            if (dt instanceof org.apache.spark.sql.types.ArrayType || dt instanceof org.apache.spark.sql.types.MapType) {
                // #568: a list or map field (a LIST / MAP group, or a bare repeated field) has its own shape check.
                if (!NativeParquetListReader.forEachLeaf(clipped, childPath, dt, leaf)) {
                    return false;
                }
                continue;
            }
            if (ct.isRepetition(Type.Repetition.REPEATED)) {
                return false;
            }
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
        // Read after every field: the level source may be under a nested field, and its levels stay valid until
        // its next batch.
        boolean[] nulls = null;
        if (levelSource != null) {
            int[] levels = levelSource.rowLevels();
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

    @Override
    public void setRebase(ParquetRebase rebase) {
        for (NativeParquetColumn f : fields) {
            f.setRebase(rebase);
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
        return ((NativeParquetColumn) levelField).rowsRemaining();
    }

    @Override
    public void close() {
        for (NativeParquetColumn f : fields) {
            f.close();
        }
    }
}
