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
package io.vecruntime.parquet.write;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import static io.vecruntime.kernels.VectorBuffers.LE_INT;

/**
 * Writes one top-level struct, array or map field (#842) from its nested lane
 * (Arrow's layout: a struct's children of its length, a list's or map's
 * offsets into its children) into parquet-java's column writers, one per leaf,
 * with the repetition and definition levels of the Dremel encoding.
 *
 * <p>The schema is the one Spark's {@code SparkToParquetSchemaConverter} writes
 * outside {@code writeLegacyFormat}: a struct is a group of its fields, an
 * array the 3-level {@code LIST} ({@code repeated group list { element }}) and
 * a map the {@code MAP} ({@code repeated group key_value { key; value }}). Per
 * row, a null value at any level writes a null to every leaf under it, at the
 * definition level of its nearest defined ancestor; an empty array or map does
 * the same one level deeper; the first element of a repeated group takes the
 * enclosing repetition level and the others the group's own.
 */
final class NestedFieldWriter {

    /**
     * One leaf column of the field: its descriptor and, once a value reached
     * it, its encoder.
     */
    private static final class Leaf {
        final ColumnDescriptor column;
        ColumnEncoder encoder;
        VecType lane;
        ColumnWriter writer;

        Leaf(ColumnDescriptor column) {
            this.column = column;
        }
    }

    private final Type field;
    private final IdentityHashMap<Type, Leaf> leaves = new IdentityHashMap<>();
    // Every node's leaves, for the nulls of a null or empty value.
    private final IdentityHashMap<Type, Leaf[]> under = new IdentityHashMap<>();
    private final List<Leaf> all = new ArrayList<>();

    NestedFieldWriter(MessageType schema, Type field) {
        this.field = field;
        List<String> path = new ArrayList<>();
        path.add(field.getName());
        collect(schema, field, path);
    }

    /** The leaf columns of the field, in schema order. */
    List<ColumnDescriptor> columns() {
        List<ColumnDescriptor> out = new ArrayList<>(all.size());
        for (Leaf l : all) {
            out.add(l.column);
        }
        return out;
    }

    private Leaf[] collect(MessageType schema, Type t, List<String> path) {
        Leaf[] out;
        if (t.isPrimitive()) {
            Leaf l = new Leaf(schema.getColumnDescription(path.toArray(new String[0])));
            leaves.put(t, l);
            all.add(l);
            out = new Leaf[] {l};
        } else {
            List<Leaf> below = new ArrayList<>();
            for (Type c : t.asGroupType().getFields()) {
                path.add(c.getName());
                for (Leaf l : collect(schema, c, path)) {
                    below.add(l);
                }
                path.remove(path.size() - 1);
            }
            out = below.toArray(new Leaf[0]);
        }
        under.put(t, out);
        return out;
    }

    /**
     * Writes rows {@code [from, to)} of the field's lane {@code v}; the caller
     * ends each record.
     */
    void write(VectorBuffers v, int from, int to,
               ColumnWriteStore store) {
        for (Leaf l : all) {
            l.writer = store.getColumnWriter(l.column);
        }
        for (int i = from; i < to; i++) {
            node(field, v, i, 0, 0, 0);
        }
    }

    private void node(Type t, VectorBuffers v, int i,
                      int rep, int def, int depth) {
        if (t.isRepetition(Type.Repetition.OPTIONAL)) {
            MemorySegment validity = v.validity();
            if (validity != null && !Bitmap.isSet(validity, i)) {
                nulls(t, rep, def);
                return;
            }
            def++;
        }
        if (t.isPrimitive()) {
            Leaf l = leaves.get(t);
            if (l.encoder == null || l.lane != v.type()) {
                // Bound per lane type: a batch from another source may carry the same leaf in another lane.
                l.encoder = ColumnEncoder.forColumn(v.type(), l.column);
                l.lane = v.type();
            }
            l.encoder.put(v, i, rep, def, l.writer);
            return;
        }
        GroupType g = t.asGroupType();
        LogicalTypeAnnotation a = g.getLogicalTypeAnnotation();
        boolean list = a instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
        boolean map = a instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation;
        if (list || map) {
            expect(v, list ? VecType.LIST : VecType.MAP, g);
            GroupType repeated = g.getType(0).asGroupType();
            MemorySegment offsets = v.offsets();
            int start = offsets.get(LE_INT, (long) i << 2);
            int end = offsets.get(LE_INT, (long) (i + 1) << 2);
            if (start == end) {
                nulls(g, rep, def); // empty: defined to this group, nothing below it
                return;
            }
            List<VectorBuffers> children = v.children();
            int inner = depth + 1;
            for (int j = start; j < end; j++) {
                int r = j == start ? rep : inner;
                // The repeated group is present for each element: one more definition level.
                for (int f = 0; f < repeated.getFieldCount(); f++) {
                    node(repeated.getType(f), children.get(f), j, r,
                            def + 1, inner);
                }
            }
            return;
        }
        expect(v, VecType.STRUCT, g);
        List<VectorBuffers> children = v.children();
        if (a instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation) {
            // The engine carries a variant as the struct (value, metadata), the order of Spark's variant vectors
            // (NativeParquetSupport.VARIANT_READ_TYPE); the file's group is matched by name, in its own order.
            for (int f = 0; f < g.getFieldCount(); f++) {
                String name = g.getType(f).getName();
                int k = switch (name) {
                    case "value" -> 0;
                    case "metadata" -> 1;
                    default -> throw new IllegalArgumentException("variant group field '" + name + "' (a shredded variant) is not written natively: " + g);
                };
                node(g.getType(f), children.get(k), i, rep,
                        def, depth);
            }
            return;
        }
        for (int f = 0; f < g.getFieldCount(); f++) {
            node(g.getType(f), children.get(f), i, rep,
                    def, depth);
        }
    }

    private void nulls(Type t, int rep, int def) {
        for (Leaf l : under.get(t)) {
            l.writer.writeNull(rep, def);
        }
    }

    private static void expect(VectorBuffers v, VecType type, GroupType g) {
        if (v.type() != type) {
            throw new IllegalArgumentException("a " + v.type() + " lane for " + g);
        }
    }
}
