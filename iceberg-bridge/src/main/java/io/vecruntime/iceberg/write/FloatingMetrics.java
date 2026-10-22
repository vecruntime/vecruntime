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
package io.vecruntime.iceberg.write;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.iceberg.DoubleFieldMetrics;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FloatFieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * The metrics a Parquet footer cannot give Iceberg: per float and double field,
 * the NaN count and bounds that skip NaN (#842). Iceberg's own Parquet writers
 * collect them while writing (its {@code FloatFieldMetrics} / {@code
 * DoubleFieldMetrics}) and hand them to {@code ParquetUtil.footerMetrics},
 * which takes everything else from the footer; this collects the same from the
 * batches, with Iceberg's own builders, so the merged metrics are the ones
 * Iceberg's writer reports.
 *
 * <p>Covers float and double fields at the top level and inside structs. A
 * float or double inside a list or a map is refused by {@link #check}: its
 * values are not row-aligned, and a file whose bounds could include a NaN must
 * not be committed.
 */
final class FloatingMetrics {

    /**
     * One float or double field: its position as a path of struct child
     * indexes, its rows and nulls so far (a null parent struct counts as a
     * null), and the builder of its NaN count and bounds.
     */
    private static final class Leaf {
        final int id;
        final List<Integer> path;
        final boolean isFloat;
        final FloatFieldMetrics.Builder floats;
        final DoubleFieldMetrics.Builder doubles;
        long values;
        long nulls;

        Leaf(int id, List<Integer> path, boolean isFloat) {
            this.id = id;
            this.path = path;
            this.isFloat = isFloat;
            this.floats = isFloat ? new FloatFieldMetrics.Builder(id) : null;
            this.doubles = isFloat ? null : new DoubleFieldMetrics.Builder(id);
        }
    }

    private final List<Leaf> leaves = new ArrayList<>();

    FloatingMetrics(Schema schema) {
        collect(schema.asStruct(), List.of());
    }

    /** Whether {@code schema} has a float or double only inside a list or a map. */
    static boolean check(Schema schema) {
        return !repeatedFloating(schema.asStruct(), false);
    }

    private static boolean repeatedFloating(Type type, boolean repeated) {
        switch (type.typeId()) {
            case FLOAT:
            case DOUBLE:
                return repeated;
            case STRUCT:
                for (Types.NestedField f : type.asStructType().fields()) {
                    if (repeatedFloating(f.type(), repeated)) {
                        return true;
                    }
                }
                return false;
            case LIST:
                return repeatedFloating(type.asListType()
                        .elementType(),
                        true);
            case MAP:
                return repeatedFloating(type.asMapType()
                        .keyType(),
                        true)
                        || repeatedFloating(type.asMapType()
                        .valueType(),
                        true);
            default:
                return false;
        }
    }

    private void collect(Types.StructType struct, List<Integer> prefix) {
        List<Types.NestedField> fields = struct.fields();
        for (int k = 0; k < fields.size(); k++) {
            Types.NestedField f = fields.get(k);
            List<Integer> path = new ArrayList<>(prefix);
            path.add(k);
            switch (f.type().typeId()) {
                case FLOAT -> leaves.add(new Leaf(f.fieldId(), List.copyOf(path), true));
                case DOUBLE -> leaves.add(new Leaf(f.fieldId(), List.copyOf(path), false));
                case STRUCT -> collect(f.type()
                                        .asStructType(),
                        path);
                default -> {}
            }
        }
    }

    /**
     * Adds rows {@code [first, end)} of {@code columns} (one per top-level
     * field).
     */
    void add(List<? extends VectorBuffers> columns, int first, int end) {
        for (Leaf leaf : leaves) {
            VectorBuffers v = columns.get(leaf.path.get(0));
            List<VectorBuffers> parents = new ArrayList<>(leaf.path.size() - 1);
            for (int d = 1; d < leaf.path.size(); d++) {
                parents.add(v);
                v = v.children().get(leaf.path.get(d));
            }
            boolean anyParentNulls = false;
            for (VectorBuffers p : parents) {
                anyParentNulls |= p.hasNulls();
            }
            leaf.values += end - first;
            // The segments once per batch rather than through the per-element accessors.
            MemorySegment data = v.data();
            MemorySegment validity = v.validity();
            if (validity == null && !anyParentNulls) {
                if (leaf.isFloat) {
                    for (int r = first; r < end; r++) {
                        leaf.floats.addValue((float) data.get(VectorBuffers.LE_DOUBLE, (long) r << 3));
                    }
                } else {
                    for (int r = first; r < end; r++) {
                        leaf.doubles.addValue(data.get(VectorBuffers.LE_DOUBLE, (long) r << 3));
                    }
                }
                continue;
            }
            for (int r = first; r < end; r++) {
                if ((validity != null && !Bitmap.isSet(validity, r))
                        || (anyParentNulls && underNull(parents, r))) {
                    leaf.nulls++;
                } else if (leaf.isFloat) {
                    leaf.floats.addValue((float) data.get(VectorBuffers.LE_DOUBLE, (long) r << 3));
                } else {
                    leaf.doubles.addValue(data.get(VectorBuffers.LE_DOUBLE, (long) r << 3));
                }
            }
        }
    }

    private static boolean underNull(List<VectorBuffers> parents, int r) {
        for (VectorBuffers p : parents) {
            if (p.isNull(r)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Per field: the builders' NaN count and bounds, with the value count
     * (nulls included) and null count of every row, as Iceberg's metrics count
     * them.
     */
    Stream<FieldMetrics<?>> metrics() {
        return leaves.stream().map(l -> {
            if (l.isFloat) {
                FieldMetrics<Float> m = l.floats.build();
                return new FieldMetrics<>(l.id, l.values, l.nulls, m.nanValueCount(),
                        m.lowerBound(), m.upperBound());
            }
            FieldMetrics<Double> m = l.doubles.build();
            return new FieldMetrics<>(l.id, l.values, l.nulls, m.nanValueCount(),
                    m.lowerBound(), m.upperBound());
        });
    }
}
