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
import io.vecruntime.kernels.NestedVectorBuffers;
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
 * <p>Covers float and double fields at any depth: at the top level, inside
 * structs, and as (or under) list elements and map keys or values. A field
 * under a list or map counts each element written, as Iceberg's element
 * writers count them; elements of a null list or map do not exist and are not
 * counted, and an element under a null struct inside an element counts as null.
 */
final class FloatingMetrics {

    /**
     * One step from a column to a nested child: a struct field, or a list's
     * elements / a map's keys or values.
     */
    private record Step(boolean repeated, int child) {}

    /**
     * One float or double field: its steps from its top-level column, its
     * values and nulls so far, and the builder of its NaN count and bounds.
     */
    private static final class Leaf {
        final int id;
        final int column;
        final List<Step> steps;
        final boolean isFloat;
        final FloatFieldMetrics.Builder floats;
        final DoubleFieldMetrics.Builder doubles;
        long values;
        long nulls;

        Leaf(int id, int column, List<Step> steps,
             boolean isFloat) {
            this.id = id;
            this.column = column;
            this.steps = steps;
            this.isFloat = isFloat;
            this.floats = isFloat ? new FloatFieldMetrics.Builder(id) : null;
            this.doubles = isFloat ? null : new DoubleFieldMetrics.Builder(id);
        }
    }

    private final List<Leaf> leaves = new ArrayList<>();

    FloatingMetrics(Schema schema) {
        List<Types.NestedField> fields = schema.asStruct().fields();
        for (int c = 0; c < fields.size(); c++) {
            collect(
                    fields.get(c).fieldId(),
                    fields.get(c).type(),
                    c,
                    List.of());
        }
    }

    private void collect(int id, Type type, int column,
                         List<Step> steps) {
        switch (type.typeId()) {
            case FLOAT -> leaves.add(new Leaf(id, column, List.copyOf(steps), true));
            case DOUBLE -> leaves.add(new Leaf(id, column, List.copyOf(steps), false));
            case STRUCT -> {
                List<Types.NestedField> fields = type.asStructType().fields();
                for (int k = 0; k < fields.size(); k++) {
                    collect(
                            fields.get(k).fieldId(),
                            fields.get(k).type(),
                            column,
                            with(steps, new Step(false, k)));
                }
            }
            case LIST -> {
                Types.ListType list = type.asListType();
                collect(list.elementId(), list.elementType(), column,
                        with(steps, new Step(true, 0)));
            }
            case MAP -> {
                Types.MapType map = type.asMapType();
                collect(map.keyId(), map.keyType(), column,
                        with(steps, new Step(true, 0)));
                collect(map.valueId(), map.valueType(), column,
                        with(steps, new Step(true, 1)));
            }
            default -> {}
        }
    }

    private static List<Step> with(List<Step> steps, Step step) {
        List<Step> out = new ArrayList<>(steps);
        out.add(step);
        return out;
    }

    /**
     * Adds rows {@code [first, end)} of {@code columns} (one per top-level
     * field).
     */
    void add(List<? extends VectorBuffers> columns, int first, int end) {
        for (Leaf leaf : leaves) {
            VectorBuffers v = columns.get(leaf.column);
            // A null column is a COPIED column on the M4b copy path (its values were not gathered). A copied
            // float/double is never NaN-tracked (the planner declines the copy otherwise), so it contributes
            // no metric -- skip it.
            if (v == null) {
                continue;
            }
            if (leaf.steps.isEmpty() && v.validity() == null) {
                addAll(leaf, v.data(), first, end);
                continue;
            }
            // The leaf's rows [first, end) of its own lane, plus, per row, whether an enclosing struct is null.
            int[] rows = null;
            boolean[] nullAbove = null;
            int n = end - first;
            for (Step step : leaf.steps) {
                if (!step.repeated()) {
                    for (int i = 0; i < n; i++) {
                        int r = rows == null ? first + i : rows[i];
                        if (v.isNull(r)) {
                            if (nullAbove == null) {
                                nullAbove = new boolean[n];
                            }
                            nullAbove[i] = true;
                        }
                    }
                    v = v.children().get(step.child());
                    continue;
                }
                // Into the elements: a null list or map (or one under a null struct) has none.
                NestedVectorBuffers nested = (NestedVectorBuffers) v;
                int total = 0;
                for (int i = 0; i < n; i++) {
                    int r = rows == null ? first + i : rows[i];
                    if ((nullAbove == null || !nullAbove[i]) && !v.isNull(r)) {
                        total += nested.end(r) - nested.start(r);
                    }
                }
                int[] next = new int[total];
                int k = 0;
                for (int i = 0; i < n; i++) {
                    int r = rows == null ? first + i : rows[i];
                    if ((nullAbove == null || !nullAbove[i]) && !v.isNull(r)) {
                        for (int e = nested.start(r);
                             e < nested.end(r);
                             e++) {
                            next[k++] = e;
                        }
                    }
                }
                rows = next;
                n = total;
                nullAbove = null;
                v = v.children().get(step.child());
            }
            MemorySegment data = v.data();
            MemorySegment validity = v.validity();
            for (int i = 0; i < n; i++) {
                int r = rows == null ? first + i : rows[i];
                leaf.values++;
                if ((nullAbove != null && nullAbove[i])
                        || (validity != null && !Bitmap.isSet(validity, r))) {
                    leaf.nulls++;
                } else {
                    add(leaf, data.get(VectorBuffers.LE_DOUBLE, (long) r << 3));
                }
            }
        }
    }

    private static void addAll(Leaf leaf, MemorySegment data, int first,
            int end) {
        leaf.values += end - first;
        for (int r = first; r < end; r++) {
            add(leaf, data.get(VectorBuffers.LE_DOUBLE, (long) r << 3));
        }
    }

    private static void add(Leaf leaf, double value) {
        if (leaf.isFloat) {
            leaf.floats.addValue((float) value);
        } else {
            leaf.doubles.addValue(value);
        }
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
