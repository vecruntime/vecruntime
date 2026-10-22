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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;

/**
 * The partition values of a batch's rows for an Iceberg partition spec (#842,
 * W3): per spec field, the source column's value in Iceberg's internal
 * representation (an int for a date, micros for a timestamp, a {@code
 * CharSequence} for a string, ...) put through the field's transform as Iceberg
 * binds it ({@code Transform.bind(sourceType)}), so identity, bucket, truncate,
 * year, month, day, hour and void are Iceberg's own functions. The time
 * transforms over dates and timestamps -- the common partitioning -- take
 * Iceberg's {@link DateTimeUtil} directly on the primitive value instead of
 * boxing it first.
 *
 * <p>Source columns must be top-level fields of the write schema; {@link
 * #unsupported} names the first one that is not.
 */
final class PartitionKeys {

    /**
     * One spec field: the write-schema position of its source, its source type
     * and its bound transform.
     */
    private record Field(int column, Type.TypeID source, Type sourceType,
                         String transform, SerializableFunction<Object, Object> fn) {}

    private final PartitionSpec spec;
    private final Schema schema;
    private final Field[] fields;

    @SuppressWarnings("unchecked")
    PartitionKeys(PartitionSpec spec, Schema schema) {
        String reason = unsupported(spec, schema);
        if (reason != null) {
            throw new UnsupportedOperationException(reason);
        }
        this.spec = spec;
        this.schema = schema;
        List<PartitionField> specFields = spec.fields();
        this.fields = new Field[specFields.size()];
        List<Types.NestedField> columns = schema.columns();
        for (int k = 0; k < fields.length; k++) {
            PartitionField pf = specFields.get(k);
            int column = -1;
            for (int c = 0; c < columns.size(); c++) {
                if (columns.get(c).fieldId() == pf.sourceId()) {
                    column = c;
                }
            }
            Type sourceType = columns.get(column).type();
            fields[k] = new Field(
                    column,
                    sourceType.typeId(),
                    sourceType,
                    pf.transform().toString(),
                    (SerializableFunction<Object, Object>) pf.transform().bind(sourceType));
        }
    }

    /**
     * Why a spec's partition values cannot be computed from the batches (null
     * when they can): every source must be a top-level write-schema column of a
     * type whose values the columns carry.
     */
    static String unsupported(PartitionSpec spec, Schema schema) {
        for (PartitionField pf : spec.fields()) {
            Types.NestedField top = null;
            for (Types.NestedField c : schema.columns()) {
                if (c.fieldId() == pf.sourceId()) {
                    top = c;
                }
            }
            if (top == null) {
                return "partition source "
                        + pf.sourceId()
                        + " ("
                        + pf.name()
                        + ") is not a top-level column";
            }
            switch (top.type().typeId()) {
                case BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, STRING, BINARY, DECIMAL -> {}
                default -> {
                    return "partition source type " + top.type() + " is not supported";
                }
            }
        }
        return null;
    }

    int size() {
        return fields.length;
    }

    /**
     * The partition values of rows {@code [first, end)}: {@code
     * values[field][row - first]}, each as Iceberg's transform returns it
     * ({@code null} for a null source or a void transform).
     */
    Object[][] compute(List<? extends VectorBuffers> columns, int first, int end) {
        int n = end - first;
        Object[][] out = new Object[fields.length][n];
        for (int k = 0; k < fields.length; k++) {
            Field f = fields[k];
            VectorBuffers v = columns.get(f.column());
            Object[] dst = out[k];
            if (f.transform().equals("void")) {
                continue;
            }
            boolean time = f.transform().equals("year")
                    || f.transform().equals("month")
                    || f.transform().equals("day")
                    || f.transform().equals("hour");
            for (int r = 0; r < n; r++) {
                int row = first + r;
                if (v.isNull(row)) {
                    continue;
                }
                if (time && f.source() == Type.TypeID.DATE) {
                    int days = v.getInt(row);
                    dst[r] = switch (f.transform()) {
                                case "year" -> DateTimeUtil.daysToYears(days);
                                case "month" -> DateTimeUtil.daysToMonths(days);
                                default -> days;
                            };
                } else if (time && f.source() == Type.TypeID.TIMESTAMP) {
                    long micros = v.getLong(row);
                    dst[r] = switch (f.transform()) {
                                case "year" -> DateTimeUtil.microsToYears(micros);
                                case "month" -> DateTimeUtil.microsToMonths(micros);
                                case "day" -> DateTimeUtil.microsToDays(micros);
                                default -> DateTimeUtil.microsToHours(micros);
                            };
                } else {
                    dst[r] = f.fn().apply(sourceValue(v, row, f));
                }
            }
        }
        return out;
    }

    /**
     * Element {@code row} of {@code v} in Iceberg's internal representation for
     * the field's source type.
     */
    private static Object sourceValue(VectorBuffers v, int row, Field f) {
        return switch (f.source()) {
            case BOOLEAN -> v.getBoolean(row);
            case INTEGER, DATE -> v.getInt(row);
            case LONG, TIMESTAMP -> v.getLong(row);
            // A float column arrives in a FLOAT64 lane.
            case FLOAT -> (float) v.getDouble(row);
            case DOUBLE -> v.getDouble(row);
            case STRING -> v.getString(row);
            case BINARY -> ByteBuffer.wrap(v.getUtf8Bytes(row));
            case DECIMAL -> {
                int scale = ((Types.DecimalType) f.sourceType()).scale();
                if (v.type() == VecType.DECIMAL128) {
                    yield new BigDecimal(v.getDecimal128(row), scale);
                }
                yield BigDecimal.valueOf(v.type() == VecType.INT32 ? v.getInt(row) : v.getLong(row),
                        scale);
            }
            default -> throw new IllegalStateException("unsupported partition source " + f.source());
        };
    }

    /**
     * Whether rows {@code a} and {@code b} (indexes into {@code values}' rows)
     * have the same partition.
     */
    static boolean samePartition(Object[][] values, int a, int b) {
        for (Object[] field : values) {
            if (!Objects.equals(field[a], field[b])) {
                return false;
            }
        }
        return true;
    }

    /**
     * The partition of row {@code r} of {@code values}, as a new {@link
     * PartitionKey}.
     */
    PartitionKey key(Object[][] values, int r) {
        PartitionKey key = new PartitionKey(spec, schema);
        for (int k = 0; k < values.length; k++) {
            key.set(k, values[k][r]);
        }
        return key;
    }

    /** Whether row {@code r} of {@code values} is in partition {@code key}. */
    static boolean inPartition(Object[][] values, int r, PartitionKey key) {
        for (int k = 0; k < values.length; k++) {
            if (!Objects.equals(values[k][r], key.get(k, Object.class))) {
                return false;
            }
        }
        return true;
    }
}
