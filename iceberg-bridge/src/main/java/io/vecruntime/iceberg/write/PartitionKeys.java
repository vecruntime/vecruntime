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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import io.vecruntime.kernels.Decimal128;
import io.vecruntime.kernels.FixedBinaryVectorBuffers;
import io.vecruntime.kernels.IcebergTransformKernels;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * The partition values of a batch's rows for an Iceberg partition spec (#842,
 * W3; #901 item 11): per spec field, the source column's value put through the
 * field's transform, computed on whole lanes with {@link
 * IcebergTransformKernels} -- the same Murmur3 bucket, truncate and time
 * transforms Iceberg's own {@code Transforms} compute, never boxing a value per
 * row. bucket and the time transforms reduce to an {@code int}; truncate of an
 * integral or decimal reduces to an {@code int}/{@code long}; identity and the
 * object-valued truncates (string, binary, decimal) keep the value itself.
 *
 * <p>Rows are grouped by their partition without a {@link PartitionKey} per row:
 * each row carries a hash of its field results, rows that hash and compare equal
 * share a group, and one {@code PartitionKey} is materialised per distinct
 * group when the writer needs one. Nulls are kept exactly as Iceberg keeps them
 * -- a null source (or a null enclosing struct) gives a null partition value,
 * and the values and paths are Iceberg's own.
 *
 * <p>A source may be a top-level write-schema field or a field nested inside
 * (possibly nested) structs (#901 item 7): the leaf lane is read through the
 * struct children, and a row is null when the leaf or any enclosing struct is
 * null, as Iceberg's accessor reads it. {@link #unsupported} names the first
 * source a batch cannot supply.
 */
final class PartitionKeys {

    /** How a field's transform result is represented. */
    private enum Kind {
        /**
         * An {@code int} result (bucket, truncate-int, year/month/day/hour,
         * identity int/date, void).
         */
        INT,
        /** A {@code long} result (truncate-long, identity long/timestamp). */
        LONG,
        /** The value itself (identity, or truncate of a string / binary / decimal). */
        OBJECT
    }

    /**
     * One spec field: the write-schema position of its top-level source, the
     * chain of struct child indices down to the leaf (empty for a top-level
     * source), the leaf source type, the transform and how its result is held.
     */
    private record Field(
            int column,
            List<Integer> structPath,
            Type.TypeID source,
            Type sourceType,
            Types.NestedField leaf,
            String transform,
            int param,
            Kind kind,
            org.apache.iceberg.util.SerializableFunction<Object, Object> fn) {}

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
        for (int k = 0; k < fields.length; k++) {
            PartitionField pf = specFields.get(k);
            Located located = locate(schema, pf.sourceId());
            Type sourceType = located.leaf().type();
            String transform = pf.transform().toString();
            String name = transformName(transform);
            Kind kind = kindOf(name, sourceType);
            // Object-valued results (identity, and truncate of a string / binary / decimal) go through
            // Iceberg's own bound transform, so they are byte-for-byte Iceberg's; the primitive results
            // (bucket, truncate int/long, year/month/day/hour) are the allocation-free kernel path.
            org.apache.iceberg.util.SerializableFunction<Object, Object> fn = kind == Kind.OBJECT ? (org.apache.iceberg.util.SerializableFunction<Object, Object>) pf.transform().bind(sourceType) : null;
            fields[k] = new Field(
                    located.column(),
                    located.structPath(),
                    sourceType.typeId(),
                    sourceType,
                    located.leaf(),
                    name,
                    transformParam(transform),
                    kind,
                    fn);
        }
    }

    /**
     * The transform name without its parameter: {@code bucket}, {@code
     * truncate}, {@code identity}, ...
     */
    private static String transformName(String transform) {
        int b = transform.indexOf('[');
        return b < 0 ? transform : transform.substring(0, b);
    }

    /**
     * The {@code N} of a {@code bucket[N]} or {@code truncate[N]} transform; 0
     * otherwise.
     */
    private static int transformParam(String transform) {
        int b = transform.indexOf('[');
        if (b < 0) {
            return 0;
        }
        return Integer.parseInt(transform.substring(b + 1, transform.indexOf(']')));
    }

    private static Kind kindOf(String name, Type sourceType) {
        return switch (name) {
            case "void" -> Kind.INT; // always null; INT lane carries the null flag
            case "bucket" -> Kind.INT;
            case "year", "month", "day", "hour" -> Kind.INT;
            case "truncate" -> switch (sourceType.typeId()) {
                        case INTEGER -> Kind.INT;
                        case LONG -> Kind.LONG;
                        case DECIMAL -> Kind.OBJECT; // a BigDecimal, like Iceberg's TruncateDecimal
                        default -> Kind.OBJECT; // string / binary
                    };
            default -> switch (sourceType.typeId()) { // identity
                        case INTEGER, DATE -> Kind.INT;
                        case LONG, TIMESTAMP -> Kind.LONG;
                        default -> Kind.OBJECT;
                    };
        };
    }

    /**
     * A located source: its top-level column, the struct path to the leaf, and
     * the leaf field.
     */
    private record Located(int column, List<Integer> structPath, Types.NestedField leaf) {}

    /**
     * Finds the source field {@code sourceId} in {@code schema}: its top-level
     * column index and, when it is nested, the chain of struct child indices
     * down to it.
     */
    private static Located locate(Schema schema, int sourceId) {
        // The column and struct-child indices are PHYSICAL: a v3 unknown field is dropped from the columns the
        // writer sees (SparkLanes), so it takes no slot. An unknown field can never be (or contain) a partition
        // source, so skipping it here only renumbers the siblings a source is actually found among.
        List<Types.NestedField> columns = schema.columns();
        int column = 0;
        for (int c = 0; c < columns.size(); c++) {
            Types.NestedField top = columns.get(c);
            if (top.type().typeId() == Type.TypeID.UNKNOWN) {
                continue;
            }
            if (top.fieldId() == sourceId) {
                return new Located(column, List.of(), top);
            }
            Located nested = pathTo(top, sourceId);
            if (nested != null) {
                return new Located(column, nested.structPath(), nested.leaf());
            }
            column++;
        }
        throw new IllegalStateException("partition source " + sourceId + " is not in the write schema");
    }

    /**
     * The PHYSICAL struct-child path from {@code field}'s type to field {@code
     * id} and the leaf field itself, or null when {@code id} is not a struct
     * descendant. Physical means an unknown struct child takes no slot, matching
     * the children the writer keeps.
     */
    private static Located pathTo(Types.NestedField field, int id) {
        if (!field.type().isStructType()) {
            return null;
        }
        List<Types.NestedField> fs = field.type()
                .asStructType()
                .fields();
        int child = 0;
        for (int i = 0; i < fs.size(); i++) {
            Types.NestedField f = fs.get(i);
            if (f.type().typeId() == Type.TypeID.UNKNOWN) {
                continue;
            }
            if (f.fieldId() == id) {
                return new Located(0, List.of(child), f);
            }
            Located rest = pathTo(f, id);
            if (rest != null) {
                List<Integer> p = new java.util.ArrayList<>(rest.structPath().size() + 1);
                p.add(child);
                p.addAll(rest.structPath());
                return new Located(0, List.copyOf(p), rest.leaf());
            }
            child++;
        }
        return null;
    }

    /**
     * Why a spec's partition values cannot be computed from the batches (null
     * when they can): every source must be a write-schema field -- top-level or
     * nested inside structs -- whose leaf is one of the supported types.
     */
    static String unsupported(PartitionSpec spec, Schema schema) {
        for (PartitionField pf : spec.fields()) {
            Located located;
            try {
                located = locate(schema, pf.sourceId());
            } catch (IllegalStateException e) {
                return "partition source "
                        + pf.sourceId()
                        + " ("
                        + pf.name()
                        + ") is not in the write schema";
            }
            // locate only descends through structs, so a located leaf is always reachable as a single lane.
            switch (located.leaf()
                           .type()
                           .typeId()) {
                case BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, STRING, BINARY, DECIMAL, FIXED, UUID -> {}
                default -> {
                    return "partition source type " + located.leaf().type() + " is not supported";
                }
            }
        }
        return null;
    }

    int size() {
        return fields.length;
    }

    /**
     * The partition values of rows {@code [first, end)}: one {@link Values} that
     * holds, per field, an {@code int}/{@code long} result lane or the object
     * values, plus a per-row group hash, so the writer can group rows without a
     * {@link PartitionKey} each.
     */
    Values compute(List<? extends VectorBuffers> columns, int first, int end) {
        return new Values(columns, first, end);
    }

    /**
     * The computed partition values of one call's rows {@code [first, end)},
     * indexed by {@code row - first}. Primitive results live in {@code longs}
     * (an {@code int} widened), object results in {@code objects}; {@code
     * nulls} marks a null partition value. {@code hash[r]} folds the row's
     * fields so grouping can bucket first and compare only within a bucket.
     */
    final class Values {
        private final int first;
        private final int n;
        // The resolved leaf lane of each field, and the per-field enclosing-struct-null flags (null for a
        // top-level source). Kept so an OBJECT field groups and materialises straight off the lane.
        private final VectorBuffers[] leaves;
        private final boolean[][] parentNulls;
        // [field][row]: primitive (int widened / long) results; objects materialised lazily, once per group.
        private final long[][] longs;
        private final Object[][] objects;
        private final boolean[][] nulls;
        private final long[] hash;

        @SuppressWarnings("unchecked")
        Values(List<? extends VectorBuffers> columns, int first, int end) {
            this.first = first;
            this.n = end - first;
            int f = fields.length;
            this.leaves = new VectorBuffers[f];
            this.parentNulls = new boolean[f][];
            this.longs = new long[f][];
            this.objects = new Object[f][];
            this.nulls = new boolean[f][n];
            this.hash = new long[n];
            for (int k = 0; k < f; k++) {
                computeField(k, columns);
            }
            for (int r = 0; r < n; r++) {
                long h = 1;
                for (int k = 0; k < f; k++) {
                    h = h * 31 + fieldHash(k, r);
                }
                hash[r] = h;
            }
        }

        private long fieldHash(int k, int r) {
            if (nulls[k][r]) {
                return 0;
            }
            if (fields[k].kind() == Kind.OBJECT) {
                return objectHash(k, r); // computed on the lane, no Java object
            }
            return longs[k][r];
        }

        private void computeField(int k, List<? extends VectorBuffers> columns) {
            Field field = fields[k];
            VectorBuffers leaf = resolveLeaf(field, columns);
            leaves[k] = leaf;
            boolean[] parentNull = parentNulls(field, columns);
            parentNulls[k] = parentNull;
            boolean[] fieldNull = nulls[k];
            if (field.kind() != Kind.OBJECT) {
                longs[k] = new long[n];
            } else {
                objects[k] = new Object[n]; // slots filled lazily in partitionValue
            }
            if (field.transform().equals("void")) {
                java.util.Arrays.fill(fieldNull, true);
                return;
            }
            for (int r = 0; r < n; r++) {
                int row = first + r;
                if ((parentNull != null && parentNull[r]) || leaf.isNull(row)) {
                    fieldNull[r] = true;
                    continue;
                }
                if (field.kind() != Kind.OBJECT) {
                    longs[k][r] = primitiveResult(field, leaf, row);
                }
                // OBJECT fields need nothing per row here: they group on the lane and materialise on demand.
            }
        }

        /**
         * The {@code int}/{@code long} result of a primitive-valued transform
         * at {@code row}.
         */
        private long primitiveResult(Field field, VectorBuffers v, int row) {
            String t = field.transform();
            return switch (t) {
                case "bucket" -> bucketHash(field, v, row);
                case "year" -> field.source() == Type.TypeID.DATE ? IcebergTransformKernels.daysToYears(v.getInt(row)) : IcebergTransformKernels.daysToYears(IcebergTransformKernels.microsToDays(v.getLong(row)));
                case "month" -> field.source() == Type.TypeID.DATE ? IcebergTransformKernels.daysToMonths(v.getInt(row)) : IcebergTransformKernels.daysToMonths(IcebergTransformKernels.microsToDays(v.getLong(row)));
                case "day" -> field.source() == Type.TypeID.DATE ? v.getInt(row) : IcebergTransformKernels.microsToDays(v.getLong(row));
                case "hour" -> IcebergTransformKernels.microsToHours(v.getLong(row));
                case "truncate" -> field.kind() == Kind.INT ? IcebergTransformKernels.truncate(v.getInt(row), field.param()) : IcebergTransformKernels.truncate(v.getLong(row), field.param());
                default -> // identity of an int, date, long or timestamp
                        field.kind() == Kind.INT ? v.getInt(row) : v.getLong(row);
            };
        }

        /** Iceberg's bucket of the value at {@code row}. */
        private int bucketHash(Field field, VectorBuffers v, int row) {
            int h = switch (field.source()) {
                case INTEGER, LONG, DATE, TIMESTAMP ->
                        IcebergTransformKernels.hashLong(v.type() == VecType.INT32 ? v.getInt(row) : v.getLong(row));
                case STRING, BINARY -> IcebergTransformKernels.hashBytes(v.getUtf8Bytes(row));
                case FIXED, UUID -> IcebergTransformKernels.hashBytes(((FixedBinaryVectorBuffers) v).bytes(row));
                case DECIMAL ->
                        v.type() == VecType.DECIMAL128 ? IcebergTransformKernels.hashDecimal128(Decimal128.hi(v.data(), row), Decimal128.lo(v.data(), row)) : IcebergTransformKernels.hashDecimal(v.type() == VecType.INT32 ? v.getInt(row) : v.getLong(row));
                default -> throw new IllegalStateException("bucket over " + field.source());
            };
            return IcebergTransformKernels.bucket(h, field.param());
        }

        /**
         * A grouping hash for an OBJECT field (identity, or truncate of a
         * string / binary / decimal) at {@code row}, computed on the leaf lane
         * without materialising the value. Equal transformed values hash equal;
         * {@link #objectsEqual} confirms a hash collision.
         */
        private long objectHash(int k, int r) {
            Field field = fields[k];
            VectorBuffers v = leaves[k];
            int row = first + r;
            boolean truncate = field.transform().equals("truncate");
            return switch (field.source()) {
                case STRING, BINARY -> {
                    byte[] b = v.getUtf8Bytes(row);
                    int len = truncate && field.source() == Type.TypeID.STRING
                            ? IcebergTransformKernels.truncateUtf8ByteLength(MemorySegment.ofArray(b), 0, b.length, field.param())
                            : truncate ? Math.min(field.param(), b.length) : b.length;
                    yield IcebergTransformKernels.hashBytes(MemorySegment.ofArray(b), 0, len);
                }
                case FIXED, UUID -> {
                    byte[] b = ((FixedBinaryVectorBuffers) v).bytes(row);
                    yield IcebergTransformKernels.hashBytes(b);
                }
                case DECIMAL -> {
                    long hi;
                    long lo;
                    if (v.type() == VecType.DECIMAL128) {
                        hi = Decimal128.hi(v.data(), row);
                        lo = Decimal128.lo(v.data(), row);
                    } else {
                        long u = v.type() == VecType.INT32 ? v.getInt(row) : v.getLong(row);
                        hi = u < 0 ? -1L : 0L;
                        lo = u;
                    }
                    if (truncate) {
                        long[] t = new long[2];
                        IcebergTransformKernels.truncateDecimal128(hi, lo, field.param(), t);
                        hi = t[0];
                        lo = t[1];
                    }
                    yield Decimal128.hash(hi, lo);
                }
                case BOOLEAN -> v.getBoolean(row) ? 1L : 0L;
                case FLOAT -> Float.floatToIntBits((float) v.getDouble(row));
                case DOUBLE -> Double.doubleToLongBits(v.getDouble(row));
                default -> throw new IllegalStateException("object group over " + field.source());
            };
        }

        /**
         * Whether the OBJECT field {@code k} has the same transformed value at
         * rows {@code a} and {@code b}, compared on the lane with no Java
         * object (both already known non-null).
         */
        private boolean objectsEqual(int k, int a, int b) {
            Field field = fields[k];
            VectorBuffers v = leaves[k];
            int ra = first + a;
            int rb = first + b;
            boolean truncate = field.transform().equals("truncate");
            switch (field.source()) {
                case STRING, BINARY:
                    {
                        byte[] x = v.getUtf8Bytes(ra);
                        byte[] y = v.getUtf8Bytes(rb);
                        int lx = truncate && field.source() == Type.TypeID.STRING
                                ? IcebergTransformKernels.truncateUtf8ByteLength(MemorySegment.ofArray(x), 0, x.length, field.param())
                                : truncate ? Math.min(field.param(), x.length) : x.length;
                        int ly = truncate && field.source() == Type.TypeID.STRING
                                ? IcebergTransformKernels.truncateUtf8ByteLength(MemorySegment.ofArray(y), 0, y.length, field.param())
                                : truncate ? Math.min(field.param(), y.length) : y.length;
                        return java.util.Arrays.equals(x, 0, lx, y, 0, ly);
                    }
                case FIXED, UUID:
                    {
                        return java.util.Arrays.equals(((FixedBinaryVectorBuffers) v).bytes(ra), ((FixedBinaryVectorBuffers) v).bytes(rb));
                    }
                case DECIMAL:
                    {
                        long[] da = decimalLimbs(field, v, ra, truncate);
                        long[] db = decimalLimbs(field, v, rb, truncate);
                        return da[0] == db[0] && da[1] == db[1];
                    }
                case BOOLEAN:
                    return v.getBoolean(ra) == v.getBoolean(rb);
                case FLOAT:
                    // Identity of a float: the stored lane value (NaN groups with NaN, as a bit compare).
                    return Float.floatToIntBits((float) v.getDouble(ra)) == Float.floatToIntBits((float) v.getDouble(rb));
                case DOUBLE:
                    return Double.doubleToLongBits(v.getDouble(ra)) == Double.doubleToLongBits(v.getDouble(rb));
                default:
                    throw new IllegalStateException("object compare over " + field.source());
            }
        }

        /**
         * The (possibly truncated) unscaled 128-bit value of a decimal at
         * {@code row} as {@code {hi, lo}}.
         */
        private long[] decimalLimbs(Field field, VectorBuffers v, int row,
                boolean truncate) {
            long hi;
            long lo;
            if (v.type() == VecType.DECIMAL128) {
                hi = Decimal128.hi(v.data(), row);
                lo = Decimal128.lo(v.data(), row);
            } else {
                long u = v.type() == VecType.INT32 ? v.getInt(row) : v.getLong(row);
                hi = u < 0 ? -1L : 0L;
                lo = u;
            }
            if (truncate) {
                long[] t = new long[2];
                IcebergTransformKernels.truncateDecimal128(hi, lo, field.param(), t);
                return t;
            }
            return new long[] {hi, lo};
        }

        /**
         * Element {@code row} of {@code v} in Iceberg's internal representation
         * for the field's source type, used only to materialise the Java
         * partition value once per distinct group.
         */
        private Object sourceValue(Field field, VectorBuffers v, int row) {
            return switch (field.source()) {
                case BOOLEAN -> v.getBoolean(row);
                case INTEGER, DATE -> v.getInt(row);
                case LONG, TIMESTAMP -> v.getLong(row);
                case FLOAT -> (float) v.getDouble(row);
                case DOUBLE -> v.getDouble(row);
                case STRING -> v.getString(row);
                case BINARY -> ByteBuffer.wrap(v.getUtf8Bytes(row));
                case FIXED -> ByteBuffer.wrap(((FixedBinaryVectorBuffers) v).bytes(row));
                case UUID -> uuidOf(((FixedBinaryVectorBuffers) v).bytes(row));
                case DECIMAL -> {
                    int scale = ((Types.DecimalType) field.sourceType()).scale();
                    if (v.type() == VecType.DECIMAL128) {
                        yield new BigDecimal(v.getDecimal128(row), scale);
                    }
                    yield BigDecimal.valueOf(v.type() == VecType.INT32 ? v.getInt(row) : v.getLong(row),
                            scale);
                }
                default -> throw new IllegalStateException("unsupported partition source " + field.source());
            };
        }

        int size() {
            return n;
        }

        boolean fieldIsNull(int field, int r) {
            return nulls[field][r];
        }

        /**
         * Whether rows {@code a} and {@code b} (0-based into this call) have
         * the same partition.
         */
        boolean samePartition(int a, int b) {
            if (hash[a] != hash[b]) {
                return false;
            }
            for (int k = 0; k < fields.length; k++) {
                if (nulls[k][a] != nulls[k][b]) {
                    return false;
                }
                if (nulls[k][a]) {
                    continue;
                }
                if (fields[k].kind() == Kind.OBJECT) {
                    if (!objectsEqual(k, a, b)) {
                        return false;
                    }
                } else if (longs[k][a] != longs[k][b]) {
                    return false;
                }
            }
            return true;
        }

        long hashOf(int r) {
            return hash[r];
        }

        /** The partition of row {@code r} of this call, as a new {@link PartitionKey}. */
        PartitionKey key(int r) {
            PartitionKey key = new PartitionKey(spec, schema);
            for (int k = 0; k < fields.length; k++) {
                key.set(k, partitionValue(k, r));
            }
            return key;
        }

        /**
         * Whether row {@code r} is in partition {@code key} (its already-set
         * field values).
         */
        boolean inPartition(int r, PartitionKey key) {
            for (int k = 0; k < fields.length; k++) {
                if (!Objects.equals(partitionValue(k, r), key.get(k, Object.class))) {
                    return false;
                }
            }
            return true;
        }

        /**
         * The transform result of field {@code k} at row {@code r}, boxed as
         * Iceberg's PartitionKey holds it.
         */
        private Object partitionValue(int k, int r) {
            if (nulls[k][r]) {
                return null;
            }
            Field field = fields[k];
            if (field.kind() == Kind.OBJECT) {
                // Materialise the Java value (String / ByteBuffer / BigDecimal / ...) once per distinct group:
                // grouping ran on the lane, so this is called about once per partition, not per row. Iceberg's
                // own bound transform gives the exact value its PartitionKey would hold, and it is cached.
                Object cached = objects[k][r];
                if (cached == null) {
                    cached = field.fn().apply(sourceValue(field, leaves[k], first + r));
                    objects[k][r] = cached;
                }
                return cached;
            }
            // Box as Iceberg's result type: an Integer for an INT result, a Long for a LONG result. (A mixed
            // int/long ternary would promote both arms to long and wrongly box every INT result as a Long.)
            if (field.kind() == Kind.INT) {
                return Integer.valueOf((int) longs[k][r]);
            }
            return Long.valueOf(longs[k][r]);
        }
    }

    /**
     * The leaf lane of a field: the top column, then the struct children down
     * the path.
     */
    private static VectorBuffers resolveLeaf(Field field, List<? extends VectorBuffers> columns) {
        VectorBuffers v = columns.get(field.column());
        for (int idx : field.structPath()) {
            v = v.children().get(idx);
        }
        return v;
    }

    /**
     * For a nested source, a per-row flag that any enclosing struct is null
     * (null when the source is top-level, so nothing encloses it). A struct's
     * child lane is full length, so a child slot under a null struct still
     * exists; this is what makes its partition value null, as Iceberg's
     * accessor returns null through a null struct.
     */
    private static boolean[] parentNulls(Field field, List<? extends VectorBuffers> columns) {
        List<Integer> path = field.structPath();
        if (path.isEmpty()) {
            return null;
        }
        VectorBuffers v = columns.get(field.column());
        boolean[] any = null;
        VectorBuffers node = v;
        for (int depth = 0; depth < path.size(); depth++) {
            // `node` is the struct whose field path[depth] we descend into; a null struct nulls the leaf.
            if (node.hasNulls()) {
                if (any == null) {
                    any = new boolean[node.length()];
                }
                for (int i = 0; i < node.length(); i++) {
                    if (node.isNull(i)) {
                        any[i] = true;
                    }
                }
            }
            node = node.children().get(path.get(depth));
        }
        return any;
    }

    private static java.util.UUID uuidOf(byte[] bytes) {
        long hi = 0;
        long lo = 0;
        for (int i = 0; i < 8; i++) {
            hi = (hi << 8) | (bytes[i] & 0xffL);
        }
        for (int i = 8; i < 16; i++) {
            lo = (lo << 8) | (bytes[i] & 0xffL);
        }
        return new java.util.UUID(hi, lo);
    }

    // Static helpers kept so the writer's call sites read naturally.

    static boolean samePartition(Values values, int a, int b) {
        return values.samePartition(a, b);
    }

    static boolean inPartition(Values values, int r, PartitionKey key) {
        return values.inPartition(r, key);
    }
}
