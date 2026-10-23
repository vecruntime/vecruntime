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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

import io.vecruntime.kernels.FixedBinaryVectorBuffers;
import io.vecruntime.kernels.NestedVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * The write side's boundary between Spark's lanes and the Arrow lanes of an
 * Iceberg type. Spark holds some Iceberg types in another shape than Arrow (and
 * the file format) does; the columnar writer works on the Arrow form, as other
 * Arrow engines do, so a batch from Spark is converted here once and the Parquet
 * encoders write the lanes as they are.
 *
 * <ul>
 *   <li>{@code fixed(L)}: Spark's binary (a {@link VecType#UTF8}-shaped lane)
 *       becomes Arrow's {@code FixedSizeBinary(L)}. A value of another length
 *       fails the write, as it does in Iceberg's writer.</li>
 *   <li>{@code uuid}: Spark's string (the {@code 8-4-4-4-12} text form) becomes
 *       {@code FixedSizeBinary(16)}, the UUID's big-endian bytes, as Arrow's
 *       canonical {@code arrow.uuid} extension and Iceberg's file format hold
 *       it. Text that is not a UUID fails the write, as {@code UUID.fromString}
 *       fails it in Iceberg's writer.</li>
 * </ul>
 *
 * Columns that need no conversion pass through untouched.
 */
final class SparkLanes {

    private SparkLanes() {}

    /**
     * Whether any field of {@code schema}, at any depth, needs the conversion
     * or the stripping {@link #toArrow} does -- a {@code fixed} or {@code uuid}
     * lane to convert, or a v3 {@code unknown} field to drop.
     */
    static boolean needed(Schema schema) {
        return schema.columns().stream()
                .anyMatch(f -> f.type().typeId() == Type.TypeID.UNKNOWN || needed(f.type()));
    }

    private static boolean needed(Type type) {
        return switch (type.typeId()) {
            case FIXED, UUID, UNKNOWN -> true;
            case STRUCT, LIST, MAP ->
                    type.asNestedType().fields().stream()
                            .anyMatch(f -> f.type().typeId() == Type.TypeID.UNKNOWN || needed(f.type()));
            default -> false;
        };
    }

    /**
     * {@code columns} (one per top-level field of {@code schema}) as the
     * writer's physical columns, with every converted lane allocated in {@code
     * arena}; the caller keeps the arena open while the result is written.
     *
     * <p>A v3 {@code unknown} field is DROPPED -- the top-level column is
     * omitted from the result, and an unknown struct child is omitted from the
     * struct's children -- so the result lines up one-for-one with the Parquet
     * schema Iceberg writes ({@code ParquetSchemaUtil.convert} /
     * {@code TypeToMessageType}, which omit unknown fields the same way). An
     * unknown list element or map key/value cannot occur here: such a write is
     * declined before it reaches the writer ({@code
     * IcebergDataCommitBridge.unsupportedType}).
     */
    static List<VectorBuffers> toArrow(Schema schema, List<? extends VectorBuffers> columns, Arena arena) {
        List<Types.NestedField> fields = schema.columns();
        List<VectorBuffers> out = new ArrayList<>(columns.size());
        for (int c = 0; c < columns.size(); c++) {
            Type type = fields.get(c).type();
            if (type.typeId() == Type.TypeID.UNKNOWN) {
                // Not stored in a data file: the column is dropped so the result matches Iceberg's Parquet schema.
                continue;
            }
            out.add(convert(type, columns.get(c), arena));
        }
        return out;
    }

    private static VectorBuffers convert(Type type, VectorBuffers v, Arena arena) {
        if (!needed(type)) {
            return v;
        }
        switch (type.typeId()) {
            case FIXED:
                return toFixed(v, ((Types.FixedType) type).length(), arena);
            case UUID:
                return toUuid(v, arena);
            case UNKNOWN:
                // Unreachable: a top-level/struct unknown is dropped by the caller, and an unknown list element
                // or map value is declined before the write reaches here.
                throw new IllegalArgumentException("the v3 unknown type is not stored in data files");
            case STRUCT:
                {
                    NestedVectorBuffers s = (NestedVectorBuffers) v;
                    List<Types.NestedField> fs = type.asStructType().fields();
                    List<VectorBuffers> children = new ArrayList<>(fs.size());
                    for (int k = 0; k < fs.size(); k++) {
                        if (fs.get(k)
                              .type()
                              .typeId()
                                == Type.TypeID.UNKNOWN) {
                            // Dropped, as Iceberg's TypeToMessageType omits an unknown struct field from the group.
                            continue;
                        }
                        children.add(
                                convert(fs.get(k).type(),
                                        s.children().get(k), arena));
                    }
                    return NestedVectorBuffers.struct(s.length(), s.validity(), children);
                }
            case LIST:
                {
                    NestedVectorBuffers l = (NestedVectorBuffers) v;
                    return NestedVectorBuffers.list(
                            l.length(),
                            l.validity(),
                            l.offsets(),
                            convert(type.asListType().elementType(),
                                    l.children().get(0), arena));
                }
            case MAP:
                {
                    NestedVectorBuffers m = (NestedVectorBuffers) v;
                    Types.MapType mt = type.asMapType();
                    return NestedVectorBuffers.map(
                            m.length(),
                            m.validity(),
                            m.offsets(),
                            convert(mt.keyType(),
                                    m.children().get(0), arena),
                            convert(mt.valueType(),
                                    m.children().get(1), arena));
                }
            default:
                return v;
        }
    }

    /**
     * A binary lane (offsets and bytes, or dictionary ids) as a FIXED_BINARY
     * lane of {@code width}.
     */
    private static VectorBuffers toFixed(VectorBuffers v, int width, Arena arena) {
        if (v.type() != VecType.UTF8) {
            throw new IllegalArgumentException("fixed[" + width + "] from a " + v.type() + " lane");
        }
        int n = v.length();
        FixedBinaryVectorBuffers out = FixedBinaryVectorBuffers.allocate(arena, n, width, v.validity());
        MemorySegment data = out.data();
        for (int i = 0; i < n; i++) {
            if (v.isNull(i)) {
                continue;
            }
            byte[] b = v.getUtf8Bytes(i);
            if (b.length != width) {
                throw new IllegalArgumentException("a value of " + b.length + " bytes for a fixed[" + width + "] column");
            }
            MemorySegment.copy(b, 0, data, ValueLayout.JAVA_BYTE, (long) i * width,
                    width);
        }
        return out;
    }

    /**
     * A string lane of UUIDs in text form as a FIXED_BINARY(16) lane of their
     * big-endian bytes.
     */
    private static VectorBuffers toUuid(VectorBuffers v, Arena arena) {
        if (v.type() != VecType.UTF8) {
            throw new IllegalArgumentException("uuid from a " + v.type() + " lane");
        }
        int n = v.length();
        FixedBinaryVectorBuffers out = FixedBinaryVectorBuffers.allocate(arena, n, 16, v.validity());
        MemorySegment data = out.data();
        for (int i = 0; i < n; i++) {
            if (!v.isNull(i)) {
                uuidBytes(v.getUtf8Bytes(i), data, (long) i << 4);
            }
        }
        return out;
    }

    /**
     * Writes the 16 big-endian bytes of the UUID in {@code text} ({@code 8-4-4-4-12}
     * hex digits, either case) at {@code at}.
     */
    static void uuidBytes(byte[] text, MemorySegment out, long at) {
        if (text.length != 36
                || text[8] != '-'
                || text[13] != '-'
                || text[18] != '-'
                || text[23] != '-') {
            throw notUuid(text);
        }
        int k = 0;
        int i = 0;
        while (i < 36) {
            if (i == 8
                    || i == 13
                    || i == 18
                    || i == 23) {
                i++; // the dash
                continue;
            }
            int hi = Character.digit(text[i], 16);
            int lo = Character.digit(text[i + 1], 16);
            if (hi < 0 || lo < 0) {
                throw notUuid(text);
            }
            out.set(ValueLayout.JAVA_BYTE, at + k++, (byte) (hi << 4 | lo));
            i += 2;
        }
    }

    private static IllegalArgumentException notUuid(byte[] text) {
        return new IllegalArgumentException("not a UUID: '" + new String(text, java.nio.charset.StandardCharsets.UTF_8) + "'");
    }
}
