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

import io.vecruntime.kernels.VecType;
import io.vecruntime.spark.adapter.TypeMapping;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

/**
 * The SINGLE source of truth for which Spark types the native Parquet reader
 * ({@link NativeParquetColumnReader} / {@code ColumnChunkDecoder}) can actually
 * decode. Both the planner ({@code VectorParquetScanPlanner.reason}) and the
 * reader consult this, so the plan-time support check and the runtime decoder
 * can never drift -- a type the planner admits is exactly a type the reader
 * decodes.
 *
 * <p>The native reader decodes the fixed lanes INT32 (also {@code DATE}, and
 * {@code TINYINT}/{@code SMALLINT} narrowed to their width on the INT32 lane),
 * INT64, FLOAT64, BOOL, the {@code INT64}-lane narrow decimal ({@code p<=18}),
 * and UTF8. Each file's physical types are checked against these lanes at open
 * ({@code VectorParquetScanExec.physicalMatches}), and so is each file's
 * calendar-rebase mode. TIMESTAMP is the INT64 lane of micros (INT64 {@code
 * MICROS}, or {@code MILLIS} scaled), and INT96 is converted at flush.
 * TIMESTAMP_NTZ, BINARY and FLOAT have engine lanes (INT64, UTF8, FLOAT64;
 * #705, #712); a collated string rides the UTF8 lane. TIME (#787) has no
 * engine lane: it is decoded on the INT64 lane of the same layout ({@link
 * #decodeLane}) and emitted as Spark's Arrow {@code TimeNanoVector}, which
 * operators carry as a column without a lane. Lists, maps and structs are
 * readable when every leaf below them is (#568).
 */
public final class NativeParquetSupport {

    private NativeParquetSupport() {}

    /**
     * #812: whether the native reader can decode a top-level column of this
     * type: {@link #isReadable}, or a struct of the paths Spark's
     * PushVariantIntoScan extracts from a variant column (assembled per row from
     * the variant group, {@code NativeParquetVariantColumn}).
     */
    public static boolean isReadableColumn(DataType dt) {
        return isVariantStruct(dt) || isReadable(dt);
    }

    /** #812: Spark's PushVariantIntoScan struct of extracted variant paths. */
    public static boolean isVariantStruct(DataType dt) {
        return dt instanceof org.apache.spark.sql.types.StructType st && org.apache.spark.sql.execution.datasources.VariantMetadata$.MODULE$.isVariantStruct(st);
    }

    /** #812: whether a type holds a variant, or a pushed variant struct, anywhere. */
    public static boolean containsVariant(DataType dt) {
        if (dt instanceof org.apache.spark.sql.types.VariantType || isVariantStruct(dt)) {
            return true;
        }
        if (dt instanceof org.apache.spark.sql.types.ArrayType at) {
            return containsVariant(at.elementType());
        }
        if (dt instanceof org.apache.spark.sql.types.MapType mt) {
            return containsVariant(mt.keyType()) || containsVariant(mt.valueType());
        }
        if (dt instanceof org.apache.spark.sql.types.StructType st) {
            for (org.apache.spark.sql.types.StructField f : st.fields()) {
                if (containsVariant(f.dataType())) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Whether the native reader can decode a column of this type. */
    public static boolean isReadable(DataType dt) {
        // #568: nested types are readable when every leaf below them is: lists, maps and structs at any depth.
        if (dt instanceof org.apache.spark.sql.types.ArrayType at) {
            return isReadable(at.elementType());
        }
        if (dt instanceof org.apache.spark.sql.types.MapType mt) {
            return isReadable(mt.keyType()) && isReadable(mt.valueType());
        }
        if (dt instanceof org.apache.spark.sql.types.StructType st) {
            if (st.fields().length == 0) {
                return false;
            }
            // Spark's PushVariantIntoScan (#787): a struct of the paths a query extracts from a variant column. Read
            // natively at the top level only (#812, isReadableColumn); below it, Spark's scan.
            if (org.apache.spark.sql.execution.datasources.VariantMetadata$.MODULE$.isVariantStruct(st)) {
                return false;
            }
            for (org.apache.spark.sql.types.StructField f : st.fields()) {
                if (!isReadable(f.dataType())) {
                    return false;
                }
            }
            return true;
        }
        if (dt instanceof IntegerType || dt instanceof DateType) {
            return true; // INT32 physical, INT32 lane
        }
        if (dt instanceof ByteType || dt instanceof ShortType) {
            return true; // INT32 physical, INT32 lane narrowed to the declared width (VectorNarrowIntColumnVector)
        }
        if (dt instanceof LongType) {
            return true; // INT64 physical, INT64 lane
        }
        if (dt instanceof TimestampType) {
            return true; // INT64 MICROS (or MILLIS, scaled) physical, INT64 lane of micros; INT96 falls over per file
        }
        if (dt instanceof BooleanType) {
            return true; // BOOLEAN physical, BOOL lane (bitmap)
        }
        if (dt instanceof org.apache.spark.sql.types.VariantType) {
            // #787: an unshredded variant group, read as the struct of its two binaries (variantAsStruct); a file
            // with a shredded variant falls over to Spark's reader (hasShreddedVariant).
            return true;
        }
        if (dt instanceof DoubleType) {
            return true; // DOUBLE physical, FLOAT64 lane
        }
        if (dt instanceof StringType) {
            // UTF8; a collated string (#568) has no engine lane but the same bytes: decoded on the UTF8 lane and
            // emitted as an Arrow string vector, which operators carry as a column without a lane (decodeLane).
            return true;
        }
        if (dt instanceof DecimalType d) {
            // p <= 18: the INT64 lane (INT32 / INT64 widened, or FIXED_LEN_BYTE_ARRAY / BINARY bytes converted);
            // p > 18: the DECIMAL128 lane (FIXED_LEN_BYTE_ARRAY / BINARY bytes converted). Each file's physical
            // type and decimal annotation are checked at open (VectorParquetScanExec.physicalMatches).
            return d.precision() <= 38;
        }
        // No engine lane (option A of the #559 note; today TIME, #787): decoded on a lane of the same layout and
        // emitted as Spark's Arrow vector, so operators carry the column as a foreign column.
        return decodeLane(dt) != null;
    }

    /**
     * The decoder lane for a Spark type: {@link TypeMapping#vecTypeOf} for a
     * type with an engine lane, else the lane whose bytes a column without one
     * shares -- TIME on INT64 (nanoseconds of the day, #787). FLOAT decodes its
     * 4-byte IEEE bits on INT32 into a {@code Float4Vector}, which operators
     * widen onto the FLOAT64 lane (#712); BINARY and collated strings share the
     * UTF8 lane. The scan emits TIME as an Arrow {@code TimeNanoVector}, which
     * Spark's {@code
     * ArrowColumnVector} reads; operators pass it through as a column without a
     * lane, as they do over Spark's own scan. {@code null}: not decodable.
     */
    public static VecType decodeLane(DataType dt) {
        if (dt instanceof FloatType) {
            return VecType.INT32;
        }
        if (dt instanceof BinaryType) {
            return VecType.UTF8;
        }
        if (dt instanceof StringType) {
            return VecType.UTF8; // a collated string too: the same bytes, no engine lane (#568)
        }
        if (dt instanceof TimestampNTZType) {
            return VecType.INT64;
        }
        // #812: ANSI intervals, as stored: months on INT32 (Arrow IntervalYearVector), micros on INT64 (Arrow
        // DurationVector); Spark's ArrowColumnVector reads both.
        if (dt instanceof org.apache.spark.sql.types.YearMonthIntervalType) {
            return VecType.INT32;
        }
        if (dt instanceof org.apache.spark.sql.types.DayTimeIntervalType) {
            return VecType.INT64;
        }
        if (dt instanceof org.apache.spark.sql.types.TimeType) {
            // #787: Spark's nanoseconds of the day; INT64 TIME(MICROS) scaled by 1000 at decode, as Spark's
            // LongAsNanosUpdater does. No engine lane: emitted as a TimeNanoVector (ArrowOutput.newVector).
            return VecType.INT64;
        }
        return TypeMapping.vecTypeOf(dt);
    }

    /**
     * True for a type the scan decodes but no engine lane holds (operators
     * carry it as a foreign column).
     */
    public static boolean isForeign(DataType dt) {
        return dt instanceof FloatType;
    }

    /**
     * The struct an unshredded variant group is read as (#787): its {@code
     * value} and {@code metadata} binaries, in that order -- Spark's {@code
     * ColumnVector.getVariant} reads child 0 as the value and child 1 as the
     * metadata, so the struct column the scan emits is the variant column.
     * Clipping by name puts the file's group in this order whatever order the
     * file stores it in.
     */
    public static final org.apache.spark.sql.types.StructType VARIANT_READ_TYPE = new org.apache.spark.sql.types.StructType().add("value", org.apache.spark.sql.types.DataTypes.BinaryType, false).add("metadata", org.apache.spark.sql.types.DataTypes.BinaryType, false);

    /**
     * {@code dt} with every variant, at any depth, replaced by {@link
     * #VARIANT_READ_TYPE}.
     */
    public static DataType variantAsStruct(DataType dt) {
        if (dt instanceof org.apache.spark.sql.types.VariantType) {
            return VARIANT_READ_TYPE;
        }
        if (dt instanceof org.apache.spark.sql.types.ArrayType at) {
            DataType e = variantAsStruct(at.elementType());
            return e.equals(at.elementType()) ? dt : new org.apache.spark.sql.types.ArrayType(e, at.containsNull());
        }
        if (dt instanceof org.apache.spark.sql.types.MapType mt) {
            DataType k = variantAsStruct(mt.keyType());
            DataType v = variantAsStruct(mt.valueType());
            return k.equals(mt.keyType()) && v.equals(mt.valueType())
                    ? dt
                    : new org.apache.spark.sql.types.MapType(k, v, mt.valueContainsNull());
        }
        if (dt instanceof org.apache.spark.sql.types.StructType st) {
            // #812: a pushed variant struct is the variant group's own business (fileReadSchema), left as it is.
            return isVariantStruct(st) ? dt : variantAsStruct(st);
        }
        return dt;
    }

    /**
     * {@link #variantAsStruct(DataType)} over a schema; the same instance when
     * it holds no variant.
     */
    public static org.apache.spark.sql.types.StructType variantAsStruct(org.apache.spark.sql.types.StructType st) {
        org.apache.spark.sql.types.StructField[] fs = st.fields();
        org.apache.spark.sql.types.StructField[] out = null;
        for (int i = 0; i < fs.length; i++) {
            DataType t = variantAsStruct(fs[i].dataType());
            if (!t.equals(fs[i].dataType())) {
                if (out == null) {
                    out = fs.clone();
                }
                out[i] = new org.apache.spark.sql.types.StructField(fs[i].name(), t, fs[i].nullable(),
                        fs[i].metadata());
            }
        }
        return out == null ? st : new org.apache.spark.sql.types.StructType(out);
    }

    /**
     * Whether a file schema holds a shredded variant: a group with a {@code
     * typed_value} field beside its {@code metadata} (the shredding spec's
     * layout). Reading such a variant means rebuilding values from the typed
     * columns, so the file goes to Spark's reader. Matching any such group,
     * not only the requested ones, can only send a file to Spark's reader.
     */
    public static boolean hasShreddedVariant(org.apache.parquet.schema.GroupType g) {
        if (g.containsField("typed_value") && g.containsField("metadata")) {
            return true;
        }
        for (org.apache.parquet.schema.Type t : g.getFields()) {
            if (!t.isPrimitive() && hasShreddedVariant(t.asGroupType())) {
                return true;
            }
        }
        return false;
    }
}
