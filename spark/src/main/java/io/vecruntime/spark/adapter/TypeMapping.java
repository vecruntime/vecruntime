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
package io.vecruntime.spark.adapter;

import io.vecruntime.kernels.VecType;
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

/** Spark logical type to kernel physical type. */
public final class TypeMapping {

    private TypeMapping() {}

    /** Physical type for a supported Spark type, or {@code null} if unsupported. */
    public static VecType vecTypeOf(DataType dt) {
        if (dt instanceof IntegerType
                || dt instanceof DateType
                || dt instanceof ByteType
                || dt instanceof ShortType) {
            // TINYINT and SMALLINT ride INT32 lanes (#327): every value fits, and the output wrapper
            // (VectorNarrowIntColumnVector) hands Spark the declared type back.
            return VecType.INT32;
        }
        if (dt instanceof LongType || dt instanceof TimestampType || dt instanceof TimestampNTZType) {
            // TIMESTAMP_NTZ is the same micros as TIMESTAMP, without a zone (#705): kernels compare, hash and
            // sort it as a long; expressions that read the session time zone accept only TimestampType.
            return VecType.INT64;
        }
        if (isCarriedLong(dt)) {
            return VecType.INT64;
        }
        if (dt instanceof org.apache.spark.sql.types.YearMonthIntervalType) {
            // #812: months, an int; see isCarriedLong.
            return VecType.INT32;
        }
        if (dt instanceof DoubleType) {
            return VecType.FLOAT64;
        }
        if (dt instanceof FloatType) {
            // FLOAT rides the FLOAT64 lane (#712): every float widens exactly, comparisons, min / max and
            // sorts are exact on the widened value, the compiler rounds float arithmetic and casts back to
            // single precision after every operation, and the output wrapper (VectorFloatColumnVector)
            // hands Spark the declared type back.
            return VecType.FLOAT64;
        }
        if (dt instanceof BooleanType) {
            return VecType.BOOL;
        }
        if (dt instanceof StringType) {
            // Kernels compare, group and sort bytes, which is a UTF8_BINARY string's order and equality. A
            // collated string (STRING COLLATE UTF8_LCASE, ...) is the same bytes, so it rides the UTF8 lane to be
            // carried -- adapted, copied, gathered, shuffled, broadcast -- but no kernel computes on it (#705):
            // isSupported refuses it, and every key and comparison site asks isCollated.
            return VecType.UTF8;
        }
        if (dt instanceof BinaryType) {
            // BINARY (#705): offsets and bytes, the UTF8 lane's layout. Equality, ordering (unsigned bytes) and
            // grouping are the same as a binary-collation string's; functions that read characters accept only
            // StringType (ExpressionCompiler), so over binary they stay Spark's.
            return VecType.UTF8;
        }
        if (dt instanceof DecimalType d) {
            // Unscaled value in long lanes, like Spark's own WritableColumnVector for Decimal(p <= 18);
            // wider decimals are two-limb DECIMAL128 lanes (#28, option 1).
            return d.precision() <= MAX_DECIMAL_PRECISION ? VecType.INT64 : VecType.DECIMAL128;
        }
        return null;
    }

    /**
     * Whether values of this type have a physical lane: they can be adapted
     * from a Spark vector, compacted, gathered, appended and emitted. Wider
     * than {@link #isSupported}, which also asks that the kernels compute on
     * the lane -- the DECIMAL128 lane can be carried (#257) but no expression
     * or key kernel reads it yet (#258, #259), so operators consult this only
     * where they merely move a column.
     */
    public static boolean hasLane(DataType dt) {
        return vecTypeOf(dt) != null;
    }

    /** Decimals wider than this have no 64-bit representation and fall back. */
    public static final int MAX_DECIMAL_PRECISION = 18;

    /**
     * #812: Spark 4.1's {@code time} (nanoseconds of the day) and {@code
     * interval day to second} (microseconds) are longs, and {@code interval
     * year to month} (months, {@link #vecTypeOf}) an int, in every Spark vector
     * and row. On those lanes the kernels carry, compare, hash, sort, group and
     * join them exactly as Spark does -- a long's order is the value's -- so
     * these are lanes. Nothing else: arithmetic on them has its own semantics
     * in Spark (an interval's {@code +} overflows with an error; {@code time}
     * takes no {@code +}), and casts and functions read them as times and
     * intervals, so {@link #isComputeRestricted} keeps every expression but
     * references, literals, comparisons and null checks over them Spark's.
     */
    public static boolean isCarriedLong(DataType dt) {
        return dt instanceof org.apache.spark.sql.types.TimeType || dt instanceof org.apache.spark.sql.types.DayTimeIntervalType;
    }

    /**
     * #812: a type with a lane on which only carrying, comparison, null checks
     * and keys run (isCarriedLong).
     */
    public static boolean isComputeRestricted(DataType dt) {
        return isCarriedLong(dt) || dt instanceof org.apache.spark.sql.types.YearMonthIntervalType;
    }

    public static boolean isDecimal(DataType dt) {
        return dt instanceof DecimalType;
    }

    /**
     * Whether the kernels compute on this type (a lane the expression and key
     * kernels read).
     */
    public static boolean isSupported(DataType dt) {
        VecType t = vecTypeOf(dt);
        return t != null && t != VecType.DECIMAL128 && !isCollated(dt);
    }

    /**
     * {@code dt} with every collated string replaced by a plain one (#705): the
     * layout that carries its bytes, for code that moves columns and picks its
     * encoding by the type (the shuffle's IPC streams). The operators'
     * attributes keep the collation; only the carrier forgets it.
     */
    public static DataType withoutCollation(DataType dt) {
        if (dt instanceof StringType) {
            return org.apache.spark.sql.types.DataTypes.StringType;
        }
        if (dt instanceof org.apache.spark.sql.types.ArrayType a) {
            return new org.apache.spark.sql.types.ArrayType(withoutCollation(a.elementType()), a.containsNull());
        }
        if (dt instanceof org.apache.spark.sql.types.MapType m) {
            return new org.apache.spark.sql.types.MapType(withoutCollation(m.keyType()), withoutCollation(m.valueType()), m.valueContainsNull());
        }
        if (dt instanceof org.apache.spark.sql.types.StructType s) {
            org.apache.spark.sql.types.StructField[] fields = s.fields().clone();
            for (int i = 0; i < fields.length; i++) {
                org.apache.spark.sql.types.StructField f = fields[i];
                fields[i] = new org.apache.spark.sql.types.StructField(f.name(), withoutCollation(f.dataType()), f.nullable(),
                        f.metadata());
            }
            return new org.apache.spark.sql.types.StructType(fields);
        }
        return dt;
    }

    /**
     * Whether {@code dt} is, or holds anywhere inside an array, map or struct,
     * a string whose collation is not UTF8_BINARY (#705): bytes then do not
     * decide its equality, order or hash, so it may be carried on the UTF8 lane
     * but never keyed, compared, hashed or ordered by the kernels.
     */
    public static boolean isCollated(DataType dt) {
        if (dt instanceof StringType st) {
            return !st.isUTF8BinaryCollation();
        }
        if (dt instanceof org.apache.spark.sql.types.ArrayType a) {
            return isCollated(a.elementType());
        }
        if (dt instanceof org.apache.spark.sql.types.MapType m) {
            return isCollated(m.keyType()) || isCollated(m.valueType());
        }
        if (dt instanceof org.apache.spark.sql.types.StructType s) {
            for (org.apache.spark.sql.types.StructField f : s.fields()) {
                if (isCollated(f.dataType())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isWideDecimal(DataType dt) {
        return dt instanceof DecimalType d && d.precision() > MAX_DECIMAL_PRECISION;
    }

    /**
     * The nested lane of an array, map or struct type (#705): {@link
     * VecType#LIST}, {@link VecType#MAP} or {@link VecType#STRUCT} when every
     * leaf below it has a lane ({@link #hasLane}) or is itself such a nested
     * type; {@code null} otherwise (and for any non-nested type). Deliberately
     * separate from {@link #vecTypeOf}: operators keep carrying nested columns
     * as Spark's vectors, and only the consumers of nested lanes (nested
     * expressions, the shuffle) ask for one.
     */
    public static VecType nestedTypeOf(DataType dt) {
        if (dt instanceof org.apache.spark.sql.types.ArrayType a) {
            return carriesNested(a.elementType()) ? VecType.LIST : null;
        }
        if (dt instanceof org.apache.spark.sql.types.MapType m) {
            return carriesNested(m.keyType()) && carriesNested(m.valueType())
                    ? VecType.MAP
                    : null;
        }
        if (dt instanceof org.apache.spark.sql.types.StructType s) {
            for (org.apache.spark.sql.types.StructField f : s.fields()) {
                if (!carriesNested(f.dataType())) {
                    return null;
                }
            }
            return VecType.STRUCT;
        }
        return null;
    }

    /**
     * Whether a value of {@code dt} can sit inside a nested lane: a lane, or a
     * nested lane.
     */
    public static boolean carriesNested(DataType dt) {
        return hasLane(dt) || nestedTypeOf(dt) != null;
    }

    /**
     * The lane of any type that has one, flat ({@link #vecTypeOf}) or nested
     * ({@link #nestedTypeOf}).
     */
    public static VecType laneOf(DataType dt) {
        VecType t = vecTypeOf(dt);
        return t != null ? t : nestedTypeOf(dt);
    }
}
