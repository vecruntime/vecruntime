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
package io.vecruntime.iceberg.read;

import io.vecruntime.kernels.VecType;
import io.vecruntime.parquet.read.LeafReadSpec;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * How a Parquet leaf is read as an Iceberg primitive type (#901 R1): the lane
 * and every conversion between the stored values and the Iceberg type,
 * including the promotions Iceberg allows after a file was written ({@code int
 * -> long}, {@code float -> double}, a decimal's precision widened). Decided
 * from the Iceberg type, with no Spark type in the middle; the result equals
 * what the Spark adapter decides for the Spark type Iceberg maps the Iceberg
 * type to (IcebergReadSpecsSuite checks every pairing).
 */
public final class IcebergLeafSpecs {

    private IcebergLeafSpecs() {}

    /** Decimals up to this precision are their unscaled values on an INT64 lane. */
    static final int MAX_LONG_DECIMAL = 18;

    /**
     * The read spec of a {@code stored} leaf read as {@code expected}, or null
     * when the pairing is not one the readers decode (or not a promotion
     * Iceberg allows): the caller then keeps Iceberg's reader.
     */
    public static LeafReadSpec of(Type.PrimitiveType expected, PrimitiveType stored) {
        PrimitiveTypeName p = stored.getPrimitiveTypeName();
        LogicalTypeAnnotation a = stored.getLogicalTypeAnnotation();
        switch (expected.typeId()) {
            case BOOLEAN:
                return p == PrimitiveTypeName.BOOLEAN ? LeafReadSpec.onLane(VecType.BOOL)
                        .arrowType(ArrowType.Bool.INSTANCE)
                        .build()
                        : null;
            case INTEGER:
                return p == PrimitiveTypeName.INT32 && !unsigned32(a)
                        ? LeafReadSpec.onLane(VecType.INT32)
                                .arrowType(new ArrowType.Int(32, true))
                                .build()
                        : null;
            case LONG:
                if (p == PrimitiveTypeName.INT64 || p == PrimitiveTypeName.INT32) {
                    // INT32 is an int promoted to long after the file was written (or an unsigned int32).
                    return LeafReadSpec.onLane(VecType.INT64)
                            .widen(unsigned32(a) ? LeafReadSpec.WIDEN_UINT32_TO_LONG : LeafReadSpec.WIDEN_NONE)
                            .arrowType(new ArrowType.Int(64, true))
                            .build();
                }
                return null;
            case FLOAT:
                return p == PrimitiveTypeName.FLOAT ? LeafReadSpec.onLane(VecType.INT32)
                        .float32(true)
                        .arrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))
                        .build()
                        : null;
            case DOUBLE:
                if (p == PrimitiveTypeName.DOUBLE || p == PrimitiveTypeName.FLOAT) {
                    return LeafReadSpec.onLane(VecType.FLOAT64)
                            .widen(p == PrimitiveTypeName.FLOAT ? LeafReadSpec.WIDEN_FLOAT_TO_DOUBLE : LeafReadSpec.WIDEN_NONE)
                            .arrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                            .build();
                }
                return null;
            case DATE:
                return p == PrimitiveTypeName.INT32 ? LeafReadSpec.onLane(VecType.INT32)
                        .dateLane(true)
                        .arrowType(new ArrowType.Date(DateUnit.DAY))
                        .build()
                        : null;
            case TIMESTAMP:
                {
                    if (p != PrimitiveTypeName.INT64) {
                        return null;
                    }
                    boolean zoned = ((Types.TimestampType) expected).shouldAdjustToUTC();
                    boolean millis = a instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation t && t.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS;
                    if (a instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation t && t.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS) {
                        return null;
                    }
                    return LeafReadSpec.onLane(VecType.INT64)
                            .millis(millis)
                            .timestampLane(zoned)
                            .arrowType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, zoned ? "UTC" : null))
                            .build();
                }
            case STRING:
                return p == PrimitiveTypeName.BINARY ? LeafReadSpec.onLane(VecType.UTF8)
                        .dictionaryIdsAllowed(true)
                        .arrowType(ArrowType.Utf8.INSTANCE)
                        .build()
                        : null;
            case BINARY:
            case FIXED:
                return p == PrimitiveTypeName.BINARY || p == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                        ? LeafReadSpec.onLane(VecType.UTF8)
                                .arrowType(ArrowType.Utf8.INSTANCE)
                                .build()
                        : null;
            case DECIMAL:
                return decimal((Types.DecimalType) expected, stored);
            default:
                // time, uuid, timestamp_ns, variant, geometry, unknown: Iceberg's reader.
                return null;
        }
    }

    private static LeafReadSpec decimal(Types.DecimalType d, PrimitiveType stored) {
        PrimitiveTypeName p = stored.getPrimitiveTypeName();
        LogicalTypeAnnotation a = stored.getLogicalTypeAnnotation();
        // Iceberg promotes a decimal's precision only; the scale stays, so the unscaled values do not move.
        int fileScale = a instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation da ? da.getScale() : 0;
        if (fileScale != d.scale()) {
            return null;
        }
        boolean wide = d.precision() > MAX_LONG_DECIMAL;
        boolean integral = p == PrimitiveTypeName.INT32 || p == PrimitiveTypeName.INT64;
        if (!integral && p != PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && p != PrimitiveTypeName.BINARY) {
            return null;
        }
        if (!wide && !integral) {
            // A FIXED_LEN_BYTE_ARRAY or BYTE_ARRAY decimal of up to 18 digits is not an INT64 lane.
            return null;
        }
        return LeafReadSpec.onLane(wide ? VecType.DECIMAL128 : VecType.INT64)
                .widen(wide && integral ? LeafReadSpec.WIDEN_LONG_TO_DEC128 : LeafReadSpec.WIDEN_NONE)
                .decimal(true)
                .arrowType(wide ? new ArrowType.Decimal(d.precision(), d.scale(), 128) : new ArrowType.Int(64, true))
                .build();
    }

    private static boolean unsigned32(LogicalTypeAnnotation a) {
        return a instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation i && !i.isSigned() && i.getBitWidth() == 32;
    }
}
