/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import io.vecruntime.spark.adapter.TypeMapping;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;

/**
 * The SINGLE source of truth for which Spark types the native Parquet reader
 * ({@link NativeParquetColumnReader} / {@code ColumnChunkDecoder}) can actually
 * decode. Both the planner ({@code VectorParquetScanPlanner.reason}) and the
 * reader consult this, so the plan-time support check and the runtime decoder
 * can never drift -- a type the planner admits is exactly a type the reader
 * decodes.
 *
 * <p>Slice 1 decodes the fixed lanes INT32 (also {@code DATE}), INT64, FLOAT64,
 * the {@code INT64}-lane narrow decimal ({@code p<=18}), and UTF8. Deliberately
 * excluded: {@code BOOLEAN} (bit-packed, no decode path yet), {@code
 * TINYINT}/{@code SMALLINT} (INT32-physical but need the narrow output wrapper
 * -- deferred), {@code TIMESTAMP}/{@code TIMESTAMP_NTZ} (INT64-physical but
 * need rebase/int96 handling verified per file -- deferred), {@code BINARY},
 * wide decimals ({@code p>18}), and every nested/complex type.
 */
public final class NativeParquetSupport {

    private NativeParquetSupport() {}

    /** Whether the native reader can decode a column of this type. */
    public static boolean isReadable(DataType dt) {
        if (dt instanceof IntegerType || dt instanceof DateType) {
            return true; // INT32 physical, INT32 lane
        }
        if (dt instanceof LongType) {
            return true; // INT64 physical, INT64 lane
        }
        if (dt instanceof DoubleType) {
            return true; // DOUBLE physical, FLOAT64 lane
        }
        if (dt instanceof StringType st) {
            return st.isUTF8BinaryCollation(); // UTF8; a collated string has no lane
        }
        if (dt instanceof DecimalType d) {
            return d.precision() <= TypeMapping.MAX_DECIMAL_PRECISION; // INT32/INT64-physical narrow decimal
        }
        return false;
    }
}
