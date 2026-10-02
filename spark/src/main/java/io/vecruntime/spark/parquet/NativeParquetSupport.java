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

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;

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
 * ({@code VectorParquetScanExec.physicalMatches}). Deliberately excluded: {@code
 * TIMESTAMP}/{@code TIMESTAMP_NTZ} (INT64-physical but need rebase/int96
 * handling verified per file -- deferred), {@code FLOAT}, {@code BINARY},
 * wide decimals ({@code p>18}), and every nested/complex type.
 */
public final class NativeParquetSupport {

    private NativeParquetSupport() {}

    /** Whether the native reader can decode a column of this type. */
    public static boolean isReadable(DataType dt) {
        if (dt instanceof IntegerType || dt instanceof DateType) {
            return true; // INT32 physical, INT32 lane
        }
        if (dt instanceof ByteType || dt instanceof ShortType) {
            return true; // INT32 physical, INT32 lane narrowed to the declared width (VectorNarrowIntColumnVector)
        }
        if (dt instanceof LongType) {
            return true; // INT64 physical, INT64 lane
        }
        if (dt instanceof BooleanType) {
            return true; // BOOLEAN physical, BOOL lane (bitmap)
        }
        if (dt instanceof DoubleType) {
            return true; // DOUBLE physical, FLOAT64 lane
        }
        if (dt instanceof StringType st) {
            return st.isUTF8BinaryCollation(); // UTF8; a collated string has no lane
        }
        if (dt instanceof DecimalType d) {
            // p <= 18: the INT64 lane (INT32 / INT64 widened, or FIXED_LEN_BYTE_ARRAY / BINARY bytes converted);
            // p > 18: the DECIMAL128 lane (FIXED_LEN_BYTE_ARRAY / BINARY bytes converted). Each file's physical
            // type and decimal annotation are checked at open (VectorParquetScanExec.physicalMatches).
            return d.precision() <= 38;
        }
        return false;
    }
}
