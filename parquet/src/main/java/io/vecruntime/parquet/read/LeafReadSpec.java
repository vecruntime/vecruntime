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
package io.vecruntime.parquet.read;

import io.vecruntime.kernels.VecType;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * How one Parquet leaf column is read (#900 phase 2): the target lane and the
 * conversions between the stored values and what the caller asked for, decided
 * once per column by the engine's adapter (Spark builds it from its {@code
 * DataType}, Iceberg would from its schema) and never consulted per value.
 *
 * <p>Plain data on purpose. A leaf reader copies these fields into its own
 * final fields at construction and branches on them in its batch loops, exactly
 * as it branched on the {@code DataType} checks it replaces; nothing here is
 * called from a hot loop, so no call site can turn megamorphic. What the
 * Parquet column itself decides -- its physical type, a FIXED_LEN_BYTE_ARRAY
 * width, INT96 -- is read from the {@code ColumnDescriptor}, not repeated here.
 */
public final class LeafReadSpec {

    /** Read as stored. */
    public static final int WIDEN_NONE = 0;

    /** INT32 read as a double. */
    public static final int WIDEN_INT_TO_DOUBLE = 1;

    /** FLOAT read as a double. */
    public static final int WIDEN_FLOAT_TO_DOUBLE = 2;

    /** A DATE's days read as a timestamp's micros (through the calendar rebase). */
    public static final int WIDEN_DAYS_TO_MICROS = 3;

    /** An unsigned INT32 (UINT_32) read as a long, zero-extended. */
    public static final int WIDEN_UINT32_TO_LONG = 4;

    /**
     * An INT32 / INT64 read as a decimal wider than 18 digits: sign-extended to
     * a 128-bit slot.
     */
    public static final int WIDEN_LONG_TO_DEC128 = 5;

    /** An unsigned INT64 (UINT_64) read as a 128-bit decimal, zero-extended. */
    public static final int WIDEN_ULONG_TO_DEC128 = 6;

    /**
     * The Arrow type of the vectors the values are written into; null: the
     * lane's own (see {@link #arrowType()}).
     */
    private final ArrowType arrowType;

    /** The lane the values land on. */
    public final VecType lane;

    /** One of the {@code WIDEN_*} conversions, applied in place after decoding. */
    public final int widen;

    /**
     * Digits a decimal's unscaled values move left (the requested scale less
     * the stored one).
     */
    public final int rescale;

    /**
     * 8 or 16: the INT32 lane's values narrowed to a byte or a short; 0
     * otherwise.
     */
    public final int narrowBits;

    /**
     * Each present value is multiplied by 1000 (a TIMESTAMP stored as MILLIS, a
     * TIME as MICROS).
     */
    public final boolean millis;

    /** The values are a date's days (the calendar rebase applies). */
    public final boolean dateLane;

    /**
     * The values are a timestamp's micros with the session time zone (the
     * calendar rebase applies).
     */
    public final boolean timestampLane;

    /**
     * The values are a decimal (a FIXED_LEN_BYTE_ARRAY or BINARY column is
     * converted from big-endian bytes).
     */
    public final boolean decimal;

    /** A FLOAT kept as its own 4-byte bits (a 4-byte vector on the INT32 lane). */
    public final boolean float32;

    /**
     * A {@code uuid}: a FIXED_LEN_BYTE_ARRAY(16) staged on the UTF8 path whose
     * 16 big-endian bytes are formatted into the canonical lowercase
     * {@code 8-4-4-4-12} text at flush, as Iceberg's Spark reader produces it.
     */
    public final boolean uuid;

    /**
     * A UTF8 string compared by bytes: its dictionary-encoded batches may be
     * emitted as dictionary ids.
     */
    public final boolean dictionaryIdsAllowed;

    private LeafReadSpec(Builder b) {
        this.lane = b.lane;
        this.arrowType = b.arrowType;
        this.widen = b.widen;
        this.rescale = b.rescale;
        this.narrowBits = b.narrowBits;
        this.millis = b.millis;
        this.dateLane = b.dateLane;
        this.timestampLane = b.timestampLane;
        this.decimal = b.decimal;
        this.float32 = b.float32;
        this.uuid = b.uuid;
        this.dictionaryIdsAllowed = b.dictionaryIdsAllowed;
    }

    /** A spec that reads the column onto {@code lane} as stored. */
    public static Builder onLane(VecType lane) {
        return new Builder(lane);
    }

    /**
     * The Arrow type of the vectors this leaf's batches are written into: the
     * one the adapter set, else the lane's own vector type. A type with no
     * engine lane (FLOAT's bits, TIME, intervals, TIMESTAMP_NTZ, a timestamp
     * with its zone) is always set by the adapter, which knows what the engine
     * above reads.
     */
    public ArrowType arrowType() {
        if (arrowType != null) {
            return arrowType;
        }
        return switch (lane) {
            case BOOL -> ArrowType.Bool.INSTANCE;
            case INT32 -> new ArrowType.Int(32, true);
            case INT64 -> new ArrowType.Int(64, true);
            case FLOAT64 -> new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case UTF8 -> ArrowType.Utf8.INSTANCE;
            default -> throw new IllegalStateException("no Arrow type for lane " + lane + "; the adapter must set one");
        };
    }

    @Override
    public String toString() {
        return "LeafReadSpec["
                + lane
                + (widen != WIDEN_NONE ? ", widen " + widen : "")
                + (rescale != 0 ? ", rescale " + rescale : "")
                + (narrowBits != 0 ? ", narrow " + narrowBits : "")
                + (millis ? ", x1000" : "")
                + (dateLane ? ", date" : "")
                + (timestampLane ? ", timestamp" : "")
                + (decimal ? ", decimal" : "")
                + (float32 ? ", float32" : "")
                + (uuid ? ", uuid" : "")
                + (dictionaryIdsAllowed ? ", ids" : "")
                + "]";
    }

    /** Builds a {@link LeafReadSpec}; every field defaults to "as stored". */
    public static final class Builder {
        private final VecType lane;
        private ArrowType arrowType;
        private int widen = WIDEN_NONE;
        private int rescale;
        private int narrowBits;
        private boolean millis;
        private boolean dateLane;
        private boolean timestampLane;
        private boolean decimal;
        private boolean float32;
        private boolean uuid;
        private boolean dictionaryIdsAllowed;

        private Builder(VecType lane) {
            if (lane == null) {
                throw new IllegalArgumentException("no lane");
            }
            this.lane = lane;
        }

        public Builder widen(int widen) {
            if (widen < WIDEN_NONE || widen > WIDEN_ULONG_TO_DEC128) {
                throw new IllegalArgumentException("unknown widening " + widen);
            }
            this.widen = widen;
            return this;
        }

        public Builder rescale(int digits) {
            if (digits < 0 || digits > 38) {
                throw new IllegalArgumentException("rescale out of range: " + digits);
            }
            this.rescale = digits;
            return this;
        }

        public Builder narrowBits(int bits) {
            if (bits != 0 && bits != 8 && bits != 16) {
                throw new IllegalArgumentException("narrow to 8 or 16 bits, not " + bits);
            }
            this.narrowBits = bits;
            return this;
        }

        public Builder millis(boolean on) {
            this.millis = on;
            return this;
        }

        public Builder dateLane(boolean on) {
            this.dateLane = on;
            return this;
        }

        public Builder timestampLane(boolean on) {
            this.timestampLane = on;
            return this;
        }

        public Builder decimal(boolean on) {
            this.decimal = on;
            return this;
        }

        public Builder float32(boolean on) {
            this.float32 = on;
            return this;
        }

        public Builder uuid(boolean on) {
            this.uuid = on;
            return this;
        }

        public Builder dictionaryIdsAllowed(boolean on) {
            this.dictionaryIdsAllowed = on;
            return this;
        }

        /**
         * The Arrow type of the batch vectors (default: the lane's own; see
         * {@link LeafReadSpec#arrowType()}).
         */
        public Builder arrowType(ArrowType type) {
            this.arrowType = type;
            return this;
        }

        public LeafReadSpec build() {
            return new LeafReadSpec(this);
        }
    }
}
