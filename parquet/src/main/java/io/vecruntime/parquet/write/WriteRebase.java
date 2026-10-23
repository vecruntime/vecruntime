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
package io.vecruntime.parquet.write;

/**
 * The calendar rebase a Parquet write applies to its date and timestamp values
 * (#842), the write-side mirror of {@link
 * io.vecruntime.parquet.read.CalendarRebase}: the engine decides whether a
 * value in the proleptic Gregorian calendar is written as is, rebased to the
 * hybrid Julian calendar ({@code LEGACY}), or refused when it is ancient
 * ({@code EXCEPTION}) -- Spark through its {@code RebaseDateTime} and {@code
 * DataSourceUtils}, another engine its own way. A column encoder calls the
 * method matching its column (a {@code DATE} INT32 the day method; a non-INT96
 * timestamp the datetime-micros method; an INT96 timestamp the INT96 method),
 * once per value, exactly where Spark's {@code ParquetWriteSupport} applies its
 * {@code dateRebaseFunc} / {@code timestampRebaseFunc} / {@code int96RebaseFunc}.
 *
 * <p>{@link #NONE} writes every value as stored -- the proleptic Gregorian
 * calendar, the Spark 4 default ({@code CORRECTED}).
 *
 * <p>The rebase only changes values below the Gregorian switch point, so the
 * day/micros methods leave a modern value untouched and {@link
 * #datetimeActive()} / {@link #int96Active()} let the fast writer skip the
 * whole pass for a batch (or column) that is all modern, after a cheap min
 * check.
 */
public interface WriteRebase {

    /** The proleptic Gregorian day below which a date is rebased (or refused). */
    int switchGregorianDay();

    /**
     * The proleptic Gregorian micros below which a timestamp is rebased (or
     * refused).
     */
    long switchGregorianTs();

    /**
     * Whether DATE / non-INT96 TIMESTAMP columns need any rebase (false:
     * CORRECTED).
     */
    boolean datetimeActive();

    /** Whether INT96 TIMESTAMP columns need any rebase (false: CORRECTED). */
    boolean int96Active();

    /**
     * A DATE's proleptic Gregorian days, rebased to hybrid Julian (LEGACY) or
     * refused (EXCEPTION).
     */
    int rebaseDays(int days);

    /**
     * A TIMESTAMP's proleptic Gregorian micros (non-INT96), rebased (LEGACY) or
     * refused (EXCEPTION).
     */
    long rebaseMicros(long micros);

    /**
     * An INT96 TIMESTAMP's proleptic Gregorian micros, rebased (LEGACY) or
     * refused (EXCEPTION).
     */
    long rebaseInt96Micros(long micros);

    /** No rebase and no conversion: a CORRECTED write. */
    WriteRebase NONE = new WriteRebase() {
        @Override
        public int switchGregorianDay() {
            return Integer.MIN_VALUE;
        }

        @Override
        public long switchGregorianTs() {
            return Long.MIN_VALUE;
        }

        @Override
        public boolean datetimeActive() {
            return false;
        }

        @Override
        public boolean int96Active() {
            return false;
        }

        @Override
        public int rebaseDays(int days) {
            return days;
        }

        @Override
        public long rebaseMicros(long micros) {
            return micros;
        }

        @Override
        public long rebaseInt96Micros(long micros) {
            return micros;
        }
    };
}
