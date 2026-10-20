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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A file's calendar handling for its date and timestamp leaves (#900 phase 2):
 * the engine decides whether a file written in the hybrid Julian calendar is
 * rebased, refused or read as is, and how an INT96 timestamp is converted --
 * Spark through its {@code RebaseDateTime} and the file's footer keys, another
 * engine its own way. A reader calls it once per decoded batch, never per
 * value.
 *
 * <p>{@link #NONE} reads every value as stored: what a file in the proleptic
 * Gregorian calendar needs.
 */
public interface CalendarRebase {

    /**
     * A batch of {@code n} dates, INT32 days, in {@code data}; null rows per
     * {@code validity} (null: none).
     */
    void fixDays(MemorySegment data, MemorySegment validity, int n);

    /**
     * A batch of {@code n} dates decoded into INT64 slots ({@code data}),
     * turned into a timestamp's micros (a DATE read as TIMESTAMP_NTZ) after any
     * rebase of the days.
     */
    void daysToMicros(MemorySegment data, MemorySegment validity, int n);

    /**
     * A batch of {@code n} timestamps, INT64 micros; {@code int96}: they were
     * stored as INT96.
     */
    void fixMicros(MemorySegment data, MemorySegment validity, int n,
                   boolean int96);

    /** No rebase and no conversion. */
    CalendarRebase NONE = new CalendarRebase() {
        private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
        private static final long MICROS_PER_DAY = 86_400_000_000L;

        @Override
        public void fixDays(MemorySegment data, MemorySegment validity, int n) {}

        @Override
        public void daysToMicros(MemorySegment data, MemorySegment validity, int n) {
            for (int i = 0; i < n; i++) {
                if (validity != null && (validity.get(ValueLayout.JAVA_BYTE, i >>> 3) & (1 << (i & 7))) == 0) {
                    continue;
                }
                long at = (long) i << 3;
                data.set(LE_LONG, at, Math.multiplyExact((long) (int) data.get(LE_LONG, at), MICROS_PER_DAY));
            }
        }

        @Override
        public void fixMicros(MemorySegment data, MemorySegment validity, int n,
                              boolean int96) {}
    };
}
