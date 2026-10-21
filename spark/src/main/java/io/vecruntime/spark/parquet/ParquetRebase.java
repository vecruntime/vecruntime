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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.ZoneId;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;

/**
 * One file's calendar rebase and INT96 conversion (#568, work item 6), applied
 * to a decoded batch as Spark's {@code ParquetVectorUpdaterFactory} applies
 * them per value:
 *
 * <ul>
 *   <li>a date or timestamp below the last Julian switch ({@link
 *       RebaseDateTime#lastSwitchJulianDay} / {@link
 *       RebaseDateTime#lastSwitchJulianTs}) fails the read under {@code
 *       EXCEPTION} ({@link DataSourceUtils#newRebaseExceptionInRead}) and is
 *       rebased from the hybrid calendar under {@code LEGACY} ({@link
 *       RebaseDateTime#rebaseJulianToGregorianDays} / {@link
 *       RebaseDateTime#rebaseJulianToGregorianMicros}, in the file's zone);
 *       {@code CORRECTED} leaves it;
 *   <li>an INT96 timestamp is rebased by the file's INT96 mode, then converted
 *       from the file's zone to UTC ({@link DateTimeUtils#convertTz}) when the
 *       session asks for it and the writer is not parquet-mr.
 * </ul>
 *
 * Above the switch both rebase functions are the identity, so a batch whose
 * present values are all at or above it -- every batch of any modern data --
 * costs one pass of comparisons and is left as decoded.
 */
public final class ParquetRebase implements io.vecruntime.parquet.read.CalendarRebase {

    /** No rebase, no conversion: a CORRECTED file. */
    public static final ParquetRebase CORRECTED = new ParquetRebase("CORRECTED", null, "CORRECTED", null, null);

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ZoneId UTC = ZoneId.of("UTC");

    private final boolean dateLegacy;
    private final boolean dateFail;
    private final String datetimeTz;
    private final boolean int96Legacy;
    private final boolean int96Fail;
    private final String int96Tz;
    private final ZoneId convertTz;

    /**
     * Modes as {@code LegacyBehaviorPolicy} names ({@code EXCEPTION}, {@code
     * LEGACY}, {@code CORRECTED}); {@code convertTz} null when no INT96
     * time-zone conversion applies.
     */
    public ParquetRebase(String datetimeMode, String datetimeTz, String int96Mode,
                         String int96Tz, String convertTz) {
        this.dateLegacy = "LEGACY".equals(datetimeMode);
        this.dateFail = "EXCEPTION".equals(datetimeMode);
        this.datetimeTz = datetimeTz;
        this.int96Legacy = "LEGACY".equals(int96Mode);
        this.int96Fail = "EXCEPTION".equals(int96Mode);
        this.int96Tz = int96Tz;
        this.convertTz = convertTz == null ? null : ZoneId.of(convertTz);
    }

    private static boolean present(MemorySegment validity, int i) {
        return validity == null || (validity.get(ValueLayout.JAVA_BYTE, i >>> 3) & (1 << (i & 7))) != 0;
    }

    /** A batch of {@code n} dates (INT32 days) in {@code data}. */
    @Override
    public void fixDays(MemorySegment data, MemorySegment validity, int n) {
        if (!dateLegacy && !dateFail) {
            return;
        }
        int sw = RebaseDateTime.lastSwitchJulianDay();
        for (int i = 0; i < n; i++) {
            if (!present(validity, i)) {
                continue;
            }
            long at = (long) i << 2;
            int v = data.get(LE_INT, at);
            if (v < sw) {
                if (dateFail) {
                    throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
                }
                data.set(LE_INT, at, RebaseDateTime.rebaseJulianToGregorianDays(v));
            }
        }
    }

    /**
     * A DATE column read as {@code timestamp_ntz} (Spark's {@code
     * DateToTimestampNTZUpdater} / {@code ...WithRebaseUpdater}): a batch of
     * {@code n} days, each widened into an INT64 slot of {@code data}, becomes
     * micros at midnight UTC after the file's date rebase. {@link
     * Math#multiplyExact} fails a day out of range, as Spark's {@code
     * daysToMicros} does.
     */
    @Override
    public void daysToMicros(MemorySegment data, MemorySegment validity, int n) {
        int sw = RebaseDateTime.lastSwitchJulianDay();
        boolean rebase = dateLegacy || dateFail;
        for (int i = 0; i < n; i++) {
            if (!present(validity, i)) {
                continue;
            }
            long at = (long) i << 3;
            int days = (int) data.get(LE_LONG, at);
            if (rebase && days < sw) {
                if (dateFail) {
                    throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
                }
                days = RebaseDateTime.rebaseJulianToGregorianDays(days);
            }
            data.set(LE_LONG, at, Math.multiplyExact((long) days, MICROS_PER_DAY));
        }
    }

    private static final long MICROS_PER_DAY = 86_400_000_000L;

    /**
     * A batch of {@code n} timestamps (INT64 micros) in {@code data}; {@code
     * int96}: stored as INT96.
     */
    @Override
    public void fixMicros(MemorySegment data, MemorySegment validity, int n,
                          boolean int96) {
        boolean legacy = int96 ? int96Legacy : dateLegacy;
        boolean fail = int96 ? int96Fail : dateFail;
        ZoneId convert = int96 ? convertTz : null;
        if (!legacy && !fail && convert == null) {
            return;
        }
        long sw = RebaseDateTime.lastSwitchJulianTs();
        String tz = int96 ? int96Tz : datetimeTz;
        for (int i = 0; i < n; i++) {
            if (!present(validity, i)) {
                continue;
            }
            long at = (long) i << 3;
            long v = data.get(LE_LONG, at);
            long out = v;
            if (v < sw) {
                if (fail) {
                    throw DataSourceUtils.newRebaseExceptionInRead(int96 ? "Parquet INT96" : "Parquet");
                }
                if (legacy) {
                    out = RebaseDateTime.rebaseJulianToGregorianMicros(tz, v);
                }
            }
            if (convert != null) {
                out = DateTimeUtils.convertTz(out, convert, UTC);
            }
            if (out != v) {
                data.set(LE_LONG, at, out);
            }
        }
    }
}
