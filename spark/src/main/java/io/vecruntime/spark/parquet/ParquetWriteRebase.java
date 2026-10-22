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

import io.vecruntime.parquet.write.WriteRebase;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;

/**
 * A Parquet write's calendar rebase (#842), the write-side mirror of {@link
 * ParquetRebase}: it applies the same transform Spark's {@code
 * ParquetWriteSupport} applies through {@code
 * DataSourceUtils.createDateRebaseFuncInWrite} / {@code
 * createTimestampRebaseFuncInWrite} for each mode.
 *
 * <ul>
 *   <li>{@code LEGACY}: a date's days / a timestamp's micros below the Gregorian
 *       switch ({@link RebaseDateTime#lastSwitchGregorianDay} / {@link
 *       RebaseDateTime#lastSwitchGregorianTs}) are rebased from the proleptic
 *       Gregorian to the hybrid Julian calendar ({@link
 *       RebaseDateTime#rebaseGregorianToJulianDays} / {@link
 *       RebaseDateTime#rebaseGregorianToJulianMicros}, in the session time
 *       zone, as Spark's writer uses {@code SQLConf.sessionLocalTimeZone});
 *   <li>{@code EXCEPTION}: such a value fails the write with Spark's error
 *       ({@link DataSourceUtils#newRebaseExceptionInWrite});
 *   <li>{@code CORRECTED}: every value is written as stored (use {@link
 *       WriteRebase#NONE}).
 * </ul>
 *
 * The datetime mode drives DATE and non-INT96 TIMESTAMP columns; the INT96 mode
 * drives INT96 TIMESTAMP columns, as Spark splits them.
 */
public final class ParquetWriteRebase implements WriteRebase {

    private final boolean dateLegacy;
    private final boolean dateFail;
    private final boolean int96Legacy;
    private final boolean int96Fail;
    private final String sessionTz;

    /**
     * Modes as {@code LegacyBehaviorPolicy} names ({@code CORRECTED} / {@code
     * LEGACY} / {@code EXCEPTION}).
     */
    public ParquetWriteRebase(String datetimeMode, String int96Mode, String sessionTz) {
        this.dateLegacy = "LEGACY".equals(datetimeMode);
        this.dateFail = "EXCEPTION".equals(datetimeMode);
        this.int96Legacy = "LEGACY".equals(int96Mode);
        this.int96Fail = "EXCEPTION".equals(int96Mode);
        this.sessionTz = sessionTz;
    }

    /**
     * Whether either mode is LEGACY (so the footer carries the legacy-datetime
     * key).
     */
    public boolean datetimeLegacy() {
        return dateLegacy;
    }

    /**
     * Whether the INT96 mode is LEGACY (so the footer carries the legacy-INT96
     * key).
     */
    public boolean int96Legacy() {
        return int96Legacy;
    }

    @Override
    public int switchGregorianDay() {
        return RebaseDateTime.lastSwitchGregorianDay();
    }

    @Override
    public long switchGregorianTs() {
        return RebaseDateTime.lastSwitchGregorianTs();
    }

    @Override
    public boolean datetimeActive() {
        return dateLegacy || dateFail;
    }

    @Override
    public boolean int96Active() {
        return int96Legacy || int96Fail;
    }

    @Override
    public int rebaseDays(int days) {
        if (days < RebaseDateTime.lastSwitchGregorianDay()) {
            if (dateFail) {
                throw DataSourceUtils.newRebaseExceptionInWrite("Parquet");
            }
            if (dateLegacy) {
                return RebaseDateTime.rebaseGregorianToJulianDays(days);
            }
        }
        return days;
    }

    @Override
    public long rebaseMicros(long micros) {
        if (micros < RebaseDateTime.lastSwitchGregorianTs()) {
            if (dateFail) {
                throw DataSourceUtils.newRebaseExceptionInWrite("Parquet");
            }
            if (dateLegacy) {
                return RebaseDateTime.rebaseGregorianToJulianMicros(sessionTz, micros);
            }
        }
        return micros;
    }

    @Override
    public long rebaseInt96Micros(long micros) {
        if (micros < RebaseDateTime.lastSwitchGregorianTs()) {
            if (int96Fail) {
                throw DataSourceUtils.newRebaseExceptionInWrite("Parquet INT96");
            }
            if (int96Legacy) {
                return RebaseDateTime.rebaseGregorianToJulianMicros(sessionTz, micros);
            }
        }
        return micros;
    }
}
