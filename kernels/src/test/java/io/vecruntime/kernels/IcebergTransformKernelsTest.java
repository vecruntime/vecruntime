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
package io.vecruntime.kernels;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The Iceberg table spec's own examples ("Bucket Transform Details" and
 * "Truncate Transform Details"). The engine-level suite in iceberg-bridge
 * compares every kernel with Iceberg's functions on random data.
 */
class IcebergTransformKernelsTest {

    @Test
    void bucketHashesAreTheSpecsExamples() {
        assertEquals(2017239379, IcebergTransformKernels.hashLong(34));
        assertEquals(2017239379, IcebergTransformKernels.hashLong(34L));
        assertEquals(-500754589, IcebergTransformKernels.hashDecimal(1420)); // 14.20
        int date = (int) LocalDate.of(2017, 11, 16).toEpochDay();
        assertEquals(-653330422, IcebergTransformKernels.hashLong(date));
        long ts = LocalDateTime.of(2017, 11, 16, 22, 31, 8).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
        assertEquals(-2047944441, IcebergTransformKernels.hashLong(ts));
        assertEquals(1210000089, IcebergTransformKernels.hashBytes("iceberg".getBytes(StandardCharsets.UTF_8)));
        assertEquals(-188683207, IcebergTransformKernels.hashBytes(new byte[] {0, 1, 2, 3}));
    }

    @Test
    void truncateIsTheSpecsExamples() {
        assertEquals(0, IcebergTransformKernels.truncate(1, 10));
        assertEquals(-10, IcebergTransformKernels.truncate(-1, 10));
        assertEquals(0L, IcebergTransformKernels.truncate(1L, 10L));
        assertEquals(-10L, IcebergTransformKernels.truncate(-1L, 10L));
        assertEquals(1050L, IcebergTransformKernels.truncate(1065L, 50L)); // 10.65 truncated to 10.50
    }

    @Test
    void timeTransformsFloorBeforeTheEpoch() {
        int d = (int) LocalDate.of(1969, 12, 31).toEpochDay();
        assertEquals(-1, IcebergTransformKernels.daysToYears(d));
        assertEquals(-1, IcebergTransformKernels.daysToMonths(d));
        assertEquals(-1, IcebergTransformKernels.microsToDays(-1L));
        assertEquals(-1, IcebergTransformKernels.microsToHours(-1L));
        int march2017 = (int) LocalDate.of(2017, 3, 5).toEpochDay();
        assertEquals(47, IcebergTransformKernels.daysToYears(march2017));
        assertEquals(47 * 12 + 2, IcebergTransformKernels.daysToMonths(march2017));
    }
}
