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

import java.math.BigDecimal;
import java.math.BigInteger;
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
    void decimalBucketHashMatchesTheMinimalTwosComplementBytes() {
        // Iceberg hashes unscaledValue().toByteArray(); hashDecimal must equal hashing those exact bytes,
        // formed from the limbs with no BigInteger.
        long[] longs = {
            0L,
            1L,
            -1L,
            127L,
            128L,
            -128L,
            -129L,
            255L,
            256L,
            -256L,
            -257L,
            32767L,
            -32768L,
            16777215L,
            16777216L,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            1420L,
            -1420L,
            999999999999999999L,
            -999999999999999999L
        };
        for (long v : longs) {
            byte[] ref = BigInteger.valueOf(v).toByteArray();
            assertEquals(ref.length, IcebergTransformKernels.minimalTwosComplementLength(v < 0 ? -1L : 0L, v),
                    "byte length of " + v);
            assertEquals(IcebergTransformKernels.hashBytes(ref),
                    IcebergTransformKernels.hashDecimal(v), "hashDecimal(" + v + ")");
        }
        // 14.20 from the spec: unscaled 1420, scale 2.
        assertEquals(-500754589, IcebergTransformKernels.hashDecimal(1420));
    }

    @Test
    void wideDecimalBucketHashMatchesTheMinimalTwosComplementBytes() {
        BigInteger[] wide = {
            BigInteger.ZERO,
            BigInteger.ONE,
            BigInteger.valueOf(-1),
            BigInteger.valueOf(Long.MAX_VALUE),
            BigInteger.valueOf(Long.MIN_VALUE),
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
            BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
            BigInteger.TEN.pow(37),
            BigInteger.TEN.pow(37).negate(),
            BigInteger.ONE.shiftLeft(120),
            BigInteger.ONE.shiftLeft(120).negate(),
            new BigInteger("170141183460469231731687303715884105727"), // 2^127 - 1
            new BigInteger("-170141183460469231731687303715884105728") // -2^127
        };
        for (BigInteger v : wide) {
            long hi = Decimal128.hiOf(v);
            long lo = Decimal128.loOf(v);
            byte[] ref = v.toByteArray();
            assertEquals(ref.length, IcebergTransformKernels.minimalTwosComplementLength(hi, lo), "byte length of " + v);
            assertEquals(IcebergTransformKernels.hashBytes(ref), IcebergTransformKernels.hashDecimal128(hi, lo),
                    "hashDecimal128(" + v + ")");
            // A BigDecimal with a non-zero scale hashes by the unscaled integer too.
            assertEquals(IcebergTransformKernels.hashBytes(new BigDecimal(v, 6).unscaledValue().toByteArray()),
                    IcebergTransformKernels.hashDecimal128(hi, lo), "scaled " + v);
        }
    }

    @Test
    void truncateUtf8ByCodePoints() {
        // ASCII: byte length equals code points.
        checkTruncateUtf8("hello", 3, "hel");
        checkTruncateUtf8("hello", 5, "hello");
        checkTruncateUtf8("hello", 9, "hello");
        checkTruncateUtf8("", 2, "");
        // 2-byte code points (é = U+00E9).
        checkTruncateUtf8("éàü", 2, "éà");
        checkTruncateUtf8("aéb", 2, "aé");
        // 3-byte code points (CJK).
        checkTruncateUtf8("日本語", 2, "日本");
        // 4-byte code points (a surrogate pair in UTF-16 is one code point): U+1F600, U+1F601.
        checkTruncateUtf8("\uD83D\uDE00\uD83D\uDE01x", 1, "\uD83D\uDE00");
        checkTruncateUtf8("\uD83D\uDE00\uD83D\uDE01x", 2, "\uD83D\uDE00\uD83D\uDE01");
        checkTruncateUtf8("a\uD83D\uDE00b", 2, "a\uD83D\uDE00");
    }

    private static void checkTruncateUtf8(String s, int codePoints, String expected) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        int len = IcebergTransformKernels.truncateUtf8ByteLength(java.lang.foreign.MemorySegment.ofArray(b), 0, b.length, codePoints);
        assertEquals(expected, new String(b, 0, len, StandardCharsets.UTF_8), "truncate '" + s + "' to " + codePoints);
        // Equal to Iceberg's semantics: the first `codePoints` code points of the decoded string.
        int cp = Math.min(codePoints, s.codePointCount(0, s.length()));
        assertEquals(s.substring(0, s.offsetByCodePoints(0, cp)), new String(b, 0, len, StandardCharsets.UTF_8));
    }

    @Test
    void truncateDecimal128MatchesFloorMod() {
        int[] widths = {1, 2, 5, 10, 50, 100,
                1000};
        BigInteger[] values = {
            BigInteger.ZERO,
            BigInteger.ONE,
            BigInteger.valueOf(-1),
            BigInteger.valueOf(1065),
            BigInteger.valueOf(-1065),
            BigInteger.valueOf(Long.MAX_VALUE),
            BigInteger.valueOf(Long.MIN_VALUE),
            BigInteger.TEN.pow(30),
            BigInteger.TEN.pow(30).negate(),
            // The widest a DECIMAL(38) unscaled value can be, +-(10^38 - 1): well inside 128 bits, so
            // truncate never underflows the 128-bit range (Iceberg uses BigInteger and never overflows).
            BigInteger.TEN.pow(38).subtract(BigInteger.ONE),
            BigInteger.TEN
                    .pow(38)
                    .subtract(BigInteger.ONE)
                    .negate()
        };
        long[] out = new long[2];
        for (int w : widths) {
            for (BigInteger v : values) {
                BigInteger expected = v.subtract(v.mod(BigInteger.valueOf(w)));
                IcebergTransformKernels.truncateDecimal128(Decimal128.hiOf(v), Decimal128.loOf(v), w, out);
                BigInteger got = Decimal128.toBigInteger(out[0], out[1]);
                assertEquals(expected, got, "truncate " + v + " by " + w);
            }
        }
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
