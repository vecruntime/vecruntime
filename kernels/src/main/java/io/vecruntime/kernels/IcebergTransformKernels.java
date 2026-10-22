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

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;

/**
 * The Iceberg partition transforms (the table spec's "Partition Transforms" and
 * "Bucket Transform Details") over whole lanes: what Iceberg's {@code
 * Transforms} compute per value, on the engine's representations. Dates are
 * INT32 days, timestamps INT64 microseconds from the epoch, decimals of up to
 * 18 digits INT64 unscaled values. Each kernel writes only the result slots;
 * the caller keeps the input's validity, since every transform maps null to
 * null.
 *
 * <p>
 * Spark-free and Iceberg-free: the engine compiles Iceberg's Spark functions
 * ({@code BucketFunction}, {@code TruncateFunction}, {@code YearsFunction} ...)
 * to these, which is what lets a hash or range distributed write, whose
 * exchange and sort keys are those functions, stay on batches.
 */
public final class IcebergTransformKernels {

    private IcebergTransformKernels() {}

    private static final long MICROS_PER_HOUR = 3_600_000_000L;
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    // ---------------------------------------------------------------- time

    /**
     * Years from 1970 of a date (floor, as Iceberg's {@code
     * DateTimeUtil.daysToYears}).
     */
    public static int daysToYears(int days) {
        return DateKernels.field(DateKernels.Field.YEAR, days) - 1970;
    }

    /** Months from 1970-01 of a date. */
    public static int daysToMonths(int days) {
        return (DateKernels.field(DateKernels.Field.YEAR, days) - 1970) * 12 + DateKernels.field(DateKernels.Field.MONTH, days) - 1;
    }

    /** The day of a timestamp, in days from the epoch (UTC; floor). */
    public static int microsToDays(long micros) {
        return (int) Math.floorDiv(micros, MICROS_PER_DAY); // Iceberg narrows the same way
    }

    /** Hours from the epoch of a timestamp (floor). */
    public static int microsToHours(long micros) {
        return (int) Math.floorDiv(micros, MICROS_PER_HOUR);
    }

    /** The time transforms, by source and unit. */
    public enum Time {
        DATE_YEARS,
        DATE_MONTHS,
        DATE_DAYS,
        TIMESTAMP_YEARS,
        TIMESTAMP_MONTHS,
        TIMESTAMP_DAYS,
        TIMESTAMP_HOURS
    }

    /** {@code out[i] = transform(a[i])}: INT32 days or INT64 micros in, INT32 out. */
    public static void time(Time t, VectorBuffers a, MemorySegment out) {
        MemorySegment d = a.data();
        int n = a.length();
        switch (t) {
            case DATE_YEARS -> {
                for (int i = 0; i < n; i++) {
                    out.setAtIndex(VectorBuffers.LE_INT, i, daysToYears(d.getAtIndex(VectorBuffers.LE_INT, i)));
                }
            }
            case DATE_MONTHS -> {
                for (int i = 0; i < n; i++) {
                    out.setAtIndex(VectorBuffers.LE_INT, i, daysToMonths(d.getAtIndex(VectorBuffers.LE_INT, i)));
                }
            }
            case DATE_DAYS -> MemorySegment.copy(d, 0, out, 0, (long) n * 4);
            case TIMESTAMP_YEARS -> {
                for (int i = 0; i < n; i++) {
                    out.setAtIndex(VectorBuffers.LE_INT, i, daysToYears(microsToDays(d.getAtIndex(VectorBuffers.LE_LONG, i))));
                }
            }
            case TIMESTAMP_MONTHS -> {
                for (int i = 0; i < n; i++) {
                    out.setAtIndex(VectorBuffers.LE_INT, i, daysToMonths(microsToDays(d.getAtIndex(VectorBuffers.LE_LONG, i))));
                }
            }
            case TIMESTAMP_DAYS -> {
                for (int i = 0; i < n; i++) {
                    out.setAtIndex(VectorBuffers.LE_INT, i, microsToDays(d.getAtIndex(VectorBuffers.LE_LONG, i)));
                }
            }
            case TIMESTAMP_HOURS -> {
                for (int i = 0; i < n; i++) {
                    out.setAtIndex(VectorBuffers.LE_INT, i, microsToHours(d.getAtIndex(VectorBuffers.LE_LONG, i)));
                }
            }
        }
    }

    // ---------------------------------------------------------------- truncate

    /** {@code v - floorMod(v, w)} on an int, wrapping as Iceberg's Java does. */
    public static int truncate(int v, int w) {
        return v - (((v % w) + w) % w);
    }

    /**
     * {@code v - floorMod(v, w)} on a long (a long, or a decimal's unscaled
     * value).
     */
    public static long truncate(long v, long w) {
        return v - (((v % w) + w) % w);
    }

    /** {@code out[i] = truncate(a[i], w)}; INT32 in and out. */
    public static void truncateInt(VectorBuffers a, int w, MemorySegment out) {
        MemorySegment d = a.data();
        int n = a.length();
        for (int i = 0; i < n; i++) {
            out.setAtIndex(VectorBuffers.LE_INT, i, truncate(d.getAtIndex(VectorBuffers.LE_INT, i), w));
        }
    }

    /**
     * {@code out[i] = truncate(a[i], w)}; INT64 in and out (a long, or a
     * decimal's unscaled value).
     */
    public static void truncateLong(VectorBuffers a, long w, MemorySegment out) {
        MemorySegment d = a.data();
        int n = a.length();
        for (int i = 0; i < n; i++) {
            out.setAtIndex(VectorBuffers.LE_LONG, i, truncate(d.getAtIndex(VectorBuffers.LE_LONG, i), w));
        }
    }

    // ---------------------------------------------------------------- bucket

    /**
     * {@code (hash & Integer.MAX_VALUE) % n}: the bucket of a 32-bit Murmur3
     * hash.
     */
    public static int bucket(int hash, int n) {
        return (hash & Integer.MAX_VALUE) % n;
    }

    /**
     * Bucket hash of an int, long, date, time or timestamp: Murmur3 of the
     * value as a long.
     */
    public static int hashLong(long v) {
        int h = mixH1(0, mixK1((int) v));
        h = mixH1(h, mixK1((int) (v >>> 32)));
        return fmix(h, 8);
    }

    /**
     * Bucket hash of {@code len} bytes (a string's UTF-8 bytes, a binary, a
     * decimal's unscaled bytes).
     */
    public static int hashBytes(MemorySegment s, long start, int len) {
        int h = 0;
        int body = len & ~3;
        for (int i = 0; i < body; i += 4) {
            h = mixH1(h, mixK1(s.get(VectorBuffers.LE_INT, start + i)));
        }
        int tail = len & 3;
        if (tail > 0) {
            int k = 0;
            if (tail == 3) {
                k ^= (s.get(java.lang.foreign.ValueLayout.JAVA_BYTE, start + body + 2) & 0xff) << 16;
            }
            if (tail >= 2) {
                k ^= (s.get(java.lang.foreign.ValueLayout.JAVA_BYTE, start + body + 1) & 0xff) << 8;
            }
            k ^= s.get(java.lang.foreign.ValueLayout.JAVA_BYTE, start + body) & 0xff;
            h ^= mixK1(k);
        }
        return fmix(h, len);
    }

    /** Bucket hash of a byte array. */
    public static int hashBytes(byte[] b) {
        return hashBytes(MemorySegment.ofArray(b), 0, b.length);
    }

    /**
     * Bucket hash of a decimal: Murmur3 of the unscaled value's minimal
     * big-endian two's-complement bytes ({@code BigInteger.toByteArray}).
     */
    public static int hashDecimal(long unscaled) {
        return hashBytes(BigInteger.valueOf(unscaled)
                .toByteArray());
    }

    /**
     * {@code out[i] = bucket(a[i], n)}; INT32 or INT64 lanes in (hashed as
     * longs), INT32 out.
     */
    public static void bucketIntegral(VectorBuffers a, int n, MemorySegment out) {
        MemorySegment d = a.data();
        int rows = a.length();
        if (a.type() == VecType.INT32) {
            for (int i = 0; i < rows; i++) {
                out.setAtIndex(VectorBuffers.LE_INT, i, bucket(hashLong(d.getAtIndex(VectorBuffers.LE_INT, i)), n));
            }
        } else {
            for (int i = 0; i < rows; i++) {
                out.setAtIndex(VectorBuffers.LE_INT, i, bucket(hashLong(d.getAtIndex(VectorBuffers.LE_LONG, i)), n));
            }
        }
    }

    /**
     * {@code out[i] = bucket(a[i], n)} over an INT64 lane of decimal unscaled
     * values.
     */
    public static void bucketDecimal(VectorBuffers a, int n, MemorySegment out) {
        MemorySegment d = a.data();
        int rows = a.length();
        for (int i = 0; i < rows; i++) {
            out.setAtIndex(VectorBuffers.LE_INT, i, bucket(hashDecimal(d.getAtIndex(VectorBuffers.LE_LONG, i)), n));
        }
    }

    /**
     * {@code out[i] = bucket(a[i], n)} over a UTF8 (string or binary) lane. A
     * dictionary-encoded lane hashes each dictionary entry once and maps the
     * ids.
     */
    public static void bucketBytes(VectorBuffers a, int n, MemorySegment out) {
        int rows = a.length();
        VectorBuffers dict = a.dictionary();
        if (dict != null) {
            int[] byEntry = new int[dict.length()];
            MemorySegment off = dict.offsets();
            MemorySegment bytes = dict.data();
            for (int e = 0; e < byEntry.length; e++) {
                int s = off.getAtIndex(VectorBuffers.LE_INT, e);
                int end = off.getAtIndex(VectorBuffers.LE_INT, e + 1);
                byEntry[e] = bucket(hashBytes(bytes, s, end - s), n);
            }
            MemorySegment ids = a.data();
            MemorySegment validity = a.validity();
            for (int i = 0; i < rows; i++) {
                // A null row's id may be anything; its slot is never read.
                int id = validity == null || Bitmap.isSet(validity, i)
                        ? ids.getAtIndex(VectorBuffers.LE_INT, i)
                        : 0;
                out.setAtIndex(VectorBuffers.LE_INT, i, byEntry.length == 0 ? 0 : byEntry[id]);
            }
            return;
        }
        MemorySegment off = a.offsets();
        MemorySegment bytes = a.data();
        for (int i = 0; i < rows; i++) {
            int s = off.getAtIndex(VectorBuffers.LE_INT, i);
            int end = off.getAtIndex(VectorBuffers.LE_INT, i + 1);
            out.setAtIndex(VectorBuffers.LE_INT, i, bucket(hashBytes(bytes, s, end - s), n));
        }
    }

    // Murmur3 x86_32, seed 0 (Guava's murmur3_32_fixed, which Iceberg uses).

    private static int mixK1(int k1) {
        k1 *= 0xcc9e2d51;
        k1 = Integer.rotateLeft(k1, 15);
        return k1 * 0x1b873593;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        return h1 * 5 + 0xe6546b64;
    }

    private static int fmix(int h1, int length) {
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
