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

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The encoder decoded by parquet-java's {@code DeltaBinaryPackingValuesReader}:
 * sorted, random and extreme values.
 */
class DeltaBinaryPackedEncoderTest {

    private static DeltaBinaryPackingValuesReader reader(ByteSink s, int n) throws Exception {
        DeltaBinaryPackingValuesReader r = new DeltaBinaryPackingValuesReader();
        r.initFromPage(n,
                ByteBufferInputStream.wrap(ByteBuffer.wrap(s.array(), 0, s.size())));
        return r;
    }

    private static long[] data(Random r, int n, int shape) {
        long[] v = new long[n];
        long x = r.nextLong();
        for (int i = 0; i < n; i++) {
            v[i] = switch (shape) {
                        case 0 -> i / 10 + 1; // sorted, runs (ticket numbers)
                        case 1 -> r.nextInt(1_000_000); // small random
                        case 2 -> r.nextLong(); // full-range random: deltas overflow
                        case 3 -> i % 2 == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
                        default -> x += r.nextInt(5) - 2; // a random walk
                    };
        }
        return v;
    }

    @Test
    void intsRoundTrip() throws Exception {
        Random r = new Random(3);
        for (int shape = 0; shape < 5; shape++) {
            for (int n : new int[] {
                0,
                1,
                2,
                31,
                32,
                33,
                127,
                128,
                129,
                130,
                1000,
                20_000
            }) {
                long[] v = data(r, n, shape);
                ByteSink s = new ByteSink(16);
                new DeltaBinaryPackedEncoder(true).encodeInts(v, n, s);
                ByteSink scalar = new ByteSink(16);
                new DeltaBinaryPackedEncoder(false).encodeInts(v, n, scalar);
                assertArrayEquals(scalar.toByteArray(), s.toByteArray(), "vector and scalar bytes, shape " + shape);
                DeltaBinaryPackingValuesReader d = reader(s, n);
                for (int i = 0; i < n; i++) {
                    assertEquals((int) v[i], d.readInteger(),
                            "shape " + shape + ", n " + n + ", i " + i);
                }
            }
        }
    }

    @Test
    void longsRoundTrip() throws Exception {
        Random r = new Random(4);
        for (int shape = 0; shape < 5; shape++) {
            for (int n : new int[] {
                0,
                1,
                2,
                31,
                32,
                33,
                127,
                128,
                129,
                130,
                1000,
                20_000
            }) {
                long[] v = data(r, n, shape);
                ByteSink s = new ByteSink(16);
                new DeltaBinaryPackedEncoder(true).encodeLongs(v, n, s);
                ByteSink scalar = new ByteSink(16);
                new DeltaBinaryPackedEncoder(false).encodeLongs(v, n, scalar);
                assertArrayEquals(scalar.toByteArray(), s.toByteArray(), "vector and scalar bytes, shape " + shape);
                DeltaBinaryPackingValuesReader d = reader(s, n);
                for (int i = 0; i < n; i++) {
                    assertEquals(v[i], d.readLong(), "shape " + shape + ", n " + n + ", i " + i);
                }
            }
        }
    }
}
