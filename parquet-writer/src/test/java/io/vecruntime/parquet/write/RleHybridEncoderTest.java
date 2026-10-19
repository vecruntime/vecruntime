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

import java.io.ByteArrayInputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import io.vecruntime.kernels.ArrowLayout;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * The hybrid encoder decoded by parquet-java's own decoder, for runs,
 * alternations and every bit width.
 */
class RleHybridEncoderTest {

    private static int[] decode(ByteSink s, int bitWidth, int n) throws Exception {
        RunLengthBitPackingHybridDecoder d = new RunLengthBitPackingHybridDecoder(bitWidth,
                new ByteArrayInputStream(s.array(), 0, s.size()));
        int[] out = new int[n];
        for (int i = 0; i < n; i++) {
            out[i] = d.readInt();
        }
        return out;
    }

    private static int[] mixed(Random r, int n, int bitWidth) {
        int max = bitWidth == 0 ? 0 : (int) Math.min(Integer.MAX_VALUE, (1L << bitWidth) - 1);
        int[] v = new int[n];
        int i = 0;
        while (i < n) {
            int len = r.nextInt(4) == 0 ? 1 + r.nextInt(40) : 1 + r.nextInt(3);
            int x = max == 0 ? 0 : r.nextInt(max) + (r.nextBoolean() ? 1 : 0);
            for (int k = 0;
                 k < len && i < n;
                 k++) {
                v[i++] = x;
            }
        }
        return v;
    }

    @Test
    void everyBitWidthRoundTrips() throws Exception {
        Random r = new Random(7);
        for (int bw = 0; bw <= 31; bw++) {
            for (int n : new int[] {0, 1, 7, 8, 9, 63,
                    64, 65, 1000, 4097}) {
                int[] v = mixed(r, n, bw);
                ByteSink s = new ByteSink(16);
                RleHybridEncoder.encode(v, n, bw, s);
                assertArrayEquals(v, decode(s, bw, n), "bitWidth " + bw + ", n " + n);
            }
        }
    }

    @Test
    void validityLevelsRoundTrip() throws Exception {
        Random r = new Random(11);
        try (Arena a = Arena.ofConfined()) {
            for (int n : new int[] {1, 8, 63, 64, 65, 130,
                    1000, 5000}) {
                for (int density : new int[] {0, 1, 50, 97, 100}) {
                    boolean[] nulls = new boolean[n];
                    boolean[] runs = new boolean[n];
                    for (int i = 0; i < n; i++) {
                        nulls[i] = r.nextInt(100) < density;
                        runs[i] = (i / 37) % 3 == 0;
                    }
                    for (boolean[] pattern : new boolean[][] {nulls, runs}) {
                        MemorySegment validity = ArrowLayout.validityFrom(a, pattern, n);
                        for (int[] range : new int[][] {{0, n}, {n / 3, n}, {0, n / 2}}) {
                            int from = range[0], to = range[1];
                            int[] expected = new int[to - from];
                            for (int i = from; i < to; i++) {
                                expected[i - from] = pattern[i] ? 0 : 1;
                            }
                            ByteSink s = new ByteSink(16);
                            RleHybridEncoder.encodeValidity(validity, from, to, s);
                            assertArrayEquals(expected, decode(s, 1, to - from), "n "
                                    + n
                                    + ", density "
                                    + density
                                    + ", ["
                                    + from
                                    + ", "
                                    + to
                                    + ")");
                        }
                    }
                }
            }
        }
    }
}
