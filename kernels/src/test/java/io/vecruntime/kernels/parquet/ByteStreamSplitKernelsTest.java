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
package io.vecruntime.kernels.parquet;

import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The three {@code BYTE_STREAM_SPLIT} transposes and the two {@code
 * DELTA_BINARY_PACKED} prefix sums must agree with their scalar references
 * everywhere: every count up to a few registers, every start offset within a
 * value, strides with and without slack, and splits that end exactly at the
 * array's end (where the wide loads must stop and the tail go scalar).
 */
class ByteStreamSplitKernelsTest {

    @Test
    void transposesAgreeWithTheScalarGather() {
        Random rnd = new Random(55917);
        for (int rep = 0; rep < 400; rep++) {
            int stride = rnd.nextInt(300);
            int slack = rnd.nextInt(3) == 0 ? 0 : rnd.nextInt(40); // 0: the last stream ends at the array's end
            int start = rnd.nextInt(17);
            for (int width : new int[] {4, 8}) {
                byte[] src = new byte[start + width * stride + slack];
                rnd.nextBytes(src);
                int from = stride == 0 ? 0 : rnd.nextInt(stride + 1);
                int n = stride - from;
                String what = "width="
                        + width
                        + " stride="
                        + stride
                        + " slack="
                        + slack
                        + " start="
                        + start
                        + " from="
                        + from;
                if (width == 8) {
                    long[] want = new long[n];
                    ByteStreamSplitKernels.longsScalar(src, start, stride, from, want, n);
                    long[] got = new long[n];
                    ByteStreamSplitKernels.longsSwar(src, start, stride, from, got, n);
                    assertArrayEquals(want, got, "swar " + what);
                    got = new long[n];
                    ByteStreamSplitKernels.longsVector(src, start, stride, from, got, n);
                    assertArrayEquals(want, got, "vector " + what);
                    got = new long[n];
                    ByteStreamSplitKernels.longs(src, start, stride, from, got, n);
                    assertArrayEquals(want, got, "dispatch " + ByteStreamSplitKernels.MODE + " " + what);
                    for (int k = 0; k < n; k++) {
                        long v = 0;
                        for (int j = 0; j < 8; j++) {
                            v |= ((long) (src[start + j * stride + from + k] & 0xFF)) << (8 * j);
                        }
                        assertEquals(v, want[k], "scalar " + what + " k=" + k);
                    }
                } else {
                    int[] want = new int[n];
                    ByteStreamSplitKernels.intsScalar(src, start, stride, from, want, n);
                    int[] got = new int[n];
                    ByteStreamSplitKernels.intsSwar(src, start, stride, from, got, n);
                    assertArrayEquals(want, got, "swar " + what);
                    got = new int[n];
                    ByteStreamSplitKernels.intsVector(src, start, stride, from, got, n);
                    assertArrayEquals(want, got, "vector " + what);
                    got = new int[n];
                    ByteStreamSplitKernels.ints(src, start, stride, from, got, n);
                    assertArrayEquals(want, got, "dispatch " + ByteStreamSplitKernels.MODE + " " + what);
                }
            }
        }
    }

    @Test
    void vectorPrefixSumAgreesWithTheSerialOneIncludingWraparound() {
        Random rnd = new Random(55918);
        for (int rep = 0; rep < 500; rep++) {
            int n = rnd.nextInt(130);
            int off = rnd.nextInt(9);
            int[] a = new int[off + n + 3];
            for (int i = 0; i < a.length; i++) {
                a[i] = rep % 3 == 0 ? rnd.nextInt() : rnd.nextInt(1 << rnd.nextInt(31));
            }
            int md = rep % 5 == 0 ? Integer.MIN_VALUE : rnd.nextInt();
            int carry = rnd.nextInt();
            int[] s = a.clone();
            int[] v = a.clone();
            int ls = DeltaBinaryPackedReader.scanIntsScalar(s, off, n, md, carry);
            int lv = DeltaBinaryPackedReader.scanIntsVector(v, off, n, md, carry);
            assertArrayEquals(s, v, "rep " + rep + " n=" + n + " off=" + off);
            assertEquals(ls, lv, "carry, rep " + rep);
        }
    }
}
