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

import java.lang.foreign.Arena;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * An array or struct as bytes that sort, compared unsigned, exactly as Spark
 * orders the value (#705): the sort kernels then order a nested key as a string
 * key. Spark compares an array element by element, a null element before any
 * other and a shorter array before a longer one it is a prefix of, and a struct
 * field by field, a null field first; a descending key reverses the whole
 * comparison, which the kernels do over the bytes.
 *
 * <p>Every encoding is self-delimiting, so comparing two concatenations is
 * comparing their parts in turn:
 *
 * <ul>
 *   <li>an array: per element a 0x01 then the element as a child, and a closing
 *       0x00 (shorter first);
 *   <li>a struct: each field as a child;
 *   <li>a child: 0x00 if null, else 0x01 and the value (null first);
 *   <li>BOOL one byte; INT32, INT64 big-endian with the sign bit flipped;
 *       FLOAT64 Spark's total order ({@link SortKernels#doubleKey}: -0.0 equal
 *       to 0.0, NaN last); DECIMAL128 the 16-byte two's complement big-endian,
 *       sign flipped (one column, one scale);
 *   <li>UTF8 (strings and binary): the bytes with each 0x00 written as 0x00
 *       0x01, closed by 0x00 0x00, so a prefix sorts first and every other byte
 *       keeps its unsigned order.
 * </ul>
 *
 * Maps are not orderable in Spark and are refused by the planner.
 */
public final class NestedSortKey {

    private NestedSortKey() {}

    /**
     * Lane {@code v}'s rows as a UTF8 lane of their order-preserving encodings,
     * top-level nulls kept.
     */
    public static VectorBuffers encode(Arena arena, VectorBuffers v, int n) {
        Out out = new Out(32 * Math.max(1, n));
        int[] offsets = new int[n + 1];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            if (v.isNull(i)) {
                nulls[i] = true;
            } else {
                value(v, i, out);
            }
            offsets[i + 1] = out.size;
        }
        return ArrowLayout.ofUtf8(arena, out.buf, offsets, n, nulls);
    }

    /** One non-null value's encoding (for tests). */
    public static byte[] encodeValue(VectorBuffers v, int i) {
        Out out = new Out(32);
        value(v, i, out);
        return Arrays.copyOf(out.buf, out.size);
    }

    private static void child(VectorBuffers v, int i, Out out) {
        if (v.isNull(i)) {
            out.b(0);
        } else {
            out.b(1);
            value(v, i, out);
        }
    }

    private static void value(VectorBuffers v, int i, Out out) {
        switch (v.type()) {
            case BOOL -> out.b(v.getBoolean(i) ? 1 : 0);
            case INT32 -> out.be32(v.getInt(i) ^ Integer.MIN_VALUE);
            case INT64 -> out.be64(v.getLong(i) ^ Long.MIN_VALUE);
            case FLOAT64 -> out.be64(SortKernels.doubleKey(v.getDouble(i)));
            case DECIMAL128 -> out.dec(v.getDecimal128(i));
            case UTF8 -> {
                for (byte x : v.getUtf8Bytes(i)) {
                    out.b(x);
                    if (x == 0) {
                        out.b(1);
                    }
                }
                out.b(0);
                out.b(0);
            }
            case LIST -> {
                NestedVectorBuffers nv = (NestedVectorBuffers) v;
                VectorBuffers e = nv.children().get(0);
                for (int j = nv.start(i);
                     j < nv.end(i);
                     j++) {
                    out.b(1);
                    child(e, j, out);
                }
                out.b(0);
            }
            case STRUCT -> {
                for (VectorBuffers f : v.children()) {
                    child(f, i, out);
                }
            }
            case MAP -> throw new IllegalArgumentException("a map is not orderable");
        }
    }

    private static final class Out {
        byte[] buf;
        int size;

        Out(int capacity) {
            buf = new byte[capacity];
        }

        private void ensure(int more) {
            if (size + more > buf.length) {
                buf = Arrays.copyOf(buf,
                        Math.max(buf.length * 2, size + more));
            }
        }

        void b(int x) {
            ensure(1);
            buf[size++] = (byte) x;
        }

        void be32(int x) {
            ensure(4);
            for (int k = 3; k >= 0; k--) {
                buf[size++] = (byte) (x >>> (8 * k));
            }
        }

        void be64(long x) {
            ensure(8);
            for (int k = 7; k >= 0; k--) {
                buf[size++] = (byte) (x >>> (8 * k));
            }
        }

        void dec(BigInteger x) {
            byte[] be = x.toByteArray();
            byte pad = (byte) (x.signum() < 0 ? 0xFF : 0);
            ensure(16);
            for (int k = 0; k < 16; k++) {
                int idx = be.length - 16 + k;
                byte b = idx >= 0 ? be[idx] : pad;
                buf[size++] = k == 0 ? (byte) (b ^ 0x80) : b;
            }
        }
    }
}
