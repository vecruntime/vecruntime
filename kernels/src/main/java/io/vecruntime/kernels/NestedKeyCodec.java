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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A nested value as one canonical byte string, and back (#705): what lets a
 * nested grouping key ride the UTF8 key lane the group table already hashes
 * and compares. Two values encode to the same bytes exactly when they are
 * equal element by element, field by field, nulls included -- the equality
 * Spark groups nested keys by, once its optimizer has normalised float leaves
 * (NormalizeFloatingNumbers wraps such keys before they reach an aggregate).
 *
 * <p>The encoding of a non-null value: a LIST or MAP writes its entry count
 * (4 bytes) then each entry (a MAP's key then value); a STRUCT writes its
 * fields in order; every child is preceded by a presence byte (0 = null, 1 =
 * present). Leaves: BOOL 1 byte, INT32 4, INT64 and FLOAT64 8 (raw bits),
 * DECIMAL128 16 (two's complement, little-endian), UTF8 a 4-byte length then
 * its bytes (binary rides this lane, so the bytes are taken as they are).
 * A null top-level value is a null in the lane, not an encoding.
 */
public final class NestedKeyCodec {

    private NestedKeyCodec() {}

    /** The lane tree of a nested type: its lane type and its children's shapes. */
    public record Shape(VecType type, List<Shape> children) {
        public Shape {
            children = List.copyOf(children);
        }

        public static Shape leaf(VecType type) {
            return new Shape(type, List.of());
        }
    }

    /** Lane {@code v}'s rows as a UTF8 lane of their encodings, nulls kept. */
    public static VectorBuffers encode(Arena arena, VectorBuffers v, int n) {
        Out out = new Out(64 * Math.max(1, n));
        int[] offsets = new int[n + 1];
        boolean[] nulls = new boolean[n];
        for (int i = 0; i < n; i++) {
            if (v.isNull(i)) {
                nulls[i] = true;
            } else {
                value(v, i, out, false);
            }
            offsets[i + 1] = out.size;
        }
        return ArrowLayout.ofUtf8(arena, out.buf, offsets, n, nulls);
    }

    /** One non-null value's encoding (exposed for tests). */
    public static byte[] encodeValue(VectorBuffers v, int i) {
        Out out = new Out(64);
        value(v, i, out, false);
        return Arrays.copyOf(out.buf, out.size);
    }

    /**
     * One non-null value's encoding with float leaves normalised the way
     * Spark's map-key dedup compares them ({@code NaN == NaN}, {@code -0.0 ==
     * 0.0}): used only to detect duplicate map keys, never to store the key, so
     * the stored key keeps its original bits. Two keys Spark considers equal
     * encode to the same bytes here.
     */
    public static byte[] encodeKeyForDedup(VectorBuffers v, int i) {
        Out out = new Out(64);
        value(v, i, out, true);
        return Arrays.copyOf(out.buf, out.size);
    }

    private static void child(VectorBuffers v, int i, Out out,
            boolean normalizeFloats) {
        if (v.isNull(i)) {
            out.b(0);
        } else {
            out.b(1);
            value(v, i, out, normalizeFloats);
        }
    }

    private static void value(VectorBuffers v, int i, Out out,
            boolean normalizeFloats) {
        switch (v.type()) {
            case BOOL -> out.b(v.getBoolean(i) ? 1 : 0);
            case INT32 -> out.i32(v.getInt(i));
            case INT64 -> out.i64(v.getLong(i));
            case FLOAT64 -> {
                double d = v.getDouble(i);
                // Spark's map-key dedup normalises NaN and -0.0 so equal values encode alike.
                if (normalizeFloats) {
                    d = Double.isNaN(d)
                            ? Double.NaN
                            : (d == 0.0 ? 0.0 : d);
                }
                out.i64(Double.doubleToRawLongBits(d));
            }
            case DECIMAL128 -> out.dec(v.getDecimal128(i));
            case FIXED_BINARY -> throw new IllegalStateException("no FIXED_BINARY keys: the lane does not reach operators");
            case UTF8 -> {
                byte[] b = v.getUtf8Bytes(i);
                out.i32(b.length);
                out.bytes(b);
            }
            case LIST -> {
                NestedVectorBuffers nv = (NestedVectorBuffers) v;
                VectorBuffers e = nv.children().get(0);
                out.i32(nv.size(i));
                for (int j = nv.start(i);
                     j < nv.end(i);
                     j++) {
                    child(e, j, out, normalizeFloats);
                }
            }
            case MAP -> {
                NestedVectorBuffers nv = (NestedVectorBuffers) v;
                VectorBuffers k = nv.children().get(0);
                VectorBuffers val = nv.children().get(1);
                out.i32(nv.size(i));
                for (int j = nv.start(i);
                     j < nv.end(i);
                     j++) {
                    child(k, j, out, normalizeFloats);
                    child(val, j, out, normalizeFloats);
                }
            }
            case STRUCT -> {
                for (VectorBuffers f : v.children()) {
                    child(f, i, out, normalizeFloats);
                }
            }
        }
    }

    /**
     * The nested lane of shape {@code shape} whose row {@code i} is the value
     * {@code encoded[i]} encodes, or null where {@code encoded[i]} is null.
     */
    public static VectorBuffers decode(Arena arena, Shape shape, byte[][] encoded) {
        Node root = Node.of(shape);
        for (byte[] e : encoded) {
            if (e == null) {
                root.appendNull();
            } else {
                In in = new In(e);
                root.append(in);
                if (in.pos != e.length) {
                    throw new IllegalArgumentException("trailing bytes in a nested key encoding");
                }
            }
        }
        return root.build(arena);
    }

    /** Collects one lane's rows while decoding. */
    private abstract static class Node {
        final VecType type;
        boolean[] nulls = new boolean[16];
        int rows;

        Node(VecType type) {
            this.type = type;
        }

        static Node of(Shape s) {
            return switch (s.type()) {
                case LIST, MAP -> new Repeated(s);
                case STRUCT -> new Struct(s);
                default -> new Leaf(s.type());
            };
        }

        final void appendNull() {
            grow();
            nulls[rows] = true;
            fillNull();
            rows++;
        }

        final void append(In in) {
            grow();
            read(in);
            rows++;
        }

        /** A present child: its presence byte, then its value. */
        final void appendChild(In in) {
            if (in.b() == 0) {
                appendNull();
            } else {
                append(in);
            }
        }

        private void grow() {
            if (rows == nulls.length) {
                nulls = Arrays.copyOf(nulls, rows * 2);
                growValues(rows * 2);
            }
        }

        final boolean[] nullsOrNone() {
            for (int i = 0; i < rows; i++) {
                if (nulls[i]) {
                    return Arrays.copyOf(nulls, rows);
                }
            }
            return null;
        }

        abstract void read(In in);

        /** A null row's placeholder value (a struct's fields stay row-aligned). */
        abstract void fillNull();

        abstract void growValues(int capacity);

        abstract VectorBuffers build(Arena arena);
    }

    private static final class Leaf extends Node {
        private long[] longs = new long[16];
        private BigInteger[] decimals;
        private byte[] bytes;
        private int[] offsets;
        private int byteCount;

        Leaf(VecType type) {
            super(type);
            if (type == VecType.DECIMAL128) {
                decimals = new BigInteger[16];
            } else if (type == VecType.UTF8) {
                bytes = new byte[64];
                offsets = new int[17];
            }
        }

        @Override
        void growValues(int capacity) {
            longs = Arrays.copyOf(longs, capacity);
            if (decimals != null) {
                decimals = Arrays.copyOf(decimals, capacity);
            }
            if (offsets != null) {
                offsets = Arrays.copyOf(offsets, capacity + 1);
            }
        }

        @Override
        void read(In in) {
            switch (type) {
                case BOOL -> longs[rows] = in.b();
                case INT32 -> longs[rows] = in.i32();
                case INT64, FLOAT64 -> longs[rows] = in.i64();
                case DECIMAL128 -> decimals[rows] = in.dec();
                case UTF8 -> {
                    int len = in.i32();
                    if (byteCount + len > bytes.length) {
                        bytes = Arrays.copyOf(bytes,
                                Math.max(bytes.length * 2, byteCount + len));
                    }
                    System.arraycopy(in.buf, in.pos, bytes, byteCount, len);
                    in.pos += len;
                    byteCount += len;
                    offsets[rows + 1] = byteCount;
                }
                default -> throw new IllegalStateException("not a leaf: " + type);
            }
        }

        @Override
        void fillNull() {
            longs[rows] = 0;
            if (decimals != null) {
                decimals[rows] = BigInteger.ZERO;
            }
            if (offsets != null) {
                offsets[rows + 1] = byteCount;
            }
        }

        @Override
        VectorBuffers build(Arena arena) {
            boolean[] n = nullsOrNone();
            return switch (type) {
                case BOOL -> {
                    boolean[] v = new boolean[rows];
                    for (int i = 0; i < rows; i++) {
                        v[i] = longs[i] != 0;
                    }
                    yield ArrowLayout.ofBooleans(arena, v, n);
                }
                case INT32 -> {
                    int[] v = new int[rows];
                    for (int i = 0; i < rows; i++) {
                        v[i] = (int) longs[i];
                    }
                    yield ArrowLayout.ofInts(arena, v, n);
                }
                case INT64 -> ArrowLayout.ofLongs(arena, Arrays.copyOf(longs, rows), n);
                case FLOAT64 -> {
                    double[] v = new double[rows];
                    for (int i = 0; i < rows; i++) {
                        v[i] = Double.longBitsToDouble(longs[i]);
                    }
                    yield ArrowLayout.ofDoubles(arena, v, n);
                }
                case DECIMAL128 -> ArrowLayout.ofDecimal128(arena, Arrays.copyOf(decimals, rows), n);
                case UTF8 -> ArrowLayout.ofUtf8(arena, bytes, offsets, rows, n);
                default -> throw new IllegalStateException("not a leaf: " + type);
            };
        }
    }

    /** A LIST (one child) or a MAP (keys, values). */
    private static final class Repeated extends Node {
        private final Node[] children;
        private int[] offsets = new int[17];

        Repeated(Shape s) {
            super(s.type());
            children = s.children().stream()
                    .map(Node::of)
                    .toArray(Node[]::new);
        }

        @Override
        void growValues(int capacity) {
            offsets = Arrays.copyOf(offsets, capacity + 1);
        }

        @Override
        void read(In in) {
            int count = in.i32();
            for (int j = 0; j < count; j++) {
                for (Node c : children) {
                    c.appendChild(in);
                }
            }
            offsets[rows + 1] = offsets[rows] + count;
        }

        @Override
        void fillNull() {
            offsets[rows + 1] = offsets[rows];
        }

        @Override
        VectorBuffers build(Arena arena) {
            var off = ArrowLayout.allocateOffsets(arena, rows);
            java.lang.foreign.MemorySegment.copy(offsets, 0, off, VectorBuffers.LE_INT, 0,
                    rows + 1);
            var validity = ArrowLayout.validityFrom(arena, nullsOrNone(), rows);
            if (type == VecType.LIST) {
                return NestedVectorBuffers.list(rows, validity, off, children[0].build(arena));
            }
            return NestedVectorBuffers.map(rows, validity, off, children[0].build(arena),
                    children[1].build(arena));
        }
    }

    private static final class Struct extends Node {
        private final Node[] fields;

        Struct(Shape s) {
            super(VecType.STRUCT);
            fields = s.children().stream()
                    .map(Node::of)
                    .toArray(Node[]::new);
        }

        @Override
        void growValues(int capacity) {}

        @Override
        void read(In in) {
            for (Node f : fields) {
                f.appendChild(in);
            }
        }

        @Override
        void fillNull() {
            for (Node f : fields) {
                f.appendNull();
            }
        }

        @Override
        VectorBuffers build(Arena arena) {
            List<VectorBuffers> built = new ArrayList<>(fields.length);
            for (Node f : fields) {
                built.add(f.build(arena));
            }
            return NestedVectorBuffers.struct(rows, ArrowLayout.validityFrom(arena, nullsOrNone(), rows), built);
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

        void b(int v) {
            ensure(1);
            buf[size++] = (byte) v;
        }

        void i32(int v) {
            ensure(4);
            for (int k = 0; k < 4; k++) {
                buf[size++] = (byte) (v >>> (8 * k));
            }
        }

        void i64(long v) {
            ensure(8);
            for (int k = 0; k < 8; k++) {
                buf[size++] = (byte) (v >>> (8 * k));
            }
        }

        void dec(BigInteger v) {
            byte[] be = v.toByteArray();
            byte pad = (byte) (v.signum() < 0 ? 0xFF : 0);
            ensure(16);
            for (int k = 0; k < 16; k++) {
                int idx = be.length - 1 - k;
                buf[size++] = idx >= 0 ? be[idx] : pad;
            }
        }

        void bytes(byte[] b) {
            ensure(b.length);
            System.arraycopy(b, 0, buf, size, b.length);
            size += b.length;
        }
    }

    private static final class In {
        final byte[] buf;
        int pos;

        In(byte[] buf) {
            this.buf = buf;
        }

        int b() {
            return buf[pos++] & 0xFF;
        }

        int i32() {
            int v = 0;
            for (int k = 0; k < 4; k++) {
                v |= (buf[pos++] & 0xFF) << (8 * k);
            }
            return v;
        }

        long i64() {
            long v = 0;
            for (int k = 0; k < 8; k++) {
                v |= (buf[pos++] & 0xFFL) << (8 * k);
            }
            return v;
        }

        BigInteger dec() {
            byte[] be = new byte[16];
            for (int k = 0; k < 16; k++) {
                be[15 - k] = buf[pos++];
            }
            return new BigInteger(be);
        }
    }
}
