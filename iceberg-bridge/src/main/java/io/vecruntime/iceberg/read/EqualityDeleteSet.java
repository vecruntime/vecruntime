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
package io.vecruntime.iceberg.read;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * One group of equality-delete rows (#901 R2): the rows of a table's equality
 * delete files that share the same equality fields, as a set probed with whole
 * batches of the engine's lanes -- no row object, no reflection, no boxing per
 * value. Spark-free.
 *
 * <p>Each key is its fields encoded in one canonical byte form, from Iceberg's
 * internal values on the delete side and from the lanes on the data side, so
 * two keys are equal exactly when Iceberg's {@code StructLikeSet} finds them
 * equal: null equals null; ints, longs, dates, times and timestamps by value;
 * floats and doubles by their bits with NaN canonical ({@code Float.compare}
 * order: {@code -0.0} differs from {@code 0.0}, NaN equals NaN); strings by
 * their UTF-8 bytes; binaries and fixed by their bytes; a uuid by its canonical
 * lowercase 8-4-4-4-12 text (the form the scan reads it as, so both sides
 * compare text to text); decimals by their unscaled value
 * at the column's scale.
 */
public final class EqualityDeleteSet {

    /** The field kinds a key is built from. */
    private enum Kind {
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        STRING,
        BINARY,
        DECIMAL,
        UUID
    }

    private final Kind[] kinds;
    private final int size;

    // Open addressing over the keys' bytes: slot -> key index + 1 (0 = empty).
    private int[] table;
    private int[] hashes;
    private byte[][] keys;

    private EqualityDeleteSet(Kind[] kinds, List<byte[]> encoded) {
        this.kinds = kinds;
        int cap = Integer.highestOneBit(Math.max(4, encoded.size() * 2) - 1) << 1;
        this.table = new int[cap];
        this.hashes = new int[encoded.size()];
        this.keys = new byte[encoded.size()][];
        int n = 0;
        for (byte[] k : encoded) {
            int h = hash(k, 0, k.length);
            if (find(k, k.length, h) >= 0) {
                continue;
            }
            keys[n] = k;
            hashes[n] = h;
            insert(n, h);
            n++;
        }
        this.size = n;
    }

    /** Whether Iceberg type {@code t} is one a key can hold. */
    public static boolean supports(Type t) {
        return kindOf(t) != null;
    }

    /**
     * The set of {@code rows} (Iceberg's internal values, e.g. a loaded {@code
     * StructLikeSet}), whose fields are those of {@code deleteType} in order.
     */
    public static EqualityDeleteSet of(Types.StructType deleteType, Iterable<? extends StructLike> rows) {
        List<Types.NestedField> fields = deleteType.fields();
        Kind[] kinds = new Kind[fields.size()];
        int[] scales = new int[fields.size()];
        for (int i = 0; i < kinds.length; i++) {
            Type t = fields.get(i).type();
            kinds[i] = kindOf(t);
            if (kinds[i] == null) {
                throw new IllegalArgumentException("equality field " + fields.get(i).name() + " of type " + t + " is not supported");
            }
            scales[i] = t instanceof Types.DecimalType d ? d.scale() : 0;
        }
        List<byte[]> encoded = new java.util.ArrayList<>();
        KeyBuffer buf = new KeyBuffer();
        for (StructLike row : rows) {
            buf.reset();
            for (int i = 0; i < kinds.length; i++) {
                encodeInternal(buf, kinds[i], scales[i], row.get(i, Object.class));
            }
            encoded.add(buf.copy());
        }
        return new EqualityDeleteSet(kinds, encoded);
    }

    /**
     * The set of an equality delete group's keys, read STRAIGHT from the delete
     * files rather than from Iceberg's loaded {@code StructLikeSet} (#rtypes).
     *
     * <p>Iceberg's {@code BaseDeleteLoader} loads a fixed/binary equality delete
     * into a {@code StructLikeSet} that collapses to one element: {@code
     * GenericParquetReaders$FixedReader.read(byte[])} reuses the caller's buffer
     * (returns the same array every row), and {@code StructLikeSet.add} keeps
     * only the {@code StructLike} reference, so every row ends up aliasing one
     * mutated array and they dedup to one key. Reading the records here and
     * encoding each into its own byte key BEFORE the next record is read (the
     * reused buffer is copied by {@link #encodeInternal}) keeps every distinct
     * key, so a 143-key fixed delete deletes the spec-correct rows.
     *
     * <p>{@code deleteSchema} is the equality fields in the delete type's order;
     * {@code files} are those fields' values as {@link InputFile}s to read with
     * Iceberg's generic Parquet reader. Applicability (which delete files reach
     * this data file, by sequence number) is decided by the caller exactly as
     * before -- only the load of the already-selected files changes.
     */
    public static EqualityDeleteSet ofDeleteFiles(org.apache.iceberg.Schema deleteSchema, Iterable<org.apache.iceberg.io.InputFile> files) {
        Types.StructType deleteType = deleteSchema.asStruct();
        List<Types.NestedField> fields = deleteType.fields();
        Kind[] kinds = new Kind[fields.size()];
        int[] scales = new int[fields.size()];
        for (int i = 0; i < kinds.length; i++) {
            Type t = fields.get(i).type();
            kinds[i] = kindOf(t);
            if (kinds[i] == null) {
                throw new IllegalArgumentException("equality field " + fields.get(i).name() + " of type " + t + " is not supported");
            }
            scales[i] = t instanceof Types.DecimalType d ? d.scale() : 0;
        }
        // One InternalRecordWrapper reused across rows: it hands encodeInternal the SAME Iceberg-internal values
        // the loaded StructLikeSet would (e.g. a uuid as java.util.UUID). We encode each row into its own byte key
        // immediately, BEFORE reading the next -- so FixedReader's reused byte[] is already copied into the key and
        // the collapse that sinks Iceberg's StructLikeSet (one shared mutated array) never happens here.
        org.apache.iceberg.data.InternalRecordWrapper wrapper = new org.apache.iceberg.data.InternalRecordWrapper(deleteType);
        List<byte[]> encoded = new java.util.ArrayList<>();
        KeyBuffer buf = new KeyBuffer();
        for (org.apache.iceberg.io.InputFile file : files) {
            try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> reader = org.apache.iceberg.parquet.Parquet.read(file)
                    .project(deleteSchema)
                    .createReaderFunc(fileSchema -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(deleteSchema, fileSchema))
                    .build()) {
                for (org.apache.iceberg.data.Record record : reader) {
                    StructLike row = wrapper.wrap(record);
                    buf.reset();
                    for (int i = 0; i < kinds.length; i++) {
                        encodeInternal(buf, kinds[i], scales[i], row.get(i, Object.class));
                    }
                    encoded.add(buf.copy());
                }
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        return new EqualityDeleteSet(kinds, encoded);
    }

    /** The number of distinct keys. */
    public int size() {
        return size;
    }

    /**
     * Clears in {@code live} (a bitmap of rows, set = live) the rows {@code
     * 0..n} of {@code columns} -- the equality fields' lanes, in the delete
     * type's order -- whose key is in the set. Rows already clear are not
     * probed.
     */
    public void clearDeleted(VectorBuffers[] columns, int n, MemorySegment live) {
        if (size == 0) {
            return;
        }
        KeyBuffer buf = new KeyBuffer();
        for (int i = 0; i < n; i++) {
            if (!Bitmap.isSet(live, i)) {
                continue;
            }
            buf.reset();
            for (int k = 0; k < kinds.length; k++) {
                encodeLane(buf, kinds[k], columns[k], i);
            }
            if (find(buf.bytes, buf.len, hash(buf.bytes, 0, buf.len)) >= 0) {
                Bitmap.clear(live, i);
            }
        }
    }

    // ------------------------------------------------------------------ encoding

    private static Kind kindOf(Type t) {
        switch (t.typeId()) {
            case INTEGER:
            case DATE:
                return Kind.INT;
            case LONG:
            case TIME:
            case TIMESTAMP:
                return Kind.LONG;
            case FLOAT:
                return Kind.FLOAT;
            case DOUBLE:
                return Kind.DOUBLE;
            case BOOLEAN:
                return Kind.BOOLEAN;
            case STRING:
                return Kind.STRING;
            case BINARY:
            case FIXED:
                return Kind.BINARY;
            case UUID:
                return Kind.UUID;
            case DECIMAL:
                return Kind.DECIMAL;
            default:
                return null;
        }
    }

    private static final byte NULL = 0;
    private static final byte VALUE = 1;

    private static void encodeInternal(KeyBuffer b, Kind k, int scale,
            Object v) {
        if (v == null) {
            b.put(NULL);
            return;
        }
        b.put(VALUE);
        switch (k) {
            // Iceberg's internal values (days, micros); a loader handing out Java time values is read the same way.
            case INT -> b.putInt(v instanceof java.time.LocalDate d
                    ? (int) d.toEpochDay()
                    : ((Number) v).intValue());
            case LONG -> b.putLong(longOf(v));
            case FLOAT -> b.putInt(Float.floatToIntBits(((Number) v).floatValue()));
            case DOUBLE -> b.putLong(Double.doubleToLongBits(((Number) v).doubleValue()));
            case BOOLEAN -> b.put((byte) (((Boolean) v) ? 1 : 0));
            case STRING -> b.putBytes(v.toString()
                    .getBytes(StandardCharsets.UTF_8));
            case BINARY -> b.putBytes(bytesOf(v));
            case UUID -> {
                // The native scan reads a uuid as its canonical lowercase 8-4-4-4-12 text (NativeParquetSupport),
                // so the delete value is encoded the same way and the data lane (below) is compared text to text.
                // Iceberg's internal uuid value may be a java.util.UUID, its canonical text, or the 16 raw bytes
                // (byte[] or ByteBuffer), depending on the delete loader's data model; all three normalise here.
                String text;
                if (v instanceof java.util.UUID u) {
                    text = u.toString();
                } else if (v instanceof CharSequence cs) {
                    text = cs.toString();
                } else {
                    text = org.apache.iceberg.util.UUIDUtil.convert(bytesOf(v)).toString();
                }
                b.putBytes(text.getBytes(StandardCharsets.UTF_8));
            }
            case DECIMAL -> b.putBytes(((BigDecimal) v).setScale(scale)
                    .unscaledValue()
                    .toByteArray());
        }
    }

    /**
     * The bytes of a binary/fixed/uuid delete value: a {@code byte[]} (generic
     * data model) or a {@link ByteBuffer}.
     */
    private static byte[] bytesOf(Object v) {
        if (v instanceof byte[] a) {
            return a;
        }
        ByteBuffer bb = ((ByteBuffer) v).duplicate();
        byte[] a = new byte[bb.remaining()];
        bb.get(a);
        return a;
    }

    private static long longOf(Object v) {
        if (v instanceof java.time.OffsetDateTime t) {
            return org.apache.iceberg.util.DateTimeUtil.microsFromTimestamptz(t);
        }
        if (v instanceof java.time.LocalDateTime t) {
            return org.apache.iceberg.util.DateTimeUtil.microsFromTimestamp(t);
        }
        if (v instanceof java.time.LocalTime t) {
            return org.apache.iceberg.util.DateTimeUtil.microsFromTime(t);
        }
        return ((Number) v).longValue();
    }

    private static void encodeLane(KeyBuffer b, Kind k, VectorBuffers c,
            int i) {
        if (c.isNull(i)) {
            b.put(NULL);
            return;
        }
        b.put(VALUE);
        switch (k) {
            case INT -> b.putInt(c.getInt(i));
            case LONG -> b.putLong(c.getLong(i));
            // A float rides the FLOAT64 lane (widened exactly) or its own 4-byte bits.
            case FLOAT -> b.putInt(Float.floatToIntBits(c.type() == VecType.FLOAT64
                    ? (float) c.getDouble(i)
                    : Float.intBitsToFloat(c.getInt(i))));
            case DOUBLE -> b.putLong(Double.doubleToLongBits(c.getDouble(i)));
            case BOOLEAN -> b.put((byte) (c.getBoolean(i) ? 1 : 0));
            // A uuid rides the UTF8 lane as its canonical text (the same bytes the delete side encodes).
            case STRING, BINARY, UUID -> b.putBytes(c.getUtf8Bytes(i));
            case DECIMAL -> {
                BigInteger unscaled = c.type() == VecType.DECIMAL128 ? c.getDecimal128(i) : BigInteger.valueOf(c.getLong(i));
                b.putBytes(unscaled.toByteArray());
            }
        }
    }

    /** A growable byte buffer for one key. */
    private static final class KeyBuffer {
        byte[] bytes = new byte[64];
        int len;

        void reset() {
            len = 0;
        }

        private void ensure(int extra) {
            if (len + extra > bytes.length) {
                bytes = Arrays.copyOf(bytes,
                        Math.max(bytes.length * 2, len + extra));
            }
        }

        void put(byte v) {
            ensure(1);
            bytes[len++] = v;
        }

        void putInt(int v) {
            ensure(4);
            for (int s = 0; s < 32; s += 8) {
                bytes[len++] = (byte) (v >>> s);
            }
        }

        void putLong(long v) {
            ensure(8);
            for (int s = 0; s < 64; s += 8) {
                bytes[len++] = (byte) (v >>> s);
            }
        }

        void putBytes(byte[] a) {
            putInt(a.length);
            ensure(a.length);
            System.arraycopy(a, 0, bytes, len, a.length);
            len += a.length;
        }

        byte[] copy() {
            return Arrays.copyOf(bytes, len);
        }
    }

    // ------------------------------------------------------------------ the table

    private static int hash(byte[] b, int from, int len) {
        int h = 0x9747b28c;
        for (int i = from; i < from + len; i++) {
            h = (h ^ b[i]) * 0x01000193;
        }
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        return h ^ (h >>> 13);
    }

    private int find(byte[] k, int len, int h) {
        int mask = table.length - 1;
        int s = h & mask;
        while (true) {
            int e = table[s];
            if (e == 0) {
                return -1;
            }
            byte[] cand = keys[e - 1];
            if (hashes[e - 1] == h && cand.length == len && Arrays.equals(cand, 0, len, k, 0, len)) {
                return e - 1;
            }
            s = (s + 1) & mask;
        }
    }

    private void insert(int index, int h) {
        int mask = table.length - 1;
        int s = h & mask;
        while (table[s] != 0) {
            s = (s + 1) & mask;
        }
        table[s] = index + 1;
    }
}
