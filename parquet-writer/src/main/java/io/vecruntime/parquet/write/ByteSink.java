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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * A growable little-endian byte buffer: the page and dictionary bodies the page
 * encoders build before handing them to parquet-java's page writer. Bulk
 * appends copy whole {@link MemorySegment} ranges; nothing is written a byte at
 * a time on the fixed-width paths.
 */
final class ByteSink {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    private byte[] buf;
    private MemorySegment seg;
    private int size;

    ByteSink(int capacity) {
        buf = new byte[Math.max(16, capacity)];
        seg = MemorySegment.ofArray(buf);
    }

    int size() {
        return size;
    }

    byte[] array() {
        return buf;
    }

    void reset() {
        size = 0;
    }

    void ensure(int extra) {
        if (size + extra > buf.length) {
            buf = Arrays.copyOf(buf,
                    Math.max(buf.length * 2, size + extra));
            seg = MemorySegment.ofArray(buf);
        }
    }

    void writeByte(int b) {
        ensure(1);
        buf[size++] = (byte) b;
    }

    void writeIntLE(int v) {
        ensure(4);
        seg.set(LE_INT, size, v);
        size += 4;
    }

    void writeLongLE(long v) {
        ensure(8);
        seg.set(LE_LONG, size, v);
        size += 8;
    }

    /**
     * Overwrites four bytes at {@code at} (a length prefix written after its
     * body).
     */
    void setIntLE(int at, int v) {
        seg.set(LE_INT, at, v);
    }

    void writeUnsignedVarInt(int v) {
        ensure(5);
        while ((v & ~0x7F) != 0) {
            buf[size++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buf[size++] = (byte) v;
    }

    void write(byte[] b, int off, int len) {
        ensure(len);
        System.arraycopy(b, off, buf, size, len);
        size += len;
    }

    /** Appends {@code len} bytes of {@code src} from {@code offset}. */
    void write(MemorySegment src, long offset, int len) {
        ensure(len);
        MemorySegment.copy(src, offset, seg, size, len);
        size += len;
    }

    void write(ByteSink other) {
        write(other.buf, 0, other.size);
    }

    /** A copy of the bytes written, for a page that outlives this sink's reuse. */
    byte[] toByteArray() {
        return Arrays.copyOf(buf, size);
    }
}
