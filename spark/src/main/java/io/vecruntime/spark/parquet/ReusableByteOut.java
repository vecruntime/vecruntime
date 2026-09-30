/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
package io.vecruntime.spark.parquet;

import java.io.OutputStream;

/**
 * A minimal reused {@link OutputStream} over a growable {@code byte[]}: the scan
 * reader writes each decompressed Parquet page into it with
 * {@code BytesInput.writeAllTo}, reads the backing array directly (no defensive
 * copy), and {@link #reset()}s it for the next page -- so a whole column chunk
 * decompresses through one buffer instead of a {@code toByteArray} allocation
 * per page (the allocation the JFR profile flagged). Not thread safe; one per
 * column reader.
 */
final class ReusableByteOut extends OutputStream {

    private byte[] buf;
    private int len;

    ReusableByteOut(int initialCapacity) {
        buf = new byte[Math.max(initialCapacity, 64)];
    }

    @Override
    public void write(int b) {
        ensure(len + 1);
        buf[len++] = (byte) b;
    }

    @Override
    public void write(byte[] src, int off, int n) {
        ensure(len + n);
        System.arraycopy(src, off, buf, len, n);
        len += n;
    }

    private void ensure(int need) {
        if (need > buf.length) {
            int cap = buf.length;
            while (cap < need) {
                cap <<= 1;
            }
            buf = java.util.Arrays.copyOf(buf, cap);
        }
    }

    /** The backing array; valid for {@code [0, size())}. Not copied. */
    byte[] array() {
        return buf;
    }

    int size() {
        return len;
    }

    void reset() {
        len = 0;
    }
}
