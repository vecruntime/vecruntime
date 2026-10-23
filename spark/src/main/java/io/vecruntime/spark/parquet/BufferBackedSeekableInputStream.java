/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vecruntime.spark.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.parquet.io.SeekableInputStream;

/**
 * A {@link SeekableInputStream} over one or more in-memory byte buffers already
 * read from the file, so {@code ParquetFileReader} issues no further GETs for
 * the regions the buffers cover (#834). Two shapes:
 *
 * <ul>
 *   <li><b>whole object</b> ({@link #wholeObject}): the entire file in one
 *       buffer, served from a single ranged GET on the prefetch thread for a
 *       small file (part 1 of #834). Footer, column chunks and page indexes
 *       then all come from memory.
 *   <li><b>planned ranges</b> ({@link #plannedRanges}): a sparse set of
 *       coalesced byte ranges ({@link ParquetReadRangePlanner}), each with its
 *       own buffer. A read inside a range is served from its buffer; a read
 *       outside every range (there should be none when the planner was fed the
 *       reader's exact needs) falls through to the delegate stream, which keeps
 *       correctness even if the plan missed a byte.
 * </ul>
 *
 * <p>This class is NOT thread-safe: one reader, one stream, one owning thread
 * -- the contract the scan already keeps for {@code ParquetFileReader} (a
 * file's reader is used by one thread at a time). The buffers are heap {@code
 * byte[]}; their total size is what the caller charges against the read-ahead
 * byte budget.
 */
public final class BufferBackedSeekableInputStream extends SeekableInputStream {

    /** One coalesced range and the bytes read for it. */
    static final class Loaded {
        final long offset;
        final byte[] data;

        Loaded(long offset, byte[] data) {
            this.offset = offset;
            this.data = data;
        }

        long end() {
            return offset + data.length;
        }
    }

    private final Loaded[] loaded; // sorted by offset, non-overlapping
    private final long fileLength;
    // For a read outside every loaded range; null when none is expected.
    private final SeekableInputStream delegate;
    private long pos;
    private int cursor; // index into loaded of the range that may hold pos, a hint

    private BufferBackedSeekableInputStream(Loaded[] loaded, long fileLength, SeekableInputStream delegate) {
        this.loaded = loaded;
        this.fileLength = fileLength;
        this.delegate = delegate;
    }

    /**
     * Reads the whole file ({@code length} bytes from offset 0) from {@code
     * source} into one buffer and returns a stream over it. {@code source} is
     * positioned and consumed; the caller closes it.
     */
    public static BufferBackedSeekableInputStream wholeObject(SeekableInputStream source, long length) throws IOException {
        if (length < 0 || length > Integer.MAX_VALUE - 8) {
            throw new IOException("file too large for whole-object prefetch: " + length);
        }
        byte[] buf = new byte[(int) length];
        source.seek(0L);
        source.readFully(buf, 0, buf.length);
        return new BufferBackedSeekableInputStream(new Loaded[] {new Loaded(0L, buf)}, length, null);
    }

    /**
     * Reads each planned range in {@code ranges} from {@code source} into its
     * own buffer and returns a stream over them. The ranges must be sorted by
     * offset and non-overlapping, as {@link ParquetReadRangePlanner#plan}
     * returns them. {@code delegate} serves a read that falls outside every
     * loaded range (defensive; the plan should cover the reader's needs).
     */
    public static BufferBackedSeekableInputStream plannedRanges(SeekableInputStream source, List<ParquetReadRangePlanner.Range> ranges, long fileLength,
            SeekableInputStream delegate)
            throws IOException {
        Loaded[] out = new Loaded[ranges.size()];
        long prevEnd = -1L;
        for (int i = 0; i < ranges.size(); i++) {
            ParquetReadRangePlanner.Range r = ranges.get(i);
            if (r.offset < prevEnd) {
                throw new IOException("planned ranges overlap or are unsorted at " + r);
            }
            if (r.length > Integer.MAX_VALUE - 8) {
                throw new IOException("planned range too large: " + r);
            }
            byte[] buf = new byte[(int) r.length];
            source.seek(r.offset);
            source.readFully(buf, 0, buf.length);
            out[i] = new Loaded(r.offset, buf);
            prevEnd = r.end();
        }
        return new BufferBackedSeekableInputStream(out, fileLength, delegate);
    }

    /** The heap bytes these buffers hold, for the read-ahead byte budget. */
    public long bufferedBytes() {
        long sum = 0;
        for (Loaded l : loaded) {
            sum += l.data.length;
        }
        return sum;
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public void seek(long newPos) {
        this.pos = newPos;
    }

    /**
     * The loaded range containing {@code at}, or {@code -1}. Cursor hint, then
     * a linear scan.
     */
    private int rangeContaining(long at) {
        if (loaded.length == 0) {
            return -1;
        }
        if (cursor < loaded.length && loaded[cursor].offset <= at && at < loaded[cursor].end()) {
            return cursor;
        }
        for (int i = 0; i < loaded.length; i++) {
            if (loaded[i].offset <= at && at < loaded[i].end()) {
                cursor = i;
                return i;
            }
        }
        return -1;
    }

    @Override
    public int read() throws IOException {
        if (pos >= fileLength) {
            return -1;
        }
        int idx = rangeContaining(pos);
        if (idx >= 0) {
            Loaded l = loaded[idx];
            int b = l.data[(int) (pos - l.offset)] & 0xFF;
            pos++;
            return b;
        }
        if (delegate == null) {
            throw new EOFException("byte " + pos + " not in any prefetched range");
        }
        delegate.seek(pos);
        int b = delegate.read();
        if (b >= 0) {
            pos++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (pos >= fileLength) {
            return -1;
        }
        int idx = rangeContaining(pos);
        if (idx >= 0) {
            Loaded l = loaded[idx];
            int from = (int) (pos - l.offset);
            int n = Math.min(len, l.data.length - from);
            System.arraycopy(l.data, from, b, off, n);
            pos += n;
            return n;
        }
        if (delegate == null) {
            throw new EOFException("byte " + pos + " not in any prefetched range");
        }
        delegate.seek(pos);
        int n = delegate.read(b, off, len);
        if (n > 0) {
            pos += n;
        }
        return n;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        int done = 0;
        while (done < len) {
            int n = read(bytes, start + done, len - done);
            if (n < 0) {
                throw new EOFException("end of file at " + pos + " needing " + (len - done) + " more");
            }
            done += n;
        }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        int len = buf.remaining();
        if (len == 0) {
            return 0;
        }
        if (pos >= fileLength) {
            return -1;
        }
        int idx = rangeContaining(pos);
        if (idx >= 0) {
            Loaded l = loaded[idx];
            int from = (int) (pos - l.offset);
            int n = Math.min(len, l.data.length - from);
            buf.put(l.data, from, n);
            pos += n;
            return n;
        }
        if (delegate == null) {
            throw new EOFException("byte " + pos + " not in any prefetched range");
        }
        delegate.seek(pos);
        int n = delegate.read(buf);
        if (n > 0) {
            pos += n;
        }
        return n;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = read(buf);
            if (n < 0) {
                throw new EOFException("end of file at " + pos + " with " + buf.remaining() + " remaining");
            }
        }
    }

    @Override
    public void close() {
        // The buffers are the source's bytes already copied out; nothing of ours to release but the
        // delegate,
        // when we kept one for a miss.
        if (delegate != null) {
            try {
                delegate.close();
            } catch (IOException ignored) {
                // closing a spent stream
            }
        }
    }
}
