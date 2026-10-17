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
package io.vecruntime.spark.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.github.luben.zstd.ZstdDecompressCtx;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

/**
 * #735: a {@link CompressionCodecFactory} the native scan installs
 * ({@code ParquetReadOptions.withCodecFactory}) so a column chunk's data pages
 * decompress THROUGH ONE REUSED {@code byte[]} per decompressor instead of a
 * fresh array per page. parquet-java's own factory allocates a page-sized array
 * every page -- {@code BytesInput$StreamBytesInput.toByteArray} (snappy, via the
 * Hadoop codec's stream) and {@code HeapByteBufferAllocator.allocate} (zstd,
 * gzip) were ~17 GB (zstd) / 11 GB (snappy) of the 3 TB scan-only allocation per
 * executor window. The decompressed bytes are written into a growable reused
 * buffer and returned as {@code BytesInput.from(byte[], off, len)}; #786's
 * in-place path ({@code NativeParquetColumnReader.heapView}) then reads that
 * array with no further copy.
 *
 * <h2>Buffer lifetime (the correctness invariant)</h2>
 * A reused buffer is valid only until the NEXT {@code decompress} on the same
 * decompressor. parquet-java calls {@code decompress} exactly once per
 * {@code readPage()} (the {@code DataPage.accept} visitor in {@code
 * ColumnChunkPageReadStore$ColumnChunkPageReader}), one page at a time, so at
 * most one data page's reused bytes are alive between two {@code readPage()}
 * calls.
 * <ul>
 *   <li>A FLAT (non-repeated) leaf feeds each page to the decoder and the
 *       decoder consumes it before the reader pulls the next page -- one page is
 *       live at a time, so a single reused buffer is safe. This is where almost
 *       all of the scan's pages (and the allocation) are.
 *   <li>A REPEATED leaf (a list/map element) pulls pages AHEAD and queues
 *       several at once ({@code NativeParquetColumnReader.pullPage}); those
 *       cannot all share one reused buffer. The reader copies a repeated leaf's
 *       page out of the reused buffer into its own array ({@code ownedView} with
 *       {@code reuseDecompress} on), so the decompressor may reuse freely.
 *   <li>A DICTIONARY page lives for the whole chunk, and parquet-java routes it
 *       through the SAME decompressor instance ({@code readDictionaryPage}), so
 *       a reused buffer would be overwritten by the first data page. The
 *       decompressor therefore ALWAYS allocates a fresh array for a dictionary
 *       page -- it tells them apart because the reader decodes the dictionary
 *       (its one call) before it reads any data page, so the first
 *       {@code decompress} after {@code getDecompressor} is the dictionary.
 * </ul>
 *
 * <p>Only ZSTD and SNAPPY are reused here (the two the profile flagged); every
 * other codec delegates to parquet-java's default {@link CodecFactory} and is
 * returned unchanged. Not thread safe: one factory + its decompressors per
 * {@code ParquetFileReader}, which one thread uses at a time (prefetch builds a
 * reader on one background thread, hands it over, and the task thread drives it).
 */
final class ReusingCodecFactory implements CompressionCodecFactory {

    private final CodecFactory delegate;

    ReusingCodecFactory(org.apache.hadoop.conf.Configuration conf, int pageSizeHint) {
        // The delegate owns the Hadoop codecs for every non-reused codec, and the compressors (write path,
        // never used here). pageSizeHint is parquet-java's own default page size; the reused buffers grow.
        this.delegate = new CodecFactory(conf, pageSizeHint);
    }

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        return delegate.getCompressor(codecName);
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        switch (codecName) {
            case ZSTD:
                return new ZstdReusingDecompressor();
            case SNAPPY:
                return new SnappyReusingDecompressor();
            default:
                return delegate.getDecompressor(codecName);
        }
    }

    @Override
    public void release() {
        delegate.release();
    }

    /**
     * A reused growable {@code byte[]} for a column chunk's data pages, and a
     * fresh array for the chunk's one dictionary page (which outlives every
     * reused page). The first {@code decompress} after construction is the
     * dictionary when the chunk has one -- parquet-java decodes it first.
     */
    private abstract static class ReusingDecompressor implements BytesInputDecompressor {
        byte[] buf = new byte[0];
        // The chunk's dictionary page is decoded before any data page; it must not share the reused buffer.
        boolean first = true;

        /**
         * A buffer of at least {@code n} bytes to decompress a data page into
         * (reused across pages).
         */
        final byte[] reused(int n) {
            if (buf.length < n) {
                int cap = Math.max(n, Math.max(64, buf.length * 2));
                buf = new byte[cap];
            }
            return buf;
        }

        /**
         * Decompress {@code in} ({@code uncompressedSize} bytes out) into
         * {@code dst} at offset 0; return bytes written.
         */
        abstract int decompressInto(BytesInput in, int uncompressedSize, byte[] dst) throws IOException;

        @Override
        public final BytesInput decompress(BytesInput in, int uncompressedSize) throws IOException {
            if (first) {
                // Dictionary page (or a chunk with no dictionary -- the first data page then owns a fresh array
                // for this one call; later pages reuse). Either way its bytes must not be overwritten by a reuse.
                first = false;
                byte[] fresh = new byte[Math.max(uncompressedSize, 0)];
                int n = decompressInto(in, uncompressedSize, fresh);
                return BytesInput.from(fresh, 0, n);
            }
            byte[] dst = reused(uncompressedSize);
            int n = decompressInto(in, uncompressedSize, dst);
            return BytesInput.from(dst, 0, n);
        }

        @Override
        public void release() {
            buf = new byte[0];
        }
    }

    /** ZSTD via zstd-jni's {@link ZstdDecompressCtx}, reused across pages. */
    private static final class ZstdReusingDecompressor extends ReusingDecompressor {
        private final ZstdDecompressCtx ctx = new ZstdDecompressCtx();

        @Override
        @SuppressWarnings("deprecation") // BytesInput.toByteArray(): the compressed source, read once per page
        int decompressInto(BytesInput in, int uncompressedSize, byte[] dst) throws IOException {
            byte[] src = in.toByteArray();
            // zstd-jni decompresses the whole frame; the uncompressed size is known from the page header.
            return ctx.decompressByteArray(dst, 0, dst.length, src, 0, src.length);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output,
                               int uncompressedSize)
                throws IOException {
            // The sized/direct overload: parquet-java takes it only with a DIRECT allocator (our allocator is
            // heap, so this is not reached in the scan). zstd-jni writes between buffers and leaves `output`'s
            // position past the written bytes, which is what the caller's `flip()` then expects.
            ctx.decompress(output, input);
        }

        @Override
        public void release() {
            super.release();
            ctx.close();
        }
    }

    /** SNAPPY via {@link Snappy}'s array API, reused across pages. */
    private static final class SnappyReusingDecompressor extends ReusingDecompressor {
        @Override
        @SuppressWarnings("deprecation") // BytesInput.toByteArray(): the compressed source, read once per page
        int decompressInto(BytesInput in, int uncompressedSize, byte[] dst) throws IOException {
            byte[] src = in.toByteArray();
            if (src.length == 0) {
                return 0;
            }
            // Snappy.uncompress(src, srcOff, srcLen, dst, dstOff): writes into the reused dst, returns the count.
            return Snappy.uncompress(src, 0, src.length, dst, 0);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output,
                               int uncompressedSize)
                throws IOException {
            // Snappy.uncompress(src, dst) returns the bytes written and leaves `dst`'s position past them --
            // which is what the caller's `flip()` expects. (Heap allocator never reaches this overload.)
            Snappy.uncompress(input, output);
        }
    }
}
