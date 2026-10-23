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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.Statistics;

/**
 * Writes one flat BOOLEAN column (a BOOL lane) straight to its {@link
 * PageWriter}, with no per-value object and no parquet-java column writer.
 *
 * <p>Parquet v2 data pages, with BOOLEAN values encoded as PLAIN: each value a
 * single bit, least significant bit first, packed into {@code ceil(n / 8)}
 * bytes with no length prefix (the Parquet spec's PLAIN for booleans). The
 * definition levels of an optional column stay in the v2 level section as the
 * RLE / bit-packed hybrid of the validity bits. parquet-java's {@code
 * DefaultV2ValuesWriterFactory} picks the RLE values writer for booleans
 * instead, but Iceberg's own vectorized Spark reader rejects an RLE-encoded
 * boolean column ({@code VectorizedPageIterator.initDataReader}), so the writer
 * emits PLAIN, which every reader accepts at its default settings. No
 * dictionary. Page statistics are the min, max and null count, and -- as
 * parquet-java does -- a BOOLEAN column gets no Bloom filter (its value
 * collector never feeds one).
 */
final class BooleanChunkWriter implements ChunkWriter {

    private final ColumnDescriptor column;
    private final boolean optional;
    private final int pageSize;
    private final int pageRowLimit;

    private PageWriter pages;
    // The chunk's Bloom filter and where it goes: parquet-java creates one for a boolean column when Bloom
    // filters are on but never inserts a value (its value collector does not hash booleans), so ours is left
    // empty too and the two filters compare equal.
    private org.apache.parquet.column.values.bloomfilter.BloomFilterWriter bloomWriter;
    private org.apache.parquet.column.values.bloomfilter.BloomFilter bloom;
    private final ParquetProperties props;

    // The page being built: its validity (bit set = value) and its boolean values as 0/1.
    private long[] pageValidity = new long[64];
    private int[] bits = new int[256];
    private int pageRows;
    private int pageValues;
    private int trueCount;

    private final ByteSink body = new ByteSink(4 * 1024);
    private final ByteSink levels = new ByteSink(1024);

    BooleanChunkWriter(ColumnDescriptor column, ParquetProperties props) {
        this.column = column;
        this.props = props;
        this.optional = column.getMaxDefinitionLevel() > 0;
        this.pageSize = props.getPageSizeThreshold();
        this.pageRowLimit = props.getPageRowCountLimit();
    }

    @Override
    public void startChunk(PageWriter pageWriter, org.apache.parquet.column.values.bloomfilter.BloomFilterWriter blooms) {
        // parquet-java writes a Bloom filter for a boolean column when they are on, but inserts nothing into
        // it; ours is created and written the same way so the filters match.
        this.bloomWriter = blooms;
        this.bloom = blooms == null ? null : ChunkBloom.newFilter(column, props);
        this.pages = pageWriter;
        pageRows = 0;
        pageValues = 0;
        trueCount = 0;
    }

    @Override
    public void write(VectorBuffers v, int from, int to) throws IOException {
        MemorySegment validity = v.validity();
        MemorySegment data = v.data();
        ensurePage(to - from);
        for (int i = from; i < to; i++) {
            if (validity != null && !Bitmap.isSet(validity, i)) {
                pageRows++;
                continue;
            }
            boolean set = Bitmap.isSet(data, i);
            pageValidity[pageRows >>> 6] |= 1L << pageRows;
            pageRows++;
            bits[pageValues++] = set ? 1 : 0;
            if (set) {
                trueCount++;
            }
        }
        if (pageRows >= pageRowLimit || pageValues >= pageSize * 8) {
            flushPage();
        }
    }

    private void ensurePage(int extra) {
        int rows = pageRows + extra;
        if (rows > bits.length) {
            bits = Arrays.copyOf(bits, Math.max(bits.length * 2, rows));
        }
        int words = (rows + 63) >>> 6;
        if (words > pageValidity.length) {
            pageValidity = Arrays.copyOf(pageValidity, Math.max(pageValidity.length * 2, words));
        }
    }

    void flushPage() throws IOException {
        if (pageRows == 0) {
            return;
        }
        levels.reset();
        if (optional) {
            RleHybridEncoder.encodeValidity(MemorySegment.ofArray(pageValidity), 0, pageRows, levels);
        }
        // PLAIN BOOLEAN: the values bit-packed, one bit per value, least significant bit first, in
        // ceil(pageValues / 8) bytes with no length prefix and no RLE header (Iceberg's vectorized reader
        // rejects RLE-encoded boolean values; PLAIN is readable by every reader at its default settings).
        body.reset();
        body.packBits(bits, 0, pageValues, (pageValues + 7) & ~7, 1);
        pages.writePageV2(
                pageRows,
                pageRows - pageValues,
                pageRows,
                BytesInput.empty(),
                BytesInput.from(levels.toByteArray()),
                Encoding.PLAIN,
                BytesInput.from(body.toByteArray()),
                statistics(),
                new org.apache.parquet.column.statistics.SizeStatistics(column.getPrimitiveType(), 0L, java.util.List.of(),
                        java.util.List.of()),
                null); // no geospatial statistics
        Arrays.fill(pageValidity, 0, (pageRows + 63) >>> 6, 0L);
        pageRows = 0;
        pageValues = 0;
        trueCount = 0;
    }

    private Statistics<?> statistics() {
        BooleanStatistics s = (BooleanStatistics) Statistics.createStats(column.getPrimitiveType());
        s.incrementNumNulls(pageRows - pageValues);
        if (pageValues > 0) {
            // Min is false iff any value is false; max is true iff any value is true.
            if (trueCount < pageValues) {
                s.updateStats(false);
            }
            if (trueCount > 0) {
                s.updateStats(true);
            }
        }
        return s;
    }

    @Override
    public void endChunk() throws IOException {
        flushPage();
        if (bloom != null) {
            bloomWriter.writeBloomFilter(bloom);
        }
    }

    @Override
    public long bufferedBytes() {
        return pages.getMemSize() + ((long) pageValues >>> 3) + 1;
    }
}
