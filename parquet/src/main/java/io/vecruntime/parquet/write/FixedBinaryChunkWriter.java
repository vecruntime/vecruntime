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
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;

/**
 * Writes one flat {@code FIXED_LEN_BYTE_ARRAY} column -- a fixed-size binary or
 * UUID carried in a FIXED_BINARY lane, or a decimal of precision beyond 18 held
 * as a DECIMAL128 lane and written big-endian two's-complement -- straight to
 * its {@link PageWriter}, with no per-value object and no parquet-java column
 * writer.
 *
 * <p>Every value is {@code width} bytes. Parquet v2 encodes the column exactly
 * as it encodes a BINARY one ({@code DefaultV2ValuesWriterFactory}): {@code
 * RLE_DICTIONARY} pages while the chunk's dictionary stays under {@code
 * parquet.dictionary.page.size} and pays on the first page, then {@code
 * DELTA_BYTE_ARRAY}. The only shape differences from {@link BinaryChunkWriter}
 * are that the values are a fixed width (so the PLAIN dictionary page is the
 * entries back to back with no length prefix) and that the min/max statistics
 * use the column's own comparator -- unsigned bytes for a plain fixed binary or
 * UUID, signed two's-complement for a decimal -- taken from parquet-java's
 * {@link Statistics} for the primitive type rather than a hand-rolled compare.
 */
final class FixedBinaryChunkWriter implements ChunkWriter {

    /** How the {@code width} value bytes are read from the lane. */
    enum Kind {
        /** A FIXED_BINARY lane: the value's bytes as they are. */
        FIXED,
        /**
         * A DECIMAL128 lane (16-byte stride): the low {@code width} bytes,
         * big-endian.
         */
        DECIMAL
    }

    private final Kind kind;
    private final int width;
    private final ColumnDescriptor column;
    private final boolean optional;
    private final int pageSize;
    private final int pageRowLimit;
    private final int dictionaryLimit;
    private final boolean dictionaryEnabled;

    private PageWriter pages;
    private final ParquetProperties props;
    private org.apache.parquet.column.values.bloomfilter.BloomFilterWriter bloomWriter;
    private org.apache.parquet.column.values.bloomfilter.BloomFilter bloom;

    // The page being built.
    private long[] pageValidity = new long[64];
    private int[] ids = new int[256];
    private int pageRows;
    private int pageValues;
    private final ByteSink plain = new ByteSink(4 * 1024);
    // The page's min and max, by the column's comparator, accumulated into a fresh Statistics per page.
    private Statistics<?> pageStats;
    private long[] pageSeen = new long[16];

    // The chunk's dictionary: entries back to back (each exactly width bytes), an open-addressing table.
    private boolean dictionary;
    private boolean firstPageDone;
    private boolean overflow;
    private boolean forceDictionary;
    private int dictionaryPagesWritten;
    private final ByteSink entryBytes = new ByteSink(4 * 1024);
    private int[] entryHash = new int[64];
    private int entryCount;
    private int[] table = new int[128];

    private final ByteSink body = new ByteSink(4 * 1024);
    private final ByteSink levels = new ByteSink(1024);
    private final ByteSink suffixes = new ByteSink(4 * 1024);
    private final ByteSink rleScratch = new ByteSink(1024);
    private final DeltaBinaryPackedEncoder delta = new DeltaBinaryPackedEncoder();
    private int[] prefixLengths = new int[256];
    private int[] suffixLengths = new int[256];
    // One value's bytes, reused: the lane's bytes (FIXED) or the big-endian two's-complement (DECIMAL).
    private final byte[] scratch;
    private final MemorySegment scratchSeg;

    FixedBinaryChunkWriter(Kind kind, int width, ColumnDescriptor column,
                           ParquetProperties props) {
        this.kind = kind;
        this.width = width;
        this.column = column;
        this.props = props;
        this.optional = column.getMaxDefinitionLevel() > 0;
        this.pageSize = props.getPageSizeThreshold();
        this.pageRowLimit = props.getPageRowCountLimit();
        this.dictionaryLimit = props.getDictionaryPageSizeThreshold();
        this.dictionaryEnabled = props.isDictionaryEnabled(column);
        this.scratch = new byte[width];
        this.scratchSeg = MemorySegment.ofArray(scratch);
    }

    @Override
    public void startChunk(PageWriter pageWriter, org.apache.parquet.column.values.bloomfilter.BloomFilterWriter blooms) {
        this.bloomWriter = blooms;
        this.bloom = blooms == null ? null : ChunkBloom.newFilter(column, props);
        this.pages = pageWriter;
        pageRows = 0;
        pageValues = 0;
        entryBytes.reset();
        entryCount = 0;
        Arrays.fill(table, 0);
        dictionaryPagesWritten = 0;
        firstPageDone = false;
        overflow = false;
        forceDictionary = false;
        dictionary = dictionaryEnabled;
        pageStats = null;
    }

    /** Fills {@link #scratch} with the value at row {@code i}. */
    private void valueAt(MemorySegment data, int i) {
        if (kind == Kind.FIXED) {
            MemorySegment.copy(data, ValueLayout.JAVA_BYTE, (long) i * width, scratch, 0,
                    width);
        } else {
            long base = (long) i << 4;
            // Little-endian 16 bytes -> big-endian width bytes: the low `width` bytes, most significant first.
            for (int k = 0; k < width; k++) {
                scratch[width - 1 - k] = data.get(ValueLayout.JAVA_BYTE, base + k);
            }
        }
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
            valueAt(data, i);
            pageValidity[pageRows >>> 6] |= 1L << pageRows;
            pageRows++;
            if (bloom != null) {
                bloom.insertHash(bloom.hash(Binary.fromReusedByteArray(scratch)));
            }
            if (dictionary) {
                int id = idOf();
                ids[pageValues++] = id;
                if (overflow) {
                    // The dictionary has reached its size limit: flush what is dictionary-encoded so far as an
                    // RLE_DICTIONARY page, then the rest of the chunk is plain (DELTA_BYTE_ARRAY), as
                    // parquet-java does (its already-written pages stay dictionary, the dictionary page is kept).
                    overflow = false;
                    forceDictionary = true;
                    flushPage();
                    forceDictionary = false;
                    dictionary = false;
                }
            } else {
                plain.write(scratch, 0, width);
                pageValues++;
                updateMinMax();
            }
            // Check the page size per value, as parquet-java does. While dictionary-encoded, cut strictly past
            // the raw-size threshold so that when the dictionary fills exactly at the threshold the overflow
            // path above wins and writes an RLE_DICTIONARY page first; after fallback, cut at the threshold.
            boolean full = dictionary ? encodedBytes() > pageSize : encodedBytes() >= pageSize;
            if (pageRows >= pageRowLimit || full) {
                flushPage();
            }
        }
    }

    private void ensurePage(int extra) {
        int rows = pageRows + extra;
        if (rows > ids.length) {
            ids = Arrays.copyOf(ids, Math.max(ids.length * 2, rows));
        }
        int words = (rows + 63) >>> 6;
        if (words > pageValidity.length) {
            pageValidity = Arrays.copyOf(pageValidity, Math.max(pageValidity.length * 2, words));
        }
    }

    private long encodedBytes() {
        // parquet-java sizes a page (and the row group) by the raw value bytes while the column is still
        // dictionary-encoded (FallbackValuesWriter.getBufferedSize returns rawDataByteSize), so a dictionary
        // page is cut at the same raw size and the encoding sequence agrees. After fallback it is the plain bytes.
        if (dictionary) {
            return (long) pageValues * width;
        }
        return plain.size();
    }

    /**
     * The RLE / bit-packed size of the page's dictionary ids (bit-width byte
     * included), as parquet-java's dictionary writer reports it to its
     * first-page payoff check.
     */
    private long rlePackedBytes() {
        int bitWidth = RleHybridEncoder.bitWidth(Math.max(0, entryCount - 1));
        rleScratch.reset();
        rleScratch.writeByte(bitWidth);
        RleHybridEncoder.encode(ids, pageValues, bitWidth, rleScratch);
        return rleScratch.size();
    }

    private int hashScratch() {
        int h = 1;
        for (int k = 0; k < width; k++) {
            h = 31 * h + scratch[k];
        }
        return h ^ (h >>> 16);
    }

    /** The dictionary id of {@link #scratch}, adding it if new. */
    private int idOf() {
        int h = hashScratch();
        int mask = table.length - 1;
        int slot = h & mask;
        MemorySegment entries = entryBytes.segment();
        while (true) {
            int e = table[slot];
            if (e == 0) {
                break;
            }
            int id = e - 1;
            if (entryHash[id] == h && MemorySegment.mismatch(entries, (long) id * width, (long) (id + 1) * width, scratchSeg,
                    0, width)
                    < 0) {
                return id;
            }
            slot = (slot + 1) & mask;
        }
        int id = entryCount;
        if (id >= entryHash.length) {
            entryHash = Arrays.copyOf(entryHash, entryHash.length * 2);
        }
        entryBytes.write(scratch, 0, width);
        entryHash[id] = h;
        table[slot] = ++entryCount;
        if (entryCount * 2 > table.length) {
            rehash();
        }
        if ((long) entryCount * width > dictionaryLimit) {
            overflow = true;
        }
        return id;
    }

    private void rehash() {
        int[] t = new int[table.length * 2];
        int mask = t.length - 1;
        for (int e = 0; e < entryCount; e++) {
            int s = entryHash[e] & mask;
            while (t[s] != 0) {
                s = (s + 1) & mask;
            }
            t[s] = e + 1;
        }
        table = t;
    }

    /**
     * Stops dictionary encoding: the page being built is rewritten plain from
     * its ids, the rest stays plain.
     */
    private void fallBack() {
        dictionary = false;
        plain.reset();
        pageStats = null;
        MemorySegment entries = entryBytes.segment();
        for (int k = 0; k < pageValues; k++) {
            int id = ids[k];
            plain.write(entries, (long) id * width, width);
            updateMinMaxBytes(entries, (long) id * width);
        }
    }

    private Statistics<?> freshStats() {
        return Statistics.createStats(column.getPrimitiveType());
    }

    private void updateMinMax() {
        updateMinMaxBytes(scratchSeg, 0);
    }

    private void updateMinMaxBytes(MemorySegment seg, long offset) {
        if (pageStats == null) {
            pageStats = freshStats();
        }
        byte[] b = new byte[width];
        MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, offset, b, 0, width);
        pageStats.updateStats(Binary.fromConstantByteArray(b));
    }

    void flushPage() throws IOException {
        if (pageRows == 0) {
            return;
        }
        if (dictionary && !firstPageDone && !forceDictionary) {
            // parquet-java's first-page payoff uses the RLE-packed id size plus the dictionary bytes against the
            // raw value bytes (DictionaryValuesWriter.isCompressionSatisfying over initialWriter.getBytes()).
            long raw = (long) pageValues * width;
            long dictBytes = (long) entryCount * width;
            if (rlePackedBytes() + dictBytes >= raw) {
                fallBack();
            }
        }
        firstPageDone = true;
        levels.reset();
        if (optional) {
            RleHybridEncoder.encodeValidity(MemorySegment.ofArray(pageValidity), 0, pageRows, levels);
        }
        body.reset();
        Encoding valuesEncoding;
        if (dictionary) {
            pageMinMaxFromIds();
            int bitWidth = RleHybridEncoder.bitWidth(Math.max(0, entryCount - 1));
            body.writeByte(bitWidth);
            RleHybridEncoder.encode(ids, pageValues, bitWidth, body);
            valuesEncoding = Encoding.RLE_DICTIONARY;
            dictionaryPagesWritten++;
        } else {
            deltaByteArray(body);
            valuesEncoding = Encoding.DELTA_BYTE_ARRAY;
        }
        Statistics<?> s = freshStats();
        s.incrementNumNulls(pageRows - pageValues);
        if (pageStats != null && pageStats.hasNonNullValue()) {
            s.updateStats(Binary.fromConstantByteArray(pageStats.getMinBytes()));
            s.updateStats(Binary.fromConstantByteArray(pageStats.getMaxBytes()));
        }
        pages.writePageV2(
                pageRows,
                pageRows - pageValues,
                pageRows,
                BytesInput.empty(),
                BytesInput.from(levels.toByteArray()),
                valuesEncoding,
                BytesInput.from(body.toByteArray()),
                s,
                // Size statistics (Parquet 2.10): a fixed-width column reports no unencoded byte-array bytes
                // (it is not BYTE_ARRAY) and, flat, no level histograms, as parquet-java writes them.
                new org.apache.parquet.column.statistics.SizeStatistics(column.getPrimitiveType(), 0L, java.util.List.of(),
                        java.util.List.of()),
                null); // no geospatial statistics
        Arrays.fill(pageValidity, 0, (pageRows + 63) >>> 6, 0L);
        pageRows = 0;
        pageValues = 0;
        plain.reset();
        pageStats = null;
    }

    /**
     * The page's values ({@link #plain}: width bytes per value) as {@code
     * DELTA_BYTE_ARRAY}: prefix lengths, suffix lengths (both {@code
     * DELTA_BINARY_PACKED}) then the suffixes. The prefix chain starts afresh
     * on each page.
     */
    private void deltaByteArray(ByteSink out) {
        if (prefixLengths.length < pageValues) {
            prefixLengths = new int[Math.max(pageValues, prefixLengths.length * 2)];
            suffixLengths = new int[prefixLengths.length];
        }
        suffixes.reset();
        MemorySegment p = plain.segment();
        int prevStart = 0;
        for (int k = 0; k < pageValues; k++) {
            int start = k * width;
            long m = k == 0 ? 0 : MemorySegment.mismatch(p, prevStart, prevStart + width, p, start,
                    start + width);
            int prefix = m < 0 ? width : (int) m;
            prefixLengths[k] = prefix;
            suffixLengths[k] = width - prefix;
            suffixes.write(p, start + prefix, width - prefix);
            prevStart = start;
        }
        delta.encodeInts(prefixLengths, pageValues, out);
        delta.encodeInts(suffixLengths, pageValues, out);
        out.write(suffixes);
    }

    /** The page's min and max over its distinct ids. */
    private void pageMinMaxFromIds() {
        int words = (entryCount + 63) >>> 6;
        if (pageSeen.length < words) {
            pageSeen = new long[Math.max(words, pageSeen.length * 2)];
        } else {
            Arrays.fill(pageSeen, 0, words, 0L);
        }
        MemorySegment entries = entryBytes.segment();
        for (int k = 0; k < pageValues; k++) {
            int id = ids[k];
            long bit = 1L << id;
            if ((pageSeen[id >>> 6] & bit) == 0) {
                pageSeen[id >>> 6] |= bit;
                updateMinMaxBytes(entries, (long) id * width);
            }
        }
    }

    @Override
    public void endChunk() throws IOException {
        flushPage();
        if (bloom != null) {
            bloomWriter.writeBloomFilter(bloom);
        }
        if (dictionaryPagesWritten > 0) {
            // PLAIN dictionary page for FIXED_LEN_BYTE_ARRAY: the entries back to back, no length prefix.
            ByteSink d = new ByteSink(entryCount * width);
            d.write(entryBytes.segment(), 0, entryCount * width);
            pages.writeDictionaryPage(new DictionaryPage(BytesInput.from(d.toByteArray()), entryCount, Encoding.PLAIN));
        }
    }

    @Override
    public long bufferedBytes() {
        return pages.getMemSize() + encodedBytes() + (long) entryCount * width;
    }
}
