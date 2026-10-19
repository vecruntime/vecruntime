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
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;

import static io.vecruntime.kernels.VectorBuffers.LE_INT;

/**
 * Writes one flat BINARY column (a UTF8 lane: strings or binary) straight to
 * its {@link PageWriter}, from plain offsets and bytes or from dictionary ids,
 * with no per-value object.
 *
 * <p>As {@link FixedWidthChunkWriter}, it writes Parquet v2 with parquet-java's
 * v2 choices: {@code RLE_DICTIONARY} pages while the dictionary stays under
 * {@code parquet.dictionary.page.size} bytes (each entry counted with its
 * 4-byte length) and pays on the first page, {@code DELTA_BYTE_ARRAY} from the
 * page where it stops.
 *
 * <p>The chunk's dictionary is its own: entries are the distinct values in
 * first-seen order, kept as bytes in one buffer and found through an
 * open-addressing table on a hash of their bytes. A dictionary-encoded input
 * column is mapped through a per-input-dictionary id cache, so each of its
 * distinct entries is hashed once per chunk, not once per row. Page statistics
 * (min, max, null count) compare unsigned bytes, the order parquet-java uses
 * for strings; in dictionary mode only the page's distinct ids are compared.
 */
final class BinaryChunkWriter implements ChunkWriter {

    private final ColumnDescriptor column;
    private final boolean optional;
    private final int pageSize;
    private final int pageRowLimit;
    private final int dictionaryLimit;
    private final boolean dictionaryEnabled;

    private PageWriter pages;

    // The page being built.
    private long[] pageValidity = new long[64];
    private int[] ids = new int[4096];
    private int pageRows;
    private int pageValues;
    private final ByteSink plain = new ByteSink(64 * 1024);
    private byte[] min;
    private byte[] max;
    private MemorySegment minSeg;
    private MemorySegment maxSeg;
    private long[] pageSeen = new long[16];

    // The chunk's dictionary.
    private boolean dictionary;
    private boolean firstPageDone;
    private int dictionaryPagesWritten;
    private final ByteSink entryBytes = new ByteSink(64 * 1024);
    private int[] entryStart = new int[1025];
    private int[] entryHash = new int[1024];
    private int entryCount;
    private int[] table = new int[2048];
    private long dictionaryByteSize;

    // Input-dictionary id -> chunk id (+1; 0 = not mapped yet), for the input dictionary last seen.
    private VectorBuffers inputDictionary;
    private int[] inputMap = new int[0];

    private final ByteSink body = new ByteSink(64 * 1024);
    private final ByteSink levels = new ByteSink(1024);
    private final ByteSink suffixes = new ByteSink(64 * 1024);
    private final DeltaBinaryPackedEncoder delta = new DeltaBinaryPackedEncoder();
    private int[] prefixLengths = new int[4096];
    private int[] suffixLengths = new int[4096];

    BinaryChunkWriter(ColumnDescriptor column, ParquetProperties props) {
        this.column = column;
        this.optional = column.getMaxDefinitionLevel() > 0;
        this.pageSize = props.getPageSizeThreshold();
        this.pageRowLimit = props.getPageRowCountLimit();
        this.dictionaryLimit = props.getDictionaryPageSizeThreshold();
        this.dictionaryEnabled = props.isDictionaryEnabled(column);
    }

    @Override
    public void startChunk(PageWriter pageWriter) {
        this.pages = pageWriter;
        pageRows = 0;
        pageValues = 0;
        plain.reset();
        min = null;
        max = null;
        entryBytes.reset();
        entryCount = 0;
        dictionaryByteSize = 0;
        Arrays.fill(table, 0);
        dictionaryPagesWritten = 0;
        firstPageDone = false;
        dictionary = dictionaryEnabled;
        inputDictionary = null;
    }

    @SuppressWarnings("ReferenceEquality") // the id cache is keyed on the input dictionary object itself
    @Override
    public void write(VectorBuffers v, int from, int to) throws IOException {
        MemorySegment validity = v.validity();
        ensurePage(to - from);
        VectorBuffers dict = v.dictionary();
        MemorySegment offsets = dict != null ? dict.offsets() : v.offsets();
        MemorySegment data = dict != null ? dict.data() : v.data();
        MemorySegment inIds = dict != null ? v.data() : null;
        if (dict != null && dict != inputDictionary) {
            inputDictionary = dict;
            if (inputMap.length < dict.length()) {
                inputMap = new int[dict.length()];
            } else {
                Arrays.fill(inputMap, 0, dict.length(), 0);
            }
        }
        for (int i = from; i < to; i++) {
            if (validity != null && !Bitmap.isSet(validity, i)) {
                pageRows++;
                continue;
            }
            pageValidity[pageRows >>> 6] |= 1L << pageRows;
            pageRows++;
            int src = inIds != null ? inIds.get(LE_INT, (long) i << 2) : i;
            int start = offsets.get(LE_INT, (long) src << 2);
            int len = offsets.get(LE_INT, (long) (src + 1) << 2) - start;
            if (dictionary) {
                int id;
                if (inIds != null) {
                    id = inputMap[src] - 1;
                    if (id < 0) {
                        id = idOf(data, start, len);
                        inputMap[src] = id + 1;
                    }
                } else {
                    id = idOf(data, start, len);
                }
                ids[pageValues++] = id;
                if (!dictionary) {
                    // idOf crossed the dictionary limit and rewrote the page plain without this value.
                    plain.writeIntLE(len);
                    plain.write(data, start, len);
                    updateMinMax(data, start, len);
                }
            } else {
                plain.writeIntLE(len);
                plain.write(data, start, len);
                pageValues++;
                updateMinMax(data, start, len);
            }
        }
        if (pageRows >= pageRowLimit || encodedBytes() >= pageSize) {
            flushPage();
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
        if (dictionary) {
            return ((long) pageValues * RleHybridEncoder.bitWidth(Math.max(0, entryCount - 1)) + 7) >>> 3;
        }
        return plain.size();
    }

    private static int hash(MemorySegment data, int start, int len) {
        int h = 1;
        for (int k = 0; k < len; k++) {
            h = 31 * h + data.get(ValueLayout.JAVA_BYTE, start + k);
        }
        return h ^ (h >>> 16);
    }

    private int idOf(MemorySegment data, int start, int len) {
        int h = hash(data, start, len);
        int mask = table.length - 1;
        int slot = h & mask;
        MemorySegment entries = entryBytes.segment();
        while (true) {
            int e = table[slot];
            if (e == 0) {
                break;
            }
            int id = e - 1;
            int s = entryStart[id];
            int l = entryStart[id + 1] - s;
            if (entryHash[id] == h && l == len && MemorySegment.mismatch(entries, s, s + l, data, start,
                    start + len)
                    < 0) {
                return id;
            }
            slot = (slot + 1) & mask;
        }
        int id = entryCount;
        if (id + 2 > entryStart.length) {
            entryStart = Arrays.copyOf(entryStart, entryStart.length * 2);
        }
        if (id >= entryHash.length) {
            entryHash = Arrays.copyOf(entryHash, entryHash.length * 2);
        }
        entryStart[id] = entryBytes.size();
        entryBytes.write(data, start, len);
        entryStart[id + 1] = entryBytes.size();
        entryHash[id] = h;
        table[slot] = ++entryCount;
        dictionaryByteSize += 4 + len;
        if (entryCount * 2 > table.length) {
            rehash();
        }
        if (dictionaryByteSize > dictionaryLimit) {
            fallBack();
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
        min = null;
        max = null;
        MemorySegment entries = entryBytes.segment();
        for (int k = 0; k < pageValues; k++) {
            int s = entryStart[ids[k]];
            int l = entryStart[ids[k] + 1] - s;
            plain.writeIntLE(l);
            plain.write(entries, s, l);
            updateMinMax(entries, s, l);
        }
    }

    private void updateMinMax(MemorySegment data, int start, int len) {
        if (min == null) {
            min = copy(data, start, len);
            minSeg = MemorySegment.ofArray(min);
            max = min;
            maxSeg = minSeg;
            return;
        }
        if (compare(data, start, len, minSeg) < 0) {
            min = copy(data, start, len);
            minSeg = MemorySegment.ofArray(min);
        } else if (compare(data, start, len, maxSeg) > 0) {
            max = copy(data, start, len);
            maxSeg = MemorySegment.ofArray(max);
        }
    }

    private static byte[] copy(MemorySegment data, int start, int len) {
        byte[] b = new byte[len];
        MemorySegment.copy(data, ValueLayout.JAVA_BYTE, start, b, 0, len);
        return b;
    }

    /**
     * Unsigned lexicographic comparison of {@code data[start, start + len)}
     * with {@code b}.
     */
    private static int compare(MemorySegment data, int start, int len,
            MemorySegment other) {
        long n = other.byteSize();
        long m = MemorySegment.mismatch(data, start, start + len, other, 0,
                n);
        if (m < 0) {
            return 0;
        }
        if (m == len) {
            return -1;
        }
        if (m == n) {
            return 1;
        }
        return Integer.compare(data.get(ValueLayout.JAVA_BYTE, start + m) & 0xFF, other.get(ValueLayout.JAVA_BYTE, m) & 0xFF);
    }

    void flushPage() throws IOException {
        if (pageRows == 0) {
            return;
        }
        if (dictionary && !firstPageDone) {
            long rawSize = 0;
            for (int k = 0; k < pageValues; k++) {
                rawSize += 4 + entryStart[ids[k] + 1] - entryStart[ids[k]];
            }
            if (encodedBytes() + dictionaryByteSize >= rawSize) {
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
        BinaryStatistics s = (BinaryStatistics) Statistics.createStats(column.getPrimitiveType());
        s.incrementNumNulls(pageRows - pageValues);
        if (min != null) {
            s.updateStats(Binary.fromConstantByteArray(min));
            s.updateStats(Binary.fromConstantByteArray(max));
        }
        pages.writePageV2(
                pageRows,
                pageRows - pageValues,
                pageRows,
                BytesInput.empty(),
                BytesInput.from(levels.toByteArray()),
                valuesEncoding,
                BytesInput.from(body.toByteArray()),
                s);
        Arrays.fill(pageValidity, 0, (pageRows + 63) >>> 6, 0L);
        pageRows = 0;
        pageValues = 0;
        plain.reset();
        min = null;
        max = null;
    }

    /**
     * The page's values ({@link #plain}: 4-byte length, bytes, per value) as
     * {@code DELTA_BYTE_ARRAY}: the lengths of the prefix each value shares
     * with the previous one, then the suffix lengths, both {@code
     * DELTA_BINARY_PACKED}, then the suffixes back to back. The prefix chain
     * starts afresh on each page.
     */
    private void deltaByteArray(ByteSink out) {
        if (prefixLengths.length < pageValues) {
            prefixLengths = new int[Math.max(pageValues, prefixLengths.length * 2)];
            suffixLengths = new int[prefixLengths.length];
        }
        suffixes.reset();
        MemorySegment p = plain.segment();
        int pos = 0;
        int prevStart = 0;
        int prevLen = 0;
        for (int k = 0; k < pageValues; k++) {
            int len = p.get(LE_INT, pos);
            int start = pos + 4;
            long m = k == 0 ? 0 : MemorySegment.mismatch(p, prevStart, prevStart + prevLen, p, start,
                    start + len);
            int prefix = m < 0 ? Math.min(prevLen, len) : (int) m;
            prefixLengths[k] = prefix;
            suffixLengths[k] = len - prefix;
            suffixes.write(p, start + prefix, len - prefix);
            prevStart = start;
            prevLen = len;
            pos = start + len;
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
                int s = entryStart[id];
                updateMinMax(entries, s, entryStart[id + 1] - s);
            }
        }
    }

    @Override
    public void endChunk() throws IOException {
        flushPage();
        if (dictionaryPagesWritten > 0) {
            ByteSink d = new ByteSink((int) dictionaryByteSize);
            MemorySegment entries = entryBytes.segment();
            for (int e = 0; e < entryCount; e++) {
                int s = entryStart[e];
                int l = entryStart[e + 1] - s;
                d.writeIntLE(l);
                d.write(entries, s, l);
            }
            pages.writeDictionaryPage(new DictionaryPage(BytesInput.from(d.toByteArray()), entryCount, Encoding.PLAIN));
        }
    }

    @Override
    public long bufferedBytes() {
        return pages.getMemSize() + encodedBytes() + dictionaryByteSize;
    }
}
