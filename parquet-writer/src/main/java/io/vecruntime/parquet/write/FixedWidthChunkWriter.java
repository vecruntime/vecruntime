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
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;

import static io.vecruntime.kernels.VectorBuffers.LE_DOUBLE;
import static io.vecruntime.kernels.VectorBuffers.LE_INT;
import static io.vecruntime.kernels.VectorBuffers.LE_LONG;

/**
 * Writes one flat fixed-width column (INT32 from an INT32 lane, INT64 from an
 * INT64 lane, DOUBLE from a FLOAT64 lane) straight to its {@link PageWriter}:
 * the column's pages are built here, not through parquet-java's {@code
 * ColumnWriter}, so there is no call, no boxing and no byte-at-a-time write per
 * value.
 *
 * <p>It makes the choices parquet-java's v1 writer makes, so the file is the
 * one parquet-java would write: dictionary encoding first ({@code
 * PLAIN_DICTIONARY}), kept while the dictionary stays under {@code
 * parquet.dictionary.page.size} and, at the end of the first page, only if it
 * is smaller than the plain values; otherwise {@code PLAIN} from that page on.
 * Repetition levels are {@code BIT_PACKED} and empty (a flat column),
 * definition levels {@code RLE} with a 4-byte length prefix, taken from the
 * validity bitmap. Pages are cut at {@code parquet.page.size} bytes of values
 * or {@code parquet.page.row.count.limit} rows, checked once per slice. Page
 * statistics are the min and max of the page's values and its null count.
 */
// PLAIN_DICTIONARY and BIT_PACKED are deprecated names, but they are what parquet-java's v1 writer records.
@SuppressWarnings("deprecation")
final class FixedWidthChunkWriter {

    enum Kind {
        INT32(4),
        INT64(8),
        DOUBLE(8);

        final int width;

        Kind(int width) {
            this.width = width;
        }
    }

    private final Kind kind;
    private final ColumnDescriptor column;
    private final boolean optional;
    private final int pageSize;
    private final int pageRowLimit;
    private final int dictionaryLimit;
    private final boolean dictionaryEnabled;

    private PageWriter pages;

    // The page being built: its validity (bit set = value), its values as raw bits, and, while the chunk is
    // dictionary-encoded, their dictionary ids.
    private long[] pageValidity = new long[64];
    private long[] values = new long[4096];
    private int[] ids = new int[4096];
    private int pageRows;
    private int pageValues;

    // The chunk's dictionary: insertion-ordered entries, an open-addressing table of entry index + 1.
    private boolean dictionary;
    private boolean firstPageDone;
    private int dictionaryPagesWritten;
    private long[] entries = new long[1024];
    private int entryCount;
    private int[] table = new int[2048];

    private final ByteSink body = new ByteSink(64 * 1024);

    /**
     * The fast path for a (lane, column) pair, or {@code null} when the column
     * goes through parquet-java's column writer ({@link ColumnEncoder}): INT32
     * from an INT32 lane, INT64 from an INT64 lane except millisecond
     * timestamps (which divide), DOUBLE from a FLOAT64 lane, all flat.
     */
    static Kind kindFor(io.vecruntime.kernels.VecType lane, ColumnDescriptor column) {
        if (column.getMaxRepetitionLevel() != 0) {
            return null;
        }
        var type = column.getPrimitiveType();
        var logical = type.getLogicalTypeAnnotation();
        return switch (lane) {
            case INT32 -> type.getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32 ? Kind.INT32 : null;
            case INT64 ->
                    type.getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64 && !(logical instanceof org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts && ts.getUnit() == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS)
                            ? Kind.INT64
                            : null;
            case FLOAT64 -> type.getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE ? Kind.DOUBLE : null;
            default -> null;
        };
    }

    FixedWidthChunkWriter(Kind kind, ColumnDescriptor column, ParquetProperties props) {
        this.kind = kind;
        this.column = column;
        this.optional = column.getMaxDefinitionLevel() > 0;
        this.pageSize = props.getPageSizeThreshold();
        this.pageRowLimit = props.getPageRowCountLimit();
        this.dictionaryLimit = props.getDictionaryPageSizeThreshold();
        this.dictionaryEnabled = props.isDictionaryEnabled(column);
    }

    /** Starts a column chunk (a row group) on {@code pages}. */
    void startChunk(PageWriter pageWriter) {
        this.pages = pageWriter;
        pageRows = 0;
        pageValues = 0;
        entryCount = 0;
        Arrays.fill(table, 0);
        dictionaryPagesWritten = 0;
        firstPageDone = false;
        dictionary = dictionaryEnabled;
    }

    /** Appends rows {@code [from, to)} of {@code v}. */
    void write(VectorBuffers v, int from, int to) throws IOException {
        MemorySegment validity = v.validity();
        MemorySegment data = v.data();
        int n = to - from;
        ensurePage(n);
        for (int i = from; i < to; i++) {
            if (validity != null && !Bitmap.isSet(validity, i)) {
                pageRows++;
                continue;
            }
            long raw = switch (kind) {
                case INT32 -> data.get(LE_INT, (long) i << 2);
                case INT64 -> data.get(LE_LONG, (long) i << 3);
                case DOUBLE -> Double.doubleToRawLongBits(data.get(LE_DOUBLE, (long) i << 3));
            };
            pageValidity[pageRows >>> 6] |= 1L << pageRows;
            pageRows++;
            values[pageValues] = raw;
            if (dictionary) {
                ids[pageValues] = idOf(raw);
            }
            pageValues++;
        }
        if (dictionary && (long) entryCount * kind.width > dictionaryLimit) {
            fallBack();
        }
        if (pageRows >= pageRowLimit || encodedBytes() >= pageSize) {
            flushPage();
        }
    }

    private void ensurePage(int extra) {
        int rows = pageRows + extra;
        if (rows > values.length) {
            int cap = Math.max(values.length * 2, rows);
            values = Arrays.copyOf(values, cap);
            ids = Arrays.copyOf(ids, cap);
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
        return (long) pageValues * kind.width;
    }

    private int idOf(long raw) {
        int mask = table.length - 1;
        int h = mix(raw) & mask;
        while (true) {
            int slot = table[h];
            if (slot == 0) {
                if (entryCount == entries.length) {
                    entries = Arrays.copyOf(entries, entries.length * 2);
                }
                entries[entryCount] = raw;
                table[h] = ++entryCount;
                if (entryCount * 2 > table.length) {
                    rehash();
                }
                return entryCount - 1;
            }
            if (entries[slot - 1] == raw) {
                return slot - 1;
            }
            h = (h + 1) & mask;
        }
    }

    private void rehash() {
        int[] t = new int[table.length * 2];
        int mask = t.length - 1;
        for (int e = 0; e < entryCount; e++) {
            int h = mix(entries[e]) & mask;
            while (t[h] != 0) {
                h = (h + 1) & mask;
            }
            t[h] = e + 1;
        }
        table = t;
    }

    private static int mix(long x) {
        x *= 0x9E3779B97F4A7C15L;
        return (int) (x ^ (x >>> 32));
    }

    /**
     * Stops dictionary encoding: the current page and the rest of the chunk are
     * PLAIN.
     */
    private void fallBack() {
        dictionary = false;
    }

    /** Writes the page being built, if it has rows. */
    void flushPage() throws IOException {
        if (pageRows == 0) {
            return;
        }
        if (dictionary && !firstPageDone) {
            // parquet-java keeps the dictionary only if it pays on the first page.
            long plain = (long) pageValues * kind.width;
            long encoded = encodedBytes() + (long) entryCount * kind.width;
            if (encoded >= plain) {
                fallBack();
            }
        }
        firstPageDone = true;
        body.reset();
        if (optional) {
            int at = body.size();
            body.writeIntLE(0);
            MemorySegment v = MemorySegment.ofArray(pageValidity);
            RleHybridEncoder.encodeValidity(v, 0, pageRows, body);
            body.setIntLE(at, body.size() - at - 4);
        }
        Encoding valuesEncoding;
        if (dictionary) {
            int bitWidth = RleHybridEncoder.bitWidth(Math.max(0, entryCount - 1));
            body.writeByte(bitWidth);
            RleHybridEncoder.encode(ids, pageValues, bitWidth, body);
            valuesEncoding = Encoding.PLAIN_DICTIONARY;
            dictionaryPagesWritten++;
        } else {
            writePlain(values, pageValues, body);
            valuesEncoding = Encoding.PLAIN;
        }
        pages.writePage(BytesInput.from(body.toByteArray()), pageRows, pageRows, statistics(),
                Encoding.BIT_PACKED, optional ? Encoding.RLE : Encoding.BIT_PACKED, valuesEncoding);
        Arrays.fill(pageValidity, 0, (pageRows + 63) >>> 6, 0L);
        pageRows = 0;
        pageValues = 0;
    }

    private void writePlain(long[] raw, int n, ByteSink out) {
        out.ensure(n * kind.width);
        for (int k = 0; k < n; k++) {
            if (kind == Kind.INT32) {
                out.writeIntLE((int) raw[k]);
            } else {
                out.writeLongLE(raw[k]);
            }
        }
    }

    private Statistics<?> statistics() {
        Statistics<?> s = Statistics.createStats(column.getPrimitiveType());
        s.incrementNumNulls(pageRows - pageValues);
        if (pageValues == 0) {
            return s;
        }
        switch (kind) {
            case INT32 -> {
                int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
                for (int k = 0; k < pageValues; k++) {
                    int x = (int) values[k];
                    min = Math.min(min, x);
                    max = Math.max(max, x);
                }
                IntStatistics is = (IntStatistics) s;
                is.updateStats(min);
                is.updateStats(max);
            }
            case INT64 -> {
                long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
                for (int k = 0; k < pageValues; k++) {
                    min = Math.min(min, values[k]);
                    max = Math.max(max, values[k]);
                }
                LongStatistics ls = (LongStatistics) s;
                ls.updateStats(min);
                ls.updateStats(max);
            }
            case DOUBLE -> {
                // Per value: parquet-java's NaN and signed-zero rules stay its own.
                DoubleStatistics ds = (DoubleStatistics) s;
                for (int k = 0; k < pageValues; k++) {
                    ds.updateStats(Double.longBitsToDouble(values[k]));
                }
            }
        }
        return s;
    }

    /** Writes the last page and, if any page used it, the dictionary page. */
    void endChunk() throws IOException {
        flushPage();
        if (dictionaryPagesWritten > 0) {
            ByteSink d = new ByteSink(entryCount * kind.width);
            writePlain(entries, entryCount, d);
            pages.writeDictionaryPage(new DictionaryPage(BytesInput.from(d.toByteArray()), entryCount, Encoding.PLAIN_DICTIONARY));
        }
    }

    /**
     * Bytes buffered for the row-group size check: the open page's values and
     * the dictionary.
     */
    long bufferedBytes() {
        return pages.getMemSize() + encodedBytes() + (long) entryCount * kind.width;
    }
}
