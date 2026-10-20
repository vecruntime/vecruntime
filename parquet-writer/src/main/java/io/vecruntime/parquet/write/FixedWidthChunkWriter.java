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
 * <p>The file is Parquet v2, with the choices parquet-java's v2 writer
 * ({@code PARQUET_2_0}) makes: data page v2, dictionary encoding first ({@code
 * RLE_DICTIONARY} pages, a {@code PLAIN} dictionary page), kept while the
 * dictionary stays under {@code parquet.dictionary.page.size} and, at the end
 * of the first page, only if it is smaller than the plain values; otherwise,
 * from that page on, {@code DELTA_BINARY_PACKED} for INT32 and INT64 and
 * {@code PLAIN} for DOUBLE. Definition levels are RLE, taken from the validity
 * bitmap, stored apart from the values as data page v2 keeps them, and there
 * are no repetition levels (a flat column). Pages are cut at {@code
 * parquet.page.size} bytes of values or {@code parquet.page.row.count.limit}
 * rows, checked once per slice. Page statistics are the min and max of the
 * page's values and its null count.
 */
final class FixedWidthChunkWriter implements ChunkWriter {

    enum Kind {
        INT32(4),
        /**
         * A decimal of precision up to 9 carried in an INT64 lane of unscaled
         * values, written as INT32.
         */
        INT32_FROM_INT64(4),
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
    // The key of each table slot, inline, so a probe reads one array.
    private long[] tableKeys = new long[2048];

    private final ByteSink body = new ByteSink(64 * 1024);
    private final ByteSink levels = new ByteSink(1024);
    private final DeltaBinaryPackedEncoder delta = new DeltaBinaryPackedEncoder();

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
        var p = type.getPrimitiveTypeName();
        boolean millis = logical instanceof org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts && ts.getUnit() == org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
        boolean decimal = logical instanceof org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
        return switch (lane) {
            case INT32 -> p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32 ? Kind.INT32 : null;
            case INT64 -> {
                if (p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64 && !millis) {
                    yield Kind.INT64;
                }
                if (p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32 && decimal) {
                    yield Kind.INT32_FROM_INT64;
                }
                yield null;
            }
            case FLOAT64 -> p == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE ? Kind.DOUBLE : null;
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
    @Override
    public void startChunk(PageWriter pageWriter) {
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
    @Override
    public void write(VectorBuffers v, int from, int to) throws IOException {
        MemorySegment validity = v.validity();
        MemorySegment data = v.data();
        int n = to - from;
        ensurePage(n);
        int start = pageValues;
        if (validity == null) {
            // No nulls: the values in one pass, the validity bits as whole words.
            switch (kind) {
                case INT32 -> {
                    for (int i = from; i < to; i++) {
                        values[pageValues++] = data.get(LE_INT, (long) i << 2);
                    }
                }
                case INT32_FROM_INT64 -> {
                    for (int i = from; i < to; i++) {
                        values[pageValues++] = (int) data.get(LE_LONG, (long) i << 3);
                    }
                }
                case INT64 -> {
                    for (int i = from; i < to; i++) {
                        values[pageValues++] = data.get(LE_LONG, (long) i << 3);
                    }
                }
                case DOUBLE -> {
                    for (int i = from; i < to; i++) {
                        values[pageValues++] = Double.doubleToRawLongBits(data.get(LE_DOUBLE, (long) i << 3));
                    }
                }
            }
            setValidRange(pageRows, n);
            pageRows += n;
        } else {
            for (int i = from; i < to; i++) {
                if (!Bitmap.isSet(validity, i)) {
                    pageRows++;
                    continue;
                }
                long raw = switch (kind) {
                    case INT32 -> data.get(LE_INT, (long) i << 2);
                    case INT32_FROM_INT64 -> (int) data.get(LE_LONG, (long) i << 3);
                    case INT64 -> data.get(LE_LONG, (long) i << 3);
                    case DOUBLE -> Double.doubleToRawLongBits(data.get(LE_DOUBLE, (long) i << 3));
                };
                pageValidity[pageRows >>> 6] |= 1L << pageRows;
                pageRows++;
                values[pageValues++] = raw;
            }
        }
        if (dictionary) {
            for (int k = start; k < pageValues; k++) {
                ids[k] = idOf(values[k]);
            }
        }
        if (dictionary && (long) entryCount * kind.width > dictionaryLimit) {
            fallBack();
        }
        if (pageRows >= pageRowLimit || encodedBytes() >= pageSize) {
            flushPage();
        }
    }

    /** Sets validity bits {@code [from, from + count)} of the page being built. */
    private void setValidRange(int from, int count) {
        int i = from;
        int end = from + count;
        while (i < end && (i & 63) != 0) {
            pageValidity[i >>> 6] |= 1L << i;
            i++;
        }
        while (i + 64 <= end) {
            pageValidity[i >>> 6] = -1L;
            i += 64;
        }
        while (i < end) {
            pageValidity[i >>> 6] |= 1L << i;
            i++;
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
                tableKeys[h] = raw;
                table[h] = ++entryCount;
                if (entryCount * 2 > table.length) {
                    rehash();
                }
                return entryCount - 1;
            }
            if (tableKeys[h] == raw) {
                return slot - 1;
            }
            h = (h + 1) & mask;
        }
    }

    private void rehash() {
        int[] t = new int[table.length * 2];
        long[] k = new long[t.length];
        int mask = t.length - 1;
        for (int e = 0; e < entryCount; e++) {
            int h = mix(entries[e]) & mask;
            while (t[h] != 0) {
                h = (h + 1) & mask;
            }
            t[h] = e + 1;
            k[h] = entries[e];
        }
        table = t;
        tableKeys = k;
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
        levels.reset();
        if (optional) {
            RleHybridEncoder.encodeValidity(MemorySegment.ofArray(pageValidity), 0, pageRows, levels);
        }
        body.reset();
        Encoding valuesEncoding;
        if (dictionary) {
            int bitWidth = RleHybridEncoder.bitWidth(Math.max(0, entryCount - 1));
            body.writeByte(bitWidth);
            RleHybridEncoder.encode(ids, pageValues, bitWidth, body);
            valuesEncoding = Encoding.RLE_DICTIONARY;
            dictionaryPagesWritten++;
        } else if (kind == Kind.DOUBLE) {
            writePlain(values, pageValues, body);
            valuesEncoding = Encoding.PLAIN;
        } else {
            if (kind.width == 4) {
                delta.encodeInts(values, pageValues, body);
            } else {
                delta.encodeLongs(values, pageValues, body);
            }
            valuesEncoding = Encoding.DELTA_BINARY_PACKED;
        }
        pages.writePageV2(
                pageRows,
                pageRows - pageValues,
                pageRows,
                BytesInput.empty(),
                BytesInput.from(levels.toByteArray()),
                valuesEncoding,
                BytesInput.from(body.toByteArray()),
                statistics(),
                // Size statistics (Parquet 2.10), as parquet-java writes them for a flat fixed-width column: no
                // byte-array bytes and no level histograms.
                new org.apache.parquet.column.statistics.SizeStatistics(column.getPrimitiveType(), 0L,
                        java.util.List.of(), java.util.List.of()),
                null); // no geospatial statistics
        Arrays.fill(pageValidity, 0, (pageRows + 63) >>> 6, 0L);
        pageRows = 0;
        pageValues = 0;
    }

    private void writePlain(long[] raw, int n, ByteSink out) {
        if (kind.width == 4) {
            out.writeIntsLE(raw, n);
        } else {
            out.writeLongsLE(raw, n);
        }
    }

    private Statistics<?> statistics() {
        Statistics<?> s = Statistics.createStats(column.getPrimitiveType());
        s.incrementNumNulls(pageRows - pageValues);
        if (pageValues == 0) {
            return s;
        }
        switch (kind) {
            case INT32, INT32_FROM_INT64 -> {
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
    @Override
    public void endChunk() throws IOException {
        flushPage();
        if (dictionaryPagesWritten > 0) {
            ByteSink d = new ByteSink(entryCount * kind.width);
            writePlain(entries, entryCount, d);
            pages.writeDictionaryPage(new DictionaryPage(BytesInput.from(d.toByteArray()), entryCount, Encoding.PLAIN));
        }
    }

    /**
     * Bytes buffered for the row-group size check: the open page's values and
     * the dictionary.
     */
    @Override
    public long bufferedBytes() {
        return pages.getMemSize() + encodedBytes() + (long) entryCount * kind.width;
    }
}
