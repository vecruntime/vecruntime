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
package io.vecruntime.parquet.read;

import java.lang.foreign.MemorySegment;

import org.apache.parquet.column.page.PageReadStore;

/**
 * One column of a Parquet scan, with no engine above it (#900 phase 2): a leaf
 * column chunk ({@link LeafColumnReader}) or a nested reader over several. A
 * batch is an opaque handle the column made: the engine's adapter wraps it as
 * its own column and hands it back ({@link #release}) when the batch is done.
 */
public interface ColumnReader {

    /**
     * Begin a row group of {@code rows} rows: each leaf takes its page reader
     * from {@code store}.
     */
    void startRowGroup(PageReadStore store, int rows);

    /**
     * The next {@code n} rows of the row group, as a batch handle the caller
     * owns.
     */
    Object readBatch(int n);

    /**
     * {@link #readBatch} decoding only the rows set in {@code selection} (#611);
     * the other rows are null or zero, and the batch's selection excludes them.
     */
    Object readBatchSelected(int n, MemorySegment selection);

    /** Whether {@link #readBatchSelected} can beat decoding the batch whole. */
    boolean decodesSelectedFaster();

    /** Skips the next {@code n} rows of the row group without decoding them. */
    void skipRows(int n);

    /** Returns a batch's memory to the column (pooled or closed). */
    void release(Object batch);

    /** How many released vectors each leaf keeps for reuse. */
    void setPoolLimit(int limit);

    /**
     * #735: whether the scan installed a codec factory that reuses its buffers,
     * so a page the decompressor returns lives in a buffer reused by the next
     * page. A repeated leaf (which pulls pages ahead and queues several) must
     * then copy each page out; a flat leaf consumes one page at a time and
     * reads it in place either way. Default off: with the default codec factory
     * each page is its own fresh array and nothing copies (#786).
     */
    void setReuseDecompress(boolean reuse);

    /**
     * This file's calendar rebase and INT96 conversion (#568 item 6), for every
     * date and timestamp leaf below. Abstract on purpose: a reader that forgot
     * to forward it would read a LEGACY file without its rebase.
     */
    void setRebase(CalendarRebase rebase);

    /**
     * Emit dictionary-encoded strings as dictionary ids (#612), where the
     * column allows it.
     */
    void setDictionaryIds(boolean enabled);

    /** Rows still unemitted in the current row group. */
    int rowsRemaining();

    void close();
}
