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

import io.vecruntime.kernels.VectorBuffers;
import org.apache.parquet.column.page.PageWriter;

/**
 * A column written straight to its page writer, one column chunk per row group.
 */
interface ChunkWriter {

    /** Starts a column chunk (a row group) on {@code pages}. */
    void startChunk(PageWriter pages);

    /** Appends rows {@code [from, to)} of {@code v}. */
    void write(VectorBuffers v, int from, int to) throws IOException;

    /** Writes the last page and the dictionary page, if any page used it. */
    void endChunk() throws IOException;

    /**
     * Bytes buffered for the row-group size check: written pages, the open page
     * and the dictionary.
     */
    long bufferedBytes();
}
