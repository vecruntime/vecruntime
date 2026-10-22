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
package io.vecruntime.iceberg.write;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.Bitmap;
import io.vecruntime.kernels.SegmentVectorBuffers;
import io.vecruntime.kernels.VecType;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;

/**
 * The data half of a merge-on-read write (#901 M1): the INSERT and REINSERT
 * rows of a delta batch, picked out of it by row index, written by the columnar
 * data writer ({@link IcebergDataTaskWriter}) instead of Iceberg's row-by-row
 * delta writer.
 *
 * <p>With row lineage, the batch's last two given columns are the rows' {@code
 * _row_id} and {@code _last_updated_sequence_number}, from the write's
 * metadata: a REINSERT keeps both, an INSERT writes them null (Iceberg assigns
 * them at commit), as Iceberg's {@code ExtractRowLineage} does.
 */
public final class IcebergDeltaDataWriter {

    private final IcebergDataTaskWriter writer;
    private final boolean lineage;

    public IcebergDeltaDataWriter(IcebergWriteSpec spec, int partitionId, long taskId,
            Configuration conf, boolean lineage) {
        this.writer = new IcebergDataTaskWriter(spec, partitionId, taskId, conf);
        this.lineage = lineage;
    }

    /**
     * Writes rows {@code rows[0..count)} of {@code columns} (the data columns
     * in write-schema order, then the two lineage columns when the table has
     * lineage). {@code reinsert[k]}: row {@code rows[k]} is a REINSERT.
     */
    public void write(List<? extends VectorBuffers> columns, int[] rows, int count,
                      boolean[] reinsert) {
        if (count == 0) {
            return;
        }
        int[] idx = rows.length == count ? rows : java.util.Arrays.copyOf(rows, count);
        try (Arena arena = Arena.ofConfined()) {
            List<VectorBuffers> gathered = new ArrayList<>(Gather.rows(columns, idx, arena));
            if (lineage) {
                int n = gathered.size();
                for (int c = n - 2; c < n; c++) {
                    gathered.set(c, insertsNull(gathered.get(c), reinsert, count, arena));
                }
            }
            writer.write(gathered, 0, count);
        }
    }

    /**
     * {@code lane}'s values with every INSERT row null (its lineage is assigned
     * at commit).
     */
    private static VectorBuffers insertsNull(VectorBuffers lane, boolean[] reinsert, int count,
            Arena arena) {
        MemorySegment validity = ArrowLayout.allocateBitmap(arena, count);
        MemorySegment src = lane.validity();
        for (int i = 0; i < count; i++) {
            if (reinsert[i] && (src == null || Bitmap.isSet(src, i))) {
                Bitmap.set(validity, i);
            }
        }
        if (lane.type() != VecType.INT64) {
            throw new IllegalArgumentException("a row lineage column is not an INT64 lane: " + lane.type());
        }
        return SegmentVectorBuffers.fixedWidth(VecType.INT64, count, validity, lane.data());
    }

    /** The data files written, once (open files are finished first). */
    public DataFile[] dataFiles() {
        return writer.dataFiles();
    }

    /** Abandons the task: the files written are deleted. */
    public void abort(Throwable cause) {
        writer.abort(cause);
    }
}
