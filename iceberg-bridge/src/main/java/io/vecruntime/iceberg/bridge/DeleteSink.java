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
package io.vecruntime.iceberg.bridge;

import java.io.IOException;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;

/**
 * Where one write task's deletes go, one data file's run of positions per call:
 * deletion vectors on a v3 table ({@link ColumnarDvWriter}), Parquet position
 * delete files on a v2 table ({@link ColumnarPositionDeleteWriter}).
 */
public interface DeleteSink extends AutoCloseable {

    /**
     * Records positions {@code positions[0..count)} of the data file at {@code
     * dataFilePath} as deleted.
     */
    void deleteFile(String dataFilePath, long[] positions, int count,
                    PartitionSpec spec, StructLike partition);

    /**
     * The delete files written, the data files they reference and the delete
     * files they replace, once closed.
     */
    DeleteWriteResult result();

    @Override
    void close() throws IOException;
}
