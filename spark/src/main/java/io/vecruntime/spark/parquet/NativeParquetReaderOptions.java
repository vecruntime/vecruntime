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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;

/**
 * #735: the reader options the native Parquet scan opens files with -- the
 * reused-buffer {@link ReusingCodecFactory} and the pooled {@link
 * PooledHeapAllocator} -- in one place, so the scan ({@code
 * VectorParquetScanExec}) and the JMH benchmark ({@code
 * ParquetPageVersionBenchmark}) measure the same read path. Each call builds a
 * FRESH factory and allocator: both are stateful (a reused page buffer, a pool)
 * and bound to one {@code ParquetFileReader}, which one thread drives at a time.
 */
public final class NativeParquetReaderOptions {

    /** parquet-java's own default page size, the growable buffers' starting hint. */
    private static final int PAGE_SIZE_HINT = 1 << 20;

    /**
     * A few chunk buffers kept per reader; the fact scans read one requested
     * column chunk at a time per group.
     */
    private static final int POOL_BUFFERS = 8;

    /**
     * The pool's total heap budget per reader (one row group's requested chunks
     * fit comfortably).
     */
    private static final long POOL_MAX_BYTES = 64L << 20;

    private NativeParquetReaderOptions() {}

    /**
     * Install the reused-buffer codec factory and/or the pooled chunk allocator
     * on {@code builder}, each only when its flag is on. Returns {@code builder}
     * for chaining.
     */
    public static ParquetReadOptions.Builder apply(ParquetReadOptions.Builder builder, Configuration conf, boolean reuseDecompressBuffers,
            boolean poolChunkBuffers) {
        if (reuseDecompressBuffers) {
            builder.withCodecFactory(new ReusingCodecFactory(conf, PAGE_SIZE_HINT));
        }
        if (poolChunkBuffers) {
            builder.withAllocator(new PooledHeapAllocator(POOL_BUFFERS, POOL_MAX_BYTES));
        }
        return builder;
    }
}
