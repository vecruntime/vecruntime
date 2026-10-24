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

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.spark.SparkExecutorCache;

/**
 * A {@link BaseDeleteLoader} that consults Iceberg's own per-executor {@link
 * SparkExecutorCache}, so a delete file referenced by many data files of the
 * same table is read and parsed once per executor instead of once per data file
 * (#857).
 *
 * <p>vecruntime's native Iceberg scan builds a plain {@code BaseDeleteLoader}
 * whose {@code canCache()} returns false, so a partition-granularity
 * position-delete file (one delete file shared by every data file in a
 * partition) is re-read and re-parsed for each data file. Iceberg's own Spark
 * reader avoids this with a private {@code CachingDeleteLoader} that delegates
 * to {@code SparkExecutorCache}; this class is the public equivalent our
 * reflective scan can instantiate, so operators get one knob ({@code
 * spark.sql.iceberg.executor-cache.*}) and the behaviour matches Iceberg's
 * reader byte for byte.
 *
 * <p>The two overrides mirror Iceberg's {@code
 * BaseReader.SparkDeleteFilter.CachingDeleteLoader} exactly: {@link
 * #canCache(long)} admits a value no larger than the cache's per-entry ceiling,
 * and {@link #getOrLoad} keys the shared cache by the delete file's own
 * location under a per-table group so the cache is scoped and invalidated per
 * table. The cached values ({@code PositionDeleteIndex} / {@code
 * StructLikeSet}) are produced once and only read afterwards by the scan (it
 * never mutates them), so sharing them across concurrent tasks is safe;
 * delete-file locations are immutable in Iceberg, so a cached entry can never
 * go stale.
 */
public class CachingDeleteLoader extends BaseDeleteLoader {

    private final SparkExecutorCache cache;
    private final String group;

    /**
     * @param loadInputFile opens a delete file through the partition's {@code
     *     FileIO} (the same handle Iceberg's reader opens them with)
     * @param cache the process-wide executor cache (Iceberg's {@code
     *     SparkExecutorCache.getOrCreate()}); never null
     * @param group the cache group this table's entries live under, so they are
     *     scoped and invalidated per table (Iceberg uses the table name)
     */
    public CachingDeleteLoader(Function<DeleteFile, InputFile> loadInputFile, SparkExecutorCache cache, String group) {
        super(loadInputFile);
        this.cache = cache;
        this.group = group;
    }

    @Override
    protected boolean canCache(long size) {
        return size <= cache.maxEntrySize();
    }

    @Override
    protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
        return cache.getOrLoad(group, key, valueSupplier, valueSize);
    }
}
