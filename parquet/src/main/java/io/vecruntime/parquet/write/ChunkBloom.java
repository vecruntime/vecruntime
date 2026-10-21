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

import java.util.OptionalDouble;
import java.util.OptionalLong;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.bloomfilter.AdaptiveBlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;

/**
 * A column chunk's Bloom filter (#842), sized as parquet-java's column
 * writers size it: from the column's expected NDV and FPP when an NDV is set
 * (capped at {@code parquet.bloom.filter.max.bytes}), the adaptive filter when
 * {@code parquet.bloom.filter.adaptive.enabled}, and otherwise the maximum
 * size. The values are hashed with the filter's own hash, so a reader probes
 * it as it probes parquet-java's.
 */
final class ChunkBloom {

    private ChunkBloom() {}

    /**
     * A new filter for one chunk of {@code column}, or {@code null} when the
     * column has none.
     */
    static BloomFilter newFilter(ColumnDescriptor column, ParquetProperties props) {
        if (!props.isBloomFilterEnabled(column)) {
            return null;
        }
        int max = props.getMaxBloomFilterBytes();
        OptionalLong ndv = props.getBloomFilterNDV(column);
        OptionalDouble fpp = props.getBloomFilterFPP(column);
        double p = fpp.isPresent() ? fpp.getAsDouble() : BlockSplitBloomFilter.DEFAULT_FPP;
        if (ndv.isPresent()) {
            return new BlockSplitBloomFilter(BlockSplitBloomFilter.optimalNumOfBits(ndv.getAsLong(), p) / 8, max);
        }
        if (props.getAdaptiveBloomFilterEnabled(column)) {
            return new AdaptiveBlockSplitBloomFilter(max, props.getBloomFilterCandidatesCount(column), p, column);
        }
        return new BlockSplitBloomFilter(max, max);
    }
}
