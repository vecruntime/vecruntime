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

import java.io.Serializable;
import java.util.List;

import org.apache.iceberg.StructLike;

/**
 * One output file's worth of a distributed bin-pack copy (#957 M4c): the
 * ordered source (file, row-group ordinals) entries whose byte-copied row
 * groups fill one target-sized output, plus the partition they all share.
 * Planned on the driver and shipped to one executor task, so it must be
 * serializable -- every field is ({@link StructLike} here is Iceberg's
 * serializable {@code PartitionData}, copied by the planner).
 */
public final class BinPackCopyBin implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * One source file's contribution to a bin: its location and the row-group
     * ordinals to copy.
     */
    public static final class Entry implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String location;
        private final int[] rowGroups;

        public Entry(String location, int[] rowGroups) {
            this.location = location;
            this.rowGroups = rowGroups;
        }

        public String location() {
            return location;
        }

        /**
         * The row-group ordinals to copy from {@link #location()} (empty/null
         * means all).
         */
        public int[] rowGroups() {
            return rowGroups;
        }
    }

    @SuppressWarnings("serial")
    private final List<Entry> entries;

    // Iceberg PartitionData; null for an unpartitioned spec.
    @SuppressWarnings("serial")
    private final StructLike partition;

    public BinPackCopyBin(List<Entry> entries, StructLike partition) {
        this.entries = entries;
        this.partition = partition;
    }

    public List<Entry> entries() {
        return entries;
    }

    /**
     * The partition tuple all this bin's files share, or null for an
     * unpartitioned spec.
     */
    public StructLike partition() {
        return partition;
    }
}
