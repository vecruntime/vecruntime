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

/**
 * Each row's definition level in the last batch (#568): for a flat leaf, the
 * row's level; for a list or a map, the level of the row's first entry. A
 * struct's nulls are where that level is below the struct's own, whichever of
 * its fields supplies it.
 */
public interface RowLevels {

    /** The last batch's per-row levels, valid until the next batch. */
    int[] rowLevels();

    /**
     * Ask the reader to keep its per-row levels (a flat leaf records them only
     * on request).
     */
    void recordRowLevels();
}
