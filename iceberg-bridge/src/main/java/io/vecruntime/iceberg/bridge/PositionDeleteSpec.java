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

import java.io.Serializable;
import java.util.Map;

/**
 * What a task needs to write a v2 table's position delete files (#901 M2), read
 * on the driver from the write: the delete granularity ({@code FILE}: one
 * delete file per data file, merging the file's previous file-scoped deletes;
 * {@code PARTITION}: one per partition), the target delete file size, the
 * Parquet properties of delete files (the {@code write.delete.parquet.*} keys
 * over the {@code write.parquet.*} ones), and the operation id the files are
 * named by.
 */
public record PositionDeleteSpec(boolean fileGranularity, long targetFileSize, Map<String, String> properties,
        String operationId)
        implements Serializable {}
