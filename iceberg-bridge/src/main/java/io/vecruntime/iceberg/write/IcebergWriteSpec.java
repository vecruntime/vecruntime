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
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

/**
 * What a task needs to write an Iceberg write's data files (#842, W2): the
 * table (a serializable copy, as Iceberg's own Spark writer ships it), the
 * write schema, the output partition spec, the file-size target, the write's
 * properties and its operation id. Built on the driver from Iceberg's own
 * {@code SparkWrite} (see {@code IcebergDataCommitBridge.specOf}), so the write
 * options and session settings Iceberg resolved are the ones used.
 */
public final class IcebergWriteSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    // A SerializableTable copy (Table itself is not Serializable), as Iceberg's Spark writer ships it.
    @SuppressWarnings("serial")
    private final Table table;

    private final Schema writeSchema;
    private final int specId;
    private final long targetFileSize;

    // Map.copyOf: an immutable, serializable map.
    @SuppressWarnings("serial")
    private final Map<String, String> properties;

    private final String operationId;

    public IcebergWriteSpec(Table table, Schema writeSchema, int specId,
                            long targetFileSize, Map<String, String> properties, String operationId) {
        this.table = table;
        this.writeSchema = writeSchema;
        this.specId = specId;
        this.targetFileSize = targetFileSize;
        this.properties = Map.copyOf(properties);
        this.operationId = operationId;
    }

    public Table table() {
        return table;
    }

    public Schema writeSchema() {
        return writeSchema;
    }

    public PartitionSpec spec() {
        return table.specs().get(specId);
    }

    public long targetFileSize() {
        return targetFileSize;
    }

    /**
     * The table's properties overlaid with the write's own ({@code
     * SparkWriteConf.writeProperties}).
     */
    public Map<String, String> properties() {
        return properties;
    }

    public String operationId() {
        return operationId;
    }
}
