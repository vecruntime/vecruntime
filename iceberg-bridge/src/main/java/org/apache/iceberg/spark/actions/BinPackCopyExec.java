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
package org.apache.iceberg.spark.actions;

import java.util.List;

import io.vecruntime.iceberg.write.BinPackCopyBin;
import io.vecruntime.iceberg.write.BinPackCopyWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Runs a distributed bin-pack copy (#957 M4c): one Spark task per output bin,
 * so the byte copy runs on the executors -- opening source files through the
 * table's {@link org.apache.iceberg.io.FileIO}, appending their row groups into
 * one output file, and returning the resulting {@link DataFile} to the driver,
 * which commits it through the same {@code RewriteFileGroup} result as a decode
 * rewrite. This replaces the earlier driver-streamed copy, which could not
 * scale to TB-size groups.
 *
 * <p>The table (a serializable copy), the output spec id, the write schema, the
 * table properties and the Hadoop configuration are broadcast; each task builds
 * its own {@link OutputFileFactory} from {@link TaskContext} ids so output file
 * names are unique. A task-side {@link LongAccumulator} counts the row groups
 * copied, so a test can prove the copy actually ran in tasks (and across more
 * than one task for a multi-bin group).
 */
final class BinPackCopyExec {

    private BinPackCopyExec() {}

    /**
     * Copies {@code bins} in parallel, one task per bin, and returns the output
     * data files. {@code copiedGroups} is incremented once per copied row group
     * on the executor.
     */
    static List<DataFile> run(
            JavaSparkContext jsc,
            Table serializableTable,
            int outputSpecId,
            Schema writeSchema,
            Configuration conf,
            List<BinPackCopyBin> bins,
            long taskIdBase,
            LongAccumulator copiedGroups) {
        Broadcast<Table> tableB = jsc.broadcast(serializableTable);
        Broadcast<Schema> schemaB = jsc.broadcast(writeSchema);
        Broadcast<SerializableConfiguration> confB = jsc.broadcast(new SerializableConfiguration(conf));

        // One partition per bin, so each bin is one task.
        List<DataFile> out = jsc.parallelize(bins, bins.size())
                                .map(
                                        bin -> copyOneBin(
                                                bin,
                                                tableB.value(),
                                                outputSpecId,
                                                schemaB.value(),
                                                confB.value().value(),
                                                taskIdBase,
                                                copiedGroups))
                                .collect();
        return out;
    }

    private static DataFile copyOneBin(
            BinPackCopyBin bin,
            Table table,
            int outputSpecId,
            Schema writeSchema,
            Configuration conf,
            long taskIdBase,
            LongAccumulator copiedGroups) {
        PartitionSpec outputSpec = table.specs().get(outputSpecId);
        TaskContext ctx = TaskContext.get();
        int partitionId = ctx == null ? 0 : ctx.partitionId();
        // A task id unique across the job: base + this task's attempt id keeps output names distinct.
        long taskId = taskIdBase + (ctx == null ? 0L : ctx.taskAttemptId());
        OutputFileFactory files = OutputFileFactory.builderFor(table, partitionId, taskId)
                .format(FileFormat.PARQUET)
                .defaultSpec(outputSpec)
                .build();
        BinPackCopyWriter writer = new BinPackCopyWriter(table, outputSpec, writeSchema, files, bin.partition(),
                conf, n -> copiedGroups.add(n));
        try {
            for (BinPackCopyBin.Entry e : bin.entries()) {
                writer.copyGroups(e.location(), e.rowGroups());
            }
            return writer.finish();
        } catch (RuntimeException | Error ex) {
            writer.abort(ex);
            throw ex;
        }
    }
}
