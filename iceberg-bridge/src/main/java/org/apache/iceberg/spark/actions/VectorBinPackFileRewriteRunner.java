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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.vecruntime.iceberg.write.BinPackCopyPlanner;
import io.vecruntime.iceberg.write.BinPackCopyWriter;
import io.vecruntime.iceberg.write.IcebergWriteSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A vecruntime {@code bin-pack} rewrite runner that compacts a file group by
 * copying the input files' Parquet row groups byte for byte into target-sized
 * outputs, instead of decoding and re-encoding every row (#957 M4c). It extends
 * Iceberg's own {@link SparkBinPackFileRewriteRunner} so a file that cannot be
 * copied falls back to exactly Iceberg's bin-pack, and the two output sets are
 * committed together through the same {@code RewriteDataFilesCommitManager} as
 * any other bin-pack.
 *
 * <p>Lives in {@code org.apache.iceberg.spark.actions} because its superclass
 * and the {@code spark()}/{@code table()}/{@code spec(int)} accessors it uses
 * are package-private in Iceberg; {@code RewriteDataFilesSparkAction} exposes no
 * public hook to supply a custom {@link org.apache.iceberg.actions.FileRewriteRunner},
 * so the action installs this runner reflectively (see the driver bridge). It is
 * engaged only when {@code spark.vecruntime.iceberg.rewrite.binpackCopy.enabled}
 * is set; otherwise the stock runner is left in place.
 *
 * <h2>Per-group plan</h2>
 *
 * For each file group {@code rewrite} splits the group's scan tasks by whether
 * each file is copyable ({@link BinPackCopyPlanner}):
 *
 * <ul>
 *   <li><b>copyable files</b> are grouped by partition and bin-packed by raw
 *       byte copy ({@link BinPackCopyWriter}) on the driver -- a byte copy is
 *       I/O only, no Spark job and no decode;
 *   <li><b>non-copyable files</b> (deletes attached, schema evolution, a
 *       different spec, a codec mismatch, v3 unmaterialised lineage, an
 *       unreadable encoding) are handed to {@link SparkBinPackFileRewriteRunner}
 *       as a sub-group, which decodes, applies any deletes, re-clusters and
 *       re-encodes them exactly as Iceberg's own bin-pack does.
 * </ul>
 *
 * The union of the two output sets is returned, so the commit sees one set of
 * added files for the group and the input files are all replaced.
 *
 * <p>If the copy path throws for any reason, the whole group falls back to the
 * stock runner ({@code super.rewrite}); correctness never depends on the copy
 * succeeding.
 */
public class VectorBinPackFileRewriteRunner extends SparkBinPackFileRewriteRunner {

    private static final Logger LOG = LoggerFactory.getLogger(VectorBinPackFileRewriteRunner.class);

    private final SparkSession spark;
    private final Table table;

    public VectorBinPackFileRewriteRunner(SparkSession spark, Table table) {
        super(spark, table);
        this.spark = spark;
        this.table = table;
    }

    @Override
    public String description() {
        return "VECTOR-BIN-PACK-COPY";
    }

    @Override
    public Set<DataFile> rewrite(RewriteFileGroup group) {
        try {
            return rewriteWithCopy(group);
        } catch (RuntimeException e) {
            // Any failure of the copy path is non-fatal: fall back to the stock bin-pack for the whole group,
            // which is always correct. Logged so the fallback is visible.
            LOG.warn("vecruntime bin-pack copy failed for group {}, falling back to the stock bin-pack",
                    group.info(), e);
            return super.rewrite(group);
        }
    }

    private Set<DataFile> rewriteWithCopy(RewriteFileGroup group) {
        PartitionSpec outputSpec = spec(group.outputSpecId());
        // The current table schema is the write schema for a bin-pack rewrite (no projection).
        Schema icebergSchema = table.schema();
        // The vanilla (un-shaded) Parquet form of the write schema, field ids and all, as the engine's writer
        // produces it, so the copyable comparison in BinPackCopyPlanner uses the writer's exact MessageType.
        MessageType parquetWriteSchema = BinPackCopyWriter.writeSchema(icebergSchema);
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        Map<String, String> properties = table.properties();
        Configuration conf = spark.sessionState().newHadoopConf();

        // v3 row lineage: a bin-pack rewrite of a format-v3 table must materialise each row's _row_id and
        // _last_updated_sequence_number (Iceberg's own rewrite assigns them), and a byte copy of a file that
        // does not already store them cannot. Rather than probe each file for materialised lineage, decline the
        // copy for the whole group on v3 and let Iceberg's bin-pack handle it -- correctness over the fast path.
        int formatVersion = ((org.apache.iceberg.HasTableOperations) table).operations()
                .current()
                .formatVersion();
        boolean lineage = formatVersion >= 3;

        // Plan each file. Read the footer once; reuse it for both the decision and the copy.
        List<FileScanTask> copyTasks = new ArrayList<>();
        Map<FileScanTask, ParquetMetadata> footers = new LinkedHashMap<>();
        List<FileScanTask> decodeTasks = new ArrayList<>();
        for (FileScanTask task : group.fileScanTasks()) {
            String location = task.file().location();
            if (lineage) {
                decodeTasks.add(task);
                continue;
            }
            ParquetMetadata footer;
            try {
                footer = BinPackCopyWriter.readFooter(location, conf);
            } catch (RuntimeException e) {
                LOG.debug("vecruntime bin-pack: cannot read footer of {}, decoding it", location, e);
                decodeTasks.add(task);
                continue;
            }
            BinPackCopyPlanner plan = BinPackCopyPlanner.plan(task, footer, parquetWriteSchema, outputSpec, icebergSchema, metricsConfig,
                    properties);
            if (plan.copyable()) {
                copyTasks.add(task);
                footers.put(task, footer);
            } else {
                LOG.debug("vecruntime bin-pack: decoding {} ({})", location, plan.fallbackReason());
                decodeTasks.add(task);
            }
        }

        Set<DataFile> out = new LinkedHashSet<>();

        // Copy path: bin-pack the copyable files by raw byte copy, per partition.
        if (!copyTasks.isEmpty()) {
            out.addAll(copyBinPack(group, outputSpec, icebergSchema, copyTasks, footers, conf));
        }

        // Decode path: hand the rest to the stock bin-pack as a sub-group (if any), so its deletes are applied,
        // its rows re-clustered to the output spec, and its encoding re-established.
        if (!decodeTasks.isEmpty()) {
            RewriteFileGroup subGroup = subGroup(group, decodeTasks);
            out.addAll(super.rewrite(subGroup));
        }

        return out;
    }

    private Set<DataFile> copyBinPack(RewriteFileGroup group, PartitionSpec outputSpec, Schema writeSchema,
            List<FileScanTask> copyTasks, Map<FileScanTask, ParquetMetadata> footers, Configuration conf) {
        IcebergWriteSpec writeSpec = new IcebergWriteSpec(table, writeSchema, outputSpec.specId(), group.maxOutputFileSize(),
                table.properties(), "vecruntime-binpack-" + group.info().globalIndex(), false);

        // One output-file factory for the whole copy of this group; a partition id and a task id just name the
        // files uniquely.
        OutputFileFactory files = OutputFileFactory.builderFor(table, group.info().partitionIndex(),
                        group.info().globalIndex())
                .format(org.apache.iceberg.FileFormat.PARQUET)
                .operationId(writeSpec.operationId())
                .defaultSpec(outputSpec)
                .build();

        // Group copyable files by partition; files in different partitions must not share an output file.
        Map<PartKey, List<FileScanTask>> byPartition = new LinkedHashMap<>();
        for (FileScanTask task : copyTasks) {
            StructLike part = outputSpec.isUnpartitioned() ? null : task.file().partition();
            byPartition.computeIfAbsent(new PartKey(part), k -> new ArrayList<>()).add(task);
        }

        Set<DataFile> out = new LinkedHashSet<>();
        List<BinPackCopyWriter> openWriters = new ArrayList<>();
        try {
            for (Map.Entry<PartKey, List<FileScanTask>> e : byPartition.entrySet()) {
                // The source files' own partition tuple (already computed in the manifest). A partitioned
                // table does not store the partition columns in the file, so the copied file must carry this
                // tuple or the reader reconstructs those columns as null.
                StructLike partition = outputSpec.isUnpartitioned() ? null : e.getKey().struct;
                BinPackCopyWriter writer = new BinPackCopyWriter(writeSpec, files, group.maxOutputFileSize(), partition, conf);
                openWriters.add(writer);
                for (FileScanTask task : e.getValue()) {
                    writer.copyFile(task.file().location(),
                            footers.get(task));
                }
                out.addAll(writer.dataFiles());
            }
            return out;
        } catch (RuntimeException | Error ex) {
            for (BinPackCopyWriter w : openWriters) {
                w.abort(ex);
            }
            throw ex;
        }
    }

    /**
     * A sub-group of {@code group} holding only {@code tasks}, for the stock
     * runner.
     */
    private RewriteFileGroup subGroup(RewriteFileGroup group, List<FileScanTask> tasks) {
        int expected = Math.max(1, (int) Math.round((double) tasks.size()
                / Math.max(1, group.fileScanTasks()
                                   .size())
                * group.expectedOutputFiles()));
        return new RewriteFileGroup(group.info(), tasks, group.outputSpecId(),
                group.maxOutputFileSize(), group.inputSplitSize(), expected);
    }

    /**
     * A partition struct wrapped for use as a map key (value equality over the
     * struct's fields).
     */
    private static final class PartKey {
        final StructLike struct;

        PartKey(StructLike struct) {
            this.struct = struct;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartKey other)) {
                return false;
            }
            if (struct == null || other.struct == null) {
                return java.util.Objects.equals(struct, other.struct);
            }
            if (struct.size() != other.struct.size()) {
                return false;
            }
            for (int i = 0; i < struct.size(); i++) {
                Object a = struct.get(i, Object.class);
                Object b = other.struct.get(i, Object.class);
                if (!java.util.Objects.equals(a, b)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (struct == null) {
                return 0;
            }
            int h = 1;
            for (int i = 0; i < struct.size(); i++) {
                Object a = struct.get(i, Object.class);
                h = 31 * h
                    + (a == null ? 0 : a.hashCode());
            }
            return h;
        }
    }
}
