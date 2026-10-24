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
import java.util.concurrent.atomic.LongAdder;

import io.vecruntime.iceberg.write.BinPackCopyBin;
import io.vecruntime.iceberg.write.BinPackCopyPlanner;
import io.vecruntime.iceberg.write.BinPackCopyWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
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

    /**
     * Row groups copied byte for byte across all bin-pack copies (test/metric
     * counter).
     */
    public static final LongAdder COPIED_ROW_GROUPS = new LongAdder();

    /**
     * Output files produced by copy across all bin-pack copies (test/metric
     * counter).
     */
    public static final LongAdder COPIED_OUTPUT_FILES = new LongAdder();

    /**
     * Spark tasks launched for the distributed copy across all bin-pack copies
     * (test/metric counter).
     */
    public static final LongAdder TASKS = new LongAdder();

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
                footer = BinPackCopyWriter.readFooter(table.io(), location);
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

        // A byte copy moves WHOLE row groups: it can COMBINE small files into a target-sized output, but it
        // cannot SPLIT a row group that is already bigger than the target split size. Iceberg's stock bin-pack
        // decodes and re-encodes, so it splits a large file into several target-sized outputs and may combine
        // one of those pieces with other small files in the group (TestRewriteDataFilesAction
        // #testBinPackCombineMixedFiles). When any copyable file holds a row group larger than the group's
        // input split size, our copy cannot reproduce that split-and-combine layout -- it would emit the
        // oversized row group whole, a different file count for identical data -- so fall the WHOLE group back
        // to the stock bin-pack, which produces exactly Iceberg's layout. The copy fast path then applies only
        // to groups whose files merely COMBINE up to the target (the common small-file compaction case), which
        // is where a byte copy is both valid and worthwhile. Correctness never depends on the copy running.
        long splitSize = group.inputSplitSize();
        boolean needsSplit = copyTasks.stream().anyMatch(
                task -> footers.get(task).getBlocks().stream()
                        .anyMatch(b -> b.getCompressedSize() > splitSize));
        if (needsSplit) {
            LOG.debug("vecruntime bin-pack: group {} has a row group larger than the split size {}; decoding the whole group",
                    group.info(), splitSize);
            return super.rewrite(group);
        }

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
        // The size each output bin is filled to. Iceberg's stock bin-pack reads the group with SPLIT_SIZE =
        // group.inputSplitSize() (the planner's input split size, ~ inputSize / expectedOutputFiles, floored at
        // the target file size) and writes each split as one output file, using maxOutputFileSize() only as the
        // write-side roll CAP. So the output file count is driven by inputSplitSize(), NOT maxOutputFileSize().
        // We must fill our copy bins to the same inputSplitSize(), or a mixed-size group (e.g. one large file
        // plus small ones) packs into fewer, larger bins than Iceberg produces -- a different file layout for
        // identical data. Packing to maxOutputFileSize() (1.8x target by default) collapsed the whole group
        // into one bin where Iceberg split it into expectedOutputFiles files.
        long target = group.inputSplitSize();

        // Group copyable files by partition; files in different partitions must not share an output file.
        Map<PartKey, List<FileScanTask>> byPartition = new LinkedHashMap<>();
        for (FileScanTask task : copyTasks) {
            StructLike part = outputSpec.isUnpartitioned() ? null : task.file().partition();
            byPartition.computeIfAbsent(new PartKey(part), k -> new ArrayList<>()).add(task);
        }

        // Plan bins at ROW-GROUP granularity: within one partition, fill a bin to the target size with whole
        // row groups (the copy unit), so a large file is split across bins and every output lands near target.
        List<BinPackCopyBin> bins = new ArrayList<>();
        for (Map.Entry<PartKey, List<FileScanTask>> e : byPartition.entrySet()) {
            StructLike partition = outputSpec.isUnpartitioned() ? null : copyPartition(outputSpec, e.getKey().struct);
            List<BinPackCopyBin.Entry> binEntries = new ArrayList<>();
            List<Integer> currentOrdinals = new ArrayList<>();
            String currentFile = null;
            long currentBytes = 0;
            for (FileScanTask task : e.getValue()) {
                String location = task.file().location();
                List<org.apache.parquet.hadoop.metadata.BlockMetaData> blocks = footers.get(task).getBlocks();
                for (int g = 0; g < blocks.size(); g++) {
                    if (currentFile != null && !currentFile.equals(location)) {
                        binEntries.add(new BinPackCopyBin.Entry(currentFile, toIntArray(currentOrdinals)));
                        currentOrdinals = new ArrayList<>();
                    }
                    currentFile = location;
                    currentOrdinals.add(g);
                    currentBytes += blocks.get(g).getCompressedSize();
                    if (currentBytes >= target) {
                        binEntries.add(new BinPackCopyBin.Entry(currentFile, toIntArray(currentOrdinals)));
                        bins.add(new BinPackCopyBin(binEntries, partition));
                        binEntries = new ArrayList<>();
                        currentOrdinals = new ArrayList<>();
                        currentFile = null;
                        currentBytes = 0;
                    }
                }
                // Flush the open file entry at a file boundary so a bin's entries each name one file.
                if (currentFile != null && !currentOrdinals.isEmpty()) {
                    binEntries.add(new BinPackCopyBin.Entry(currentFile, toIntArray(currentOrdinals)));
                    currentOrdinals = new ArrayList<>();
                    currentFile = null;
                }
            }
            if (!binEntries.isEmpty()) {
                bins.add(new BinPackCopyBin(binEntries, partition));
            }
        }

        if (bins.isEmpty()) {
            return new LinkedHashSet<>();
        }

        // Distribute: one Spark task per bin. A serializable table copy carries the FileIO to the executors;
        // a task-side accumulator counts copied row groups so a test can prove the copy ran in tasks.
        Table serializableTable = org.apache.iceberg.SerializableTable.copyOf(table);
        org.apache.spark.api.java.JavaSparkContext jsc = org.apache.spark.api.java.JavaSparkContext.fromSparkContext(spark.sparkContext());
        org.apache.spark.util.LongAccumulator copiedGroups = jsc.sc().longAccumulator("vecruntime-binpack-copied-groups");
        long taskIdBase = (long) group.info().globalIndex() << 20;
        List<DataFile> copied = BinPackCopyExec.run(jsc, serializableTable, outputSpec.specId(), writeSchema, conf,
                bins, taskIdBase, copiedGroups);
        COPIED_ROW_GROUPS.add(copiedGroups.value());
        COPIED_OUTPUT_FILES.add(bins.size());
        TASKS.add(bins.size());
        return new LinkedHashSet<>(copied);
    }

    private static int[] toIntArray(List<Integer> xs) {
        int[] a = new int[xs.size()];
        for (int i = 0; i < a.length; i++) {
            a[i] = xs.get(i);
        }
        return a;
    }

    /**
     * A serializable copy of a source file's partition tuple into {@code
     * outputSpec}'s partition type (Iceberg {@code PartitionData}), so the bin
     * ships to the executor and the output file keeps the source partition.
     */
    private static StructLike copyPartition(PartitionSpec outputSpec, StructLike source) {
        org.apache.iceberg.PartitionData data = new org.apache.iceberg.PartitionData(outputSpec.partitionType());
        for (int i = 0; i < data.size(); i++) {
            data.set(i, source.get(i, Object.class));
        }
        return data;
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
