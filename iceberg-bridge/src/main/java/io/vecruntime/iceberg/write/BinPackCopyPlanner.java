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

import java.util.Locale;
import java.util.Map;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Decides, for one source data file of a bin-pack {@code rewrite_data_files}
 * file group, whether the file's row groups can be copied byte for byte into a
 * compacted output file or whether the file must be decoded and re-encoded
 * (#957 M4c). This is the pure, Spark-free decision the bin-pack runner acts
 * on; it reads only the file's Parquet footer plus the table's write schema,
 * spec and properties, so it is unit-testable without a Spark session.
 *
 * <p>A bin-pack rewrite does not change any row -- it only repacks whole files
 * into target-sized outputs -- so every row group of a copyable file is copied
 * (unlike M4a's copy-on-write DELETE, which copies only the delete-free
 * groups). The output file's schema is the TABLE's current write schema; a
 * copied group's pages are valid under that footer only if the source file
 * already matches it exactly. Copy is declined (the file falls back to the
 * columnar decode/encode path, so its output is byte-for-byte what Iceberg's
 * own bin-pack would produce) when any guard fails:
 *
 * <ul>
 *   <li><b>deletes attached</b> -- a position/equality delete or a DV against
 *       the file removes rows, so a whole-group copy would keep deleted rows.
 *       The file must be decoded with its deletes applied.
 *   <li><b>spec / partition mismatch</b> -- a repartitioning rewrite writes the
 *       rows under a different partition spec than the file was written with,
 *       so the rows must be re-clustered, not copied.
 *   <li><b>schema evolution</b> -- after an {@code ALTER} the file's Parquet
 *       schema differs from the table write schema (a renamed, added, dropped,
 *       reordered or promoted column, a different field id or logical type), so
 *       a copied group's pages would be invalid under the new footer. Compared
 *       by the Parquet text form with repetition normalised, exactly as M4a's
 *       {@link RowGroupCopyPlanner#compatibleForCopy}.
 *   <li><b>v3 unmaterialised row lineage</b> -- on a format-v3 table the write
 *       schema carries {@code _row_id} and {@code
 *       _last_updated_sequence_number}; a file written by an ordinary append
 *       does not materialise them (they are inherited from the data file's
 *       {@code first_row_id} plus row position). A bin-pack rewrite MUST
 *       preserve each row's {@code _row_id} (rewriting is not a logical change),
 *       and a byte copy into a new file with a new {@code first_row_id} would
 *       re-inherit different ids. So an unmaterialised-lineage file is not
 *       copyable on v3 and is decoded, which writes each row's original
 *       {@code _row_id} explicitly. Enforced by the exact-schema guard: a file
 *       that does not carry the lineage columns does not match the v3 write
 *       schema. (On v3 Iceberg already materialises lineage into files written
 *       by the engine, so a second rewrite of an already-rewritten file does
 *       match and is copyable.)
 *   <li><b>codec mismatch</b> -- the table's {@code write.parquet.compression-codec}
 *       differs from the source file's actual page codec, so copying would
 *       leave a file whose pages are not in the table's configured codec. A
 *       rewrite is where Iceberg re-establishes the codec, so decode/re-encode.
 *   <li><b>unreadable encodings</b> -- a column chunk whose Parquet encoding
 *       neither our native scan nor Iceberg's vectorized reader can read must
 *       be rewritten, not carried forward. In practice every encoding a
 *       conforming writer emits is readable, so this guards only exotic files;
 *       it is checked defensively per chunk.
 * </ul>
 */
public final class BinPackCopyPlanner {

    private final boolean copyable;
    private final String fallbackReason;
    private final MessageType fileSchema;

    private BinPackCopyPlanner(boolean copyable, String fallbackReason, MessageType fileSchema) {
        this.copyable = copyable;
        this.fallbackReason = fallbackReason;
        this.fileSchema = fileSchema;
    }

    /**
     * Plans the handling of one source file of a bin-pack file group.
     *
     * @param task the file's scan task (its data file, deletes and spec)
     * @param footer the file's Parquet footer
     * @param writeSchema the table's current Parquet write schema (field ids
     *     and all), i.e. what a copied group must already match
     * @param outputSpec the spec the rewrite writes under
     * @param icebergSchema the table's Iceberg write schema (for NaN tracking)
     * @param metricsConfig the table's metrics modes
     * @param properties the table's properties ({@code write.parquet.*})
     */
    public static BinPackCopyPlanner plan(
            FileScanTask task,
            ParquetMetadata footer,
            MessageType writeSchema,
            PartitionSpec outputSpec,
            Schema icebergSchema,
            MetricsConfig metricsConfig,
            Map<String, String> properties) {
        MessageType fileSchema = footer.getFileMetaData().getSchema();

        if (!task.deletes().isEmpty()) {
            return decline("the file has deletes attached (must be applied, not copied)", fileSchema);
        }
        // A repartitioning rewrite moves rows to a different spec; the file's partition is the task's spec.
        if (task.spec().specId() != outputSpec.specId()) {
            return decline("the file's partition spec differs from the output spec (repartitioning)", fileSchema);
        }
        return footerEligible(footer, writeSchema, icebergSchema, metricsConfig, properties);
    }

    /**
     * The footer-only half of the decision: everything decidable from the
     * file's Parquet footer plus the table write schema, spec-free and
     * delete-free (schema match, codec, NaN tracking, readable encodings).
     * {@link #plan} layers the task-level guards (deletes, spec) on top. Exposed
     * for unit tests, which can build a real footer with no Spark session.
     */
    public static BinPackCopyPlanner footerEligible(ParquetMetadata footer, MessageType writeSchema, Schema icebergSchema,
            MetricsConfig metricsConfig, Map<String, String> properties) {
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        if (!RowGroupCopyPlanner.compatibleForCopy(writeSchema, fileSchema)) {
            return decline("the file schema differs from the table write schema (schema evolution / v3 lineage)", fileSchema);
        }
        // A copy carries the source file's page codec; a rewrite must land the table's configured codec.
        String codecMismatch = codecMismatch(footer, properties);
        if (codecMismatch != null) {
            return decline(codecMismatch, fileSchema);
        }
        // Tracking a float/double NaN count: a copied group's footer carries none, so a copy would report a
        // wrong nan_value_count. A bin-pack copy copies every group (no re-encoded groups to supply it from),
        // so a table that tracks any NaN count declines the copy for the file.
        if (RowGroupCopyPlanner.tracksNaN(icebergSchema, metricsConfig)) {
            return decline("the table tracks NaN counts for a float/double column and a copied group has none", fileSchema);
        }
        String enc = unreadableEncoding(footer);
        if (enc != null) {
            return decline(enc, fileSchema);
        }
        return new BinPackCopyPlanner(true, null, fileSchema);
    }

    private static BinPackCopyPlanner decline(String reason, MessageType fileSchema) {
        return new BinPackCopyPlanner(false, reason, fileSchema);
    }

    /**
     * Whether the source file's page codec differs from the table's configured
     * write codec. Every column chunk of every group must already be in the
     * table codec; a mixed or stale codec file falls back to the re-encode
     * path, which lands the configured codec.
     */
    private static String codecMismatch(ParquetMetadata footer, Map<String, String> properties) {
        String codecName = properties.getOrDefault(TableProperties.PARQUET_COMPRESSION, TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
        CompressionCodecName want = CompressionCodecName.fromConf(codecName.toUpperCase(Locale.ROOT));
        for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
            for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk : block.getColumns()) {
                if (chunk.getCodec() != want) {
                    return "the file's page codec " + chunk.getCodec() + " differs from the table codec " + want;
                }
            }
        }
        return null;
    }

    /**
     * Whether any column chunk uses an encoding our reader and Iceberg's
     * vectorized reader cannot read. Conforming Parquet encodings (PLAIN,
     * dictionary, RLE/bit-packed, DELTA_* and byte-stream split) are all
     * readable; this guards only files with an encoding outside that set.
     */
    private static String unreadableEncoding(ParquetMetadata footer) {
        for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
            for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk : block.getColumns()) {
                for (org.apache.parquet.column.Encoding e : chunk.getEncodings()) {
                    switch (e) {
                        case PLAIN:
                        case PLAIN_DICTIONARY:
                        case RLE:
                        case RLE_DICTIONARY:
                        case BIT_PACKED:
                        case DELTA_BINARY_PACKED:
                        case DELTA_LENGTH_BYTE_ARRAY:
                        case DELTA_BYTE_ARRAY:
                        case BYTE_STREAM_SPLIT:
                            break;
                        default:
                            return "the file uses an unreadable Parquet encoding " + e;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Whether the file can be copied byte for byte ({@code false} -&gt;
     * decode/re-encode).
     */
    public boolean copyable() {
        return copyable;
    }

    /** Why the file is not copyable, or {@code null} when it is. */
    public String fallbackReason() {
        return fallbackReason;
    }

    /** The source file's Parquet schema. */
    public MessageType fileSchema() {
        return fileSchema;
    }
}
