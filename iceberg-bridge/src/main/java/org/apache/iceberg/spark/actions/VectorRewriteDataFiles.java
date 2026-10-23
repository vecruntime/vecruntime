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

import java.lang.reflect.Field;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.spark.sql.SparkSession;

/**
 * Entry point for vecruntime's columnar bin-pack {@code rewrite_data_files}
 * (#957 M4c). Iceberg's {@link RewriteDataFilesSparkAction} picks a
 * package-private runner and exposes no public hook to supply a custom
 * {@link org.apache.iceberg.actions.FileRewriteRunner}, so this installs the
 * {@link VectorBinPackFileRewriteRunner} on a freshly built action by setting
 * the action's private {@code runner} field -- the same field Iceberg's own
 * {@code binPack()} / {@code sort()} set. The returned action is otherwise the
 * stock one: the caller sets {@code filter}, options and target size, then
 * {@code execute()} exactly as with {@code SparkActions.get(spark).rewriteDataFiles(table)}.
 *
 * <p>Gated by {@code spark.vecruntime.iceberg.rewrite.binpackCopy.enabled}
 * (default ON). When the flag is off, or the field cannot be set on this
 * Iceberg version, the stock bin-pack runner is left in place, so the action
 * still runs -- just without the byte-copy fast path. The feature is correct
 * either way; the flag only selects the fast path, so A/B is a config change.
 */
public final class VectorRewriteDataFiles {

    public static final String FLAG = "spark.vecruntime.iceberg.rewrite.binpackCopy.enabled";

    // Logged at most once per JVM when the private-field hook is not present as expected, so a repeated
    // rewrite does not spam the log while still correctly running Iceberg's stock bin-pack.
    private static final java.util.concurrent.atomic.AtomicBoolean WARNED = new java.util.concurrent.atomic.AtomicBoolean(false);

    private VectorRewriteDataFiles() {}

    /**
     * A {@code rewrite_data_files} action over {@code table} that bin-packs by
     * copying Parquet row groups byte for byte where valid, falling back to
     * Iceberg's own bin-pack per file. Equivalent to
     * {@code SparkActions.get(spark).rewriteDataFiles(table).binPack()} with the
     * vecruntime runner swapped in.
     */
    public static RewriteDataFiles binPackCopy(SparkSession spark, Table table) {
        RewriteDataFilesSparkAction action = SparkActions.get(spark)
                .rewriteDataFiles(table)
                .binPack();
        if (enabled(spark)) {
            installRunner(spark, table, action);
        }
        return action;
    }

    /** Whether the fast path is enabled (default true). */
    public static boolean enabled(SparkSession spark) {
        return spark.conf()
                    .get(FLAG, "true")
                    .trim()
                    .equalsIgnoreCase("true");
    }

    /**
     * Installs the vecruntime runner on {@code action} by setting its private
     * {@code runner} field, FAIL-CLOSED: if the field is absent, not a {@link
     * org.apache.iceberg.actions.FileRewriteRunner}, or does not currently hold
     * Iceberg's own bin-pack runner (so the Iceberg internals changed shape
     * under us), nothing is replaced -- the action runs Iceberg's stock
     * bin-pack unchanged -- and the reason is logged once per JVM. Correctness
     * never depends on the swap succeeding; only the fast path does.
     */
    private static void installRunner(SparkSession spark, Table table, RewriteDataFilesSparkAction action) {
        try {
            Field runner = RewriteDataFilesSparkAction.class.getDeclaredField("runner");
            if (!org.apache.iceberg.actions.FileRewriteRunner.class.isAssignableFrom(runner.getType())) {
                warnOnce("vecruntime: RewriteDataFilesSparkAction.runner is not a FileRewriteRunner (" + runner.getType().getName() + "); using the stock bin-pack", null);
                return;
            }
            runner.setAccessible(true);
            Object current = runner.get(action);
            // binPack() above must have set Iceberg's own bin-pack runner. If it is null or some other type,
            // the action's shape is not what we expect -- do not overwrite it.
            if (!(current instanceof SparkBinPackFileRewriteRunner)) {
                warnOnce(
                        "vecruntime: RewriteDataFilesSparkAction.runner after binPack() was "
                                + (current == null ? "null" : current.getClass().getName())
                                + ", not the stock bin-pack runner; using the stock bin-pack",
                        null);
                return;
            }
            runner.set(action, new VectorBinPackFileRewriteRunner(spark, table));
        } catch (ReflectiveOperationException | RuntimeException e) {
            // Leave the stock bin-pack runner in place: the action is still correct, only slower.
            warnOnce("vecruntime: could not install the bin-pack copy runner; using the stock bin-pack", e);
        }
    }

    private static void warnOnce(String message, Throwable cause) {
        if (WARNED.compareAndSet(false, true)) {
            if (cause == null) {
                org.slf4j.LoggerFactory.getLogger(VectorRewriteDataFiles.class).warn(message);
            } else {
                org.slf4j.LoggerFactory.getLogger(VectorRewriteDataFiles.class).warn(message, cause);
            }
        }
    }
}
