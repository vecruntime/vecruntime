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
package io.vecruntime.benchmarks;

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.RangeExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.vecruntime.PlanUtils;
import org.apache.spark.sql.vecruntime.VectorRangeExec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * {@code SELECT sum(id) FROM range(0, rows, 1, slices)} end to end in one local
 * Spark session, three ways: {@code spark} (the plugin off: Spark's generated
 * range loop feeding its generated aggregate), {@code rowRange} (the plugin on
 * but {@code spark.vecruntime.exec.range.enabled=false}: the shape before the
 * columnar leaf -- Spark's row {@code RangeExec} feeding Spark's generated
 * partial aggregate, since a row leaf converts nothing of ours above it, and
 * only the Final aggregate over the shuffle is ours behind a {@code
 * RowToColumnarExec}) and {@code vector} ({@code VectorRangeExec} feeding our
 * partial aggregate directly). One operation is one query; the difference
 * between {@code rowRange} and {@code vector} is what the leaf buys, the
 * difference to {@code spark} is where the whole chain stands against Spark's
 * codegen on a query that is nothing but generating numbers.
 *
 * <p>Needs Spark on the classpath (it is {@code provided} in this module), as
 * {@code run-tpch.sh} assembles it:
 *
 * <pre>
 * CP=benchmarks/target/benchmarks.jar:$(cat benchmarks/target/classpath.txt)
 * java --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
 *   --sun-misc-unsafe-memory-access=allow --add-opens=... (the run-tpch.sh options) \
 *   -cp $CP org.openjdk.jmh.Main RangeQueryBenchmark -wi 2 -i 3 -w 1 -r 1 -f 1
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {
            "--add-modules=jdk.incubator.vector",
            "--enable-native-access=ALL-UNNAMED",
            "-Xmx4g",
            "--sun-misc-unsafe-memory-access=allow",
            "-XX:+IgnoreUnrecognizedVMOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
            "-Dlog4j2.level=warn",
            "-Dspark.log.level=WARN"
        })
@State(Scope.Benchmark)
public class RangeQueryBenchmark {

    @Param({"spark", "rowRange", "vector"})
    String config;

    @Param({"100000000"})
    long rows;

    @Param({"8"})
    int slices;

    SparkSession spark;
    String sql;
    long expected;

    @Setup(Level.Trial)
    public void setup() {
        spark = SparkSession.builder()
                .master("local[" + slices + "]")
                .appName("RangeQueryBenchmark-" + config)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", Integer.toString(slices))
                .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
                .config("spark.vecruntime.enabled", Boolean.toString(!config.equals("spark")))
                .config("spark.vecruntime.exec.range.enabled", Boolean.toString(config.equals("vector")))
                .config("spark.driver.host", "localhost")
                .getOrCreate();
        sql = "SELECT sum(id) FROM range(0, " + rows + ", 1, " + slices + ")";
        // sum(0 .. rows - 1) = rows * (rows - 1) / 2; the even factor is halved first so nothing overflows.
        expected = rows % 2 == 0 ? rows / 2 * (rows - 1) : (rows - 1) / 2 * rows;
        // Under adaptive execution our rule runs per query stage, so the plan is judged after a run,
        // over the final plan's nodes (the tree string would also show the initial, Spark-only plan).
        Dataset<Row> df = spark.sql(sql);
        df.collect();
        SparkPlan plan = df.queryExecution().executedPlan();
        scala.collection.Seq<SparkPlan> nodes = PlanUtils.allNodes(plan);
        boolean ours = nodes.exists(n -> n instanceof VectorRangeExec);
        boolean sparks = nodes.exists(n -> n instanceof RangeExec);
        if (config.equals("vector") != ours || config.equals("vector") == sparks) {
            throw new IllegalStateException("unexpected plan for " + config + ":\n" + plan.treeString());
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        spark.stop();
    }

    @Benchmark
    public long sumOfRange() {
        Row row = spark.sql(sql)
                       .collectAsList()
                       .get(0);
        long sum = row.getLong(0);
        if (sum != expected) {
            throw new IllegalStateException("sum " + sum + " != " + expected);
        }
        return sum;
    }
}
