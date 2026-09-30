/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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

import java.lang.foreign.Arena;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.vecruntime.kernels.VectorBuffers;
import io.vecruntime.spark.adapter.SparkColumnVectorBuffers;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.spark.sql.execution.datasources.parquet.ParquetDictionary;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * {@link SparkColumnVectorBuffers#copy} of a dictionary-encoded numeric column
 * as Spark's vectorized Parquet reader leaves it -- ids over a {@link
 * ParquetDictionary} -- decoded into plain lanes (#551). q88's executor
 * profile at 1 TB has this path as its top frame with off-heap reader
 * vectors. Rows per millisecond.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
        jvmArgsAppend = {"--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@State(Scope.Thread)
public class DictionaryDecodeBenchmark {

    static final int N = 4096;

    /**
     * Distinct values in the column chunk's dictionary (a TPC-DS surrogate key
     * spans 7,200 to 86,400).
     */
    static final int DICTIONARY = 7200;

    @Param({"false", "true"})
    boolean onHeap;

    @Param({"int", "long"})
    String type;

    @Param({"0", "0.05"})
    double nullShare;

    WritableColumnVector cv;

    /** A Parquet dictionary decoding id {@code i} to a key-like value. */
    static final class KeyDictionary extends Dictionary {
        KeyDictionary() {
            super(Encoding.PLAIN_DICTIONARY);
        }

        @Override
        public int getMaxId() {
            return DICTIONARY - 1;
        }

        @Override
        public int decodeToInt(int id) {
            return id * 3 + 1;
        }

        @Override
        public long decodeToLong(int id) {
            return id * 3L + 2_450_816L;
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        DataType dt = "int".equals(type) ? DataTypes.IntegerType : DataTypes.LongType;
        cv = onHeap ? new OnHeapColumnVector(N, dt) : new OffHeapColumnVector(N, dt);
        cv.setDictionary(new ParquetDictionary(new KeyDictionary(), false));
        WritableColumnVector ids = cv.reserveDictionaryIds(N);
        Random r = new Random(551);
        for (int i = 0; i < N; i++) {
            if (r.nextDouble() < nullShare) {
                cv.putNull(i);
            } else {
                ids.putInt(i, r.nextInt(DICTIONARY));
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cv.close();
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public long decode() {
        try (Arena a = Arena.ofConfined()) {
            VectorBuffers v = SparkColumnVectorBuffers.copy(cv, N, a);
            // Consume the result so none of the work can be dropped.
            return "int".equals(type) ? v.getInt(N - 1) + v.getInt(N / 2) : v.getLong(N - 1) + v.getLong(N / 2);
        }
    }
}
