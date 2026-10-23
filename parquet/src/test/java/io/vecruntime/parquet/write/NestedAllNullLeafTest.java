/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vecruntime.parquet.write;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import io.vecruntime.kernels.ArrowLayout;
import io.vecruntime.kernels.NestedVectorBuffers;
import io.vecruntime.kernels.VectorBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A nested leaf whose every occurrence in a row group is null or absent (the
 * ancestor struct or list is null on every row) is never fed a value, so it is
 * never bound to a lane. Its pages must still be written -- PLAIN, no values --
 * for every physical type, and read back as nulls by parquet-java.
 */
class NestedAllNullLeafTest {

    private static final MessageType SCHEMA = MessageTypeParser
            .parseMessageType("""
            message spark_schema {
              required int32 id;
              optional group s {
                optional int32 i;
                optional int64 l;
                optional double d;
                optional boolean b;
                optional binary str (STRING);
                optional fixed_len_byte_array(16) dec (DECIMAL(38,6));
              }
              optional group xs (LIST) {
                repeated group list {
                  optional int64 element;
                }
              }
            }
            """);

    @TempDir
    Path dir;

    private static MemorySegment allNull(Arena a, int n) {
        MemorySegment v = ArrowLayout.allocateBitmap(a, n);
        v.fill((byte) 0);
        return v;
    }

    private static boolean[] trues(int n) {
        boolean[] b = new boolean[n];
        java.util.Arrays.fill(b, true);
        return b;
    }

    private static List<VectorBuffers> columns(Arena a, int n) {
        int[] ids = new int[n];
        for (int k = 0; k < n; k++) {
            ids[k] = k;
        }
        boolean[] nulls = trues(n);
        VectorBuffers s = NestedVectorBuffers.struct(
                n,
                allNull(a, n),
                List.of(
                        ArrowLayout.ofInts(a, new int[n], nulls),
                        ArrowLayout.ofLongs(a, new long[n], nulls),
                        ArrowLayout.ofDoubles(a, new double[n], nulls),
                        ArrowLayout.ofBooleans(a, new boolean[n], nulls),
                        ArrowLayout.ofStrings(a, new String[n]),
                        ArrowLayout.ofDecimal128(a, new java.math.BigInteger[n], nulls)));
        MemorySegment offsets = ArrowLayout.allocateOffsets(a, n);
        offsets.fill((byte) 0);
        VectorBuffers xs = NestedVectorBuffers.list(n, allNull(a, n), offsets,
                ArrowLayout.ofLongs(a, new long[0], new boolean[0]));
        return List.of(ArrowLayout.ofInts(a, ids, new boolean[n]), s, xs);
    }

    @Test
    void allNullNestedLeavesWriteAndReadBack() throws Exception {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(dir.resolve("all-null.parquet")
                .toString());
        int n = 1000;
        CodecFactory codecs = new CodecFactory(conf, 8 * 1024);
        try (ColumnarParquetWriter w = new ColumnarParquetWriter(
                     HadoopOutputFile.fromPath(p, conf),
                     SCHEMA,
                     ParquetProperties.builder().build(),
                     codecs.getCompressor(CompressionCodecName.SNAPPY),
                     64L << 10,
                     Map.of());
             Arena a = Arena.ofConfined()) {
            w.write(columns(a, n), n);
        } finally {
            codecs.release();
        }
        int read = 0;
        try (ParquetReader<Group> r = ParquetReader.builder(new GroupReadSupport(), p)
                .withConf(conf)
                .build()) {
            for (Group g = r.read();
                 g != null;
                 g = r.read()) {
                assertEquals(read, g.getInteger("id", 0));
                assertEquals(0, g.getFieldRepetitionCount("s"));
                assertEquals(0, g.getFieldRepetitionCount("xs"));
                read++;
            }
        }
        assertEquals(n, read);
    }
}
