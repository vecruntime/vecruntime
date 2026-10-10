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
package io.vecruntime.kernels.parquet;

/**
 * Unpacks eight bit-packed values of a fixed bit width. The kernels module has
 * no parquet-java dependency, but parquet-java ships a generated, fully-unrolled
 * per-width {@code BytePacker} whose {@code unpack8Values} is several times
 * faster than a hand-rolled shift loop (measured in {@code ParquetDecodeBenchmark}:
 * ~3-4x at width 1 and 10). So {@link RleBitPackingReader} takes an unpacker
 * through this seam: the Spark scan node passes a {@code BytePacker}-backed one
 * (parquet-java is on its classpath -- {@code Packer.LITTLE_ENDIAN.newBytePacker(bitWidth)}),
 * and the kernels' own tests and any caller without parquet-java pass {@code null}
 * and get the built-in scalar unpack.
 *
 * <p>Contract: read {@code bitWidth} bytes from {@code src[srcPos ..]} (LSB-first
 * within each byte, values low-to-high -- exactly parquet's LITTLE_ENDIAN packing)
 * and write the eight values to {@code dst[dstPos .. dstPos+8)}.
 */
@FunctionalInterface
public interface GroupUnpacker {
    void unpack8(byte[] src, int srcPos, int[] dst,
                 int dstPos);
}
