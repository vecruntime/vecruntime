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
package io.vecruntime.spark.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.crypto.keytools.KmsClient

/**
 * A test-only KMS for Parquet modular encryption (#812): "wraps" a data key by XOR with a fixed pad derived from
 * the master key id and Base64. No secrecy at all; it only lets a test write and read an encrypted file.
 */
final class TestOnlyKmsClient extends KmsClient {
  override def initialize(conf: Configuration, id: String, url: String, token: String): Unit = ()

  private def pad(master: String, n: Int): Array[Byte] = {
    val seed = master.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    Array.tabulate[Byte](n)(i => (seed(i % seed.length) ^ (i * 31)).toByte)
  }

  override def wrapKey(key: Array[Byte], master: String): String = {
    val p = pad(master, key.length)
    java.util.Base64.getEncoder.encodeToString(Array.tabulate[Byte](key.length)(i => (key(i) ^ p(i)).toByte))
  }

  override def unwrapKey(wrapped: String, master: String): Array[Byte] = {
    val w = java.util.Base64.getDecoder.decode(wrapped)
    val p = pad(master, w.length)
    Array.tabulate[Byte](w.length)(i => (w(i) ^ p(i)).toByte)
  }
}
