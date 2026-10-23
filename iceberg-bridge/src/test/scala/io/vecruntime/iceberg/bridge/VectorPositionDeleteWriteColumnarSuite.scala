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
package io.vecruntime.iceberg.bridge

/**
 * #901 M1 + M2: every case of [[VectorPositionDeleteWriteSuite]] with the whole v2 delta write columnar -- the
 * position delete files and the INSERT / REINSERT data files alike.
 */
class VectorPositionDeleteWriteColumnarSuite extends VectorPositionDeleteWriteSuite {
  override protected def columnarData: Boolean = true
}
