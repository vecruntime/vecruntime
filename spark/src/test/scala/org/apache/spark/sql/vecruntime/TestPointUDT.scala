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
package org.apache.spark.sql.vecruntime

import org.apache.spark.sql.types.{DataType, IntegerType, SQLUserDefinedType, UserDefinedType}

/**
 * A test-only user-defined type over an int (#812): a column type the native Parquet scan has no decode path
 * for, now that every Parquet-storable built-in type has one, so plan tests can still check the fallback.
 */
@SQLUserDefinedType(udt = classOf[TestPointUDT])
final case class TestPoint(x: Int)

final class TestPointUDT extends UserDefinedType[TestPoint] {
  override def sqlType: DataType = IntegerType
  override def serialize(p: TestPoint): Any = p.x
  override def deserialize(datum: Any): TestPoint = TestPoint(datum.asInstanceOf[Int])
  override def userClass: Class[TestPoint] = classOf[TestPoint]
}
