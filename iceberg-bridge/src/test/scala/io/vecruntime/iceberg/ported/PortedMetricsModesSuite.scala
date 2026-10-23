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
package io.vecruntime.iceberg.ported

/*
 * Ported from Apache Iceberg 1.12.0 (Apache License 2.0):
 *   core/src/test/java/org/apache/iceberg/TestMetricsModes.java
 * These exercise MetricsConfig / MetricsModes -- the metrics-mode string parsing, the sorted-column
 * defaulting, and the `write.metadata.metrics.max-inferred-column-defaults` limit (including nested
 * structs and variant). Our columnar Parquet writer reads exactly this MetricsConfig to decide which
 * per-column statistics it emits (PortedWriteMetricsConfigSuite proves the end-to-end effect), so this
 * suite pins the configuration contract the writer depends on. The original test names are preserved.
 * Upstream's parameterized formatVersion grid and the test-only TestTables harness are replaced with a
 * public HadoopTables-backed table at format version 2 (and 3 for the variant case); the assertions are
 * otherwise identical to upstream.
 */

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.iceberg.{MetricsConfig, MetricsModes, PartitionSpec, Schema, SortOrder, Table, TableProperties}
import org.apache.iceberg.MetricsModes.{Counts, Full, None => MMNone, Truncate}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField.required
import org.scalatest.funsuite.AnyFunSuite

class PortedMetricsModesSuite extends AnyFunSuite {

  private val tables = new HadoopTables(new org.apache.hadoop.conf.Configuration())

  private def newTable(schema: Schema, sortOrder: SortOrder): Table = {
    val loc = Files.createTempDirectory("ported-metricsmodes").resolve("t").toString
    tables.create(
      schema,
      PartitionSpec.unpartitioned(),
      sortOrder,
      java.util.Map.of[String, String](),
      loc
    )
  }

  test("testMetricsModeParsing") {
    assert(MetricsModes.fromString("none") == MMNone.get())
    assert(MetricsModes.fromString("nOnE") == MMNone.get())
    assert(MetricsModes.fromString("counts") == Counts.get())
    assert(MetricsModes.fromString("coUntS") == Counts.get())
    assert(MetricsModes.fromString("truncate(1)") == Truncate.withLength(1))
    assert(MetricsModes.fromString("truNcAte(10)") == Truncate.withLength(10))
    assert(MetricsModes.fromString("full") == Full.get())
    assert(MetricsModes.fromString("FULL") == Full.get())
  }

  test("testInvalidTruncationLength") {
    val e = intercept[IllegalArgumentException](MetricsModes.fromString("truncate(0)"))
    assert(e.getMessage == "Truncate length should be positive")
  }

  test("testInvalidColumnModeValue") {
    val props = Map(
      TableProperties.DEFAULT_WRITE_METRICS_MODE -> "full",
      TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col" -> "troncate(5)"
    ).asJava
    val config = MetricsConfig.from(props, null, null)
    assert(config.columnMode("col") == Full.get(), "invalid mode should default to the table default (full)")
  }

  test("testInvalidDefaultColumnModeValue") {
    val props = Map(
      TableProperties.DEFAULT_WRITE_METRICS_MODE -> "fuull",
      TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col" -> "troncate(5)"
    ).asJava
    val config = MetricsConfig.from(props, null, null)
    assert(
      config.columnMode("col") == Truncate.withLength(16),
      "invalid mode should default to the library default (truncate(16))"
    )
  }

  test("testMetricsConfigSortedColsDefault") {
    val schema = new Schema(
      required(1, "col1", Types.IntegerType.get()),
      required(2, "col2", Types.IntegerType.get()),
      required(3, "col3", Types.IntegerType.get()),
      required(4, "col4", Types.IntegerType.get())
    )
    val sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build()
    val table = newTable(schema, sortOrder)
    table
      .updateProperties()
      .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts")
      .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1", "counts")
      .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2", "none")
      .commit()
    val config = MetricsConfig.forTable(table)
    assert(config.columnMode("col1") == Counts.get(), "non-sorted existing column should not be overridden")
    assert(config.columnMode("col2") == MMNone.get(), "sorted defaults should not override user config")
    assert(config.columnMode("col3") == Truncate.withLength(16), "unspecified sorted column uses the default")
    assert(config.columnMode("col4") == Counts.get(), "unspecified normal column uses the default")
  }

  test("testMetricsConfigSortedColsDefaultByInvalid") {
    val schema = new Schema(
      required(1, "col1", Types.IntegerType.get()),
      required(2, "col2", Types.IntegerType.get()),
      required(3, "col3", Types.IntegerType.get())
    )
    val sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build()
    val table = newTable(schema, sortOrder)
    table
      .updateProperties()
      .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts")
      .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1", "full")
      .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2", "invalid")
      .commit()
    val config = MetricsConfig.forTable(table)
    assert(config.columnMode("col1") == Full.get(), "non-sorted existing column not overridden by a sorted one")
    assert(config.columnMode("col2") == Counts.get(), "original default applies for an invalid sorted-column mode")
  }

  test("testMetricsConfigInferredDefaultModeLimit") {
    val schema = new Schema(
      required(1, "col1", Types.IntegerType.get()),
      required(2, "col2", Types.IntegerType.get()),
      required(3, "col3", Types.IntegerType.get())
    )
    val table = newTable(schema, SortOrder.unsorted())
    table.updateProperties().set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "2").commit()
    val config = MetricsConfig.forTable(table)
    assert(config.columnMode("col1") == Truncate.withLength(16))
    assert(config.columnMode("col2") == Truncate.withLength(16))
    assert(config.columnMode("col3") == MMNone.get())
  }

  test("testMetricsConfigNestedTypesStructs") {
    val schema = new Schema(
      required(
        5,
        "col_struct",
        Types.StructType.of(
          required(33, "a", Types.IntegerType.get()),
          required(1, "b", Types.IntegerType.get())
        )
      ),
      required(4, "top", Types.IntegerType.get())
    )
    val table = newTable(schema, SortOrder.unsorted())
    table.updateProperties().set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "2").commit()
    val config = MetricsConfig.forTable(table)
    val modes = schema.idToName().values().asScala.map(n => n -> config.columnMode(n)).toMap
    assert(modes.keySet == Set("col_struct.a", "col_struct", "col_struct.b", "top"))
    assert(modes("col_struct.a") == Truncate.withLength(16))
    assert(modes("col_struct.b") == MMNone.get())
    assert(modes("top") == Truncate.withLength(16))
  }

  test("testMetricsVariantSupported") {
    val schema = new Schema(
      required(1, "variant", Types.VariantType.get()),
      required(2, "int", Types.IntegerType.get())
    )
    val loc = Files.createTempDirectory("ported-metricsmodes-v3").resolve("t").toString
    val table = tables.create(
      schema,
      PartitionSpec.unpartitioned(),
      SortOrder.unsorted(),
      java.util.Map.of(TableProperties.FORMAT_VERSION, "3"),
      loc
    )
    table.updateProperties().set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "1").commit()
    val config = MetricsConfig.forTable(table)
    val modes = schema.idToName().values().asScala.map(n => n -> config.columnMode(n)).toMap
    assert(modes == Map("variant" -> Truncate.withLength(16), "int" -> MMNone.get()))
  }
}
