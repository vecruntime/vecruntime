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
package io.vecruntime.iceberg.write

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression for the copy-on-write UPDATE read-back failure Apache Iceberg 1.12's own TestCopyOnWriteUpdate
 * subquery cases surfaced with the vecruntime plugin and writers on (`column ... is not in the store`).
 *
 * The M4b mixed-group copy writer wrote a rewritten row group's chunks "all copied columns first, then all
 * re-encoded columns", so a file whose UPDATE changed a schema-later column (e.g. `id` before `dep`) had its
 * chunks physically OUT of schema order. parquet-java's column-index-filtered read path -- which the engine's
 * own sampling / sort scans use during the next copy-on-write rewrite -- builds a row group's page store
 * assuming schema-ordered chunks and dropped the physically-first column, failing read-back. The writer now
 * emits chunks in schema order, so a copy-path file is byte-order-identical to one written column by column.
 *
 * The tables are tiny (3 rows, a null id), exactly as the Iceberg tests build them; every vecruntime default
 * is left on, and two sequential copy-on-write UPDATEs exercise reading a copy-path file back into the second
 * rewrite's sampling scan.
 */
class CowUpdateSubqueryReadbackSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private lazy val warehouse: Path = Files.createTempDirectory("cowsubq")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("cow-update-subquery")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "io.vecruntime.spark.VectorSparkSessionExtensions")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice.type", "hadoop")
      .config("spark.sql.catalog.ice.warehouse", warehouse.toString)
      .config("spark.sql.catalog.ice.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.resolve("spark").toString)
      // All vecruntime defaults are on; set them explicitly so the suite is self-sufficient run alone.
      .config("spark.vecruntime.scan.nativeIceberg.enabled", "true")
      .config("spark.vecruntime.iceberg.writer.enabled", "true")
      .config("spark.vecruntime.iceberg.copyUnchanged.enabled", "true")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def rows(t: String): Set[String] =
    spark.sql(s"SELECT * FROM $t ORDER BY id ASC NULLS LAST").collect().map(_.toString).toSet

  private def seed(t: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $t")
    spark.sql(
      s"CREATE TABLE $t (id INT, dep STRING) USING iceberg " +
        "TBLPROPERTIES ('write.update.mode'='copy-on-write')"
    )
    spark.sql(s"INSERT INTO $t VALUES (1, 'hr'), (2, 'hardware'), (CAST(NULL AS INT), 'hr')")
  }

  test("UPDATE WHERE id IN (subquery) rewrites and reads back") {
    val t = "ice.db.cow_in_subq"
    seed(t)
    spark.sql(
      "CREATE OR REPLACE TEMP VIEW updated_id AS SELECT * FROM VALUES (0), (1), (CAST(NULL AS INT)) AS v(value)"
    )
    spark.sql("CREATE OR REPLACE TEMP VIEW updated_dep AS SELECT * FROM VALUES ('software'), ('hr') AS v(value)")
    spark.sql(
      s"UPDATE $t SET id = -1 WHERE id IN (SELECT value FROM updated_id) AND dep IN (SELECT value FROM updated_dep)"
    )
    assert(rows(t) == Set("[-1,hr]", "[2,hardware]", "[null,hr]"))
  }

  test("UPDATE WHERE id NOT IN (subquery) rewrites and reads back") {
    val t = "ice.db.cow_notin_subq"
    seed(t)
    spark.sql("CREATE OR REPLACE TEMP VIEW nu_id AS SELECT * FROM VALUES (-1), (-2), (CAST(NULL AS INT)) AS v(value)")
    spark.sql(s"UPDATE $t SET id = -1 WHERE id NOT IN (SELECT value FROM nu_id WHERE value IS NOT NULL)")
    assert(rows(t) == Set("[-1,hr]", "[-1,hardware]", "[null,hr]"))
  }

  test("UPDATE WHERE EXISTS (subquery) rewrites and reads back") {
    val t = "ice.db.cow_exists_subq"
    seed(t)
    spark.sql("CREATE OR REPLACE TEMP VIEW ex_id AS SELECT * FROM VALUES (1), (-2) AS v(value)")
    spark.sql(s"UPDATE $t t SET id = -1 WHERE EXISTS (SELECT 1 FROM ex_id u WHERE t.id = u.value)")
    assert(rows(t) == Set("[-1,hr]", "[2,hardware]", "[null,hr]"))
  }

  test("UPDATE WHERE NOT EXISTS (subquery) rewrites and reads back") {
    val t = "ice.db.cow_notexists_subq"
    seed(t)
    spark.sql("CREATE OR REPLACE TEMP VIEW ne_id AS SELECT * FROM VALUES (1), (-2) AS v(value)")
    spark.sql(s"UPDATE $t t SET id = 5 WHERE NOT EXISTS (SELECT 1 FROM ne_id u WHERE t.id = u.value)")
    // id=1 matches ne_id (EXISTS) so stays; id=2 and id=null do not match, so NOT EXISTS holds and both -> 5.
    assert(rows(t) == Set("[1,hr]", "[5,hardware]", "[5,hr]"))
  }

  test("two sequential UPDATEs (IN then NOT IN) rewrite and read back") {
    val t = "ice.db.cow_in_notin"
    seed(t)
    spark.sql(s"UPDATE $t SET id = -1 WHERE id IN (1, null)")
    // The second UPDATE's sampling scan reads the first UPDATE's copy-path file; a reordered-chunk file used to
    // crash here with "dep ... is not in the store".
    spark.sql(s"UPDATE $t SET id = 100 WHERE id NOT IN (1, 10)")
    assert(rows(t) == Set("[100,hardware]", "[100,hr]", "[null,hr]"))
  }
}
