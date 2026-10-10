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
package io.vecruntime.spark

import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Expression, Substring}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}

class NarrowBelowJoinSuite extends VectorQuerySuite {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    // q23a-like: sales x item on the item key, grouped by a 30-character prefix of a long description.
    (0 until 60000).map(i => (i % 100, i % 3, (i % 7).toLong)).toDF("s_item", "s_date", "s_qty")
      .write.mode("overwrite").parquet(newTempPath("nbj/sales"))
    spark.read.parquet(newTempPath("nbj/sales")).createOrReplaceTempView("nbj_sales")
    (0 until 100).map(i => (i, if (i % 25 == 0) null else s"item number ${i % 40} " + ("x" * (40 + i % 60))))
      .toDF("i_item", "i_desc")
      .write.mode("overwrite").parquet(newTempPath("nbj/item"))
    spark.read.parquet(newTempPath("nbj/item")).createOrReplaceTempView("nbj_item")
  }

  private def run(sql: String): DataFrame = {
    val expected = withPlugin(enabled = false)(spark.sql(sql).collect())
    val df = withPlugin(enabled = true) { val d = spark.sql(sql); d.collect(); d }
    assertRowsEqual(expected, df.collect(), 1e-9, sql)
    assert(expected.nonEmpty, s"the test query should return rows: $sql")
    df
  }

  /** Whether some substring is computed directly above a join (the shape the rule removes). */
  private def substrAboveJoin(p: LogicalPlan): Boolean = p.exists {
    case Project(list, _: Join) => list.exists(_.exists(_.isInstanceOf[Substring]))
    case _ => false
  }

  private def substrBelowJoin(p: LogicalPlan): Boolean = p.exists {
    case j: Join => j.children.exists(_.exists {
        case Project(list, _) => list.exists(_.exists(_.isInstanceOf[Substring]))
        case _ => false
      })
    case _ => false
  }

  private val q23aShape =
    """SELECT substr(i_desc, 1, 30) d, i_item, s_date, count(*) c
      |FROM nbj_sales JOIN nbj_item ON s_item = i_item
      |GROUP BY substr(i_desc, 1, 30), i_item, s_date HAVING count(*) > 4""".stripMargin

  test("q23a's shape: the substring of the item description is computed on the item side, results unchanged") {
    val df = run(q23aShape)
    val p = df.queryExecution.optimizedPlan
    assert(substrBelowJoin(p) && !substrAboveJoin(p), p.treeString)
  }

  test("a length and a substring in a plain projection; the other side's columns stay above") {
    val df = run(
      """SELECT s_qty, length(i_desc) l, substr(i_desc, 6, 6) m
        |FROM nbj_sales JOIN nbj_item ON s_item = i_item WHERE s_date < 3""".stripMargin
    )
    val p = df.queryExecution.optimizedPlan
    assert(substrBelowJoin(p) && !substrAboveJoin(p), p.treeString)
  }

  test("the null-supplying side of an outer join: unmatched rows still get null") {
    val df = run(
      """SELECT s_item, substr(i_desc, 1, 10) d
        |FROM nbj_sales LEFT JOIN (SELECT * FROM nbj_item WHERE i_item < 50) ON s_item = i_item""".stripMargin
    )
    val p = df.queryExecution.optimizedPlan
    assert(substrBelowJoin(p), p.treeString)
  }

  test("declined: an expression of the larger side, or of both sides, or not narrowing") {
    // The larger side's column: nothing to gain, the join already carries it row for row.
    val larger =
      run("SELECT substr(cast(s_qty as string), 1, 1) q, i_item FROM nbj_sales JOIN nbj_item ON s_item = i_item")
    assert(!substrBelowJoin(larger.queryExecution.optimizedPlan), larger.queryExecution.optimizedPlan.treeString)
    // Both sides: cannot be computed on one.
    val both = run(
      "SELECT substr(concat(i_desc, cast(s_qty as string)), 1, 5) q FROM nbj_sales JOIN nbj_item ON s_item = i_item"
    )
    assert(!substrBelowJoin(both.queryExecution.optimizedPlan), both.queryExecution.optimizedPlan.treeString)
    // Not narrowing (upper keeps the width): left above.
    val upper = run("SELECT upper(i_desc) u FROM nbj_sales JOIN nbj_item ON s_item = i_item")
    val pu = upper.queryExecution.optimizedPlan
    assert(
      !pu.exists {
        case j: Join => j.children.exists(_.exists {
            case Project(list, _) =>
              list.exists(_.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.Upper]))
            case _ => false
          })
        case _ => false
      },
      pu.treeString
    )
  }

  test("the switch turns the rewrite off") {
    val df = withConf(VectorConf.NarrowBelowJoinEnabled -> "false")(run(q23aShape))
    assert(substrAboveJoin(df.queryExecution.optimizedPlan), df.queryExecution.optimizedPlan.treeString)
  }
}
