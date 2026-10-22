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

import io.vecruntime.spark.test.{TestTables, VectorQuerySuite}
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan}
import org.apache.spark.sql.vecruntime.{
  VectorExpandExec,
  VectorExpandPlanner,
  VectorFilterExec,
  VectorHashAggregateExec,
  VectorRollupExec
}

/** ExpandExec: grouping sets and the distinct-aggregate rewrite, row for row against Spark. */
@io.vecruntime.spark.test.tags.Agg
class VectorExpandSuite extends VectorQuerySuite {

  private val Expand = classOf[VectorExpandExec]
  private val Filter = classOf[VectorFilterExec]
  private val Agg = classOf[VectorHashAggregateExec]
  private val Rollup = classOf[VectorRollupExec] // a ROLLUP's Expand and partial aggregate become the chain (#383)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestTables.createMixed(spark, newTempPath("expand/t"))
    TestTables.createLineitem(spark, newTempPath("expand/lineitem"))
  }

  test("ROLLUP, CUBE and GROUPING SETS with grouping() and grouping_id()") {
    // s has nulls (i % 10 = 0) and b is a boolean key: the expand's own nulls must stay
    // distinguishable from the data's, which grouping()/grouping_id() make visible.
    val rollup = checkVectorized(
      "SELECT s, b, count(*) AS c, sum(l) AS sl, grouping(s) AS gs, grouping(b) AS gb, grouping_id() AS gid FROM t GROUP BY ROLLUP(s, b)",
      Seq(Rollup, Agg)
    )
    assert(nodesOf[ExpandExec](rollup).isEmpty, "Spark's ExpandExec should be gone")
    checkVectorized(
      "SELECT s, b, count(*) AS c, min(d) AS md, max(i) AS mi FROM t GROUP BY CUBE(s, b)",
      Seq(Expand, Agg)
    )
    checkVectorized(
      "SELECT s, dt, count(*) AS c, grouping_id() AS gid FROM t WHERE i > 100 GROUP BY GROUPING SETS ((s), (dt), (s, dt), ())",
      Seq(Expand, Filter, Agg)
    )
    checkVectorized("SELECT year(dt) AS y, s, sum(i) AS si FROM t GROUP BY ROLLUP(year(dt), s)", Seq(Rollup, Agg))
    // Grouping sets whose totals include the data's null group as well as the expand's null.
    checkVectorized("SELECT s, count(*) AS c, grouping(s) AS gs FROM t GROUP BY s WITH ROLLUP", Seq(Rollup, Agg))
  }

  test("TPC-H-shaped rollups over lineitem") {
    checkVectorized(
      "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS q, sum(l_extendedprice * (1 - l_discount)) AS rev, count(*) AS c FROM lineitem WHERE l_shipdate < DATE '1996-01-01' GROUP BY ROLLUP(l_returnflag, l_linestatus)",
      Seq(Rollup, Filter, Agg),
      tolerance = 1e-6
    )
    checkVectorized(
      "SELECT l_returnflag, year(l_shipdate) AS y, avg(l_quantity) AS aq, grouping_id() AS gid FROM lineitem GROUP BY CUBE(l_returnflag, year(l_shipdate))",
      Seq(Expand, Agg),
      tolerance = 1e-9
    )
  }

  test("#383: a rollup aggregates the finest grouping once and rolls the partials up") {
    // The partial over the Expand becomes one chain operator: the finest-set partial, then a merge per coarser set.
    def rollups(df: org.apache.spark.sql.DataFrame) = nodesOf[org.apache.spark.sql.vecruntime.VectorRollupExec](df)
    val rollup = checkVectorized(
      "SELECT s, b, count(*) AS c, sum(l) AS sl, avg(d2) AS ad, min(i) AS mi, grouping_id() AS gid FROM t GROUP BY ROLLUP(s, b)",
      Seq(Agg)
    )
    assert(
      rollups(rollup).size === 1 && rollups(rollup).head.levels.size === 3,
      rollup.queryExecution.executedPlan.treeString
    )
    // A FILTER clause is applied by the finest-set partial; the merges carry none.
    checkVectorized(
      "SELECT s, b, count(*) FILTER (WHERE i % 2 = 0) AS ce, sum(l) AS sl FROM t GROUP BY ROLLUP(s, b)",
      Seq(Agg)
    )
    // An expression key is aliased into the project below, so it is still a column reference here.
    checkVectorized(
      "SELECT year(dt) AS y, s, sum(i) AS si, count(*) AS c FROM t GROUP BY ROLLUP(year(dt), s)",
      Seq(Agg)
    )
    // Nested GROUPING SETS are a rollup too; a CUBE is not nested and keeps Spark's plan.
    assert(rollups(checkVectorized(
      "SELECT s, dt, count(*) AS c FROM t GROUP BY GROUPING SETS ((s, dt), (s), ())",
      Seq(Agg)
    )).size === 1)
    assert(rollups(checkVectorized(
      "SELECT s, b, count(*) AS c, sum(l) AS sl FROM t GROUP BY CUBE(s, b)",
      Seq(Expand, Agg)
    )).isEmpty)
    // The distinct rewrite also uses an Expand, but nulls the aggregate inputs per projection: no rewrite, same answer.
    assert(rollups(checkVectorized(
      "SELECT s, count(DISTINCT i) AS di, sum(l) AS sl FROM t GROUP BY s",
      Seq(Agg)
    )).isEmpty)
    // The switch.
    withConf(io.vecruntime.spark.VectorConf.RollupRewriteEnabled -> "false") {
      val off =
        checkVectorized("SELECT s, b, count(*) AS c, sum(l) AS sl FROM t GROUP BY ROLLUP(s, b)", Seq(Expand, Agg))
      assert(rollups(off).isEmpty)
    }
  }

  test("count(distinct) lowers to an expand; the expand and every aggregate stage are ours") {
    // RewriteDistinctAggregates: Expand with null literals per distinct group, then a keys-only
    // aggregate and a filtered one (see VectorAggregateSuite). Nothing of Spark's is left.
    val df = checkVectorized(
      "SELECT s, count(DISTINCT i) AS di, count(DISTINCT l) AS dl, sum(i) AS si FROM t GROUP BY s",
      Seq(Expand)
    )
    assert(nodesOf[ExpandExec](df).isEmpty)
    assert(nodesOf[org.apache.spark.sql.execution.aggregate.HashAggregateExec](df).isEmpty, finalPlan(df).treeString)
  }

  test("a distinct over a struct value next to other distincts: its FILTER reads the struct column") {
    // count.sql's query #2: the distinct groups become FILTER clauses over the expand's columns (an expand of a
    // struct stays Spark's), one of them the struct (i, l). The filtered count read it as a lane through a context
    // that had lost the batch's columns: "unsupported Spark type StructType".
    checkVectorized(
      "SELECT count(DISTINCT 1), count(DISTINCT i), count(DISTINCT (i, l)), count(DISTINCT (i + l)) FROM t",
      Seq(Agg)
    )
    checkVectorized("SELECT s, count(DISTINCT (i, l)) AS dil, count(DISTINCT l) AS dl FROM t GROUP BY s", Seq(Agg))
  }

  test("expand can be disabled") {
    withConf(VectorConf.ExpandEnabled -> "false") {
      val df =
        withPlugin(enabled = true) { val d = spark.sql("SELECT s, count(*) FROM t GROUP BY ROLLUP(s)"); d.collect(); d }
      assert(nodesOf[VectorExpandExec](df).isEmpty)
      assert(nodesOf[ExpandExec](df).nonEmpty)
    }
  }

  test("#901 R2: an expand projection that BUILDS a struct, array and map agrees with Spark's ExpandExec") {
    import org.apache.spark.sql.catalyst.expressions.{
      Attribute,
      AttributeReference,
      CreateArray,
      CreateMap,
      CreateNamedStruct,
      Expression,
      Literal
    }
    import org.apache.spark.sql.execution.ColumnarToRowExec
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.vecruntime.PlanUtils

    // A columnar child: the scan of `t`.
    val df = spark.sql("SELECT i, s FROM t")
    df.collect()
    val child = PlanUtils.allNodes(finalPlan(df)).find(_.supportsColumnar)
      .getOrElse(fail("no columnar scan: " + finalPlan(df).treeString))
    val ci = child.output.find(_.name == "i").get
    val cs = child.output.find(_.name == "s").get

    val output: Seq[Attribute] = Seq(
      AttributeReference("i", IntegerType)(),
      AttributeReference("st", StructType(Seq(StructField("a", IntegerType), StructField("t", StringType))))(),
      AttributeReference("arr", ArrayType(StringType))(),
      AttributeReference("m", MapType(StringType, IntegerType))(),
      AttributeReference("aom", ArrayType(MapType(StringType, IntegerType)))()
    )
    def st(a: Expression, t: Expression) = CreateNamedStruct(Seq(Literal("a"), a, Literal("t"), t))
    // Two projections (a grouping-set-like expand): one builds from the columns, one from literals/nulls.
    val projections: Seq[Seq[Expression]] = Seq(
      Seq(
        ci,
        st(ci, cs),
        CreateArray(Seq(cs, Literal(null, StringType), cs)),
        CreateMap(Seq(
          org.apache.spark.sql.catalyst.expressions.Concat(Seq(
            Literal("k"),
            org.apache.spark.sql.catalyst.expressions.Coalesce(Seq(cs, Literal("z")))
          )),
          ci
        )),
        CreateArray(Seq(CreateMap(Seq(Literal("a"), ci)), CreateMap(Seq(Literal("b"), ci))))
      ),
      Seq(
        ci,
        st(Literal(-1), Literal("u")),
        CreateArray(Seq(Literal("x"), cs)),
        CreateMap(Seq(Literal("z"), Literal(7))),
        CreateArray(Seq(CreateMap(Seq(Literal("c"), Literal(9)))))
      )
    )
    val ours = VectorExpandExec(projections, output, child)
    assert(VectorExpandPlanner.plan(ExpandExec(projections, output, ColumnarToRowExec(child))).isRight)
    val theirs = ExpandExec(projections, output, ColumnarToRowExec(child))

    val schema = StructType(output.map(a => StructField(a.name, a.dataType, a.nullable)))
    def rowsOf(plan: SparkPlan): Seq[String] = {
      val rows = if (plan.supportsColumnar) ColumnarToRowExec(plan).executeCollect() else plan.executeCollect()
      rows.map(ir => ExpandNestedRender.row(ir.copy(), schema)).toSeq.sorted
    }
    val e = rowsOf(theirs)
    val a = rowsOf(ours)
    assert(a.length === e.length, s"row counts: ours ${a.length}, Spark ${e.length}")
    assert(a === e)
  }
}

/** Renders an InternalRow and its nested values to a stable string (the expand test's comparator). */
private object ExpandNestedRender {
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
  import org.apache.spark.sql.types._

  def row(r: InternalRow, schema: StructType): String =
    schema.fields.zipWithIndex.map { case (f, i) =>
      s"${f.name}=" + (if (r.isNullAt(i)) "null" else value(r.get(i, f.dataType), f.dataType))
    }.mkString("{", ", ", "}")

  private def value(v: Any, dt: DataType): String = dt match {
    case s: StructType => row(v.asInstanceOf[InternalRow], s)
    case a: ArrayType => array(v.asInstanceOf[ArrayData], a.elementType)
    case m: MapType =>
      val md = v.asInstanceOf[MapData]
      val ks = md.keyArray(); val vs = md.valueArray()
      (0 until md.numElements()).map { i =>
        value(if (ks.isNullAt(i)) null else ks.get(i, m.keyType), m.keyType) + "->" +
          (if (vs.isNullAt(i)) "null" else value(vs.get(i, m.valueType), m.valueType))
      }.sorted.mkString("map(", ", ", ")")
    case _: StringType => if (v == null) "null" else v.toString
    case _ => String.valueOf(v)
  }

  private def array(ad: ArrayData, et: DataType): String =
    (0 until ad.numElements()).map(i => if (ad.isNullAt(i)) "null" else value(ad.get(i, et), et)).mkString(
      "[",
      ", ",
      "]"
    )
}
