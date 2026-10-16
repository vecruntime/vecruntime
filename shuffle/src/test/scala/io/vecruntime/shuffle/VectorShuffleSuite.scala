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
package io.vecruntime.shuffle

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.vecruntime.VectorShuffleExchangeExec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * #288 slice 2: the columnar exchange under our shuffle manager, `local[4]`. Hash, round-robin,
 * single and range partitionings go through `VectorShuffleExchangeExec`; every result equals the
 * same query without the plugin; AQE coalesces over our exchange; no row conversion sits between the
 * exchange and the columnar operators above it.
 */
class VectorShuffleSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var plain: SparkSession = _
  private var tempDir: Path = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("vecruntime-shuffle")
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("VectorShuffleSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.sql.warehouse.dir", tempDir.resolve("wh").toString)
      .config("spark.plugins", "io.vecruntime.spark.VectorPlugin")
      .config("spark.shuffle.manager", "org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager")
      .config("spark.vecruntime.shuffle.enabled", "true")
      .config("spark.vecruntime.exec.strictFloatingPoint", "true")
      .getOrCreate()
    val data = spark.range(0, 20000).selectExpr(
      "id",
      "cast(id % 97 as int) as k",
      "cast(id % 1000 as string) as s",
      "cast(id % 7 as double) / 3 as x",
      "date_add(date '2020-01-01', cast(id % 400 as int)) as d",
      "cast(id % 13 as decimal(12,2)) / 7 as dec",
      "id % 3 = 0 as b"
    )
    data.write.mode("overwrite").parquet(tempDir.resolve("t").toString)
    spark.read.parquet(tempDir.resolve("t").toString).createOrReplaceTempView("t")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def collectPlan(df: DataFrame): SparkPlan = {
    df.collect()
    df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case p => p
    }
  }

  /** Every node, looking through the query stages (leaves in AQE's final plan) into their plans. */
  private def nodes(plan: SparkPlan): Seq[SparkPlan] = plan +: (plan match {
    case q: QueryStageExec => nodes(q.plan)
    case p => p.children.flatMap(nodes)
  })

  private def exchanges(plan: SparkPlan): Seq[SparkPlan] = nodes(plan).collect {
    case e: VectorShuffleExchangeExec => e
    case e: ShuffleExchangeExec => e
  }

  /** Rows of `sql` with the plugin's shuffle against the same query with the plugin off, order-insensitive. */
  private def checkAgainstSpark(sql: String): Unit = {
    val ours = spark.sql(sql).collect().toSeq.sortBy(_.toString)
    val theirs = spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "false")
    try {
      val expected = spark.sql(sql).collect().toSeq.sortBy(_.toString)
      assert(ours === expected, sql)
    } finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
  }

  private def assertOurExchange(df: DataFrame, expectedCount: Int = 1): SparkPlan = {
    val plan = collectPlan(df)
    val ex = exchanges(plan)
    assert(ex.nonEmpty, s"no exchange in\n$plan")
    assert(ex.forall(_.isInstanceOf[VectorShuffleExchangeExec]), s"Spark's exchange survived in\n$plan")
    assert(ex.size === expectedCount, s"expected $expectedCount exchanges in\n$plan")
    plan
  }

  test("hash partitioning: a grouped aggregate's final over our exchange, without row conversions") {
    val sql = "select k, count(*) c, sum(x) sx, max(s) ms from t group by k"
    val plan = assertOurExchange(spark.sql(sql))
    assert(nodes(plan).collect { case r: RowToColumnarExec => r }.isEmpty, s"RowToColumnar in\n$plan")
    checkAgainstSpark(sql)
  }

  test("hash partitioning on several keys of every lane type") {
    val sql = "select k, s, d, dec, b, count(*) c from t group by k, s, d, dec, b"
    assertOurExchange(spark.sql(sql))
    checkAgainstSpark(sql)
  }

  /**
   * Struct columns (Iceberg's `_partition` on a MERGE) cross our exchange flattened into lanes and come
   * back as structs: nested structs, nulls at the struct and at the field level, and
   * a struct as a hash key -- every row lands in the partition Spark's exchange puts it in.
   */
  test("struct columns and struct hash keys cross our exchange as lanes") {
    val path = tempDir.resolve("structs").toString
    spark.range(0, 5000).selectExpr(
      "cast(id as int) as k",
      "case when id % 17 = 0 then null else named_struct(" +
        "'a', cast(id % 11 as int), " +
        "'b', case when id % 5 = 0 then null else cast(id % 23 as string) end, " +
        "'c', named_struct('x', cast(id % 3 as bigint), 'y', cast(id % 7 as decimal(9,2)))) end as st"
    ).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("st")
    val df = spark.table("st").repartition(
      5,
      org.apache.spark.sql.functions.col("st"),
      org.apache.spark.sql.functions.col("k")
    )
    val plan = collectPlan(df)
    assert(exchanges(plan).exists(_.isInstanceOf[VectorShuffleExchangeExec]), s"expected our exchange in\n$plan")
    assert(exchanges(plan).forall(_.isInstanceOf[VectorShuffleExchangeExec]), s"Spark's exchange survived in\n$plan")
    def rows(enabled: Boolean): Seq[String] = {
      spark.sessionState.conf.setConfString("spark.vecruntime.enabled", enabled.toString)
      try
        spark.table("st").repartition(
          5,
          org.apache.spark.sql.functions.col("st"),
          org.apache.spark.sql.functions.col("k")
        )
          .selectExpr("spark_partition_id() as p", "k", "st").collect().map(_.toString).toSeq.sorted
      finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
    }
    assert(rows(enabled = true) === rows(enabled = false))
  }

  /**
   * Arrays, maps and structs holding them cross our exchange as nested lanes (#705), as payload: on
   * the per-partition path (7 partitions) and the staged one (300), every row reaches the partition
   * Spark's exchange puts it in with Spark's nested value -- null values, null elements, empty arrays
   * and maps, arrays of arrays, arrays of structs, maps of arrays, arrays of binary.
   */
  /** A value as text, binary by content (an `Array[Byte]` prints its identity), through arrays, maps and structs. */
  private def show(v: Any): String = v match {
    case null => "null"
    case b: Array[Byte] => b.map("%02x".format(_)).mkString("0x", "", "")
    case r: org.apache.spark.sql.Row => r.toSeq.map(show).mkString("[", ",", "]")
    case m: scala.collection.Map[_, _] => m.map { case (k, x) => show(k) + "->" + show(x) }.mkString("{", ",", "}")
    case s: scala.collection.Seq[_] => s.map(show).mkString("(", ",", ")")
    case x => x.toString
  }

  /**
   * Nested hash and range keys (#705): an array, an array of arrays, an array of strings and a struct
   * holding an array hash as Spark's Murmur3Hash does (NestedKeyHash) -- every row in Spark's partition,
   * alone and with a flat key -- and a global order by an array runs through our exchange in Spark's order.
   */
  test("array and struct-of-array hash keys and array range keys cross our exchange") {
    val path = tempDir.resolve("nested-keys").toString
    spark.range(0, 4000).selectExpr(
      "cast(id as int) as k",
      "case when id % 9 = 0 then null when id % 7 = 0 then array() " +
        "else array(cast(id % 13 as int), null, cast(id % 5 as int)) end as ai",
      "case when id % 11 = 0 then null else array(array(id % 3, null), array()) end as aa",
      "case when id % 6 = 0 then null else array(concat('s', id % 17), null, '') end as sa",
      "named_struct('x', cast(id % 4 as double), 'y', array(cast(id % 3 as decimal(9,2)), null), " +
        "'z', cast(id % 2 = 0 as boolean)) as st"
    ).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("nk")
    def rows(df: => DataFrame, enabled: Boolean): Seq[String] = {
      spark.sessionState.conf.setConfString("spark.vecruntime.enabled", enabled.toString)
      try df.selectExpr("spark_partition_id() as p", "*").collect().map(r => show(r)).toSeq.sorted
      finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
    }
    Seq(Seq("ai"), Seq("aa"), Seq("sa"), Seq("st"), Seq("ai", "k"), Seq("k", "st", "sa")).foreach { keys =>
      def df = spark.table("nk").repartition(7, keys.map(org.apache.spark.sql.functions.col): _*)
      val plan = collectPlan(df)
      assert(
        exchanges(plan).nonEmpty && exchanges(plan).forall(_.isInstanceOf[VectorShuffleExchangeExec]),
        s"$keys: $plan"
      )
      assert(rows(df, enabled = true) === rows(df, enabled = false), keys.mkString(","))
    }
    val ordered = "select ai, k from nk order by ai desc nulls last, k"
    assertOurExchange(spark.sql(ordered))
    val ours = spark.sql(ordered).collect().map(r => show(r)).toSeq
    spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "false")
    try assert(ours === spark.sql(ordered).collect().map(r => show(r)).toSeq)
    finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
  }

  test("array, map and nested struct columns cross our exchange as nested lanes") {
    val path = tempDir.resolve("nested").toString
    spark.range(0, 6000).selectExpr(
      "cast(id as int) as k",
      "case when id % 7 = 0 then null when id % 11 = 0 then array() " +
        "else array(cast(id as int), null, cast(-id as int)) end as ai",
      "case when id % 5 = 0 then null else array(concat('k', id), null, '') end as sa",
      "case when id % 13 = 0 then null else array(array(id, null), array(), null) end as aa",
      "case when id % 6 = 0 then null when id % 8 = 0 then map() " +
        "else map(concat('a', id % 50), cast(id as double) / 3, 'b', null) end as m",
      "map(cast(id % 9 as int), array(cast(id % 4 as decimal(9,2)), null)) as ma",
      "case when id % 4 = 0 then null else named_struct('x', cast(id as bigint), " +
        "'y', array(date_add(date'2020-01-01', cast(id % 400 as int)), null)) end as sn",
      "array(named_struct('p', id % 3 = 0, 'q', cast(id as decimal(30,4))), null) as asx",
      "case when id % 10 = 0 then null else array(unhex(hex(id)), null, unhex('FF00')) end as abin"
    ).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("nested")
    Seq(7, 300).foreach { n =>
      def df = spark.table("nested").repartition(n, org.apache.spark.sql.functions.col("k"))
      val plan = collectPlan(df)
      assert(
        exchanges(plan).nonEmpty && exchanges(plan).forall(_.isInstanceOf[VectorShuffleExchangeExec]),
        s"expected only our exchange in\n$plan"
      )
      def rows(enabled: Boolean): Seq[String] = {
        spark.sessionState.conf.setConfString("spark.vecruntime.enabled", enabled.toString)
        try df.selectExpr("spark_partition_id() as p", "*").collect().map(r => show(r)).toSeq.sorted
        finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
      }
      assert(rows(enabled = true) === rows(enabled = false), s"$n partitions")
    }
    // A grouped aggregate over our exchange still carries the nested column as a buffer.
    checkAgainstSpark("select k % 10 g, count(*) c, max(size(ai)) s from nested group by k % 10")
  }

  test("hash keys that are expressions (q47's self-join on rn + 1) are materialised under our exchange") {
    // A sort-merge self-join whose one side partitions on `k + 1`: without the materialised key the
    // exchange stayed Spark's, with a row Sort and a RowToColumnar over it.
    val sql =
      "select a.k, a.c, b.c from (select k, count(*) c from t group by k) a join (select k, count(*) c from t group by k) b on a.k = b.k + 1"
    spark.sessionState.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
    try {
      val plan =
        assertOurExchange(spark.sql(sql), expectedCount = 2) // the aggregate's exchange is reused for the other side
      assert(nodes(plan).collect { case r: RowToColumnarExec => r }.isEmpty, s"RowToColumnar in\n$plan")
      assert(
        nodes(plan).collect { case s: org.apache.spark.sql.execution.SortExec => s }.isEmpty,
        s"Spark's Sort in\n$plan"
      )
      // The exchange declares the original keys (what the join required); the projection under it holds the value.
      val computed = nodes(
        plan
      ).collect { case e: VectorShuffleExchangeExec => e }.filter(_.outputPartitioning.toString.contains("+ 1"))
      assert(computed.size === 1, s"expected one exchange on k + 1 in\n$plan")
      assert(
        computed.head.child.output.exists(_.name.startsWith("_shuffle_key_")),
        s"no materialised key under\n${computed.head}"
      )
      checkAgainstSpark(sql)
      // The same key with the plain side of the join the other way round, and a wider expression.
      val sql2 =
        "select a.k, b.c from (select k, count(*) c from t group by k) a join (select k, count(*) c from t group by k) b on a.k - 2 = b.k * 1"
      assertOurExchange(
        spark.sql(sql2),
        expectedCount = 3
      ) // b.k * 1 folds to b.k: its aggregate's exchange serves the join
      checkAgainstSpark(sql2)
    } finally spark.sessionState.conf.unsetConf("spark.sql.autoBroadcastJoinThreshold")
  }

  test("single partition: a global aggregate") {
    val sql = "select count(*), sum(x), min(d), max(dec) from t"
    assertOurExchange(spark.sql(sql))
    checkAgainstSpark(sql)
  }

  test("round robin: repartition(n) keeps every row exactly once") {
    val df = spark.table("t").repartition(5)
    assertOurExchange(df.select("id"))
    assert(df.rdd.getNumPartitions === 5)
    val ids = df.select("id").collect().map(_.getLong(0)).sorted
    assert(ids.toSeq === (0L until 20000L))
  }

  /**
   * TIMESTAMP_NTZ (#705) rides the INT64 lane: as a hash key it lands where Spark's exchange puts it
   * (Spark hashes it as a long), as a range key the order is Spark's, and as a payload it reads back.
   */
  /**
   * FLOAT (#712) crosses our exchange on its FLOAT64 lane and comes back as float; a float hash key hashes
   * the float's own bits, as Spark does, and a range key keeps Spark's order.
   */
  test("float columns, hash keys and range keys cross our exchange") {
    val path = tempDir.resolve("floats").toString
    spark.range(0, 5000).selectExpr(
      "cast(id as int) as k",
      "case when id % 31 = 1 then cast('NaN' as float) when id % 31 = 2 then cast(-0.0 as float) " +
        "when id % 31 = 3 then cast('-Infinity' as float) when id % 7 = 0 then null " +
        "else cast(id / 3.0 as float) end as f",
      "named_struct('x', cast(id % 5 as float), 'y', cast(id as int)) as st"
    ).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("fl")
    def rows(df: => DataFrame, enabled: Boolean): Seq[String] = {
      spark.sessionState.conf.setConfString("spark.vecruntime.enabled", enabled.toString)
      try df.selectExpr("spark_partition_id() as p", "*").collect().map(_.toString).toSeq.sorted
      finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
    }
    def byK = spark.table("fl").repartition(5, org.apache.spark.sql.functions.col("k"))
    val plan = collectPlan(byK)
    assert(exchanges(plan).nonEmpty && exchanges(plan).forall(_.isInstanceOf[VectorShuffleExchangeExec]), s"$plan")
    assert(rows(byK, enabled = true) === rows(byK, enabled = false))
    // Float hash keys (top level and in a struct) hash the float's own bits (KeyKind.FLOAT): every row lands
    // in Spark's partition, NaN, both zeros and -Infinity included.
    Seq("f", "st").foreach { c =>
      def byF = spark.table("fl").repartition(5, org.apache.spark.sql.functions.col(c))
      val fp = collectPlan(byF)
      assert(exchanges(fp).nonEmpty && exchanges(fp).forall(_.isInstanceOf[VectorShuffleExchangeExec]), s"$c: $fp")
      assert(rows(byF, enabled = true) === rows(byF, enabled = false), c)
    }
    // A float range key: Spark's order, through our exchange.
    val ordered = "select f, k from fl order by f desc nulls last, k"
    assertOurExchange(spark.sql(ordered))
    val ours = spark.sql(ordered).collect().map(_.toString).toSeq
    spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "false")
    try assert(ours === spark.sql(ordered).collect().map(_.toString).toSeq)
    finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
    checkAgainstSpark("select f, count(*) c, max(k) m from fl group by f")
  }

  test("timestamp_ntz columns, hash keys and range keys cross our exchange") {
    val path = tempDir.resolve("ntz").toString
    spark.range(0, 20000).selectExpr(
      "cast(id % 97 as int) as k",
      "case when id % 11 = 0 then null else " +
        "cast(timestamp_micros((id % 3000 - 200) * 917000000001L) as timestamp_ntz) end as n"
    ).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("ntz")
    val grouped = "select n, count(*) c, max(k) mk from ntz group by n"
    assertOurExchange(spark.sql(grouped))
    checkAgainstSpark(grouped)
    checkAgainstSpark("select k, min(n) mn, max(n) mx from ntz group by k")
    // Each row's reduce partition is Spark's: spark_partition_id() over a repartition by the key.
    checkAgainstSpark("select spark_partition_id() p, n from (select /*+ REPARTITION(6, n) */ n from ntz)")
    val ordered = "select n, k from ntz order by n desc nulls last, k limit 3000"
    val ours = spark.sql(ordered).collect().toSeq
    spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "false")
    try assert(ours === spark.sql(ordered).collect().toSeq)
    finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
    val plan = collectPlan(spark.sql("select n, k from ntz order by n, k"))
    assert(exchanges(plan).exists(_.isInstanceOf[VectorShuffleExchangeExec]), s"$plan")
  }

  /**
   * #694: a shuffle stage first submitted after its SQL execution ended (AQE returned on a plan that
   * no longer read it) is not run -- it would only occupy the cluster under the next query. A stage
   * submitted while its execution is live, or with no execution at all, runs as before.
   */
  test("a shuffle stage submitted after its SQL execution ended does not run (#694)") {
    import java.util.concurrent.atomic.AtomicInteger
    import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
    import org.apache.spark.sql.execution.SQLExecution
    val jobs = new AtomicInteger
    val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit = jobs.incrementAndGet()
    }
    def exchange(): VectorShuffleExchangeExec = {
      spark.sessionState.conf.setConfString("spark.sql.adaptive.enabled", "false")
      try {
        val plan = spark.sql("select k, count(*) c from t group by k").queryExecution.executedPlan
        nodes(plan).collectFirst { case e: VectorShuffleExchangeExec => e }.getOrElse(fail(s"no exchange in\n$plan"))
      } finally spark.sessionState.conf.setConfString("spark.sql.adaptive.enabled", "true")
    }
    val sc = spark.sparkContext
    sc.addSparkListener(listener)
    try {
      // An execution id that is not (any longer) registered: the stage is not submitted.
      val ended = exchange()
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, Long.MaxValue.toString)
      try {
        val f = ended.mapOutputStatisticsFuture
        assert(f.isCompleted && f.value.exists(_.isFailure), "a stage of an ended execution was submitted")
      } finally sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
      _root_.org.apache.spark.sql.vecruntime.shuffle.ListenerBusWait(sc)
      assert(jobs.get === 0)
      // No execution id at all (an RDD action outside SQL): submitted as before.
      val stats =
        scala.concurrent.Await.result(exchange().mapOutputStatisticsFuture, scala.concurrent.duration.Duration.Inf)
      assert(stats != null && stats.bytesByPartitionId.sum > 0)
      _root_.org.apache.spark.sql.vecruntime.shuffle.ListenerBusWait(sc)
      assert(jobs.get === 1)
      // The kill switch runs the late stage as Spark does.
      spark.sessionState.conf.setConfString(VectorShuffleExchangeExec.SkipEndedExecutionStages, "false")
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, Long.MaxValue.toString)
      try scala.concurrent.Await.result(exchange().mapOutputStatisticsFuture, scala.concurrent.duration.Duration.Inf)
      finally {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
        spark.sessionState.conf.unsetConf(VectorShuffleExchangeExec.SkipEndedExecutionStages)
      }
      _root_.org.apache.spark.sql.vecruntime.shuffle.ListenerBusWait(sc)
      assert(jobs.get === 2)
      // A normal query (its execution live while its stages run) is untouched.
      checkAgainstSpark("select k, count(*) c, sum(x) sx from t group by k")
    } finally sc.removeSparkListener(listener)
  }

  /**
   * BINARY (#705) crosses our exchange on the UTF8 lane, always plain: as a hash key every row lands
   * where Spark puts it (both hash the bytes with hashUnsafeBytes), as a range key in Spark's unsigned
   * byte order, and as a payload it reads back byte for byte -- including bytes above 0x7f.
   */
  test("binary columns, hash keys and range keys cross our exchange") {
    val path = tempDir.resolve("bin").toString
    spark.range(0, 20000).selectExpr(
      "cast(id % 97 as int) as k",
      "case when id % 13 = 0 then null when id % 29 = 0 then x'' " +
        "else unhex(lpad(hex((id * 2654435761) % 3001), 6, 'F')) end as b"
    ).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("bin")
    val grouped = "select hex(b) h, count(*) c, max(k) mk from bin group by b" // hex: rows compare as strings
    assertOurExchange(spark.sql(grouped))
    checkAgainstSpark(grouped)
    checkAgainstSpark("select k, count(distinct b) nb from bin group by k")
    checkAgainstSpark("select spark_partition_id() p, hex(b) h from (select /*+ REPARTITION(6, b) */ b from bin)")
    val ordered = "select hex(b) h, k from bin order by b desc nulls last, k limit 3000"
    val ours = spark.sql(ordered).collect().toSeq
    spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "false")
    try assert(ours === spark.sql(ordered).collect().toSeq)
    finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
    val plan = collectPlan(spark.sql("select b, k from bin order by b, k"))
    assert(exchanges(plan).exists(_.isInstanceOf[VectorShuffleExchangeExec]), s"$plan")
  }

  test("collated string columns cross our exchange as their bytes; a collated hash key stays Spark's (#705)") {
    val path = tempDir.resolve("collated").toString
    spark.range(0, 20000).selectExpr(
      "cast(id % 97 as int) as k",
      "cast(id as int) as i",
      "collate(case when id % 13 = 0 then null else element_at(array('a', 'A', 'b', 'B', 'é', 'É'), " +
        "cast(id % 6 as int) + 1) end, 'UTF8_LCASE') as s",
      "array(collate(element_at(array('q', 'Q'), cast(id % 2 as int) + 1), 'UNICODE_CI')) as arr"
    ).write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("coll")
    // Payload: hash on k, the collated columns carried.
    val carried = spark.table("coll").repartition(8, org.apache.spark.sql.functions.col("k"))
    assertOurExchange(carried)
    checkAgainstSpark("select * from (select /*+ REPARTITION(8, k) */ * from coll)")
    // Keys: a collated hash key hashes its collation key in Spark; equal values meet in one partition.
    val byS = "select spark_partition_id() p, lower(s) ls from (select /*+ REPARTITION(8, s) */ s from coll)"
    checkAgainstSpark(byS)
    val keyed = collectPlan(spark.sql("select /*+ REPARTITION(8, s) */ s from coll"))
    assert(exchanges(keyed).forall(_.isInstanceOf[ShuffleExchangeExec]), s"$keyed")
    checkAgainstSpark("select lower(s) ls, count(*) c from coll group by s")
    // Range keys: Spark's partitioner over the rows, the collation's order.
    val ordered = "select s, k, i from coll order by s, k, i limit 3000" // i: a total order
    val ours = spark.sql(ordered).collect().toSeq
    spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "false")
    try assert(ours === spark.sql(ordered).collect().toSeq)
    finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
  }

  test("range partitioning: a global order by is Spark's rows in Spark's order") {
    val sql = "select s, x, k from t order by s desc, x, k limit 5000"
    val ours = spark.sql(sql).collect().toSeq
    spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "false")
    try assert(ours === spark.sql(sql).collect().toSeq)
    finally spark.sessionState.conf.setConfString("spark.vecruntime.enabled", "true")
    val plan = collectPlan(spark.sql("select s, x, k from t order by s desc, x, k"))
    assert(exchanges(plan).exists(_.isInstanceOf[VectorShuffleExchangeExec]), s"$plan")
  }

  test("AQE coalesces small partitions over our exchange") {
    spark.sessionState.conf.setConfString("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.sessionState.conf.setConfString("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
    spark.sessionState.conf.setConfString("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
    try {
      val df = spark.sql("select k, count(*) c from t group by k")
      val plan = collectPlan(df)
      assert(nodes(plan).collect { case r: AQEShuffleReadExec => r }.nonEmpty, s"no coalesced read in\n$plan")
      assertOurExchange(df)
      checkAgainstSpark("select k, count(*) c from t group by k")
    } finally {
      spark.sessionState.conf.unsetConf("spark.sql.adaptive.coalescePartitions.enabled")
      spark.sessionState.conf.unsetConf("spark.sql.adaptive.advisoryPartitionSizeInBytes")
      spark.sessionState.conf.unsetConf("spark.sql.adaptive.coalescePartitions.minPartitionSize")
    }
  }

  test("the stage metrics see our shuffle: bytes and records written, local blocks and bytes read, AQE's data size") {
    val stages = scala.collection.mutable.ArrayBuffer.empty[org.apache.spark.scheduler.StageInfo]
    val listener = new org.apache.spark.scheduler.SparkListener {
      override def onStageCompleted(e: org.apache.spark.scheduler.SparkListenerStageCompleted): Unit =
        stages.synchronized { stages += e.stageInfo }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      val df = spark.sql("select k, count(*) c, sum(x) sx from t group by k")
      val plan = collectPlan(df)
      val deadline = System.nanoTime() + 10000000000L
      while (
        System.nanoTime() < deadline && stages.synchronized(
          stages.map(_.taskMetrics.shuffleReadMetrics.localBlocksFetched).sum
        ) == 0
      ) Thread.sleep(50)
      val ex = exchanges(plan).collectFirst { case e: VectorShuffleExchangeExec => e }.get
      val write = stages.synchronized(stages.map(_.taskMetrics.shuffleWriteMetrics.bytesWritten).sum)
      val read = stages.synchronized(stages.map(_.taskMetrics.shuffleReadMetrics))
      assert(write > 0, "shuffle bytes written")
      val sql = ex.metrics.filter(_._1.matches(".*(Blocks|Bytes|records|Wait|Time|Size|dataSize).*")).map {
        case (k, m) => s"$k=${m.value}"
      }.toSeq.sorted.mkString(", ")
      assert(
        read.map(_.localBlocksFetched).sum > 0,
        s"local blocks fetched; SQL metrics: $sql; stage read totals: blocks=${read.map(_.localBlocksFetched).sum}/${read.map(_.remoteBlocksFetched).sum} bytes=${read.map(_.totalBytesRead).sum}"
      )
      assert(
        read.map(_.totalBytesRead).sum === write,
        s"bytes read ${read.map(_.totalBytesRead).sum} vs written $write"
      )
      assert(read.map(_.recordsRead).sum > 0, "records read")
      // dataSize is the pre-compression Arrow size, as Spark's is its rows' pre-compression size: at least the compressed bytes.
      assert(
        ex.metrics("dataSize").value >= write,
        s"AQE's data size ${ex.metrics("dataSize").value} vs bytes written $write"
      )
      assert(ex.runtimeStatistics.sizeInBytes.toLong === ex.metrics("dataSize").value)
    } finally spark.sparkContext.removeSparkListener(listener)
  }

  test("#358: unregistering a shuffle deletes our map outputs' data and index files") {
    val df = spark.sql("select k, count(*) c from t group by k")
    val plan = collectPlan(df)
    val ex = exchanges(plan).collectFirst { case e: VectorShuffleExchangeExec => e }.get
    val shuffleId = ex.shuffleDependency.shuffleId
    val disk = org.apache.spark.SparkEnv.get.blockManager.diskBlockManager
    def files() = disk.getAllFiles().filter(_.getName.startsWith(s"shuffle_${shuffleId}_")).map(_.getName).sorted
    val before = files()
    assert(before.exists(_.endsWith(".data")) && before.exists(_.endsWith(".index")), s"map outputs on disk: $before")
    // What the ContextCleaner does on every executor once the dependency is unreachable.
    assert(org.apache.spark.SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId))
    assert(files().isEmpty, s"left behind: ${files()}")
  }

  test("a shuffled hash join and a merge join read both sides from our exchanges") {
    spark.sessionState.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
    try {
      val sql =
        "select a.k, count(*) from t a join (select k, count(*) n from t group by k) b on a.k = b.k group by a.k"
      val plan = collectPlan(spark.sql(sql))
      val ex = exchanges(plan)
      assert(ex.nonEmpty && ex.forall(_.isInstanceOf[VectorShuffleExchangeExec]), s"$plan")
      checkAgainstSpark(sql)
    } finally spark.sessionState.conf.unsetConf("spark.sql.autoBroadcastJoinThreshold")
  }
}
