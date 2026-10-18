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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import io.vecruntime.spark.VectorConf
import io.vecruntime.spark.test.VectorQuerySuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.vecruntime.{PlanUtils, VectorFallback, VectorParquetScanExec}

/**
 * #808: every Parquet file of two reader conformance corpora, read by the native scan and by Spark's own reader.
 *
 *  - `apache/parquet-testing` (`parquet-testing/data`, `parquet-testing/bad_data`; commit in its `SOURCE_SHA`):
 *    files from parquet-mr of several ages, parquet-cpp, arrow-rs and Impala, and corrupt files.
 *  - Hardwood's core test resources (`hardwood/`; commit in its `SOURCE_SHA`), the corpus its reader tests run
 *    over: every physical and logical type, encodings, codecs, page indexes, bloom filters, nested shapes and
 *    malformed files.
 *
 * For each file, the reference is Spark's outcome with the plugin off: its vectorized reader, or its row-based
 * reader when the vectorized one refuses the file, or the refusal when both do. With the plugin on the scan must
 * give the same rows (compared by value, order-free) or also refuse.
 *
 * How the plugin read it is pinned in `parquet-corpus-expected.tsv`, one line per file (and, for a file not read
 * natively as a whole, per column): `native`, `native-empty`, `file-fallback` (the native scan handed the file to
 * Spark's reader), `plan-fallback: <reason>` or `refused` (Spark refuses it too). A case that moves -- a
 * native read that starts falling back, or a fallback that becomes native -- fails the suite until the file is
 * regenerated with `CORPUS_UPDATE_BASELINE=true`, so every fallback is a recorded choice, never a silent gap.
 */
class ParquetCorpusConformanceSuite extends VectorQuerySuite {

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map(VectorConf.ScanNativeParquet -> "true")

  private val roots = Seq("parquet-testing/data", "parquet-testing/bad_data", "hardwood")

  private def resourceDir(rel: String): File = {
    val url = getClass.getClassLoader.getResource(rel)
    assert(url != null, s"missing test resource directory $rel")
    new File(url.toURI)
  }

  /** Every corpus file, as (corpus-relative name, absolute path), in a stable order. */
  private lazy val corpus: Seq[(String, String)] = roots.flatMap { root =>
    val dir = resourceDir(root)
    def walk(f: File): Seq[File] =
      if (f.isDirectory) Option(f.listFiles()).toSeq.flatten.sortBy(_.getName).flatMap(walk)
      else if (f.getName.endsWith(".parquet")) Seq(f)
      else Nil
    walk(dir).map(f => (root + "/" + dir.toPath.relativize(f.toPath).toString.replace('\\', '/'), f.getAbsolutePath))
  }

  /** A value rendered by content: binaries as hex, maps by sorted entries, rows and arrays element by element. */
  private def render(v: Any): String = v match {
    case null => "null"
    case b: Array[Byte] => b.map("%02x".format(_)).mkString("0x", "", "")
    case r: Row => r.toSeq.map(render).mkString("{", ",", "}")
    case m: scala.collection.Map[_, _] => m.toSeq.map { case (k, x) => render(k) + "->" + render(x) }.sorted
        .mkString("map(", ",", ")")
    case s: scala.collection.Seq[_] => s.map(render).mkString("[", ",", "]")
    case d: Double => java.lang.Double.toString(d)
    case f: Float => java.lang.Float.toString(f)
    case other => other.toString
  }

  private def rows(df: DataFrame): Seq[String] = df.collect().toSeq.map(render).sorted

  private def outcome[T](f: => T): Either[Throwable, T] =
    try Right(f)
    catch { case e: Exception => Left(e) }

  /** Spark's own outcome, the plugin off: vectorized reader, else row-based, else the refusal. */
  private def reference(read: () => DataFrame): Either[Throwable, Seq[String]] =
    withPlugin(enabled = false) {
      outcome(rows(read())) match {
        case ok @ Right(_) => ok
        case Left(_) => withConf("spark.sql.parquet.enableVectorizedReader" -> "false")(outcome(rows(read())))
      }
    }

  /**
   * The plugin's read of `read()` against Spark's: rows must match, or both refuse. Returns how the plugin read
   * it.
   */
  private def check(what: String, read: () => DataFrame): String = {
    val expected = reference(read)
    val actual = withPlugin(enabled = true) {
      outcome {
        val df = read()
        val r = rows(df)
        val plan = finalPlan(df)
        val scans = PlanUtils.allNodes(plan).collect { case s: VectorParquetScanExec => s }
        val how =
          if (scans.isEmpty) {
            val reasons = VectorFallback.reasons(plan).map(_._2).filter(_.nonEmpty)
            "plan-fallback: " + reasons.headOption.getOrElse("no native scan").replaceAll("[\t\r\n]+", " ")
          } else {
            val rowGroups = scans.map(_.metrics("numRowGroups").value).sum
            if (rowGroups > 0) "native" else if (r.isEmpty) "native-empty" else "file-fallback"
          }
        (r, how)
      }
    }
    (expected, actual) match {
      case (Right(e), Right((a, how))) =>
        assert(e.length == a.length, s"$what: Spark returns ${e.length} rows, the plugin ${a.length} ($how)")
        val firstDiff = e.zip(a).indexWhere { case (x, y) => x != y }
        if (firstDiff >= 0) fail(s"$what ($how): first differing row, Spark ${e(firstDiff)} vs plugin ${a(firstDiff)}")
        how
      case (Left(_), Left(_)) => "refused"
      case (Right(_), Left(err)) => fail(s"$what: Spark reads it, the plugin fails: $err", err)
      case (Left(err), Right((a, how))) =>
        fail(s"$what: Spark refuses it (${err.getMessage.take(300)}), the plugin returned ${a.length} rows ($how)")
    }
  }

  /** The outcome lines of one file: the whole file, then each column when the whole is not read natively. */
  private def lines(name: String, path: String): Seq[String] = {
    val whole = check(name, () => spark.read.parquet(path))
    val columns: Seq[String] =
      if (whole == "native" || whole == "native-empty" || whole == "refused") Nil
      else withPlugin(enabled = false)(outcome(spark.read.parquet(path).schema.fieldNames.toSeq)).getOrElse(Nil)
    (s"$name\t*\t$whole") +: columns.map { c =>
      s"$name\t$c\t${check(s"$name column $c", () => spark.read.parquet(path).select(s"`${c.replace("`", "``")}`"))}"
    }
  }

  private val baselineName = "parquet-corpus-expected.tsv"

  /** The Spark line the base baseline is recorded on. */
  private val baseLine = "4.1"

  /** This run's Spark line, major.minor. */
  private val sparkLine = org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")

  private def resourcePath(name: String): java.nio.file.Path =
    Paths.get(sys.props.getOrElse("user.dir", "."), "src", "test", "resources", name)

  private def readBaseline(name: String): Option[Seq[String]] =
    Option(getClass.getClassLoader.getResource(name)).map(url =>
      new String(Files.readAllBytes(Paths.get(url.toURI)), StandardCharsets.UTF_8).linesIterator.toSeq
    )

  test("every corpus file reads as Spark reads it, and each read is the recorded native read or fallback (#808)") {
    assert(corpus.size > 300, s"corpus has only ${corpus.size} files")
    val failures = scala.collection.mutable.ArrayBuffer.empty[String]
    val actual = corpus.flatMap { case (name, path) =>
      try lines(name, path)
      catch {
        case e: org.scalatest.exceptions.TestFailedException =>
          failures += e.getMessage
          Seq(s"$name\t*\tMISMATCH")
        case e: Exception =>
          failures += s"$name: suite error $e"
          Seq(s"$name\t*\tMISMATCH")
      }
    }
    val text = actual.mkString("", "\n", "\n")
    val update = sys.env.get("CORPUS_UPDATE_BASELINE").contains("true")
    val base = readBaseline(baselineName)
    // A Spark line other than the base's (4.1) reads some files differently -- Spark 4.2 reads geospatial types --
    // so its overlay replaces the base's lines for exactly those files.
    val overlayName = s"parquet-corpus-expected-spark-$sparkLine.tsv"
    def fileOf(l: String) = l.takeWhile(_ != '\t')
    // Written before the mismatch check, so a run that finds a mismatch still records every other file's read.
    if (update) {
      if (sparkLine == baseLine) Files.write(resourcePath(baselineName), text.getBytes(StandardCharsets.UTF_8))
      else {
        val baseByFile = base.getOrElse(Nil).groupBy(fileOf)
        val differing = actual.groupBy(fileOf).filter { case (f, ls) => baseByFile.get(f).forall(_ != ls) }.keySet
        val overlay = actual.filter(l => differing.contains(fileOf(l)))
        Files.write(resourcePath(overlayName), overlay.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
      }
    }
    assert(failures.isEmpty, s"${failures.size} corpus files differ from Spark:\n${failures.mkString("\n")}")
    if (!update) {
      assert(base.isDefined, s"missing $baselineName; run with CORPUS_UPDATE_BASELINE=true")
      val overlay = if (sparkLine == baseLine) Nil else readBaseline(overlayName).getOrElse(Nil)
      val replaced = overlay.map(fileOf).toSet
      val expected = base.get.filterNot(l => replaced.contains(fileOf(l))) ++ overlay
      val moved = (expected.diff(actual).map("- " + _) ++ actual.diff(expected).map("+ " + _)).sorted
      assert(
        moved.isEmpty,
        s"corpus reads moved from $baselineName${if (overlay.nonEmpty) s" + $overlayName" else ""} on Spark " +
          s"$sparkLine (rerun with CORPUS_UPDATE_BASELINE=true if intended):\n" + moved.mkString("\n")
      )
    }
    val counts = actual.filter(_.split('\t')(1) == "*").groupBy(_.split('\t')(2).takeWhile(_ != ':')).map {
      case (k, v) => s"$k=${v.size}"
    }
    info(s"${corpus.size} files: ${counts.toSeq.sorted.mkString(", ")}")
  }
}
