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

import scala.jdk.CollectionConverters._

import org.apache.parquet.column.statistics.{
  BooleanStatistics,
  DoubleStatistics,
  FloatStatistics,
  IntStatistics,
  LongStatistics,
  Statistics
}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData}
import org.apache.spark.sql.sources._

/**
 * #962: estimate the fraction of a row group's rows a pushed filter keeps, so the native scan can decide per row
 * group whether late materialization pays. The estimate uses only evidence already at hand at read time:
 *
 *   - column statistics (min/max/null count) against the filter -- a range or equality predicate whose bounds
 *     fall outside [min, max] keeps nothing, one strictly inside keeps a crude fraction from where the bounds sit;
 *   - dictionary pages for an equality or IN predicate on a fully dictionary-encoded column (matching dictionary
 *     entries / dictionary size);
 *   - bloom filters where present (an equality/IN value the bloom filter rejects contributes nothing);
 *   - the observed selectivity of earlier row groups of the same file, blended in with hysteresis by the caller.
 *
 * The estimate is deliberately conservative: an unknown predicate or a column with no statistics is treated as
 * keeping everything (fraction 1.0), so `auto` never late-materializes a row group it cannot reason about. A
 * conjunction multiplies its conjuncts' fractions (assuming independence); a disjunction / negation is treated
 * as 1.0 (unknown), since its selectivity cannot be bounded from one side's statistics. Correctness never
 * depends on the estimate -- the decode filter is applied in full either way; only whether the OTHER columns are
 * read eagerly or at the survivors depends on it.
 */
private[vecruntime] object LateMaterializationEstimator {

  /** The fraction of a row group's rows the conjuncts of `filters` keep, in [0, 1]; 1.0 when nothing is known. */
  def estimate(
      filters: Seq[Filter],
      block: BlockMetaData,
      reader: ParquetFileReader,
      blockIndex: Int,
      dictionaries: Boolean,
      blooms: Boolean
  ): Double = {
    val total = block.getRowCount
    if (total <= 0) return 1.0
    val byPath: Map[String, ColumnChunkMetaData] =
      block.getColumns.asScala.map(c => c.getPath.toDotString -> c).toMap
    var fraction = 1.0
    for (f <- filters.flatMap(conjuncts)) {
      fraction *= clamp(one(f, total, byPath, reader, blockIndex, dictionaries, blooms))
    }
    clamp(fraction)
  }

  private def conjuncts(f: Filter): Seq[Filter] = f match {
    case And(l, r) => conjuncts(l) ++ conjuncts(r)
    case other => Seq(other)
  }

  private def clamp(d: Double): Double = if (d.isNaN) 1.0 else math.max(0.0, math.min(1.0, d))

  /** The keep-fraction of ONE (non-And) predicate; 1.0 for anything it cannot bound. */
  private def one(
      f: Filter,
      total: Long,
      byPath: Map[String, ColumnChunkMetaData],
      reader: ParquetFileReader,
      blockIndex: Int,
      dictionaries: Boolean,
      blooms: Boolean
  ): Double = f match {
    case IsNotNull(a) =>
      col(byPath, a).map { c =>
        val st = c.getStatistics
        if (st == null || !st.isNumNullsSet) 1.0 else (total - st.getNumNulls).toDouble / total
      }.getOrElse(1.0)
    case IsNull(a) =>
      col(byPath, a).map { c =>
        val st = c.getStatistics
        if (st == null || !st.isNumNullsSet) 1.0 else st.getNumNulls.toDouble / total
      }.getOrElse(1.0)
    case EqualTo(a, v) => equality(a, Seq(v), total, byPath, reader, blockIndex, dictionaries, blooms)
    case EqualNullSafe(a, v) => equality(a, Seq(v), total, byPath, reader, blockIndex, dictionaries, blooms)
    case In(a, vs) if vs != null && vs.nonEmpty =>
      equality(a, vs.toSeq, total, byPath, reader, blockIndex, dictionaries, blooms)
    case GreaterThan(a, v) => range(a, Some(v), None, lowOpen = true, highOpen = false, byPath)
    case GreaterThanOrEqual(a, v) => range(a, Some(v), None, lowOpen = false, highOpen = false, byPath)
    case LessThan(a, v) => range(a, None, Some(v), lowOpen = false, highOpen = true, byPath)
    case LessThanOrEqual(a, v) => range(a, None, Some(v), lowOpen = false, highOpen = false, byPath)
    case _ => 1.0 // Or / Not / string predicates / unknown: not bounded from one side's statistics.
  }

  private def col(byPath: Map[String, ColumnChunkMetaData], name: String): Option[ColumnChunkMetaData] =
    byPath.get(name)

  /** As a double, a numeric statistic's bound or a filter value; None for anything not numerically comparable. */
  private def num(v: Any): Option[Double] = v match {
    case null => None
    case b: java.lang.Boolean => Some(if (b) 1.0 else 0.0)
    case n: java.lang.Byte => Some(n.doubleValue())
    case n: java.lang.Short => Some(n.doubleValue())
    case n: java.lang.Integer => Some(n.doubleValue())
    case n: java.lang.Long => Some(n.doubleValue())
    case n: java.lang.Float => Some(n.doubleValue())
    case n: java.lang.Double => Some(n.doubleValue())
    case d: java.math.BigDecimal => Some(d.doubleValue())
    case d: BigDecimal => Some(d.toDouble)
    case d: java.sql.Date => Some(d.toLocalDate.toEpochDay.toDouble)
    case d: java.time.LocalDate => Some(d.toEpochDay.toDouble)
    case _ => None
  }

  /** The numeric [min, max] of a column chunk's statistics, when both are set and numeric. */
  private def minMax(st: Statistics[_]): Option[(Double, Double)] = st match {
    case s: IntStatistics if hasMinMax(s) => Some((s.getMin.toDouble, s.getMax.toDouble))
    case s: LongStatistics if hasMinMax(s) => Some((s.getMin.toDouble, s.getMax.toDouble))
    case s: DoubleStatistics if hasMinMax(s) => Some((s.getMin, s.getMax))
    case s: FloatStatistics if hasMinMax(s) => Some((s.getMin.toDouble, s.getMax.toDouble))
    case s: BooleanStatistics if hasMinMax(s) => Some((if (s.getMin) 1.0 else 0.0, if (s.getMax) 1.0 else 0.0))
    case _ => None
  }

  private def hasMinMax(st: Statistics[_]): Boolean = st != null && st.hasNonNullValue && !st.isEmpty

  /**
   * A range predicate's keep-fraction from the column's [min, max]: 0 when the range misses it entirely, a
   * linear fraction of [min, max] the range covers otherwise (uniformity assumption), scaled by the non-null
   * fraction. 1.0 when the statistics are missing or not numeric (e.g. a string range).
   */
  private def range(
      a: String,
      low: Option[Any],
      high: Option[Any],
      lowOpen: Boolean,
      highOpen: Boolean,
      byPath: Map[String, ColumnChunkMetaData]
  ): Double = {
    val cc = col(byPath, a).orNull
    if (cc == null) return 1.0
    val st = cc.getStatistics
    minMax(st) match {
      case None => 1.0
      case Some((mn, mx)) =>
        val lo = low.flatMap(num)
        val hi = high.flatMap(num)
        // If the predicate has a numeric bound that the statistics prove misses the whole chunk, keep nothing.
        val missesLow = lo.exists(l => if (lowOpen) l >= mx else l > mx)
        val missesHigh = hi.exists(h => if (highOpen) h <= mn else h < mn)
        if (missesLow || missesHigh) return 0.0
        val nonNull = if (st.isNumNullsSet) 1.0 - st.getNumNulls.toDouble / math.max(1L, cc.getValueCount) else 1.0
        val span = mx - mn
        if (span <= 0.0) return nonNull // a single-value chunk the predicate does not miss: keeps all non-nulls
        val coverLo = math.max(mn, lo.getOrElse(mn))
        val coverHi = math.min(mx, hi.getOrElse(mx))
        val covered = math.max(0.0, coverHi - coverLo) / span
        clamp(covered) * nonNull
    }
  }

  /**
   * An equality or IN predicate's keep-fraction. In order of evidence: statistics whose [min, max] exclude every
   * value keep nothing; a bloom filter that rejects every value keeps nothing; a fully dictionary-encoded column
   * keeps (matching entries / dictionary size) of its non-nulls. Otherwise a crude guess capped at the non-null
   * fraction.
   */
  private def equality(
      a: String,
      values: Seq[Any],
      total: Long,
      byPath: Map[String, ColumnChunkMetaData],
      reader: ParquetFileReader,
      blockIndex: Int,
      dictionaries: Boolean,
      blooms: Boolean
  ): Double = {
    val cc = col(byPath, a).orNull
    if (cc == null) return 1.0
    val st = cc.getStatistics
    val nonNullCount = if (st != null && st.isNumNullsSet) total - st.getNumNulls else total
    if (nonNullCount <= 0) return 0.0
    val nonNull = nonNullCount.toDouble / total

    // 1. Statistics: every numeric value outside [min, max] -> keeps nothing.
    minMax(st).foreach { case (mn, mx) =>
      if (values.forall(v => num(v).isDefined)) {
        val anyInRange = values.exists(v => num(v).exists(d => d >= mn && d <= mx))
        if (!anyInRange) return 0.0
      }
    }

    // 2. Bloom filter: present and rejecting every value -> keeps nothing.
    if (blooms) {
      try {
        val bf = reader.getBloomFilterDataReader(reader.getRowGroups.get(blockIndex)).readBloomFilter(cc)
        if (bf != null) {
          val anyMaybe = values.exists { v =>
            bloomHashValue(v).forall(h => bf.findHash(bf.hash(h)))
          }
          if (!anyMaybe) return 0.0
        }
      } catch { case _: Throwable => () }
    }

    // 3. Dictionary: a fully dictionary-encoded column -> fraction of distinct dictionary ids that match.
    if (dictionaries) {
      dictionaryMatchFraction(cc, reader, blockIndex, values).foreach(fr => return fr * nonNull)
    }

    // 4. Fallback: a crude guess capped at the non-null fraction, never claiming better than a small constant.
    math.min(nonNull, 0.1 * values.size)
  }

  /**
   * The fraction of a dictionary-encoded column's distinct dictionary entries that match `values`, when EVERY
   * page of the chunk is dictionary-encoded (so the dictionary is the column's full value domain in this row
   * group); None when the column is not fully dictionary-encoded or the dictionary cannot be read.
   */
  private def dictionaryMatchFraction(
      cc: ColumnChunkMetaData,
      reader: ParquetFileReader,
      blockIndex: Int,
      values: Seq[Any]
  ): Option[Double] = {
    try {
      if (!cc.hasDictionaryPage) return None
      val encs = cc.getEncodingStats
      if (encs == null || !encs.hasDictionaryPages || encs.hasNonDictionaryEncodedPages) return None
      val fileSchema = reader.getFileMetaData.getSchema
      val path = cc.getPath.toArray
      if (!fileSchema.containsPath(path)) return None
      val descriptor = fileSchema.getColumnDescription(path)
      val dictReader: org.apache.parquet.column.page.DictionaryPageReadStore =
        reader.getDictionaryReader(reader.getRowGroups.get(blockIndex))
      val page = dictReader.readDictionaryPage(descriptor)
      if (page == null) return None
      val dict = page.getEncoding.initDictionary(descriptor, page)
      if (dict == null) return None
      val size = dict.getMaxId + 1
      if (size <= 0) return None
      var matches = 0
      var id = 0
      while (id < size) {
        if (values.exists(v => dictEntryEquals(dict, id, v, cc))) matches += 1
        id += 1
      }
      Some(matches.toDouble / size)
    } catch { case _: Throwable => None }
  }

  private def dictEntryEquals(
      dict: org.apache.parquet.column.Dictionary,
      id: Int,
      v: Any,
      cc: ColumnChunkMetaData
  ): Boolean = {
    import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
    try {
      cc.getPrimitiveType.getPrimitiveTypeName match {
        case INT32 => num(v).contains(dict.decodeToInt(id).toDouble)
        case INT64 => num(v).contains(dict.decodeToLong(id).toDouble)
        case DOUBLE => num(v).contains(dict.decodeToDouble(id))
        case FLOAT => num(v).contains(dict.decodeToFloat(id).toDouble)
        case BOOLEAN => num(v).contains(if (dict.decodeToBoolean(id)) 1.0 else 0.0)
        case BINARY | FIXED_LEN_BYTE_ARRAY =>
          val b = dict.decodeToBinary(id)
          v match {
            case s: String => b.toStringUsingUTF8 == s
            case us: org.apache.spark.unsafe.types.UTF8String => b.toStringUsingUTF8 == us.toString
            case other => b.toStringUsingUTF8 == String.valueOf(other)
          }
        case _ => false
      }
    } catch { case _: Throwable => false }
  }

  /** The value a bloom filter hashes, matching how Parquet writes equality values; None when not supported. */
  private def bloomHashValue(v: Any): Option[Any] = v match {
    case null => None
    case s: String => Some(org.apache.parquet.io.api.Binary.fromString(s))
    case us: org.apache.spark.unsafe.types.UTF8String => Some(org.apache.parquet.io.api.Binary.fromString(us.toString))
    case n: java.lang.Integer => Some(n)
    case n: java.lang.Long => Some(n)
    case n: java.lang.Float => Some(n)
    case n: java.lang.Double => Some(n)
    case _ => None
  }
}
